import random
import re
import time
import unicodedata
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi, hf_hub_download
from huggingface_hub.utils import EntryNotFoundError, HfHubHTTPError, RepositoryNotFoundError
from loguru import logger

from models import CatalogRecord, StockMasterRecord


class CatalogManager:
    def __init__(self, hf_repo: str, hf_token: str, data_path: Path):
        self.hf_repo = hf_repo
        self.hf_token = hf_token
        self.data_path = data_path
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.api = HfApi() if hf_repo and hf_token else None

        # ファイルパス定義
        self.paths = {
            "catalog": "catalog/documents_index.parquet",
            "master": "meta/stocks_master.parquet",
            "listing": "meta/listing_history.parquet",
            "index": "meta/index_history.parquet",
            "name": "meta/name_history.parquet",
        }

        self.catalog_df = self._load_parquet("catalog")
        self.master_df = self._load_parquet("master")

        # 【最重要】一括コミット用バッファ
        self._commit_operations = {}
        self._snapshots = {}  # 整合性保護のためのロールバックスナップショット
        logger.info("CatalogManager を初期化しました。")

        # 全ファイルの整合性チェックと最新スキーマへのアップグレード
        self._retrospective_cleanse()

    def _retrospective_cleanse(self):
        """データディレクトリ内の全Parquetファイルを走査し、不備があれば自動修正してアップロード"""
        if not self.api:
            return

        logger.info("Starting integrity check for all Parquet files...")
        updated_count = 0

        # 1. 定義済み主要ファイルのチェック
        for key in self.paths.keys():
            try:
                # 既にロード済みの catalog_df, master_df は _load_parquet でクレンジング済み
                df = self.catalog_df if key == "catalog" else (self.master_df if key == "master" else None)
                if df is None:
                    df = self._load_parquet(key)

                # カタログの場合、18カラム未満なら強制保存してスキーマ拡張
                if key == "catalog" and len(df.columns) < 18:
                    self._save_and_upload(key, df, defer=True)
                    updated_count += 1
            except Exception:
                continue

        # 2. マスターの全Binファイルを走査
        try:
            files = self.api.list_repo_files(repo_id=self.hf_repo, repo_type="dataset")
            bin_files = [f for f in files if "master/bin/" in f and f.endswith(".parquet")]

            for b_file in bin_files:
                local_tmp = self.data_path / "temp_cleanse.parquet"
                self.api.hf_hub_download(
                    repo_id=self.hf_repo,
                    filename=b_file,
                    repo_type="dataset",
                    token=self.hf_token,
                    local_dir=str(self.data_path),
                    local_dir_use_symlinks=False,
                )
                df_bin = pd.read_parquet(self.data_path / b_file)

                # スキーマ不適合があればクレンジングして予約
                # (具体的な rec チェックではなく、モデルとの不一致を基準にする)
                df_clean = self._clean_dataframe("master", df_bin)
                if len(df_clean.columns) != len(df_bin.columns):
                    logger.info(f"Cleaned up bin file schema: {b_file}")
                    df_clean.to_parquet(local_tmp, index=False, compression="zstd")
                    self.add_commit_operation(b_file, local_tmp)
                    updated_count += 1
        except Exception:
            pass

    def _clean_dataframe(self, key: str, df: pd.DataFrame) -> pd.DataFrame:
        """全てのDataFrameに対して共通のクレンジングを適用"""
        if df.empty:
            return df

        # 0. カラム名の正規化（空白除去）
        df.columns = df.columns.astype(str).str.strip()

        # 【追加】全文字列カラムの空文字を明示的に None (NULL) に統一
        for col in df.columns:
            if df[col].dtype == "object":
                # 空白のみの文字列も NULL 扱いとする
                df[col] = df[col].apply(lambda x: None if (isinstance(x, str) and not x.strip()) else x)

        # 1. 不要なインデックス由来カラムの除去
        drop_targets = ["index", "level_0", "Unnamed: 0"]
        cols_to_drop = [c for c in drop_targets if c in df.columns]

        if cols_to_drop:
            logger.debug(f"{key}: Removed unnecessary columns: {cols_to_drop}")
            df = df.drop(columns=cols_to_drop)

        # 2. カタログの場合、モデル定義のカラム構成を強制 (18カラム化)
        if key == "catalog":
            # NaN を None に置換
            df = df.replace({pd.NA: None, float("nan"): None})

            # モデル定義の全フィールドを取得
            expected_cols = list(CatalogRecord.model_fields.keys())

            # 既存のカラムのみでPydanticバリデーションを通し、不足分をNoneで補完
            validated = []
            for rec_dict in df.to_dict("records"):
                try:
                    # 欠落しているフィールドがあっても Pydantic がデフォルト値を補完
                    validated.append(CatalogRecord(**rec_dict).model_dump())
                except Exception as e:
                    # 必須項目(doc_id等)が欠けている場合のみエラー
                    logger.warning(f"クレンジング中のバリデーション不備 (doc_id: {rec_dict.get('doc_id')}): {e}")
                    # 構造だけでも維持するため、辞書として可能な限り保持
                    row = {col: rec_dict.get(col) for col in expected_cols}
                    validated.append(row)

            df = pd.DataFrame(validated)
            # カラム順をモデル定義に合わせる
            df = df[expected_cols]

            # 【重要】データ型の正規化 (2024.0 回避のための Int64 適用)
            # pandas の浮動小数点化を阻止し、整数または NULL として保存
            if "fiscal_year" in df.columns:
                df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")
            if "num_months" in df.columns:
                df["num_months"] = pd.to_numeric(df["num_months"], errors="coerce").astype("Int64")

        # 3. マスタの場合、codeを確実に文字列化
        if key == "master" and "code" in df.columns:
            df["code"] = df["code"].astype(str).str.strip()

        # 4. Object型の安定化 (None を保持しつつ文字列化)
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: str(x) if (x is not None and not pd.isna(x)) else None)

        return df

    def _normalize_company_name(self, name: str) -> str:
        """比較判定のために法人格や空白を除去して正規化する (NFKC対応版)"""
        if not name or not isinstance(name, str):
            return ""

        # 1. NFKC正規化 (全角数字・英字を半角に、㈱ などを (株) に分解)
        n = unicodedata.normalize("NFKC", name)

        # 2. 全ての空白除去
        n = n.replace(" ", "").replace("\u3000", "")

        # 3. 代表的な法人格表記を除去
        # NFKC後の (株) や (有) などに対応できるようパターンを整理
        patterns = [
            r"株式会社",
            r"有限会社",
            r"合同会社",
            r"合資会社",
            r"合名会社",
            r"一般社団法人",
            r"一般財団法人",
            r"公益社団法人",
            r"公益財団法人",
            r"\(株\)",
            r"\(有\)",
            r"\(合\)",
            r"\(社\)",
            r"\(財\)",
        ]
        for p in patterns:
            n = re.sub(p, "", n)

        return n.strip()

    def add_commit_operation(self, repo_path: str, local_path: Path):
        """コミットバッファに操作を追加（重複は最新で上書き）"""
        self._commit_operations[repo_path] = CommitOperationAdd(path_in_repo=repo_path, path_or_fileobj=str(local_path))
        logger.debug(f"コミットバッファに追加: {repo_path}")

    def take_snapshot(self):
        """現在のGlobal状態のスナップショットをメモリに取得 (不整合発生時のロールバック用)"""
        # 主要ファイルをロードしてスナップショットに保存
        self._snapshots = {
            "catalog": self.catalog_df.copy(),
            "master": self.master_df.copy(),
            "listing": self._load_parquet("listing").copy(),
            "index": self._load_parquet("index").copy(),
            "name": self._load_parquet("name").copy(),
        }
        logger.info("Global 状態のスナップショットを取得しました (安全性確保)")

    def rollback(self, message: str = "RaW-V Failure: Automated Recovery Rollback"):
        """スナップショットの状態を強制的に書き戻し、Globalデータの整合性を復旧する"""
        if not self._snapshots:
            logger.error("❌ スナップショットが存在しないため、ロールバックできません。")
            return False

        logger.warning(f"⛔ ロールバックを開始します: {message}")

        # 既存のコミット予約をすべて破棄
        self._commit_operations = {}

        # スナップショットの内容を強制的に上書き予約
        for key, df in self._snapshots.items():
            self._save_and_upload(key, df, defer=True)

        # 一括コミットの実行 (事実上の差し戻し)
        success = self.push_commit(f"ROLLBACK: {message}")
        if success:
            logger.success("✅ ロールバック・コミットが完了しました。整合性は復旧されました。")
            # メモリ上の最新状態もスナップショットに戻す
            self.catalog_df = self._snapshots["catalog"]
            self.master_df = self._snapshots["master"]
        else:
            logger.critical(
                "❌ ロールバック自体に失敗しました！"
                "Hugging Face上のデータが壊れている可能性があります。直ちに手動確認が必要です。"
            )
        return success

    def _load_parquet(self, key: str, force_download: bool = False) -> pd.DataFrame:
        filename = self.paths[key]
        try:
            local_path = hf_hub_download(
                repo_id=self.hf_repo,
                filename=filename,
                repo_type="dataset",
                token=self.hf_token,
                force_download=force_download,
            )
            df = pd.read_parquet(local_path)
            # 【絶対ガード】読み込み直後にクレンジング
            df = self._clean_dataframe(key, df)
            logger.debug(f"ロード成功: {filename} ({len(df)} rows)")
            return df
        except RepositoryNotFoundError:
            logger.error(f"リポジトリが見つかりません: {self.hf_repo}")
            logger.error("環境変数 HF_REPO の設定を確認してください")
            raise
        except (EntryNotFoundError, requests.exceptions.HTTPError) as e:
            # EntryNotFoundError (HFライブラリ) または 生の 404 (パッチ適用時) をハンドリング
            is_404 = isinstance(e, EntryNotFoundError) or (
                hasattr(e, "response") and e.response is not None and e.response.status_code == 404
            )

            if not is_404:
                # 404 以外なら上位または Exception へ飛ばす
                raise e

            logger.info(f"ファイルが存在しないため新規作成します: {filename}")
            if key == "catalog":
                cols = list(CatalogRecord.model_fields.keys())
                return pd.DataFrame(columns=cols)
            elif key == "master":
                cols = list(StockMasterRecord.model_fields.keys())
                return pd.DataFrame(columns=cols)
            elif key == "listing":
                return pd.DataFrame(columns=["code", "type", "event_date"])
            elif key == "index":
                return pd.DataFrame(columns=["index_name", "code", "type", "event_date"])
            elif key == "name":
                return pd.DataFrame(columns=["code", "old_name", "new_name", "change_date"])
            return pd.DataFrame()
        except HfHubHTTPError as e:
            logger.error(f"HF API エラー ({e.response.status_code}): {filename}")
            logger.error(f"詳細: {e}")
            if e.response.status_code == 401:
                logger.error("認証エラー: HF_TOKEN が無効または期限切れの可能性があります")
            elif e.response.status_code == 403:
                logger.error("アクセス拒否: リポジトリへのアクセス権限がありません")
            raise
        except Exception as e:
            logger.error(f"予期しないエラー: {filename} - {type(e).__name__}: {e}")
            raise

    def is_processed(self, doc_id: str) -> bool:
        if self.catalog_df.empty:
            return False
        return doc_id in self.catalog_df["doc_id"].values

    def update_catalog(self, new_records: List[Dict]) -> bool:
        """カタログを更新 (Pydanticバリデーション実施)"""
        if not new_records:
            return True

        validated = []
        for rec in new_records:
            try:
                validated.append(CatalogRecord(**rec).model_dump())
            except Exception as e:
                logger.error(f"カタログレコードのバリデーション失敗 (doc_id: {rec.get('doc_id')}): {e}")

        if not validated:
            return False

        new_df = pd.DataFrame(validated)

        # 【修正】一時的に結合したDataFrameを作成（メモリ上の状態は変更しない）
        temp_catalog = pd.concat([self.catalog_df, new_df], ignore_index=True).drop_duplicates(
            subset=["doc_id"], keep="last"
        )

        # 【修正】アップロード成功時のみ、メモリ上のカタログを更新
        if self._save_and_upload("catalog", temp_catalog):
            self.catalog_df = temp_catalog
            logger.success(f"✅ カタログ更新成功: {len(validated)} 件")
            return True
        else:
            logger.error("カタログのアップロードに失敗したため、メモリ上の状態を保持します")
            return False

    def _save_and_upload(self, key: str, df: pd.DataFrame, defer: bool = False) -> bool:
        filename = self.paths[key]
        local_file = self.data_path / Path(filename).name

        # 【絶対ガード】保存直前に最終クレンジング
        df = self._clean_dataframe(key, df)

        df.to_parquet(local_file, index=False, compression="zstd")

        if self.api:
            if defer:
                # バッファに追加して終了 (パスをキーにして最新のもので上書き)
                self.add_commit_operation(filename, local_file)
                return True

            max_retries = 5  # 強化
            for attempt in range(max_retries):
                try:
                    self.api.upload_file(
                        path_or_fileobj=str(local_file),
                        path_in_repo=filename,
                        repo_id=self.hf_repo,
                        repo_type="dataset",
                        token=self.hf_token,
                    )
                    logger.success(f"アップロード成功: {filename}")
                    return True
                except Exception as e:
                    # HfHubHTTPErrorの型チェックを行い、429の場合のみリトライ
                    if isinstance(e, HfHubHTTPError) and e.response.status_code == 429:
                        wait_time = int(e.response.headers.get("Retry-After", 60)) + 5
                        logger.warning(f"Rate limit exceeded. Waiting {wait_time}s before retry ({attempt + 1}/5)...")
                        time.sleep(wait_time)
                        continue

                    # その他のHTTPエラー (5xx等) もリトライ対象にする
                    if isinstance(e, HfHubHTTPError) and e.response.status_code >= 500:
                        wait_time = 15 * (attempt + 1)
                        logger.warning(
                            f"Master HF Server Error ({e.response.status_code}). "
                            f"Waiting {wait_time}s... ({attempt + 1}/5)"
                        )
                        time.sleep(wait_time)
                        continue

                    logger.warning(f"アップロード一時エラー: {filename} - {e} - Retrying ({attempt + 1}/5)...")
                    time.sleep(10 * (attempt + 1))
            logger.error(f"❌ アップロードに最終的に失敗しました: {filename}")
            return False
        return True

    def upload_raw(self, local_path: Path, repo_path: str, defer: bool = False) -> bool:
        """ローカルの生データを Hugging Face の raw/ フォルダにアップロード"""
        if not local_path.exists():
            logger.error(f"ファイルが存在しないためアップロードできません: {local_path}")
            return False

        if self.api:
            if defer:
                self.add_commit_operation(repo_path, local_path)
                logger.debug(f"RAWコミットバッファに追加: {repo_path}")
                return True

            max_retries = 5  # 強化
            for attempt in range(max_retries):
                try:
                    self.api.upload_file(
                        path_or_fileobj=str(local_path),
                        path_in_repo=repo_path,
                        repo_id=self.hf_repo,
                        repo_type="dataset",
                        token=self.hf_token,
                    )
                    logger.debug(f"RAWアップロード成功: {repo_path}")
                    return True
                except Exception as e:
                    if isinstance(e, HfHubHTTPError) and e.response.status_code == 429:
                        wait_time = int(e.response.headers.get("Retry-After", 60)) + 5
                        logger.warning(f"Rate limit exceeded for RAW. Waiting {wait_time}s... ({attempt + 1}/5)")
                        time.sleep(wait_time)
                        continue

                    logger.warning(f"RAWアップロード一時エラー: {repo_path} - {e} - Retrying ({attempt + 1}/5)...")
                    time.sleep(10 * (attempt + 1))
            return False
        return True

    def upload_raw_folder(self, folder_path: Path, path_in_repo: str, defer: bool = False) -> bool:
        """フォルダ単位での一括アップロード (リトライ付)"""
        if not folder_path.exists():
            return True  # アップロード対象なしは成功とみなす

        if self.api:
            if defer:
                # フォルダ内の各ファイルを個別にバッファに追加
                for f in folder_path.glob("**/*"):
                    if f.is_file():
                        r_path = f"{path_in_repo}/{f.relative_to(folder_path)}"
                        self._commit_operations[r_path] = CommitOperationAdd(
                            path_in_repo=r_path, path_or_fileobj=str(f)
                        )
                logger.debug(f"RAWフォルダをコミットバッファに追加: {path_in_repo}")
                return True

            max_retries = 5  # 3回から5回に強化
            for attempt in range(max_retries):
                try:
                    self.api.upload_folder(
                        folder_path=str(folder_path),
                        path_in_repo=path_in_repo,
                        repo_id=self.hf_repo,
                        repo_type="dataset",
                        token=self.hf_token,
                    )
                    logger.success(f"一括アップロード成功: {path_in_repo} (from {folder_path})")
                    return True
                except Exception as e:
                    if isinstance(e, HfHubHTTPError) and e.response.status_code == 429:
                        wait_time = int(e.response.headers.get("Retry-After", 60)) + 5
                        logger.warning(
                            f"Folder Upload Rate limit exceeded. Waiting {wait_time}s... ({attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue

                    logger.warning(f"アップロード一時エラー: {e} - Retrying ({attempt + 1}/{max_retries})...")
                    time.sleep(10)

            logger.error(f"一括アップロード失敗 (Give up): {path_in_repo}")
            return False
        return True

    def update_listing_history(self, new_events: pd.DataFrame) -> bool:
        history = self._load_parquet("listing")

        # 初回実行時（ファイルが存在せず、イベントも空）の場合でも空ファイルを保存
        if new_events.empty:
            if history.empty:
                # 空の履歴ファイルを初期化して保存
                return self._save_and_upload("listing", history)
            return True

        history = pd.concat([history, new_events], ignore_index=True).drop_duplicates()
        return self._save_and_upload("listing", history)

    def update_index_history(self, new_events: pd.DataFrame) -> bool:
        history = self._load_parquet("index")

        # 初回実行時（ファイルが存在せず、イベントも空）の場合でも空ファイルを保存
        if new_events.empty:
            if history.empty:
                # 空の履歴ファイルを初期化して保存
                return self._save_and_upload("index", history)
            return True

        history = pd.concat([history, new_events], ignore_index=True).drop_duplicates()
        return self._save_and_upload("index", history)

    def get_listing_history(self) -> pd.DataFrame:
        """現在の上場履歴マスタを取得"""
        return self._load_parquet("listing")

    def get_index_history(self) -> pd.DataFrame:
        """現在の指数採用履歴マスタを取得"""
        return self._load_parquet("index")

    def update_stocks_master(self, incoming_data: pd.DataFrame):
        """マスタ更新 & 時系列リコンシリエーション (世界最高水準の歴史再構築ロジック)"""
        if incoming_data.empty:
            return True

        # 1. バリデーションと型正規化
        records = incoming_data.to_dict("records")
        validated = []
        for rec in records:
            try:
                rec = {k: (v if not pd.isna(v) else None) for k, v in rec.items()}
                # is_active の型正規化
                if isinstance(rec.get("is_active"), str):
                    rec["is_active"] = rec["is_active"].lower() in ["true", "1", "yes"]
                # 日付の正規化 (10文字制限)
                if rec.get("last_submitted_at"):
                    rec["last_submitted_at"] = str(rec["last_submitted_at"])[:10]
                validated.append(StockMasterRecord(**rec).model_dump())
            except Exception as e:
                logger.error(f"銘柄情報のバリデーション失敗 (code: {rec.get('code')}): {e}")

        if not validated:
            return True
        incoming_df = pd.DataFrame(validated)

        # 2. 既存データとの統合 (リコンシリエーション)
        # 既存マスタを「過去の状態の一つ」として扱い、全てのタイムラインをマージする
        current_m = self.master_df.copy()
        # カラム自体の存在をケア (NULL は NULL のまま維持)
        if "last_submitted_at" not in current_m.columns:
            current_m["last_submitted_at"] = None

        # 全ての既知の状態を統合
        all_states = pd.concat([current_m, incoming_df], ignore_index=True)

        # 重複排除 (同じ code, company_name, last_submitted_at は不要)
        all_states.drop_duplicates(subset=["code", "company_name", "last_submitted_at"], inplace=True)

        # 3. 社名変更の歴史的変遷を解析
        name_history = self._load_parquet("name")
        new_history_events = []

        for code, group in all_states.groupby("code"):
            # 提出日時の昇順でソート
            sorted_group = group.sort_values("last_submitted_at", ascending=True)

            # --- A. 基点(Baseline)の決定 ---
            # 1. まずは既に確定した履歴(name_history)から最新の名前を探す (最も信頼できる過去)
            prev_name = None
            if not name_history.empty:
                code_history = name_history[name_history["code"] == code]
                if not code_history.empty:
                    prev_name = code_history.iloc[-1]["new_name"]

            # 2. 履歴がない場合、既存マスタ(stocks_master)の名前を暫定基点とする
            # ただし、マスタ名は JPX 由来(略称)の可能性がある。
            is_baseline_from_jpx = False
            if prev_name is None:
                master_entry = current_m[current_m["code"] == code]
                if not master_entry.empty:
                    prev_name = master_entry.iloc[0]["company_name"]
                    # 提出日が NULL なら JPX 由来の暫定名と判断
                    last_at = master_entry.iloc[0].get("last_submitted_at")
                    if pd.isna(last_at):
                        is_baseline_from_jpx = True

            # --- B. 逐次比較と履歴生成 ---
            for _, curr_state in sorted_group.iterrows():
                # 既存データ(JPX等の日付なしマスタ状態)は比較の「対象」ではなく「基点」なのでスキップ
                last_at = curr_state.get("last_submitted_at")
                if pd.isna(last_at):
                    continue

                curr_name = curr_state["company_name"]

                # 基点がない場合 (ARIAで完全新規に発見された銘柄)
                if prev_name is None:
                    prev_name = curr_name
                    continue

                # 正規化比較を行い、実質的な差異がある場合のみ履歴を作成
                normalized_prev = self._normalize_company_name(prev_name)
                normalized_curr = self._normalize_company_name(curr_name)

                if normalized_prev != normalized_curr:
                    # 本物の社名変更として記録
                    event = {
                        "code": code,
                        "old_name": prev_name,
                        "new_name": curr_name,
                        "change_date": last_at[:10],
                    }

                    # 重複チェック
                    exists = False
                    if not name_history.empty:
                        exists = not name_history[
                            (name_history["code"] == code)
                            & (name_history["old_name"] == event["old_name"])
                            & (name_history["new_name"] == event["new_name"])
                        ].empty

                    if not exists:
                        new_history_events.append(event)
                        logger.info(f"✨ 社名変更を検知: {code} | {prev_name} -> {curr_name}")

                    # 基点を更新
                    prev_name = curr_name
                    is_baseline_from_jpx = False  # EDINET由来になったのでフラグを落とす
                else:
                    # 「形式的な差異(略称→正式名称)」または「同一名称」の場合
                    # 履歴には残さないが、以降の比較のために基点だけは更新(正式名へ昇格)
                    if prev_name != curr_name:
                        if is_baseline_from_jpx:
                            logger.debug(
                                f"ℹ️ 基点を略称から正式名称へ昇格 (履歴には残しません): "
                                f"{code} | {prev_name} -> {curr_name}"
                            )
                        prev_name = curr_name
                        is_baseline_from_jpx = False
        # 4. 履歴の保存 (Atomic & Non-destructive)
        if new_history_events:
            new_hist_df = pd.DataFrame(new_history_events)
            name_history = pd.concat([name_history, new_hist_df], ignore_index=True).drop_duplicates()
            # defer=True を指定してコミットバッファに積む
            self._save_and_upload("name", name_history, defer=True)
            logger.info(f"時系列リコンシリエーションにより {len(new_history_events)} 件の変遷を特定しました。")
        elif name_history.empty:
            # 初回実行時などで履歴が空の場合でも、ファイルを作成して整合性を保つ
            self._save_and_upload("name", name_history, defer=True)

        # 全状態の中から、code ごとに提出日時が最新のものを抽出
        sorted_all = all_states.sort_values("last_submitted_at", ascending=False)

        # セクターと市場情報の「属性継承（Inheritance）」
        # 最新レコードが NULL や "その他" の場合、過去の有効なレコード（JPX等）から引き継ぐ
        def resolve_attr(group, col):
            # 提出日に関わらず、そのコードにおける NULL 以外の最も確かな値を探す
            # (JPXは1970年だがセクター情報は「正」であるため、全体から検索して良い)
            valid = group[col][~group[col].isin(["その他", None, "nan", ""])]
            return valid.iloc[0] if not valid.empty else None

        # 各コードの最新状態を特定しつつ、属性を補完
        best_records = []
        for _, group in sorted_all.groupby("code", sort=False):
            # 1. 物理的な最新レコードを取得 (社名と提出日時の決定用)
            latest_rec = group.iloc[0].copy()

            # 2. JPXレコード(日付なし)を特定 (属性の正解データ)
            jpx_entries = group[group["last_submitted_at"].isna()]

            if not jpx_entries.empty:
                # JPXが存在する場合、主要属性をJPXから強制取得（EDINET属性を拒絶）
                jpx_rec = jpx_entries.iloc[0]
                latest_rec["sector"] = jpx_rec["sector"]
                latest_rec["market"] = jpx_rec["market"]
                latest_rec["is_active"] = jpx_rec["is_active"]
                # 万が一 JPX のセクターが不全な場合は、過去の有効な属性から拾う（ただし優先度はJPX）
                if latest_rec["sector"] in ["その他", None, "nan", ""]:
                    latest_rec["sector"] = resolve_attr(group, "sector")
            else:
                # JPXに一度も登録されたことがない(完全新規上場等)の場合
                # JPXによる承認(同期)があるまでは、Unknown (None) 状態で隔離する
                latest_rec["is_active"] = None
                latest_rec["sector"] = None
                latest_rec["market"] = None

            best_records.append(latest_rec)

        self.master_df = pd.DataFrame(best_records)

        # defer=True を指定してコミットバッファに積む
        return self._save_and_upload("master", self.master_df, defer=True)

    def get_last_index_list(self, index_name: str) -> pd.DataFrame:
        """指定指数の構成銘柄を取得 (Phase 3用)"""
        return pd.DataFrame(columns=["code"])

    def get_sector(self, code: str) -> str:
        """証券コードから業種取得"""
        if self.master_df.empty:
            return None
        row = self.master_df[self.master_df["code"] == code]
        if not row.empty:
            val = row.iloc[0]["sector"]
            return str(val) if val is not None else None
        return None

    def save_delta(
        self, key: str, df: pd.DataFrame, run_id: str, chunk_id: str, custom_filename: str = None, defer: bool = False
    ) -> bool:
        """デルタファイルを保存してアップロード (Worker用)"""
        if df.empty:
            return True

        if custom_filename:
            filename = custom_filename
        else:
            filename = f"{Path(self.paths[key]).stem}.parquet"

        delta_path = f"temp/deltas/{run_id}/{chunk_id}/{filename}"
        local_file = self.data_path / f"delta_{run_id}_{chunk_id}_{filename}"

        # 【絶対ガード】保存直前に最終クレンジング
        df = self._clean_dataframe(key, df)

        df.to_parquet(local_file, index=False, compression="zstd")

        return self.upload_raw(local_file, delta_path, defer=defer)

    def mark_chunk_success(self, run_id: str, chunk_id: str, defer: bool = False) -> bool:
        """チャンク処理成功フラグ (_SUCCESS) を作成 (Worker用)"""
        success_path = f"temp/deltas/{run_id}/{chunk_id}/_SUCCESS"
        local_file = self.data_path / f"SUCCESS_{run_id}_{chunk_id}"
        local_file.touch()

        return self.upload_raw(local_file, success_path, defer=defer)

    def load_deltas(self, run_id: str) -> Dict[str, pd.DataFrame]:
        """全デルタを収集してマージ (Merger用)"""
        if not self.api:
            logger.warning("API初期化されていないためデルタ収集不可")
            return {}

        deltas = {}

        try:
            # 【整合性強化】HF Hub のリスト取得自体をリトライし、反映遅延に対処
            folder = f"temp/deltas/{run_id}"
            files = []
            for attempt in range(3):
                files = self.api.list_repo_files(repo_id=self.hf_repo, repo_type="dataset")
                target_files = [f for f in files if f.startswith(folder)]
                if target_files:
                    break
                logger.warning(f"デルタフォルダが見つかりません。再試行中... ({attempt + 1}/3)")
                time.sleep(10)

            # チャンクごとにグループ化
            chunks = {}
            for f in target_files:
                parts = f.split("/")
                if len(parts) < 4:
                    continue
                chunk_id = parts[3]
                if chunk_id not in chunks:
                    chunks[chunk_id] = []
                chunks[chunk_id].append(f)

            # _SUCCESS があるチャンクのみ処理
            valid_chunks = 0
            for chunk_id, file_list in chunks.items():
                if not any(f.endswith("_SUCCESS") for f in file_list):
                    # 【整合性強化】HF Hubの結果整合性を考慮し、1回見つからなくても
                    # 別のファイルリスト取得を試みることが望ましいが、ここでは一旦警告に留める
                    logger.warning(f"⚠️ 未完了のチャンクをスキップ: {chunk_id}")
                    continue

                valid_chunks += 1
                for remote_path in file_list:
                    if remote_path.endswith("_SUCCESS"):
                        continue

                    # キー判別
                    fname = Path(remote_path).name
                    key = None
                    if fname == "documents_index.parquet":
                        key = "catalog"
                    elif fname == "stocks_master.parquet":
                        key = "master"
                    elif fname == "listing_history.parquet":
                        key = "listing"
                    elif fname == "index_history.parquet":
                        key = "index"
                    elif fname == "name_history.parquet":
                        key = "name"
                    elif fname.startswith("financial_values_bin"):
                        bin_id = fname.replace("financial_values_bin", "").replace(".parquet", "")
                        key = f"financial_bin{bin_id}"
                    elif fname.startswith("qualitative_text_bin"):
                        bin_id = fname.replace("qualitative_text_bin", "").replace(".parquet", "")
                        key = f"text_bin{bin_id}"
                    elif fname.startswith("financial_values_"):
                        sector = fname.replace("financial_values_", "").replace(".parquet", "")
                        key = f"financial_{sector}"
                    elif fname.startswith("qualitative_text_"):
                        sector = fname.replace("qualitative_text_", "").replace(".parquet", "")
                        key = f"text_{sector}"

                    if key:
                        attempts = 2
                        for att in range(attempts):
                            try:
                                local_path = hf_hub_download(
                                    repo_id=self.hf_repo, filename=remote_path, repo_type="dataset", token=self.hf_token
                                )
                                df = pd.read_parquet(local_path)
                                if key not in deltas:
                                    deltas[key] = []
                                deltas[key].append(df)
                                break
                            except Exception as e:
                                if att == attempts - 1:
                                    logger.error(f"❌ デルタ読み込み失敗 ({remote_path}): {e}")
                                    raise
                                logger.warning(f"デルタ読み込み再試行中... ({att + 1}) {remote_path}")
                                time.sleep(5)

            logger.info(f"有効なチャンク数: {valid_chunks} / {len(chunks)}")

            # マージ結果を返す
            merged = {}
            for key, df_list in deltas.items():
                if df_list:
                    # 全てのDFのカラムを共通化（型不整合対策）
                    merged[key] = pd.concat(df_list, ignore_index=True)
                else:
                    merged[key] = pd.DataFrame()
            return merged

        except Exception as e:
            logger.error(f"デルタ収集失敗: {e}")
            return {}

    def push_commit(self, message: str = "Batch update from ARIA") -> bool:
        """
        バッファに溜まった操作をコミット実行。
        【究極の安定化】操作数が多い場合は、HF側の負荷と429エラーを避けるため、自動的に分割してコミットする。
        """
        if not self.api or not self._commit_operations:
            return True

        ops_list = list(self._commit_operations.values())
        total_ops = len(ops_list)

        # 1コミットあたりの最大操作数
        # レート制限 (128回/時) を回避するため、バッチサイズを拡大してコミット回数を削減する
        # HF側でタイムアウトしないギリギリのラインとして 500件程度が最適
        batch_size = 500
        batches = [ops_list[i : i + batch_size] for i in range(0, total_ops, batch_size)]

        logger.info(f"🚀 コミット送信開始: 合計 {total_ops} 操作を {len(batches)} バッチに分割して実行します")

        for i, batch in enumerate(batches):
            batch_msg = f"{message} (part {i + 1}/{len(batches)})"
            max_retries = 12
            success = False

            for attempt in range(max_retries):
                try:
                    self.api.create_commit(
                        repo_id=self.hf_repo,
                        repo_type="dataset",
                        operations=batch,
                        commit_message=batch_msg,
                        token=self.hf_token,
                    )
                    success = True
                    break
                except Exception as e:
                    status_code = getattr(getattr(e, "response", None), "status_code", None)

                    # 429 レート制限 または 500 サーバーエラー
                    if status_code in [429, 500]:
                        # 429の場合はより長く待機 (HFの回復を待つ)
                        wait_time = int(getattr(e.response.headers, "get", lambda x, y: y)("Retry-After", 60))
                        wait_time = max(wait_time, 60) + (attempt * 30) + random.uniform(5, 15)
                        logger.warning(
                            f"HF Server Error ({status_code}). Waiting {wait_time:.1f}s... "
                            f"(Batch {i + 1}, Attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue

                    # 409 コンフリクト または 412 前提条件失敗
                    if status_code in [409, 412]:
                        wait_time = (2 ** (attempt + 2)) + (random.uniform(10, 30))
                        logger.warning(
                            f"Commit Conflict ({status_code}). Retrying in {wait_time:.2f}s... "
                            f"(Batch {i + 1}, Attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue

                    # タイムアウト等のネットワーク例外
                    logger.warning(
                        f"通信エラー: {e} - Retrying... (Batch {i + 1}, Attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(30 * (attempt + 1))

            if not success:
                logger.error(f"❌ バッチ {i + 1} の送信に最終的に失敗しました。")
                return False

            # バッチ間に短い休憩を挟んでHF側の負荷を逃がす
            if i < len(batches) - 1:
                time.sleep(random.uniform(3, 7))

        logger.success(f"✅ 全 {total_ops} 操作のバッチコミットが完了しました")
        self._commit_operations = {}  # クリア
        return True

    def cleanup_deltas(self, run_id: str, cleanup_old: bool = True):
        """一時ファイルのクリーンアップ (Merger用)"""
        if not self.api:
            return

        try:
            files = self.api.list_repo_files(repo_id=self.hf_repo, repo_type="dataset")
            delta_root = "temp/deltas"

            # 古いフォルダの削除 (24時間以上経過したものを対象とする)
            if cleanup_old:
                now = time.time()
                delete_ops = []
                expired_runs = set()

                for f in files:
                    if not f.startswith(delta_root):
                        continue
                    parts = f.split("/")
                    if len(parts) < 3:
                        continue
                    r_id = parts[2]

                    # run_id が数値（timestamp）である前提で古いものを判定
                    try:
                        timestamp = int(r_id)
                        if (now - timestamp) > 86400:  # 24時間以上
                            delete_ops.append(f)
                            expired_runs.add(r_id)
                    except ValueError:
                        # 数値でないフォルダは無視するか、別の基準で消す
                        pass

                if delete_ops:
                    logger.info(f"古い一時フォルダを清掃中... (24時間以上経過: {len(expired_runs)} runs)")
                    for i in range(0, len(delete_ops), 50):
                        batch = delete_ops[i : i + 50]
                        # 削除操作オブジェクトのリストを作成
                        del_ops = [CommitOperationDelete(path_in_repo=p) for p in batch]
                        self.api.create_commit(
                            repo_id=self.hf_repo,
                            repo_type="dataset",
                            operations=del_ops,
                            commit_message="Automatic garbage collection of old deltas",
                        )

            # 今回のフォルダ削除（全完了後用）
            else:
                target_prefix = f"{delta_root}/{run_id}"
                delete_ops = [f for f in files if f.startswith(target_prefix)]

                if delete_ops:
                    logger.info(f"今回の一時ファイルを削除中... {run_id} ({len(delete_ops)} files)")
                    for i in range(0, len(delete_ops), 50):
                        batch = delete_ops[i : i + 50]
                        del_ops = [CommitOperationDelete(path_in_repo=p) for p in batch]
                        self.api.create_commit(
                            repo_id=self.hf_repo,
                            repo_type="dataset",
                            operations=del_ops,
                            commit_message=f"Cleanup successfully merged deltas: {run_id}",
                        )
                    logger.success(f"Cleanup completed: {run_id}")

        except Exception as e:
            logger.error(f"クリーンアップ失敗: {e}")
