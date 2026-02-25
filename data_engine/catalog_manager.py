import io
import random
import re
import time
import unicodedata
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi, hf_hub_download
from huggingface_hub.utils import EntryNotFoundError, HfHubHTTPError, RepositoryNotFoundError
from loguru import logger
from models import CatalogRecord, EdinetCodeRecord, StockMasterRecord
from utils import normalize_code


class CatalogManager:
    def __init__(self, hf_repo: str, hf_token: str, data_path: Path):
        self.hf_repo = hf_repo
        self.hf_token = hf_token
        self.data_path = data_path
        self.data_path.mkdir(parents=True, exist_ok=True)

        # 【修正】通信安定性向上のため、タイムアウト環境変数を設定
        # huggingface_hub v0.20+ / 1.x は環境変数を参照してタイムアウトを制御します
        if hf_repo and hf_token:
            import os

            os.environ["HF_HUB_TIMEOUT"] = "300"
            os.environ["HF_HUB_HTTP_TIMEOUT"] = "300"
            self.api = HfApi(token=hf_token)
        else:
            self.api = None

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

        # 【追加】起動時にEDINETコードリストを同期 (和英協同 + 集約一覧)
        # ネットワークエラーで停止しないよう、内部で例外処理
        self.edinet_codes, self.aggregation_map = self.sync_edinet_code_lists()

        # 【追加】同期したコードリストをマスタに反映
        if self.edinet_codes:
            self._update_master_from_edinet_codes()

    def _update_master_from_edinet_codes(self):
        """同期した edinet_codes および aggregation_map を master_df に反映させ、属性を最新化する"""
        from datetime import datetime

        logger.info("EDINETコードリストをマスタに反映中 (集約ブリッジ + JCN変更検知 + 上場生死判定)...")
        updated_count = 0
        listing_events = []
        today = datetime.now().strftime("%Y-%m-%d")

        # 既存マスタを edinet_code をキーにした辞書に変換 (高速化用)
        master_dict = {
            str(row["edinet_code"]): row.to_dict()
            for _, row in self.master_df.iterrows()
            if pd.notna(row.get("edinet_code"))
        }

        # 【集約ブリッジ】旧コード→新コードの付け替えを適用
        for old_code, new_code in self.aggregation_map.items():
            if new_code in master_dict:
                existing_former = master_dict[new_code].get("former_edinet_codes") or ""
                former_set = set(existing_former.split(",")) if existing_former else set()
                former_set.discard("")
                former_set.add(old_code)
                master_dict[new_code]["former_edinet_codes"] = ",".join(sorted(former_set))
                logger.debug(f"集約ブリッジ適用: {old_code} → {new_code} (旧コードをリンク)")

        for e_code, ed_rec in self.edinet_codes.items():
            # 【最適化】上場判定: 金融庁リストの「上場区分」が完全に "上場" である場合のみ
            is_listed_official = str(ed_rec.is_listed or "").strip() == "上場"

            if e_code in master_dict:
                # 既存レコードの更新
                m_rec = master_dict[e_code]

                # 【JCN変更検知】
                old_jcn = m_rec.get("jcn")
                new_jcn = ed_rec.jcn
                if old_jcn and new_jcn and str(old_jcn) != str(new_jcn):
                    logger.warning(
                        f"⚠️ JCN変更検知: {e_code} ({ed_rec.submitter_name}) 旧JCN={old_jcn} → 新JCN={new_jcn}"
                    )

                # 【リスティングイベント生成 (生死判定)】
                old_is_active = bool(m_rec.get("is_active", False))
                sec_code = ed_rec.sec_code or m_rec.get("code")
                if sec_code:
                    if old_is_active is False and is_listed_official is True:
                        listing_events.append({"code": sec_code, "type": "LISTING", "event_date": today})
                        logger.info(f"🟢 新規上場/再上場検知: {sec_code} ({ed_rec.submitter_name})")
                    elif old_is_active is True and is_listed_official is False:
                        listing_events.append({"code": sec_code, "type": "DELISTING", "event_date": today})
                        logger.info(f"🔴 上場廃止検知: {sec_code} ({ed_rec.submitter_name})")

                # 変更がある場合のみ更新 (誠実な同期)
                updates = {
                    "jcn": ed_rec.jcn or m_rec.get("jcn"),
                    "code": sec_code,
                    "company_name": ed_rec.submitter_name,
                    "company_name_en": ed_rec.submitter_name_en or m_rec.get("company_name_en"),
                    "industry_edinet": ed_rec.industry_edinet,
                    "industry_edinet_en": ed_rec.industry_edinet_en or m_rec.get("industry_edinet_en"),
                    "is_listed_edinet": is_listed_official,
                    "is_active": is_listed_official,  # EDINETの完全移譲
                }

                changed = False
                for k, v in updates.items():
                    if m_rec.get(k) != v:
                        m_rec[k] = v
                        changed = True

                if changed:
                    master_dict[e_code] = m_rec
                    updated_count += 1
            else:
                # 新規レコードの追加
                sec_code = ed_rec.sec_code
                if sec_code and is_listed_official:
                    listing_events.append({"code": sec_code, "type": "LISTING", "event_date": today})

                new_master_rec = StockMasterRecord(
                    edinet_code=e_code,
                    code=sec_code,
                    jcn=ed_rec.jcn,
                    company_name=ed_rec.submitter_name,
                    company_name_en=ed_rec.submitter_name_en,
                    industry_edinet=ed_rec.industry_edinet,
                    industry_edinet_en=ed_rec.industry_edinet_en,
                    is_listed_edinet=is_listed_official,
                    is_active=is_listed_official,  # EDINETの完全移譲
                )
                master_dict[e_code] = new_master_rec.model_dump()
                updated_count += 1

        if updated_count > 0:
            self.master_df = pd.DataFrame(list(master_dict.values()))
            self.master_df = self._clean_dataframe("master", self.master_df)
            logger.success(f"マスタ同期完了: {updated_count} 件のレコードを更新/追加しました。")
            self._save_and_upload("master", self.master_df, defer=True)

        if listing_events:
            events_df = pd.DataFrame(listing_events)
            self.update_listing_history(events_df)
            logger.success(f"上場履歴同期完了: {len(events_df)} 件のイベントを追加予約しました。")

    def sync_edinet_code_lists(self) -> Tuple[Dict[str, EdinetCodeRecord], Dict[str, str]]:
        """金融庁から和英両方のコードリストおよび集約一覧を取得し、協同してマスタベースを構築する"""
        urls = {
            "jp": "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip",
            "en": "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelisteng/Edinetcode.zip",
            "agg": "https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/download/ESE140190.csv",
        }

        results = {}
        agg_map = {}  # 旧コード -> 新コード
        try:
            logger.info("EDINETコードリスト (和英) の同期を開始...")

            # 日本語版の取得と解析
            res_jp = requests.get(urls["jp"], timeout=30)
            res_jp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(res_jp.content)) as z:
                csv_file = [f for f in z.namelist() if f.endswith(".csv")][0]
                df_jp = pd.read_csv(z.open(csv_file), encoding="cp932", skiprows=1)

            # 英語版の取得と解析 (業種翻訳の抽出用)
            res_en = requests.get(urls["en"], timeout=30)
            res_en.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(res_en.content)) as z:
                csv_file = [f for f in z.namelist() if f.endswith(".csv")][0]
                df_en = pd.read_csv(z.open(csv_file), encoding="cp932", skiprows=1)

            # 【重要】集約一覧 (ESE140190.csv) の取得と解析 (コード付け替え対応)
            # 物理検証結果: エンコーディングは CP932、1行目はタイトル行
            try:
                res_agg = requests.get(urls["agg"], timeout=30)
                res_agg.raise_for_status()
                # CSV 読み込み: [集約処理日, 廃止EDINETコード, 継続EDINETコード] 形式を想定
                # 1行目が「EDINETコード集約一覧,,」のため skiprows=1
                df_agg = pd.read_csv(io.BytesIO(res_agg.content), encoding="cp932", skiprows=1)
                for _, agg_row in df_agg.iterrows():
                    # 0:処理日, 1:廃止コード, 2:継続コード
                    old_c = str(agg_row.iloc[1]).strip()
                    new_c = str(agg_row.iloc[2]).strip()
                    if old_c and new_c and old_c != new_c:
                        agg_map[old_c] = new_c
                logger.info(f"EDINETコード集約一覧をロード: {len(agg_map)} 件の付け替えを特定")
            except Exception as ae:
                logger.warning(f"集約一覧の取得・解析に失敗しました (継続可能): {ae}")

            # 名寄せ: EDINETコードをキーにする
            # 日本語版をベースにし、英語版から業種名を補完
            for _, row in df_jp.iterrows():
                e_code = str(row["ＥＤＩＮＥＴコード"])

                # 英語版から対応するレコードを検索
                en_row = df_en[df_en.iloc[:, 0] == e_code]
                ind_en = en_row.iloc[0]["Submitter's industry"] if not en_row.empty else None

                # 数値型の可能性があるカラムを安全に文字列化 (2024.0 回避)
                def safe_int_str(val):
                    if pd.isna(val):
                        return None
                    try:
                        return str(int(float(val)))
                    except Exception:
                        return str(val)

                res_dict = {
                    "edinet_code": e_code,
                    "submitter_type": row.get("提出者種別"),
                    "is_listed": row.get("上場区分"),
                    "is_consolidated": row.get("連結の有無"),
                    "capital": float(row["資本金"]) if pd.notna(row.get("資本金")) else None,
                    "settlement_date": str(row.get("決算日")),
                    "submitter_name": str(row.get("提出者名")),
                    "submitter_name_en": str(row.get("提出者名（英字）")),
                    "submitter_name_kana": str(row.get("提出者名（ヨミ）")),
                    "address": str(row.get("所在地")),
                    "industry_edinet": str(row.get("提出者業種")),
                    "industry_edinet_en": ind_en,
                    "sec_code": normalize_code(str(row["証券コード"]))
                    if pd.notna(row.get("証券コード")) and str(row["証券コード"]).strip()
                    else None,
                    "jcn": safe_int_str(row.get("提出者法人番号")),
                }
                results[e_code] = EdinetCodeRecord(**res_dict)

            logger.success(f"EDINETコードリスト同期完了: {len(results)} 件")

        except Exception as e:
            logger.error(f"EDINETコードリストの同期に失敗しました: {e}")
            # 失敗した場合は既存の master_df から最小限の情報を復元することを検討

        return results, agg_map

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

        # 2. カタログの場合、モデル定義のカラム構成を強制 (現在は27カラムに拡張)
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

        # 3. 証券コードの正規化 (5桁統一)
        targets = ["master", "listing", "index", "name"]
        if key in targets and "code" in df.columns:
            df["code"] = df["code"].apply(normalize_code)

        # 4. Object型の安定化 (None を保持しつつ文字列化)
        for col in df.columns:
            if df[col].dtype == "object":
                # 【最重要】論理値が含まれる場合は文字列化を回避
                # 既に 'True' / 'False'（文字列）になってしまっている場合の復旧処置も兼ねる
                has_string_bools = df[col].isin(["True", "False"]).any()
                if has_string_bools:
                    # 文字列の 'True'/'False' を正規の Boolean に戻す (None は維持)
                    df[col] = df[col].map({"True": True, "False": False, True: True, False: False}, na_action="ignore")

                # 改めてチェックし、純粋な文字列カラムのみを as_type(str) 相当の処理にかける
                is_pure_bool = df[col].isin([True, False]).any()
                if not is_pure_bool:
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
        # doc_id が存在し、かつステータスが 'success' または 'retracted' (取下済) の場合のみ「処理済み」とみなす
        # これにより、pending や failure の書類は自動的に再処理の対象になる
        # retracted の書類は再送しても無意味なため、処理済みとして扱う
        processed = self.catalog_df[
            (self.catalog_df["doc_id"] == doc_id) & (self.catalog_df["processed_status"].isin(["success", "retracted"]))
        ]
        return not processed.empty

    def get_status(self, doc_id: str) -> Optional[str]:
        """指定した doc_id の現在のステータスを取得"""
        if self.catalog_df.empty:
            return None
        match = self.catalog_df[self.catalog_df["doc_id"] == doc_id]
        if match.empty:
            return None
        return match.iloc[0]["processed_status"]

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
                # 【最適解】情報の損失を伴う切り捨てを廃止し、ソースの精度を維持する
                # (Datetime型への変換は後続の保存レイヤーまたはPydanticモデルに委ねる)
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
        # 【重要】インデックスをリセットして結合
        all_states = pd.concat([current_m, incoming_df], ignore_index=True)

        # 重複排除 (属性の変化も「新しい証言」として受け入れる)
        # 以前は subset=["code", "company_name", "last_submitted_at"] のみだったため、
        # NULL属性の古いレコードが最新のJPX属性をブロックしていた。
        all_states.drop_duplicates(
            subset=["code", "company_name", "last_submitted_at", "is_active", "sector_jpx_33", "market"], inplace=True
        )

        # 3. 社名変更の歴史的変遷を解析
        name_history = self._load_parquet("name")
        new_history_events = []

        processed_codes = set()

        for code, group in all_states.groupby("code"):
            processed_codes.add(code)

            # 提出日時の昇順でソート (これがないと sorted_group が未定義になる)
            sorted_group = group.sort_values("last_submitted_at", ascending=True)

            # --- C. 歴史の完全再構築 (Full History Rebuild) ---
            # 既存の履歴、現在のマスタ、新規データを全て「イベント」として時系列に並べ直す

            timeline_events = []

            # 1. 既存マスタ & 新規データからのイベント抽出
            for _, row in sorted_group.iterrows():
                if pd.notna(row.get("last_submitted_at")):
                    timeline_events.append(
                        {"date": row["last_submitted_at"], "name": row["company_name"], "source": "master_or_incoming"}
                    )

            # 2. 既存履歴(name_history)からのイベント抽出
            # これまでの記録も「過去の証言」として採用する
            if not name_history.empty:
                code_hist = name_history[name_history["code"] == code].sort_values("change_date")
                if not code_hist.empty:
                    # 【重要: 自己修復シードの注入】
                    # 一番最初の社名変更イベントの「old_name」を歴史の夜明けとして植え付ける
                    first_hist = code_hist.iloc[0]
                    timeline_events.append(
                        {
                            "date": "0000-00-00",
                            "name": first_hist["old_name"],
                            "source": "history_seed",
                        }
                    )

                for _, h_row in code_hist.iterrows():
                    timeline_events.append(
                        {"date": h_row["change_date"], "name": h_row["new_name"], "source": "history"}
                    )

            # 3. 時系列ソート (古い順)
            # 日付型への変換とソート
            # (注意: 文字列比較でも YYYY-MM-DD 形式なら概ね機能するが、pd.to_datetime推奨)
            timeline_events.sort(key=lambda x: str(x["date"]))

            # 4. 歴史の再生 (Replay)
            current_tracking_name = None

            # 初期値の推論:
            # タイムラインの最初のイベントの「前」の状態は分からない。
            # しかし、最初のイベント名が「最初の名前」であることは確定できる。

            rebuilt_code_events = []

            for evt in timeline_events:
                evt_name = evt["name"]
                evt_date = evt["date"]

                if current_tracking_name is None:
                    current_tracking_name = evt_name
                    continue

                # 正規化して比較
                norm_curr = self._normalize_company_name(current_tracking_name)
                norm_evt = self._normalize_company_name(evt_name)

                if norm_curr != norm_evt:
                    # 変更検知
                    # 過去に記録されたイベントと全く同じもの(日時・新旧名)であれば、
                    # 重複排除されるが、ここでは意図的に「再生成」する。
                    rebuilt_code_events.append(
                        {"code": code, "old_name": current_tracking_name, "new_name": evt_name, "change_date": evt_date}
                    )
                    logger.info(f"🔄 Rebuild History: {code} | {current_tracking_name} -> {evt_name} ({evt_date})")
                    current_tracking_name = evt_name

            # 5. 結果の格納 (メモリ上の更新)
            # このコードに関する新しい履歴を確定リストに追加
            # (重複除外は後続の drop_duplicates で行われるが、
            #  古い誤った履歴(未来->過去)を消すために、後で name_history からこのコード分を除外する必要がある)
            new_history_events.extend(rebuilt_code_events)

        # 4. 履歴の保存 (Atomic & Non-destructive)
        # 【修正】History Evaporation（履歴の蒸発）バグを修正。
        # 以前は processed_codes に該当する全履歴を削除していたが、
        # これでは別期間の実行時に既存の履歴が消えてしまう。
        # 既存の履歴を保持したまま、新しい変遷のみをマージして重複排除する。

        if new_history_events:
            new_hist_df = pd.DataFrame(new_history_events)
            name_history = pd.concat([name_history, new_hist_df], ignore_index=True)

        if processed_codes:  # 変更があってもなくても、ファイル更新（削除の反映）は必要
            name_history = name_history.drop_duplicates()
            # defer=True を指定してコミットバッファに積む
            self._save_and_upload("name", name_history, defer=True)
            if new_history_events:
                logger.info(f"時系列リコンシリエーション: {len(new_history_events)} 件の変遷を特定 (Clean Rebuild)")
            else:
                logger.info("時系列リコンシリエーション: 変更なし (履歴はクリーニングされました)")

        # 全状態の中から、code ごとに提出日時が最新のものを抽出
        sorted_all = all_states.sort_values("last_submitted_at", ascending=False)

        # セクターと市場情報の「属性継承（Inheritance）」
        # 最新レコードが NULL や "その他" の場合、過去の有効なレコード（JPX等）から引き継ぐ
        def resolve_attr(group, col):
            # 提出日に関わらず、そのコードにおける NULL 以外の最も確かな値を探す
            # (JPXは1970年だがセクター情報は「正」であるため、全体から検索して良い)
            if col not in group.columns:
                return None
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
                latest_rec["sector_jpx_33"] = jpx_rec.get("sector_jpx_33")
                latest_rec["market"] = jpx_rec.get("market")
                # 万が一 JPX のセクターが不全な場合は、過去の有効な属性から拾う（ただし優先度はJPX）
                if latest_rec.get("sector_jpx_33") in ["その他", None, "nan", ""]:
                    latest_rec["sector_jpx_33"] = resolve_attr(group, "sector_jpx_33")
            else:
                # JPXに一度も登録されたことがない(完全新規上場等)の場合
                latest_rec["sector_jpx_33"] = None
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
            col_name = "sector_jpx_33" if "sector_jpx_33" in self.master_df.columns else "sector"
            val = row.iloc[0].get(col_name)
            return str(val) if val is not None else None
        return None

    def save_delta(
        self,
        key: str,
        df: pd.DataFrame,
        run_id: str,
        chunk_id: str,
        custom_filename: str = None,
        defer: bool = False,
        local_only: bool = False,
    ) -> bool:
        """
        デルタファイルを保存してアップロード。
        local_only=True の場合、HFにはアップロードせずローカルディレクトリに保存のみ行う (GHA Artifact用)。
        """
        if df.empty:
            return True

        if custom_filename:
            filename = custom_filename
        else:
            filename = f"{Path(self.paths[key]).stem}.parquet"

        # リポジトリ内パス
        delta_repo_path = f"temp/deltas/{run_id}/{chunk_id}/{filename}"

        # ローカル保存先 (Mergerが収集しやすいように構造化)
        local_delta_dir = self.data_path / "deltas" / str(run_id) / str(chunk_id)
        local_delta_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_delta_dir / filename

        # 【絶対ガード】保存直前に最終クレンジング
        df = self._clean_dataframe(key, df)

        df.to_parquet(local_file, index=False, compression="zstd")

        if local_only:
            logger.debug(f"Delta saved locally (local_only): {local_file}")
            return True

        return self.upload_raw(local_file, delta_repo_path, defer=defer)

    def mark_chunk_success(self, run_id: str, chunk_id: str, defer: bool = False, local_only: bool = False) -> bool:
        """チャンク処理成功フラグ (_SUCCESS) を作成"""
        success_repo_path = f"temp/deltas/{run_id}/{chunk_id}/_SUCCESS"

        local_delta_dir = self.data_path / "deltas" / str(run_id) / str(chunk_id)
        local_delta_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_delta_dir / "_SUCCESS"
        local_file.touch()

        if local_only:
            logger.debug(f"Chunk success marked locally: {local_file}")
            return True

        return self.upload_raw(local_file, success_repo_path, defer=defer)

    def load_deltas(self, run_id: str) -> Dict[str, pd.DataFrame]:
        """
        全デルタを収集してマージ (Merger用)
        ローカル (data/deltas/{run_id}) とリモート (HF) の両方をスキャンする。
        """
        deltas = {}
        processed_chunks = set()

        # --- A. ローカルスキャン (GHA Artifacts 等でダウンロード済みの場合) ---
        local_run_dir = self.data_path / "deltas" / str(run_id)
        if local_run_dir.exists():
            logger.info(f"Checking local deltas in {local_run_dir}")
            for chunk_dir in local_run_dir.iterdir():
                if not chunk_dir.is_dir():
                    continue

                chunk_id = chunk_dir.name
                if not (chunk_dir / "_SUCCESS").exists():
                    logger.warning(f"⚠️ 未完了のローカルチャンクをスキップ: {chunk_id}")
                    continue

                processed_chunks.add(chunk_id)
                for p_file in chunk_dir.glob("*.parquet"):
                    key = self._get_key_from_filename(p_file.name)
                    if key:
                        try:
                            df = pd.read_parquet(p_file)
                            deltas.setdefault(key, []).append(df)
                        except Exception as e:
                            logger.error(f"❌ ローカルデルタ読み込み失敗 ({p_file.name}): {e}")

        # --- B. リモートスキャン (Hugging Face Repository) ---
        if self.api:
            try:
                folder = f"temp/deltas/{run_id}"
                files = []
                # 反映遅延に対処
                for attempt in range(3):
                    files = self.api.list_repo_files(repo_id=self.hf_repo, repo_type="dataset")
                    target_files = [f for f in files if f.startswith(folder)]
                    if target_files:
                        break
                    if attempt < 2:
                        logger.warning(f"リモートデルタフォルダが見つかりません。再試行中... ({attempt + 1}/3)")
                        time.sleep(10)

                # チャンクごとにグループ化
                remote_chunks = {}
                for f in target_files:
                    parts = f.split("/")
                    if len(parts) < 4:
                        continue
                    chunk_id = parts[3]
                    # すでにローカルで処理済みのチャンクはスキップ (重複防止)
                    if chunk_id in processed_chunks:
                        continue
                    remote_chunks.setdefault(chunk_id, []).append(f)

                valid_remote_count = 0
                for chunk_id, file_list in remote_chunks.items():
                    if not any(f.endswith("_SUCCESS") for f in file_list):
                        logger.warning(f"⚠️ 未完了のリモートチャンクをスキップ: {chunk_id}")
                        continue

                    valid_remote_count += 1
                    for remote_path in file_list:
                        if remote_path.endswith("_SUCCESS"):
                            continue

                        key = self._get_key_from_filename(Path(remote_path).name)
                        if key:
                            attempts = 2
                            for att in range(attempts):
                                try:
                                    local_path = hf_hub_download(
                                        repo_id=self.hf_repo,
                                        filename=remote_path,
                                        repo_type="dataset",
                                        token=self.hf_token,
                                    )
                                    df = pd.read_parquet(local_path)
                                    deltas.setdefault(key, []).append(df)
                                    break
                                except Exception as e:
                                    if att == attempts - 1:
                                        logger.error(f"❌ リモートデルタ読み込み失敗 ({remote_path}): {e}")
                                    else:
                                        time.sleep(5)

                logger.info(f"収集結果: Local Chunks={len(processed_chunks)}, Remote Chunks={valid_remote_count}")

            except Exception as e:
                logger.error(f"リモートデルタ収集失敗: {e}")

        # --- C. 最終マージ ---
        merged = {}
        for key, df_list in deltas.items():
            if df_list:
                merged[key] = pd.concat(df_list, ignore_index=True)
            else:
                merged[key] = pd.DataFrame()
        return merged

    def _get_key_from_filename(self, fname: str) -> Optional[str]:
        """ファイル名から内部キーを判定する"""
        if fname == "documents_index.parquet":
            return "catalog"
        if fname == "stocks_master.parquet":
            return "master"
        if fname == "listing_history.parquet":
            return "listing"
        if fname == "index_history.parquet":
            return "index"
        if fname == "name_history.parquet":
            return "name"
        if fname.startswith("financial_values_bin"):
            bin_id = fname.replace("financial_values_bin", "").replace(".parquet", "")
            return f"financial_bin{bin_id}"
        if fname.startswith("qualitative_text_bin"):
            bin_id = fname.replace("qualitative_text_bin", "").replace(".parquet", "")
            return f"text_bin{bin_id}"
        if fname.startswith("financial_values_"):
            sector = fname.replace("financial_values_", "").replace(".parquet", "")
            return f"financial_{sector}"
        if fname.startswith("qualitative_text_"):
            sector = fname.replace("qualitative_text_", "").replace(".parquet", "")
            return f"text_{sector}"
        return None

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
        # 【修正】Hugging Face API 制限 (128 req/hour) とタイムアウト回避のため、
        # GHA並列数(20) を考慮してバッチサイズを 200 に縮小し、合計リクエスト数を抑制する。
        # (600 files / 200 = 3 commits * 20 jobs = 60 req < 128 req)
        batch_size = 200

        batches = [ops_list[i : i + batch_size] for i in range(0, total_ops, batch_size)]

        logger.info(f"🚀 コミット送信開始: 合計 {total_ops} 操作を {len(batches)} バッチに分割して実行します")

        for i, batch in enumerate(batches):
            batch_msg = f"{message} (part {i + 1}/{len(batches)})"
            max_retries = 12
            success = False

            for attempt in range(max_retries):
                try:
                    # 【重要】create_commit はリクエストが重いため、個別にタイムアウトを設定
                    # (パッケージのバージョンによっては直接引数を取らない場合があるため、セッション側で保護)
                    self.api.create_commit(
                        repo_id=self.hf_repo,
                        repo_type="dataset",
                        operations=batch,
                        commit_message=batch_msg,
                        token=self.hf_token,
                    )
                    success = True
                    break
                except BaseException as e:
                    if isinstance(e, Exception):
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
                            # 20並列以上の環境下では、待機時間を広めに分散させる (10〜70秒 + 指数)
                            wait_time = (2 ** (attempt + 1)) * 5 + (random.uniform(10, 60))
                            logger.warning(
                                f"Commit Conflict ({status_code}). Retrying in {wait_time:.2f}s... "
                                f"(Batch {i + 1}, Attempt {attempt + 1}/{max_retries})"
                            )
                            time.sleep(wait_time)
                            continue

                        # タイムアウト等のネットワーク例外
                        wait_time = (attempt + 1) * 20 + random.uniform(5, 15)
                        logger.warning(
                            f"通信エラー ({e}): {wait_time:.1f}秒待機して再試行します... "
                            f"(Batch {i + 1}, Attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                    else:
                        # KeyboardInterrupt や SystemExit など、通常の例外以外で終了する場合
                        logger.critical(
                            f"⚠️ プロセスがシグナルまたは致命的な例外によって中断されました: {type(e).__name__}"
                        )
                        raise e

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

            # 削除対象のファイルリストを作成
            delete_files = []

            if cleanup_old:
                # 24時間以上経過したものを対象とする
                from datetime import datetime, timezone

                now = datetime.now(timezone.utc)
                expired_runs = set()

                for f in files:
                    if not f.startswith(delta_root):
                        continue
                    parts = f.split("/")
                    if len(parts) < 3:
                        continue
                    r_id = parts[2]

                    # 【修正】run_id は 'backfill-YYYY-MM-DD-NNNNNN' 等の形式
                    # 日付部分を正規表現で抽出し、24時間以上経過しているかを判定
                    try:
                        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", r_id)
                        if date_match:
                            run_date = datetime.strptime(date_match.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
                            if (now - run_date).total_seconds() > 86400:
                                delete_files.append(f)
                                expired_runs.add(r_id)
                        else:
                            # 日付を含まないrun_id（純粋な数値タイムスタンプ等）も処理
                            try:
                                timestamp = int(r_id)
                                if (now.timestamp() - timestamp) > 86400:
                                    delete_files.append(f)
                                    expired_runs.add(r_id)
                            except ValueError:
                                # パース不能なrun_idは7日以上経過とみなしてクリーンアップ
                                delete_files.append(f)
                                expired_runs.add(r_id)
                    except Exception:
                        pass

                if delete_files:
                    logger.info(f"古い一時フォルダを清掃中... (24時間以上経過: {len(expired_runs)} runs)")

            else:
                # 今回のランIDのみ対象
                target_prefix = f"{delta_root}/{run_id}"
                delete_files = [f for f in files if f.startswith(target_prefix)]
                if delete_files:
                    logger.info(f"今回の一時ファイルを削除中... {run_id} ({len(delete_files)} files)")

            if not delete_files:
                return

            # バッチサイズを拡大 (50 -> 500) してAPIコール数を削減
            batch_size = 500
            total_batches = (len(delete_files) + batch_size - 1) // batch_size

            for i in range(0, len(delete_files), batch_size):
                batch = delete_files[i : i + batch_size]
                del_ops = [CommitOperationDelete(path_in_repo=p) for p in batch]

                batch_num = (i // batch_size) + 1
                commit_msg = f"Cleanup deltas (Batch {batch_num}/{total_batches})"

                # リトライロジック (Backoff)
                max_retries = 10
                success = False
                for attempt in range(max_retries):
                    try:
                        self.api.create_commit(
                            repo_id=self.hf_repo,
                            repo_type="dataset",
                            operations=del_ops,
                            commit_message=commit_msg,
                            token=self.hf_token,
                        )
                        success = True
                        break
                    except Exception as e:
                        if isinstance(e, HfHubHTTPError) and e.response.status_code == 429:
                            wait_time = int(e.response.headers.get("Retry-After", 60)) + 5
                            logger.warning(
                                f"Cleanup Rate limit exceeded. Waiting {wait_time}s... "
                                f"(Batch {batch_num}/{total_batches}, Attempt {attempt + 1})"
                            )
                            time.sleep(wait_time)
                            continue

                        logger.warning(
                            f"Cleanup error: {e}. Retrying... "
                            f"(Batch {batch_num}/{total_batches}, Attempt {attempt + 1})"
                        )
                        time.sleep(10 * (attempt + 1))

                if success:
                    logger.debug(f"Cleanup batch {batch_num}/{total_batches} done.")
                    if batch_num < total_batches:
                        time.sleep(2)  # バッチ間のクールダウン
                else:
                    logger.error(f"❌ Cleanup batch {batch_num} failed permanently.")

            logger.success("Cleanup sequence completed.")

        except Exception as e:
            logger.error(f"クリーンアップ全体失敗: {e}")
