import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
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

        # 【追加】バッチコミット用バッファ
        self._commit_operations = {}  # パスをキーとした辞書 (一回のコミットでパスの重複を避ける)
        logger.info("CatalogManager を初期化しました。")

    def _load_parquet(self, key: str) -> pd.DataFrame:
        filename = self.paths[key]
        try:
            local_path = hf_hub_download(
                repo_id=self.hf_repo, filename=filename, repo_type="dataset", token=self.hf_token
            )
            df = pd.read_parquet(local_path)
            # 【重要】マスタの場合、codeカラムを確実に文字列化（前方一致や検索の安定化）
            if key == "master" and "code" in df.columns:
                df["code"] = df["code"].astype(str).str.strip()
            logger.debug(f"ロード成功: {filename} ({len(df)} rows)")
            return df
        except RepositoryNotFoundError:
            logger.error(f"❌ リポジトリが見つかりません: {self.hf_repo}")
            logger.error("環境変数 HF_REPO の設定を確認してください")
            raise
        except EntryNotFoundError:
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
            logger.error(f"❌ HF API エラー ({e.response.status_code}): {filename}")
            logger.error(f"詳細: {e}")
            if e.response.status_code == 401:
                logger.error("認証エラー: HF_TOKEN が無効または期限切れの可能性があります")
            elif e.response.status_code == 403:
                logger.error("アクセス拒否: リポジトリへのアクセス権限がありません")
            raise
        except Exception as e:
            logger.error(f"❌ 予期しないエラー: {filename} - {type(e).__name__}: {e}")
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
            logger.error("❌ カタログのアップロードに失敗したため、メモリ上の状態を保持します")
            return False

    def _save_and_upload(self, key: str, df: pd.DataFrame, defer: bool = False) -> bool:
        filename = self.paths[key]
        local_file = self.data_path / Path(filename).name

        # 型の安定化
        for col in df.columns:
            if df[col].dtype == "object":
                # ブール値が含まれる場合は文字列化を避ける
                if not df[col].apply(lambda x: isinstance(x, bool)).any():
                    df[col] = df[col].astype(str).replace("None", "")

        df.to_parquet(local_file, index=False, compression="zstd")

        if self.api:
            if defer:
                # 【重要】rec カラム（インデックス残骸）の完全排除
                if "rec" in df.columns:
                    df = df.drop(columns=["rec"])

                # バッファに追加して終了 (最新のもので上書き)
                self._commit_operations[filename] = CommitOperationAdd(
                    path_in_repo=filename, path_or_fileobj=str(local_file)
                )
                logger.debug(f"コミットバッファに追加: {filename}")
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
            return False
        return True

    def upload_raw(self, local_path: Path, repo_path: str, defer: bool = False) -> bool:
        """ローカルの生データを Hugging Face の raw/ フォルダにアップロード"""
        if not local_path.exists():
            logger.error(f"ファイルが存在しないためアップロードできません: {local_path}")
            return False

        if self.api:
            if defer:
                self._commit_operations[repo_path] = CommitOperationAdd(
                    path_in_repo=repo_path, path_or_fileobj=str(local_path)
                )
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

    def update_stocks_master(self, new_master: pd.DataFrame):
        """マスタ更新 (Pydantic バリデーション実施)"""
        if new_master.empty:
            return

        records = new_master.to_dict("records")
        validated = []
        for rec in records:
            try:
                validated.append(StockMasterRecord(**rec).model_dump())
            except Exception as e:
                logger.error(f"銘柄マスタのバリデーション失敗 (code: {rec.get('code')}): {e}")

        if not validated:
            return
        valid_df = pd.DataFrame(validated)

        # 社名変更チェック
        if not self.master_df.empty:
            merged = pd.merge(
                self.master_df[["code", "company_name"]],
                valid_df[["code", "company_name"]],
                on="code",
                suffixes=("_old", "_new"),
            )
            changed = merged[merged["company_name_old"] != merged["company_name_new"]]
            if not changed.empty:
                today = datetime.now().strftime("%Y-%m-%d")
                name_history = self._load_parquet("name")
                for _, row in changed.iterrows():
                    name_history = pd.concat(
                        [
                            name_history,
                            pd.DataFrame(
                                [
                                    {
                                        "code": row["code"],
                                        "old_name": row["company_name_old"],
                                        "new_name": row["company_name_new"],
                                        "change_date": today,
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
                self._save_and_upload("name", name_history.drop_duplicates())

        self.master_df = valid_df
        return self._save_and_upload("master", self.master_df)  # 【修正】戻り値を返す

    def get_last_index_list(self, index_name: str) -> pd.DataFrame:
        """指定指数の構成銘柄を取得 (Phase 3用)"""
        return pd.DataFrame(columns=["code"])

    def get_sector(self, code: str) -> str:
        """証券コードから業種取得"""
        if self.master_df.empty:
            return "その他"
        row = self.master_df[self.master_df["code"] == code]
        if not row.empty:
            return str(row.iloc[0]["sector"])
        return "その他"

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

        # 型の安定化
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str)

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
            # フォルダ内の全ファイルをリスト
            folder = f"temp/deltas/{run_id}"
            files = self.api.list_repo_files(repo_id=self.hf_repo, repo_type="dataset")
            target_files = [f for f in files if f.startswith(folder)]

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
                                # 【重要】rec カラム排除
                                if "rec" in df.columns:
                                    df = df.drop(columns=["rec"])
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
        """バッファに溜まった操作を一括でコミット実行"""
        if not self.api or not self._commit_operations:
            return True

        max_retries = 8  # リトライ回数を増やして競合に備える
        ops_list = list(self._commit_operations.values())

        for attempt in range(max_retries):
            try:
                self.api.create_commit(
                    repo_id=self.hf_repo,
                    repo_type="dataset",
                    operations=ops_list,
                    commit_message=message,
                    token=self.hf_token,
                )
                logger.success(f"✅ バッチコミット成功: {len(ops_list)} 操作")
                self._commit_operations = {}  # クリア
                return True
            except Exception as e:
                # 429 レート制限
                if isinstance(e, HfHubHTTPError) and e.response.status_code == 429:
                    wait_time = int(e.response.headers.get("Retry-After", 60)) + 5
                    logger.warning(f"Commit Rate limit exceeded. Waiting {wait_time}s... ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue

                # 409 コンフリクト (他のジョブが同時にコミットした)
                if isinstance(e, HfHubHTTPError) and e.response.status_code == 409:
                    # 指数バックオフ + ジッター
                    wait_time = (2**attempt) + (random.uniform(0, 5))
                    logger.warning(
                        f"Commit Conflict (409). Retrying in {wait_time:.2f}s... ({attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                logger.warning(f"コミット失敗: {e} - Retrying ({attempt + 1}/{max_retries})...")
                time.sleep(10 * (attempt + 1))

        logger.error("❌ バッチコミットに最終的に失敗しました")
        return False

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
