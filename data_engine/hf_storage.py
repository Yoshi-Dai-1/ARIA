"""
Hugging Face Storage Layer — ARIA データの永続化・読み込み・コミットを一元管理するモジュール。

CatalogManager から分離された純粋な I/O 関心事:
- Parquet の読み込み / 保存 / アップロード
- RAW ファイル (ZIP/PDF) のアップロード
- コミットバッファリングとバッチコミット
"""

import random
import time
from pathlib import Path
from typing import Dict

import pandas as pd
import requests
from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download
from huggingface_hub.utils import EntryNotFoundError, HfHubHTTPError, RepositoryNotFoundError
from loguru import logger
from models import ARIA_SCHEMAS, CatalogRecord, IndexEvent, ListingEvent, StockMasterRecord


class HfStorage:
    """Hugging Face Hub との通信・永続化を担当する I/O 層"""

    def __init__(self, hf_repo: str, hf_token: str, data_path: Path, paths: Dict[str, str]):
        """
        Args:
            hf_repo: Hugging Face リポジトリ ID
            hf_token: Hugging Face API トークン
            data_path: ローカルデータディレクトリ
            paths: 内部キーからリポジトリパスへの対応表 (e.g., {"catalog": "catalog/documents_index.parquet"})
        """
        self.hf_repo = hf_repo
        self.hf_token = hf_token
        self.data_path = data_path
        self.paths = paths

        # 【修正】通信安定性向上のため、タイムアウト環境変数を設定
        import os

        if hf_repo and hf_token:
            os.environ["HF_HUB_TIMEOUT"] = "300"
            os.environ["HF_HUB_HTTP_TIMEOUT"] = "300"
            self.api = HfApi(token=hf_token)
        else:
            self.api = None

        # コミットバッファ: {repo_path: CommitOperationAdd or (DataFrame, local_path)}
        self._commit_operations: Dict = {}

    # ──────────────────────────────────────────────
    # Parquet 読み込み
    # ──────────────────────────────────────────────
    def load_parquet(self, key: str, clean_fn=None, force_download: bool = False) -> pd.DataFrame:
        """
        HF リポジトリから Parquet ファイルを読み込む。
        バッファに保留中のデータがあればそちらを優先する (Read-Your-Writes)。

        Args:
            key: 内部キー ("catalog", "master" 等)
            clean_fn: DataFrame に適用するクレンジング関数 (optional)
            force_download: キャッシュを無視して再ダウンロードするか
        """
        filename = self.paths[key]

        # 【重要: Lost Update 防止】保留中のコミット（メモリ上）があれば、リモートより優先する
        if filename in self._commit_operations:
            data = self._commit_operations[filename]
            logger.debug(f"メモリ上の保留中データをロードに使用します: {filename}")
            return data[0] if isinstance(data, tuple) else data

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
            if clean_fn:
                df = clean_fn(key, df)
            logger.debug(f"ロード成功: {filename} ({len(df)} rows)")
            return df
        except RepositoryNotFoundError:
            logger.error(f"リポジトリが見つかりません: {self.hf_repo}")
            logger.error("環境変数 HF_REPO の設定を確認してください")
            raise
        except (EntryNotFoundError, requests.exceptions.HTTPError) as e:
            is_404 = isinstance(e, EntryNotFoundError) or (
                hasattr(e, "response") and e.response is not None and e.response.status_code == 404
            )

            if not is_404:
                raise e

            logger.info(f"ファイルが存在しないため新規作成します: {filename}")
            if key == "catalog":
                cols = list(CatalogRecord.model_fields.keys())
                return pd.DataFrame(columns=cols)
            elif key == "master":
                cols = list(StockMasterRecord.model_fields.keys())
                return pd.DataFrame(columns=cols)
            elif key == "listing":
                cols = list(ListingEvent.model_fields.keys())
                return pd.DataFrame(columns=cols)
            elif key == "index":
                cols = list(IndexEvent.model_fields.keys())
                return pd.DataFrame(columns=cols)
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

    # ──────────────────────────────────────────────
    # Parquet 保存 & アップロード
    # ──────────────────────────────────────────────
    def save_and_upload(self, key: str, df: pd.DataFrame, clean_fn=None, defer: bool = False) -> bool:
        """Parquet ファイルをローカル保存し、HF にアップロードする。"""
        filename = self.paths[key]
        local_file = self.data_path / filename
        local_file.parent.mkdir(parents=True, exist_ok=True)

        # 【絶対ガード】保存直前に最終クレンジング
        if clean_fn:
            df = clean_fn(key, df)

        # 【Phase 3: 金型アーキテクチャ】明示スキーマで型ブレを物理的に排除
        schema = ARIA_SCHEMAS.get(key)
        df.to_parquet(local_file, index=False, compression="zstd", schema=schema)

        if self.api:
            if defer:
                # 【重要】バッファには (DataFrame, 物理パス) を保持する
                # これにより load_parquet での再利用 (Read-Your-Writes) を可能にする
                self._commit_operations[filename] = (df, local_file)
                logger.debug(f"コミットバッファに追加: {filename}")
                return True

            return self._upload_with_retry(str(local_file), filename)
        return True

    # ──────────────────────────────────────────────
    # RAW ファイルアップロード
    # ──────────────────────────────────────────────
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

            return self._upload_with_retry(str(local_path), repo_path)
        return True

    def upload_raw_folder(self, folder_path: Path, path_in_repo: str, defer: bool = False) -> bool:
        """フォルダ単位での一括アップロード (リトライ付)"""
        if not folder_path.exists():
            return True  # アップロード対象なしは成功とみなす

        if self.api:
            if defer:
                for f in folder_path.glob("**/*"):
                    if f.is_file():
                        r_path = f"{path_in_repo}/{f.relative_to(folder_path)}"
                        self._commit_operations[r_path] = CommitOperationAdd(
                            path_in_repo=r_path, path_or_fileobj=str(f)
                        )
                logger.debug(f"RAWフォルダをコミットバッファに追加: {path_in_repo}")
                return True

            max_retries = 5
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

    # ──────────────────────────────────────────────
    # コミットバッファ操作
    # ──────────────────────────────────────────────
    def add_commit_operation(self, repo_path: str, local_path: Path):
        """コミットバッファに操作を追加（重複は最新で上書き）"""
        self._commit_operations[repo_path] = CommitOperationAdd(path_in_repo=repo_path, path_or_fileobj=str(local_path))
        logger.debug(f"コミットバッファに追加: {repo_path}")

    @property
    def has_pending_operations(self) -> bool:
        """保留中のコミット操作があるかどうか"""
        return bool(self._commit_operations)

    def clear_operations(self):
        """コミットバッファをクリアする"""
        self._commit_operations = {}

    def push_commit(self, message: str = "Batch update from ARIA") -> bool:
        """
        バッファに溜まった操作をコミット実行。
        操作数が多い場合は、HF側の負荷と429エラーを避けるため、自動的に分割してコミットする。
        """
        if not self.api or not self._commit_operations:
            return True

        # バッファ内のデータを CommitOperationAdd に変換
        ops_list = []
        for repo_path, data in self._commit_operations.items():
            if isinstance(data, tuple):
                _, local_path = data
                ops_list.append(CommitOperationAdd(path_in_repo=repo_path, path_or_fileobj=str(local_path)))
            else:
                ops_list.append(data)

        total_ops = len(ops_list)

        # 1コミットあたりの最大操作数
        # HF API制限 (128 req/hour) とタイムアウト回避のため、バッチサイズを200に設定
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

    # ──────────────────────────────────────────────
    # 内部ヘルパー
    # ──────────────────────────────────────────────
    def _upload_with_retry(self, local_path_str: str, repo_path: str, max_retries: int = 5) -> bool:
        """単一ファイルのアップロード（リトライ付き）"""
        for attempt in range(max_retries):
            try:
                self.api.upload_file(
                    path_or_fileobj=local_path_str,
                    path_in_repo=repo_path,
                    repo_id=self.hf_repo,
                    repo_type="dataset",
                    token=self.hf_token,
                )
                logger.success(f"アップロード成功: {repo_path}")
                return True
            except Exception as e:
                if isinstance(e, HfHubHTTPError) and e.response.status_code == 429:
                    wait_time = int(e.response.headers.get("Retry-After", 60)) + 5
                    logger.warning(f"Rate limit exceeded. Waiting {wait_time}s... ({attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue

                if isinstance(e, HfHubHTTPError) and e.response.status_code >= 500:
                    wait_time = 15 * (attempt + 1)
                    logger.warning(
                        f"HF Server Error ({e.response.status_code}). "
                        f"Waiting {wait_time}s... ({attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

                logger.warning(f"アップロード一時エラー: {repo_path} - {e} - Retrying ({attempt + 1}/{max_retries})...")
                time.sleep(10 * (attempt + 1))

        logger.error(f"❌ アップロードに最終的に失敗しました: {repo_path}")
        return False
