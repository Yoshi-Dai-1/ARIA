"""
Catalog Manager (Facade) — ARIA のデータレイクハウス状態管理の中核。
I/O処理は HfStorage、デルタ管理は DeltaManager、複雑な名寄せは ReconciliationEngine に委譲し、
自身は状態（DF）の保持とオーケストレーションに専念する。
"""

import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests

# --- 分離されたモジュールのインポート ---
from delta_manager import DeltaManager
from hf_storage import HfStorage
from loguru import logger
from models import EdinetCodeRecord
from reconciliation_engine import ReconciliationEngine


class CatalogManager:
    def __init__(self, hf_repo: str, hf_token: str, data_path: Path, scope: str = None):
        if scope is None:
            from config import ARIA_SCOPE

            scope = ARIA_SCOPE
        self.scope = scope.capitalize()

        # サブモジュールの初期化
        paths = {
            "catalog": "catalog/documents_index.parquet",
            "master": "meta/stocks_master.parquet",
            "listing": "meta/listing_history.parquet",
            "index": "meta/index_history.parquet",
            "name": "meta/name_history.parquet",
        }
        self.storage = HfStorage(hf_repo, hf_token, data_path, paths)
        self.delta_mgr = DeltaManager(self.storage, data_path, paths, clean_fn=self._clean_dataframe)
        self.engine = ReconciliationEngine(self)

        # 状態の初期化
        self._snapshots = {}
        self.edinet_codes = {}
        self.aggregation_map = {}

        # データのロード
        self.catalog_df = self.storage.load_parquet("catalog", clean_fn=self._clean_dataframe)
        self.master_df = self.storage.load_parquet("master", clean_fn=self._clean_dataframe)

        logger.info("CatalogManager (Facade) を初期化しました。")

        # 整合性チェックと最新スキーマへのアップグレード
        self._retrospective_cleanse()

        # 起動時にEDINETコードリストを同期しマスタに反映
        self.edinet_codes, self.aggregation_map = self.sync_edinet_code_lists()
        if self.edinet_codes:
            self.engine.update_master_from_edinet_codes()
            if self.storage.has_pending_operations:
                logger.info("初期マスター構築を検知しました。直ちに Hugging Face に保存します。")
                self.storage.push_commit("Initial Master Build from EDINET")

    # ──────────────────────────────────────────────
    # 委譲 (Delegations)
    # ──────────────────────────────────────────────
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
        return self.delta_mgr.save_delta(key, df, run_id, chunk_id, custom_filename, defer, local_only)

    def mark_chunk_success(self, run_id: str, chunk_id: str, defer: bool = False, local_only: bool = False) -> bool:
        return self.delta_mgr.mark_chunk_success(run_id, chunk_id, defer, local_only)

    def load_deltas(self, run_id: str) -> Dict[str, pd.DataFrame]:
        return self.delta_mgr.load_deltas(run_id)

    def cleanup_deltas(self, run_id: str, cleanup_old: bool = True):
        self.delta_mgr.cleanup_deltas(run_id, cleanup_old)

    def push_commit(self, message: str = "Batch update from ARIA") -> bool:
        return self.storage.push_commit(message)

    def upload_raw(self, local_path: Path, repo_path: str, defer: bool = False) -> bool:
        return self.storage.upload_raw(local_path, repo_path, defer)

    def upload_raw_folder(self, folder_path: Path, path_in_repo: str, defer: bool = False) -> bool:
        return self.storage.upload_raw_folder(folder_path, path_in_repo, defer)

    def get_sector(self, code: str) -> str:
        if self.master_df.empty:
            logger.info("マスタファイルが空です。EDINET APIから初期構築を行います。")
            self.edinet_codes, self.aggregation_map = self.sync_edinet_code_lists()
            if self.edinet_codes:
                self.engine.update_master_from_edinet_codes()
                self.storage.push_commit("Initial Master Build from EDINET")
        row = self.master_df[self.master_df["code"] == code]
        if not row.empty:
            col_name = "sector_jpx_33" if "sector_jpx_33" in self.master_df.columns else "sector"
            val = row.iloc[0].get(col_name)
            return str(val) if val is not None else None
        return None

    def update_stocks_master(self, incoming_data: pd.DataFrame):
        return self.engine.update_stocks_master(incoming_data)

    # ──────────────────────────────────────────────
    # State Management & Validation
    # ──────────────────────────────────────────────
    def _clean_dataframe(self, key: str, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        try:
            if key == "catalog":
                from models import CatalogRecord

                df["is_amendment"] = df["is_amendment"].astype(bool) if "is_amendment" in df.columns else False
                records = []
                for _, row in df.iterrows():
                    d = {k: (v if pd.notna(v) else None) for k, v in row.to_dict().items()}
                    records.append(CatalogRecord(**d).model_dump())
                return pd.DataFrame(records)

            elif key == "master":
                from models import StockMasterRecord

                records = []
                for _, row in df.iterrows():
                    d = {k: (v if pd.notna(v) else None) for k, v in row.to_dict().items()}
                    records.append(StockMasterRecord(**d).model_dump())
                return pd.DataFrame(records)
        except Exception as e:
            logger.warning(f"データクレンジングエラー ({key}): {e} - フォールバックとして元のDFを返します。")

        return df

    def _retrospective_cleanse(self):
        logger.info("データ構造の健全性確認を開始します (Retrospective Cleanse)...")
        updates_needed = False

        try:
            old_catalog_len = len(self.catalog_df)
            new_catalog = self._clean_dataframe("catalog", self.catalog_df.copy())
            if not new_catalog.equals(self.catalog_df):
                logger.warning(f"CatalogSchemaの不一致を検知。自動修正します。({old_catalog_len}件)")
                self.catalog_df = new_catalog
                self.storage.save_and_upload("catalog", self.catalog_df, defer=True)
                updates_needed = True

            old_master_len = len(self.master_df)
            new_master = self._clean_dataframe("master", self.master_df.copy())
            if not new_master.equals(self.master_df):
                logger.warning(f"StockMasterSchemaの不一致を検知。自動修正します。({old_master_len}件)")
                self.master_df = new_master
                self.storage.save_and_upload("master", self.master_df, defer=True)
                updates_needed = True

            if updates_needed:
                logger.success("✅ データ構造の自動修正予約が完了しました。")

        except Exception as e:
            logger.error(f"Retrospective Cleanse に失敗しました: {e}")

    def take_snapshot(self):
        self._snapshots = {
            "catalog": self.catalog_df.copy(),
            "master": self.master_df.copy(),
            "listing": self.storage.load_parquet("listing").copy(),
            "index": self.storage.load_parquet("index").copy(),
            "name": self.storage.load_parquet("name").copy(),
        }
        logger.info("Global 状態のスナップショットを取得しました (安全性確保)")

    def rollback(self, message: str = "RaW-V Failure: Automated Recovery Rollback"):
        if not self._snapshots:
            logger.error("❌ スナップショットが存在しないため、ロールバックできません。")
            return False

        logger.warning(f"⛔ ロールバックを開始します: {message}")
        self.storage.clear_operations()

        for key, df in self._snapshots.items():
            self.storage.save_and_upload(key, df, clean_fn=self._clean_dataframe, defer=True)

        success = self.storage.push_commit(f"ROLLBACK: {message}")
        if success:
            logger.success("✅ ロールバック・コミットが完了しました。整合性は復旧されました。")
            self.catalog_df = self._snapshots["catalog"]
            self.master_df = self._snapshots["master"]
        else:
            logger.critical("❌ ロールバック自体に失敗しました！")
        return success

    # ──────────────────────────────────────────────
    # FSA (金融庁) リスト同期機能
    # ──────────────────────────────────────────────
    def sync_edinet_code_lists(self) -> Tuple[Dict[str, EdinetCodeRecord], Dict[str, str]]:
        import io

        logger.info("金融庁からEDINETコードリストを取得・解析中...")
        codes = {}
        aggregation_map = {}

        # --- 集約一覧 (aggregation_map) の取得 ---
        url_consolidated = "https://disclosure2.edinet-fsa.go.jp/weee0040.zip"
        try:
            res_c = requests.get(url_consolidated, timeout=15)
            if res_c.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(res_c.content)) as z:
                    csv_filename = [f for f in z.namelist() if f.endswith(".csv")][0]
                    with z.open(csv_filename) as f:
                        df_agg = pd.read_csv(f, encoding="cp932", skiprows=1)
                        for _, row in df_agg.iterrows():
                            o_code = str(row["提出者等のEDINETコード（変更前）"]).strip()
                            n_code = str(row["提出者等のEDINETコード（変更後）"]).strip()
                            if len(o_code) == 6 and len(n_code) == 6:
                                aggregation_map[o_code] = n_code
                logger.info(f"提出者集約一覧を取得しました: {len(aggregation_map)}件の「旧→新」マッピング")
        except Exception as e:
            logger.warning(f"提出者集約一覧の取得に失敗しました: {e}。集約ブリッジ機能はスキップされます。")

        def safe_int_str(val):
            if pd.isna(val) or val is None or str(val).strip() == "":
                return None
            try:
                return str(int(float(val)))
            except ValueError:
                return str(val).strip()

        # --- 和文リスト (JCN, 上場区分, 業種等) ---
        url_ja = "https://disclosure2.edinet-fsa.go.jp/weee0020.zip"
        try:
            res = requests.get(url_ja, timeout=15)
            if res.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                    csv_filename = [f for f in z.namelist() if f.endswith(".csv")][0]
                    with z.open(csv_filename) as f:
                        df_ja = pd.read_csv(f, encoding="cp932", skiprows=1)
                        for _, row in df_ja.iterrows():
                            e_code = str(row["ＥＤＩＮＥＴコード"]).strip()
                            if len(e_code) != 6:
                                continue
                            sec_code = safe_int_str(row["証券コード"])
                            # API側の "0" という異常な証券コードを排除
                            if sec_code == "0" or sec_code == "0000" or sec_code == "00000":
                                sec_code = None
                            jcn = safe_int_str(row["提出者法人番号"])
                            codes[e_code] = EdinetCodeRecord(
                                edinet_code=e_code,
                                jcn=jcn,
                                submitter_type=row.get("提出者種別"),
                                is_listed=row.get("上場区分"),
                                is_consolidated=row.get("連結の有無"),
                                capital=safe_int_str(row.get("資本金")),
                                settlement_date=str(row.get("決算日") or "").strip() or None,
                                submitter_name=str(row.get("提出者名") or "").strip() or None,
                                submitter_name_en=str(row.get("提出者名（英字）") or "").strip() or None,
                                submitter_name_kana=str(row.get("提出者名（ヨミ）") or "").strip() or None,
                                address=str(row.get("所在地") or "").strip() or None,
                                industry_edinet=str(row.get("提出者業種") or "").strip() or None,
                                sec_code=sec_code,
                            )
            else:
                logger.error(f"EDINETコードリスト本体のダウンロード失敗: HTTP {res.status_code}")
                return {}, {}
        except Exception as e:
            logger.error(f"EDINETコードリストの処理中にエラー: {e}")
            return {}, {}

        # --- 英文リスト (industry_edinet_en のみ補完) ---
        url_en = "https://disclosure2.edinet-fsa.go.jp/weee0030.zip"
        try:
            res_en = requests.get(url_en, timeout=15)
            if res_en.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(res_en.content)) as z:
                    csv_filename = [f for f in z.namelist() if f.endswith(".csv")][0]
                    with z.open(csv_filename) as f:
                        df_en = pd.read_csv(f, encoding="cp932", skiprows=1)
                        for _, row in df_en.iterrows():
                            e_code = str(row["Edinet Code"]).strip()
                            ind_en = str(row.get("Industry") or "").strip() or None
                            if e_code in codes and ind_en:
                                codes[e_code].industry_edinet_en = ind_en
        except Exception as e:
            logger.warning(f"英文EDINETコードリストの取得・反映に失敗しました: {e} (和文のみで続行します)")

        logger.info(f"EDINETコードリストの構築完了: {len(codes)} 件抽出")
        return codes, aggregation_map

    # ──────────────────────────────────────────────
    # Catalog / History Methods
    # ──────────────────────────────────────────────
    def is_processed(self, doc_id: str) -> bool:
        if self.catalog_df.empty:
            return False
        return doc_id in self.catalog_df["doc_id"].values

    def get_status(self, doc_id: str) -> str:
        if self.catalog_df.empty:
            return "unknown"
        row = self.catalog_df[self.catalog_df["doc_id"] == doc_id]
        if not row.empty:
            return str(row.iloc[0]["processed_status"])
        return "unknown"

    def update_catalog(self, new_records: List[Dict]):
        if not new_records:
            return

        df_new = pd.DataFrame(new_records)
        df_new = self._clean_dataframe("catalog", df_new)

        if self.catalog_df.empty:
            self.catalog_df = df_new
        else:
            self.catalog_df = pd.concat([self.catalog_df, df_new], ignore_index=True)
            self.catalog_df.drop_duplicates(subset=["doc_id"], keep="last", inplace=True)

        self.storage.save_and_upload("catalog", self.catalog_df, clean_fn=self._clean_dataframe, defer=True)
        logger.info(f"カタログを更新・コミットバッファに追加しました (全 {len(self.catalog_df)} 件)")

    def update_listing_history(self, new_events: pd.DataFrame):
        hist_df = self.storage.load_parquet("listing")
        m_df = pd.concat([hist_df, new_events], ignore_index=True)
        m_df.drop_duplicates(subset=["code", "type", "event_date"], keep="last", inplace=True)
        m_df.sort_values(["event_date", "code"], ascending=[False, True], inplace=True)
        self.storage.save_and_upload("listing", m_df, defer=True)

    def update_index_history(self, new_events: pd.DataFrame):
        hist_df = self.storage.load_parquet("index")
        m_df = pd.concat([hist_df, new_events], ignore_index=True)
        m_df.drop_duplicates(subset=["index_name", "code", "type", "event_date"], keep="last", inplace=True)
        m_df.sort_values(["event_date", "index_name", "code"], ascending=[False, True, True], inplace=True)
        self.storage.save_and_upload("index", m_df, defer=True)

    def get_listing_history(self) -> pd.DataFrame:
        return self.storage.load_parquet("listing")

    def get_index_history(self) -> pd.DataFrame:
        return self.storage.load_parquet("index")
