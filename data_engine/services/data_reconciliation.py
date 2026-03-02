"""
ARIA Data Reconciliation Engine
Model-Driven Automated Quality Assurance

旧 integrity_audit.py を完全に置き換え、以下の「4層・11項目」の自律検証を行う：
Layer 1: スキーマ照合（マスター vs Pydantic Model）
Layer 2: 物理ファイル照合（ZIP/PDFの存在と破損チェック）
Layer 3: 分析マスタ照合（Binアサインメント、doc_id重複、孤児レコード）
Layer 4: APIカタログ照合（10年枠内のメタデータ不一致検証）
"""

import json
import logging
import os
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from data_engine.catalog_manager import CatalogManager
from data_engine.core import utils

# ARIA モジュール
from data_engine.core.models import CatalogRecord, IndexEvent, ListingEvent, StockMasterRecord

logger = logging.getLogger(__name__)


class DataReconciliationEngine:
    def __init__(self, hf_repo: str, hf_token: str, data_path: Path):
        self.hf_repo = hf_repo
        self.hf_token = hf_token
        self.data_path = data_path

        # 内部状態として CatalogManager を持つが、スコープは 'All' にして全量監査を行う
        self.cm = CatalogManager(hf_repo, hf_token, data_path, scope="All")

        # 検証エラーの集計
        self.anomalies = {"Layer1_Schema": [], "Layer2_Physical": [], "Layer3_Analytical": [], "Layer4_Catalog": []}

    def _report_anomaly(self, layer: str, message: str, doc_id: str = None, details: Any = None):
        anomaly = {"message": message}
        if doc_id:
            anomaly["doc_id"] = doc_id
        if details:
            anomaly["details"] = details

        self.anomalies[layer].append(anomaly)
        logger.warning(f"[{layer}] Anomaly detected: {message}")

    # ==========================================
    # Layer 1: Schema Reconciliation
    # ==========================================
    def reconcile_schemas(self):
        """PydanticモデルとParquetカラムの完全一致を検証"""
        logger.info("--- [Layer 1] Schema Reconciliation ---")

        check_targets = [
            ("catalog", "documents_index.parquet", CatalogRecord),
            ("master", "stocks_master.parquet", StockMasterRecord),
            ("listing", "listing_history.parquet", ListingEvent),
            ("index", "index_history.parquet", IndexEvent),
        ]

        for key, filename, model_class in check_targets:
            try:
                df = self.cm.hf.load_parquet(key)
                if df.empty:
                    logger.info(f"{filename} is empty. Skipping schema check.")
                    continue

                model_fields = set(model_class.model_fields.keys())
                parquet_columns = set(df.columns)

                missing_in_parquet = model_fields - parquet_columns
                extra_in_parquet = parquet_columns - model_fields

                if missing_in_parquet:
                    self._report_anomaly(
                        "Layer1_Schema",
                        f"{filename} is missing fields defined in {model_class.__name__}",
                        details=list(missing_in_parquet),
                    )

                if extra_in_parquet:
                    self._report_anomaly(
                        "Layer1_Schema",
                        f"{filename} has ghost fields not defined in {model_class.__name__}",
                        details=list(extra_in_parquet),
                    )

                if not missing_in_parquet and not extra_in_parquet:
                    logger.info(f"✅ {filename} matches {model_class.__name__} perfectly.")
            except Exception as e:
                self._report_anomaly("Layer1_Schema", f"Failed to verify schema for {filename}: {e}")

    # ==========================================
    # Layer 2: Physical Asset Reconciliation
    # ==========================================
    def reconcile_physical_assets(self, sample_size: int = 50):
        """物理ファイル(ZIP/PDF)の存在と整合性を検証"""
        logger.info("--- [Layer 2] Physical Asset Reconciliation ---")

        catalog_df = self.cm.catalog_df
        if catalog_df.empty:
            logger.info("Catalog is empty. Skipping physical checks.")
            return

        try:
            # Hugging Face の raw ディレクトリ配下のファイルリストを取得
            files = self.cm.hf.api.list_repo_files(repo_id=self.hf_repo, repo_type="dataset")
            raw_files = set([f for f in files if f.startswith("raw/edinet/")])

            # 存在すべきファイルの導出
            expected_zips = {}
            expected_pdfs = {}

            for _, row in catalog_df.iterrows():
                doc_id = row["doc_id"]
                submit_date_str = row.get("submit_at")
                if not submit_date_str:
                    continue

                # ユーティリティ関数による共通パス導出 (SSOT)
                dt = pd.to_datetime(submit_date_str)
                base_dir = utils.get_edinet_repo_path(dt)

                if str(row.get("xbrl_flag", "0")) == "1" or row.get("raw_zip_path"):
                    expected_zips[doc_id] = f"{base_dir}/zip/{doc_id}.zip"

                if str(row.get("pdf_flag", "0")) == "1" or row.get("pdf_path"):
                    expected_pdfs[doc_id] = f"{base_dir}/pdf/{doc_id}.pdf"

            # 存在確認 (Existence check)
            missing_zips = [doc_id for doc_id, path in expected_zips.items() if path not in raw_files]
            missing_pdfs = [doc_id for doc_id, path in expected_pdfs.items() if path not in raw_files]

            if missing_zips:
                self._report_anomaly(
                    "Layer2_Physical",
                    f"{len(missing_zips)} expected ZIP files are missing from HF storage.",
                    details=missing_zips[:10],
                )
            if missing_pdfs:
                self._report_anomaly(
                    "Layer2_Physical",
                    f"{len(missing_pdfs)} expected PDF files are missing from HF storage.",
                    details=missing_pdfs[:10],
                )

            logger.info(
                f"Verified existence of {len(expected_zips) - len(missing_zips)} ZIPs "
                f"and {len(expected_pdfs) - len(missing_pdfs)} PDFs."
            )

            # ランダムサンプリングによる ZIP 破損チェック (Integrity check)
            if expected_zips:
                import random

                from huggingface_hub import hf_hub_download

                available_zips = [path for path in expected_zips.values() if path in raw_files]
                sample_paths = random.sample(available_zips, min(sample_size, len(available_zips)))

                logger.info(f"Running deep CRC integrity check on {len(sample_paths)} sampled ZIP files...")
                corrupted = []
                for p in sample_paths:
                    try:
                        local_p = hf_hub_download(
                            repo_id=self.hf_repo, filename=p, repo_type="dataset", token=self.hf_token
                        )
                        with zipfile.ZipFile(local_p) as z:
                            # testzip() は破損ファイルの最初の名前を返し、正常なら None を返す
                            bad_file = z.testzip()
                            if bad_file:
                                corrupted.append((p, f"Bad inner file: {bad_file}"))
                    except Exception as e:
                        corrupted.append((p, str(e)))

                if corrupted:
                    self._report_anomaly(
                        "Layer2_Physical", f"Found {len(corrupted)} corrupted ZIP files.", details=corrupted
                    )
                else:
                    logger.info(f"✅ Deep CRC check passed for all {len(sample_paths)} sampled ZIP files.")

        except Exception as e:
            self._report_anomaly("Layer2_Physical", f"Physical reconciliation failed: {e}")

    # ==========================================
    # Layer 3: Analytical Data Reconciliation
    # ==========================================
    def reconcile_analytical_data(self):
        """分析用Parquetとカタログの整合性 (孤児・重複・Bin)"""
        logger.info("--- [Layer 3] Analytical Data Reconciliation ---")

        try:
            # Binファイル群の取得
            files = self.cm.hf.api.list_repo_files(repo_id=self.hf_repo, repo_type="dataset")
            bin_files = [
                f
                for f in files
                if f.startswith("data/financial_values_bin") or f.startswith("data/qualitative_text_bin")
            ]

            if not bin_files:
                logger.info("No Master Data chunks (Bins) found.")
                return

            all_docs_in_bins = set()
            duplicates = []
            bin_mismatches = []

            # 各 Bin ファイルの監査
            for bf in bin_files:
                try:
                    df = self.cm.hf.load_parquet_lazy(bf)  # 実装次第でメモリに乗せる
                    if df.empty:
                        continue

                    # 期待されるBin名 (ファイル名から抽出: 例 'financial_values_bin12' -> '12')
                    import re

                    match = re.search(r"_bin(\w+)\.parquet", bf)
                    expected_bin = match.group(1) if match else "Unknown"

                    # 1. 重複チェック
                    if "doc_id" in df.columns and "key" in df.columns:
                        # context_ref がある場合はそれも一意キーに含める
                        dup_keys = ["doc_id", "key"]
                        if "context_ref" in df.columns:
                            dup_keys.append("context_ref")

                        dups = df[df.duplicated(subset=dup_keys, keep=False)]
                        if not dups.empty:
                            duplicates.append((bf, len(dups)))

                    # 2. Bin アサインメントの正確性
                    if "code" in df.columns:
                        # 妥当な証券コードを持つレコードで、先頭2桁がBinと異なるものを探す
                        valid_codes = df[df["code"].notna() & (df["code"] != "")]
                        if not valid_codes.empty:
                            mismatched = valid_codes[valid_codes["code"].astype(str).str[:2] != str(expected_bin)]
                            if not mismatched.empty:
                                bin_mismatches.append((bf, len(mismatched)))

                    # 孤児チェック用に doc_id を収集
                    if "doc_id" in df.columns:
                        all_docs_in_bins.update(df["doc_id"].dropna().unique())

                except Exception as e:
                    self._report_anomaly("Layer3_Analytical", f"Failed to audit bin {bf}: {e}")

            if duplicates:
                self._report_anomaly(
                    "Layer3_Analytical",
                    f"Detected duplicate records in {len(duplicates)} physical bin files.",
                    details=duplicates,
                )
            else:
                logger.info("✅ No duplicate records found in any bins.")

            if bin_mismatches:
                self._report_anomaly(
                    "Layer3_Analytical",
                    f"Detected bin assignment mismatches in {len(bin_mismatches)} files.",
                    details=bin_mismatches,
                )
            else:
                logger.info("✅ All records are correctly assigned to their respective physical bins.")

            # 3. 孤児データチェック (Catalog <-> Master Data)
            catalog_docs = set(self.cm.catalog_df["doc_id"].dropna().unique())

            orphans_in_bins = all_docs_in_bins - catalog_docs
            if orphans_in_bins:
                self._report_anomaly(
                    "Layer3_Analytical",
                    f"Detected {len(orphans_in_bins)} orphan docs in Master Bins (Not in Catalog).",
                    details=list(orphans_in_bins)[:10],
                )

        except Exception as e:
            self._report_anomaly("Layer3_Analytical", f"Analytical reconciliation failed: {e}")

    # ==========================================
    # Layer 4: API Catalog Reconciliation
    # ==========================================
    def reconcile_api_catalog(self, days_to_check: int = 5):
        """直近N日間のAPI応答とカタログを照合"""
        logger.info(f"--- [Layer 4] API Catalog Reconciliation (Last {days_to_check} days) ---")

        catalog_df = self.cm.catalog_df
        if catalog_df.empty:
            logger.info("Catalog is empty.")
            return

        api_key = os.getenv("EDINET_API_KEY")
        if not api_key:
            logger.error("EDINET_API_KEY environment variable is missing for Layer 4.")
            return

        from data_engine.engines.edinet_engine import EdinetEngine

        edinet = EdinetEngine(api_key, self.data_path)

        start_date = (datetime.now() - timedelta(days=days_to_check)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        try:
            # APIから最新メタデータを取得
            api_meta = edinet.fetch_metadata(start_date, end_date)
            if not api_meta:
                logger.info("No API metadata found for comparison.")
                return

            api_dict = {row["docID"]: row for row in api_meta}
            df_subset = catalog_df[catalog_df["doc_id"].isin(api_dict.keys())]

            mismatches = []
            # 照合項目：API由来であって、ARIAが内部で生成・加工しない「生の属性」
            check_fields = [
                ("submit_at", "submitDateTime"),
                ("doc_type", "docTypeCode"),
                ("ordinance_code", "ordinanceCode"),
                ("form_code", "formCode"),
                ("withdrawal_status", "withdrawalStatus"),
            ]

            for _, row in df_subset.iterrows():
                doc_id = row["doc_id"]
                api_row = api_dict[doc_id]

                for local_f, api_f in check_fields:
                    local_val = str(row.get(local_f) or "").strip()
                    api_val = str(api_row.get(api_f) or "").strip()

                    if local_val != api_val:
                        # withdrawalStatus は None と "0" を等価扱いする
                        if local_f == "withdrawal_status" and (
                            local_val in ["", "0", "None"] and api_val in ["", "0", "None"]
                        ):
                            continue

                        mismatches.append(
                            {"doc_id": doc_id, "field": local_f, "local_value": local_val, "api_value": api_val}
                        )

            if mismatches:
                self._report_anomaly(
                    "Layer4_Catalog",
                    f"Detected {len(mismatches)} metadata drift(s) against FSA EDINET API.",
                    details=mismatches[:20],
                )
            else:
                logger.info("✅ Catalog is perfectly synchronized with EDINET API.")

        except Exception as e:
            self._report_anomaly("Layer4_Catalog", f"API reconciliation failed: {e}")

    def run_full_audit(self) -> Dict[str, Any]:
        """全レイヤーの監査を実行し結果レポートを生成"""
        logger.info("Starting Extreme Integrity Audit (Data Reconciliation)...")

        self.reconcile_schemas()
        self.reconcile_physical_assets()
        self.reconcile_analytical_data()
        self.reconcile_api_catalog()

        total_anomalies = sum(len(v) for v in self.anomalies.values())

        report = {
            "timestamp": datetime.now().isoformat(),
            "status": "FAILED" if total_anomalies > 0 else "PASSED",
            "total_anomalies": total_anomalies,
            "details": self.anomalies,
        }

        if total_anomalies > 0:
            logger.error(f"Integrity Audit detected {total_anomalies} anomalies. See report for details.")
        else:
            logger.info("INTEGRITY AUDIT PASSED: Zero anomalies detected in the foundation.")

        return report


def main():
    import argparse
    import os

    from dotenv import load_dotenv

    load_dotenv()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--save-report", action="store_true", help="Save the reconciliation report to JSON")
    args = parser.parse_args()

    hf_repo = os.getenv("HF_REPO")
    hf_token = os.getenv("HF_TOKEN")

    if not hf_repo or not hf_token:
        logger.critical("HF_REPO and HF_TOKEN are required.")
        sys.exit(1)

    engine = DataReconciliationEngine(hf_repo, hf_token, Path("data"))
    report = engine.run_full_audit()

    if args.save_report:
        report_path = Path("logs/reconciliation_report.json")
        report_path.parent.mkdir(exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"Report saved to {report_path}")

    # GitHub Actions で異常を検知させるための Exit Code (Fail Fast)
    if report["status"] == "FAILED":
        sys.exit(2)


if __name__ == "__main__":
    main()
