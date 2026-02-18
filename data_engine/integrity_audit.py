import os
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

# プロジェクトルートの追加 (インポート前に実行)
project_root = Path(__file__).parent.parent
if str(project_root / "data_engine") not in sys.path:
    sys.path.insert(0, str(project_root / "data_engine"))

from catalog_manager import CatalogManager
from edinet_xbrl_prep.edinet_api import request_term
from huggingface_hub import HfApi
from loguru import logger


class ExtremeIntegrityAuditor:
    def __init__(self):
        self.hf_token = os.getenv("HF_TOKEN")
        self.hf_repo = os.getenv("HF_REPO")
        self.data_path = Path("data").resolve()

        if not self.hf_token or not self.hf_repo:
            logger.critical("HF_TOKEN / HF_REPO が未設定です。")
            sys.exit(1)

        self.api = HfApi(token=self.hf_token)
        self.catalog = CatalogManager(self.hf_repo, self.hf_token, self.data_path)

    def calculate_audit_period(self, days=3652):
        """実行日から10年分を動的に算出"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def check_file_corruption(self, file_path):
        """ZIPファイルの破損チェック"""
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                return zf.testzip() is None
        except Exception:
            return False

    def run_full_audit(self):
        start_date, end_date = self.calculate_audit_period()
        logger.info(f"=== Extreme Integrity Audit Started ({start_date} to {end_date}) ===")

        # 1. EDINET API から正解値（Ground Truth）を一括取得
        api_key = os.getenv("EDINET_API_KEY", "7c32d62d8e9543db88b58e8733e98fbb")

        # 本番運用を想定し、1日ずつチェック（メモリ保護）
        current_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        cat_df = self.catalog.catalog_df
        repo_files = self.api.list_repo_files(repo_id=self.hf_repo, repo_type="dataset")

        while current_date_dt <= end_date_dt:
            date_str = current_date_dt.strftime("%Y-%m-%d")
            logger.info(f"Auditing Date: {date_str}...")

            # APIから正解を取得 (v2)
            try:
                truth_res = request_term(api_key, date_str, date_str)
                if not truth_res:
                    current_date_dt += timedelta(days=1)
                    continue

                truth_list = truth_res[0].data if truth_res else []
            except Exception as e:
                logger.error(f"API Error at {date_str}: {e}")
                current_date_dt += timedelta(days=1)
                continue

            for truth in truth_list:
                doc_id = truth.docID
                # カタログ上のレコードを抽出
                cat_row = cat_df[cat_df["doc_id"] == doc_id]

                if cat_row.empty:
                    if truth.withdrawalStatus == "0":  # 取下済みでないなら致命的な欠落
                        logger.error(f"❌ [MISSING-RECORD] {doc_id} ({truth.filerName}) - Not found in catalog!")
                    continue

                # 37項目相当のメタデータ照合
                # (docID, submitDateTime, edinetCode, secCode, JCN, filerName, etc...)
                # カタログ(26カラム)とAPIレスポンスの対応を確認
                mismatches = []
                target = cat_row.iloc[0]

                # 同期チェックすべき重要項目 (main.py の record 生成ロジックと100%一致させる)
                # Derived Fields (main.py と同じロジックで算出)
                period_end = (truth.periodEnd or "").strip() or None
                fiscal_year = int(period_end[:4]) if period_end else None

                dtc = truth.docTypeCode
                parent_id = truth.parentDocID
                is_amendment = parent_id is not None or str(dtc).endswith("1") or "訂正" in (truth.docDescription or "")

                check_map = {
                    "company_name": (truth.filerName or "").strip() or "Unknown",
                    "submit_at": (truth.submitDateTime or "").strip() or "None",
                    "edinet_code": (truth.edinetCode or "").strip() or "None",
                    "jcn": (truth.JCN or "").strip() or "None",
                    "form_code": (truth.formCode or "").strip() or "None",
                    "doc_type": dtc or "",
                    "issuer_edinet_code": (truth.issuerEdinetCode or "").strip() or "None",
                    "fund_code": (truth.fundCode or "").strip() or "None",
                    "ordinance_code": (truth.ordinanceCode or "").strip() or "None",
                    "period_start": (truth.periodStart or "").strip() or "None",
                    "period_end": period_end or "None",
                    "title": (truth.docDescription or "").strip() or "None",
                    "parent_doc_id": (truth.parentDocID or "").strip() or "None",
                    "withdrawal_status": (truth.withdrawalStatus or "").strip() or "None",
                    "disclosure_status": (truth.disclosureStatus or "").strip() or "None",
                    "current_report_reason": (truth.currentReportReason or "").strip() or "None",
                    "fiscal_year": str(fiscal_year) if fiscal_year else "None",
                    "is_amendment": str(is_amendment),
                }

                for col, truth_val in check_map.items():
                    cat_val = (str(target.get(col)) or "").strip() or "None"
                    # normalize boolean str
                    if col == "is_amendment":
                        cat_val = str(bool(target.get(col)))

                    if str(cat_val) != str(truth_val):
                        mismatches.append(f"{col}: {cat_val} != {truth_val}")

                if mismatches:
                    logger.error(f"❌ [METADATA-MISMATCH] {doc_id}: {', '.join(mismatches)}")

                # 物理ファイルチェック
                zip_path = f"raw/{doc_id}/{doc_id}.zip"
                if truth.xbrlFlag == "1" and zip_path not in repo_files:
                    logger.error(f"❌ [FILE-MISSING] {doc_id}.zip is missing in repository!")

            current_date_dt += timedelta(days=1)

        logger.info("Audit completed.")


if __name__ == "__main__":
    auditor = ExtremeIntegrityAuditor()
    auditor.run_full_audit()
