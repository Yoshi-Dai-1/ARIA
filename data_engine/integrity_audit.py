import argparse
import os
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

from catalog_manager import CatalogManager
from edinet_xbrl_prep.edinet_xbrl_prep.edinet_api import request_term
from huggingface_hub import HfApi
from loguru import logger
from network_utils import patch_all_networking
from utils import get_edinet_repo_path


class ExtremeIntegrityAuditor:
    def __init__(self):
        # 全体的な通信の堅牢化を適用
        patch_all_networking()

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

    def run_full_audit(self, start_date_str=None, end_date_str=None):
        if start_date_str and end_date_str:
            start_date, end_date = start_date_str, end_date_str
        else:
            start_date, end_date = self.calculate_audit_period()

        logger.info(f"=== Extreme Integrity Audit Started ({start_date} to {end_date}) ===")

        # 1. EDINET API から正解値（Ground Truth）を一括取得
        # (imports moved to top)

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

                # 同期チェックすべき重要項目
                check_map = {
                    "company_name": truth.filerName,
                    "submit_at": truth.submitDateTime,
                    "edinet_code": truth.edinetCode,
                    "jcn": truth.JCN,
                    "form_code": truth.formCode,
                    "doc_type": truth.docTypeCode,
                }

                for col, val in check_map.items():
                    target_val = target.get(col)
                    # datetime オブジェクトの場合は文字列に変換して比較 (APIは文字列形式)
                    if hasattr(target_val, "strftime"):
                        # EDINETの標準形式 (YYYY-MM-DD HH:MM) に合わせる
                        target_val = target_val.strftime("%Y-%m-%d %H:%M")

                    # 期待値(val)も None なら空文字として扱う
                    s_target = str(target_val) if target_val is not None else ""
                    s_truth = str(val) if val is not None else ""

                    if s_target != s_truth:
                        mismatches.append(f"{col}: {s_target} != {s_truth}")

                if mismatches:
                    logger.error(f"❌ [METADATA-MISMATCH] {doc_id}: {', '.join(mismatches)}")

                # 物理ファイルチェック
                # Partitioned Path (year/month/day) を正確に生成
                zip_path = get_edinet_repo_path(doc_id, truth.submitDateTime, suffix="zip")
                if truth.xbrlFlag == "1" and zip_path not in repo_files:
                    logger.error(f"❌ [FILE-MISSING] {zip_path} is missing in repository!")

            current_date_dt += timedelta(days=1)

        logger.info("Audit completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARIA Extreme Integrity Auditor")
    parser.add_argument("--start", type=str, help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End Date (YYYY-MM-DD)")
    args = parser.parse_args()

    auditor = ExtremeIntegrityAuditor()
    auditor.run_full_audit(args.start, args.end)
