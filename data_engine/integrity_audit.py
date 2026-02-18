import os
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

# プロジェクトルートの追加 (インポート前に実行)
project_root = Path(__file__).parent.parent
if str(project_root / "data_engine") not in sys.path:
    sys.path.insert(0, str(project_root / "data_engine"))

from catalog_manager import CatalogManager  # noqa: E402
from edinet_xbrl_prep.edinet_api import request_term  # noqa: E402
from huggingface_hub import HfApi  # noqa: E402
from loguru import logger  # noqa: E402
from metadata_transformer import MetadataTransformer  # noqa: E402


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

                # 同期チェックすべき重要項目 (MetadataTransformer経由で完全一致検証)
                # 監査ロジック:
                # 1. APIの生データ (truth) を MetadataTransformer に通して "あるべきレコード (expected)" を生成
                # 2. カタログの実際のレコード (target) と比較
                # ※ zip_ok, pdf_ok は監査では不明な場合があるが、メタデータ比較においては影響が少ない項目は除外または実ファイル確認結果を使用可能
                # ここでは純粋なメタデータフィールドを比較する

                expected = MetadataTransformer.transform(
                    row=truth.model_dump(),  # Pydantic model to dict
                    docid=doc_id,
                    title=truth.docDescription,
                    zip_ok=False,  # メタデータ比較用ダミー (パス比較で除外)
                    pdf_ok=False,  # メタデータ比較用ダミー
                    raw_base_dir=None,
                )

                # 比較対象カラム (パスやステータス以外)
                compare_cols = [
                    "company_name",
                    "submit_at",
                    "edinet_code",
                    "jcn",
                    "form_code",
                    "doc_type",
                    "issuer_edinet_code",
                    "fund_code",
                    "ordinance_code",
                    "period_start",
                    "period_end",
                    "title",
                    "parent_doc_id",
                    "withdrawal_status",
                    "disclosure_status",
                    "current_report_reason",
                    "fiscal_year",
                    "num_months",
                    "is_amendment",
                    "code",
                ]

                check_map = {k: expected[k] for k in compare_cols}

                for col, truth_val in check_map.items():
                    cat_val = target.get(col)

                    # None と "None" 文字列のゆらぎ吸収 (CatalogはParquet経由でNoneがNaNやNoneになる)
                    # 文字列化して比較
                    tv_str = str(truth_val) if truth_val is not None else "None"
                    cv_str = str(cat_val) if cat_val is not None and str(cat_val) != "nan" else "None"

                    # 空文字とNoneの等価性
                    if tv_str == "" and cv_str == "None":
                        continue
                    if tv_str == "None" and cv_str == "":
                        continue

                    if tv_str != cv_str:
                        mismatches.append(f"{col}: {cv_str} != {tv_str}")

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
