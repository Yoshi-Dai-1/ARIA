import logging
import shutil
from pathlib import Path

import pandas as pd

from catalog_manager import CatalogManager
from models import CatalogRecord

# ロガー設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_catalog_cleansing():
    print("=== Testing Catalog Cleansing ===")

    # ダミーの「汚染された」データを作成
    # 1. 12カラムしかない (古いスキーマ)
    # 2. 'rec' カラムがある
    # 3. index.name が 'rec'

    dirty_data = [
        {
            "doc_id": "S100TEST",
            "code": "9999",
            "company_name": "Test Entry",
            "submit_at": "2024-01-01",
            "doc_type": "120",
            "pdf_path": "path/to.pdf",
            "raw_zip_path": "path/to.zip",
            "processed_status": "success",
            "source": "EDINET",
            # periodStart, periodEnd, fiscal_year が欠落
            "rec": 1,  # 汚染
        },
        {
            "doc_id": "S100TEST2",
            "code": "8888",
            "company_name": "Test Entry 2",
            "submit_at": "2024-01-02",
            "doc_type": "120",
            "pdf_path": "path/to2.pdf",
            "raw_zip_path": "path/to2.zip",
            "processed_status": "success",
            "source": "EDINET",
            "rec": 2,
        },
    ]

    df_dirty = pd.DataFrame(dirty_data)
    df_dirty.index.name = "rec"

    print(f"Dirty Data Columns: {df_dirty.columns.tolist()}")
    print(f"Dirty Data Index Name: {df_dirty.index.name}")
    print(f"Dirty Data Shape: {df_dirty.shape}")

    # CatalogManagerのモック (HFへの接続はしないが、_clean_dataframeは使える)
    # data_path を一時ディレクトリに
    temp_dir = Path("temp_test_catalog")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()

    # Mock Class Definition
    class MockCatalogManager(CatalogManager):
        def _load_parquet(self, key: str) -> pd.DataFrame:
            # 常に空のDataFrame（正しいカラム構成）を返す
            return pd.DataFrame(columns=list(CatalogRecord.model_fields.keys()))

    # Mockを使って初期化（__init__でのダウンロードも回避される）
    mgr = MockCatalogManager(hf_repo="dummy/repo", hf_token="dummy_token", data_path=temp_dir)
    # api属性をNoneにしておく（リクエスト防止）
    mgr.api = None

    # _load_parquet をモック化（ダウンロード防止）
    # 既存データは無いものとして空DFを返す
    mgr._load_parquet = lambda key: pd.DataFrame(columns=list(CatalogRecord.model_fields.keys()))

    # 1. _clean_dataframe の直接テスト
    print("\n--- Running _clean_dataframe ---")
    df_clean = mgr._clean_dataframe("catalog", df_dirty.copy())

    print(f"Clean Data Columns: {df_clean.columns.tolist()}")
    print(f"Clean Data Index Name: {df_clean.index.name}")
    print(f"Clean Data Shape: {df_clean.shape}")

    # 検証
    expected_cols = list(CatalogRecord.model_fields.keys())

    # rec除去
    if "rec" in df_clean.columns:
        print("❌ FAILED: 'rec' column still exists!")
    else:
        print("✅ SUCCESS: 'rec' column removed.")

    # index名除去
    if df_clean.index.name == "rec":
        print("❌ FAILED: Index name is still 'rec'!")
    else:
        print("✅ SUCCESS: Index name cleared.")

    # カラム数 (35) - CatalogRecord更新後
    expected_count = len(CatalogRecord.model_fields)
    print(f"Expected Column Count: {expected_count}")

    if len(df_clean.columns) == expected_count:
        print(f"✅ SUCCESS: Column count is {expected_count}.")
    else:
        print(f"❌ FAILED: Column count is {len(df_clean.columns)} (Expected {expected_count}).")

    # カラム一致
    if list(df_clean.columns) == expected_cols:
        print("✅ SUCCESS: Column names match CatalogRecord model.")
    else:
        print("❌ FAILED: Column names do not match.")
        print(f"Actual: {df_clean.columns.tolist()}")
        print(f"Expect: {expected_cols}")

    # period_start 等の補完確認
    row = df_clean.iloc[0]
    if "period_start" in row:  # None or NaN
        print(f"✅ SUCCESS: period_start exists (Value: {row['period_start']})")
    else:
        print("❌ FAILED: period_start missing.")

    # 2. _save_and_upload のシミュレーション
    print("\n--- Running _save_and_upload emulation ---")
    # mgr.paths["catalog"] にファイルパスを設定
    mgr.paths = {"catalog": "catalog/documents_index.parquet"}
    (temp_dir / "catalog").mkdir()

    # save
    mgr._save_and_upload("catalog", df_dirty.copy(), defer=True)

    # saved file check
    saved_path = temp_dir / "catalog" / "documents_index.parquet"
    if saved_path.exists():
        df_saved = pd.read_parquet(saved_path)
        print(f"Saved File Columns: {df_saved.columns.tolist()}")
        if "rec" not in df_saved.columns and len(df_saved.columns) == 18:
            print("✅ SUCCESS: Saved file is clean.")
        else:
            print("❌ FAILED: Saved file is dirty.")
    else:
        print("❌ FAILED: File not saved.")

    # Clean up
    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    test_catalog_cleansing()
