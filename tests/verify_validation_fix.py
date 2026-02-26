import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

project_root = Path(__file__).parent.parent.resolve()
sys.path.append(str(project_root / "data_engine"))

# グローバルに HfApi をモック
mock_hf_api_class = MagicMock()
mock_hf_api_instance = mock_hf_api_class.return_value
mock_hf_api_instance.upload_file.return_value = True
mock_hf_api_instance.create_commit.return_value = True

with (
    patch("huggingface_hub.HfApi", mock_hf_api_class),
    patch("huggingface_hub.hf_hub_download", side_effect=lambda **k: str(Path(k.get("filename")).name)),
):
    from catalog_manager import CatalogManager


def test_validation_fix():
    print("\n[START] Verifying Validation Fix for JPX Data")

    test_data_dir = project_root / "temp_test_reconciliation"
    if test_data_dir.exists():
        shutil.rmtree(test_data_dir)
    test_data_dir.mkdir(parents=True)

    try:
        # Mock data resembling JPX master
        jpx_data = pd.DataFrame(
            [
                {"code": "13010", "company_name": "KYOKUYO", "sector_jpx_33": "Fishery", "market": "Prime"},
                {"code": "13050", "company_name": "ETF_TEST", "sector_jpx_33": "Others", "market": "ETF・ETN"},
            ]
        )

        # Instance with mocked HF and mocked loading
        with (
            patch("catalog_manager.HfApi", return_value=mock_hf_api_instance),
            patch("catalog_manager.CatalogManager._load_parquet", return_value=pd.DataFrame()),
        ):
            cm = CatalogManager(hf_repo="mock/repo", hf_token="mock_token", data_path=test_data_dir)

            # This should now NOT log errors
            print("Running update_stocks_master with JPX data (no edinet_code)...")
            success = cm.update_stocks_master(jpx_data)

            assert success, "Reconciliation failed"

            # Check if records were accepted
            master = cm.get_master_df() if hasattr(cm, "get_master_df") else cm.master_df
            print(f"Master size: {len(master)}")
            assert len(master) >= 2

            # Verify edinet_code is None but record exists
            etf_rec = master[master["code"] == "13050"].iloc[0]
            print(f"ETF Record: {etf_rec.to_dict()}")
            # edinet_code can be None or NaN depending on pandas
            assert pd.isna(etf_rec["edinet_code"])
            assert etf_rec["market"] == "ETF・ETN"

            print("\n[SUCCESS] Validation error resolved and JPX data integrated.")
    finally:
        if test_data_dir.exists():
            shutil.rmtree(test_data_dir)


if __name__ == "__main__":
    try:
        test_validation_fix()
    except Exception as e:
        import traceback

        traceback.print_exc()
        print(f"\n[FAILURE] {e}")
        sys.exit(1)
