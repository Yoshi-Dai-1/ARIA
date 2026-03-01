import shutil
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

# Define paths
root = Path("/Users/yoshi_dai/repos/ARIA").resolve()
sys.path.append(str(root))
sys.path.append(str(root / "data_engine"))

try:
    from data_engine.catalog_manager import CatalogManager
    from data_engine.models import EdinetCodeRecord

    print("DEBUG: Successfully imported CatalogManager and models")
except ImportError as e:
    print(f"DEBUG: ImportError: {e}")
    sys.exit(1)


def test_audit_logic():
    DATA_PATH = root / "tests" / "temp_audit_test"
    if DATA_PATH.exists():
        shutil.rmtree(DATA_PATH)
    DATA_PATH.mkdir(parents=True, exist_ok=True)

    # Mock environment
    hf_repo = None
    hf_token = None

    # Create valid dummy parquet files
    (DATA_PATH / "catalog").mkdir(exist_ok=True)
    (DATA_PATH / "meta").mkdir(exist_ok=True)

    catalog_path = DATA_PATH / "catalog/documents_index.parquet"
    master_path = DATA_PATH / "meta/stocks_master.parquet"
    listing_path = DATA_PATH / "meta/listing_history.parquet"

    pd.DataFrame(columns=["doc_id"]).to_parquet(catalog_path)
    pd.DataFrame(columns=["edinet_code", "code", "is_active", "is_listed_edinet"]).to_parquet(master_path)
    pd.DataFrame(columns=["code", "type", "event_date"]).to_parquet(listing_path)

    mock_paths = {
        "catalog/documents_index.parquet": str(catalog_path),
        "meta/stocks_master.parquet": str(master_path),
        "meta/listing_history.parquet": str(listing_path),
        "meta/index_history.parquet": str(DATA_PATH / "meta/index_history.parquet"),
        "meta/name_history.parquet": str(DATA_PATH / "meta/name_history.parquet"),
    }

    for p in mock_paths.values():
        if not Path(p).exists():
            pd.DataFrame().to_parquet(p)

    def side_effect(repo_id, filename, **kwargs):
        return mock_paths.get(filename, "missing")

    with patch.object(CatalogManager, "sync_edinet_code_lists", return_value=({}, {})):
        with patch.object(CatalogManager, "_discover_edinet_code", return_value=("E00001", "1234567890123")):
            with patch("data_engine.catalog_manager.hf_hub_download", side_effect=side_effect):
                print("\n--- Phase 1: Testing Listed Scope ---")
                catalog = CatalogManager(hf_repo, hf_token, DATA_PATH, scope="Listed")

                test_data = pd.DataFrame(
                    [
                        {
                            "code": "1332",
                            "company_name": "Nissui",
                            "sector_jpx_33": "-",
                            "is_consolidated": "有",
                            "is_active": True,
                        }
                    ]
                )
                catalog.update_stocks_master(test_data)

                catalog.edinet_codes = {
                    "E00004": EdinetCodeRecord(
                        edinet_code="E00004", submitter_name="Kaneko", is_listed="上場", sec_code="1376"
                    )
                }
                catalog._update_master_from_edinet_codes()

                print("\n--- Phase 2: Testing Unlisted Scope ---")
                catalog_un = CatalogManager(hf_repo, hf_token, DATA_PATH, scope="Unlisted")
                catalog_un.edinet_codes = {
                    "E41521": EdinetCodeRecord(
                        edinet_code="E41521", submitter_name="Haga", is_listed="非上場", sec_code=None
                    ),
                    "E00004": EdinetCodeRecord(
                        edinet_code="E00004", submitter_name="Kaneko", is_listed="上場", sec_code="1376"
                    ),
                }
                catalog_un._update_master_from_edinet_codes()

                master_un = catalog_un.master_df
                assert "E41521" in master_un["edinet_code"].values
                assert "E00004" not in master_un["edinet_code"].values
                print("PASS: Scope Filtering (Unlisted) Verified")

    # Cleanup temp data AFTER test session if desired, but we'll leave it for now to avoid the 10 items issue
    # rm -rf DATA_PATH


if __name__ == "__main__":
    try:
        test_audit_logic()
        print("\n🎯 ALL AUDIT TESTS PASSED")
        # Final cleanup of data directory to keep git clean
        DATA_PATH = Path("/Users/yoshi_dai/repos/ARIA/tests/temp_audit_test")
        if DATA_PATH.exists():
            shutil.rmtree(DATA_PATH)
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
