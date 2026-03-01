import os
import sys
from pathlib import Path

import pandas as pd

# Set up paths
sys.path.append(os.getcwd() + "/data_engine")

from catalog_manager import CatalogManager
from market_engine import MarketDataEngine


def verify_log_and_agg_fixes():
    print("--- 1. Testing MarketDataEngine Index Names ---")
    engine = MarketDataEngine(data_path=Path("data"))
    for name, strategy in engine.strategies.items():
        if hasattr(strategy, "index_name"):
            print(f"Index ID: {name:20} -> Display Name: {strategy.index_name}")

    print("\n--- 2. Testing CatalogManager Logging & Aggregation Summary ---")

    # Mocking
    CatalogManager._load_parquet = lambda self, key, force=False: pd.DataFrame(
        columns=["edinet_code", "code", "company_name", "is_active", "former_edinet_codes"]
    )
    CatalogManager.sync_edinet_code_lists = lambda self: ({}, {})
    CatalogManager._save_and_upload = lambda self, key, df, defer=False: True

    cm = CatalogManager(hf_repo="mock", hf_token="mock", data_path=Path("data"), scope="All")

    # Test Discovery Log
    cm._discover_edinet_code("83010", name="日本銀行")

    # Test Aggregation Summary (Total Count)
    # Simulate a master with some aggregations already existing
    cm.master_df = pd.DataFrame(
        [
            {
                "edinet_code": "E1",
                "company_name": "A",
                "code": "10010",
                "is_active": True,
                "former_edinet_codes": "E_OLD1,E_OLD2",
            },
            {
                "edinet_code": "E2",
                "company_name": "B",
                "code": "10020",
                "is_active": True,
                "former_edinet_codes": "E_OLD3",
            },
        ]
    )

    # Manually trigger the log logic from _update_master_from_edinet_codes
    # This will print the success log with "総保持 3件"
    cm.edinet_codes = {}
    cm.aggregation_map = {}
    cm._update_master_from_edinet_codes()


if __name__ == "__main__":
    verify_log_and_agg_fixes()
