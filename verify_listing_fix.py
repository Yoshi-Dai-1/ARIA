import logging
from pathlib import Path

import pandas as pd

from market_engine import MarketDataEngine

# ロガー設定
logging.basicConfig(level=logging.INFO)


def test_listing_initialization():
    print("=== Testing Listing History Initialization Logic ===")

    # ダミーデータ等の準備
    # MarketDataEngineのアタッチ (パスはダミー)
    engine = MarketDataEngine(Path("dummy_data"))

    # Case 1: Masterが存在し、Historyがない場合 (The Bug Case)
    print("\n--- Case 1: Masters exist, History missing (Initial Run or Deleted) ---")

    # Old Master (既存カタログ)
    old_master = pd.DataFrame(
        {
            "code": ["1001", "1002"],
            "company_name": ["Test 1", "Test 2"],
            "sector": ["Tech", "Bank"],
            "market": ["Prime", "Prime"],
            "is_active": [True, True],
        }
    )

    # New Master (最新取得) - 変化なし
    new_master = old_master.copy()

    # Old History - None or Empty
    old_history = pd.DataFrame()  # Empty

    # 実行
    events = engine.update_listing_history(old_master, new_master, old_history)

    print(f"Events Generated: {len(events)}")
    if not events.empty:
        print("Events Head:")
        print(events.head())

        # 検証: 全銘柄がLISTINGとして初期化されていること
        if len(events) == 2 and all(events["type"] == "LISTING"):
            print("✅ SUCCESS: All codes initialized as LISTING events.")
        else:
            print("❌ FAILED: Unexpected events content.")
            print(events)
    else:
        print("❌ FAILED: No events generated (Bug reproduced).")

    # Case 2: Masterが更新された場合 (Normal Update)
    print("\n--- Case 2: New Listing Added (Normal Update) ---")

    # New Master に1件追加
    new_master_2 = pd.DataFrame(
        {
            "code": ["1001", "1002", "1003"],
            "company_name": ["Test 1", "Test 2", "Test 3 New"],
            "sector": ["Tech", "Bank", "Retail"],
            "market": ["Prime", "Prime", "Growth"],
        }
    )

    # Old History (既にデータがある状態)
    old_history_2 = pd.DataFrame(
        {"code": ["1001", "1002"], "type": ["LISTING", "LISTING"], "event_date": ["2020-01-01", "2020-01-01"]}
    )

    events_2 = engine.update_listing_history(old_master, new_master_2, old_history_2)

    print(f"Events Generated: {len(events_2)}")
    if len(events_2) == 1 and events_2.iloc[0]["code"] == "1003":
        print("✅ SUCCESS: New listing detected correctly.")
    else:
        print("❌ FAILED: Normal update logic broken.")


if __name__ == "__main__":
    test_listing_initialization()
