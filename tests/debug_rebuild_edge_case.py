import pandas as pd


def simulate_rebuild_logic_v1(all_codes, name_history_df, new_events_list):
    """
    Current implementation logic simulation
    """
    # ... inside loop, we populate new_events_list ...

    # Logic in catalog_manager.py:
    if new_events_list:
        rebuilt_codes = set(e["code"] for e in new_events_list)

        if not name_history_df.empty:
            # Only remove if we have NEW events
            filtered_history = name_history_df[~name_history_df["code"].isin(rebuilt_codes)]
            return pd.concat([filtered_history, pd.DataFrame(new_events_list)], ignore_index=True)

    return name_history_df


def test_zero_change_clearing():
    print("--- Testing Zero-Change Clearing Bug ---")

    # Scenario:
    # Existing History (Bad): Code 9999 changed from "B" to "A" (Time travel artifact)
    # Rebuild Truth: Code 9999 has always been "A". No changes.

    name_history = pd.DataFrame(
        [{"code": "9999", "old_name": "Company B", "new_name": "Company A", "change_date": "2023-01-01"}]
    )

    # The rebuild loop runs...
    # It sees: 2022: A, 2023: A.
    # It generates NO events because name never changed.
    new_events = []

    # We processed code 9999, but new_events is empty.

    # Run simulation
    all_processed_codes = {"9999"}
    result = simulate_rebuild_logic_v1(all_processed_codes, name_history, new_events)

    print("Result History:\n", result)

    if not result.empty and result.iloc[0]["code"] == "9999":
        print("❌ BUG CONFIRMED: Bad history for 9999 remains because no new events were generated.")
    else:
        print("✅ SUCCESS: Bad history cleared.")


if __name__ == "__main__":
    test_zero_change_clearing()
