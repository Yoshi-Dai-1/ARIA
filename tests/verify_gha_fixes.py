import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Mocking constants
RAW_BASE_DIR = Path("/tmp/aria_test/raw")


def normalize_code(code: str) -> str:
    """証券コードを 5 桁に正規化する (4桁なら末尾0付与、5桁なら維持)"""
    if not code:
        return ""
    c = str(code).strip()
    return c + "0" if len(c) == 4 else c


def test_sec_code_logic():
    print("--- Testing secCode Logic ---")

    # Mock data
    row_success = {"secCode": "1234", "submitDateTime": "2022-06-15 10:00:00"}
    row_fail = {"secCode": None, "submitDateTime": "2022-06-15 10:00:00"}
    row_5digit = {"secCode": "56780", "submitDateTime": "2022-06-15 10:00:00"}

    # Mock DataFrame
    df = pd.DataFrame({"dummy": [1, 2, 3]})

    # Case 1: Standard 4-digit code
    print(f"Testing 4-digit code: {row_success['secCode']}")
    sec_code_meta = row_success.get("secCode", "")
    code_result = normalize_code(sec_code_meta) if sec_code_meta else "99990"
    print(f"Result: {code_result}")
    assert code_result == "12340", f"Expected 12340, got {code_result}"

    # Case 2: 5-digit code
    print(f"Testing 5-digit code: {row_5digit['secCode']}")
    sec_code_meta = row_5digit.get("secCode", "")
    code_result = normalize_code(sec_code_meta) if sec_code_meta else "99990"
    print(f"Result: {code_result}")
    assert code_result == "56780", f"Expected 56780, got {code_result}"

    # Case 3: Missing code (Fall back to None - NULL Architecture)
    print(f"Testing missing code: {row_fail['secCode']}")
    sec_code_meta = row_fail.get("secCode")
    # Logic simulation:
    code_result = normalize_code(sec_code_meta) if (sec_code_meta and str(sec_code_meta).strip()) else None

    print(f"Result: {code_result}")
    assert code_result is None, f"Expected None, got {code_result}"

    # Apply to DF (Simulation)
    df["code"] = code_result
    print(f"DF Column:\n{df['code']}")


def test_path_logic():
    print("\n--- Testing Path Logic ---")
    rows = [
        {"submitDateTime": "2022-06-15 10:00:00", "expected": "year=2022/month=06/day=15"},
        {"submitDateTime": "2024-01-01 10:00:00", "expected": "year=2024/month=01/day=01"},
    ]

    for row in rows:
        submit_date_str = row["submitDateTime"]
        submit_date = datetime.strptime(submit_date_str, "%Y-%m-%d %H:%M:%S")

        save_dir = (
            RAW_BASE_DIR
            / "edinet"
            / f"year={submit_date.year}"
            / f"month={submit_date.month:02d}"
            / f"day={submit_date.day:02d}"
        )

        rel_path = str(save_dir).replace(str(RAW_BASE_DIR) + "/", "")
        # Remove "edinet/" prefix for comparison if needed, or check full end
        print(f"Date: {submit_date_str} -> Path: {rel_path}")
        assert f"edinet/{row['expected']}" in str(save_dir), f"Expected {row['expected']} in {save_dir}"


if __name__ == "__main__":
    try:
        test_sec_code_logic()
        test_path_logic()
        print("\n✅ All Logic Verification Passed!")
    except AssertionError as e:
        print(f"\n❌ Verification Failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        sys.exit(1)
