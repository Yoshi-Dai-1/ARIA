from datetime import datetime
from pathlib import Path


def simulate_path_logic(docid, submit_date_str):
    # Mocking basic variables from main.py
    RAW_BASE_DIR = Path("data/raw")

    # parse_datetime mockup
    submit_date = datetime.strptime(submit_date_str, "%Y-%m-%d %H:%M:%S")

    # save_dir logic from main.py
    save_dir = (
        RAW_BASE_DIR
        / "edinet"
        / f"year={submit_date.year}"
        / f"month={submit_date.month:02d}"
        / f"day={submit_date.day:02d}"
    )

    raw_zip = save_dir / f"{docid}.zip"

    # rel_zip_path logic from main.py (after fix)
    # rel_zip_path = str(raw_zip.relative_to(RAW_BASE_DIR.parent))
    # In main.py: raw_zip.relative_to(RAW_BASE_DIR.parent)
    # RAW_BASE_DIR is data/raw, so parent is data.
    # So raw_zip.relative_to(data) should be raw/edinet/year=...

    rel_path = str(raw_zip.relative_to(RAW_BASE_DIR.parent))
    return rel_path


if __name__ == "__main__":
    docid = "S100P91R"
    date_str = "2022-10-03 09:17:45"
    result = simulate_path_logic(docid, date_str)
    print(f"Index Path: {result}")
    expected = "raw/edinet/year=2022/month=10/day=03/S100P91R.zip"
    assert result == expected, f"Expected {expected}, got {result}"
    print("✅ Verification Successful!")
