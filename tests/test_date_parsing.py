import sys
from datetime import datetime
from pathlib import Path

# Add data_engine to sys.path
sys.path.append(str(Path(__file__).parent.parent / "data_engine"))

from main import parse_datetime


def test_parse_datetime():
    cases = [
        ("2022-10-03 09:17:45", datetime(2022, 10, 3, 9, 17, 45)),
        ("2022-10-03 09:17", datetime(2022, 10, 3, 9, 17)),
        ("2022-10-03", datetime(2022, 10, 3)),
        ("", None),  # Should return current time, let's just check it doesn't crash
        (None, None),
    ]

    for dt_str, expected in cases:
        try:
            result = parse_datetime(dt_str)
            if expected:
                assert result == expected, f"Failed for {dt_str}: expected {expected}, got {result}"
            print(f"✅ Success: '{dt_str}' -> {result}")
        except Exception as e:
            print(f"❌ Failed for {dt_str}: {e}")
            sys.exit(1)


if __name__ == "__main__":
    test_parse_datetime()
