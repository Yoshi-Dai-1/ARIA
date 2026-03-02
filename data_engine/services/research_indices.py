import io

import pandas as pd
import requests


def inspect_csv(url, name):
    print(f"\n=== Inspecting {name} ===")
    print(f"URL: {url}")
    try:
        r = requests.get(url)
        r.raise_for_status()
        content = r.content.decode("shift_jis", errors="ignore")  # Most Nikkei/JPX CSVs are Shift-JIS

        print("--- First 15 lines of raw content ---")
        lines = content.splitlines()
        for i, line in enumerate(lines[:15]):
            print(f"{i}: {line}")

        print("\n--- Attempting simple parsing ---")
        # Try finding header row
        header_row = 0
        for i, line in enumerate(lines[:10]):
            if "コード" in line or "Code" in line:
                header_row = i
                break

        print(f"Detected header row: {header_row}")
        df = pd.read_csv(io.StringIO(content), header=header_row)
        print("Columns:", df.columns.tolist())
        print(df.head(3))

    except Exception as e:
        print(f"Error inspecting {name}: {e}")


indices = [
    (
        "Nikkei High Dividend 50",
        "https://indexes.nikkei.co.jp/nkave/archives/file/nikkei_high_dividend_yield_50_weight_jp.csv",
    ),
    ("JPX-Nikkei 400", "https://indexes.nikkei.co.jp/nkave/archives/file/jpx_nikkei_index_400_weight_jp.csv"),
    ("JPX-Nikkei Mid Small", "https://indexes.nikkei.co.jp/nkave/archives/file/jpx_nikkei_mid_small_weight_jp.csv"),
]

for name, url in indices:
    inspect_csv(url, name)
