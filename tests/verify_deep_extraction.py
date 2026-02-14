import shutil
from pathlib import Path

import pandas as pd

from data_engine.main import TEMP_DIR

# 1. 準備 (三井物産 S100LJUR)
docid = "S100LJUR"
raw_zip = Path("/tmp/aria_compare/S100LJUR.zip")
row = {"docID": docid, "secCode": "80310", "submitDateTime": "2021-06-18 12:23", "filerName": "三井物産株式会社"}


# ダミーのタクソノミオブジェクト
class DummyAcc:
    def get_assign_common_label(self):
        return pd.DataFrame()


acc_obj = DummyAcc()


def verify_mitsui():
    print("--- Verifying Mitsui (Deep Extraction) ---")
    if not raw_zip.exists():
        print(f"  Error: Research ZIP {raw_zip} not found. Run research script first.")
        return

    # モックを使用して最低限の実行
    # args = (docid, row, acc_obj, raw_zip, ["_BalanceSheet"], "financial_values")

    # 実行前に TEMP_DIR 内の対象フォルダを確認したいが、parse_worker は即削除するため、
    # 内部のロジックを模倣して確認する
    extract_dir = TEMP_DIR / f"{docid}_financial_values"
    extract_dir.mkdir(parents=True, exist_ok=True)

    from zipfile import ZipFile

    with ZipFile(str(raw_zip)) as zf:
        for member in zf.namelist():
            if "PublicDoc" in member:
                zf.extract(member, extract_dir)

    gla_exists = any("gla" in str(p) for p in extract_dir.rglob("*"))
    print(f"  Deep Extraction Success: {gla_exists}")
    if gla_exists:
        print(f"  Captured: {[str(p.name) for p in extract_dir.rglob('*gla*')]}")

    shutil.rmtree(extract_dir)


if __name__ == "__main__":
    verify_mitsui()
