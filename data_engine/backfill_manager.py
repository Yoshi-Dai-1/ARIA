import argparse
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from huggingface_hub import HfApi, hf_hub_download

# 設定
DATA_PATH = Path("data").resolve()
META_DIR = DATA_PATH / "meta"
CURSOR_FILE = "backfill_cursor.json"
HF_REPO = os.getenv("HF_REPO")
HF_TOKEN = os.getenv("HF_TOKEN")
# 1回の遡り期間（14日＝2週間）
BACKFILL_DAYS = 14
# 限界日（これより前はAPIがない）
LIMIT_DATE = date(2014, 4, 1)


def get_jst_today():
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()


def load_cursor():
    """HFからカーソルファイルをダウンロードして読み込む"""
    try:
        META_DIR.mkdir(parents=True, exist_ok=True)
        local_path = hf_hub_download(
            repo_id=HF_REPO,
            filename=f"meta/{CURSOR_FILE}",
            repo_type="dataset",
            token=HF_TOKEN,
            local_dir=str(DATA_PATH),
        )
        with open(local_path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Cursor load failed (First run?): {e}")
        return None


def save_cursor(next_start_date_str):
    """次の開始日（より過去の日付）を保存してアップロード"""
    cursor_data = {"next_target_start": next_start_date_str}
    local_path = META_DIR / CURSOR_FILE

    with open(local_path, "w") as f:
        json.dump(cursor_data, f, indent=2)

    api = HfApi(token=HF_TOKEN)
    try:
        api.upload_file(
            path_or_fileobj=local_path,
            path_in_repo=f"meta/{CURSOR_FILE}",
            repo_id=HF_REPO,
            repo_type="dataset",
            commit_message=f"Update backfill cursor to {next_start_date_str}",
        )
        print(f"Cursor updated: {next_start_date_str}")
    except Exception as e:
        print(f"Failed to upload cursor: {e}")
        # カーソル更新失敗は致命的ではない（再実行されるだけ）が、ログは残す


def calculate_next_period():
    """
    カーソルを確認し、次に取得すべき期間（start_date, end_date）を決定する。
    期間は 'end_date' から 'start_date' へと過去に向かって進む。
    """
    cursor = load_cursor()

    if cursor and "next_target_start" in cursor:
        # カーソルがある場合：その日付から BACKFILL_DAYS 分遡る
        # cursor["next_target_start"] は、前回取得した期間の「前日」になっているはず
        cursor_date = datetime.strptime(cursor["next_target_start"], "%Y-%m-%d").date()
        end_date = cursor_date
    else:
        # 初回：昨日を開始点とする（日次バッチとの重複を避けるため、少し余裕を持つ）
        end_date = get_jst_today() - timedelta(days=1)

    # 限界チェック
    if end_date < LIMIT_DATE:
        print(f"Reached limit date ({LIMIT_DATE}). Backfill complete.")
        return None, None

    # 開始日の計算
    start_date = end_date - timedelta(days=BACKFILL_DAYS - 1)  # end_dateを含めてBACKFILL_DAYS日間

    # 開始日が限界を超えていたらクリップする
    if start_date < LIMIT_DATE:
        start_date = LIMIT_DATE

    return start_date, end_date


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-only", action="store_true", help="Print dates and exit")
    parser.add_argument("--update-cursor", type=str, help="Update cursor to specific date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.update_cursor:
        # カーソル強制更新モード（成功後に呼ばれる想定）
        # 引数で渡された日付の「前日」を次のターゲットにする
        done_date = datetime.strptime(args.update_cursor, "%Y-%m-%d").date()
        next_target = done_date - timedelta(days=1)
        save_cursor(next_target.strftime("%Y-%m-%d"))
        return

    start, end = calculate_next_period()

    if start is None:
        print("FINISHED")  # GHA側で検知するためのキーワード
        return

    print(f"START={start.strftime('%Y-%m-%d')}")
    print(f"END={end.strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    main()
