import pandas as pd


def normalize_code(code) -> str:
    """
    証券コードを ARIA 規格 (5桁) に正規化した文字列として返す。
    - None / NaN は空文字列 "" に変換。
    - 数値 (float/int) は文字列に変換。
    - "1301.0" のような Excel 由来の .0 サフィックスを除去。
    - 4桁の場合は末尾に "0" を付与して 5 桁化する。
    - すでに 5 桁以上の場合はそのまま返す。
    """
    if code is None or pd.isna(code):
        return ""

    # 文字列化して空白除去
    c = str(code).strip()

    # Excel/Float 由来の ".0" を除去 (例: "1301.0" -> "1301")
    if c.endswith(".0"):
        c = c[:-2]

    # 4桁なら 5 桁化 (日本株の基本ルール)
    if len(c) == 4:
        return c + "0"

    return c


def get_edinet_repo_path(doc_id: str, submit_at: str, suffix: str = "zip") -> str:
    """
    EDINET書類のリポジトリ内パスを生成する (Partitioned Structure)
    例: raw/edinet/year=2024/month=10/day=23/S100BK7G.zip
    """
    if not submit_at or len(str(submit_at)) < 10:
        # 日付不明な場合はフォールバック (基本的には発生しない想定)
        return f"raw/edinet/unknown/{doc_id}.{suffix}"

    # 日付部分のみ抽出 (YYYY-MM-DD)
    d = str(submit_at)[:10]
    try:
        y, m, day = d.split("-")
        return f"raw/edinet/year={y}/month={m}/day={day}/{doc_id}.{suffix}"
    except Exception:
        return f"raw/edinet/unknown/{doc_id}.{suffix}"
