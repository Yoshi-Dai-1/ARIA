#!/usr/bin/env python3
"""
証券コード 5 桁正規化スクリプト (ARIA プロジェクト標準)
data_engine/utils.py の実装に基づき、Excel由来の .0 や None/NaN を考慮。
"""

import sys

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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 normalize_code.py <stock_code>")
        sys.exit(1)

    for arg in sys.argv[1:]:
        print(normalize_code(arg))
