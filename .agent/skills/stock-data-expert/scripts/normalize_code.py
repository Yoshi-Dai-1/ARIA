#!/usr/bin/env python3
"""
証券コード 5 桁正規化スクリプト (ARIA プロジェクト標準)
Usage:
    python3 normalize_code.py 1234
    python3 normalize_code.py 12345
"""

import sys


def normalize_code(code: str) -> str:
    """証券コードを 5 桁に正規化する (4桁なら末尾0付与、5桁なら維持)"""
    if not code:
        return ""
    c = str(code).strip()
    return c + "0" if len(c) == 4 else c


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 normalize_code.py <stock_code>")
        sys.exit(1)

    for arg in sys.argv[1:]:
        print(normalize_code(arg))
