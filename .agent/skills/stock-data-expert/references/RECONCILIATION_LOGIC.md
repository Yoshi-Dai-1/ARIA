# 正規化・リコンシリエーション詳細 (Reconciliation Logic)

## 1. 証券コード正規化プロトコル
ARIA では、あらゆる入力ソースからの証券コードを以下の `normalize_code` ロジックで 5 桁に統一する。

```python
def normalize_code(code: str) -> str:
    if not code: return ""
    c = str(code).strip()
    return c + "0" if len(c) == 4 else c
```

### ケーススタディ
- **2593 (伊藤園)** -> `25930`
- **25935 (伊藤園優先株)** -> `25935` (そのまま)
- **9984 (ソフトバンクG)** -> `99840`

## 2. 属性解決の優先順位 (Resolution Priority)
特定の銘柄属性（市場、セクター名など）を確定させる際、以下の順序で非 NULL 値を採用する。
1. **JPX Master**: 最新の市場分類のプライマリソース。
2. **Catalog (EDINET)**: 会社名（商号）の最新ソース。
3. **Existing Master**: 過去に確定済みの属性があればそれを維持（属性継承）。

## 3. 上場イベントの命名規則
- `LISTING`: 初回検知時。
- `DELISTING`: 前回の `is_active=True` 銘柄が消失した時。
- `RE_LISTING`: 一度 `DELISTING` になった銘柄が再出現した時。
