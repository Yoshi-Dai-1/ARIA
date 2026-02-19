# 正規化・リコンシリエーション詳細 (Reconciliation Logic)

ARIA のデータ整合性を支える、物理実装に基づいた厳格なロジックです。

## 1. 証券コード正規化プロトコル
[normalize_code.py](../scripts/normalize_code.py) を使用し、すべての入力ソースからのコードを 5 桁に統一する。

## 2. 属性解決の優先順位 (`resolve_attr`)
[CatalogManager.update_stocks_master](file:///Users/yoshi_dai/repos/ARIA/data_engine/catalog_manager.py#L674) の実装に基づき、以下の優先順位で銘柄属性（セクター、市場等）を確定させる。

1.  **JPX Master (プライマリ)**: 市場属性の絶対的な正解。EDINET の属性よりも優先される。
2.  **EDINET / Catalog (セカンダリ)**: 新規上場時など JPX 未登録の場合のみ一時的に参照。
3.  **Existing Master (フォールバック)**: 上記が NULL の場合のみ、過去の有効な値を継承する。

**禁忌:** JPX に一度も登録されたことがない銘柄（完全新規上場前など）は、JPX による承認（同期）があるまで、`is_active`, `sector`, `market` を `Unknown` (None) として隔離管理する。

## 3. 上場イベントの命名規則
- `LISTING`: システムによる初回検知時。
- `DELISTING`: 前回の `is_active=True` 銘柄が物理的に消失した時。
- `RE_LISTING`: 過去に `DELISTING` 済みのコードが再出現した時。
