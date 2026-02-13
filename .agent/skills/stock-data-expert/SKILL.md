---
name: stock-data-expert
description: Specialized expertise in Japanese stock data structures (JPX/EDINET), 5-digit code normalization, and ARIA-specific data reconciliation logic.
---

# 証券データ・エキスパート (Stock Data Expert)

このスキルは、ARIA プロジェクトにおける「データの真実」を司るドメイン知識の集合体です。

## 1. 核心的知識領域 (Core Knowledge)
- **証券コードの 5 桁正規化**: 
    - 普通株等は末尾に `0` を補足。優先株等は 5 桁を維持。
    - 入力ソースに関わらず、プラットフォーム内では常に 5 桁で統一・比較する。
- **ソース間のリコンシリエーション**:
    - **JPX**: 市場属性・セクターのマスター。
    - **EDINET**: 商号変更・財務情報の最新ソース。
    - JPX に存在しない銘柄が EDINET で先行検知された場合の `Unknown` 状態の管理。
- **上場判定ロジック**:
    - `is_active=True` の銘柄のみを消失（廃止）判定の母数とする純化ロジックの遵守。

## 2. データ構造参照
詳細なマッピングルールやスキーマ定義は以下を参照すること。
- [正規化・リコンシリエーション詳細](references/RECONCILIATION_LOGIC.md)
- [ARIA データスキーマ定義](references/SCHEMA_DEFINITION.md)

## 3. 異常検知と対応
- **型劣化の防止**: 論理値（is_active）が文字列に変換されるなどの「汚染」を検知した場合、即座にクレンジング処理を介入させる。
- **履歴の連続性**: `listing_history` における `RE_LISTING` や `DELISTING` のイベント発生条件の厳格な適用。
