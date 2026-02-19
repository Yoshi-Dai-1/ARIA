---
name: stock-data-expert
description: 日本の証券データ（JPX/EDINET）のドメイン知識と、ARIA 規格への正規化・整合性管理の専門スキル。
---
# 証券データ・エキスパート (Stock Data Expert)

ARIA における「証券データの真実」を管理するためのドメイン知識とツールを提供します。

## 1. 証券コードの 5 桁正規化
あらゆる入力ソースからの証券コードを 5 桁に統一します。
- **修正ロジック**: [normalize_code.py](scripts/normalize_code.py) を参照。
- **対応範囲**: Excel 由来の `.0` サフィックス、`None/NaN`、4桁コードの 0 埋めに対応。

## 2. 属性解決とリコンシリエーション
[CatalogManager.update_stocks_master](file:///Users/yoshi_dai/repos/ARIA/data_engine/catalog_manager.py#L674) の実装に基づき、以下の優先順位を遵守します。
1. **JPX Master (Primary)**: 市場属性の絶対的な正解。
2. **EDINET / Catalog (Secondary)**: 会社名の最新ソース。
3. **Existing Master (Fallback)**: 過去の有効な値を継承。

## 3. 異常検知の閾値 (Anomaly Thresholds)
以下の状態を検知した場合、直ちに処理を停止し整合性を疑います：
- **指数構成銘柄 < 40件**: API エラーまたはデータ欠落の可能性。
- **bin=No への割り当て**: 証券コードの抽出失敗（NULL 非許容）。

## 詳細リファレンス
- [正規化ロジック詳細](references/RECONCILIATION_LOGIC.md)
- [ARIA データスキーマ定義](references/SCHEMA_DEFINITION.md): bin sharding 構造の定義
