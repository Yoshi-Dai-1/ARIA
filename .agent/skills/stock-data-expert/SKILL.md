---
name: stock-data-expert
description: 日本の証券データ（JPX/EDINET）のドメイン知識と 5 桁正規化ロジックの専門スキル。証券コードの正規化、市場間のデータ突合、上場判定ロジックが必要な場合に使用します。
---
# 証券データ・エキスパート (Stock Data Expert)

このスキルは、ARIA における「証券データの真実」を管理するためのドメイン知識とツールを提供します。

## 核心的タスク

### 1. 証券コードの 5 桁正規化
ARIA では、すべての証券コードを 5 桁で統一して管理します。
- 4 桁コード（普通株等）: 末尾に `0` を付与。
- 5 桁コード（優先株等）: そのまま維持。

**自動ツール:**
[normalize_code.py](scripts/normalize_code.py) を使用して、確実に正規化を行ってください。
```bash
python3 .agent/skills/stock-data-expert/scripts/normalize_code.py <code>
```

### 2. データ・リコンシリエーション
JPX（市場マスター）と EDINET（最新財務・社名変更）の情報を突合し、データの誠実性を担保します。

## 詳細リファレンス
以下の詳細情報は `references/` ディレクトリを参照してください。

- [正規化・リコンシリエーション詳細](references/RECONCILIATION_LOGIC.md): 複雑な突合ロジック
- [ARIA データスキーマ定義](references/SCHEMA_DEFINITION.md): DB カラム定義と型情報
- **異常検知**: 型劣化の防止や `listing_history` の連続性管理ルール
