---
description: Download and sync market data from JPX and EDINET to the local catalog.
---

# 市場データ更新ワークフロー (Market Data Sync Workflow)

このワークフローは、市場マスターデータの最新化、セキュリティコードの5桁正規化、および上場履歴の同期を一括して行う手順を定義する。

## 実行ステップ

### 1. JPX マスターデータの取得
// turbo
- `MarketDataEngine` を使用して JPX から最新の銘柄一覧を取得する。
- 証券コードを 5 桁に正規化（4桁は末尾に `0` を補足）する。

### 2. EDINET メタデータの同期
// turbo
- `main.py` を実行し、直近の EDINET 提出書類メタデータを取得する。
- 新規上場銘柄や商号変更の有無を検知する。

### 3. 上場履歴とカタログの更新
// turbo
- `update_listing_history` を実行し、`RE_LISTING` や `DELISTING` イベントを生成する。
- `CatalogManager` を介して `stocks_master.parquet` と `listing_history.parquet` を保存する。

### 4. 実行結果の検証
- 更新されたデータ件数を報告。
- 異常（想定外の欠損値など）がないか `code-review-specialist` により簡易監査を実施。
