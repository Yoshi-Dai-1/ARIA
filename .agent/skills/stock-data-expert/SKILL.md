---
name: stock-data-expert
description: 日本の証券データ（JPX/EDINET）のドメイン知識と、ARIA 規格への正規化・整合性管理の専門スキル。
---
# 証券データ・エキスパート (Stock Data Expert)

ARIA における「証券データの真実」を管理するためのドメイン知識とツールを提供します。

## 1. 証券コードの 5 桁正規化と SICC ルール
あらゆる入力ソースからの証券コードを 5 桁に統一し、SICC（証券コード協議会）の採番ルールに基づいて親子関係を定義します。
- **5 桁正規化**: 4 桁コードには末尾に `0` を付与。英文字を含むコード（例：`145A0`）も同様に 5 桁として扱う。
- **親子紐付け (Preferred Stock Linkage)**: 
    - 5 桁目が `0` 以外（5, 6, 7, 8, 3, 4）の銘柄は「種類株」と判定。
    - 上 4 桁が共通する `0` 銘柄（普通株）を `parent_code` として自動的にリンク。
    - 種類株は親銘柄から `edinet_code` や `jcn` を継承することを許可する。

## 2. 属性解決とリコンシリエーション (Hierarchy of Truth)
[CatalogManager.update_stocks_master](file:///Users/yoshi_dai/repos/ARIA/data_engine/catalog_manager.py) の実装に基づき、以下の優先順位を遵守します。
1. **EDINET Document API (Real-time)**: IPO 銘柄の `secCode` から `edinetCode/jcn` を逆引きする最優先ソース。
2. **EDINET / Catalog (Regulatory)**: 会社名および公的ステータスの正解。
3. **JPX Master (Market)**: 業種区分および市場名の補完ソース。

## 3. 登録ガードと IPO 自動発見 (Registration Guard & Discovery)
二重登録（証券コード不明レコードの増殖）を物理的に防ぐため、以下のプロトコルを適用します。
- **IPO Discovery**: JPX にのみ存在する JCN 不明銘柄に対し、直近 30 日間の書類一覧 API をスキャン。`secCode` が一致する書類から識別子を動的に抽出。
- **Registration Guard**: 書類 API でも識別子が発見できない一般事業会社は、マスタへの登録を「保留（スキップ）」する。
- **例外**: ETF / REIT / PRO Market は EDINET 名簿対象外のため、JPX 情報のみでの登録を許可。

## 4. 履歴の自己修復 (History Self-Healing)
- **Status-based History**: `is_active` フラグの遷移に基づき、`LISTING` / `DELISTING` イベントを一貫して生成。
- **0000-00-00 Seed**: 社名変更履歴の起点としてシードレコードを注入。

## 3. 異常検知の閾値 (Anomaly Thresholds)
以下の状態を検知した場合、直ちに処理を停止し整合性を疑います：
- **指数構成銘柄 < 40件**: API エラーまたはデータ欠落の可能性。
- **bin=No への割り当て**: 証券コードの抽出失敗（NULL 非許容）。

## 詳細リファレンス
- [正規化ロジック詳細](references/RECONCILIATION_LOGIC.md)
- [ARIA データスキーマ定義](references/SCHEMA_DEFINITION.md): bin sharding 構造の定義
- [EDINET API V2 物理検証済み事実集](references/EDINET_API_FACTS.md): エンコーディング、レスポンスフィールド、opeDateTime等の実測値
