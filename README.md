# ARIA - Analytics & Research Intelligence Assistant

**ARIA** (アリア) は、日本の金融市場データを自動収集・分析するための完全無料のデータレイクハウスプラットフォームです。

## プロジェクト名の由来

**ARIA** は以下の頭文字から命名されました:
- **A**nalytics: データ分析
- **R**esearch: 投資調査
- **I**ntelligence: 知的情報処理
- **A**ssistant: 投資判断の支援

また、音楽用語の「アリア（詠唱）」にも由来しており、複雑な金融データを美しく調和のとれた洞察へと変換する、という理念を表現しています。

## 特徴

- **完全無料・永続自動蓄積**: GitHub Actions による毎日の自動収集で、一切のコストなくデータを蓄積し続けます
- **Work Class Robustness**: 中間デルタファイルと Master First 戦略による完全なデータ整合性
- **Unified Harvester**: 「今日」のリアルタイム監視と「過去」の歴史遡及を1つのパイプラインで並行処理
- **ARIA_SCOPE**: `aria_config.json` による一元管理（Listed, Unlisted, All）。目的に応じたフィルタリング・抽出を、設定ファイル1箇所の変更で全パイプラインに適用
- **JCN 主導の不変ビン分割**: 法人番号 (JCN) を物理パスの分散キーに使用し、証券コードや EDINET コードの変更に左右されない永続的なデータ配置を実現
- **EDINET コード集約ブリッジ**: 合併等でコードが変更されても、金融庁公式の集約一覧 (ESE140190.csv) を用いて歴史を自動で繋ぐ名寄せ機能
- **API V2 増分同期**: `opeDateTime` パラメータを活用し、差分のみを効率的に取得
- **Market Data Pipeline**: EDINET とは独立した市場データ収集エンジン (Nikkei 225, TOPIX 対応)
- **Hugging Face Integration**: 単一リポジトリでの効率的なデータレイク管理

## アーキテクチャ (Monorepo)

本プロジェクトは **Monorepo** 構成を採用しており、データ収集エンジンと Web フロントエンドが分かれています。

### 1. Data Engine (`data_engine/`)
Python による堅牢なデータ処理基盤。Facade パターンと単一責任原則に基づくモジュール構成。
- **Unified Harvester (`edinet_harvester.yml`)**: 開示書類の並列収集・解析 (Worker/Merger)。本日分と歴史分を同時に走査。
- **Pipeline (`pipeline.py`)**: Worker/Merger パイプラインの実行エンジン。`main.py` (CLIエントリーポイント) から呼び出される。
- **Catalog Manager (`catalog_manager.py`)**: Facade として以下の専門モジュールをオーケストレーション:
  - `hf_storage.py`: Hugging Face I/O 層（Parquet 読み書き、バッチコミット）
  - `delta_manager.py`: Worker/Merger 間のデルタファイル管理
  - `reconciliation_engine.py`: EDINET コード名寄せ、IPO 自動発見、スコープフィルタリング
- **Data Reconciliation (`data_reconciliation.py`)**: モデル駆動型 4 層 11 項目の自動監査エンジン (スキーマ / 物理ファイル / 分析マスタ / API カタログ)
- **Backfill Manager (`backfill_manager.py`)**: 2016年2月15日からの過去データ取得管理。7日単位の増分管理。
- **Market Data Pipeline (`market_main.py`)**: 市場データと銘柄属性の同期。
- **Master Merger (`master_merger.py`)**: JCN 主導不変分割による物理データ配置。
- **PyArrow Schema Enforcement**: `models.py` の Pydantic モデルから PyArrow スキーマを自動導出 (`pydantic_to_pyarrow()`) し、全 Parquet 書き出しに適用。10年間の無人運転でも型ブレ (Schema Drift) が物理的に発生しない SSOT 設計。

### 2. Web Frontend (`web_frontend/`)
Vite + React による投資分析ダッシュボード（開発中）。

## データ構造

Hugging Face 上のデータ構造はスケーラビリティと不変性を考慮して設計されています。

```
financial-lakehouse/
├── raw/                            # 生データ（ZIP, PDF）
│   └── edinet/
│       └── year=YYYY/month=MM/day=DD/
│           ├── zip/                # ZIPアーカイブ（1万ファイル制限対応）
│           └── pdf/                # PDF書類（1万ファイル制限対応）
├── catalog/                        # ドキュメントインデックス
│   └── documents_index.parquet     # 30カラム構成
├── meta/                           # メタデータ
│   ├── stocks_master.parquet       # 銘柄マスタ (EDINETコードリスト連動)
│   ├── listing_history.parquet     # 上場・廃止・再上場イベント履歴
│   ├── index_history.parquet       # 指数採用・除外イベント履歴
│   ├── name_history.parquet        # 社名変更イベント履歴
│   └── backfill_cursor.json        # バックフィル進行状況
├── master/                         # 分析用マスタデータ
│   ├── financial_values/           # 財務数値（BS, PL, CF, SS）
│   │   └── bin=JXX/data.parquet    # JCN末尾2桁で不変分割
│   ├── qualitative_text/           # 定性情報（注記）
│   │   └── bin=JXX/data.parquet    # JCN末尾2桁で不変分割
│   └── indices/                    # 指数構成データ
│       ├── Nikkei225/
│       │   ├── constituents/year=YYYY/data_YYYYMMDD.parquet
│       │   └── history.parquet
│       └── TOPIX/
│           ├── constituents/year=YYYY/data_YYYYMMDD.parquet
│           └── history.parquet
```

## 使い方

### 環境変数の設定

```bash
EDINET_API_KEY=your_api_key
HF_REPO=[YOUR_USERNAME]/financial-lakehouse
HF_TOKEN=your_huggingface_token
```

### 1. EDINET データ収集 (通常フロー)

基本的には GitHub Actions で自動実行されますが、手動実行も可能です。

```bash
# Workerモード (解析とDelta作成)
PYTHONPATH=data_engine python data_engine/main.py --mode worker --run-id <RUN_ID> --chunk-id <CHUNK_ID> --start 2024-06-01 --end 2024-06-01

# Mergerモード (統合とMaster更新)
PYTHONPATH=data_engine python data_engine/main.py --mode merger --run-id <RUN_ID>
```

### 2. 統合データ収集 (2016年2月15日〜現在)

`edinet_harvester.yml` により、2時間おき（JST偶数時、06時を除く）に自動実行されます。本日の新着書類と、過去の歴史データ（7日刻み）を並列で取得します。
- **Hybrid Ingestion**: 「今」のポーリングと「過去」の遡及を一つのパイプラインで両立。
- **ARIA_SCOPE**: `data_engine/aria_config.json` の `aria_scope` フィールドで制御。`Listed` (上場企業のみ), `Unlisted` (非上場企業のみ), `All` (全量) の切り替えに対応。設定ファイルによる一元管理（SSOT）のため、1箇所の変更で全ワークフロー・全スクリプトに適用されます。
- **Zero Drift Architecture**: Discovery ジョブが取得したメタデータを Artifact として共有し、全 Worker が同一のメタデータを使用。
- **Status Sync (取下検知)**: 最新 API で「取下」が検知された場合、カタログを遡及的に更新。
- **Self-healing (Auto-retry)**: 過去の失敗・未完了書類を自動救済。

### 3. 市場データ収集

毎日 06:30 JST に自動実行され、前日分のデータを取得します。

```bash
PYTHONPATH=data_engine python data_engine/market_main.py --mode all
```

## Licensing & Compliance

ARIA プロジェクトは、以下のライセンスおよび規約を遵守しています。

### 1. ソフトウェアライセンス
- **ARIA Core**: [MIT License](LICENSE) (推奨)
- **依存ライブラリ (`edinet_xbrl_prep`)**: [MIT License](https://github.com/overtheweb/edinet-xbrl-prep)
    - 商用利用・改変・再配布が可能です。利用時は著作権表示およびライセンス全文の掲載が必要です。

### 2. データ利用規約 (法令データ・公共データ)
- **金融庁 EDINET API**: [公共データ利用規約（第1.0版）](https://www.fsa.go.jp/search/opndata.html)
    - **出典記載**: 必須。
    - 記載例：「出典：金融庁 EDINET API を元に ARIA が作成」
    - 商用利用（有料サービス、広告表示等）は公式に許可されています。

### 3. 商用利用時の重要事項（JPX/日経データ）
- **JPX データの制限**: 日本取引所グループのサイトから取得した指数（TOPIX等）や業種データの**商用目的での再配布・提供は禁止**されています。
- **ARIA の対応策**: 
    - ARIA をウェブアプリ等で商用提供する場合、JPXデータに依存しない **"Pure EDINET Mode"** への移行を推奨します。これは、EDINET 自体が提供する公的な属性データのみを使用する設計です。
    - 指数データを扱う場合は、個別のライセンス契約（JPX/日経）を締結するか、ユーザー自身がローカルで実行する「デスクトップツール」としての形式を維持してください。

---
## 貢献

プルリクエストを歓迎します。大きな変更の場合は、まず issue を開いて変更内容を議論してください。

## 免責事項

このプロジェクトは投資判断の参考情報を提供するものであり、投資助言ではありません。投資は自己責任で行ってください。
