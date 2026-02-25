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
- **ARIA_SCOPE**: 目的（上場、非上場、全量）に応じたフィルタリング・抽出
- **JCN 主導の不変ビン分割**: 法人番号 (JCN) を物理パスの分散キーに使用し、証券コードや EDINET コードの変更に左右されない永続的なデータ配置を実現
- **EDINET コード集約ブリッジ**: 合併等でコードが変更されても、金融庁公式の集約一覧 (ESE140190.csv) を用いて歴史を自動で繋ぐ名寄せ機能
- **API V2 増分同期**: `opeDateTime` パラメータを活用し、差分のみを効率的に取得
- **Market Data Pipeline**: EDINET とは独立した市場データ収集エンジン (Nikkei 225, TOPIX 対応)
- **Hugging Face Integration**: 単一リポジトリでの効率的なデータレイク管理

## アーキテクチャ (Monorepo)

本プロジェクトは **Monorepo** 構成を採用しており、データ収集エンジンと Web フロントエンドが分かれています。

### 1. Data Engine (`data_engine/`)
Python による堅牢なデータ処理基盤。
- **Unified Harvester (`edinet_harvester.yml`)**: 開示書類の並列収集・解析 (Worker/Merger)。本日分と歴史分を同時に走査。
- **Backfill Manager (`backfill_manager.py`)**: 2016年2月15日からの過去データ取得管理。7日単位の増分管理。
- **Market Data Pipeline (`market_main.py`)**: 市場データと銘柄属性の同期。
- **Catalog Manager (`catalog_manager.py`)**: EDINET コードリスト同期、集約ブリッジ、上場生死判定、名寄せ。
- **Master Merger (`master_merger.py`)**: JCN 主尾不変分割による物理データ配置。

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
- **ARIA_SCOPE Settings**: `All` (デフォルト), `Listed`, `Unlisted` の切り替えに対応。
- **Zero Drift Architecture**: Discovery ジョブが取得したメタデータを Artifact として共有し、全 Worker が同一のメタデータを使用。
- **Status Sync (取下検知)**: 最新 API で「取下」が検知された場合、カタログを遡及的に更新。
- **Self-healing (Auto-retry)**: 過去の失敗・未完了書類を自動救済。

### 3. 市場データ収集

毎日 06:30 JST に自動実行され、前日分のデータを取得します。

```bash
PYTHONPATH=data_engine python data_engine/market_main.py --mode all
```

## ライセンス

MIT License

## 貢献

プルリクエストを歓迎します。大きな変更の場合は、まず issue を開いて変更内容を議論してください。

## 免責事項

このプロジェクトは投資判断の参考情報を提供するものであり、投資助言ではありません。投資は自己責任で行ってください。
