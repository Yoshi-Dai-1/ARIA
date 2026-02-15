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

- **Work Class Robustness**: 中間デルタファイルとMaster First戦略による完全なデータ整合性
- **Parallel Processing**: GitHub Actions Matrixによる高速並列データ収集 (20並列)
- **Market Data Pipeline**: EDINETとは独立した市場データ収集エンジン (Nikkei 225, TOPIX対応)
- **Hugging Face Integration**: データセット `[YOUR_USERNAME]/financial-lakehouse` で公開可能
- **Automated Backfill (2016-Present)**: 2016年2月15日からの全書類を20並列で遡り取得するオートパイロット機能

## アーキテクチャ (Monorepo)

本プロジェクトは **Monorepo** 構成を採用しており、データ収集エンジンとWebフロントエンドが分かれています。

### 1. Data Engine (`data_engine/`)
Pythonによる堅牢なデータ処理基盤。
- **EDINET Data Pipeline (`main.py`)**: 開示書類の並列収集・解析 (Worker/Merger)。
- **Backfill Manager (`backfill_manager.py`)**: 2016年2月15日からの過去データ取得管理。
- **Market Data Pipeline (`market_main.py`)**: 市場データと銘柄属性の同期。

### 2. Web Frontend (`web_frontend/`)
Vite + React による投資分析ダッシュボード（開発中）。

## データ構造

Hugging Face上のデータ構造はスケーラビリティを考慮して設計されています。

```
financial-lakehouse/
├── raw/                            # 生データ（ZIP, PDF）
│   └── edinet/
│       └── year=YYYY/month=MM/day=DD/ # 日次パーティション (1万件制限回避)
├── catalog/                        # ドキュメントインデックス
│   └── documents_index.parquet
├── meta/                           # メタデータ
│   ├── stocks_master.parquet       # 証券コード, 銘柄名, is_activeフラグ
│   ├── listing_history.parquet     # 上場・廃止・再上場イベント履歴
│   ├── index_history.parquet       # 指数採用・除外イベント履歴
│   ├── name_history.parquet        # 社名変更イベント履歴
│   └── backfill_cursor.json        # バックフィル進行状況（次に取得する日付）
├── master/                         # 分析用マスタデータ
│   ├── financial_values/           # 財務数値（BS, PL, CF, SS）
│   │   └── bin=XX/data.parquet     # 証券コード上2桁で不変分割
│   ├── qualitative_text/           # 定性情報（注記）
│   │   └── bin=XX/data.parquet     # 証券コード上2桁で不変分割
│   └── indices/                    # 指数構成データ
│       ├── Nikkei225/
│       │   ├── constituents/year=YYYY/data_YYYYMMDD.parquet  # 日次スナップショット
│       │   └── history.parquet     # 採用・除外イベント履歴
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

### 1. EDINETデータ収集 (通常フロー)

基本的にはGitHub Actionsで自動実行されますが、手動実行も可能です。

```bash
# Workerモード (解析とDelta作成)
PYTHONPATH=data_engine python data_engine/main.py --mode worker --run-id <RUN_ID> --chunk-id <CHUNK_ID> --start 2024-06-01 --end 2024-06-01

# Mergerモード (統合とMaster更新)
PYTHONPATH=data_engine python data_engine/main.py --mode merger --run-id <RUN_ID>
```

### 2. 過去データの自動バックフィル (2016年2月15日〜現在)

`hourly_backfill.yml` により、毎時15分（JST 01:15-05:15, 07:15-23:15）に自動実行されます。2016年2月15日以降の全データを、14日刻みで「過去から現在へ（Forward）」順に **20並列** で取得します。
※2016年2月14日以前のデータについては、APIの制限により取得不可（EDINET Webサイトでの手動検索は可能）。

### 3. 市場データ収集

毎日 06:30 JST に自動実行され、前日分のデータを取得します。

```bash
# 昨日分のデータを取得 (デフォルト)
PYTHONPATH=data_engine python data_engine/market_main.py --mode all

# 特定日を指定して取得 (過去データの補完など)
PYTHONPATH=data_engine python data_engine/market_main.py --mode all --target-date 2024-06-01
```

## ライセンス

MIT License

## 貢献

プルリクエストを歓迎します。大きな変更の場合は、まずissueを開いて変更内容を議論してください。

## 免責事項

このプロジェクトは投資判断の参考情報を提供するものであり、投資助言ではありません。投資は自己責任で行ってください。
