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
- **Parallel Processing**: GitHub Actions Matrixによる高速並列データ収集 (Worker/Mergerアーキテクチャ)
- **Market Data Pipeline**: EDINETとは独立した市場データ収集エンジン (Nikkei 225, TOPIX対応)
- **Hugging Face Integration**: データセット `[YOUR_USERNAME]/financial-lakehouse` で公開

## アーキテクチャ

本プロジェクトは2つの独立したパイプラインで構成されています。


### 1. EDINET Data Pipeline (`main.py`)
有価証券報告書などの開示書類を収集・解析します。
- **Worker Mode**: 書類をダウンロード・解析し、中間Deltaファイルを作成。
- **Merger Mode**: 全Workerの成果物を統合し、整合性を検証した上でMaster/Catalogを更新。

### 2. Market Data Pipeline (`market_main.py`)
市場データ（株価指数、銘柄マスタ）を管理します。
- **Stock Master**: JPX公式サイトから最新銘柄リストを取得し、上場・廃止・再上場を自動判定。
- **Indices**: 日経225、TOPIXの構成銘柄とウエイトを毎日Snapshotとして保存 (Shift-JIS/403回避対応済)。

## データ構造

Hugging Face上のデータ構造はスケーラビリティを考慮して設計されています。

```
financial-lakehouse/
├── raw/                            # 生データ（ZIP, PDF）
│   └── edinet/
│       └── year=YYYY/month=MM/     # 年月パーティション
├── catalog/                        # ドキュメントインデックス
│   └── documents_index.parquet
├── meta/                           # メタデータ
│   ├── stocks_master.parquet       # 証券コード, 銘柄名, is_activeフラグ
│   ├── listing_history.parquet     # 上場・廃止・再上場イベント履歴
│   ├── index_history.parquet       # 指数採用・除外イベント履歴
│   └── name_history.parquet        # 社名変更イベント履歴
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
python main.py --mode worker --run-id <RUN_ID> --chunk-id <CHUNK_ID> --start 2024-06-01 --end 2024-06-01

# Mergerモード (統合とMaster更新)
python main.py --mode merger --run-id <RUN_ID>
```

### 2. 市場データ収集

毎日 06:00 JST に自動実行され、前日分のデータを取得します。

```bash
# 昨日分のデータを取得 (デフォルト)
python market_main.py

# 特定日を指定して取得 (過去データの補完など)
python market_main.py --target-date 2024-06-01
```

## ライセンス

MIT License

## 貢献

プルリクエストを歓迎します。大きな変更の場合は、まずissueを開いて変更内容を議論してください。

## 免責事項

このプロジェクトは投資判断の参考情報を提供するものであり、投資助言ではありません。投資は自己責任で行ってください。
