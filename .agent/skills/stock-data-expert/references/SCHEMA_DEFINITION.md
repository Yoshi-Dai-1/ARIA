# ARIA データスキーマ定義 (Schema Definition)

ARIA プロジェクトの物理的な保存構造とデータモデルの定義です。

## 1. 物理ファイル分割 (Bin Sharding)
`MasterMerger` の実装に基づき、マスタデータはリポジトリ負荷軽減のため、**証券コードの上 2 桁**で物理分割されています。

- **パス構造**: `master/{type}/bin={XX}/data.parquet`
- **例**: `7203 (トヨタ)` -> `master/stocks_master/bin=72/data.parquet`

## 2. 主要モデル定義

### 統合ドキュメントカタログ (`CatalogRecord`)
`documents_index.parquet` に格納される加工済み情報。

| フィールド | 型 | 説明 |
| :--- | :--- | :--- |
| `code` | `str` | **証券コード (常に5桁)** |
| `jcn` | `str?` | 法人番号 (API v2 で取得) |
| `processed_status` | `str` | `success` または `retracted` (取下済) |

### 銘柄マスタ (`StockMasterRecord`)
`stocks_master` 配下の sharded parquet に格納される現在の銘柄状態。

| フィールド | 型 | 説明 |
| :--- | :--- | :--- |
| `code` | `str` | **証券コード (常に5桁)** |
| `is_active` | `bool?` | **Nullable Boolean (文字列化禁止)** |
| `last_submitted_at`| `str?` | 更新の根拠（ガードレール） |

## 3. 整合性維持の鉄則
- **Boolean 型の死守**: `is_active` 等の論理値に対し、`astype(str)` は絶対に適用しない。Pandas の Nullable Boolean として維持すること。
- **時系列ガード**: `last_submitted_at` を常に比較し、古い情報による「上書き劣化」を防ぐ。
