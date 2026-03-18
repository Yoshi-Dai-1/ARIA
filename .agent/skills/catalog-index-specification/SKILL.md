---
name: catalog-index-specification
description: ARIA カタログインデックス (`documents_index.parquet`) の詳細仕様、カラム設計、および Pure Catalog の概念。
---

# ARIA Catalog Index Specification (Pure Catalog)

## 1. 概要
`documents_index.parquet` は、EDINETから収集したAPIの事実のみを記録する純粋な元帳（Pure Catalog）です。マスタの内部管理キー (`identity_key`) を含まず、結合を前提とした設計になっています。

## 2. 物理構造とカラム順序 (Web-Ready Architecture)
UI最適化のため、Identifiers -> Timeline -> Identifiers (Supplemental) -> Domain -> Document Details -> Infrastructure の順序で配置されています。合計 33 カラムで構成されます。

### 1) Identifiers (識別子・基本情報)
| カラム名 | 型 | 役割・ロジック | 情報源 |
| :--- | :--- | :--- | :--- |
| `doc_id` | string | 書類のユニークID (EDINETが提供するS100...など) | EDINET |
| `bin_id` | string | 物理パーティションID (JCN末尾2桁等の分散キー) | システム生成 |
| `edinet_code` | string | 提出者EDINETコード (Eで始まる6桁) | EDINET |
| `code` | string | 証券コード。常に5桁化し `JP:` プレフィックスを強制 (例: JP:72030) | EDINET |
| `jcn` | string | 法人番号 (Japan Corporate Number)。13桁 | EDINET |
| `company_name` | string | 提出者名 (和文) | EDINET |

### 2) Timeline & Main Content (Web UI 最適化による前寄せ)
| カラム名 | 型 | 役割・ロジック | 情報源 |
| :--- | :--- | :--- | :--- |
| `submit_at` | string | 提出日時 (YYYY-MM-DD HH:MM) | EDINET |
| `title` | string | 書類タイトル | EDINET |
| `doc_type` | string | 書類種別コード (120:有価証券報告書等) | EDINET |

### 3) Supplemental Identifiers
| カラム名 | 型 | 役割・ロジック | 情報源 |
| :--- | :--- | :--- | :--- |
| `issuer_edinet_code` | string | 発行者EDINETコード | EDINET |
| `subject_edinet_code` | string | 公開買付対象者EDINETコード | EDINET |
| `subsidiary_edinet_code` | string | 子会社EDINETコード (カンマ区切り) | EDINET |
| `fund_code` | string | ファンドコード (投資信託等) | EDINET |

### 4) Domain/Fiscal (決算・期間属性)
| カラム名 | 型 | 役割・ロジック | 情報源 |
| :--- | :--- | :--- | :--- |
| `fiscal_year` | int | 決算年度 | EDINET |
| `period_start` | string | 決算期間開始日 | EDINET |
| `period_end` | string | 決算期間終了日 | EDINET |
| `num_months` | int | 対象月数 | EDINET |
| `accounting_standard` | string | 会計基準 (Japan GAAP, IFRS等) | EDINET |

### 5) Document Details (書類詳細特性)
| カラム名 | 型 | 役割・ロジック | 情報源 |
| :--- | :--- | :--- | :--- |
| `form_code` | string | 様式コード | EDINET |
| `ordinance_code` | string | 府令コード | EDINET |
| `is_amendment` | bool | 訂正フラグ (True/False) | EDINET |
| `parent_doc_id` | string | 訂正対象の親書類ID | EDINET |
| `withdrawal_status` | string | 取下区分 (1:取下済) | EDINET |
| `doc_info_edit_status` | string | 財務局修正状態 (1:修正情報, 2:修正された書類) | EDINET |
| `disclosure_status` | string | 開示ステータス (1:OK, 2:修正 etc.) | EDINET |
| `current_report_reason` | string | 臨時報告書の提出理由 | EDINET |
| `has_xbrl` | bool | XBRL(ZIP)が本来存在するはずかを示すAPIフラグ | EDINET |
| `has_pdf` | bool | PDFが本来存在するはずかを示すAPIフラグ | EDINET |

### 6) Infrastructure & API Lifecycle (システム・運用管理)
| カラム名 | 型 | 役割・ロジック | 情報源 |
| :--- | :--- | :--- | :--- |
| `raw_zip_path` | string | 生ZIPファイルへの物理パス | システム |
| `pdf_path` | string | 生PDFファイルへの物理パス | システム |
| `processed_status` | string | 処理ステータス (pending, parsed, success, failure, retracted)。 | システム |
| `source` | string | 情報源 (原則 `EDINET` 固定) | Logic |
| `ope_date_time` | string | 操作日時 (API V2の差分増分同期の核心項目) | EDINET |

## 3. 設計思想 (Pure Catalog)
CatalogRecord には、あえて ARIA 内部で生成・維持される `identity_key` を含めていません。
マスタの統合や分離（名寄せ）が発生した場合でも、過去に収集した書類のデータを書き換える必要がないよう、マスタ側で参照関係を結ぶ設計としています。
証券コード、EDINETコード、JCNといった識別子はすべてAPI取得当時の「事実」として保持されます。
