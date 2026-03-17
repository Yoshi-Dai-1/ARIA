---
name: stock-master-specification
description: ARIA 銘柄マスタ (`stocks_master.parquet`) の詳細仕様、データソース、および物理実装ガイド。
---

# ARIA Stock Master Specification (Engineering Sovereignty)

## 1. 概要
`stocks_master.parquet` は、ARIA プロジェクトにおける「銘柄の真実」を司る中央リポジトリです。EDINET、JPX、および内部集約リストからの情報を統合し、ウェブアプリ・API での高速な検索と、金融監査に耐えうる正確性を両立します。

## 2. 物理構造とカラム順序 (Web-Ready Architecture)
ウェブアプリでの利用を考慮し、Identifiers -> Identity -> Lifecycle -> Industry -> Financial の論理グループで物理カラムを配置しています。

### 1) Essential Web Identity (Primary Keys)
| カラム名 | 型 | 説明 | データソース |
| :--- | :--- | :--- | :--- |
| `identity_key` | string | **Primary Key**. JCN優先、次いでEDINETコード、証券コード。 | Logic |
| `edinet_code` | string | EDINET提出者コード (EXXXXX)。 | EDINET |
| `code` | string | 証券コード (5桁正規化: JP:XXXX0)。 | JPX / EDINET |

### 2) Identity Attributes
| カラム名 | 型 | 説明 |
| :--- | :--- | :--- |
| `company_name` | string | 正式商号（和文）。 |
| `company_name_en` | string | 正式商号（英文）。 |
| `company_name_kana` | string | 社名ヨミ。 |

### 3) Lifecycle & Tracking
| カラム名 | 型 | 説明 | ロジック |
| :--- | :--- | :--- | :--- |
| `is_active` | bool | 運用上の追跡対象フラグ。EDINETが「非上場」と明記している場合は、特例を除き絶対的にFalseとなる。 | システム判断 |
| `is_disappeared` | bool | 全ソースから消失したか。 | 差分判定 |
| `is_listed_edinet` | bool | 金融庁名簿上の上場フラグ。 | EDINET |
| `last_submitted_at` | string | 最終書類提出日時。 | Catalog |

### 4) Industry & Market (Normalized)
これらのカラムは `jpx_definitions.parquet` と結合して最新化されます。
- `market`, `sector_jpx_33`, `sector_jpx_17`, `size_category` 等。
- `jpx_definitions.parquet` は Slowly Changing Dimension (SCD) Type 2 を採用し、`valid_from` と `valid_to`（「昨日」閉め）を用いて履歴の完全な連続性を担保しています。

## 3. 実体識別ロジック (Identity Sovereignty)

### 1) 識別子の遷移と統合（Identity Upgrade）
ARIA は、情報の鮮度と権威に基づき、以下の優先順位で実体 ID（`identity_key`）を決定します。
1.  🔒 **JCN (13桁法人番号)**: **[最優先]** ストレージの物理パス（bin分割）と一致する最終的な ID。
2.  ⚠️ **EDINET Code (E...)**: JCN が不明な期間の暫定 ID。
3.  🔄 **Security Code (JP:...)**: 上記いずれも不明な（IPO直後等の）暫定 ID。

### 2) identity_key が「変化する」理由と工学的必要性
プログラミングの観点では、ID は一度決まったら変わらないのが理想です。しかし、現実の金融データでは「証券コードはあるが法人番号は後から判明する」という情報の不完全性が発生します。
- **必要性**: もし `identity_key` がなければ、プログラムは常に「JCN または EDINETコード または 証券コード」の 3 つすべてをチェックし続けなければならず、処理が劇的に遅くなり、バグの温床になります。
- **解決策**: ARIA では「現時点で判明している中で最も信頼できる ID」を `identity_key` と呼び、実体を 1 つの ID で代表させます。
- **遷移の管理**: ID が `JP:...` から `JCN` へ遷移した際、ARIA は内部で「名寄せ」を行い、マスタ上の関係性を更新します。カタログ側には管理キーを持たない（Pure Catalog）ため、カタログデータを書き換えることなく、マスタを介した動的な紐付けが可能です。

## 4. 消失判定の数学的根拠 (Anomaly Obsession)
銘柄が `is_disappeared=True` となるのは、以下の **AND 条件** を満たした場合のみです。
1. 今回の EDINET コードリスト同期に含まれていない。
2. 今回の JPX 上場銘柄一覧同期に含まれていない。
3. 過去に一度も `is_disappeared` になったことがない（または直近まで生存していた）。

## 5. 逆方向名寄せブリッジ (Backward Aggregation)
`ESE140190.csv` に基づき、合併・持株会社移行等のコード変更に対して「順方向置換」を排除し、以下の原則で履歴を紐付けます。
- `former_edinet_codes`: 現在の継続EDINETコードを持つレコードに対してのみ、配列（カンマ区切り）として過去の廃止コードを「逆方向追記（マージ）」し、現在の識別子の絶対性を保護と過去データとのトレーサビリティを完全確保します。
