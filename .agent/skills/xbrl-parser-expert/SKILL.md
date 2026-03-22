---
name: xbrl-parser-expert
description: 日本の証券データ（EDINET）のXBRL/iXBRL解析に関する専門スキル。「予測」を排除し、事実（名前空間・スキーマ参照）に基づく厳格なパースを実現します。
---
# XBRL パース・エキスパート (XBRL Parser Expert)

ARIAにおけるEDINETからのXBRL/Inline XBRLデータの抽出と解析を、「事実基底」かつ完全に決定論的な手法で処理するための専門スキルおよび規範です。

## 1. タクソノミの絶対的特定手法 (Facts over Prediction)
金融庁の「～以後に終了する事業年度に係る書類から適用」といった文章表現に対し、システムが日付計算による「タクソノミバージョンの推測」を行うことは厳禁です。\
XBRL仕様およびEDINET「報告書インスタンス作成ガイドライン」に基づき、パーサーは**ファイル内に物理的に宣言された名前空間（Namespace）または `schemaRef` のみに依存して**ロードする辞書（タクソノミ）を決定しなければなりません。

- **取得前判別**: EDINET API v2 のレスポンスフィールド `periodEnd` （対象期間終了日）を用いて使用タクソノミ群をフィルタリングする。
- **解析時判別**: `<xbrli:xbrl>` または `<html>` 要素の 名前空間URI、および Linkbase フォルダ内の `.xml` ファイルが参照する名前空間URI（例: `http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs/2020-03-31/...`）を正規表現等で抽出し、ロードすべきタクソノミバージョンを確定的に解決する。
- **年度マッピングの事実**: EDINETタクソノミの名前空間に含まれる日付（例: `2022-11-01`）と、適用される会計年度（例: `2023年版`）の対応関係は決定論的であり、これを明示的なマッピング辞書として保持し、物理的なURI文字列と照合すること。

## 2. NULL 基底アーキテクチャ (NULL Base Architecture)
XBRLデータ構造は企業によって大きく拡張（提出者別タクソノミ）されるため、特定のタグ（例: `jppfs_cor:NetSales`）が存在しない場合があります。
- 要素が見つからない場合、決して `0` や `-1` や `""`（空文字）などのSentinel Value（番兵値）で埋めず、**必ず `None` (NULL) を返す**こと。
- `0件` の結果と `要素なし` は、財務・監査上、全く異なる意味を持ちます。「異常なゼロ」を発生させないため、欠落は欠落として扱います。

## 3. フェイルファスト (Fail-Fast Parsing)
- スキーマのURIと、処理対象のタクソノミ辞書のバージョンが不一致の場合、推し量ってパースを続行せず、例外（例: `TaxonomyVersionMismatchError`）を発生させて**直ちに停止**すること。
- メタデータ（`periodEnd`等）と、パースしたXBRL内の `CurrentPeriodEndDateDEI` が矛盾する場合、データの誠実性が担保できないため、警告を記録し処理を停止（または隔離キューへ移動）すること。

## 4. リソースエコノミー (API & Memory Economy)
大量の有価証券報告書のXBRL（数十MB単位のファイルが数千件）を処理するため、以下の制限を順守します。
- メモリ上の巨大なDOMツリー構築（`xml.etree`全体ロードなど）を避け、可能な限り SAX や `lxml` の `iterparse` を使用したストリーミングパースを検討すること。
- 頻出するタクソノミ辞書（xsd/xml）ファイルは、実行ごとに再読み込みせず、メモリ（または高速なKVS等）にキャッシングし、IOアクセスを最小化すること。

## 5. ドキュメント不可知性 (Document Agnostic Parsing)
XBRLデータは、有価証券報告書だけでなく、四半期報告書、半期報告書、臨時報告書など様々な開示書類に添付されます。
- 書類を分類・フィルタリングする際は、特定の `formCode` や `ordinanceCode` のホワイトリスト（ハードコード）に依存してはならず、**EDINET APIの `xbrlFlag == 1` を第一の物理的事実として優先**すること。
- XBRLZIP展開時に、ファイル名に `"asr"` (Annual Securities Report) などの特定書類を示す文字列が含まれることを前提とするロジックは厳禁です。
- **物理的フォルダ構造の事実**:
    - **`PublicDoc/`**: 本表、注記等の一般開示用XBRL/Linkbaseの格納場所。
    - **`AuditDoc/`**: 監査報告書用のXBRL/Linkbaseの格納場所。
- **探索アルゴリズムの原則**: `PublicDoc/` または `AuditDoc/` フォルダを起点とし、フォルダ内の全ファイルを走査して `*pre.xml` (Presentation), `*cal.xml` (Calculation), `*lab.xml` (Label) などのサフィックスを持つファイルを動的に特定・抽出すること。書類種別（asr/qsr/ssr等）に関わらず、このディレクトリ構造は不変の「事実」です。

## 6. 多言語・多基準タクソノミパース (Multi-Standard Role Agnosticism)
EDINETには日本基準（JP GAAP）と国際財務報告基準（IFRS）等、複数基準のデータが混在して提出されます。
これらを一元的に抽出するためには、パーサーの参照するRole URI辞書をハードコードされた単一基準の文字列に依存させてはいけません。
- **JP GAAP**: `<link:presentationLink xlink:role="http://disclosure.edinet-fsa.go.jp/role/jppfs/rol_BalanceSheet">` のように、`_BalanceSheet` や `_StatementOfIncome` といった人間が判読可能な英単語サフィックスを持ちます。
- **IFRS**: `<link:presentationLink xlink:role="http://xbrl.ifrs.org/role/ifrs/ias_1_2014-03-05_role-210000">` のように、国際基準に対応するため役割が `_role-210000` などの**6桁の数値コード文字列**で厳格に管理されています。
- **US GAAP (米国基準)**: `<link:presentationLink xlink:role="http://disclosure.edinet-fsa.go.jp/role/us-gaap/...">` 等の名前空間を持ちます。提出者により「数値タグ付け（Detailed Tagging）」を行う場合と、「包括タグ（Block Tagging / Text Block）」のみで行う場合があります。
- **JMIS (修正国際基準)**: 提出ルールはUS GAAPと同一であり、物理構造も同様の挙動を示します。

## 7. Roleマッピング辞書のリスクと抽出アーキテクチャ (Hazards of Role Mapping)
EDINETタクソノミの設定規約書において、企業は**「提出者別拡張タクソノミ（Company-Specific Extension Taxonomy）」**を作成し、全く新しい独自の `Role URI` を自由に定義することが公式に認められています。

1. **Roleハードコード（ホワイトリスト）の禁止**:
   `fs_dict = {"BS": ["_role-210000", "_BalanceSheet"]}` のような固定のRole URI文字列辞書を用いて抽出対象データをフィルタリングする手法は、未知の拡張Role URIを持つファクトデータを**サイレントに欠落（Drop）させる致命的なデータ漏洩の根本原因**となります。
2. **包括的データ抽出（Exhaustive Extraction）への転換**:
   工学的主権に基づき、特定のRoleのみを「予測・期待」して検索するのではなく、Arelleがパースした**すべてのファクトデータを無条件で抽出し、カタログへ格納する（`financial_values`）アーキテクチャ**を絶対の仕様とします。
3. **米国基準（US GAAP）の正解**:
   US GAAP書類は包括タグ（Text Block）を多用する傾向にありますが、数値タグが存在する場合は `financial_values` へ、包括タグのみの場合は `qualitative_text` へ格納されます。これはバグではなく「提出されたXBRLの物理的事実（Facts over Prediction）」に従った正常な挙動です。

## 8. Arelle直接ラベル注入 (Arelle-Native Label Injection)
ARIAのラベル解決は3層構造で動作します。FSAタクソノミ（`1c_Taxonomy.zip`）にはIFRSラベルが物理的に含まれていないため、Arelleのスキーマ参照チェーンを通じた直接解決が不可欠です。

### 3層ラベル解決チェーン
1. **リンクベース由来** (`link_base_file_analyzer.py`): `_pre.xml` + `_lab.xml` から `jpcrp_cor` / `jppfs_cor` のラベルを取得。JP GAAP書類で主に機能する。
2. **FSA共通タクソノミ** (`account_list_common`): FSA配布の `1c_Taxonomy.zip` から `jpcrp` / `jppfs` ラベルを取得。**IFRSラベルは含まれていない（物理的事実: 全年度で0件確認済み）。**
3. **Arelle直接解決** (`_safe_label()`): `fact.concept.label(lang, preferredLabel)` を使用。企業`.xsd`の`<import>`チェーンを辿り、IFRS Foundationタクソノミ（例: `http://xbrl.ifrs.org/taxonomy/2014-03-05/...`）のラベルを自動解決する。**年度固有URLが企業提出ファイルに埋め込まれているため、未来のタクソノミバージョンにも自動対応する。**

### QNameフォールバック防御
Arelleはラベルが未定義の場合、要素のQName文字列（例: `ifrs-full:CashAndCashEquivalents`）をフォールバックとして返す。これは「偽ラベル」であり事実ではないため、`_safe_label()` で検出・除外し、`None` (NULL) として保存する。

### IFRSラベルロールの物理的事実
IFRS Foundationタクソノミは以下のラベルロールのみを定義している:
- `label` → ARIAの `label_jp` / `label_en`（100%解決）
- `totalLabel`, `periodStartLabel`, `periodEndLabel`, `terseLabel`（補助ロール、ARIAでは現在未取得）
- **`verboseLabel` は未定義** → ARIAの `label_jp_long` / `label_en_long` はIFRS書類で物理的にNULLとなる。これは仕様通りの正常な挙動である。

