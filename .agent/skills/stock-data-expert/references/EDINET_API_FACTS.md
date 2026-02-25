# EDINET API V2 物理検証済み事実集

> このドキュメントは、推測を排し、物理的にダウンロード・実行して確認した「不変の事実」のみを記録する。

## 1. 書類一覧 API (documents.json)
- **エンドポイント**: `https://api.edinet-fsa.go.jp/api/v2/documents.json`
- **リクエストパラメータ**:
  - `date` (必須, YYYY-MM-DD)
  - `type` (1: メタデータのみ, 2: 書類一覧+メタデータ)
  - `Subscription-Key` (APIキー, クエリまたはヘッダー)
  - `opeDateTime` (任意, HH:mm:ss, この時刻以降に更新されたもののみ取得)
- **レスポンス `results` 配列**: 27フィールド
  - seqNumber, docID, edinetCode, secCode, JCN, filerName, fundCode, ordinanceCode, formCode, docTypeCode, periodStart, periodEnd, submitDateTime, docDescription, issuerEdinetCode, subjectEdinetCode, subsidiaryEdinetCode, currentReportReason, parentDocID, opeDateTime, withdrawalStatus, docInfoEditStatus, disclosureStatus, xbrlFlag, pdfFlag, csvFlag, legalStatus
- **レート制限**: 1IPあたり約5リクエスト/秒

## 2. 書類取得 API
- **エンドポイント**: `https://api.edinet-fsa.go.jp/api/v2/documents/{docID}`
- **type パラメータ**:
  - `1`: XBRL+PDF (ZIP)
  - `2`: PDF のみ
  - `3`: 代替書面 (CSV)
  - `4`: 英語版 (ZIP)
  - `5`: 添付書類 (ZIP)

## 3. EDINETコード集約一覧 (ESE140190.csv)
- **URL**: `https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/download/ESE140190.csv`
- **エンコーディング**: **CP932 (Shift-JIS)** ※ UTF-8 ではない (物理検証済み)
- **レイアウト**:
  - 1行目: `EDINETコード集約一覧,,` (タイトル行、skiprows=1 が必要)
  - 2行目: ヘッダー `集約処理日,廃止EDINETコード,継続EDINETコード`

## 4. EDINETコードリスト (Edinetcode.zip)
- **URL (日本語)**: `https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip`
- **URL (英語)**: `https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelisteng/Edinetcode.zip`
- **エンコーディング**: **CP932** (物理検証済み)
- **レイアウト**: skiprows=1 が必要

## 5. 提出者情報の変更 (仕様書 3-1-8)
- **3-1-8-1**: EDINET コードに紐づく属性（提出者名、証券コード、**法人番号**）は変更され得る
- **3-1-8-2**: EDINET コード自体が変更される理由:
  1. 特定有価証券の発行者の変更
  2. EDINET コードの集約（同一提出者が複数コードを保持していた場合の整理）

## 6. 法人番号 (JCN) の性質
- **管轄**: 国税庁 (番号法第8条)
- **不変性**: 法人の一生を通じて不変（名称変更・本店移転・代表者変更でも変わらない）
- **再利用禁止**: 解散後も他の法人に再割り当てされない
- **唯一の閉鎖**: 合併（吸収：消滅会社の番号閉鎖 / 新設：両社の番号閉鎖・新番号付与）
- **EDINET上**: 3-1-8-1 により「登録値としてのJCN」は変更可能（誤登録修正等）

## 7. opeDateTime の挙動
- **リクエスト時**: `HH:mm:ss` 形式。指定日の「この時刻以降に更新されたもの」のみフィルタ
- **レスポンス時**: `yyyy-MM-dd HH:mm:ss` 形式。メタデータ抽出が行われた日時
