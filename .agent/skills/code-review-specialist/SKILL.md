---
name: code-review-specialist
description: Specialized code review skill for ensuring safety, financial accuracy, and engineering rigor within the ARIA investment app.
---

# コードレビュー・スペシャリスト (Code Review Specialist)

このスキルは、ARIA プロジェクトにおける「品質の番人」です。あらゆるコード変更に対し、投資アプリとしての妥当性と工学的な堅牢性を監査します。

## 1. 監査の四柱 (Four Pillars of Audit)

### ① 財務計算の精度 (Financial Accuracy)
- **浮動小数点の回避**: 金額計算において `float` ではなく `Decimal` や整数単位が使われているか。
- **丸め処理の明示**: 四捨五入、切り捨て、切り上げのロジックに曖昧さがないか。
- **オーバーフロー防止**: 巨大な資産総額を扱う際の型の耐性。

### ② データ整合性 (Data Integrity)
- **NULL/None の扱い**: 「不明」と「ゼロ」が混同されていないか。
- **型バリデーション**: 入力値が Pydantic 等で厳格にガードされているか。
- **冪等性**: 同じ処理を二回実行してもデータが壊れないか。

### ③ セキュリティと資産保護 (Privacy & Assets)
- **秘密情報の露出**: APIキーや個人情報がログに出力されていないか。
- **不適切なアクセス**: ストレージ（Hugging Face Hub 等）への書き込み権限が最小限か。

### ④ FMEA (故障モード影響解析)
- 変更点が、依存する他のモジュール（例：`market_engine.py`）にどう影響するか。
- 「最悪のケース（例：証券コードの重複）」が発生した時の挙動は定義されているか。

## 2. レビュー・プロセス (Review Protocol)
1. **変更の把握**: `git diff` や変更依頼から本質的な意図を抽出。
2. **リスク抽出**: 上記の 4 項目に基づき、潜在的なリスクをリストアップ。
3. **対案の提示**: 単なる否定ではなく、「こう書く方が安全で、投資判断を誤らせない」という具体案を提示。
4. **初心者への翻訳**: 技術的な懸念が、投資運用上のどのようなリスク（例：損益の誤表示）に繋がるかを日常語で解説。

## 3. リファレンス
- [財務計算監査チェックリスト](references/FINANCIAL_AUDIT.md)
- [セキュリティ・ガイドライン](references/SECURITY_GUIDE.md)
