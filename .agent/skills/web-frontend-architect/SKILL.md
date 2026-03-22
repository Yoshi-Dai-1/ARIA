---
name: web-frontend-architect
description: ARIAのWebフロントエンド設計における工学的主権およびアーキテクチャ規範。特にXBRLデータの表示戦略（US GAAP含む）に関する事実に基づく設計指針を提供します。
---
# Webフロントエンド・アーキテクト (Web Frontend Architect)

ARIAのデータをユーザーに提供するWebフロントエンド（Vite/React等を想定）を設計・実装するための専門スキルおよび規範です。バックエンド（Data Engine）の物理的設計事実に基づき、最も高速で強牢なレイテンシ・アーキテクチャを実現します。

## 1. 財務データの表示戦略 (Financial Data Display Strategy)

バックエンドの抽出データは、その性質から明確に2つのストレージ（Parquet列指向DB）に分割されています。フロントエンドは、ユーザーの要求や対象企業の「会計基準」に応じて、クエリ先を動的にルーティングしなければなりません。

### A. 計算可能データ（JP GAAP / IFRS）の表示
- **対象**: `financial_values.parquet` (`isTextBlock_flg = 0`)
- **レンダリング方針**:
  - ここには売上高、純利益などの「純粋な数値（整数・浮動小数点）」が、科目名（`element_name`）や所属表（`role`）のメタデータと共に格納されています。
  - フロントエンドはこれらの数値をAPI経由で取得し、D3.js、Recharts、または各種UIライブラリを用いて、**インタラクティブなチャート（グラフ）や動的な財務ダッシュボード**を自由に構築可能です。

### B. 包括タグ・テキストデータ（US GAAP / JMIS等）の表示
- **対象**: `qualitative_text.parquet` (`isTextBlock_flg = 1`)
- **レンダリング方針**:
  - ここには、「経営方針」などの定性テキストだけでなく、**US GAAP（米国基準）やJMIS（修正国際基準）等における「貸借対照表」「損益計算書」全体をひとつの巨大タグとして囲んだ生のHTML文字列**が含まれる場合があります。
  - フロントエンドは対象企業の会計基準のみで判断せず、`financial_values` に数値データが存在するか、あるいは `qualitative_text` に表形式のHTMLが含まれているかをデータの実態（`isTextBlock_flg`）に基づき判定してください。
  - **アーキテクチャ正解**: 数値データが欠落（Block Tagging）している場合、`qualitative_text` テーブルから該当表（Role等で判別）の生HTML文字列（`data_str` カラム）を取得し、フロントエンド側で React の `dangerouslySetInnerHTML`（または安全なサニタイズ処理を経たDOMインジェクション）を用いて、**そのままHTMLの表として画面にレンダリング**します。

## 2. 実装上の必須確認事項 (Implementation Checklist)
- [ ] 企業マスタ（`stocks_master`）や APIレスポンスから、対象企業の「会計基準（Accounting Standard）」を事前判定しているか？
- [ ] 会計基準が「US GAAP（米国基準）」または「JMIS（修正国際基準）」である場合、グラフ描画コンポーネントを即座にフォールバックさせ、HTML直接レンダリングモードへ切り替えているか？
- [ ] `qualitative_text` の生HTMLを描画する際、悪意あるスクリプトを防ぐための適切なサニタイズ（DOMPurify等）を実施しているか？
