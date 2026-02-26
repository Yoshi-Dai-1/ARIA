---
name: aria-engineer
description: ARIA プロジェクトにおける技術的真実、データ整合性、および厳格な実装のためのエンジニアリング規範。
---
# ARIA エンジニア (ARIA Engineer)

このスキルは、ARIA プロジェクトにおける技術的判断の「憲法」です。

## 1. 行動規範 (Manifesto)
- **事実の優位性**: 物理的なコード、型定義、実行ログに基づいた「事実」のみを根拠とする。
- **異常値への執着**: 「0件」や「空の結果」が正常なゼロか異常なゼロかを数学的に証明できるまで停止しない。
- **FMEA の義務**: 重大な変更前には必ず故障モードとその影響を洗い出す。

## 2. 物理的な掟 (Physical Facts)
- **RaW-V (Read-after-Write Verification)**: 破壊的更新前には必ず [CatalogManager.take_snapshot](file:///Users/yoshi_dai/repos/ARIA/data_engine/catalog_manager.py#L220) を実行。
- **NaN / Null Integrity**: pandas 由来の NaN が Pydantic モデルの文字列フィールドを破壊するのを防ぐため、`field_validator` による強制変換を必須とする。
- **Network Stability**: 外部通信を伴う処理では [network_utils.patch_all_networking](file:///Users/yoshi_dai/repos/ARIA/data_engine/network_utils.py) の適用を必須とする。

## 3. 監査手法 (Audit Methodology)
- **Mass-Scale Stress Test**: 1,000件規模の擬似レコードを用い、Parquet の物理的統合と bin への均等分散を検証する。
- **Self-Healing Logic**: 履歴再構築時に `0000-00-00` シードを注入し、バックフィルによる情報の断絶を防止する。

## 4. 環境制御
- **CI 最適化**: ログの肥大化を防ぐため `HF_HUB_DISABLE_PROGRESS_BARS=1` および `TQDM_DISABLE=1` を強制する。

## 参照リソース
- [エンジニアリングパターン](references/patterns.md): FMEA、冪等性の具体例
- [技術用語翻訳ガイド](references/PEDAGOGICAL_GUIDE.md): 投資家向けの分かりやすい説明指針
