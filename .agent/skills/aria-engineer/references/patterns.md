# ARIA Engineering Patterns

ARIA の物理的な掟（Code-rooted Facts）に基づいた実装パターンです。

## 1. RaW-V (Reliability and Watch-ability Verification)
マスタデータの更新など、破壊的な操作を伴う処理では以下の手順を「義務」とする。
- **Snapshot**: 処理開始前に `CatalogManager.take_snapshot()` を実行。
- **Rollback**: 予期せぬエラーや整合性不備を検知した場合、即座に `CatalogManager.rollback()` を実行し、データ状態を復旧させる。

## 2. データ整合性の禁忌
- **Nullable Boolean の死守**: `is_active` 等のブール値カラムに対し、`astype(str)` や `fillna("")` を適用してはならない。これらは分析時の曖昧さを生む原因となる。
- **不変シャッディング**: 証券コードの上 2 桁による物理分割（`bin=XX`）を維持し、ファイル名やパス構造を独断で変更してはならない。

## 3. 型設計
- **Pydantic Strict Mode**: 外部データのバリデーションには常に `BaseModel` を使用し、意図しない型変換を遮断する。
