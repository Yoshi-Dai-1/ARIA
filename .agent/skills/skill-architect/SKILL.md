---
name: skill-architect
description: プロジェクトの成長に伴い、新しいスキルを自律的に設計・提案するためのメタスキル。
---
# スキル・アーキテクト (Skill Architect)

プロジェクトの健全な進化のため、新しいスキルの必要性を検知し、設計を管理します。

## 1. 提案・監視基準
- **重複の検知**: 同じロジックや手順が複数の場所で繰り返されている場合。
- **認知的負荷**: 既存スキルが肥大化し、コンテキスト消費が増大した場合。
- **専門性のカプセル化**: 特定の API や業務ドメインに関する知識が必要な場合。

## 2. スキル管理ワークフロー
スキルを作成・更新する際は、必ず **skill-creator** のツールを活用してください。

### ① スキルの初期化 (Initialization)
`init_skill.py` を使用して、標準的な構造を生成します。
```bash
python3 .agent/skills/skill-creator/scripts/init_skill.py <name> --path .agent/skills
```

### ② 実装と検証 (Implementation & Validation)
`SKILL.md` を記述し、`quick_validate.py` で検証を行います。
```bash
python3 .agent/skills/skill-creator/scripts/quick_validate.py .agent/skills/<name>
```

## 3. 設計原則 (Skill Creator 準拠)
- **簡潔さ**: `SKILL.md` は 100 行以内を目指す。
- **段階的開示**: 詳細な仕様や例は `references/` に分離する。
- **低い自由度の提供**: 脆弱な操作は `scripts/` に固定化する。
