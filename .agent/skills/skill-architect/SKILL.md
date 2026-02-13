---
name: skill-architect
description: A meta-skill for identifying, proposing, and creating new specialized agent skills to expand the agent's capabilities within the project.
---

# スキル・アーキテクト (Skill Architect Meta-Skill)

このスキルは、プロジェクトの成長に伴い「専門知識をカプセル化した新しいスキル」を自律的に設計・提案するためのメタ知能です。

## 1. 提案基準 (Activation Criteria for New Skills)
以下の状況を検知した場合、新しいスキルの作成をユーザーに提案する。
- **高度な専門性**: 特定のドメイン（例：高度な会計基準の解釈、特定のAPI仕様）に関する複雑な指示が繰り返される場合。
- **ワークフローの定型化**: 複数ステップのデバッグやデータ移行など、再現性が必要な手順が確立された場合。
- **認知的負荷の分散**: `top-tier-engineer` などの既存スキルが肥大化し、コンテキスト消費が増大した場合。

## 2. スキル設計ガイドライン (Design Principles)
`agentskills.io` 仕様に基づき、以下の構造を強制する。
1. **段階的開示 (Progressive Disclosure)**: 
    - `description` は 100 トークン以内でターゲットを絞る。
    - 詳細な技術仕様は `references/` に配置し、必要時のみ読み込む。
2. **疎結合**: スキル同士が依存しすぎず、単体で完結した能力を持つように設計する。
3. **自己文書化**: `SKILL.md` を読めば、そのスキルの目的と使い方が明確であること。

## 3. 作成プロセス (Creation Workflow)
1. **提案**: ユーザーに対し「新しいスキル [name] の作成」を、その理由と構成（references 等）とともに提案する。
2. **設計**: `implementation_plan.md` にスキルの階層構造を追記する。
3. **実装**: 指定されたディレクトリ構造 (.agent/skills/[name]/) でファイルを生成する。
4. **検証**: リンターやバリデーションを行い、正しく認識されるか確認する。
