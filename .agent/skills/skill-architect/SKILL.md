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

## 2. 知能の自律監視・改善義務 (Autonomous Intelligence Management)
プロジェクトの健全な進化を保つため、スキルアーキテクトは以下の義務を負う。

- **自律的判断**: 全ての Rules, Skills, Workflows の現状を監視し、追加・統合・分離・削除・修正が必要なタイミングを自動で検知する。
- **報告と透明性**: 修正・変更が必要と判断した場合、即座にユーザーへその理由と背景を報告する。
- **公式仕様の遵守**: あらゆる改善は、以下の技術基準を絶対的なバイブルとして遵守し、曖昧さを排除して実行する。
    - [Antigravity Skills Spec](https://antigravity.google/docs/skills)
    - [Rules & Workflows Spec](https://antigravity.google/docs/rules-workflows)
    - [Agent Skills Standard (agentskills.io)](https://agentskills.io/home)

## 3. スキル保守と進化 (Skill Maintenance & Evolution)
方針の変更やコードの進化に合わせ、既存スキルの「陳腐化」を検知し、以下の対応を行う。
- **修正提案と義務的な報告**: スキルやマニュアル（KI含む）を書き換える際は、独断で行わず、必ずユーザーへ「背景・理由（なぜ変えるのか？何のメリットがあるのか？）」を初心者にも分かりやすく解説した上で提案し、承認を得る。
- **統合・分割**: 分散した知識が混乱を招く場合、スキルの整理・統合を提案する。

## 3. スキル設計ガイドライン (Design Principles)
`agentskills.io` 仕様に基づき、以下の構造を強制する。
1. **段階的開示 (Progressive Disclosure)**: 
    - `description` は 100 トークン以内でターゲットを絞る。
    - 詳細な技術仕様は `references/` に配置し、必要時のみ読み込む。
2. **疎結合と柔軟性**: 方針変更に強いよう、過度に硬直的なルールはリファレンスに逃がし、`SKILL.md` は目的（ゴール）を重視する。
3. **自己文書化**: `SKILL.md` を読めば、そのスキルの目的と使い方が明確であること。

## 4. プロセス (Management Workflow)
1. **検知/提案**: 「方針変更」や「新知見」を検知した際、新規作成または既存スキルの修正を提案する。その際、**「なぜこの修正（または作成）が今の投資アプリ開発に役立つのか」**を必ず言語化する。
2. **設計**: `implementation_plan.md` に変更内容を反映。
3. **実行**: .agent/skills/ 配下のファイルを生成・更新。
