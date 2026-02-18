#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path


def validate_skill(skill_path):
    path = Path(skill_path)
    if not path.is_dir():
        print(f"Error: {path} is not a directory.")
        return False

    errors = []

    # 1. Check for SKILL.md
    skill_md = path / "SKILL.md"
    if not skill_md.exists():
        errors.append("Missing SKILL.md")
    else:
        # 2. Check for frontmatter
        content = skill_md.read_text()
        if not content.startswith("---"):
            errors.append("SKILL.md missing frontmatter start (---)")
        else:
            match = re.search(r"---(.*?)---", content, re.DOTALL)
            if not match:
                errors.append("SKILL.md missing frontmatter end (---)")
            else:
                frontmatter = match.group(1)
                if "name:" not in frontmatter:
                    errors.append("Frontmatter missing 'name'")
                if "description:" not in frontmatter:
                    errors.append("Frontmatter missing 'description'")

    # 3. Check for mandatory directories (warning only)
    for d in ["scripts", "references"]:
        if not (path / d).is_dir():
            print(f"Warning: Missing recommended directory '{d}/'")

    if errors:
        for err in errors:
            print(f"❌ {err}")
        return False

    print(f"✅ {path.name}: Skill is valid!")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate skill structure.")
    parser.add_argument("path", help="Path to the skill directory")
    args = parser.parse_args()

    if not validate_skill(args.path):
        sys.exit(1)
