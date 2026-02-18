#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path


def create_skill(name, base_path):
    skill_dir = Path(base_path) / name
    if skill_dir.exists():
        print(f"Error: Directory {skill_dir} already exists.")
        sys.exit(1)

    # 1. Create directory structure
    skill_dir.mkdir(parents=True)
    (skill_dir / "scripts").mkdir()
    (skill_dir / "references").mkdir()
    (skill_dir / "assets").mkdir()

    # 2. Create SKILL.md template
    skill_md_content = f"""---
name: {name}
description: A brief description of the {name} skill and its purpose.
---

# {name.replace("-", " ").title()}

This is a template for the {name} skill.

## Core Tasks
- Task 1: Describe what the agent can do.

## Usage
Provide examples of how to invoke tools or scripts.

## References
- [API Reference](references/api_reference.md)
"""
    with open(skill_dir / "SKILL.md", "w") as f:
        f.write(skill_md_content)

    # 3. Create example script
    example_script = """#!/usr/bin/env python3
def main():
    print("Example script for the skill.")

if __name__ == "__main__":
    main()
"""
    with open(skill_dir / "scripts" / "example.py", "w") as f:
        f.write(example_script)

    # 4. Create reference placeholder
    with open(skill_dir / "references" / "api_reference.md", "w") as f:
        f.write("# API Reference\n\nPlaceholder for API documentation.")

    # 5. Create asset placeholder
    with open(skill_dir / "assets" / "example_asset.txt", "w") as f:
        f.write("Placeholder asset.")

    print(f"✅ Created skill directory: {skill_dir}")
    print("✅ Created SKILL.md")
    print("✅ Created scripts/example.py")
    print("✅ Created references/api_reference.md")
    print("✅ Created assets/example_asset.txt")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize a new Antigravity skill.")
    parser.add_argument("name", help="Name of the skill (e.g., my-awesome-skill)")
    parser.add_argument("--path", default=".", help="Base path to create the skill in (default: .)")
    args = parser.parse_args()

    create_skill(args.name, args.path)
