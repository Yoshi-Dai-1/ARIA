#!/usr/bin/env python3
import argparse
import os
import sys
import zipfile
from pathlib import Path


def package_skill(skill_path, output_dir):
    path = Path(skill_path)
    if not path.is_dir():
        print(f"Error: {path} is not a directory.")
        sys.exit(1)

    skill_name = path.name
    output_path = Path(output_dir) / f"{skill_name}.skill"

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(path.parent)
                zipf.write(file_path, arcname)

    print(f"✅ Packaged skill to: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Package a skill directory.")
    parser.add_argument("path", help="Path to the skill directory")
    parser.add_argument("--out", default=".", help="Output directory (default: .)")
    args = parser.parse_args()

    package_skill(args.path, args.out)
