#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def run(cmd: str, execute: bool) -> None:
    """Prints command and runs it if execute is True."""
    print(f"  [EXEC] {cmd}")
    if execute:
        subprocess.run(cmd, shell=True, check=True)


def update_file_regex(
    path: Path,
    pattern: str,
    replacement: str,
    execute: bool,
    count: int = 0,  # 0 replaces all matches by default
) -> None:
    """Performs regex replacement on a file or previews it during dry-run."""
    if not path.exists():
        print(f"  [ERROR] File {path} not found!")
        sys.exit(1)

    content = path.read_text(encoding="utf-8")
    new_content, replacements_made = re.subn(pattern, replacement, content, count=count)

    if replacements_made == 0:
        print(f"  [WARN] No match found in {path} for pattern: {pattern}")
    else:
        print(f"  [MATCH] Replaced {replacements_made} occurrence(s) in {path}")

    if execute:
        path.write_text(new_content, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(
        description="Release automation script for version bumping and changelog generation."
    )
    parser.add_argument("version", help="Target version (e.g. 0.4.0 or v0.4.0)")
    parser.add_argument(
        "-e",
        "--execute",
        action="store_true",
        help="Apply changes and perform git commit. Default is dry-run mode.",
    )

    args = parser.parse_args()
    new_ver = args.version.lstrip("v")
    today = datetime.now().strftime("%Y-%m-%d")
    is_dry_run = not args.execute

    if is_dry_run:
        print("DRY-RUN MODE ACTIVE (No files will be modified)")
        print("Run with '--execute' or '-e' to apply changes.\n")
    else:
        print("EXECUTING RELEASE BUMP...\n")

    print(f"Target Version : {new_ver}")
    print(f"Release Date   : {today}\n")

    # Bump Cargo.toml
    print("1. Bumping Cargo.toml...")
    run(f"cargo set-version {new_ver}", execute=args.execute)

    # Bump pyproject.toml
    print("\n2. Bumping pyproject.toml...")
    update_file_regex(
        path=Path("pyproject.toml"),
        pattern=r'version = "[^"]+"',
        replacement=f'version = "{new_ver}"',
        execute=args.execute,
        count=1,
    )

    # Update uv.lock to match pyproject.toml
    print("\n2b. Updating uv.lock...")
    run("uv lock", execute=args.execute)

    # Generate CHANGELOG.md via git-cliff
    print("\n3. Generating CHANGELOG.md via git-cliff...")
    run(f"git cliff --tag v{new_ver} -o CHANGELOG.md", execute=args.execute)

    # Update docs/roadmap.md
    print("\n4. Updating docs/roadmap.md...")
    update_file_regex(
        path=Path("docs/roadmap.md"),
        pattern=r"\[[uU]nreleased\]",
        replacement=f"[v{new_ver}] - {today}",
        execute=args.execute,
    )

    # Git Commit
    print("\n5. Staging and committing changes...")
    run(
        f"git commit -am 'chore(release): bump version to {new_ver}'",
        execute=args.execute,
    )

    if is_dry_run:
        print("\nDry-run complete. Everything looks good!")
    else:
        print(f"\nSuccessfully updated files and created local commit for v{new_ver}!")


if __name__ == "__main__":
    main()
