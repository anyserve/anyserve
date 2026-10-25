#!/usr/bin/env python3
"""Check local Markdown links in repository documentation."""

from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit

root = Path(__file__).resolve().parents[1]
paths = subprocess.check_output(
    ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "*.md"],
    cwd=root,
).decode().split("\0")
errors = []
for name in sorted(set(paths) - {""}):
    path = root / name
    if not path.is_file():
        continue
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        for target in re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", line):
            target = target.strip().split(' "', 1)[0].strip("<>")
            url = urlsplit(target)
            if url.scheme or url.netloc or not url.path:
                continue
            destination = path.parent / unquote(url.path)
            if not destination.exists():
                errors.append(f"{name}:{line_number}: missing {target}")
if errors:
    sys.exit("\n".join(errors))
print("Local documentation links are valid.")
