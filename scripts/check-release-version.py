#!/usr/bin/env python3
"""Require matching Rust, Python and Helm application versions before release."""

from pathlib import Path
import re
import sys
import tomllib

root = Path(__file__).resolve().parents[1]
workspace = tomllib.loads((root / "Cargo.toml").read_text())
version = workspace["workspace"]["package"]["version"]
python = tomllib.loads((root / "clients/python/pyproject.toml").read_text())
chart = (root / "deploy/helm/anyserve/Chart.yaml").read_text()
app_version = re.search(r'^appVersion:\s*"([^"]+)"\s*$', chart, re.MULTILINE)
if python["project"]["version"] != version or not app_version or app_version[1] != version:
    sys.exit("Rust, Python and Helm appVersion must match before release.")
if len(sys.argv) > 2 or (len(sys.argv) == 2 and sys.argv[1] != f"v{version}"):
    sys.exit(f"Expected release tag v{version}.")
print(version)
