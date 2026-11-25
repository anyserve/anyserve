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
compose = (root / "deploy/vps/docker-compose.yml").read_text()
compose_ver = re.search(r'image:\s*ghcr\.io/anyserve/anyserve:v([^\s]+)', compose)
if not compose_ver:
    sys.exit("Could not find anyserve image version in deploy/vps/docker-compose.yml.")
modal = (root / "console/src/ConnectWorkerModal.tsx").read_text()
download_vers = re.findall(r'/releases/download/v([^/]+)/', modal)
archive_vers = re.findall(
    r'anyserve-v([0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+?)?)-(?:x86_64|aarch64)-',
    modal,
)
modal_vers = download_vers + archive_vers
if not download_vers or not archive_vers:
    sys.exit("Could not find release versions in console/src/ConnectWorkerModal.tsx.")
if compose_ver[1] != version or any(v != version for v in modal_vers):
    sys.exit("Compose and ConnectWorkerModal versions must match workspace version.")
if len(sys.argv) > 2 or (len(sys.argv) == 2 and sys.argv[1] != f"v{version}"):
    sys.exit(f"Expected release tag v{version}.")
print(version)
