#!/usr/bin/env python3
"""Bump release version atomically across Rust, Python, Helm, Compose, and Console.

Usage:
    python3 scripts/bump-version.py 0.7.0
    python3 scripts/bump-version.py v0.7.0 --dry-run
    python3 scripts/bump-version.py 0.7.0 --chart-version 0.7.0
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import subprocess
import sys

SEMVER_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$")


def replace_exact(path: Path, pattern: str, repl: str, *, flags: int = 0, min_count: int = 1) -> tuple[str, int]:
    original = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, repl, original, flags=flags)
    if count < min_count:
        raise RuntimeError(f"{path}: expected at least {min_count} match(es) for pattern {pattern!r}, found {count}")
    return updated, count


def bump_all(root: Path, version: str, *, chart_version: str | None, dry_run: bool, skip_lock: bool) -> list[str]:
    planned: list[tuple[Path, str]] = []

    # 1. Cargo.toml ([workspace.package] version)
    cargo_path = root / "Cargo.toml"
    cargo_updated, _ = replace_exact(
        cargo_path,
        r'(\[workspace\.package\][^\[]*?^version\s*=\s*")[^"]+(")',
        rf'\g<1>{version}\g<2>',
        flags=re.MULTILINE | re.DOTALL,
    )
    planned.append((cargo_path, cargo_updated))

    # 2. clients/python/pyproject.toml ([project] version)
    py_path = root / "clients/python/pyproject.toml"
    py_updated, _ = replace_exact(
        py_path,
        r'(\[project\][^\[]*?^version\s*=\s*")[^"]+(")',
        rf'\g<1>{version}\g<2>',
        flags=re.MULTILINE | re.DOTALL,
    )
    planned.append((py_path, py_updated))

    # 3. deploy/helm/anyserve/Chart.yaml (appVersion, plus optional chart version)
    chart_path = root / "deploy/helm/anyserve/Chart.yaml"
    chart_updated, _ = replace_exact(
        chart_path,
        r'^(appVersion:\s*")[^"]+("\s*)$',
        rf'\g<1>{version}\g<2>',
        flags=re.MULTILINE,
    )
    if chart_version is not None:
        chart_updated, chart_count = re.subn(
            r'^(version:\s*)[^\s]+(\s*)$',
            rf'\g<1>{chart_version}\g<2>',
            chart_updated,
            count=1,
            flags=re.MULTILINE,
        )
        if chart_count != 1:
            raise RuntimeError(f"{chart_path}: failed to update Helm chart version")
    planned.append((chart_path, chart_updated))

    # 4. deploy/vps/docker-compose.yml (ghcr.io/anyserve/anyserve:vX.Y.Z)
    compose_path = root / "deploy/vps/docker-compose.yml"
    compose_updated, _ = replace_exact(
        compose_path,
        r'(ghcr\.io/anyserve/anyserve:v)[^\s}]+',
        rf'\g<1>{version}',
    )
    planned.append((compose_path, compose_updated))

    # 5. console/src/ConnectWorkerModal.tsx (download URLs & archive names)
    modal_path = root / "console/src/ConnectWorkerModal.tsx"
    modal_text = modal_path.read_text(encoding="utf-8")
    modal_updated, c1 = re.subn(
        r'(/releases/download/v)[^/]+(/)',
        rf'\g<1>{version}\g<2>',
        modal_text,
    )
    modal_updated, c2 = re.subn(
        r'(anyserve-v)[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+?)?(-(x86_64|aarch64)-)',
        rf'\g<1>{version}\g<2>',
        modal_updated,
    )
    if c1 < 1 or c2 < 1:
        raise RuntimeError(
            f"{modal_path}: expected release download URL and archive version matches (got {c1}, {c2})"
        )
    planned.append((modal_path, modal_updated))

    changed: list[str] = []
    for path, content in planned:
        rel = str(path.relative_to(root))
        if path.read_text(encoding="utf-8") != content:
            changed.append(rel)
            if not dry_run:
                path.write_text(content, encoding="utf-8")

    if not dry_run and not skip_lock and (root / "Cargo.lock").exists():
        # Refresh workspace member versions inside Cargo.lock offline without network access.
        subprocess.run(
            ["cargo", "metadata", "--offline", "--format-version", "1"],
            cwd=root,
            stdout=subprocess.DEVNULL,
            check=True,
        )
        changed.append("Cargo.lock")

    if not dry_run:
        check_script = root / "scripts/check-release-version.py"
        subprocess.run(
            [sys.executable, str(check_script), f"v{version}"],
            cwd=root,
            check=True,
        )

    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description="Atomically bump AnyServe release version.")
    parser.add_argument("version", help="Target SemVer version (e.g. 0.7.0 or v0.7.0)")
    parser.add_argument("--chart-version", default=None, help="Optional Helm Chart.yaml version bump")
    parser.add_argument("--root", default=None, help="Override repository root directory")
    parser.add_argument("--dry-run", action="store_true", help="Preview files to update without writing")
    parser.add_argument("--skip-lock", action="store_true", help="Skip refreshing Cargo.lock via cargo metadata")
    args = parser.parse_args()

    version = args.version.strip()
    if version.startswith("v"):
        version = version[1:]
    if not SEMVER_RE.match(version):
        sys.exit(f"Invalid SemVer version: {args.version!r} (expected e.g. 0.7.0 or 0.7.0-rc.1)")
    if args.chart_version is not None and not SEMVER_RE.match(args.chart_version):
        sys.exit(f"Invalid Helm chart version: {args.chart_version!r}")

    root = Path(args.root).resolve() if args.root else Path(__file__).resolve().parents[1]
    changed = bump_all(
        root,
        version,
        chart_version=args.chart_version,
        dry_run=args.dry_run,
        skip_lock=args.skip_lock,
    )

    mode = "Dry run (no files modified)" if args.dry_run else f"Bumped release version to v{version}"
    print(mode)
    for rel in changed:
        print(f"  - {rel}")

    if not args.dry_run:
        print("\nNext steps to publish release:")
        print(f"  git commit -am \"release: v{version}\"")
        print(f"  git tag -a v{version} -m \"v{version}\"")
        print(f"  git push origin main v{version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
