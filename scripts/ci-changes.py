#!/usr/bin/env python3
"""Classify changed files to drive tiered CI jobs in .github/workflows/anyserve-ci.yml."""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import subprocess
import sys

ALL_PYTHON_VERSIONS = ["3.10", "3.12", "3.14"]
FAST_PYTHON_VERSIONS = ["3.12"]

DOCS_ONLY_PATTERNS = (
    "docs/*",
    "docs/**",
    "website/*",
    "website/**",
    "*.md",
    "LICENSE",
    ".github/release.yml",
    ".github/workflows/docs.yml",
)

GLOBAL_CORE_FILES = {
    ".github/workflows/anyserve-ci.yml",
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
}


def matches_any(path: str, patterns: tuple[str, ...]) -> bool:
    for pat in patterns:
        if pat.endswith("/**"):
            prefix = pat[:-3]
            if path == prefix or path.startswith(prefix + "/"):
                return True
        elif fnmatch.fnmatch(path, pat):
            return True
    return False


def is_docs_only_file(path: str) -> bool:
    return matches_any(path, DOCS_ONLY_PATTERNS)


def classify_paths(paths: list[str] | None, *, full: bool) -> dict[str, object]:
    if full:
        return {
            "full": True,
            "rust": True,
            "rust_direct": True,
            "python": True,
            "console": True,
            "cloudflare": True,
            "bench": True,
            "container": True,
            "helm": True,
            "python_versions": ALL_PYTHON_VERSIONS,
        }

    # Unknown diff range (e.g. force-push or new branch): run all fast core gates safely.
    if paths is None:
        return {
            "full": False,
            "rust": True,
            "rust_direct": False,
            "python": True,
            "console": True,
            "cloudflare": True,
            "bench": False,
            "container": False,
            "helm": True,
            "python_versions": FAST_PYTHON_VERSIONS,
        }

    normalized = [p.strip().lstrip("./") for p in paths if p.strip()]
    if not normalized or all(is_docs_only_file(p) for p in normalized):
        return {
            "full": False,
            "rust": False,
            "rust_direct": False,
            "python": False,
            "console": False,
            "cloudflare": False,
            "bench": False,
            "container": False,
            "helm": False,
            "python_versions": FAST_PYTHON_VERSIONS,
        }

    if any(p in GLOBAL_CORE_FILES for p in normalized):
        rust = True
        python = True
        console = True
        cloudflare = True
        helm = True
    else:
        rust = any(
            p.startswith(("crates/", "plugins/", "examples/", "console/"))
            for p in normalized
        )
        python = any(
            p.startswith(("crates/", "clients/python/", "examples/python/", "deploy/cloudflare/"))
            for p in normalized
        )
        console = any(
            p.startswith("console/") or p == "deploy/cloudflare/src/native/proto.ts"
            for p in normalized
        )
        cloudflare = any(p.startswith("deploy/cloudflare/") for p in normalized)
        helm = any(p.startswith("deploy/helm/") for p in normalized)

    # Heavy jobs stay off on normal push/PR unless their own dedicated files changed.
    bench = any(p.startswith("bench/") for p in normalized)
    container = any(
        p in {"Dockerfile", ".dockerignore", "plugins/anyserve-auth-github/Dockerfile"}
        for p in normalized
    )
    rust_direct = any(
        "direct" in p and p.startswith("crates/")
        for p in normalized
    )

    return {
        "full": False,
        "rust": rust,
        "rust_direct": rust_direct,
        "python": python,
        "console": console,
        "cloudflare": cloudflare,
        "bench": bench,
        "container": container,
        "helm": helm,
        "python_versions": FAST_PYTHON_VERSIONS,
    }


def git_changed_files(base: str, head: str) -> list[str] | None:
    if not base or set(base) == {"0"}:
        return None
    for ref in (base, head):
        probe = subprocess.run(
            ["git", "cat-file", "-e", f"{ref}^{{commit}}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if probe.returncode != 0:
            return None
    out = subprocess.check_output(
        ["git", "diff", "--name-only", f"{base}...{head}"],
        text=True,
    )
    return [line for line in out.splitlines() if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute tiered CI job toggles.")
    parser.add_argument("--event", required=True, help="GitHub event name")
    parser.add_argument("--base", default="", help="Base commit SHA")
    parser.add_argument("--head", default="HEAD", help="Head commit SHA")
    parser.add_argument("--force-full", default="false", help="'true' to force full suite")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    args = parser.parse_args()

    full = args.force_full.lower() == "true" or args.event in {"schedule", "workflow_call"}
    changed: list[str] | None = None
    if not full and args.event in {"push", "pull_request"}:
        changed = git_changed_files(args.base, args.head)

    result = classify_paths(changed, full=full)

    print(f"CI mode: {'full' if result['full'] else 'fast'} (event={args.event})")
    if changed is not None:
        print(f"Changed files ({len(changed)}): {', '.join(changed[:20])}{' ...' if len(changed) > 20 else ''}")
    print(json.dumps(result, indent=2))

    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as fh:
            for key, value in result.items():
                if isinstance(value, bool):
                    fh.write(f"{key}={'true' if value else 'false'}\n")
                else:
                    fh.write(f"{key}={json.dumps(value, separators=(',', ':'))}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
