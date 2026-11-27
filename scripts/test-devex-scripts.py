#!/usr/bin/env python3
"""Automated hermetic tests for DevEx installer and release bump scripts."""

from __future__ import annotations

import functools
import hashlib
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import io
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
INSTALL_SH = ROOT / "scripts/install.sh"
INSTALL_PS1 = ROOT / "scripts/install.ps1"
BUMP_SCRIPT = ROOT / "scripts/bump-version.py"
CHECK_SCRIPT = ROOT / "scripts/check-release-version.py"


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


def build_fake_release_archive(dest_dir: Path, tag: str, target: str) -> tuple[Path, str]:
    archive_name = f"anyserve-{tag}-{target}.tar.gz"
    archive_path = dest_dir / archive_name
    binary_script = f"#!/bin/sh\necho 'anyserve {tag}'\n".encode("utf-8")

    with tarfile.open(archive_path, "w:gz") as tf:
        info = tarfile.TarInfo(name=f"anyserve-{tag}-{target}/anyserve")
        info.size = len(binary_script)
        info.mode = 0o755
        tf.addfile(info, io.BytesIO(binary_script))

    digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    return archive_path, digest


class DevExScriptsTest(unittest.TestCase):
    def test_install_sh_downloads_verifies_and_installs(self) -> None:
        tag = "v9.8.7"
        target = "x86_64-unknown-linux-gnu"
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            serve_dir = tmp_path / "serve"
            install_dir = tmp_path / "bin"
            serve_dir.mkdir()

            archive_path, digest = build_fake_release_archive(serve_dir, tag, target)
            checksum_file = serve_dir / f"anyserve-{tag}-SHA256SUMS.txt"
            checksum_file.write_text(f"{digest}  {archive_path.name}\n", encoding="utf-8")

            handler = functools.partial(QuietHandler, directory=str(serve_dir))
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            port = server.server_address[1]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                env = os.environ.copy()
                env.update(
                    {
                        "ANYSERVE_VERSION": tag,
                        "ANYSERVE_INSTALL_DIR": str(install_dir),
                        "ANYSERVE_BASE_URL": f"http://127.0.0.1:{port}",
                        "ANYSERVE_TARGET": target,
                    }
                )
                res = subprocess.run(
                    ["sh", str(INSTALL_SH)],
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(res.returncode, 0, msg=f"stdout={res.stdout}\nstderr={res.stderr}")
                self.assertIn("Verified SHA-256 checksum.", res.stdout)

                installed = install_dir / "anyserve"
                self.assertTrue(installed.is_file())
                mode = installed.stat().st_mode
                self.assertTrue(bool(mode & stat.S_IXUSR))

                run_bin = subprocess.run([str(installed)], capture_output=True, text=True, check=True)
                self.assertEqual(run_bin.stdout.strip(), f"anyserve {tag}")
            finally:
                server.shutdown()
                server.server_close()

    def test_install_sh_rejects_tampered_checksum(self) -> None:
        tag = "v9.8.7"
        target = "x86_64-unknown-linux-gnu"
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            serve_dir = tmp_path / "serve"
            install_dir = tmp_path / "bin"
            serve_dir.mkdir()

            archive_path, _digest = build_fake_release_archive(serve_dir, tag, target)
            checksum_file = serve_dir / f"anyserve-{tag}-SHA256SUMS.txt"
            checksum_file.write_text(f"{'0' * 64}  {archive_path.name}\n", encoding="utf-8")

            handler = functools.partial(QuietHandler, directory=str(serve_dir))
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            port = server.server_address[1]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                env = os.environ.copy()
                env.update(
                    {
                        "ANYSERVE_VERSION": tag,
                        "ANYSERVE_INSTALL_DIR": str(install_dir),
                        "ANYSERVE_BASE_URL": f"http://127.0.0.1:{port}",
                        "ANYSERVE_TARGET": target,
                    }
                )
                res = subprocess.run(
                    ["sh", str(INSTALL_SH)],
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertNotEqual(res.returncode, 0)
                self.assertIn("SHA-256 checksum mismatch", res.stderr)
                self.assertFalse((install_dir / "anyserve").exists())
            finally:
                server.shutdown()
                server.server_close()

    def test_install_ps1_security_invariants(self) -> None:
        text = INSTALL_PS1.read_text(encoding="utf-8")
        self.assertIn("Set-StrictMode -Version Latest", text)
        self.assertIn("$ErrorActionPreference = 'Stop'", text)
        self.assertIn("Get-FileHash -Path $ArchivePath -Algorithm SHA256", text)
        self.assertIn("SHA-256 checksum mismatch", text)
        self.assertNotIn("Invoke-Expression", text)

    def test_bump_version_dry_run_and_isolated_bump(self) -> None:
        # 1. Dry-run against real repo must succeed and not mutate files.
        dry = subprocess.run(
            [sys.executable, str(BUMP_SCRIPT), "9.9.9", "--dry-run"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(dry.returncode, 0, msg=dry.stderr)
        self.assertIn("Cargo.toml", dry.stdout)
        self.assertIn("console/src/ConnectWorkerModal.tsx", dry.stdout)

        # 2. Real bump inside a temporary copy of versioned manifests must pass check-release-version.py.
        files = [
            "Cargo.toml",
            "clients/python/pyproject.toml",
            "deploy/helm/anyserve/Chart.yaml",
            "deploy/vps/docker-compose.yml",
            "console/src/ConnectWorkerModal.tsx",
            "scripts/check-release-version.py",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            fake_root = Path(tmp)
            for rel in files:
                src = ROOT / rel
                dst = fake_root / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)

            res = subprocess.run(
                [
                    sys.executable,
                    str(BUMP_SCRIPT),
                    "9.9.9-rc.1",
                    "--chart-version",
                    "9.9.9-rc.1",
                    "--root",
                    str(fake_root),
                    "--skip-lock",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(res.returncode, 0, msg=f"stdout={res.stdout}\nstderr={res.stderr}")
            check = subprocess.run(
                [sys.executable, str(fake_root / "scripts/check-release-version.py"), "v9.9.9-rc.1"],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(check.returncode, 0, msg=check.stderr)
            self.assertEqual(check.stdout.strip(), "9.9.9-rc.1")
            chart_text = (fake_root / "deploy/helm/anyserve/Chart.yaml").read_text(encoding="utf-8")
            self.assertIn("version: 9.9.9-rc.1", chart_text)
            self.assertIn('appVersion: "9.9.9-rc.1"', chart_text)

    def test_ci_changes_classifier(self) -> None:
        import importlib.util

        spec = importlib.util.spec_from_file_location("ci_changes", ROOT / "scripts/ci-changes.py")
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # Docs-only changes skip all heavy and code jobs.
        docs_only = mod.classify_paths(
            ["README.md", "docs/deployment.md", "website/src/content/docs/index.mdx"],
            full=False,
        )
        self.assertFalse(docs_only["rust"])
        self.assertFalse(docs_only["python"])
        self.assertFalse(docs_only["console"])
        self.assertFalse(docs_only["cloudflare"])
        self.assertFalse(docs_only["bench"])
        self.assertFalse(docs_only["container"])

        # Normal Rust change runs fast core gates (Rust + Python 3.12), skips bench/container/rust_direct.
        rust_change = mod.classify_paths(["crates/anyserve-core/src/lib.rs"], full=False)
        self.assertTrue(rust_change["rust"])
        self.assertTrue(rust_change["python"])
        self.assertEqual(rust_change["python_versions"], ["3.12"])
        self.assertFalse(rust_change["bench"])
        self.assertFalse(rust_change["container"])
        self.assertFalse(rust_change["rust_direct"])

        # Dedicated bench/Dockerfile/direct changes opt in only those heavy jobs.
        targeted = mod.classify_paths(
            ["bench/load/console.py", ".dockerignore", "crates/anyserve-cli/tests/direct_runtime_e2e.rs"],
            full=False,
        )
        self.assertTrue(targeted["bench"])
        self.assertTrue(targeted["container"])
        self.assertTrue(targeted["rust_direct"])

        # Dotfile workflow change (.github/workflows/anyserve-ci.yml) triggers all fast core gates.
        ci_workflow = mod.classify_paths([".github/workflows/anyserve-ci.yml"], full=False)
        for key in ("rust", "python", "console", "cloudflare", "helm"):
            self.assertTrue(ci_workflow[key], f"expected {key}=True when CI workflow changes")
        self.assertFalse(ci_workflow["bench"])
        self.assertFalse(ci_workflow["container"])
        self.assertFalse(ci_workflow["rust_direct"])

        # Full mode (schedule / Release) enables everything and full Python matrix.
        full_mode = mod.classify_paths(["README.md"], full=True)
        for key in ("rust", "rust_direct", "python", "console", "cloudflare", "bench", "container", "helm"):
            self.assertTrue(full_mode[key], f"expected {key}=True in full mode")
        self.assertEqual(full_mode["python_versions"], ["3.10", "3.12", "3.14"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
