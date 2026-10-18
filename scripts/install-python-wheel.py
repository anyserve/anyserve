"""Install the newest local SDK wheel using the current Python interpreter."""

import subprocess
import sys
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    wheels = [
        wheel
        for directory in (root / "clients/python/dist", root / "target/wheels")
        for wheel in directory.glob("anyserve_runtime-*.whl")
    ]
    if not wheels:
        raise SystemExit("no SDK wheel found; run `mise run python-sdk` first")
    wheel = max(wheels, key=lambda path: path.stat().st_mtime)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--force-reinstall", str(wheel)], check=True
    )


if __name__ == "__main__":
    main()
