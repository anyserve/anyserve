#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <version-tag> <target-triple> [output-dir]" >&2
  exit 1
fi

VERSION_TAG="$1"
TARGET_TRIPLE="$2"
OUTPUT_DIR="${3:-dist}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PACKAGE_NAME="anyserve"
ARCHIVE_BASENAME="${PACKAGE_NAME}-${VERSION_TAG}-${TARGET_TRIPLE}"
STAGING_DIR="${REPO_ROOT}/target/release-artifacts/${ARCHIVE_BASENAME}"

BINARY_NAME="anyserve"
ARCHIVE_EXT="tar.gz"

case "${TARGET_TRIPLE}" in
  *-pc-windows-*)
    BINARY_NAME="anyserve.exe"
    ARCHIVE_EXT="zip"
    ;;
esac

ARCHIVE_PATH="${REPO_ROOT}/${OUTPUT_DIR}/${ARCHIVE_BASENAME}.${ARCHIVE_EXT}"

mkdir -p "${REPO_ROOT}/${OUTPUT_DIR}"
rm -rf "${STAGING_DIR}"
mkdir -p "${STAGING_DIR}"

pushd "${REPO_ROOT}" >/dev/null

if [[ "${SKIP_CONSOLE_BUILD:-0}" != "1" ]]; then
  rm -rf "${REPO_ROOT}/console/dist"
  (cd "${REPO_ROOT}/console" && npm ci && npm run build)
fi

rustup target add "${TARGET_TRIPLE}"

if [[ -n "${CARGO_ZIGBUILD_GLIBC:-}" && "${TARGET_TRIPLE}" == *-unknown-linux-gnu ]]; then
  cargo zigbuild --locked --release -p anyserve --features embed-console \
    --target "${TARGET_TRIPLE}.${CARGO_ZIGBUILD_GLIBC}"
else
  cargo build --locked --release -p anyserve --features embed-console --target "${TARGET_TRIPLE}"
fi

cp "target/${TARGET_TRIPLE}/release/${BINARY_NAME}" "${STAGING_DIR}/${BINARY_NAME}"
cp LICENSE README.md "${STAGING_DIR}/"

MAX_GLIBC="${GLIBC_MAX_VERSION:-${CARGO_ZIGBUILD_GLIBC:-}}"
if [[ -n "${MAX_GLIBC}" && "${TARGET_TRIPLE}" == *-unknown-linux-gnu ]]; then
  command -v readelf >/dev/null 2>&1 || { echo "readelf is required to verify GLIBC ceiling" >&2; exit 1; }
  python3 - "${STAGING_DIR}/${BINARY_NAME}" "${MAX_GLIBC}" <<'PY'
import re
import subprocess
import sys

binary = sys.argv[1]
max_ver = tuple(int(p) for p in sys.argv[2].split("."))
out = subprocess.check_output(["readelf", "-V", binary], text=True)
versions = sorted(
    {tuple(int(p) for p in m.split(".")) for m in re.findall(r"GLIBC_([0-9]+\.[0-9]+)", out)}
)
too_new = [v for v in versions if v > max_ver]
fmt = lambda t: ".".join(str(x) for x in t)
print(
    f"ELF GLIBC symbols in {binary}: "
    f"{', '.join(fmt(v) for v in versions) or 'none'} (max allowed: {fmt(max_ver)})"
)
if too_new:
    sys.exit(
        f"Binary requires GLIBC > {fmt(max_ver)}: {', '.join(fmt(v) for v in too_new)}"
    )
PY
fi

if [[ "${ARCHIVE_EXT}" == "zip" ]]; then
  pushd "$(dirname "${STAGING_DIR}")" >/dev/null
  pwsh -NoLogo -NoProfile -Command "Compress-Archive -Path '${ARCHIVE_BASENAME}' -DestinationPath '${ARCHIVE_BASENAME}.zip' -Force"
  mv "${ARCHIVE_BASENAME}.zip" "${ARCHIVE_PATH}"
  popd >/dev/null
else
  tar -C "$(dirname "${STAGING_DIR}")" -czf "${ARCHIVE_PATH}" "$(basename "${STAGING_DIR}")"
fi

popd >/dev/null

echo "${ARCHIVE_PATH}"
