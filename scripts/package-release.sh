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

rustup target add "${TARGET_TRIPLE}"
cargo build --locked --release -p anyserve --target "${TARGET_TRIPLE}"

cp "target/${TARGET_TRIPLE}/release/${BINARY_NAME}" "${STAGING_DIR}/${BINARY_NAME}"
cp LICENSE README.md "${STAGING_DIR}/"

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
