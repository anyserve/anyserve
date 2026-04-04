#!/bin/sh

set -eu

REPO="${ANYSERVE_INSTALL_REPO:-anyserve/anyserve}"
INSTALL_DIR="${ANYSERVE_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${ANYSERVE_VERSION:-latest}"

usage() {
  cat <<'EOF'
Install AnyServe from GitHub Releases.

Usage:
  install.sh [--version <tag>] [--dir <path>] [--repo <owner/name>]

Options:
  --version <tag>  Install a specific tag such as v0.2.0. Defaults to latest.
  --dir <path>     Install directory. Defaults to ~/.local/bin.
  --repo <repo>    GitHub repository to download from. Defaults to anyserve/anyserve.
  -h, --help       Show this help.

Environment:
  ANYSERVE_VERSION
  ANYSERVE_INSTALL_DIR
  ANYSERVE_INSTALL_REPO
EOF
}

log() {
  printf '%s\n' "$*" >&2
}

die() {
  log "error: $*"
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

http_get() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -H "Accept: application/vnd.github+json" -H "User-Agent: anyserve-install" "$1"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -qO- --header="Accept: application/vnd.github+json" --user-agent="anyserve-install" "$1"
    return
  fi

  die "either curl or wget is required"
}

download_file() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "$2" "$1"
    return
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
    return
  fi

  die "either curl or wget is required"
}

sha256_cmd() {
  if command -v sha256sum >/dev/null 2>&1; then
    printf 'sha256sum'
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    printf 'shasum -a 256'
    return
  fi

  printf ''
}

detect_target() {
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)
      case "$arch" in
        x86_64|amd64)
          printf 'x86_64-unknown-linux-gnu'
          return
          ;;
        aarch64|arm64)
          die "prebuilt Linux arm64 binaries are not published; build from source instead"
          ;;
      esac
      ;;
    Darwin)
      case "$arch" in
        x86_64|amd64)
          printf 'x86_64-apple-darwin'
          return
          ;;
        aarch64|arm64)
          printf 'aarch64-apple-darwin'
          return
          ;;
      esac
      ;;
    *)
      die "unsupported operating system: $os"
      ;;
  esac

  die "unsupported architecture for ${os}: ${arch}"
}

resolve_version() {
  if [ "$VERSION" != "latest" ]; then
    printf '%s' "$VERSION"
    return
  fi

  api_url="https://api.github.com/repos/${REPO}/releases/latest"
  json="$(http_get "$api_url")"
  tag="$(printf '%s' "$json" | tr -d '\n' | sed -n 's/.*"tag_name":"\([^"]*\)".*/\1/p')"
  [ -n "$tag" ] || die "failed to resolve latest release tag from ${api_url}"
  printf '%s' "$tag"
}

verify_checksum() {
  checksum_tool="$(sha256_cmd)"

  if [ -z "$checksum_tool" ]; then
    log "warning: no sha256 tool found; skipping checksum verification"
    return
  fi

  expected_line="$(grep "  ${ARCHIVE_NAME}\$" "$CHECKSUM_FILE" || true)"
  [ -n "$expected_line" ] || die "checksum for ${ARCHIVE_NAME} not found in ${CHECKSUM_FILE}"

  actual_line="$($checksum_tool "$ARCHIVE_FILE")"
  actual_sum="$(printf '%s' "$actual_line" | awk '{print $1}')"
  expected_sum="$(printf '%s' "$expected_line" | awk '{print $1}')"

  [ "$actual_sum" = "$expected_sum" ] || die "checksum verification failed for ${ARCHIVE_NAME}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version)
      [ $# -ge 2 ] || die "--version requires a value"
      VERSION="$2"
      shift 2
      ;;
    --dir)
      [ $# -ge 2 ] || die "--dir requires a value"
      INSTALL_DIR="$2"
      shift 2
      ;;
    --repo)
      [ $# -ge 2 ] || die "--repo requires a value"
      REPO="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

need_cmd tar
need_cmd mktemp

TARGET="$(detect_target)"
VERSION_TAG="$(resolve_version)"
ARCHIVE_NAME="anyserve-${VERSION_TAG}-${TARGET}.tar.gz"
CHECKSUM_NAME="anyserve-${VERSION_TAG}-SHA256SUMS.txt"
DOWNLOAD_BASE="https://github.com/${REPO}/releases/download/${VERSION_TAG}"

TMP_DIR="$(mktemp -d)"
ARCHIVE_FILE="${TMP_DIR}/${ARCHIVE_NAME}"
CHECKSUM_FILE="${TMP_DIR}/${CHECKSUM_NAME}"
EXTRACT_DIR="${TMP_DIR}/extract"
ARCHIVE_ROOT="anyserve-${VERSION_TAG}-${TARGET}"

cleanup() {
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT INT TERM

log "Installing AnyServe ${VERSION_TAG} for ${TARGET}"
download_file "${DOWNLOAD_BASE}/${ARCHIVE_NAME}" "${ARCHIVE_FILE}"
download_file "${DOWNLOAD_BASE}/${CHECKSUM_NAME}" "${CHECKSUM_FILE}"
verify_checksum

mkdir -p "$EXTRACT_DIR"
tar -xzf "$ARCHIVE_FILE" -C "$EXTRACT_DIR"

BINARY_PATH="${EXTRACT_DIR}/${ARCHIVE_ROOT}/anyserve"
[ -f "$BINARY_PATH" ] || die "release archive did not contain ${ARCHIVE_ROOT}/anyserve"

mkdir -p "$INSTALL_DIR"
[ -w "$INSTALL_DIR" ] || die "install directory is not writable: ${INSTALL_DIR}"

install -m 0755 "$BINARY_PATH" "${INSTALL_DIR}/anyserve"

log "Installed anyserve to ${INSTALL_DIR}/anyserve"
case ":$PATH:" in
  *:"${INSTALL_DIR}":*)
    ;;
  *)
    log "warning: ${INSTALL_DIR} is not on your PATH"
    ;;
esac

"${INSTALL_DIR}/anyserve" --version
