#!/bin/sh
# AnyServe CLI installer for macOS and Linux (x86_64 & arm64).
# Usage:
#   curl -fsSL https://anyserve.github.io/anyserve/install.sh | sh
#   curl -fsSL https://anyserve.github.io/anyserve/install.sh | sh -s -- --version v0.6.0
# Environment overrides:
#   ANYSERVE_VERSION      Pin a release tag (e.g. v0.6.0 or 0.6.0)
#   ANYSERVE_INSTALL_DIR  Install directory (default: $HOME/.local/bin)
#   ANYSERVE_REPO         GitHub repo slug (default: anyserve/anyserve)
#   ANYSERVE_BASE_URL     Custom release download base URL (for mirrors / tests)
#   ANYSERVE_TARGET       Override detected target triple

set -eu

REPO="${ANYSERVE_REPO:-anyserve/anyserve}"
VERSION="${ANYSERVE_VERSION:-}"
INSTALL_DIR="${ANYSERVE_INSTALL_DIR:-${HOME}/.local/bin}"
BASE_URL="${ANYSERVE_BASE_URL:-}"
TARGET_OVERRIDE="${ANYSERVE_TARGET:-}"

say() {
  printf '%s\n' "$*"
}

err() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Install the AnyServe CLI binary (with embedded Console) for macOS or Linux.

Usage:
  install.sh [--version <tag>] [--dir <path>]

Options:
  -v, --version <tag>   Install a specific release tag (e.g. v0.6.0)
  -d, --dir <path>      Destination directory (default: ~/.local/bin)
  -h, --help            Show this help message

Environment:
  ANYSERVE_VERSION      Same as --version
  ANYSERVE_INSTALL_DIR  Same as --dir
  ANYSERVE_BASE_URL     Override release archive base URL
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -v|--version)
      [ $# -ge 2 ] || err "missing value for $1"
      VERSION="$2"
      shift 2
      ;;
    -d|--dir)
      [ $# -ge 2 ] || err "missing value for $1"
      INSTALL_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "unknown argument: $1 (try --help)"
      ;;
  esac
done

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || err "required command '$1' not found in PATH"
}

download() {
  url="$1"
  dest="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 3 --retry-delay 1 "$url" -o "$dest"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$dest" "$url"
  else
    err "curl or wget is required to download AnyServe"
  fi
}

resolve_latest_tag() {
  latest_url="https://github.com/${REPO}/releases/latest"
  tag=""
  if command -v curl >/dev/null 2>&1; then
    effective="$(curl -fsSL -o /dev/null -w '%{url_effective}' "$latest_url" 2>/dev/null || true)"
    case "$effective" in
      */releases/tag/*)
        tag="${effective##*/releases/tag/}"
        ;;
    esac
    if [ -z "$tag" ]; then
      api_json="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null || true)"
      tag="$(printf '%s' "$api_json" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
    fi
  elif command -v wget >/dev/null 2>&1; then
    api_json="$(wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null || true)"
    tag="$(printf '%s' "$api_json" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
  else
    err "curl or wget is required"
  fi
  [ -n "$tag" ] || err "failed to resolve latest release tag for ${REPO}; set --version vX.Y.Z"
  printf '%s' "$tag"
}

detect_target() {
  if [ -n "$TARGET_OVERRIDE" ]; then
    printf '%s' "$TARGET_OVERRIDE"
    return
  fi

  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)
      case "$arch" in
        x86_64|amd64)
          printf 'x86_64-unknown-linux-gnu'
          ;;
        aarch64|arm64)
          printf 'aarch64-unknown-linux-gnu'
          ;;
        *)
          err "unsupported Linux architecture: ${arch} (supported: x86_64, aarch64)"
          ;;
      esac
      ;;
    Darwin)
      case "$arch" in
        arm64|aarch64)
          printf 'aarch64-apple-darwin'
          ;;
        x86_64)
          # Prefer native arm64 binary when running under Rosetta 2 on Apple Silicon.
          if [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = "1" ]; then
            printf 'aarch64-apple-darwin'
          else
            printf 'x86_64-apple-darwin'
          fi
          ;;
        *)
          err "unsupported macOS architecture: ${arch} (supported: arm64, x86_64)"
          ;;
      esac
      ;;
    *)
      err "unsupported OS: ${os}. For Windows, use: irm https://anyserve.github.io/anyserve/install.ps1 | iex"
      ;;
  esac
}

compute_sha256() {
  file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  elif command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$file" | awk '{print $NF}'
  else
    err "sha256sum, shasum, or openssl is required to verify download integrity"
  fi
}

need_cmd tar

TARGET="$(detect_target)"

if [ -z "$VERSION" ]; then
  VERSION="$(resolve_latest_tag)"
fi

case "$VERSION" in
  v*) TAG="$VERSION" ;;
  *) TAG="v${VERSION}" ;;
esac

ARCHIVE="anyserve-${TAG}-${TARGET}.tar.gz"
CHECKSUMS="anyserve-${TAG}-SHA256SUMS.txt"

if [ -n "$BASE_URL" ]; then
  DOWNLOAD_BASE="${BASE_URL%/}"
else
  DOWNLOAD_BASE="https://github.com/${REPO}/releases/download/${TAG}"
fi

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT INT TERM

say "Installing AnyServe ${TAG} (${TARGET})..."

download "${DOWNLOAD_BASE}/${ARCHIVE}" "${TMPDIR}/${ARCHIVE}" \
  || err "failed to download ${DOWNLOAD_BASE}/${ARCHIVE}"
download "${DOWNLOAD_BASE}/${CHECKSUMS}" "${TMPDIR}/${CHECKSUMS}" \
  || err "failed to download ${DOWNLOAD_BASE}/${CHECKSUMS}"

EXPECTED="$(awk -v name="$ARCHIVE" '$2 == name || $2 == "*" name { print $1; exit }' "${TMPDIR}/${CHECKSUMS}")"
[ -n "$EXPECTED" ] || err "checksum entry for ${ARCHIVE} not found in ${CHECKSUMS}"

ACTUAL="$(compute_sha256 "${TMPDIR}/${ARCHIVE}")"
if [ "$ACTUAL" != "$EXPECTED" ]; then
  err "SHA-256 checksum mismatch for ${ARCHIVE} (expected ${EXPECTED}, got ${ACTUAL})"
fi
say "Verified SHA-256 checksum."

tar -xzf "${TMPDIR}/${ARCHIVE}" -C "${TMPDIR}"

BIN_SRC="${TMPDIR}/anyserve-${TAG}-${TARGET}/anyserve"
if [ ! -f "$BIN_SRC" ]; then
  BIN_SRC="${TMPDIR}/anyserve"
fi
[ -f "$BIN_SRC" ] || err "expected 'anyserve' binary not found in archive ${ARCHIVE}"

mkdir -p "$INSTALL_DIR"
DEST="${INSTALL_DIR}/anyserve"
TMP_DEST="${INSTALL_DIR}/.anyserve.tmp.$$"
cp "$BIN_SRC" "$TMP_DEST"
chmod 0755 "$TMP_DEST"
mv -f "$TMP_DEST" "$DEST"

say "Installed AnyServe to ${DEST}"

case ":${PATH}:" in
  *":${INSTALL_DIR}:"*)
    ;;
  *)
    say ""
    say "Add ${INSTALL_DIR} to your PATH to run 'anyserve' directly:"
    say "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    ;;
esac

say ""
say "Quick start:"
say "  anyserve serve --console-listen 127.0.0.1:3001 --auto-token"
