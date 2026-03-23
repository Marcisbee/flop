#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

VERSION="8.0.4"
VARIANT=""
URL=""

for arg in "$@"; do
  case "$arg" in
    --version=*)
      VERSION="${arg#*=}"
      ;;
    --variant=*)
      VARIANT="${arg#*=}"
      ;;
    --url=*)
      URL="${arg#*=}"
      ;;
    -h|--help)
      cat <<'EOF'
Download a local mongod binary into this repo (gitignored).

Usage:
  ./benchmarks/finance-mongodb/download-mongod.sh [--version=8.0.4]
  ./benchmarks/finance-mongodb/download-mongod.sh --url=https://fastdl.mongodb.org/osx/mongodb-macos-arm64-8.0.4.tgz

Options:
  --version=...   MongoDB version used with auto URL generation.
  --variant=...   Archive prefix (ex: mongodb-macos-arm64, mongodb-linux-x86_64-ubuntu2204).
  --url=...       Full .tgz URL (overrides auto URL generation).
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 1
      ;;
  esac
done

detect_default_variant() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  if [[ "${os}" == "Darwin" && "${arch}" == "arm64" ]]; then
    echo "mongodb-macos-arm64"
    return
  fi
  if [[ "${os}" == "Darwin" && "${arch}" == "x86_64" ]]; then
    echo "mongodb-macos-x86_64"
    return
  fi
  if [[ "${os}" == "Linux" && "${arch}" == "x86_64" ]]; then
    echo "mongodb-linux-x86_64-ubuntu2204"
    return
  fi
  if [[ "${os}" == "Linux" && "${arch}" == "aarch64" ]]; then
    echo "mongodb-linux-aarch64-ubuntu2204"
    return
  fi
  echo ""
}

if [[ -z "${VARIANT}" ]]; then
  VARIANT="$(detect_default_variant)"
fi

if [[ -z "${URL}" ]]; then
  if [[ -z "${VARIANT}" ]]; then
    echo "Could not auto-detect MongoDB variant for this platform. Use --variant or --url." >&2
    exit 1
  fi
  if [[ "${VARIANT}" == mongodb-macos-* ]]; then
    URL="https://fastdl.mongodb.org/osx/${VARIANT}-${VERSION}.tgz"
  else
    URL="https://fastdl.mongodb.org/linux/${VARIANT}-${VERSION}.tgz"
  fi
fi

TARGET_DIR="${REPO_ROOT}/benchmarks/.tools/mongodb/${VARIANT}-${VERSION}"
TARGET_BIN="${TARGET_DIR}/bin/mongod"
STABLE_BIN="${REPO_ROOT}/benchmarks/.tools/mongodb/mongod"

if [[ -x "${TARGET_BIN}" ]]; then
  ln -sf "${TARGET_BIN}" "${STABLE_BIN}" || true
  chmod +x "${STABLE_BIN}" || true
  echo "mongod already downloaded: ${TARGET_BIN}"
  echo "stable path: ${STABLE_BIN}"
  exit 0
fi

mkdir -p "${REPO_ROOT}/benchmarks/.tools/mongodb"
TMP_DIR="$(mktemp -d)"
ARCHIVE="${TMP_DIR}/mongodb.tgz"
EXTRACT_DIR="${TMP_DIR}/extract"

echo "Downloading ${URL}"
curl -fsSL "${URL}" -o "${ARCHIVE}"

mkdir -p "${EXTRACT_DIR}"
tar -xzf "${ARCHIVE}" -C "${EXTRACT_DIR}" --strip-components=1

rm -rf "${TARGET_DIR}"
mkdir -p "${TARGET_DIR}"
cp -R "${EXTRACT_DIR}/." "${TARGET_DIR}/"
chmod +x "${TARGET_BIN}" || true
ln -sf "${TARGET_BIN}" "${STABLE_BIN}" || true
chmod +x "${STABLE_BIN}" || true

echo "Downloaded mongod to: ${TARGET_BIN}"
echo "stable path: ${STABLE_BIN}"
