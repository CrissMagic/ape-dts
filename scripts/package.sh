#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/package.sh <target> [output-directory]

Targets:
  centos7-x86_64  Linux x86_64, glibc 2.17 (CentOS/RHEL 7)
  linux-x86_64    Linux x86_64, glibc 2.31 or newer
  linux-arm64     Linux ARM64, glibc 2.31 or newer

Output:
  ape-dts-<version>-<target>.tar.gz
  ape-dts-<version>-<target>.tar.gz.sha256

The package contains ape-dts, ape-dts.sh, and log4rs.yaml. Task configuration is not included.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

TARGET_ENVIRONMENT="${1:-}"
if [[ -z "${TARGET_ENVIRONMENT}" ]]; then
  usage >&2
  exit 1
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

case "${TARGET_ENVIRONMENT}" in
  centos7-x86_64)
    TARGET_TRIPLE="x86_64-unknown-linux-gnu"
    CROSS_CONFIG_PATH="${REPO_ROOT}/scripts/cross/centos7-x86_64.toml"
    ;;
  linux-x86_64)
    TARGET_TRIPLE="x86_64-unknown-linux-gnu"
    CROSS_CONFIG_PATH="${REPO_ROOT}/Cross.toml"
    ;;
  linux-arm64)
    TARGET_TRIPLE="aarch64-unknown-linux-gnu"
    CROSS_CONFIG_PATH="${REPO_ROOT}/Cross.toml"
    ;;
  *)
    echo "unsupported target '${TARGET_ENVIRONMENT}'; run bash ./scripts/package.sh --help" >&2
    exit 1
    ;;
esac

BUILD_TARGET_DIR="target/package/${TARGET_ENVIRONMENT}"

for command in cargo cross docker tar; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command '${command}' was not found" >&2
    exit 1
  fi
done

if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  echo "required command 'sha256sum' or 'shasum' was not found" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker is installed but the daemon is not available" >&2
  exit 1
fi

if [[ ! -f "${CROSS_CONFIG_PATH}" ]]; then
  echo "cross config not found: ${CROSS_CONFIG_PATH}" >&2
  exit 1
fi

VERSION=$(sed -n 's/^version[[:space:]]*=[[:space:]]*"\([^"]*\)"/\1/p' "${REPO_ROOT}/dt-main/Cargo.toml" | head -n 1)
if [[ -z "${VERSION}" ]]; then
  echo "failed to read dt-main version" >&2
  exit 1
fi

OUTPUT_ARGUMENT="${2:-dist}"
if [[ "${OUTPUT_ARGUMENT}" = /* ]]; then
  OUTPUT_DIR="${OUTPUT_ARGUMENT}"
else
  OUTPUT_DIR="${REPO_ROOT}/${OUTPUT_ARGUMENT}"
fi
mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR=$(cd "${OUTPUT_DIR}" && pwd)

STAGE_DIR=$(mktemp -d "${OUTPUT_DIR}/.package.XXXXXX")
HAD_CROSS_CONFIG=0
PREVIOUS_CROSS_CONFIG=""
if [[ -n "${CROSS_CONFIG+x}" ]]; then
  HAD_CROSS_CONFIG=1
  PREVIOUS_CROSS_CONFIG="${CROSS_CONFIG}"
fi

cleanup() {
  if [[ "${STAGE_DIR}" == "${OUTPUT_DIR}"/.package.* ]]; then
    rm -rf -- "${STAGE_DIR}"
  fi
  if [[ "${HAD_CROSS_CONFIG}" == "1" ]]; then
    export CROSS_CONFIG="${PREVIOUS_CROSS_CONFIG}"
  else
    unset CROSS_CONFIG
  fi
}
trap cleanup EXIT

export CROSS_CONFIG="${CROSS_CONFIG_PATH}"
cd "${REPO_ROOT}"

cross build --locked --release --target "${TARGET_TRIPLE}" --target-dir "${BUILD_TARGET_DIR}" -p dt-main --bin dt-main --features metrics

BINARY_PATH="${REPO_ROOT}/${BUILD_TARGET_DIR}/${TARGET_TRIPLE}/release/dt-main"
if [[ ! -f "${BINARY_PATH}" ]]; then
  echo "build succeeded but binary was not found: ${BINARY_PATH}" >&2
  exit 1
fi

cp "${BINARY_PATH}" "${STAGE_DIR}/ape-dts"
cp "${REPO_ROOT}/scripts/ape-dts.sh" "${STAGE_DIR}/ape-dts.sh"
cp "${REPO_ROOT}/log4rs.yaml" "${STAGE_DIR}/log4rs.yaml"
chmod 0755 "${STAGE_DIR}/ape-dts"
chmod 0755 "${STAGE_DIR}/ape-dts.sh"
chmod 0644 "${STAGE_DIR}/log4rs.yaml"

ARCHIVE_NAME="ape-dts-${VERSION}-${TARGET_ENVIRONMENT}.tar.gz"
ARCHIVE_PATH="${OUTPUT_DIR}/${ARCHIVE_NAME}"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"
rm -f -- "${ARCHIVE_PATH}" "${CHECKSUM_PATH}"
tar -czf "${ARCHIVE_PATH}" -C "${STAGE_DIR}" ape-dts ape-dts.sh log4rs.yaml

if command -v sha256sum >/dev/null 2>&1; then
  (cd "${OUTPUT_DIR}" && sha256sum "${ARCHIVE_NAME}") > "${CHECKSUM_PATH}"
else
  (cd "${OUTPUT_DIR}" && shasum -a 256 "${ARCHIVE_NAME}") > "${CHECKSUM_PATH}"
fi

echo "package: ${ARCHIVE_PATH}"
echo "sha256: ${CHECKSUM_PATH}"
