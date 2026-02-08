#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: build-release-artifacts.sh [--output-dir releases] [--version X.Y.Z] [--platform host|linux/amd64|linux/arm64|macos/amd64|macos/arm64]

Builds release artifacts for the requested platform.

Important:
- `ragmail-py` is built by PyInstaller and must be built on the target OS/arch.
- Cross-platform requests on a non-matching host fail with guidance.

Generated artifacts:
- tarball: ragmail-v<version>-<suffix>.tar.gz
- optional .deb on Linux (amd64/arm64) when `dpkg-deb` is available

Each tarball includes:
- ragmail      (Rust CLI entrypoint)
- ragmail-py   (PyInstaller Python bridge executable)
EOF
}

target_for_platform() {
  case "$1" in
    macos/amd64)
      echo "x86_64-apple-darwin"
      ;;
    macos/arm64)
      echo "aarch64-apple-darwin"
      ;;
    linux/amd64)
      echo "x86_64-unknown-linux-gnu"
      ;;
    linux/arm64)
      echo "aarch64-unknown-linux-gnu"
      ;;
    *)
      return 1
      ;;
  esac
}

suffix_for_target() {
  case "$1" in
    x86_64-apple-darwin)
      echo "macos-amd64"
      ;;
    aarch64-apple-darwin)
      echo "macos-arm64"
      ;;
    x86_64-unknown-linux-gnu)
      echo "linux-amd64"
      ;;
    aarch64-unknown-linux-gnu)
      echo "linux-arm64"
      ;;
    *)
      echo "$1"
      ;;
  esac
}

platform_for_target() {
  case "$1" in
    x86_64-apple-darwin)
      echo "macos/amd64"
      ;;
    aarch64-apple-darwin)
      echo "macos/arm64"
      ;;
    x86_64-unknown-linux-gnu)
      echo "linux/amd64"
      ;;
    aarch64-unknown-linux-gnu)
      echo "linux/arm64"
      ;;
    *)
      echo "$1"
      ;;
  esac
}

deb_arch_for_target() {
  case "$1" in
    x86_64-unknown-linux-gnu)
      echo "amd64"
      ;;
    aarch64-unknown-linux-gnu)
      echo "arm64"
      ;;
    *)
      echo ""
      ;;
  esac
}

canonicalize_platform() {
  local raw
  raw="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "${raw}" in
    host)
      echo "host"
      ;;
    macos/amd64|macos/x86_64|darwin/amd64|darwin/x86_64|macos-amd64|macos-x86_64|darwin-amd64|darwin-x86_64)
      echo "macos/amd64"
      ;;
    macos/arm64|macos/aarch64|darwin/arm64|darwin/aarch64|macos-arm64|macos-aarch64|darwin-arm64|darwin-aarch64)
      echo "macos/arm64"
      ;;
    linux/amd64|linux/x86_64|linux-amd64|linux-x86_64)
      echo "linux/amd64"
      ;;
    linux/arm64|linux/aarch64|linux-arm64|linux-aarch64)
      echo "linux/arm64"
      ;;
    *)
      return 1
      ;;
  esac
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

output_dir="releases"
version=""
platform="host"
legacy_host_triple=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      output_dir="${2:-}"
      shift 2
      ;;
    --version)
      version="${2:-}"
      shift 2
      ;;
    --platform)
      platform="${2:-}"
      shift 2
      ;;
    --host-triple)
      legacy_host_triple="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${version}" ]]; then
  if [[ ! -f VERSION ]]; then
    echo "VERSION file is missing" >&2
    exit 1
  fi
  version="$(tr -d '[:space:]' < VERSION)"
fi

if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "version must be semantic x.y.z, got: ${version}" >&2
  exit 1
fi

if ! platform="$(canonicalize_platform "${platform}")"; then
  echo "unsupported platform: ${platform}" >&2
  usage >&2
  exit 2
fi

if [[ -n "${legacy_host_triple}" && "${platform}" != "host" ]]; then
  echo "cannot combine --host-triple with --platform" >&2
  exit 2
fi

host_triple="$(rustc -vV | awk '/^host: / { print $2 }')"
if [[ -z "${host_triple}" ]]; then
  echo "unable to detect host triple" >&2
  exit 1
fi

if [[ -n "${legacy_host_triple}" ]]; then
  target_triple="${legacy_host_triple}"
elif [[ "${platform}" == "host" ]]; then
  target_triple="${host_triple}"
else
  target_triple="$(target_for_platform "${platform}")"
fi

if [[ "${target_triple}" != "${host_triple}" ]]; then
  target_platform="$(platform_for_target "${target_triple}")"
  host_platform="$(platform_for_target "${host_triple}")"
  echo "cross-platform build requested (${target_platform}) from host (${host_platform})." >&2
  echo "ragmail-py is built by PyInstaller and must be built on the target OS/arch." >&2
  echo "Run this on a ${target_platform} machine or use .github/workflows/release.yml." >&2
  exit 1
fi

suffix="$(suffix_for_target "${target_triple}")"
deb_arch="$(deb_arch_for_target "${target_triple}")"

cargo build --manifest-path rust/Cargo.toml --release -p ragmail-cli --target "${target_triple}"

rust_binary_path="rust/target/${target_triple}/release/ragmail"
if [[ ! -x "${rust_binary_path}" ]]; then
  echo "release binary missing at ${rust_binary_path}" >&2
  exit 1
fi

bridge_build_dir="$(mktemp -d)"
bundle_dir="$(mktemp -d)"
cleanup() {
  rm -rf "${bridge_build_dir}" "${bundle_dir}"
}
trap cleanup EXIT

./just.d/scripts/build-python-bridge.sh --output-dir "${bridge_build_dir}"
bridge_binary_path="${bridge_build_dir}/ragmail-py"
if [[ ! -x "${bridge_binary_path}" ]]; then
  echo "bridge binary missing at ${bridge_binary_path}" >&2
  exit 1
fi

mkdir -p "${output_dir}"
base_name="ragmail-v${version}-${suffix}"
tarball_path="${output_dir}/${base_name}.tar.gz"
cp "${rust_binary_path}" "${bundle_dir}/ragmail"
cp "${bridge_binary_path}" "${bundle_dir}/ragmail-py"
chmod 0755 "${bundle_dir}/ragmail" "${bundle_dir}/ragmail-py"

if [[ "${RAGMAIL_RELEASE_RUNTIME_SMOKE:-1}" == "1" ]]; then
  if ! "${bundle_dir}/ragmail" version >/dev/null 2>&1; then
    echo "warning: runtime smoke failed to execute bundled ragmail (continuing)." >&2
  fi
  if ! "${bundle_dir}/ragmail" search --help >/dev/null 2>&1; then
    echo "warning: runtime smoke failed to execute bundled ragmail passthrough (continuing)." >&2
    echo "         set RAGMAIL_RELEASE_RUNTIME_SMOKE=0 to skip runtime smoke probes." >&2
  fi
fi

tar -C "${bundle_dir}" -czf "${tarball_path}" ragmail ragmail-py
echo "Built ${tarball_path}"

artifacts=("${tarball_path}")
if [[ -n "${deb_arch}" ]] && command -v dpkg-deb >/dev/null 2>&1; then
  deb_path="${output_dir}/ragmail_${version}_${deb_arch}.deb"
  ./just.d/scripts/package-deb.sh \
    --version "${version}" \
    --arch "${deb_arch}" \
    --binary "${rust_binary_path}" \
    --bridge-binary "${bridge_binary_path}" \
    --output "${deb_path}"
  artifacts+=("${deb_path}")
elif [[ -n "${deb_arch}" ]]; then
  echo "Skipping .deb packaging: dpkg-deb not found." >&2
fi

(
  cd "${output_dir}"
  rm -f SHA256SUMS
  for artifact in "${artifacts[@]}"; do
    shasum -a 256 "$(basename "${artifact}")"
  done > SHA256SUMS
)
echo "Built ${output_dir}/SHA256SUMS"
