#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: build-release-artifacts.sh [--output-dir releases] [--version X.Y.Z] [--host-triple triple]

Builds host-native release artifacts:
- tarball: ragmail-v<version>-<suffix>.tar.gz
- optional .deb on Linux (amd64/arm64) when dpkg-deb is available

Each tarball includes:
- ragmail      (Rust CLI entrypoint)
- ragmail-py   (PyInstaller Python bridge executable)
EOF
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

output_dir="releases"
version=""
host_triple=""

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
    --host-triple)
      host_triple="${2:-}"
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

if [[ -z "${host_triple}" ]]; then
  host_triple="$(rustc -vV | awk '/^host: / { print $2 }')"
fi

if [[ -z "${host_triple}" ]]; then
  echo "unable to detect host triple" >&2
  exit 1
fi

case "${host_triple}" in
  x86_64-apple-darwin)
    suffix="macos-amd64"
    ;;
  aarch64-apple-darwin)
    suffix="macos-arm64"
    ;;
  x86_64-unknown-linux-gnu)
    suffix="linux-amd64"
    deb_arch="amd64"
    ;;
  aarch64-unknown-linux-gnu)
    suffix="linux-arm64"
    deb_arch="arm64"
    ;;
  *)
    suffix="${host_triple}"
    ;;
esac

cargo build --manifest-path rust/Cargo.toml --release -p ragmail-cli

rust_binary_path="rust/target/release/ragmail"
if [[ ! -x "${rust_binary_path}" ]]; then
  echo "release binary missing at ${rust_binary_path}" >&2
  exit 1
fi

bridge_build_dir="$(mktemp -d)"
trap 'rm -rf "${bridge_build_dir}"' EXIT
./just.d/scripts/build-python-bridge.sh --output-dir "${bridge_build_dir}"
bridge_binary_path="${bridge_build_dir}/ragmail-py"
if [[ ! -x "${bridge_binary_path}" ]]; then
  echo "bridge binary missing at ${bridge_binary_path}" >&2
  exit 1
fi

mkdir -p "${output_dir}"
base_name="ragmail-v${version}-${suffix}"
tarball_path="${output_dir}/${base_name}.tar.gz"
bundle_dir="$(mktemp -d)"
trap 'rm -rf "${bridge_build_dir}" "${bundle_dir}"' EXIT
cp "${rust_binary_path}" "${bundle_dir}/ragmail"
cp "${bridge_binary_path}" "${bundle_dir}/ragmail-py"
chmod 0755 "${bundle_dir}/ragmail" "${bundle_dir}/ragmail-py"
tar -C "${bundle_dir}" -czf "${tarball_path}" ragmail ragmail-py
echo "Built ${tarball_path}"

artifacts=("${tarball_path}")
if [[ "${host_triple}" == *"unknown-linux-gnu" ]] && command -v dpkg-deb >/dev/null 2>&1; then
  deb_path="${output_dir}/ragmail_${version}_${deb_arch}.deb"
  ./just.d/scripts/package-deb.sh \
    --version "${version}" \
    --arch "${deb_arch}" \
    --binary "${rust_binary_path}" \
    --bridge-binary "${bridge_binary_path}" \
    --output "${deb_path}"
  artifacts+=("${deb_path}")
fi

(
  cd "${output_dir}"
  rm -f SHA256SUMS
  for artifact in "${artifacts[@]}"; do
    shasum -a 256 "$(basename "${artifact}")"
  done > SHA256SUMS
)
echo "Built ${output_dir}/SHA256SUMS"
