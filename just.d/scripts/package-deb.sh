#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: package-deb.sh --version X.Y.Z --arch amd64|arm64 --binary /path/to/ragmail --bridge-binary /path/to/ragmail-py --output /path/to/pkg.deb [--package-name ragmail]
EOF
}

version=""
arch=""
binary=""
bridge_binary=""
output=""
package_name="ragmail"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      version="${2:-}"
      shift 2
      ;;
    --arch)
      arch="${2:-}"
      shift 2
      ;;
    --binary)
      binary="${2:-}"
      shift 2
      ;;
    --output)
      output="${2:-}"
      shift 2
      ;;
    --bridge-binary)
      bridge_binary="${2:-}"
      shift 2
      ;;
    --package-name)
      package_name="${2:-}"
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

if [[ -z "${version}" || -z "${arch}" || -z "${binary}" || -z "${bridge_binary}" || -z "${output}" ]]; then
  usage >&2
  exit 2
fi

if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid version: ${version}" >&2
  exit 1
fi

if [[ "${arch}" != "amd64" && "${arch}" != "arm64" ]]; then
  echo "unsupported deb arch: ${arch}" >&2
  exit 1
fi

if [[ ! -x "${binary}" ]]; then
  echo "binary is not executable: ${binary}" >&2
  exit 1
fi
if [[ ! -x "${bridge_binary}" ]]; then
  echo "bridge binary is not executable: ${bridge_binary}" >&2
  exit 1
fi

if ! command -v dpkg-deb >/dev/null 2>&1; then
  echo "dpkg-deb is required to build .deb packages" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

pkg_root="${tmp_dir}/pkg"
mkdir -p "${pkg_root}/DEBIAN" "${pkg_root}/usr/bin"
cp "${binary}" "${pkg_root}/usr/bin/ragmail"
cp "${bridge_binary}" "${pkg_root}/usr/bin/ragmail-py"
chmod 0755 "${pkg_root}/usr/bin/ragmail" "${pkg_root}/usr/bin/ragmail-py"

cat > "${pkg_root}/DEBIAN/control" <<EOF
Package: ${package_name}
Version: ${version}
Section: utils
Priority: optional
Architecture: ${arch}
Maintainer: ragmail maintainers
Description: ragmail Rust pipeline CLI
EOF

mkdir -p "$(dirname "${output}")"
dpkg-deb --build "${pkg_root}" "${output}" >/dev/null
echo "Built ${output}"
