#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: release-publish-assets.sh --version X.Y.Z --repo owner/repo --artifacts-dir /path/to/artifacts --output-dir /path/to/output

Collects release artifacts, writes SHA256SUMS, and generates ragmail.rb.
EOF
}

version=""
repo=""
artifacts_dir=""
output_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      version="${2:-}"
      shift 2
      ;;
    --repo)
      repo="${2:-}"
      shift 2
      ;;
    --artifacts-dir)
      artifacts_dir="${2:-}"
      shift 2
      ;;
    --output-dir)
      output_dir="${2:-}"
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

if [[ -z "${version}" || -z "${repo}" || -z "${artifacts_dir}" || -z "${output_dir}" ]]; then
  usage >&2
  exit 2
fi

if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid version: ${version}" >&2
  exit 1
fi

mkdir -p "${output_dir}/flat" "${output_dir}/homebrew"
find "${artifacts_dir}" -type f \( -name "*.tar.gz" -o -name "*.deb" \) -exec cp {} "${output_dir}/flat/" \;

if ! ls "${output_dir}/flat"/*.tar.gz >/dev/null 2>&1; then
  echo "no tarball artifacts found in ${artifacts_dir}" >&2
  exit 1
fi

(
  cd "${output_dir}/flat"
  shasum -a 256 * > SHA256SUMS
)

macos_amd64="ragmail-v${version}-macos-amd64.tar.gz"
macos_arm64="ragmail-v${version}-macos-arm64.tar.gz"
if [[ ! -f "${output_dir}/flat/${macos_amd64}" ]]; then
  echo "missing ${macos_amd64}" >&2
  exit 1
fi
if [[ ! -f "${output_dir}/flat/${macos_arm64}" ]]; then
  echo "missing ${macos_arm64}" >&2
  exit 1
fi

macos_amd64_sha="$(shasum -a 256 "${output_dir}/flat/${macos_amd64}" | awk '{print $1}')"
macos_arm64_sha="$(shasum -a 256 "${output_dir}/flat/${macos_arm64}" | awk '{print $1}')"

./just.d/scripts/generate-homebrew-formula.sh \
  --version "${version}" \
  --repo "${repo}" \
  --macos-amd64-sha "${macos_amd64_sha}" \
  --macos-arm64-sha "${macos_arm64_sha}" \
  --output "${output_dir}/homebrew/ragmail.rb"

echo "Prepared release publish artifacts in ${output_dir}"
