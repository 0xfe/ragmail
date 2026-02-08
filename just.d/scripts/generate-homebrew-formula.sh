#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: generate-homebrew-formula.sh \
  --version X.Y.Z \
  --repo owner/repo \
  --macos-amd64-sha SHA256 \
  --macos-arm64-sha SHA256 \
  --output /path/ragmail.rb
EOF
}

version=""
repo=""
macos_amd64_sha=""
macos_arm64_sha=""
output=""

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
    --macos-amd64-sha)
      macos_amd64_sha="${2:-}"
      shift 2
      ;;
    --macos-arm64-sha)
      macos_arm64_sha="${2:-}"
      shift 2
      ;;
    --output)
      output="${2:-}"
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

if [[ -z "${version}" || -z "${repo}" || -z "${macos_amd64_sha}" || -z "${macos_arm64_sha}" || -z "${output}" ]]; then
  usage >&2
  exit 2
fi

if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid version: ${version}" >&2
  exit 1
fi

macos_amd64_url="https://github.com/${repo}/releases/download/v${version}/ragmail-v${version}-macos-amd64.tar.gz"
macos_arm64_url="https://github.com/${repo}/releases/download/v${version}/ragmail-v${version}-macos-arm64.tar.gz"

mkdir -p "$(dirname "${output}")"
cat > "${output}" <<EOF
class Ragmail < Formula
  desc "Rust-first high-throughput pipeline for ragmail"
  homepage "https://github.com/${repo}"
  version "${version}"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "${macos_arm64_url}"
      sha256 "${macos_arm64_sha}"
    else
      url "${macos_amd64_url}"
      sha256 "${macos_amd64_sha}"
    end
  end

  def install
    bin.install "ragmail"
    bin.install "ragmail-py"
  end

  test do
    assert_match version.to_s, shell_output("\#{bin}/ragmail version")
  end
end
EOF

echo "Wrote ${output}"
