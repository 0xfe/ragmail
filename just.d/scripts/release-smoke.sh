#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

version="$(tr -d '[:space:]' < VERSION)"
if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid VERSION: ${version}" >&2
  exit 1
fi

host_triple="$(rustc -vV | awk '/^host: / { print $2 }')"
case "${host_triple}" in
  x86_64-apple-darwin)
    suffix="macos-amd64"
    ;;
  aarch64-apple-darwin)
    suffix="macos-arm64"
    ;;
  x86_64-unknown-linux-gnu)
    suffix="linux-amd64"
    ;;
  aarch64-unknown-linux-gnu)
    suffix="linux-arm64"
    ;;
  *)
    suffix="${host_triple}"
    ;;
esac

tmp_dir="$(mktemp -d)"
extract_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}" "${extract_dir}"' EXIT

./just.d/scripts/build-release-artifacts.sh --output-dir "${tmp_dir}" --version "${version}" --platform host

tarball="ragmail-v${version}-${suffix}.tar.gz"
if [[ ! -f "${tmp_dir}/${tarball}" ]]; then
  echo "missing release tarball: ${tmp_dir}/${tarball}" >&2
  exit 1
fi

if [[ ! -f "${tmp_dir}/SHA256SUMS" ]]; then
  echo "missing checksum file: ${tmp_dir}/SHA256SUMS" >&2
  exit 1
fi

if ! grep -q " ${tarball}$" "${tmp_dir}/SHA256SUMS"; then
  echo "checksum missing entry for ${tarball}" >&2
  exit 1
fi

tar -C "${extract_dir}" -xzf "${tmp_dir}/${tarball}"
if ! "${extract_dir}/ragmail" version >/dev/null 2>&1; then
  echo "release smoke failed: extracted ragmail binary did not run" >&2
  exit 1
fi
if ! "${extract_dir}/ragmail" search --help >/dev/null 2>&1; then
  echo "release smoke failed: extracted ragmail could not reach bundled ragmail-py" >&2
  exit 1
fi

tarball_sha="$(shasum -a 256 "${tmp_dir}/${tarball}" | awk '{print $1}')"
formula_path="${tmp_dir}/ragmail.rb"
./just.d/scripts/generate-homebrew-formula.sh \
  --version "${version}" \
  --repo "example/ragmail" \
  --macos-amd64-sha "${tarball_sha}" \
  --macos-arm64-sha "${tarball_sha}" \
  --output "${formula_path}"

if ! grep -q 'class Ragmail < Formula' "${formula_path}"; then
  echo "generated formula missing Ragmail class" >&2
  exit 1
fi

echo "Release smoke passed for v${version} (${host_triple})"
