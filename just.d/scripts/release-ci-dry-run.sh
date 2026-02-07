#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

version="$(tr -d '[:space:]' < VERSION)"
if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid VERSION: ${version}" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

cargo build --manifest-path rust/Cargo.toml --release -p ragmail-cli >/dev/null
binary="rust/target/release/ragmail-rs"
if [[ ! -x "${binary}" ]]; then
  echo "missing release binary: ${binary}" >&2
  exit 1
fi

artifacts="${tmp_dir}/artifacts"
mkdir -p "${artifacts}"

for suffix in macos-amd64 macos-arm64 linux-amd64 linux-arm64; do
  tar -C "$(dirname "${binary}")" -czf "${artifacts}/ragmail-v${version}-${suffix}.tar.gz" "$(basename "${binary}")"
done

if command -v dpkg-deb >/dev/null 2>&1; then
  ./just.d/scripts/package-deb.sh \
    --version "${version}" \
    --arch amd64 \
    --binary "${binary}" \
    --output "${artifacts}/ragmail-rs_${version}_amd64.deb" >/dev/null
  ./just.d/scripts/package-deb.sh \
    --version "${version}" \
    --arch arm64 \
    --binary "${binary}" \
    --output "${artifacts}/ragmail-rs_${version}_arm64.deb" >/dev/null
fi

publish_out="${tmp_dir}/publish"
./just.d/scripts/release-publish-assets.sh \
  --version "${version}" \
  --repo "example/ragmail" \
  --artifacts-dir "${artifacts}" \
  --output-dir "${publish_out}" >/dev/null

if [[ ! -f "${publish_out}/flat/SHA256SUMS" ]]; then
  echo "missing SHA256SUMS" >&2
  exit 1
fi
if [[ ! -f "${publish_out}/homebrew/ragmail-rs.rb" ]]; then
  echo "missing generated Homebrew formula" >&2
  exit 1
fi

tap_bare="${tmp_dir}/tap.git"
git init --bare "${tap_bare}" >/dev/null
tap_seed="${tmp_dir}/tap-seed"
git clone "${tap_bare}" "${tap_seed}" >/dev/null 2>&1
mkdir -p "${tap_seed}/Formula"
cat > "${tap_seed}/Formula/ragmail-rs.rb" <<'EOF'
class RagmailRs < Formula
  desc "seed"
  homepage "https://example.com"
  url "https://example.com/seed.tar.gz"
  sha256 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  version "0.0.0"
end
EOF
(
  cd "${tap_seed}"
  git config user.name "seed"
  git config user.email "seed@example.com"
  git add Formula/ragmail-rs.rb
  git commit -m "seed" >/dev/null
  git push origin HEAD >/dev/null
)

./just.d/scripts/publish-homebrew-tap.sh \
  --formula "${publish_out}/homebrew/ragmail-rs.rb" \
  --tap-repo "file://${tap_bare}" \
  --version "${version}" >/dev/null

echo "Release CI dry-run passed for v${version}"
