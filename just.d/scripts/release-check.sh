#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Release check failed: git tree is dirty." >&2
  exit 1
fi

if [[ ! -f VERSION ]]; then
  echo "Release check failed: VERSION file is missing." >&2
  exit 1
fi

version="$(tr -d '[:space:]' < VERSION)"
if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Release check failed: invalid VERSION '${version}'." >&2
  exit 1
fi

cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets -- -D warnings
cargo test --manifest-path rust/Cargo.toml --workspace

if [[ -x ".venv/bin/python" ]]; then
  ./just.d/scripts/test-python.sh -q
else
  echo "Release check failed: .venv is missing. Run 'just bootstrap-python' first." >&2
  exit 1
fi

cargo build --manifest-path rust/Cargo.toml --release -p ragmail-cli

bin_version="$(rust/target/release/ragmail-rs version | tr -d '[:space:]')"
if [[ "${bin_version}" != "${version}" ]]; then
  echo "Release check failed: binary version '${bin_version}' does not match VERSION '${version}'." >&2
  exit 1
fi

echo "Release checks passed for v${version}"
