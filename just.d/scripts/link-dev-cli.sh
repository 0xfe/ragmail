#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
venv_dir="${repo_root}/.venv"
rust_bin="${repo_root}/rust/target/debug/ragmail"
target_link="${venv_dir}/bin/ragmail"

if [[ ! -x "${rust_bin}" ]]; then
  echo "error: rust CLI binary missing at ${rust_bin}; run cargo build first" >&2
  exit 1
fi

if [[ ! -d "${venv_dir}/bin" ]]; then
  echo "error: ${venv_dir}/bin missing; run just bootstrap-python first" >&2
  exit 1
fi

ln -sfn "${rust_bin}" "${target_link}"
"${target_link}" --version >/dev/null

echo "Linked Rust CLI: ${target_link} -> ${rust_bin}"
