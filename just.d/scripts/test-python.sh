#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
venv_python="${repo_root}/.venv/bin/python"

# Keep the rust bridge binary current so preprocess tests do not execute a stale CLI.
if command -v cargo >/dev/null 2>&1; then
  cargo build --manifest-path "${repo_root}/rust/Cargo.toml" --bin ragmail >/dev/null
fi

if [[ -x "${venv_python}" ]]; then
  if [[ -n "${PYTHONPATH:-}" ]]; then
    exec env \
      PYTHONPATH="${repo_root}/python/lib:${PYTHONPATH}" \
      "${venv_python}" -m pytest -c "${repo_root}/python/pytest.ini" "$@"
  fi
  exec env \
    PYTHONPATH="${repo_root}/python/lib" \
    "${venv_python}" -m pytest -c "${repo_root}/python/pytest.ini" "$@"
fi

if command -v uv >/dev/null 2>&1; then
  exec env \
    UV_CACHE_DIR="${repo_root}/.uv-cache" \
    UV_PROJECT_ENVIRONMENT="${repo_root}/.venv" \
    uv run --project "${repo_root}/python" pytest -c "${repo_root}/python/pytest.ini" "$@"
fi

echo "error: no Python test runner found." >&2
echo "hint: create .venv (python -m venv .venv) or install uv." >&2
exit 1
