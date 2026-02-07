#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
venv_python="${repo_root}/.venv/bin/python"

if [[ $# -lt 1 ]]; then
  echo "usage: run-python.sh <script-or-module> [args...]" >&2
  exit 2
fi

if [[ -x "${venv_python}" ]]; then
  if [[ -n "${PYTHONPATH:-}" ]]; then
    exec env PYTHONPATH="${repo_root}/python/lib:${PYTHONPATH}" "${venv_python}" "$@"
  fi
  exec env PYTHONPATH="${repo_root}/python/lib" "${venv_python}" "$@"
fi

if command -v uv >/dev/null 2>&1; then
  exec env \
    UV_CACHE_DIR="${repo_root}/.uv-cache" \
    UV_PROJECT_ENVIRONMENT="${repo_root}/.venv" \
    uv run --project "${repo_root}/python" python "$@"
fi

if command -v python3 >/dev/null 2>&1; then
  exec python3 "$@"
fi

echo "error: no Python interpreter found (.venv/bin/python, uv, or python3)." >&2
exit 1
