#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
venv_dir="${repo_root}/.venv"
cache_dir="${repo_root}/.uv-cache"

bootstrap_with_uv() {
  if ! command -v uv >/dev/null 2>&1; then
    return 1
  fi

  echo "Using uv for Python environment bootstrap"
  UV_CACHE_DIR="${cache_dir}" uv venv "${venv_dir}"
  UV_CACHE_DIR="${cache_dir}" UV_PROJECT_ENVIRONMENT="${venv_dir}" \
    uv sync --project "${repo_root}/python" --extra dev
}

bootstrap_with_pip() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "error: python3 is required when uv bootstrap is unavailable" >&2
    return 1
  fi

  echo "Using python -m venv + pip fallback bootstrap"
  python3 -m venv "${venv_dir}"
  "${venv_dir}/bin/python" -m pip install --upgrade pip
  "${venv_dir}/bin/python" -m pip install -e "${repo_root}/python[dev]"
}

if ! bootstrap_with_uv; then
  echo "uv bootstrap failed; falling back to python -m venv + pip" >&2
  if [[ -x "${repo_root}/python/.venv/bin/ragmail" ]]; then
    echo "Reusing existing python/.venv as shared .venv"
    rm -rf "${venv_dir}"
    ln -s "${repo_root}/python/.venv" "${venv_dir}"
  else
  rm -rf "${venv_dir}"
    bootstrap_with_pip
  fi
fi

if [[ ! -x "${venv_dir}/bin/ragmail" ]]; then
  echo "error: ragmail CLI not found at ${venv_dir}/bin/ragmail after bootstrap" >&2
  exit 1
fi

"${venv_dir}/bin/ragmail" --version >/dev/null

echo "Python bootstrap complete: ${venv_dir}"
