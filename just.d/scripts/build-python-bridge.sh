#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: build-python-bridge.sh [--output-dir /path/to/output]

Builds a standalone ragmail-py bridge binary via PyInstaller.
EOF
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
output_dir="${repo_root}/python/dist"

while [[ $# -gt 0 ]]; do
  case "$1" in
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

venv_python="${repo_root}/.venv/bin/python"
if [[ ! -x "${venv_python}" ]]; then
  echo "error: ${venv_python} not found. Run 'just bootstrap' first." >&2
  exit 1
fi

if ! "${venv_python}" -c "import PyInstaller" >/dev/null 2>&1; then
  echo "error: PyInstaller is missing in .venv. Re-run bootstrap after syncing Python dev deps." >&2
  exit 1
fi

build_root="${repo_root}/python/build/pyinstaller"
dist_dir="${build_root}/dist"
work_dir="${build_root}/work"
spec_dir="${build_root}/spec"
entry_script="${repo_root}/python/scripts/pyinstaller_entry.py"

rm -rf "${build_root}"
mkdir -p "${dist_dir}" "${work_dir}" "${spec_dir}" "${output_dir}"

PYTHONPATH="${repo_root}/python/lib" "${venv_python}" -m PyInstaller \
  --noconfirm \
  --clean \
  --onefile \
  --name ragmail-py \
  --paths "${repo_root}/python/lib" \
  --collect-submodules ragmail \
  --distpath "${dist_dir}" \
  --workpath "${work_dir}" \
  --specpath "${spec_dir}" \
  "${entry_script}"

bridge_src="${dist_dir}/ragmail-py"
if [[ ! -x "${bridge_src}" ]]; then
  echo "error: expected bridge binary at ${bridge_src}" >&2
  exit 1
fi

install -m 0755 "${bridge_src}" "${output_dir}/ragmail-py"
echo "Built ${output_dir}/ragmail-py"
