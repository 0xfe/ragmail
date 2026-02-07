#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: publish-homebrew-tap.sh \
  --formula /path/to/ragmail-rs.rb \
  --tap-repo owner/repo|file:///path/to/repo|/path/to/repo \
  --version X.Y.Z \
  [--token <github-token>]
EOF
}

formula=""
tap_repo=""
version=""
token=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --formula)
      formula="${2:-}"
      shift 2
      ;;
    --tap-repo)
      tap_repo="${2:-}"
      shift 2
      ;;
    --version)
      version="${2:-}"
      shift 2
      ;;
    --token)
      token="${2:-}"
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

if [[ -z "${formula}" || -z "${tap_repo}" || -z "${version}" ]]; then
  usage >&2
  exit 2
fi

if [[ ! -f "${formula}" ]]; then
  echo "formula not found: ${formula}" >&2
  exit 1
fi

if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid version: ${version}" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

clone_url=""
if [[ "${tap_repo}" == file://* || "${tap_repo}" == /* || "${tap_repo}" == ./* || "${tap_repo}" == ../* ]]; then
  clone_url="${tap_repo}"
elif [[ -n "${token}" ]]; then
  clone_url="https://x-access-token:${token}@github.com/${tap_repo}.git"
else
  clone_url="https://github.com/${tap_repo}.git"
fi

git clone "${clone_url}" "${tmp_dir}/tap" >/dev/null 2>&1
mkdir -p "${tmp_dir}/tap/Formula"
cp "${formula}" "${tmp_dir}/tap/Formula/ragmail-rs.rb"

cd "${tmp_dir}/tap"
if git diff --quiet; then
  echo "No formula updates to publish."
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git add Formula/ragmail-rs.rb
git commit -m "ragmail-rs ${version}" >/dev/null
git push >/dev/null
echo "Published Formula/ragmail-rs.rb to ${tap_repo}"
