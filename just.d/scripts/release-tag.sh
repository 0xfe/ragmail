#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Refusing to tag release on a dirty git tree." >&2
  exit 1
fi

if [[ ! -f VERSION ]]; then
  echo "VERSION file is missing." >&2
  exit 1
fi

version="$(tr -d '[:space:]' < VERSION)"
tag="v${version}"

if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  echo "Tag already exists: ${tag}" >&2
  exit 1
fi

git tag -a "${tag}" -m "Release ${tag}"
echo "Created git tag ${tag}"
