#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Refusing to bump version on a dirty git tree." >&2
  exit 1
fi

if [[ ! -f VERSION ]]; then
  echo "VERSION file is missing" >&2
  exit 1
fi

current="$(tr -d '[:space:]' < VERSION)"
if [[ ! "${current}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  echo "VERSION must be semantic x.y.z, got: ${current}" >&2
  exit 1
fi

major="${BASH_REMATCH[1]}"
minor="${BASH_REMATCH[2]}"
patch="${BASH_REMATCH[3]}"
next="${major}.${minor}.$((patch + 1))"

printf '%s\n' "${next}" > VERSION
./just.d/scripts/sync-version.sh

echo "Version bumped: ${current} -> ${next}"
