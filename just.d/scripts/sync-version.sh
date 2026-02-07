#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

if [[ ! -f VERSION ]]; then
  echo "VERSION file is missing" >&2
  exit 1
fi

version="$(tr -d '[:space:]' < VERSION)"
if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "VERSION must be semantic x.y.z, got: ${version}" >&2
  exit 1
fi

replace_python_version() {
  local file="$1"
  local tmp
  tmp="$(mktemp)"
  awk -v v="${version}" '
    BEGIN { replaced = 0 }
    /^version = "/ && replaced == 0 {
      sub(/"[^"]+"/, "\"" v "\"")
      replaced = 1
    }
    { print }
    END {
      if (replaced == 0) {
        print "No version assignment found in " FILENAME > "/dev/stderr"
        exit 1
      }
    }
  ' "${file}" > "${tmp}"
  mv "${tmp}" "${file}"
}

replace_workspace_version() {
  local file="$1"
  local tmp
  tmp="$(mktemp)"
  awk -v v="${version}" '
    BEGIN { in_workspace = 0; replaced = 0 }
    /^\[workspace\.package\]/ { in_workspace = 1; print; next }
    /^\[/ { in_workspace = 0 }
    in_workspace == 1 && /^version = "/ {
      sub(/"[^"]+"/, "\"" v "\"")
      replaced = 1
    }
    { print }
    END {
      if (replaced == 0) {
        print "No [workspace.package] version found in " FILENAME > "/dev/stderr"
        exit 1
      }
    }
  ' "${file}" > "${tmp}"
  mv "${tmp}" "${file}"
}

replace_python_version python/pyproject.toml

perl -i -pe 's/^__version__ = ".*"/__version__ = "'"${version}"'"/' python/lib/ragmail/__init__.py

if [[ -f rust/Cargo.toml ]]; then
  replace_workspace_version rust/Cargo.toml
fi

echo "Synchronized versions to ${version}"
