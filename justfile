set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set positional-arguments := true

import "just.d/10-vars.just"
import "just.d/20-bootstrap.just"
import "just.d/25-build.just"
import "just.d/30-test.just"
import "just.d/35-lint.just"
import "just.d/40-version.just"
import "just.d/50-release.just"
import "just.d/60-bench.just"

default: help

help:
  @just --list
