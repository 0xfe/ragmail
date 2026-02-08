test:
  just test-rust

test-all:
  just test-rust
  just test-python

test-python:
  {{repo_root}}/just.d/scripts/test-python.sh

test-rust:
  cargo test --manifest-path {{rust_manifest}} --workspace

test-integration:
  {{repo_root}}/just.d/scripts/test-python.sh -m integration

test-integration-live:
  RAGMAIL_RUN_INTEGRATION_TESTS=1 {{repo_root}}/just.d/scripts/test-python.sh -m integration
