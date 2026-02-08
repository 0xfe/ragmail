bootstrap:
  {{repo_root}}/just.d/scripts/bootstrap-python.sh
  cargo fetch --manifest-path {{rust_manifest}}
  cargo build --manifest-path {{rust_manifest}} --workspace
  {{repo_root}}/just.d/scripts/link-dev-cli.sh

bootstrap-python:
  {{repo_root}}/just.d/scripts/bootstrap-python.sh

bootstrap-rust:
  cargo fetch --manifest-path {{rust_manifest}}
  cargo build --manifest-path {{rust_manifest}} --workspace
  {{repo_root}}/just.d/scripts/link-dev-cli.sh
