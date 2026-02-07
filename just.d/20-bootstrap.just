bootstrap:
  ./just.d/scripts/bootstrap-python.sh
  cargo fetch --manifest-path {{rust_manifest}}
  cargo build --manifest-path {{rust_manifest}} --workspace

bootstrap-python:
  ./just.d/scripts/bootstrap-python.sh

bootstrap-rust:
  cargo fetch --manifest-path {{rust_manifest}}
  cargo build --manifest-path {{rust_manifest}} --workspace
