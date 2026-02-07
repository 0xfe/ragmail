build:
  just build-rust

build-rust:
  cargo build --manifest-path {{rust_manifest}} --workspace

build-rust-release:
  cargo build --manifest-path {{rust_manifest}} --release -p ragmail-cli

build-release-artifacts *args:
  ./just.d/scripts/build-release-artifacts.sh {{args}}
