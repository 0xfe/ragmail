lint:
  just lint-rust

lint-rust:
  cargo fmt --manifest-path {{rust_manifest}} --all -- --check
  cargo clippy --manifest-path {{rust_manifest}} --workspace --all-targets -- -D warnings
