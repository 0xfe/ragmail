use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR missing"));
    let version_path = manifest_dir.join("../..").join("VERSION");
    let fallback = env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION missing");
    let version = fs::read_to_string(&version_path)
        .map(|raw| raw.trim().to_string())
        .unwrap_or(fallback);

    println!("cargo:rerun-if-changed={}", version_path.display());
    println!("cargo:rustc-env=RAGMAIL_VERSION={version}");
}
