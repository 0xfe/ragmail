use std::fs;
use std::path::Path;

pub(super) fn remove_file_if_exists(path: &Path) -> anyhow::Result<()> {
    if path.is_file() {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub(super) fn remove_path_if_exists(path: &Path) -> anyhow::Result<()> {
    if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else if path.is_file() {
        fs::remove_file(path)?;
    }
    Ok(())
}
