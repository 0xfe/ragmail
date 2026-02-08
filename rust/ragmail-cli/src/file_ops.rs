use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::bail;
use serde_json::Value;
use sha2::{Digest, Sha256};
pub(crate) fn parse_stage_selection(raw: &str) -> anyhow::Result<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    for stage in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match stage {
            "download" | "model" => {
                out.insert("model".to_string());
            }
            "split" => {
                out.insert("split".to_string());
            }
            "clean" | "index" | "preprocess" => {
                out.insert("preprocess".to_string());
            }
            "vectorize" | "ingest" => {
                out.insert(stage.to_string());
            }
            other => bail!("unsupported stage for rust pipeline: {other}"),
        }
    }
    if out.is_empty() {
        bail!("stage list cannot be empty");
    }
    Ok(out)
}

pub(crate) fn split_checkpoint_path(checkpoint_dir: &Path, input: &Path) -> PathBuf {
    let digest = Sha256::digest(input.to_string_lossy().as_bytes());
    let digest_hex = format!("{digest:x}");
    checkpoint_dir.join(format!(
        "{}-{}.checkpoint.json",
        input
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("mbox"),
        &digest_hex[..12]
    ))
}

pub(crate) fn load_last_position(checkpoint_path: &Path) -> anyhow::Result<u64> {
    if !checkpoint_path.exists() {
        return Ok(0);
    }
    let raw = std::fs::read_to_string(checkpoint_path)?;
    let value = serde_json::from_str::<Value>(&raw)?;
    Ok(value
        .get("last_position")
        .and_then(|entry| entry.as_u64())
        .unwrap_or(0))
}

pub(crate) fn collect_split_files(split_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(split_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if is_month_split_name(name) {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

pub(crate) fn collect_clean_files(clean_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(clean_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.ends_with(".clean.jsonl") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn is_month_split_name(name: &str) -> bool {
    if !name.ends_with(".mbox") || name.len() != 12 {
        return false;
    }
    let bytes = name.as_bytes();
    bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
}

pub(crate) fn count_non_empty_lines(path: &Path) -> anyhow::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut total = 0_u64;
    for line in reader.lines() {
        if !line?.trim().is_empty() {
            total += 1;
        }
    }
    Ok(total)
}

pub(crate) fn count_non_empty_lines_for_paths(paths: &[PathBuf]) -> anyhow::Result<u64> {
    let mut total = 0_u64;
    for path in paths {
        total += count_non_empty_lines(path)?;
    }
    Ok(total)
}

pub(crate) fn count_mbox_envelopes(path: &Path) -> anyhow::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut total = 0_u64;
    for line in reader.lines() {
        let line = line?;
        if line.starts_with("From ") {
            total += 1;
        }
    }
    Ok(total)
}

pub(crate) fn merge_index_parts(
    parts_dir: &Path,
    split_files: &[PathBuf],
    output: &Path,
) -> anyhow::Result<()> {
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let out_file = File::create(output)?;
    let mut writer = BufWriter::new(out_file);
    for split_file in split_files {
        let mbox_name = split_file
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid split file name: {}", split_file.display()))?;
        let part_path = parts_dir.join(format!("{mbox_name}.jsonl"));
        if !part_path.exists() {
            bail!("missing index part output {}", part_path.display());
        }
        let mut reader = BufReader::new(File::open(part_path)?);
        std::io::copy(&mut reader, &mut writer)?;
    }
    writer.flush()?;
    Ok(())
}
