//! Byte-offset MBOX indexing stage implementation (Rust migration).

#![forbid(unsafe_code)]

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ragmail_mbox::{MboxError, MboxMessageStream};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Stable index row contract mirrored from Python `mbox_index.py`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRecord {
    pub email_id: String,
    pub message_id: Option<String>,
    pub message_id_lower: Option<String>,
    pub mbox_file: String,
    pub offset: u64,
    pub length: u64,
}

/// Checkpoint payload for resumable index runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCheckpoint {
    pub position: u64,
    pub indexed: u64,
    pub timestamp_epoch_s: u64,
}

/// Build-time settings for indexing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildOptions {
    pub checkpoint_path: Option<PathBuf>,
    pub resume: bool,
    pub checkpoint_every: Duration,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            checkpoint_path: None,
            resume: true,
            checkpoint_every: Duration::from_secs(30),
        }
    }
}

/// Summary stats from an index build run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildStats {
    pub indexed: u64,
    pub last_position: u64,
}

/// Index builder errors.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Mbox(#[from] MboxError),
}

/// Builds an index for a single MBOX file.
pub fn build_index_for_file(
    input_path: &Path,
    mbox_file_name: &str,
    output_path: &Path,
    options: &BuildOptions,
) -> Result<BuildStats, IndexError> {
    let mut start_position = 0_u64;
    let mut indexed = 0_u64;

    if options.resume {
        if let Some(path) = options.checkpoint_path.as_deref() {
            if let Some(checkpoint) = load_checkpoint(path)? {
                start_position = checkpoint.position;
                indexed = checkpoint.indexed;
            }
        }
    }

    if !options.resume {
        if output_path.exists() {
            std::fs::remove_file(output_path)?;
        }
        if let Some(path) = options.checkpoint_path.as_deref() {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }
    }

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(path) = options.checkpoint_path.as_deref() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let file = OpenOptions::new()
        .create(true)
        .append(options.resume)
        .write(true)
        .truncate(!options.resume)
        .open(output_path)?;
    let mut writer = BufWriter::new(file);
    let mut stream = MboxMessageStream::from_path(input_path, start_position)?;
    let mut last_checkpoint = Instant::now();

    loop {
        let message = stream.next_message()?;
        let Some(message) = message else {
            break;
        };
        let record = record_from_message(&message.raw, mbox_file_name, message.offset);
        serde_json::to_writer(&mut writer, &record)?;
        writer.write_all(b"\n")?;
        indexed += 1;

        if let Some(path) = options.checkpoint_path.as_deref() {
            let due = indexed == 1
                || options.checkpoint_every.is_zero()
                || last_checkpoint.elapsed() >= options.checkpoint_every;
            if due {
                save_checkpoint(
                    path,
                    &IndexCheckpoint {
                        position: stream.resume_offset(),
                        indexed,
                        timestamp_epoch_s: now_epoch_secs(),
                    },
                )?;
                last_checkpoint = Instant::now();
            }
        }
    }
    writer.flush()?;

    if let Some(path) = options.checkpoint_path.as_deref() {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }

    Ok(BuildStats {
        indexed,
        last_position: stream.resume_offset(),
    })
}

/// Finds a record in an index JSONL file by `message_id` or `email_id`.
pub fn find_in_index(
    index_path: &Path,
    message_id: Option<&str>,
    email_id: Option<&str>,
) -> Result<Option<IndexRecord>, IndexError> {
    if !index_path.exists() {
        return Ok(None);
    }

    let message_id_lower = message_id.map(str::to_lowercase);
    let file = std::fs::File::open(index_path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<IndexRecord>(&line) else {
            continue;
        };

        if let Some(needle) = email_id {
            if record.email_id == needle {
                return Ok(Some(record));
            }
        }

        if let Some(needle) = message_id {
            if record.message_id.as_deref() == Some(needle) {
                return Ok(Some(record));
            }
            if let Some(needle_lower) = message_id_lower.as_deref() {
                if record.message_id_lower.as_deref() == Some(needle_lower) {
                    return Ok(Some(record));
                }
            }
        }
    }

    Ok(None)
}

/// Reads raw message bytes for a record using `split_dir` + `mbox_file`.
pub fn read_message_bytes(split_dir: &Path, record: &IndexRecord) -> Result<Vec<u8>, IndexError> {
    let mbox_path = if Path::new(&record.mbox_file).is_absolute() {
        PathBuf::from(&record.mbox_file)
    } else {
        split_dir.join(&record.mbox_file)
    };
    let mut file = std::fs::File::open(mbox_path)?;
    file.seek(SeekFrom::Start(record.offset))?;
    let mut buf = vec![0_u8; record.length as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

/// Creates an index row from a raw message blob.
#[must_use]
pub fn record_from_message(raw: &[u8], mbox_file_name: &str, offset: u64) -> IndexRecord {
    let headers = parse_headers(raw);
    let message_id = headers
        .get("message-id")
        .and_then(|value| normalize_message_id(value));
    let message_id_lower = message_id.as_ref().map(|value| value.to_lowercase());

    let email_id = generate_email_id(
        message_id.as_deref(),
        headers.get("from").map(String::as_str).unwrap_or(""),
        headers.get("date").map(String::as_str).unwrap_or(""),
        headers.get("subject").map(String::as_str).unwrap_or(""),
    );

    IndexRecord {
        email_id,
        message_id,
        message_id_lower,
        mbox_file: mbox_file_name.to_string(),
        offset,
        length: raw.len() as u64,
    }
}

fn normalize_message_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let trimmed = trimmed
        .strip_prefix('<')
        .unwrap_or(trimmed)
        .strip_suffix('>')
        .unwrap_or(trimmed)
        .trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_headers(raw: &[u8]) -> std::collections::BTreeMap<String, String> {
    let mut headers = std::collections::BTreeMap::new();
    let text = String::from_utf8_lossy(raw);
    let mut lines = text.lines();

    // Skip envelope line if present.
    if let Some(first) = lines.next() {
        if !first.starts_with("From ") {
            // If no envelope line, treat first line as header candidate.
            parse_header_line(first, &mut headers, &mut None::<String>);
        }
    }

    let mut current_key: Option<String> = None;
    for line in lines {
        if line.is_empty() {
            break;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(key) = current_key.as_ref() {
                headers
                    .entry(key.clone())
                    .and_modify(|value| {
                        value.push(' ');
                        value.push_str(line.trim());
                    })
                    .or_insert_with(|| line.trim().to_string());
            }
            continue;
        }
        parse_header_line(line, &mut headers, &mut current_key);
    }

    headers
}

fn parse_header_line(
    line: &str,
    headers: &mut std::collections::BTreeMap<String, String>,
    current_key: &mut Option<String>,
) {
    let Some((key, value)) = line.split_once(':') else {
        return;
    };
    let key = key.trim().to_ascii_lowercase();
    let value = value.trim().to_string();
    headers.insert(key.clone(), value);
    *current_key = Some(key);
}

fn generate_email_id(
    message_id: Option<&str>,
    from_header: &str,
    date: &str,
    subject: &str,
) -> String {
    let material = if let Some(message_id) = message_id {
        message_id.to_string()
    } else {
        let from_address = parse_from_address(from_header).to_ascii_lowercase();
        format!(
            "{from_address}|{date}|{}",
            subject.chars().take(100).collect::<String>()
        )
    };
    let digest = Sha256::digest(material.as_bytes());
    let hex = format!("{digest:x}");
    hex[..16].to_string()
}

fn parse_from_address(value: &str) -> String {
    if let Some(start) = value.find('<') {
        if let Some(end) = value[start + 1..].find('>') {
            let address = &value[start + 1..start + 1 + end];
            return address.trim().to_string();
        }
    }
    value
        .split_whitespace()
        .find(|part| part.contains('@'))
        .unwrap_or("")
        .trim_matches('"')
        .to_string()
}

fn load_checkpoint(path: &Path) -> Result<Option<IndexCheckpoint>, IndexError> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)?;
    let checkpoint = serde_json::from_str::<IndexCheckpoint>(&raw)?;
    Ok(Some(checkpoint))
}

fn save_checkpoint(path: &Path, checkpoint: &IndexCheckpoint) -> Result<(), IndexError> {
    let raw = serde_json::to_string_pretty(checkpoint)?;
    std::fs::write(path, raw)?;
    Ok(())
}

fn now_epoch_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use sha2::{Digest, Sha256};

    use super::{
        build_index_for_file, find_in_index, read_message_bytes, record_from_message, BuildOptions,
        IndexCheckpoint, IndexRecord,
    };

    fn temp_path(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!("ragmail-{prefix}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn sample_mbox() -> &'static str {
        concat!(
            "From a@example Mon Jan  1 00:00:00 +0000 2024\n",
            "Message-ID: <first@example>\n",
            "From: Alice <alice@example.com>\n",
            "Date: Mon, 1 Jan 2024 00:00:00 +0000\n",
            "Subject: One\n",
            "\n",
            "body one\n",
            "From b@example Tue Jan  2 00:00:00 +0000 2024\n",
            "From: bob@example.com\n",
            "Date: Tue, 2 Jan 2024 00:00:00 +0000\n",
            "Subject: Two\n",
            "\n",
            "body two\n",
        )
    }

    #[test]
    fn record_generation_normalizes_ids() {
        let raw = concat!(
            "From a@example Mon Jan  1 00:00:00 +0000 2024\n",
            "Message-ID: <MiXeD@Example>\n",
            "From: Alice <alice@example.com>\n",
            "Subject: Test\n",
            "\n",
            "body\n",
        )
        .as_bytes()
        .to_vec();
        let record = record_from_message(&raw, "2024-01.mbox", 123);
        assert_eq!(record.message_id.as_deref(), Some("MiXeD@Example"));
        assert_eq!(record.message_id_lower.as_deref(), Some("mixed@example"));
        assert_eq!(record.mbox_file, "2024-01.mbox");
        assert_eq!(record.offset, 123);
        assert_eq!(record.length as usize, raw.len());
        assert_eq!(record.email_id.len(), 16);
        let expected = format!("{:x}", Sha256::digest("MiXeD@Example".as_bytes()));
        assert_eq!(record.email_id, expected[..16].to_string());
    }

    #[test]
    fn index_build_writes_jsonl_records() {
        let dir = temp_path("index-build");
        let input_path = dir.join("sample.mbox");
        let output_path = dir.join("mbox_index.jsonl");
        std::fs::write(&input_path, sample_mbox()).expect("write input");

        let options = BuildOptions::default();
        let stats = build_index_for_file(&input_path, "2024-01.mbox", &output_path, &options)
            .expect("build index");
        assert_eq!(stats.indexed, 2);

        let raw = std::fs::read_to_string(output_path).expect("read output");
        let rows: Vec<IndexRecord> = raw
            .lines()
            .map(|line| serde_json::from_str::<IndexRecord>(line).expect("parse row"))
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].mbox_file, "2024-01.mbox");
        assert_eq!(rows[0].message_id.as_deref(), Some("first@example"));
        assert_eq!(rows[1].message_id, None);
        assert_eq!(rows[1].email_id.len(), 16);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn resume_from_checkpoint_appends_remaining_records() {
        let dir = temp_path("index-resume");
        let input_path = dir.join("sample.mbox");
        let output_path = dir.join("mbox_index.jsonl");
        let checkpoint_path = dir.join("mbox_index.checkpoint.json");
        let bytes = sample_mbox().as_bytes().to_vec();
        std::fs::write(&input_path, &bytes).expect("write input");

        let full_stats = build_index_for_file(
            &input_path,
            "2024-01.mbox",
            &output_path,
            &BuildOptions::default(),
        )
        .expect("full build");
        assert_eq!(full_stats.indexed, 2);

        let first_line = std::fs::read_to_string(&output_path)
            .expect("read")
            .lines()
            .next()
            .expect("first line")
            .to_string();
        std::fs::write(&output_path, format!("{first_line}\n")).expect("write partial output");

        let second_offset = bytes
            .windows(b"From b@example".len())
            .position(|window| window == b"From b@example")
            .expect("second offset") as u64;
        let checkpoint = IndexCheckpoint {
            position: second_offset,
            indexed: 1,
            timestamp_epoch_s: 0,
        };
        std::fs::write(
            &checkpoint_path,
            serde_json::to_string(&checkpoint).expect("serialize checkpoint"),
        )
        .expect("write checkpoint");

        let options = BuildOptions {
            checkpoint_path: Some(checkpoint_path.clone()),
            resume: true,
            checkpoint_every: Duration::from_secs(1),
        };
        let stats = build_index_for_file(&input_path, "2024-01.mbox", &output_path, &options)
            .expect("resume build");

        assert_eq!(stats.indexed, 2);
        assert!(!checkpoint_path.exists());
        let rows = std::fs::read_to_string(&output_path).expect("read final");
        assert_eq!(rows.lines().count(), 2);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn find_and_read_message_bytes_work() {
        let dir = temp_path("index-read");
        let input_path = dir.join("sample.mbox");
        let split_dir = dir.join("split");
        std::fs::create_dir_all(&split_dir).expect("create split");
        let split_file = split_dir.join("2024-01.mbox");
        let index_path = dir.join("mbox_index.jsonl");

        let bytes = sample_mbox().as_bytes().to_vec();
        std::fs::write(&input_path, &bytes).expect("write input");
        std::fs::write(&split_file, &bytes).expect("write split");

        build_index_for_file(
            &input_path,
            "2024-01.mbox",
            &index_path,
            &BuildOptions::default(),
        )
        .expect("index build");

        let record = find_in_index(&index_path, Some("first@example"), None)
            .expect("find")
            .expect("record");
        let raw = read_message_bytes(&split_dir, &record).expect("read");
        let text = std::str::from_utf8(&raw).expect("utf8");
        assert!(text.contains("Subject: One"));

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn index_row_contract_shape_matches_python_fields() {
        let dir = temp_path("index-contract");
        let input_path = dir.join("sample.mbox");
        let output_path = dir.join("mbox_index.jsonl");
        std::fs::write(&input_path, sample_mbox()).expect("write input");

        build_index_for_file(
            &input_path,
            "sample.mbox",
            &output_path,
            &BuildOptions::default(),
        )
        .expect("build index");

        let first = std::fs::read_to_string(&output_path)
            .expect("read output")
            .lines()
            .next()
            .expect("first line")
            .to_string();
        let value: serde_json::Value = serde_json::from_str(&first).expect("valid json");
        let obj = value.as_object().expect("object");
        let keys: std::collections::BTreeSet<String> = obj.keys().cloned().collect();
        let expected = std::collections::BTreeSet::from([
            "email_id".to_string(),
            "message_id".to_string(),
            "message_id_lower".to_string(),
            "mbox_file".to_string(),
            "offset".to_string(),
            "length".to_string(),
        ]);
        assert_eq!(keys, expected);

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn indexes_sample_fixture_without_timezone_envelope() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../python/tests/fixtures/sample.mbox");
        assert!(
            fixture.exists(),
            "fixture not found at {}",
            fixture.display()
        );

        let dir = temp_path("index-fixture");
        let output_path = dir.join("mbox_index.jsonl");
        let stats = build_index_for_file(
            &fixture,
            "sample.mbox",
            &output_path,
            &BuildOptions::default(),
        )
        .expect("build index");
        assert!(stats.indexed > 0);
        assert_eq!(
            std::fs::read_to_string(&output_path)
                .expect("read")
                .lines()
                .count() as u64,
            stats.indexed
        );

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
