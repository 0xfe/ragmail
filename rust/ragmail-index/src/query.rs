use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::{IndexError, IndexRecord};

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
