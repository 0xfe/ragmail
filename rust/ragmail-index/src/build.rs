use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ragmail_mbox::MboxMessageStream;

use crate::{record_from_message, BuildOptions, BuildStats, IndexCheckpoint, IndexError};

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
