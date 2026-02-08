use std::io::Cursor;
use std::path::PathBuf;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::split::split_mbox_by_month_with_options_and_limit;
use crate::stream::{MONTHS, WEEKDAYS};
use crate::{
    is_valid_from_line, split_mbox_by_month, split_mbox_by_month_with_options, MboxMessageStream,
};

fn sample_mbox() -> Vec<u8> {
    let sample = concat!(
        "From a@example Mon Jan  1 00:00:00 +0000 2024\n",
        "Message-ID: <a@example>\n",
        "Subject: One\n",
        "\n",
        "line 1\n",
        ">From escaped\n",
        "From this is not a boundary\n",
        "\n",
        "From b@example Tue Jan  2 00:00:00 +0000 2024\n",
        "Subject: Two\n",
        "\n",
        "line 2\n",
        "From c@example Wed Jan  3 00:00:00 +0000 2024\n",
        "Subject: Three\n",
        "\n",
        "line 3\n",
    );
    sample.as_bytes().to_vec()
}

fn collect_all(mut stream: MboxMessageStream<Cursor<Vec<u8>>>) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    loop {
        let next = stream.next_message().expect("next_message failed");
        let Some(message) = next else {
            break;
        };
        out.push((message.offset, message.raw));
    }
    out
}

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

#[test]
fn envelope_line_validation_matches_expected_shape() {
    assert!(is_valid_from_line(
        b"From a@example Mon Jan  1 00:00:00 2024\n"
    ));
    assert!(is_valid_from_line(
        b"From a@example Mon Jan  1 00:00:00 +0000 2024\n"
    ));
    assert!(is_valid_from_line(
        b"From a@example Tue Feb 10 12:59:59 UTC 2024\n"
    ));
    assert!(!is_valid_from_line(b"From missing fields\n"));
    assert!(!is_valid_from_line(
        b"From a@example Mon Jan 40 00:00:00 +0000 2024\n"
    ));
    assert!(!is_valid_from_line(
        b"From a@example Mon Jan  1 99:00:00 +0000 2024\n"
    ));
    assert!(!is_valid_from_line(b"From this is not a boundary\n"));
}

#[test]
fn stream_reads_messages_and_preserves_offsets() {
    let data = sample_mbox();
    let stream = MboxMessageStream::new(Cursor::new(data.clone()), 0).expect("create stream");
    let messages = collect_all(stream);
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].0, 0);

    let second_offset = data
        .windows(b"From b@example".len())
        .position(|w| w == b"From b@example")
        .expect("second offset") as u64;
    assert_eq!(messages[1].0, second_offset);
    assert!(std::str::from_utf8(&messages[0].1)
        .expect("utf8")
        .contains("From this is not a boundary"));
}

#[test]
fn stream_resume_syncs_to_next_boundary() {
    let data = sample_mbox();
    let start_offset = data
        .windows(b"line 1\n".len())
        .position(|w| w == b"line 1\n")
        .expect("line 1 offset") as u64;
    let stream = MboxMessageStream::new(Cursor::new(data), start_offset).expect("create stream");
    let messages = collect_all(stream);
    assert_eq!(messages.len(), 2);
    let first = std::str::from_utf8(&messages[0].1).expect("utf8");
    assert!(first.starts_with("From b@example"));
}

#[test]
fn split_by_month_writes_period_files() {
    let data = sample_mbox();
    let root = temp_path("split");
    let input = root.join("sample.mbox");
    let out = root.join("split");
    std::fs::write(&input, data).expect("write input");

    let stats = split_mbox_by_month(&input, &out, 0, None).expect("split");
    assert_eq!(stats.processed, 3);
    assert_eq!(stats.written, 3);
    assert_eq!(stats.skipped, 0);
    assert_eq!(stats.errors, 0);
    assert_eq!(stats.by_month.len(), 1);
    assert!(out.join("2024-01.mbox").exists());

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn split_by_month_respects_year_filter() {
    let sample = concat!(
        "From a@example Mon Jan  1 00:00:00 +0000 2024\n",
        "Subject: One\n",
        "\n",
        "line 1\n",
        "From b@example Tue Jan  2 00:00:00 +0000 2025\n",
        "Subject: Two\n",
        "\n",
        "line 2\n",
    );
    let root = temp_path("split-filter");
    let input = root.join("sample.mbox");
    let out = root.join("split");
    std::fs::write(&input, sample.as_bytes()).expect("write input");

    let mut years = std::collections::BTreeSet::new();
    years.insert(2025);
    let stats = split_mbox_by_month(&input, &out, 0, Some(&years)).expect("split");
    assert_eq!(stats.processed, 2);
    assert_eq!(stats.written, 1);
    assert_eq!(stats.skipped, 1);
    assert!(out.join("2025-01.mbox").exists());
    assert!(!out.join("2024-01.mbox").exists());

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn split_by_month_writes_checkpoint_file() {
    let data = sample_mbox();
    let root = temp_path("split-checkpoint");
    let input = root.join("sample.mbox");
    let out = root.join("split");
    let checkpoint = root.join("split.checkpoint.json");
    std::fs::write(&input, data).expect("write input");

    let stats =
        split_mbox_by_month_with_options(&input, &out, 0, None, Some(&checkpoint), Duration::ZERO)
            .expect("split");
    assert_eq!(stats.written, 3);
    assert!(checkpoint.exists());

    let checkpoint_text = std::fs::read_to_string(&checkpoint).expect("read checkpoint");
    assert!(checkpoint_text.contains("\"last_position\""));
    assert!(checkpoint_text.contains("\"written\":3"));

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn split_resume_from_offset_does_not_duplicate_messages() {
    let data = sample_mbox();
    let root = temp_path("split-resume-no-dup");
    let input = root.join("sample.mbox");
    let out = root.join("split");
    std::fs::write(&input, data).expect("write input");
    std::fs::create_dir_all(&out).expect("create out");

    let mut stream = MboxMessageStream::from_path(&input, 0).expect("stream");
    let first = stream.next_message().expect("next").expect("first");
    let resume_offset = stream.resume_offset();

    let month_path = out.join("2024-01.mbox");
    std::fs::write(&month_path, &first.raw).expect("write first");

    let stats = split_mbox_by_month(&input, &out, resume_offset, None).expect("resume split");
    assert_eq!(stats.written, 2);

    let merged = std::fs::read_to_string(&month_path).expect("read merged");
    let from_count = merged
        .lines()
        .filter(|line| is_valid_from_line(format!("{line}\n").as_bytes()))
        .count();
    assert_eq!(from_count, 3);

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn split_handles_many_months_with_low_writer_limit() {
    let root = temp_path("split-low-writer-limit");
    let input = root.join("sample-many-months.mbox");
    let out = root.join("split");
    let mut sample = String::new();
    for month in 1..=12 {
        let weekday = WEEKDAYS[(month as usize) % WEEKDAYS.len()];
        let month_name = MONTHS[(month - 1) as usize];
        sample.push_str(&format!(
            "From m{month}@example.com {weekday} {month_name}  1 00:00:00 +0000 2024\n"
        ));
        sample.push_str(&format!("Message-ID: <m{month}@example.com>\n"));
        sample.push_str(&format!("Subject: Month {month}\n"));
        sample.push('\n');
        sample.push_str("body\n");
    }
    std::fs::write(&input, sample.as_bytes()).expect("write input");

    let stats = split_mbox_by_month_with_options_and_limit(
        &input,
        &out,
        0,
        None,
        None,
        Duration::from_secs(30),
        2,
        None,
        Duration::from_millis(250),
    )
    .expect("split");

    assert_eq!(stats.processed, 12);
    assert_eq!(stats.written, 12);
    for month in 1..=12 {
        assert!(
            out.join(format!("2024-{month:02}.mbox")).exists(),
            "expected output for month {month:02}"
        );
    }

    std::fs::remove_dir_all(root).expect("cleanup");
}
