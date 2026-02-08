use std::path::PathBuf;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::{
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
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/tests/fixtures/sample.mbox");
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
