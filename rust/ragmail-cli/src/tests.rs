use super::*;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

fn sample_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../python/tests/fixtures/sample.mbox")
        .canonicalize()
        .expect("sample fixture path")
}

fn temp_base(name: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("ragmail-cli-{name}-{}-{now}", std::process::id()));
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(unix)]
fn make_mock_bridge_script(base: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let script = base.join("mock-bridge.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
stage="${2:-}"
if [ "$stage" = "vectorize" ]; then
  echo '{"status":"ok","stage":"vectorize","processed":11,"files":2}'
  exit 0
fi
if [ "$stage" = "ingest" ]; then
  echo '{"status":"ok","stage":"ingest","processed":7,"files":2}'
  exit 0
fi
echo '{"status":"error","message":"unexpected stage"}'
exit 1
"#,
    )
    .expect("write mock bridge");
    let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod");
    script
}

fn count_lines(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|raw| raw.lines().count())
        .unwrap_or(0)
}

#[test]
fn parse_stage_selection_rejects_unknown_stage() {
    let err = parse_stage_selection("split,index,unknown").expect_err("unknown stage");
    assert!(err.to_string().contains("unsupported stage"));
}

#[test]
fn parse_stage_selection_accepts_python_bridge_stages() {
    let stages = parse_stage_selection("vectorize,ingest,split").expect("stages");
    assert!(stages.contains("vectorize"));
    assert!(stages.contains("ingest"));
    assert!(stages.contains("split"));
}

#[test]
fn pipeline_writes_state_and_outputs() {
    let temp = temp_base("state");
    let input = sample_fixture();
    let inputs = [input];
    let options = PipelineRunOptions {
        input_mboxes: &inputs,
        workspace_name: "rs-pipeline-state",
        base_dir: Some(temp.as_path()),
        stages_raw: "split,preprocess",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    run_pipeline(&options).expect("pipeline run");

    let root = temp.join("rs-pipeline-state");
    assert!(root.join("workspace.json").exists());
    assert!(root.join("state.json").exists());
    assert!(root.join("split/mbox_index.jsonl").exists());
    assert!(root.join("logs/pipeline.log").exists());
    assert!(root.join("logs/split.log").exists());
    assert!(root.join("logs/preprocess.log").exists());
    let clean_count = std::fs::read_dir(root.join("clean"))
        .expect("clean dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        .count();
    let spam_count = std::fs::read_dir(root.join("spam"))
        .expect("spam dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        .count();
    let report_count = std::fs::read_dir(root.join("reports"))
        .expect("reports dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("summary"))
        .count();
    assert!(clean_count >= 1, "expected at least one clean output");
    assert!(spam_count >= 1, "expected at least one spam output");
    assert!(report_count >= 1, "expected at least one summary output");

    let state_raw = std::fs::read_to_string(root.join("state.json")).expect("state");
    let state: Value = serde_json::from_str(&state_raw).expect("state json");
    for stage in ["split", "preprocess"] {
        let stage_entry = state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get(stage))
            .and_then(Value::as_object);
        let status = stage_entry
            .and_then(|entry| entry.get("status"))
            .and_then(Value::as_str);
        assert_eq!(status, Some("done"), "stage {stage} should be done");
        let duration = stage_entry
            .and_then(|entry| entry.get("details"))
            .and_then(Value::as_object)
            .and_then(|details| details.get("duration_s"))
            .and_then(Value::as_f64)
            .or_else(|| {
                stage_entry
                    .and_then(|entry| entry.get("details"))
                    .and_then(Value::as_object)
                    .and_then(|details| details.get("duration_s"))
                    .and_then(Value::as_u64)
                    .map(|value| value as f64)
            });
        assert!(
            duration.is_some(),
            "stage {stage} should include details.duration_s"
        );
    }
    let _ = std::fs::remove_dir_all(temp);
}

#[test]
fn pipeline_resume_skips_done_split_stage() {
    let temp = temp_base("resume");
    let input = sample_fixture();
    let inputs = [input];
    let first = PipelineRunOptions {
        input_mboxes: &inputs,
        workspace_name: "rs-pipeline-resume",
        base_dir: Some(temp.as_path()),
        stages_raw: "split",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    run_pipeline(&first).expect("first run");
    let state_path = temp.join("rs-pipeline-resume/state.json");
    let before = std::fs::read_to_string(&state_path).expect("state before");
    let second = PipelineRunOptions {
        input_mboxes: &inputs,
        workspace_name: "rs-pipeline-resume",
        base_dir: Some(temp.as_path()),
        stages_raw: "split",
        resume: true,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    run_pipeline(&second).expect("resume run");
    let after = std::fs::read_to_string(state_path).expect("state after");
    assert_eq!(
        before, after,
        "resume run should skip stage without state mutation"
    );
    let _ = std::fs::remove_dir_all(temp);
}

#[test]
fn pipeline_refresh_archives_previous_split_outputs() {
    let temp = temp_base("refresh");
    let input = sample_fixture();
    let inputs = [input];
    let first = PipelineRunOptions {
        input_mboxes: &inputs,
        workspace_name: "rs-pipeline-refresh",
        base_dir: Some(temp.as_path()),
        stages_raw: "split",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    run_pipeline(&first).expect("first run");
    let second = PipelineRunOptions {
        input_mboxes: &inputs,
        workspace_name: "rs-pipeline-refresh",
        base_dir: Some(temp.as_path()),
        stages_raw: "split",
        resume: true,
        refresh: true,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    run_pipeline(&second).expect("refresh run");

    let old_dir = temp.join("rs-pipeline-refresh/old");
    assert!(old_dir.exists(), "refresh should create archive root");
    let mut saw_archived_split = false;
    for entry in std::fs::read_dir(old_dir).expect("read old") {
        let entry = entry.expect("old entry");
        let split_dir = entry.path().join("split");
        let has_mbox = split_dir.exists()
            && std::fs::read_dir(split_dir)
                .expect("split archive dir")
                .filter_map(Result::ok)
                .any(|file| file.path().extension().and_then(|ext| ext.to_str()) == Some("mbox"));
        if has_mbox {
            saw_archived_split = true;
            break;
        }
    }
    assert!(saw_archived_split, "expected archived split outputs");
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_vectorize_and_ingest_use_python_bridge_commands() {
    let _guard = env_lock().lock().expect("env lock");
    let temp = temp_base("py-bridge");
    let workspace_root = temp.join("rs-pipeline-py");
    let clean_dir = workspace_root.join("clean");
    std::fs::create_dir_all(&clean_dir).expect("clean dir");
    std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n{}\n").expect("clean 1");
    std::fs::write(clean_dir.join("2024-02.clean.jsonl"), "{}\n").expect("clean 2");
    let args_log = temp.join("bridge-args.log");
    let script = make_mock_bridge_script(&temp);

    std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
    std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);

    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-py",
        base_dir: Some(temp.as_path()),
        stages_raw: "vectorize,ingest",
        resume: false,
        refresh: false,
        checkpoint_interval: 5,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
    std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
    assert!(result.is_ok(), "pipeline should succeed: {result:?}");

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state");
    let state: Value = serde_json::from_str(&state_raw).expect("state json");
    let vectorize_status = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("vectorize"))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("status"))
        .and_then(Value::as_str);
    let ingest_status = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("ingest"))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("status"))
        .and_then(Value::as_str);
    assert_eq!(vectorize_status, Some("done"));
    assert_eq!(ingest_status, Some("done"));
    let vectorize_total = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("vectorize"))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("details"))
        .and_then(Value::as_object)
        .and_then(|details| details.get("total"))
        .and_then(Value::as_u64);
    let ingest_total = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("ingest"))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("details"))
        .and_then(Value::as_object)
        .and_then(|details| details.get("total"))
        .and_then(Value::as_u64);
    assert_eq!(vectorize_total, Some(3));
    assert_eq!(ingest_total, Some(3));
    let vectorize_duration = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("vectorize"))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("details"))
        .and_then(Value::as_object)
        .and_then(|details| details.get("duration_s"));
    let ingest_duration = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("ingest"))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("details"))
        .and_then(Value::as_object)
        .and_then(|details| details.get("duration_s"));
    assert!(
        vectorize_duration.is_some(),
        "vectorize stage should include details.duration_s"
    );
    assert!(
        ingest_duration.is_some(),
        "ingest stage should include details.duration_s"
    );

    let args_lines = std::fs::read_to_string(args_log).expect("args log");
    assert!(
        args_lines.contains("py vectorize"),
        "expected vectorize call"
    );
    assert!(args_lines.contains("py ingest"), "expected ingest call");
    assert!(
        args_lines.contains("--no-resume"),
        "expected explicit click boolean flag for resume=false"
    );
    assert!(
        !args_lines.contains("--resume true") && !args_lines.contains("--resume false"),
        "must not pass click boolean options as extra positional values"
    );
    assert!(workspace_root.join("logs/vectorize.log").exists());
    assert!(workspace_root.join("logs/ingest.log").exists());
    let _ = std::fs::remove_dir_all(temp);
}

#[test]
fn pipeline_marks_split_failed_in_state_on_missing_input() {
    let temp = temp_base("split-fail");
    let missing = temp.join("missing-input.mbox");
    let inputs = [missing];
    let options = PipelineRunOptions {
        input_mboxes: &inputs,
        workspace_name: "rs-pipeline-split-fail",
        base_dir: Some(temp.as_path()),
        stages_raw: "split",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };

    let result = run_pipeline(&options);
    assert!(result.is_err(), "expected split stage failure");

    let state_path = temp.join("rs-pipeline-split-fail/state.json");
    let state_raw = std::fs::read_to_string(state_path).expect("state");
    let state: Value = serde_json::from_str(&state_raw).expect("state json");
    let split = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("split"))
        .and_then(Value::as_object)
        .expect("split stage entry");
    assert_eq!(
        split.get("status").and_then(Value::as_str),
        Some("failed"),
        "split stage should be marked failed"
    );
    let error = split
        .get("details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("error"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        !error.is_empty(),
        "split failed state should include error details"
    );
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_marks_vectorize_failed_on_bridge_error() {
    let _guard = env_lock().lock().expect("env lock");
    let temp = temp_base("vectorize-fail");
    let workspace_root = temp.join("rs-pipeline-vectorize-fail");
    let clean_dir = workspace_root.join("clean");
    std::fs::create_dir_all(&clean_dir).expect("clean dir");
    std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n").expect("clean file");

    use std::os::unix::fs::PermissionsExt;
    let script = temp.join("mock-bridge-fail.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -eu
stage="${2:-}"
if [ "$stage" = "vectorize" ]; then
  echo "simulated bridge failure" >&2
  exit 3
fi
echo '{"status":"ok","stage":"ingest","processed":0,"files":0}'
"#,
    )
    .expect("write mock bridge fail");
    let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod");

    std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-vectorize-fail",
        base_dir: Some(temp.as_path()),
        stages_raw: "vectorize",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
    assert!(result.is_err(), "expected vectorize bridge failure");

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
    let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
    let vectorize = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("vectorize"))
        .and_then(Value::as_object)
        .expect("vectorize stage");
    assert_eq!(
        vectorize.get("status").and_then(Value::as_str),
        Some("failed")
    );
    let error = vectorize
        .get("details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("error"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(error.contains("python bridge stage 'vectorize' failed"));
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_retries_vectorize_on_retryable_bridge_failure() {
    use std::os::unix::fs::PermissionsExt;

    let _guard = env_lock().lock().expect("env lock");
    let temp = temp_base("vectorize-retry");
    let workspace_root = temp.join("rs-pipeline-vectorize-retry");
    let clean_dir = workspace_root.join("clean");
    std::fs::create_dir_all(&clean_dir).expect("clean dir");
    std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n{}\n{}\n").expect("clean file");

    let args_log = temp.join("bridge-args.log");
    let counter_file = temp.join("bridge-counter.txt");
    let script = temp.join("mock-bridge-vectorize-retry.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
count_file="${RAGMAIL_BRIDGE_COUNTER:?}"
count=0
if [ -f "$count_file" ]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
if [ "$count" -lt 2 ]; then
  echo "temporary connection error" >&2
  exit 75
fi
stage="${2:-}"
if [ "$stage" = "vectorize" ]; then
  echo '{"status":"ok","stage":"vectorize","processed":3,"files":1}'
  exit 0
fi
echo '{"status":"error","message":"unexpected stage"}'
exit 1
"#,
    )
    .expect("write retry script");
    let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod");

    std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
    std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);
    std::env::set_var("RAGMAIL_BRIDGE_COUNTER", &counter_file);
    std::env::set_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES", "2");
    std::env::set_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS", "1");

    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-vectorize-retry",
        base_dir: Some(temp.as_path()),
        stages_raw: "vectorize",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
    std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
    std::env::remove_var("RAGMAIL_BRIDGE_COUNTER");
    std::env::remove_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES");
    std::env::remove_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS");
    assert!(result.is_ok(), "expected retry to recover: {result:?}");

    assert_eq!(count_lines(&args_log), 2, "expected one retry attempt");
    let attempts = std::fs::read_to_string(counter_file)
        .expect("counter")
        .trim()
        .parse::<u64>()
        .expect("counter value");
    assert_eq!(attempts, 2);

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
    let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
    let vectorize = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("vectorize"))
        .and_then(Value::as_object)
        .expect("vectorize stage");
    assert_eq!(
        vectorize.get("status").and_then(Value::as_str),
        Some("done")
    );
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_does_not_retry_vectorize_on_non_retryable_bridge_failure() {
    use std::os::unix::fs::PermissionsExt;

    let _guard = env_lock().lock().expect("env lock");
    let temp = temp_base("vectorize-no-retry");
    let workspace_root = temp.join("rs-pipeline-vectorize-no-retry");
    let clean_dir = workspace_root.join("clean");
    std::fs::create_dir_all(&clean_dir).expect("clean dir");
    std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n").expect("clean file");

    let args_log = temp.join("bridge-args.log");
    let script = temp.join("mock-bridge-vectorize-no-retry.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
echo "invalid command usage" >&2
exit 2
"#,
    )
    .expect("write no-retry script");
    let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod");

    std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
    std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);
    std::env::set_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES", "3");
    std::env::set_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS", "1");

    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-vectorize-no-retry",
        base_dir: Some(temp.as_path()),
        stages_raw: "vectorize",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
    std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
    std::env::remove_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES");
    std::env::remove_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS");
    assert!(result.is_err(), "expected non-retryable failure");
    assert_eq!(
        count_lines(&args_log),
        1,
        "non-retryable errors should not retry"
    );

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
    let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
    let vectorize = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("vectorize"))
        .and_then(Value::as_object)
        .expect("vectorize stage");
    assert_eq!(
        vectorize.get("status").and_then(Value::as_str),
        Some("failed")
    );
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_marks_ingest_failed_on_bridge_error() {
    let _guard = env_lock().lock().expect("env lock");
    let temp = temp_base("ingest-fail");
    let workspace_root = temp.join("rs-pipeline-ingest-fail");
    let clean_dir = workspace_root.join("clean");
    std::fs::create_dir_all(&clean_dir).expect("clean dir");
    std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n").expect("clean file");

    use std::os::unix::fs::PermissionsExt;
    let script = temp.join("mock-bridge-ingest-fail.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -eu
stage="${2:-}"
if [ "$stage" = "ingest" ]; then
  echo "simulated ingest failure" >&2
  exit 4
fi
echo '{"status":"ok","stage":"vectorize","processed":0,"files":0}'
"#,
    )
    .expect("write mock bridge fail");
    let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod");

    std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-ingest-fail",
        base_dir: Some(temp.as_path()),
        stages_raw: "ingest",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
    assert!(result.is_err(), "expected ingest bridge failure");

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
    let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
    let ingest = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("ingest"))
        .and_then(Value::as_object)
        .expect("ingest stage");
    assert_eq!(ingest.get("status").and_then(Value::as_str), Some("failed"));
    let error = ingest
        .get("details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("error"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(error.contains("python bridge stage 'ingest' failed"));
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_retries_ingest_on_retryable_bridge_failure() {
    use std::os::unix::fs::PermissionsExt;

    let _guard = env_lock().lock().expect("env lock");
    let temp = temp_base("ingest-retry");
    let workspace_root = temp.join("rs-pipeline-ingest-retry");
    let clean_dir = workspace_root.join("clean");
    std::fs::create_dir_all(&clean_dir).expect("clean dir");
    std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n{}\n").expect("clean file");

    let args_log = temp.join("bridge-args.log");
    let counter_file = temp.join("bridge-counter.txt");
    let script = temp.join("mock-bridge-ingest-retry.sh");
    std::fs::write(
            &script,
            r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
count_file="${RAGMAIL_BRIDGE_COUNTER:?}"
count=0
if [ -f "$count_file" ]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
if [ "$count" -lt 2 ]; then
  echo "temporary service unavailable" >&2
  exit 111
fi
stage="${2:-}"
if [ "$stage" = "ingest" ]; then
  echo '{"status":"ok","stage":"ingest","processed":2,"files":1,"skipped_exists":1,"skipped_errors":1}'
  exit 0
fi
echo '{"status":"error","message":"unexpected stage"}'
exit 1
"#,
        )
        .expect("write retry script");
    let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod");

    std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
    std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);
    std::env::set_var("RAGMAIL_BRIDGE_COUNTER", &counter_file);
    std::env::set_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES", "2");
    std::env::set_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS", "1");

    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-ingest-retry",
        base_dir: Some(temp.as_path()),
        stages_raw: "ingest",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
    std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
    std::env::remove_var("RAGMAIL_BRIDGE_COUNTER");
    std::env::remove_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES");
    std::env::remove_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS");
    assert!(result.is_ok(), "expected retry to recover: {result:?}");

    assert_eq!(count_lines(&args_log), 2, "expected one retry attempt");
    let attempts = std::fs::read_to_string(counter_file)
        .expect("counter")
        .trim()
        .parse::<u64>()
        .expect("counter value");
    assert_eq!(attempts, 2);

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
    let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
    let ingest = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("ingest"))
        .and_then(Value::as_object)
        .expect("ingest stage");
    assert_eq!(ingest.get("status").and_then(Value::as_str), Some("done"));
    let details = ingest
        .get("details")
        .and_then(Value::as_object)
        .expect("ingest details");
    assert_eq!(details.get("total").and_then(Value::as_u64), Some(2));
    assert_eq!(details.get("skipped").and_then(Value::as_u64), Some(2));
    assert_eq!(details.get("errors").and_then(Value::as_u64), Some(1));
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_does_not_retry_ingest_on_non_retryable_bridge_failure() {
    use std::os::unix::fs::PermissionsExt;

    let _guard = env_lock().lock().expect("env lock");
    let temp = temp_base("ingest-no-retry");
    let workspace_root = temp.join("rs-pipeline-ingest-no-retry");
    let clean_dir = workspace_root.join("clean");
    std::fs::create_dir_all(&clean_dir).expect("clean dir");
    std::fs::write(clean_dir.join("2024-01.clean.jsonl"), "{}\n").expect("clean file");

    let args_log = temp.join("bridge-args.log");
    let script = temp.join("mock-bridge-ingest-no-retry.sh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
set -eu
if [ -n "${RAGMAIL_BRIDGE_ARGS_LOG:-}" ]; then
  echo "$@" >> "$RAGMAIL_BRIDGE_ARGS_LOG"
fi
echo "bad arguments" >&2
exit 2
"#,
    )
    .expect("write no-retry script");
    let mut perms = std::fs::metadata(&script).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod");

    std::env::set_var("RAGMAIL_PY_BRIDGE_BIN", &script);
    std::env::set_var("RAGMAIL_BRIDGE_ARGS_LOG", &args_log);
    std::env::set_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES", "3");
    std::env::set_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS", "1");

    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-ingest-no-retry",
        base_dir: Some(temp.as_path()),
        stages_raw: "ingest",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    std::env::remove_var("RAGMAIL_PY_BRIDGE_BIN");
    std::env::remove_var("RAGMAIL_BRIDGE_ARGS_LOG");
    std::env::remove_var("RAGMAIL_PY_BRIDGE_MAX_RETRIES");
    std::env::remove_var("RAGMAIL_PY_BRIDGE_RETRY_DELAY_MS");
    assert!(result.is_err(), "expected non-retryable failure");
    assert_eq!(
        count_lines(&args_log),
        1,
        "non-retryable errors should not retry"
    );

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
    let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
    let ingest = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("ingest"))
        .and_then(Value::as_object)
        .expect("ingest stage");
    assert_eq!(ingest.get("status").and_then(Value::as_str), Some("failed"));
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_marks_preprocess_failed_for_index_alias_on_unreadable_split_file() {
    use std::os::unix::fs::PermissionsExt;

    let temp = temp_base("preprocess-index-alias-fail");
    let workspace_root = temp.join("rs-pipeline-preprocess-index-alias-fail");
    let split_dir = workspace_root.join("split");
    std::fs::create_dir_all(&split_dir).expect("split dir");
    let split_file = split_dir.join("2024-01.mbox");
    std::fs::write(
        &split_file,
        b"From a@example.com Mon Jan  1 01:02:03 +0000 2024\n\n",
    )
    .expect("split file");
    let mut perms = std::fs::metadata(&split_file).expect("meta").permissions();
    perms.set_mode(0o000);
    std::fs::set_permissions(&split_file, perms).expect("chmod 000");

    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-preprocess-index-alias-fail",
        base_dir: Some(temp.as_path()),
        stages_raw: "index",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    let mut restore = std::fs::metadata(&split_file)
        .expect("meta restore")
        .permissions();
    restore.set_mode(0o644);
    let _ = std::fs::set_permissions(&split_file, restore);
    assert!(result.is_err(), "expected preprocess failure");

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
    let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
    let preprocess = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("preprocess"))
        .and_then(Value::as_object)
        .expect("preprocess stage");
    assert_eq!(
        preprocess.get("status").and_then(Value::as_str),
        Some("failed")
    );
    let error = preprocess
        .get("details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("error"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        !error.is_empty(),
        "preprocess failure should include error detail"
    );
    let _ = std::fs::remove_dir_all(temp);
}

#[cfg(unix)]
#[test]
fn pipeline_marks_preprocess_failed_for_clean_alias_on_unreadable_split_file() {
    use std::os::unix::fs::PermissionsExt;

    let temp = temp_base("preprocess-clean-alias-fail");
    let workspace_root = temp.join("rs-pipeline-preprocess-clean-alias-fail");
    let split_dir = workspace_root.join("split");
    std::fs::create_dir_all(&split_dir).expect("split dir");
    let split_file = split_dir.join("2024-01.mbox");
    std::fs::write(
        &split_file,
        b"From a@example.com Mon Jan  1 01:02:03 +0000 2024\n\n",
    )
    .expect("split file");
    let mut perms = std::fs::metadata(&split_file).expect("meta").permissions();
    perms.set_mode(0o000);
    std::fs::set_permissions(&split_file, perms).expect("chmod 000");

    let options = PipelineRunOptions {
        input_mboxes: &[],
        workspace_name: "rs-pipeline-preprocess-clean-alias-fail",
        base_dir: Some(temp.as_path()),
        stages_raw: "clean",
        resume: false,
        refresh: false,
        checkpoint_interval: 1,
        years: &[],
        clean_dir: None,
        embeddings_dir: None,
        db_path: None,
        ingest_batch_size: None,
        embedding_batch_size: None,
        chunk_size: None,
        chunk_overlap: None,
        compact_every: None,
        skip_exists_check: false,
        repair_embeddings: true,
    };
    let result = run_pipeline(&options);
    let mut restore = std::fs::metadata(&split_file)
        .expect("meta restore")
        .permissions();
    restore.set_mode(0o644);
    let _ = std::fs::set_permissions(&split_file, restore);
    assert!(result.is_err(), "expected preprocess failure");

    let state_raw = std::fs::read_to_string(workspace_root.join("state.json")).expect("state json");
    let state: Value = serde_json::from_str(&state_raw).expect("state parsed");
    let preprocess = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("preprocess"))
        .and_then(Value::as_object)
        .expect("preprocess stage");
    assert_eq!(
        preprocess.get("status").and_then(Value::as_str),
        Some("failed")
    );
    let error = preprocess
        .get("details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("error"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        !error.is_empty(),
        "preprocess failure should include error detail"
    );
    let _ = std::fs::remove_dir_all(temp);
}
