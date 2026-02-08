use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_workspace(name: &str) -> Workspace {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("ragmail-core-{name}-{}-{now}", std::process::id()));
    Workspace::new(name, root)
}

#[test]
fn workspace_ensure_writes_config_and_state() {
    let ws = temp_workspace("ensure");
    ws.ensure().expect("ensure");
    assert!(ws.config_path().exists());
    assert!(!ws.state_path().exists());
    ws.reset_state().expect("reset");
    assert!(ws.state_path().exists());
    let _ = std::fs::remove_dir_all(&ws.root);
}

#[test]
fn workspace_update_stage_merges_details() {
    let ws = temp_workspace("stage-merge");
    ws.ensure().expect("ensure");
    ws.reset_state().expect("reset");
    let mut running = Map::new();
    running.insert("files".to_string(), Value::from(3));
    ws.update_stage("split", "running", Some(running))
        .expect("running");
    let mut done = Map::new();
    done.insert("processed".to_string(), Value::from(10));
    ws.update_stage("split", "done", Some(done)).expect("done");
    assert!(ws.stage_done("split").expect("stage_done"));

    let state = ws.load_state().expect("load");
    let details = state
        .get("stages")
        .and_then(Value::as_object)
        .and_then(|stages| stages.get("split"))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("details"))
        .and_then(Value::as_object)
        .expect("details");
    assert_eq!(details.get("files").and_then(Value::as_i64), Some(3));
    assert_eq!(details.get("processed").and_then(Value::as_i64), Some(10));
    let _ = std::fs::remove_dir_all(&ws.root);
}

#[test]
fn workspace_refresh_archives_selected_dirs_and_clears_state() {
    let ws = temp_workspace("refresh");
    ws.ensure().expect("ensure");
    std::fs::write(ws.split_dir().join("2024-01.mbox"), b"data").expect("write split");
    std::fs::write(ws.clean_dir().join("2024-01.clean.jsonl"), b"{}\n").expect("write clean");
    std::fs::write(ws.spam_dir().join("2024-01.spam.jsonl"), b"{}\n").expect("write spam");
    std::fs::write(ws.reports_dir().join("2024-01.mbox.summary"), b"summary").expect("write rep");
    std::fs::write(ws.split_dir().join("mbox_index.jsonl"), b"{}\n").expect("write index");
    std::fs::create_dir_all(ws.checkpoints_dir().join("split-rs")).expect("mkdir split-rs");
    std::fs::create_dir_all(ws.checkpoints_dir().join("preprocess-rs"))
        .expect("mkdir preprocess-rs");
    ws.update_stage("split", "done", None).expect("split state");
    ws.update_stage("preprocess", "done", None)
        .expect("preprocess state");

    let stages = ["split", "preprocess"]
        .into_iter()
        .map(str::to_string)
        .collect();
    ws.apply_refresh(&stages).expect("refresh");

    assert!(!ws.split_dir().join("mbox_index.jsonl").exists());
    assert!(!ws.checkpoints_dir().join("split-rs").exists());
    assert!(!ws.checkpoints_dir().join("preprocess-rs").exists());
    assert!(ws.split_dir().exists());
    assert!(ws.clean_dir().exists());
    assert!(ws.spam_dir().exists());
    assert!(ws.reports_dir().exists());

    let old_root = ws.root.join("old");
    assert!(old_root.exists());
    let old_entries = std::fs::read_dir(old_root).expect("old dir").count();
    assert!(old_entries >= 1);

    assert!(!ws.stage_done("split").expect("split done"));
    assert!(!ws.stage_done("preprocess").expect("preprocess done"));
    let _ = std::fs::remove_dir_all(&ws.root);
}
