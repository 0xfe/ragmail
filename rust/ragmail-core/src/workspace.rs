use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::Local;
use serde_json::{json, Map, Value};

fn timestamp_iso() -> String {
    Local::now().to_rfc3339()
}

fn refresh_stamp() -> String {
    Local::now().format("%y%m%d%H%M%S").to_string()
}

#[derive(Debug, Clone)]
pub struct Workspace {
    pub name: String,
    pub root: PathBuf,
}

impl Workspace {
    #[must_use]
    pub fn new(name: impl Into<String>, root: PathBuf) -> Self {
        Self {
            name: name.into(),
            root,
        }
    }

    #[must_use]
    pub fn inputs_dir(&self) -> PathBuf {
        self.root.join("inputs")
    }

    #[must_use]
    pub fn split_dir(&self) -> PathBuf {
        self.root.join("split")
    }

    #[must_use]
    pub fn clean_dir(&self) -> PathBuf {
        self.root.join("clean")
    }

    #[must_use]
    pub fn embeddings_dir(&self) -> PathBuf {
        self.root.join("embeddings")
    }

    #[must_use]
    pub fn spam_dir(&self) -> PathBuf {
        self.root.join("spam")
    }

    #[must_use]
    pub fn db_dir(&self) -> PathBuf {
        self.root.join("db")
    }

    #[must_use]
    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    #[must_use]
    pub fn checkpoints_dir(&self) -> PathBuf {
        self.root.join(".checkpoints")
    }

    #[must_use]
    pub fn reports_dir(&self) -> PathBuf {
        self.root.join("reports")
    }

    #[must_use]
    pub fn cache_dir(&self) -> PathBuf {
        self.root.join("cache")
    }

    #[must_use]
    pub fn config_path(&self) -> PathBuf {
        self.root.join("workspace.json")
    }

    #[must_use]
    pub fn state_path(&self) -> PathBuf {
        self.root.join("state.json")
    }

    pub fn ensure(&self) -> anyhow::Result<()> {
        fs::create_dir_all(&self.root)?;
        for path in [
            self.inputs_dir(),
            self.split_dir(),
            self.clean_dir(),
            self.embeddings_dir(),
            self.spam_dir(),
            self.db_dir(),
            self.logs_dir(),
            self.checkpoints_dir(),
            self.reports_dir(),
            self.cache_dir(),
        ] {
            fs::create_dir_all(path)?;
        }
        if !self.config_path().exists() {
            let payload = json!({
                "name": self.name,
                "root": self.root,
                "created_at": timestamp_iso(),
                "paths": {
                    "inputs": "inputs",
                    "split": "split",
                    "clean": "clean",
                    "embeddings": "embeddings",
                    "spam": "spam",
                    "db": "db",
                    "logs": "logs",
                    "checkpoints": ".checkpoints",
                    "reports": "reports",
                    "cache": "cache",
                }
            });
            fs::write(self.config_path(), serde_json::to_vec_pretty(&payload)?)?;
        }
        Ok(())
    }

    pub fn load_state(&self) -> anyhow::Result<Value> {
        let path = self.state_path();
        if !path.exists() {
            return Ok(json!({"stages": {}, "updated_at": timestamp_iso()}));
        }
        let raw = fs::read_to_string(path)?;
        let value = serde_json::from_str::<Value>(&raw)?;
        Ok(value)
    }

    pub fn save_state(&self, mut state: Value) -> anyhow::Result<()> {
        if let Some(obj) = state.as_object_mut() {
            obj.insert("updated_at".to_string(), Value::String(timestamp_iso()));
        }
        fs::write(self.state_path(), serde_json::to_vec_pretty(&state)?)?;
        Ok(())
    }

    pub fn reset_state(&self) -> anyhow::Result<()> {
        self.save_state(json!({"stages": {}, "updated_at": timestamp_iso()}))
    }

    pub fn stage_done(&self, stage: &str) -> anyhow::Result<bool> {
        let state = self.load_state()?;
        Ok(state
            .get("stages")
            .and_then(Value::as_object)
            .and_then(|stages| stages.get(stage))
            .and_then(Value::as_object)
            .and_then(|entry| entry.get("status"))
            .and_then(Value::as_str)
            == Some("done"))
    }

    pub fn update_stage(
        &self,
        stage: &str,
        status: &str,
        details: Option<Map<String, Value>>,
    ) -> anyhow::Result<()> {
        let mut state = self.load_state()?;
        let state_obj = state
            .as_object_mut()
            .context("workspace state must be a JSON object")?;
        let stages = state_obj
            .entry("stages")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .context("workspace state.stages must be an object")?;
        let entry = stages
            .entry(stage.to_string())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .context("workspace stage entry must be an object")?;

        entry.insert("status".to_string(), Value::String(status.to_string()));
        if status == "running" {
            entry.insert("started_at".to_string(), Value::String(timestamp_iso()));
        }
        if status == "done" || status == "failed" || status == "interrupted" {
            entry.insert("completed_at".to_string(), Value::String(timestamp_iso()));
        }
        if let Some(details_map) = details {
            let details_entry = entry
                .entry("details".to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            let details_obj = details_entry
                .as_object_mut()
                .context("workspace stage details must be an object")?;
            for (key, value) in details_map {
                details_obj.insert(key, value);
            }
        }
        self.save_state(state)
    }

    pub fn apply_refresh(&self, stages: &BTreeSet<String>) -> anyhow::Result<()> {
        let stamp = refresh_stamp();
        let archive_root = self.root.join("old").join(stamp);
        if stages.contains("split") {
            self.archive_dir(&self.split_dir(), "split", &archive_root)?;
            remove_path_if_exists(&self.checkpoints_dir().join("split-rs"))?;
        }
        if stages.contains("index") {
            remove_file_if_exists(&self.split_dir().join("mbox_index.jsonl"))?;
            remove_file_if_exists(&self.checkpoints_dir().join("mbox_index.checkpoint.json"))?;
            remove_path_if_exists(&self.checkpoints_dir().join("mbox_index-rs"))?;
        }
        if stages.contains("clean") {
            self.archive_dir(&self.clean_dir(), "clean", &archive_root)?;
            self.archive_dir(&self.spam_dir(), "spam", &archive_root)?;
            self.archive_dir(&self.reports_dir(), "reports", &archive_root)?;
        }

        let mut state = self.load_state()?;
        if let Some(stages_obj) = state
            .as_object_mut()
            .and_then(|obj| obj.get_mut("stages"))
            .and_then(Value::as_object_mut)
        {
            for stage in stages {
                stages_obj.remove(stage);
            }
        }
        self.save_state(state)
    }

    fn archive_dir(&self, path: &Path, name: &str, archive_root: &Path) -> anyhow::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let should_archive = if path.is_dir() {
            match fs::read_dir(path) {
                Ok(mut entries) => entries.next().is_some(),
                Err(_) => false,
            }
        } else {
            true
        };
        if !should_archive {
            return Ok(());
        }

        fs::create_dir_all(archive_root)?;
        let mut target = archive_root.join(name);
        if target.exists() {
            let mut idx = 1_u32;
            loop {
                let candidate = archive_root.join(format!("{name}-{idx}"));
                if !candidate.exists() {
                    target = candidate;
                    break;
                }
                idx += 1;
            }
        }
        fs::rename(path, &target)?;
        fs::create_dir_all(path)?;
        Ok(())
    }
}

fn remove_file_if_exists(path: &Path) -> anyhow::Result<()> {
    if path.is_file() {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> anyhow::Result<()> {
    if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else if path.is_file() {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
        let _ = fs::remove_dir_all(&ws.root);
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
        let _ = fs::remove_dir_all(&ws.root);
    }

    #[test]
    fn workspace_refresh_archives_selected_dirs_and_clears_state() {
        let ws = temp_workspace("refresh");
        ws.ensure().expect("ensure");
        fs::write(ws.split_dir().join("2024-01.mbox"), b"data").expect("write split");
        fs::write(ws.clean_dir().join("2024-01.clean.jsonl"), b"{}\n").expect("write clean");
        fs::write(ws.spam_dir().join("2024-01.spam.jsonl"), b"{}\n").expect("write spam");
        fs::write(ws.reports_dir().join("2024-01.mbox.summary"), b"summary").expect("write rep");
        fs::write(ws.split_dir().join("mbox_index.jsonl"), b"{}\n").expect("write index");
        fs::create_dir_all(ws.checkpoints_dir().join("split-rs")).expect("mkdir split-rs");
        fs::create_dir_all(ws.checkpoints_dir().join("mbox_index-rs")).expect("mkdir index-rs");
        ws.update_stage("split", "done", None).expect("split state");
        ws.update_stage("index", "done", None).expect("index state");
        ws.update_stage("clean", "done", None).expect("clean state");

        let stages = ["split", "index", "clean"]
            .into_iter()
            .map(str::to_string)
            .collect();
        ws.apply_refresh(&stages).expect("refresh");

        assert!(!ws.split_dir().join("mbox_index.jsonl").exists());
        assert!(!ws.checkpoints_dir().join("split-rs").exists());
        assert!(!ws.checkpoints_dir().join("mbox_index-rs").exists());
        assert!(ws.split_dir().exists());
        assert!(ws.clean_dir().exists());
        assert!(ws.spam_dir().exists());
        assert!(ws.reports_dir().exists());

        let old_root = ws.root.join("old");
        assert!(old_root.exists());
        let old_entries = fs::read_dir(old_root).expect("old dir").count();
        assert!(old_entries >= 1);

        assert!(!ws.stage_done("split").expect("split done"));
        assert!(!ws.stage_done("index").expect("index done"));
        assert!(!ws.stage_done("clean").expect("clean done"));
        let _ = fs::remove_dir_all(&ws.root);
    }
}
