#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub branch_id: String,
    pub idempotency_key: String,
    pub payload: String,
}

impl CommitRequest {
    pub fn new(
        branch_id: impl Into<String>,
        idempotency_key: impl Into<String>,
        payload: impl Into<String>,
    ) -> Self {
        Self {
            branch_id: branch_id.into(),
            idempotency_key: idempotency_key.into(),
            payload: payload.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitAck {
    pub branch_id: String,
    pub epoch: u64,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableCommit {
    pub branch_id: String,
    pub epoch: u64,
    pub idempotency_key: String,
    pub payload: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BranchWalService {
    next_epoch: BTreeMap<String, u64>,
    idempotent: BTreeMap<String, CommitAck>,
    commit_log: Vec<DurableCommit>,
    #[serde(skip)]
    durable_path: Option<PathBuf>,
}

impl BranchWalService {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();
        let mut service = if path.exists() {
            serde_json::from_slice(&fs::read(&path).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?
        } else {
            Self::default()
        };
        service.durable_path = Some(path);
        Ok(service)
    }

    pub fn commit(&mut self, request: CommitRequest) -> Result<CommitAck, String> {
        let key = idempotency_key(&request.branch_id, &request.idempotency_key);
        if let Some(existing) = self.idempotent.get(&key) {
            return Ok(existing.clone());
        }
        let epoch = self
            .next_epoch
            .entry(request.branch_id.clone())
            .or_insert(1);
        let ack = CommitAck {
            branch_id: request.branch_id,
            epoch: *epoch,
            idempotency_key: request.idempotency_key,
        };
        *epoch += 1;
        self.commit_log.push(DurableCommit {
            branch_id: ack.branch_id.clone(),
            epoch: ack.epoch,
            idempotency_key: ack.idempotency_key.clone(),
            payload: request.payload,
        });
        self.idempotent.insert(key, ack.clone());
        self.persist()?;
        Ok(ack)
    }

    pub fn commit_log(&self) -> &[DurableCommit] {
        &self.commit_log
    }

    fn persist(&self) -> Result<(), String> {
        let Some(path) = &self.durable_path else {
            return Ok(());
        };
        let body = serde_json::to_vec_pretty(self).map_err(|error| error.to_string())?;
        persist_body(path, &body)
    }
}

fn idempotency_key(branch_id: &str, idempotency_key: &str) -> String {
    format!("{branch_id}\u{1f}{idempotency_key}")
}

trait DurableWriteOps {
    fn create_dir_all(&mut self, path: &Path) -> Result<(), String>;
    fn write_file(&mut self, path: &Path, body: &[u8]) -> Result<(), String>;
    fn sync_file(&mut self, path: &Path) -> Result<(), String>;
    fn rename(&mut self, source: &Path, target: &Path) -> Result<(), String>;
    fn sync_dir(&mut self, path: &Path) -> Result<(), String>;
}

struct StdDurableWriteOps;

impl DurableWriteOps for StdDurableWriteOps {
    fn create_dir_all(&mut self, path: &Path) -> Result<(), String> {
        fs::create_dir_all(path).map_err(|error| error.to_string())
    }

    fn write_file(&mut self, path: &Path, body: &[u8]) -> Result<(), String> {
        let mut file = File::create(path).map_err(|error| error.to_string())?;
        file.write_all(body).map_err(|error| error.to_string())
    }

    fn sync_file(&mut self, path: &Path) -> Result<(), String> {
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())
    }

    fn rename(&mut self, source: &Path, target: &Path) -> Result<(), String> {
        fs::rename(source, target).map_err(|error| error.to_string())
    }

    fn sync_dir(&mut self, path: &Path) -> Result<(), String> {
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())
    }
}

fn persist_body(path: &Path, body: &[u8]) -> Result<(), String> {
    let mut ops = StdDurableWriteOps;
    persist_body_with_ops(path, body, &mut ops)
}

fn persist_body_with_ops(
    path: &Path,
    body: &[u8],
    ops: &mut impl DurableWriteOps,
) -> Result<(), String> {
    let parent = non_empty_parent(path);
    if let Some(parent) = parent {
        ops.create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("json.tmp");
    ops.write_file(&tmp_path, body)?;
    ops.sync_file(&tmp_path)?;
    ops.rename(&tmp_path, path)?;
    if let Some(parent) = parent {
        ops.sync_dir(parent)?;
    }
    Ok(())
}

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingOps {
        events: Vec<String>,
    }

    impl DurableWriteOps for RecordingOps {
        fn create_dir_all(&mut self, path: &Path) -> Result<(), String> {
            self.events.push(format!("mkdir:{}", path.display()));
            Ok(())
        }

        fn write_file(&mut self, path: &Path, body: &[u8]) -> Result<(), String> {
            self.events
                .push(format!("write:{}:{}", path.display(), body.len()));
            Ok(())
        }

        fn sync_file(&mut self, path: &Path) -> Result<(), String> {
            self.events.push(format!("sync_file:{}", path.display()));
            Ok(())
        }

        fn rename(&mut self, source: &Path, target: &Path) -> Result<(), String> {
            self.events
                .push(format!("rename:{}->{}", source.display(), target.display()));
            Ok(())
        }

        fn sync_dir(&mut self, path: &Path) -> Result<(), String> {
            self.events.push(format!("sync_dir:{}", path.display()));
            Ok(())
        }
    }

    #[test]
    fn durable_persist_syncs_temp_file_before_rename_and_parent_after() {
        let mut ops = RecordingOps::default();
        let path = PathBuf::from("/keeper/state.json");

        persist_body_with_ops(&path, b"{}", &mut ops).expect("persist body");

        assert_eq!(
            ops.events,
            vec![
                "mkdir:/keeper",
                "write:/keeper/state.json.tmp:2",
                "sync_file:/keeper/state.json.tmp",
                "rename:/keeper/state.json.tmp->/keeper/state.json",
                "sync_dir:/keeper",
            ]
        );
    }
}
