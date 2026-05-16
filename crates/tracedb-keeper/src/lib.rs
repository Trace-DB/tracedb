#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
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
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let tmp_path = path.with_extension("json.tmp");
        let body = serde_json::to_vec_pretty(self).map_err(|error| error.to_string())?;
        fs::write(&tmp_path, body).map_err(|error| error.to_string())?;
        fs::rename(&tmp_path, path).map_err(|error| error.to_string())?;
        Ok(())
    }
}

fn idempotency_key(branch_id: &str, idempotency_key: &str) -> String {
    format!("{branch_id}\u{1f}{idempotency_key}")
}
