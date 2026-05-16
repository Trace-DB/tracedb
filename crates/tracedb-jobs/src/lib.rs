#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum JobKind {
    GenerateEmbedding,
    RegenerateEmbedding,
    BuildTextIndex,
    BuildVectorIndex,
    CompactSegment,
    ReindexTable,
    ValidatePolicy,
    RefreshSummary,
    ExportSubject,
    PurgeSubject,
    BackupDatabase,
    VerifyDatabase,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JobStatus {
    Queued,
    Leased,
    Running,
    CommittingResult,
    Succeeded,
    FailedRetryable,
    FailedPermanent,
    Canceled,
    DeadLettered,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerId(pub String);

impl WorkerId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceJob {
    pub job_id: String,
    pub kind: JobKind,
    pub target: String,
    pub idempotency_key: String,
    pub lease_owner: Option<WorkerId>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub status: JobStatus,
    pub last_error: Option<String>,
}

impl TraceJob {
    pub fn new(
        kind: JobKind,
        target: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Self {
        let target = target.into();
        let idempotency_key = idempotency_key.into();
        Self {
            job_id: format!("job:{}:{idempotency_key}", kind_name(&kind)),
            kind,
            target,
            idempotency_key,
            lease_owner: None,
            attempts: 0,
            max_attempts: 3,
            status: JobStatus::Queued,
            last_error: None,
        }
    }

    pub fn lease(mut self, worker_id: WorkerId) -> Self {
        self.lease_owner = Some(worker_id);
        self.attempts += 1;
        self.status = JobStatus::Leased;
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JobCatalog {
    jobs: BTreeMap<String, TraceJob>,
}

impl JobCatalog {
    pub fn enqueue(
        &mut self,
        kind: JobKind,
        target: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Result<TraceJob, String> {
        let job = TraceJob::new(kind, target, idempotency_key);
        let existing = self.jobs.entry(job.idempotency_key.clone()).or_insert(job);
        Ok(existing.clone())
    }

    pub fn lease_next(
        &mut self,
        worker_id: WorkerId,
        kind: JobKind,
    ) -> Result<Option<TraceJob>, String> {
        let key = self
            .jobs
            .iter()
            .find(|(_, job)| job.kind == kind && job.status == JobStatus::Queued)
            .map(|(key, _)| key.clone());
        let Some(key) = key else {
            return Ok(None);
        };
        let leased = self
            .jobs
            .remove(&key)
            .expect("key from jobs")
            .lease(worker_id);
        self.jobs.insert(key, leased.clone());
        Ok(Some(leased))
    }

    pub fn depth_by_status(&self, status: JobStatus) -> usize {
        self.jobs
            .values()
            .filter(|job| job.status == status)
            .count()
    }

    pub fn jobs(&self) -> Vec<TraceJob> {
        self.jobs.values().cloned().collect()
    }
}

fn kind_name(kind: &JobKind) -> &'static str {
    match kind {
        JobKind::GenerateEmbedding => "generate_embedding",
        JobKind::RegenerateEmbedding => "regenerate_embedding",
        JobKind::BuildTextIndex => "build_text_index",
        JobKind::BuildVectorIndex => "build_vector_index",
        JobKind::CompactSegment => "compact_segment",
        JobKind::ReindexTable => "reindex_table",
        JobKind::ValidatePolicy => "validate_policy",
        JobKind::RefreshSummary => "refresh_summary",
        JobKind::ExportSubject => "export_subject",
        JobKind::PurgeSubject => "purge_subject",
        JobKind::BackupDatabase => "backup_database",
        JobKind::VerifyDatabase => "verify_database",
    }
}
