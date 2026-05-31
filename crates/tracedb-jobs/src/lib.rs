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
    VacuumArtifacts,
    ReindexTable,
    ValidatePolicy,
    RefreshSummary,
    FeatureRefresh,
    ExportSubject,
    PurgeSubject,
    BackupDatabase,
    RestoreVerification,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_after_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_checksum: Option<[u8; 32]>,
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
            lease_token: None,
            lease_expires_at_ms: None,
            next_attempt_after_ms: None,
            artifact_path: None,
            artifact_checksum: None,
        }
    }

    pub fn lease(mut self, worker_id: WorkerId) -> Self {
        self = self.lease_at(worker_id, 0, 30_000);
        self
    }

    pub fn lease_at(mut self, worker_id: WorkerId, now_ms: u64, lease_ms: u64) -> Self {
        let next_attempt = self.attempts + 1;
        self.lease_token = Some(format!("lease:{}:{next_attempt}", self.job_id));
        self.lease_expires_at_ms = Some(now_ms.saturating_add(lease_ms.max(1)));
        self.lease_owner = Some(worker_id);
        self.attempts += 1;
        self.status = JobStatus::Leased;
        self.next_attempt_after_ms = None;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum JobEvent {
    Enqueued {
        job: TraceJob,
    },
    Leased {
        job_id: String,
        worker_id: WorkerId,
        lease_token: String,
        lease_expires_at_ms: u64,
    },
    Heartbeat {
        job_id: String,
        lease_token: String,
        lease_expires_at_ms: u64,
    },
    Completed {
        job_id: String,
        lease_token: String,
    },
    Failed {
        job_id: String,
        lease_token: Option<String>,
        error: String,
        permanent: bool,
        next_attempt_after_ms: u64,
    },
}

impl JobEvent {
    pub fn enqueued(job: TraceJob) -> Self {
        Self::Enqueued { job }
    }

    pub fn leased(
        job_id: impl Into<String>,
        worker_id: WorkerId,
        lease_token: impl Into<String>,
        lease_expires_at_ms: u64,
    ) -> Self {
        Self::Leased {
            job_id: job_id.into(),
            worker_id,
            lease_token: lease_token.into(),
            lease_expires_at_ms,
        }
    }

    pub fn completed(job_id: impl Into<String>, lease_token: impl Into<String>) -> Self {
        Self::Completed {
            job_id: job_id.into(),
            lease_token: lease_token.into(),
        }
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

    pub fn apply_event(&mut self, event: JobEvent) -> Result<(), String> {
        match event {
            JobEvent::Enqueued { job } => {
                self.jobs.entry(job.idempotency_key.clone()).or_insert(job);
            }
            JobEvent::Leased {
                job_id,
                worker_id,
                lease_token,
                lease_expires_at_ms,
            } => {
                let key = self
                    .key_for_job_id(&job_id)
                    .ok_or_else(|| format!("unknown job {job_id}"))?;
                let job = self.jobs.get_mut(&key).expect("key exists");
                job.lease_owner = Some(worker_id);
                job.lease_token = Some(lease_token);
                job.lease_expires_at_ms = Some(lease_expires_at_ms);
                job.next_attempt_after_ms = None;
                job.attempts += 1;
                job.status = JobStatus::Leased;
            }
            JobEvent::Heartbeat {
                job_id,
                lease_token,
                lease_expires_at_ms,
            } => {
                let job = self.job_mut_for_token(&job_id, Some(&lease_token))?;
                job.lease_expires_at_ms = Some(lease_expires_at_ms);
            }
            JobEvent::Completed {
                job_id,
                lease_token,
            } => {
                self.complete(job_id, Some(&lease_token))?;
            }
            JobEvent::Failed {
                job_id,
                lease_token,
                error,
                permanent,
                next_attempt_after_ms,
            } => {
                self.fail(
                    job_id,
                    lease_token.as_deref(),
                    error,
                    permanent,
                    next_attempt_after_ms,
                )?;
            }
        }
        Ok(())
    }

    pub fn lease_next(
        &mut self,
        worker_id: WorkerId,
        kind: JobKind,
    ) -> Result<Option<TraceJob>, String> {
        self.lease_next_at(worker_id, kind, 0, 30_000)
    }

    pub fn lease_next_at(
        &mut self,
        worker_id: WorkerId,
        kind: JobKind,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<TraceJob>, String> {
        let key = self
            .jobs
            .iter()
            .find(|(_, job)| job.kind == kind && job_is_leasable(job, now_ms))
            .map(|(key, _)| key.clone());
        let Some(key) = key else {
            return Ok(None);
        };
        let leased = self
            .jobs
            .remove(&key)
            .expect("key from jobs")
            .lease_at(worker_id, now_ms, lease_ms);
        self.jobs.insert(key, leased.clone());
        Ok(Some(leased))
    }

    pub fn heartbeat(
        &mut self,
        job_id: impl AsRef<str>,
        lease_token: impl AsRef<str>,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<TraceJob, String> {
        let job = self.job_mut_for_token(job_id.as_ref(), Some(lease_token.as_ref()))?;
        job.lease_expires_at_ms = Some(now_ms.saturating_add(lease_ms.max(1)));
        Ok(job.clone())
    }

    pub fn complete(
        &mut self,
        job_id: impl AsRef<str>,
        lease_token: Option<&str>,
    ) -> Result<TraceJob, String> {
        let job = self.job_mut_for_token(job_id.as_ref(), lease_token)?;
        job.status = JobStatus::Succeeded;
        job.lease_expires_at_ms = None;
        job.next_attempt_after_ms = None;
        Ok(job.clone())
    }

    pub fn fail(
        &mut self,
        job_id: impl AsRef<str>,
        lease_token: Option<&str>,
        error: impl Into<String>,
        permanent: bool,
        next_attempt_after_ms: u64,
    ) -> Result<TraceJob, String> {
        let job = self.job_mut_for_token(job_id.as_ref(), lease_token)?;
        job.last_error = Some(error.into());
        job.lease_expires_at_ms = None;
        job.next_attempt_after_ms = Some(next_attempt_after_ms);
        job.status = if permanent {
            JobStatus::FailedPermanent
        } else if job.attempts >= job.max_attempts {
            JobStatus::DeadLettered
        } else {
            JobStatus::FailedRetryable
        };
        Ok(job.clone())
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

    fn key_for_job_id(&self, job_id: &str) -> Option<String> {
        self.jobs
            .iter()
            .find(|(_, job)| job.job_id == job_id)
            .map(|(key, _)| key.clone())
    }

    fn job_mut_for_token(
        &mut self,
        job_id: &str,
        lease_token: Option<&str>,
    ) -> Result<&mut TraceJob, String> {
        let key = self
            .key_for_job_id(job_id)
            .ok_or_else(|| format!("unknown job {job_id}"))?;
        let job = self.jobs.get_mut(&key).expect("key exists");
        if let Some(lease_token) = lease_token {
            if job.lease_token.as_deref() != Some(lease_token) {
                return Err(format!("invalid lease token for {job_id}"));
            }
        }
        Ok(job)
    }
}

fn job_is_leasable(job: &TraceJob, now_ms: u64) -> bool {
    match job.status {
        JobStatus::Queued => true,
        JobStatus::FailedRetryable => job
            .next_attempt_after_ms
            .map(|ready_at| ready_at <= now_ms)
            .unwrap_or(true),
        JobStatus::Leased | JobStatus::Running => job
            .lease_expires_at_ms
            .map(|expires_at| expires_at <= now_ms)
            .unwrap_or(false),
        _ => false,
    }
}

fn kind_name(kind: &JobKind) -> &'static str {
    match kind {
        JobKind::GenerateEmbedding => "generate_embedding",
        JobKind::RegenerateEmbedding => "regenerate_embedding",
        JobKind::BuildTextIndex => "build_text_index",
        JobKind::BuildVectorIndex => "build_vector_index",
        JobKind::CompactSegment => "compact_segment",
        JobKind::VacuumArtifacts => "vacuum_artifacts",
        JobKind::ReindexTable => "reindex_table",
        JobKind::ValidatePolicy => "validate_policy",
        JobKind::RefreshSummary => "refresh_summary",
        JobKind::FeatureRefresh => "feature_refresh",
        JobKind::ExportSubject => "export_subject",
        JobKind::PurgeSubject => "purge_subject",
        JobKind::BackupDatabase => "backup_database",
        JobKind::RestoreVerification => "restore_verification",
        JobKind::VerifyDatabase => "verify_database",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_catalog_applies_wal_events_for_idempotent_replay_and_status_changes() {
        let mut catalog = JobCatalog::default();
        let job = TraceJob::new(JobKind::CompactSegment, "segment:seg-1", "compact:seg-1");
        let events = vec![
            JobEvent::enqueued(job.clone()),
            JobEvent::leased(
                job.job_id.clone(),
                WorkerId::new("worker-1"),
                "lease-token-1",
                1_000,
            ),
            JobEvent::completed(job.job_id.clone(), "lease-token-1"),
        ];

        for event in events {
            catalog.apply_event(event).expect("apply event");
        }

        let jobs = catalog.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, JobStatus::Succeeded);
        assert_eq!(jobs[0].lease_owner, Some(WorkerId::new("worker-1")));
    }

    #[test]
    fn job_catalog_leases_expired_jobs_retries_and_dead_letters() {
        let mut catalog = JobCatalog::default();
        catalog
            .enqueue(JobKind::BuildVectorIndex, "segment:seg-1", "vector:seg-1")
            .expect("enqueue");

        let first = catalog
            .lease_next_at(
                WorkerId::new("worker-1"),
                JobKind::BuildVectorIndex,
                1_000,
                100,
            )
            .expect("lease")
            .expect("job");
        let blocked = catalog
            .lease_next_at(
                WorkerId::new("worker-2"),
                JobKind::BuildVectorIndex,
                1_050,
                100,
            )
            .expect("lease blocked");
        assert!(blocked.is_none());

        let expired = catalog
            .lease_next_at(
                WorkerId::new("worker-2"),
                JobKind::BuildVectorIndex,
                1_101,
                100,
            )
            .expect("expired lease")
            .expect("job after expiry");
        assert_ne!(first.lease_token, expired.lease_token);
        assert_eq!(expired.lease_owner, Some(WorkerId::new("worker-2")));

        catalog
            .fail(
                expired.job_id.clone(),
                expired.lease_token.as_deref(),
                "boom",
                false,
                1_200,
            )
            .expect("retryable failure");
        let retry = catalog
            .lease_next_at(
                WorkerId::new("worker-3"),
                JobKind::BuildVectorIndex,
                1_200,
                100,
            )
            .expect("lease retry")
            .expect("retry job");
        catalog
            .fail(
                retry.job_id.clone(),
                retry.lease_token.as_deref(),
                "boom again",
                false,
                1_300,
            )
            .expect("dead letter failure");

        let jobs = catalog.jobs();
        assert_eq!(jobs[0].status, JobStatus::DeadLettered);
        assert_eq!(jobs[0].last_error.as_deref(), Some("boom again"));
    }
}
