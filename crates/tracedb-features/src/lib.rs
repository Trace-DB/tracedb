#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tracedb_jobs::{JobCatalog, JobKind, TraceJob};
use tracedb_modules::{
    AccessPathDescriptor, ExplainHookDescriptor, SegmentCodecDescriptor, TraceDbModule,
    TypeDescriptor, WalDecoderDescriptor,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FeatureFreshnessMode {
    Strict,
    Lazy,
    AllowDirty,
    OnRead,
    AllowStale,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FeatureLifecycleStatus {
    Ready,
    Dirty,
    Pending,
    Failed,
    Stale,
    Deprecated,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FeatureLifecycle {
    pub feature_id: String,
    pub source_columns: Vec<String>,
    pub status: FeatureLifecycleStatus,
    pub source_hash: u64,
    pub valid_for_epoch: u64,
    pub last_job_id: Option<String>,
}

impl FeatureLifecycle {
    pub fn new(feature_id: impl Into<String>, source_columns: Vec<String>) -> Self {
        Self {
            feature_id: feature_id.into(),
            source_columns,
            status: FeatureLifecycleStatus::Missing,
            source_hash: 0,
            valid_for_epoch: 0,
            last_job_id: None,
        }
    }

    pub fn mark_dirty(&mut self, epoch: u64, source_hash: u64) {
        self.status = FeatureLifecycleStatus::Dirty;
        self.valid_for_epoch = epoch;
        self.source_hash = source_hash;
    }

    pub fn enqueue_recompute(
        &mut self,
        jobs: &mut JobCatalog,
        target: &str,
    ) -> Result<TraceJob, String> {
        let job = jobs.enqueue(
            JobKind::GenerateEmbedding,
            target,
            format!("{}:{target}:{}", self.feature_id, self.source_hash),
        )?;
        self.last_job_id = Some(job.job_id.clone());
        self.status = FeatureLifecycleStatus::Pending;
        Ok(job)
    }

    pub fn mark_failed(&mut self) {
        self.status = FeatureLifecycleStatus::Failed;
    }

    pub fn is_usable_for(&self, mode: FeatureFreshnessMode) -> bool {
        match mode {
            FeatureFreshnessMode::Strict => self.status == FeatureLifecycleStatus::Ready,
            FeatureFreshnessMode::Lazy => {
                matches!(
                    self.status,
                    FeatureLifecycleStatus::Ready | FeatureLifecycleStatus::Dirty
                )
            }
            FeatureFreshnessMode::AllowDirty => {
                matches!(
                    self.status,
                    FeatureLifecycleStatus::Ready | FeatureLifecycleStatus::Dirty
                )
            }
            FeatureFreshnessMode::OnRead => self.status != FeatureLifecycleStatus::Failed,
            FeatureFreshnessMode::AllowStale => !matches!(
                self.status,
                FeatureLifecycleStatus::Failed | FeatureLifecycleStatus::Deprecated
            ),
        }
    }
}

pub struct FeaturesModule;

impl TraceDbModule for FeaturesModule {
    fn module_id(&self) -> &str {
        "tracedb-features"
    }

    fn types(&self) -> Vec<TypeDescriptor> {
        vec![TypeDescriptor {
            type_id: "FEATURE_STATE".to_string(),
        }]
    }

    fn access_paths(&self) -> Vec<AccessPathDescriptor> {
        vec![AccessPathDescriptor {
            access_path_id: "FeatureStatePath".to_string(),
            policy_aware: true,
        }]
    }

    fn segment_codecs(&self) -> Vec<SegmentCodecDescriptor> {
        vec![SegmentCodecDescriptor {
            codec_id: "feature-state-v1".to_string(),
        }]
    }

    fn wal_decoders(&self) -> Vec<WalDecoderDescriptor> {
        vec![WalDecoderDescriptor {
            decoder_id: "feature-wal-v1".to_string(),
        }]
    }

    fn explain_hooks(&self) -> Vec<ExplainHookDescriptor> {
        vec![ExplainHookDescriptor {
            hook_id: "feature-explain-v1".to_string(),
        }]
    }
}
