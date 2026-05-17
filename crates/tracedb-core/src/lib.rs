#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, TraceDbError>;

#[derive(Debug, Error)]
pub enum TraceDbError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unknown table: {0}")]
    UnknownTable(String),
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
    #[error("invalid record: {0}")]
    InvalidRecord(String),
    #[error("invalid vector dimensions for {column}: expected {expected}, got {actual}")]
    InvalidVectorDimensions {
        column: String,
        expected: usize,
        actual: usize,
    },
    #[error("wal corruption: {0}")]
    WalCorruption(String),
    #[error("manifest corruption: {0}")]
    ManifestCorruption(String),
    #[error("module {module} rejected: {reason}")]
    ModuleRejected { module: String, reason: String },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid command: {0}")]
    InvalidCommand(String),
}

#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash,
)]
pub struct Epoch(u64);

impl Epoch {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash,
)]
pub struct Lsn(u64);

impl Lsn {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash,
)]
pub struct VersionId(u64);

impl VersionId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FeatureStatus {
    Ready,
    Dirty,
    Pending,
    Failed,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DerivedFeatureState {
    pub source_columns: Vec<String>,
    pub source_hash: u64,
    pub model_id: Option<String>,
    pub model_version: Option<String>,
    pub created_epoch: Epoch,
    pub valid_for_epoch: Epoch,
    pub status: FeatureStatus,
}

impl DerivedFeatureState {
    pub fn ready(source_columns: Vec<String>, source_hash: u64, epoch: Epoch) -> Self {
        Self {
            source_columns,
            source_hash,
            model_id: None,
            model_version: None,
            created_epoch: epoch,
            valid_for_epoch: epoch,
            status: FeatureStatus::Ready,
        }
    }

    pub fn missing(source_columns: Vec<String>, epoch: Epoch) -> Self {
        Self {
            source_columns,
            source_hash: 0,
            model_id: None,
            model_version: None,
            created_epoch: epoch,
            valid_for_epoch: epoch,
            status: FeatureStatus::Missing,
        }
    }

    pub fn dirty_from(previous: &Self, source_hash: u64, epoch: Epoch) -> Self {
        let mut state = previous.clone();
        state.source_hash = source_hash;
        state.valid_for_epoch = epoch;
        state.status = FeatureStatus::Dirty;
        state
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorColumnSchema {
    pub name: String,
    pub dimensions: usize,
    pub source_columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub primary_id_column: String,
    pub tenant_id_column: String,
    pub scalar_columns: Vec<String>,
    pub text_indexed_columns: Vec<String>,
    pub vector_columns: Vec<VectorColumnSchema>,
}

impl TableSchema {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(TraceDbError::InvalidSchema(
                "table name cannot be empty".to_string(),
            ));
        }
        if self.primary_id_column.trim().is_empty() {
            return Err(TraceDbError::InvalidSchema(
                "primary id column cannot be empty".to_string(),
            ));
        }
        if self.tenant_id_column.trim().is_empty() {
            return Err(TraceDbError::InvalidSchema(
                "tenant id column cannot be empty".to_string(),
            ));
        }
        for vector in &self.vector_columns {
            if vector.dimensions == 0 {
                return Err(TraceDbError::InvalidSchema(format!(
                    "vector column {} must have dimensions",
                    vector.name
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordInput {
    pub table: String,
    pub id: String,
    pub tenant_id: String,
    pub fields: Map<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordDeletion {
    pub table: String,
    pub id: String,
    pub tenant_id: String,
    pub tombstone: String,
}

impl RecordDeletion {
    pub fn new(
        table: impl Into<String>,
        tenant_id: impl Into<String>,
        id: impl Into<String>,
        tombstone: impl Into<String>,
    ) -> Self {
        Self {
            table: table.into(),
            tenant_id: tenant_id.into(),
            id: id.into(),
            tombstone: tombstone.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FeatureInvalidation {
    pub table: String,
    #[serde(default)]
    pub tenant_id: String,
    pub record_id: String,
    pub feature: String,
    pub status: FeatureStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModuleCommitEvent {
    pub module_id: String,
    pub event: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModuleManifest {
    pub module_id: String,
    pub version: String,
    pub trust_level: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SegmentState {
    Building,
    Built,
    Verifying,
    Verified,
    Publishing,
    Published,
    Superseded,
    Reclaimable,
    Deleted,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SegmentManifest {
    pub segment_id: String,
    pub generation: u64,
    pub state: SegmentState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndexState {
    Pending,
    Building,
    Ready,
    Stale,
    Superseded,
    Reclaimable,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexManifest {
    pub index_id: String,
    pub segment_id: String,
    pub generation: u64,
    pub kind: String,
    pub state: IndexState,
    pub policy_aware: bool,
    pub parent_manifest_generation: u64,
    pub object_path: String,
    pub checksum: u32,
    pub created_epoch: Epoch,
    pub ready_epoch: Option<Epoch>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestChecksums {
    pub parent_checksum: u32,
    pub manifest_checksum: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceDbManifest {
    pub format_version: u32,
    pub manifest_generation: u64,
    pub database_id: String,
    pub database_uuid: String,
    pub branch_id: String,
    pub parent_branch: Option<String>,
    pub branch_base_epoch: Option<Epoch>,
    pub object_prefix: Option<String>,
    pub latest_epoch: Epoch,
    pub durable_epoch: Epoch,
    pub checkpoint_epoch: Epoch,
    pub active_wal: u64,
    pub wal_start_lsn: Lsn,
    pub wal_end_lsn: Lsn,
    pub schemas: Vec<TableSchema>,
    pub segments: Vec<SegmentManifest>,
    pub indexes: Vec<IndexManifest>,
    pub feature_models: Vec<String>,
    pub modules: Vec<ModuleManifest>,
    pub job_queues: Vec<String>,
    pub snapshots: Vec<String>,
    pub checksums: ManifestChecksums,
}

impl TraceDbManifest {
    pub fn empty(database_id: String) -> Self {
        Self {
            format_version: 1,
            manifest_generation: 1,
            database_uuid: format!("uuid-{database_id}"),
            database_id,
            branch_id: "main".to_string(),
            parent_branch: None,
            branch_base_epoch: None,
            object_prefix: None,
            latest_epoch: Epoch::new(0),
            durable_epoch: Epoch::new(0),
            checkpoint_epoch: Epoch::new(0),
            active_wal: 1,
            wal_start_lsn: Lsn::new(1),
            wal_end_lsn: Lsn::new(0),
            schemas: Vec::new(),
            segments: Vec::new(),
            indexes: Vec::new(),
            feature_models: Vec::new(),
            modules: builtin_module_manifests(),
            job_queues: vec![
                "tracedb.embedding.generate".to_string(),
                "tracedb.embedding.rebuild".to_string(),
                "tracedb.segment.compact".to_string(),
                "tracedb.index.vector.build".to_string(),
                "tracedb.index.text.build".to_string(),
                "tracedb.snapshot.create".to_string(),
                "tracedb.snapshot.upload".to_string(),
                "tracedb.restore.verify".to_string(),
                "tracedb.usage.rollup".to_string(),
                "tracedb.retention.cleanup".to_string(),
            ],
            snapshots: Vec::new(),
            checksums: ManifestChecksums::default(),
        }
    }

    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.schemas.iter().find(|schema| schema.name == name)
    }
}

pub fn compute_manifest_checksum(manifest: &TraceDbManifest) -> Result<u32> {
    let mut normalized = manifest.clone();
    normalized.checksums.manifest_checksum = 0;
    let bytes = serde_json::to_vec(&normalized)?;
    Ok(checksum_bytes(&bytes))
}

pub fn builtin_module_manifests() -> Vec<ModuleManifest> {
    [
        "tracedb-text",
        "tracedb-vector",
        "tracedb-graph",
        "tracedb-temporal",
        "tracedb-policy",
        "tracedb-provenance",
        "tracedb-features",
        "tracedb-retrieval-core",
    ]
    .into_iter()
    .map(|module_id| ModuleManifest {
        module_id: module_id.to_string(),
        version: "0.1.0".to_string(),
        trust_level: "FIRST_PARTY_SIGNED".to_string(),
    })
    .collect()
}

pub fn checksum_bytes(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

pub fn source_hash(fields: &Map<String, Value>, source_columns: &[String]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for column in source_columns {
        fnv1a_update(&mut hash, column.as_bytes());
        fnv1a_update(&mut hash, b"=");
        if let Some(value) = fields.get(column) {
            fnv1a_update(&mut hash, value.to_string().as_bytes());
        }
        fnv1a_update(&mut hash, &[0]);
    }
    hash
}

fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

pub fn value_as_f32_vec(value: &Value) -> Option<Vec<f32>> {
    let array = value.as_array()?;
    let mut out = Vec::with_capacity(array.len());
    for item in array {
        out.push(item.as_f64()? as f32);
    }
    Some(out)
}

pub fn database_id_from_path(path: impl Into<PathBuf>) -> String {
    let path = path.into();
    let checksum = checksum_bytes(path.to_string_lossy().as_bytes());
    format!("db-{checksum:08x}")
}
