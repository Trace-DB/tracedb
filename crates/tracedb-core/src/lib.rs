#![forbid(unsafe_code)]

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chacha20poly1305::{
    aead::rand_core::RngCore,
    aead::{Aead, AeadCore, KeyInit, OsRng, Payload},
    XChaCha20Poly1305,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, TraceDbError>;

pub const ENCRYPTED_ARTIFACT_MAGIC: &[u8; 8] = b"TDBENC01";
pub const ARTIFACT_ENVELOPE_MAGIC: &[u8; 8] = b"TDBART01";
const XCHACHA20POLY1305_NONCE_LEN: usize = 24;

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
    #[error("crypto error: {0}")]
    Crypto(String),
    #[error("artifact corruption: {0}")]
    ArtifactCorruption(String),
    #[error("module {module} rejected: {reason}")]
    ModuleRejected { module: String, reason: String },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid command: {0}")]
    InvalidCommand(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactEnvelopeHeader {
    pub version: u32,
    pub kind: String,
    pub codec: String,
    pub segment_id: String,
    pub generation: u64,
    pub manifest_generation: u64,
    pub epoch_min: u64,
    pub epoch_max: u64,
    pub source_segment_checksum: u32,
    pub payload_len: u64,
    pub payload_checksum: u32,
    pub object_checksum: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactEnvelope {
    pub header: ArtifactEnvelopeHeader,
    pub payload: Vec<u8>,
}

impl ArtifactEnvelopeHeader {
    pub fn new(
        kind: impl Into<String>,
        codec: impl Into<String>,
        segment_id: impl Into<String>,
        generation: u64,
        manifest_generation: u64,
        epoch_min: u64,
        epoch_max: u64,
        source_segment_checksum: u32,
        payload: &[u8],
    ) -> Self {
        Self {
            version: 1,
            kind: kind.into(),
            codec: codec.into(),
            segment_id: segment_id.into(),
            generation,
            manifest_generation,
            epoch_min,
            epoch_max,
            source_segment_checksum,
            payload_len: payload.len() as u64,
            payload_checksum: checksum_bytes(payload),
            object_checksum: 0,
        }
    }
}

pub fn encode_artifact_envelope(
    mut header: ArtifactEnvelopeHeader,
    payload: &[u8],
) -> Result<Vec<u8>> {
    if header.payload_len != payload.len() as u64 {
        return Err(TraceDbError::ArtifactCorruption(format!(
            "artifact payload length mismatch: header {}, actual {}",
            header.payload_len,
            payload.len()
        )));
    }
    let payload_checksum = checksum_bytes(payload);
    if header.payload_checksum != payload_checksum {
        return Err(TraceDbError::ArtifactCorruption(format!(
            "artifact payload checksum mismatch: header {}, actual {payload_checksum}",
            header.payload_checksum
        )));
    }
    header.object_checksum = 0;
    let header_without_checksum = serialize_artifact_header(&header)?;
    header.object_checksum =
        checksum_bytes(&[header_without_checksum.as_slice(), payload].concat());
    let header_bytes = serialize_artifact_header(&header)?;
    let mut out =
        Vec::with_capacity(ARTIFACT_ENVELOPE_MAGIC.len() + 4 + header_bytes.len() + payload.len());
    out.extend_from_slice(ARTIFACT_ENVELOPE_MAGIC);
    out.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn decode_artifact_envelope(bytes: &[u8]) -> Result<ArtifactEnvelope> {
    if !bytes.starts_with(ARTIFACT_ENVELOPE_MAGIC) {
        return Err(TraceDbError::ArtifactCorruption(
            "artifact envelope magic mismatch".to_string(),
        ));
    }
    let header_len_offset = ARTIFACT_ENVELOPE_MAGIC.len();
    let header_len_end = header_len_offset + 4;
    if bytes.len() < header_len_end {
        return Err(TraceDbError::ArtifactCorruption(
            "artifact envelope truncated before header length".to_string(),
        ));
    }
    let header_len = u32::from_le_bytes(
        bytes[header_len_offset..header_len_end]
            .try_into()
            .expect("slice length"),
    ) as usize;
    let header_end = header_len_end + header_len;
    if bytes.len() < header_end {
        return Err(TraceDbError::ArtifactCorruption(
            "artifact envelope truncated before header".to_string(),
        ));
    }
    let header: ArtifactEnvelopeHeader =
        bincode::deserialize(&bytes[header_len_end..header_end])
            .map_err(|error| TraceDbError::ArtifactCorruption(error.to_string()))?;
    let payload = bytes[header_end..].to_vec();
    if header.payload_len != payload.len() as u64 {
        return Err(TraceDbError::ArtifactCorruption(format!(
            "artifact payload length mismatch: header {}, actual {}",
            header.payload_len,
            payload.len()
        )));
    }
    let payload_checksum = checksum_bytes(&payload);
    if header.payload_checksum != payload_checksum {
        return Err(TraceDbError::ArtifactCorruption(format!(
            "artifact payload checksum mismatch: expected {}, got {payload_checksum}",
            header.payload_checksum
        )));
    }
    let mut header_without_checksum = header.clone();
    header_without_checksum.object_checksum = 0;
    let header_bytes = serialize_artifact_header(&header_without_checksum)?;
    let object_checksum = checksum_bytes(&[header_bytes.as_slice(), payload.as_slice()].concat());
    if header.object_checksum != object_checksum {
        return Err(TraceDbError::ArtifactCorruption(format!(
            "artifact object checksum mismatch: expected {}, got {object_checksum}",
            header.object_checksum
        )));
    }
    Ok(ArtifactEnvelope { header, payload })
}

fn serialize_artifact_header(header: &ArtifactEnvelopeHeader) -> Result<Vec<u8>> {
    bincode::serialize(header).map_err(|error| TraceDbError::ArtifactCorruption(error.to_string()))
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
        validate_schema_identifier(&self.name, "table name")?;
        let primary_id_column =
            validate_schema_identifier(&self.primary_id_column, "primary id column")?;
        let tenant_id_column =
            validate_schema_identifier(&self.tenant_id_column, "tenant id column")?;
        if primary_id_column == tenant_id_column {
            return Err(TraceDbError::InvalidSchema(
                "primary id column and tenant id column must be distinct".to_string(),
            ));
        }

        let mut declared_source_columns = BTreeSet::new();
        declared_source_columns.insert(primary_id_column.clone());
        declared_source_columns.insert(tenant_id_column.clone());

        let scalar_columns = validate_column_set(&self.scalar_columns, "scalar column")?;
        for column in &scalar_columns {
            reject_identity_overlap(column, &primary_id_column, "primary id", "scalar")?;
            reject_identity_overlap(column, &tenant_id_column, "tenant id", "scalar")?;
            declared_source_columns.insert(column.clone());
        }

        let text_columns = validate_column_set(&self.text_indexed_columns, "text indexed column")?;
        for column in &text_columns {
            reject_identity_overlap(column, &primary_id_column, "primary id", "text indexed")?;
            reject_identity_overlap(column, &tenant_id_column, "tenant id", "text indexed")?;
            if scalar_columns.contains(column) {
                return Err(TraceDbError::InvalidSchema(format!(
                    "column {column} cannot be both scalar and text indexed"
                )));
            }
            declared_source_columns.insert(column.clone());
        }

        let mut vector_names = BTreeSet::new();
        for vector in &self.vector_columns {
            let vector_name = validate_schema_identifier(&vector.name, "vector column name")?;
            if !vector_names.insert(vector_name.clone()) {
                return Err(TraceDbError::InvalidSchema(format!(
                    "duplicate vector column {vector_name}"
                )));
            }
            reject_reserved_result_column(&vector_name)?;
            reject_identity_overlap(&vector_name, &primary_id_column, "primary id", "vector")?;
            reject_identity_overlap(&vector_name, &tenant_id_column, "tenant id", "vector")?;
            if scalar_columns.contains(&vector_name) {
                return Err(TraceDbError::InvalidSchema(format!(
                    "column {vector_name} cannot be both scalar and vector"
                )));
            }
            if text_columns.contains(&vector_name) {
                return Err(TraceDbError::InvalidSchema(format!(
                    "column {vector_name} cannot be both text indexed and vector"
                )));
            }
            if vector.dimensions == 0 {
                return Err(TraceDbError::InvalidSchema(format!(
                    "vector column {} must have dimensions",
                    vector_name
                )));
            }
            if vector.source_columns.is_empty() {
                return Err(TraceDbError::InvalidSchema(format!(
                    "vector column {vector_name} must declare source columns"
                )));
            }
            let mut source_seen = BTreeSet::new();
            for source_column in &vector.source_columns {
                let source_column =
                    validate_schema_identifier(source_column, "vector source column")?;
                if !source_seen.insert(source_column.clone()) {
                    return Err(TraceDbError::InvalidSchema(format!(
                        "duplicate vector column {vector_name} source column {source_column}"
                    )));
                }
                if !declared_source_columns.contains(&source_column) {
                    return Err(TraceDbError::InvalidSchema(format!(
                        "vector column {vector_name} source column {source_column} is not declared"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn validate_column_set(columns: &[String], kind: &str) -> Result<BTreeSet<String>> {
    let mut seen = BTreeSet::new();
    for column in columns {
        let column = validate_schema_identifier(column, kind)?;
        reject_reserved_result_column(&column)?;
        if !seen.insert(column.clone()) {
            return Err(TraceDbError::InvalidSchema(format!(
                "duplicate {kind} {column}"
            )));
        }
    }
    Ok(seen)
}

fn validate_schema_identifier(name: &str, context: &str) -> Result<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(TraceDbError::InvalidSchema(format!(
            "{context} cannot be empty"
        )));
    }
    if trimmed != name {
        return Err(TraceDbError::InvalidSchema(format!(
            "{context} {trimmed} must not have leading or trailing whitespace"
        )));
    }
    if trimmed.starts_with("__") {
        return Err(TraceDbError::InvalidSchema(format!(
            "{context} {trimmed} is reserved for GraphQL introspection"
        )));
    }
    if !is_graphql_safe_identifier(trimmed) {
        return Err(TraceDbError::InvalidSchema(format!(
            "{context} {trimmed} is not a GraphQL-safe identifier"
        )));
    }
    Ok(trimmed.to_string())
}

fn is_graphql_safe_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn reject_reserved_result_column(column: &str) -> Result<()> {
    if matches!(column, "record_id" | "version_id" | "fields" | "score") {
        return Err(TraceDbError::InvalidSchema(format!(
            "column {column} is reserved for TraceDB result metadata"
        )));
    }
    Ok(())
}

fn reject_identity_overlap(
    column: &str,
    identity_column: &str,
    identity_kind: &str,
    column_kind: &str,
) -> Result<()> {
    if column == identity_column {
        return Err(TraceDbError::InvalidSchema(format!(
            "column {column} cannot be both {identity_kind} and {column_kind}"
        )));
    }
    Ok(())
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
    #[serde(default)]
    pub table_set: Vec<String>,
    #[serde(default)]
    pub tenant_set: Vec<String>,
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
    #[serde(default)]
    pub source_segment_checksum: u32,
    #[serde(default)]
    pub payload_checksum: u32,
    #[serde(default)]
    pub artifact_format_version: u32,
    #[serde(default)]
    pub codec: String,
    pub created_epoch: Epoch,
    pub ready_epoch: Option<Epoch>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestChecksums {
    pub parent_checksum: u32,
    pub manifest_checksum: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EncryptionMetadata {
    pub algorithm: String,
    pub key_id: String,
    pub wrapped_dek_b64: String,
    pub dek_nonce_b64: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<EncryptionMetadata>,
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
            encryption: None,
            checksums: ManifestChecksums::default(),
        }
    }

    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.schemas.iter().find(|schema| schema.name == name)
    }
}

#[derive(Clone)]
pub struct MasterKey {
    bytes: [u8; 32],
    key_id: String,
}

impl fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MasterKey")
            .field("key_id", &self.key_id)
            .field("bytes", &"<redacted>")
            .finish()
    }
}

impl MasterKey {
    pub fn from_env() -> Result<Option<Self>> {
        match std::env::var("TRACEDB_MASTER_KEY_B64") {
            Ok(value) if value.trim().is_empty() => Ok(None),
            Ok(value) => Self::from_base64(&value, "TRACEDB_MASTER_KEY_B64").map(Some),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(TraceDbError::InvalidCommand(format!(
                "failed to read TRACEDB_MASTER_KEY_B64: {error}"
            ))),
        }
    }

    pub fn from_base64(value: &str, label: &str) -> Result<Self> {
        if value.chars().any(char::is_whitespace) {
            return Err(TraceDbError::InvalidCommand(format!(
                "{label} must be strict base64 without whitespace"
            )));
        }
        let decoded = BASE64_STANDARD
            .decode(value)
            .map_err(|_| TraceDbError::InvalidCommand(format!("{label} must be strict base64")))?;
        let bytes: [u8; 32] = decoded.try_into().map_err(|decoded: Vec<u8>| {
            TraceDbError::InvalidCommand(format!(
                "{label} must decode to exactly 32 bytes, got {}",
                decoded.len()
            ))
        })?;
        let digest = Sha256::digest(bytes);
        Ok(Self {
            bytes,
            key_id: format!("tracedb-root:{}", hex_prefix(&digest, 12)),
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }
}

#[derive(Clone)]
pub struct EncryptionContext {
    key_id: String,
    dek: [u8; 32],
}

impl fmt::Debug for EncryptionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptionContext")
            .field("key_id", &self.key_id)
            .field("dek", &"<redacted>")
            .finish()
    }
}

impl EncryptionContext {
    pub fn create(master_key: &MasterKey, database_id: &str) -> Result<(Self, EncryptionMetadata)> {
        let mut dek = [0u8; 32];
        let mut rng = OsRng;
        rng.fill_bytes(&mut dek);
        let cipher = XChaCha20Poly1305::new_from_slice(&master_key.bytes)
            .map_err(|error| TraceDbError::Crypto(error.to_string()))?;
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aad = dek_wrap_aad(database_id, master_key.key_id());
        let wrapped = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &dek,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|error| TraceDbError::Crypto(error.to_string()))?;
        let metadata = EncryptionMetadata {
            algorithm: "XChaCha20Poly1305".to_string(),
            key_id: master_key.key_id().to_string(),
            wrapped_dek_b64: BASE64_STANDARD.encode(wrapped),
            dek_nonce_b64: BASE64_STANDARD.encode(nonce),
        };
        Ok((
            Self {
                key_id: metadata.key_id.clone(),
                dek,
            },
            metadata,
        ))
    }

    pub fn from_metadata(
        master_key: &MasterKey,
        database_id: &str,
        metadata: &EncryptionMetadata,
    ) -> Result<Self> {
        if metadata.algorithm != "XChaCha20Poly1305" {
            return Err(TraceDbError::Crypto(format!(
                "unsupported encryption algorithm {}",
                metadata.algorithm
            )));
        }
        if metadata.key_id != master_key.key_id() {
            return Err(TraceDbError::Crypto(format!(
                "failed to unwrap database encryption key for key_id {}",
                metadata.key_id
            )));
        }
        let wrapped = BASE64_STANDARD
            .decode(&metadata.wrapped_dek_b64)
            .map_err(|_| TraceDbError::Crypto("invalid wrapped database key base64".to_string()))?;
        let nonce = BASE64_STANDARD
            .decode(&metadata.dek_nonce_b64)
            .map_err(|_| TraceDbError::Crypto("invalid database key nonce base64".to_string()))?;
        if nonce.len() != XCHACHA20POLY1305_NONCE_LEN {
            return Err(TraceDbError::Crypto(format!(
                "invalid database key nonce length {}, expected {XCHACHA20POLY1305_NONCE_LEN}",
                nonce.len()
            )));
        }
        let cipher = XChaCha20Poly1305::new_from_slice(&master_key.bytes)
            .map_err(|error| TraceDbError::Crypto(error.to_string()))?;
        let aad = dek_wrap_aad(database_id, master_key.key_id());
        let dek = cipher
            .decrypt(
                nonce.as_slice().into(),
                Payload {
                    msg: &wrapped,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| {
                TraceDbError::Crypto(format!(
                    "failed to unwrap database encryption key for key_id {}",
                    metadata.key_id
                ))
            })?;
        let dek: [u8; 32] = dek.try_into().map_err(|decoded: Vec<u8>| {
            TraceDbError::Crypto(format!(
                "invalid unwrapped database key length {}, expected 32",
                decoded.len()
            ))
        })?;
        Ok(Self {
            key_id: metadata.key_id.clone(),
            dek,
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn encrypt_artifact(&self, artifact_kind: &str, plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = XChaCha20Poly1305::new_from_slice(&self.dek)
            .map_err(|error| TraceDbError::Crypto(error.to_string()))?;
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aad = artifact_aad(artifact_kind, &self.key_id);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|error| TraceDbError::Crypto(error.to_string()))?;
        let mut body =
            Vec::with_capacity(ENCRYPTED_ARTIFACT_MAGIC.len() + nonce.len() + ciphertext.len());
        body.extend_from_slice(ENCRYPTED_ARTIFACT_MAGIC);
        body.extend_from_slice(&nonce);
        body.extend_from_slice(&ciphertext);
        Ok(body)
    }

    pub fn decrypt_artifact(&self, artifact_kind: &str, body: &[u8]) -> Result<Vec<u8>> {
        if !is_encrypted_artifact(body) {
            return Ok(body.to_vec());
        }
        let nonce_start = ENCRYPTED_ARTIFACT_MAGIC.len();
        let nonce_end = nonce_start + XCHACHA20POLY1305_NONCE_LEN;
        if body.len() <= nonce_end {
            return Err(TraceDbError::Crypto(format!(
                "encrypted {artifact_kind} artifact is missing ciphertext"
            )));
        }
        let nonce = &body[nonce_start..nonce_end];
        let ciphertext = &body[nonce_end..];
        let cipher = XChaCha20Poly1305::new_from_slice(&self.dek)
            .map_err(|error| TraceDbError::Crypto(error.to_string()))?;
        let aad = artifact_aad(artifact_kind, &self.key_id);
        cipher
            .decrypt(
                nonce.into(),
                Payload {
                    msg: ciphertext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| {
                TraceDbError::Crypto(format!(
                    "failed to decrypt {artifact_kind} artifact with key_id {}",
                    self.key_id
                ))
            })
    }
}

pub fn is_encrypted_artifact(body: &[u8]) -> bool {
    body.starts_with(ENCRYPTED_ARTIFACT_MAGIC)
}

pub fn decrypt_artifact_if_needed(
    context: Option<&EncryptionContext>,
    artifact_kind: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    if !is_encrypted_artifact(body) {
        return Ok(body.to_vec());
    }
    let context = context.ok_or_else(|| {
        TraceDbError::Crypto(
            "TRACEDB_MASTER_KEY_B64 is required to open encrypted TraceDB data".to_string(),
        )
    })?;
    context.decrypt_artifact(artifact_kind, body)
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyReceipt {
    pub method: String,
    pub path: String,
    pub key: String,
    pub body_hash: String,
    pub actor_tenant_id: String,
    pub database_id: String,
    pub branch_id: String,
    pub token_identity: String,
    pub response: String,
}

pub fn stable_body_hash(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{}", hex_bytes(&digest))
}

pub fn recover_stale_pid_lock(path: &Path, lock_label: &str) -> Result<bool> {
    let body = fs::read_to_string(path)?;
    let owner_pid = body.trim().parse::<u32>().map_err(|_| {
        TraceDbError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "{lock_label} lock {} has an invalid owner; manual operator recovery is required",
                path.display()
            ),
        ))
    })?;
    if owner_pid == std::process::id() || pid_is_running(owner_pid) {
        return Ok(false);
    }
    fs::remove_file(path)?;
    Ok(true)
}

fn dek_wrap_aad(database_id: &str, key_id: &str) -> String {
    format!("tracedb:dek-wrap:v1:{database_id}:{key_id}")
}

fn artifact_aad(artifact_kind: &str, key_id: &str) -> String {
    format!("tracedb:artifact:v1:{artifact_kind}:{key_id}")
}

fn hex_prefix(bytes: &[u8], len: usize) -> String {
    hex_bytes(&bytes[..bytes.len().min(len)])
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn pid_is_running(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(true)
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

#[cfg(test)]
mod crypto_tests {
    use super::*;

    #[test]
    fn master_key_base64_validation_is_strict() {
        let key = MasterKey::from_base64(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "TRACEDB_MASTER_KEY_B64",
        )
        .expect("valid 32-byte key");
        assert_eq!(key.key_id().len(), "tracedb-root:".len() + 24);

        let invalid_base64 = MasterKey::from_base64("not base64", "TRACEDB_MASTER_KEY_B64")
            .expect_err("invalid base64 rejected");
        assert!(
            invalid_base64
                .to_string()
                .contains("TRACEDB_MASTER_KEY_B64 must be strict base64"),
            "unexpected invalid-base64 error: {invalid_base64}"
        );

        let wrong_len = MasterKey::from_base64(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
            "TRACEDB_MASTER_KEY_B64",
        )
        .expect_err("31-byte key rejected");
        assert!(
            wrong_len
                .to_string()
                .contains("TRACEDB_MASTER_KEY_B64 must decode to exactly 32 bytes"),
            "unexpected wrong-length error: {wrong_len}"
        );
    }
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

#[cfg(test)]
mod tests {
    use super::{TableSchema, VectorColumnSchema};

    fn schema() -> TableSchema {
        TableSchema {
            name: "docs".to_string(),
            primary_id_column: "id".to_string(),
            tenant_id_column: "tenant".to_string(),
            scalar_columns: vec!["status".to_string()],
            text_indexed_columns: vec!["body".to_string()],
            vector_columns: vec![VectorColumnSchema {
                name: "embedding".to_string(),
                dimensions: 3,
                source_columns: vec!["body".to_string()],
            }],
        }
    }

    fn validation_error(schema: TableSchema) -> String {
        schema.validate().unwrap_err().to_string()
    }

    #[test]
    fn schema_validation_rejects_duplicate_and_overlapping_column_names() {
        let mut duplicate_scalar = schema();
        duplicate_scalar.scalar_columns = vec!["status".to_string(), "status".to_string()];
        assert!(validation_error(duplicate_scalar).contains("duplicate scalar column status"));

        let mut overlapping_text = schema();
        overlapping_text.text_indexed_columns = vec!["status".to_string()];
        assert!(validation_error(overlapping_text)
            .contains("column status cannot be both scalar and text indexed"));

        let mut overlapping_vector = schema();
        overlapping_vector.vector_columns[0].name = "body".to_string();
        assert!(validation_error(overlapping_vector)
            .contains("column body cannot be both text indexed and vector"));

        let mut same_identity_columns = schema();
        same_identity_columns.tenant_id_column = "id".to_string();
        assert!(validation_error(same_identity_columns)
            .contains("primary id column and tenant id column must be distinct"));
    }

    #[test]
    fn schema_validation_rejects_reserved_or_graphql_invalid_identifiers() {
        let mut invalid_table = schema();
        invalid_table.name = "bad-name".to_string();
        assert!(validation_error(invalid_table)
            .contains("table name bad-name is not a GraphQL-safe identifier"));

        let mut private_graphql_name = schema();
        private_graphql_name.scalar_columns = vec!["__typename".to_string()];
        assert!(validation_error(private_graphql_name)
            .contains("column __typename is reserved for GraphQL introspection"));

        let mut reserved_metadata = schema();
        reserved_metadata.scalar_columns = vec!["score".to_string()];
        assert!(validation_error(reserved_metadata)
            .contains("column score is reserved for TraceDB result metadata"));
    }

    #[test]
    fn schema_validation_rejects_inconsistent_vector_definitions() {
        let mut empty_vector_name = schema();
        empty_vector_name.vector_columns[0].name = " ".to_string();
        assert!(validation_error(empty_vector_name).contains("vector column name cannot be empty"));

        let mut empty_vector_source = schema();
        empty_vector_source.vector_columns[0].source_columns = Vec::new();
        assert!(validation_error(empty_vector_source)
            .contains("vector column embedding must declare source columns"));

        let mut unknown_vector_source = schema();
        unknown_vector_source.vector_columns[0].source_columns = vec!["missing".to_string()];
        assert!(validation_error(unknown_vector_source)
            .contains("vector column embedding source column missing is not declared"));
    }
}
