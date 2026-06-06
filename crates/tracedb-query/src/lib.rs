#![forbid(unsafe_code)]
//! Query engine and storage operations for TraceDB.

use base64::{
    engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD},
    Engine as _,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::Sha256;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracedb_core::{
    builtin_module_manifests, checksum_bytes, compute_manifest_checksum, database_id_from_path,
    decrypt_artifact_if_needed, stable_body_hash, value_as_f32_vec, DerivedFeatureState,
    EncryptionContext, Epoch, FeatureStatus, IdempotencyReceipt, IndexManifest, IndexState,
    MasterKey, ModuleCommitEvent, Result, TraceDbError, TraceDbManifest,
};
use tracedb_jobs::{JobCatalog, JobEvent, JobKind, TraceJob, WorkerId};
use tracedb_log::{CommitRecord, TornWalTail, Wal, WalAppendTiming};
use tracedb_modules::{ModuleRegistry, RegisteredModule};
use tracedb_planner::{
    plan_trace_query, AccessPath, AccessPathDescriptor as PlannerAccessPathDescriptor,
    AccessPathExplain, AccessPathTiming, Candidate, CandidateBatch, CandidateBudget, CostEstimate,
    ExplainOutput, FeatureFreshness, PlannerFeedback, Predicates, QueryFragment, QueryOutput,
    QueryPhaseTiming, QueryRow, ScoreComponents, Stats, TraceQuery, WorkBudget,
};
use tracedb_policy::{ActorContext, Policy, VisibilityOracle};
use tracedb_segment::SegmentRecord;
use tracedb_store::{
    ReadSnapshot, RecordStore, RecordStoreDelta, ReplacementApplyTiming, StoredRecord,
};

const CHECKPOINT_MAGIC_V2: &[u8; 8] = b"TDBCHK01";
const CHECKPOINT_MAGIC_V3: &[u8; 8] = b"TDBCHK03";
const CHECKPOINT_FORMAT_VERSION: u32 = 4;
const CHECKPOINT_LEGACY_COMPACT_FORMAT_VERSION: u32 = 2;
const CHECKPOINT_LEGACY_JSON_FORMAT_VERSION: u32 = 1;
const MIN_LEXICAL_CACHE_DOCUMENTS: usize = 2_048;
const DEFAULT_LEXICAL_CACHE_CAPACITY: usize = 64;
const POLICY_FIELD_NAME: &str = "__tracedb_policy";
const CURSOR_TOKEN_PREFIX: &str = "tdbc1";
const CURSOR_VERSION: u32 = 1;
const DEFAULT_CURSOR_TTL_SECS: u64 = 60 * 60;
type HmacSha256 = Hmac<Sha256>;

pub use tracedb_core::{
    FeatureInvalidation, ModuleManifest, RecordDeletion, RecordInput, TableSchema,
    VectorColumnSchema,
};
pub use tracedb_planner::{
    ExplainOutput as HybridExplain, QueryOutput as HybridQueryOutput, QueryRow as HybridQueryRow,
    ScoreComponents as HybridScoreComponents,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FreshnessMode {
    Strict,
    Lazy,
    AllowDirty,
}

impl FreshnessMode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Strict => "STRICT",
            Self::Lazy => "LAZY",
            Self::AllowDirty => "ALLOW_DIRTY",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HybridQuery {
    pub table: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub text_field: Option<String>,
    pub text: Option<String>,
    #[serde(default)]
    pub vector_field: Option<String>,
    pub vector: Option<Vec<f32>>,
    #[serde(default)]
    pub scalar_eq: Map<String, Value>,
    #[serde(default)]
    pub graph_seed: Option<String>,
    #[serde(default)]
    pub temporal_as_of: Option<u64>,
    pub top_k: usize,
    pub freshness: FreshnessMode,
    pub explain: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryExecutionTiming {
    pub total_ms: f64,
    pub engine_core_ms: f64,
    pub explain_build_ms: f64,
    pub materialize_ms: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TimedQueryOutput {
    pub output: QueryOutput,
    pub timing: QueryExecutionTiming,
}

#[derive(Clone, Debug, Default)]
pub struct TraceDbOpenOptions {
    master_key: Option<MasterKey>,
    encryption_context: Option<EncryptionContext>,
    use_env_master_key: bool,
}

impl TraceDbOpenOptions {
    pub fn from_env() -> Self {
        Self {
            master_key: None,
            encryption_context: None,
            use_env_master_key: true,
        }
    }

    pub fn with_master_key_b64(value: &str) -> Self {
        Self {
            master_key: Some(
                MasterKey::from_base64(value, "TRACEDB_MASTER_KEY_B64")
                    .expect("valid TraceDB master key base64"),
            ),
            encryption_context: None,
            use_env_master_key: false,
        }
    }

    pub fn without_tde() -> Self {
        Self {
            master_key: None,
            encryption_context: None,
            use_env_master_key: false,
        }
    }

    fn with_existing_encryption_context(encryption_context: Option<EncryptionContext>) -> Self {
        Self {
            master_key: None,
            encryption_context,
            use_env_master_key: false,
        }
    }

    fn master_key(&self) -> Result<Option<MasterKey>> {
        if let Some(key) = &self.master_key {
            return Ok(Some(key.clone()));
        }
        if self.use_env_master_key {
            MasterKey::from_env()
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordPutRequest {
    pub record: RecordInput,
}

impl RecordPutRequest {
    pub fn new(record: RecordInput) -> Self {
        Self { record }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordPutBatchRequest {
    #[serde(default)]
    pub include_write_timing: bool,
    pub records: Vec<RecordInput>,
}

impl RecordPutBatchRequest {
    pub fn new(records: Vec<RecordInput>) -> Self {
        Self {
            include_write_timing: false,
            records,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordPatchRequest {
    pub table: String,
    pub tenant_id: String,
    pub id: String,
    pub fields: Map<String, Value>,
}

impl RecordPatchRequest {
    pub fn new(
        table: impl Into<String>,
        tenant_id: impl Into<String>,
        id: impl Into<String>,
        fields: Map<String, Value>,
    ) -> Self {
        Self {
            table: table.into(),
            tenant_id: tenant_id.into(),
            id: id.into(),
            fields,
        }
    }

    fn into_record_input(self) -> RecordInput {
        RecordInput {
            table: self.table,
            tenant_id: self.tenant_id,
            id: self.id,
            fields: self.fields,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordDeleteRequest {
    pub table: String,
    pub tenant_id: String,
    pub id: String,
    #[serde(default = "default_tombstone")]
    pub tombstone: String,
}

impl RecordDeleteRequest {
    pub fn new(
        table: impl Into<String>,
        tenant_id: impl Into<String>,
        id: impl Into<String>,
    ) -> Self {
        Self {
            table: table.into(),
            tenant_id: tenant_id.into(),
            id: id.into(),
            tombstone: default_tombstone(),
        }
    }

    pub fn tombstone(mut self, tombstone: impl Into<String>) -> Self {
        self.tombstone = tombstone.into();
        self
    }

    fn into_deletion(self) -> RecordDeletion {
        RecordDeletion::new(self.table, self.tenant_id, self.id, self.tombstone)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordGetRequest {
    pub table: String,
    pub tenant_id: String,
    pub id: String,
}

impl RecordGetRequest {
    pub fn new(
        table: impl Into<String>,
        tenant_id: impl Into<String>,
        id: impl Into<String>,
    ) -> Self {
        Self {
            table: table.into(),
            tenant_id: tenant_id.into(),
            id: id.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecordScanRequest {
    pub table: String,
    pub tenant_id: String,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl RecordScanRequest {
    pub fn new(table: impl Into<String>, tenant_id: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            tenant_id: tenant_id.into(),
            limit: 100,
            cursor: None,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordOutput {
    pub table: String,
    pub id: String,
    pub tenant_id: String,
    pub version_id: u64,
    pub fields: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordScanOutput {
    pub records: Vec<RecordOutput>,
    pub returned_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WritePathTiming {
    pub total_ms: f64,
    pub lock_ms: f64,
    pub refresh_total_ms: f64,
    pub refresh_manifest_read_ms: f64,
    pub refresh_wal_tail_ms: f64,
    pub refresh_reopen_ms: f64,
    pub refresh_performed: bool,
    pub schema_lookup_ms: f64,
    pub store_clone_ms: f64,
    #[serde(default)]
    pub store_delta_plan_ms: f64,
    #[serde(default)]
    pub store_delta_apply_ms: f64,
    pub store_apply_ms: f64,
    #[serde(default)]
    pub store_apply_validate_identity_ms: f64,
    #[serde(default)]
    pub store_apply_validate_vector_ms: f64,
    #[serde(default)]
    pub store_apply_key_ms: f64,
    #[serde(default)]
    pub store_apply_fields_ms: f64,
    #[serde(default)]
    pub store_apply_finalize_identity_ms: f64,
    #[serde(default)]
    pub store_apply_features_ms: f64,
    #[serde(default)]
    pub store_apply_install_ms: f64,
    pub feature_invalidation_ms: f64,
    pub commit_build_ms: f64,
    pub wal_total_ms: f64,
    pub wal_lock_tail_ms: f64,
    pub wal_frame_build_ms: f64,
    pub wal_commit_prepare_ms: f64,
    pub wal_serialize_ms: f64,
    pub wal_payload_checksum_ms: f64,
    pub wal_frame_assembly_ms: f64,
    pub wal_payload_bytes: u64,
    pub wal_frame_bytes: u64,
    pub wal_write_ms: f64,
    pub wal_sync_data_ms: f64,
    pub wal_tail_update_ms: f64,
    pub store_install_ms: f64,
    pub manifest_total_ms: f64,
    pub manifest_clone_ms: f64,
    pub manifest_write_total_ms: f64,
    pub manifest_bytes: u64,
    pub manifest_checksum_ms: f64,
    pub manifest_serialize_ms: f64,
    pub manifest_write_ms: f64,
    pub manifest_sync_file_ms: f64,
    pub manifest_rename_ms: f64,
    pub manifest_sync_dir_ms: f64,
    pub cache_clear_ms: f64,
}

fn default_tombstone() -> String {
    "user_delete".to_string()
}

#[derive(Clone, Debug)]
struct CursorScope {
    kind: &'static str,
    tenant_id: String,
    database_id: String,
    branch_id: String,
    actor_digest: String,
    query_digest: String,
    limit: usize,
    order_key: String,
}

#[derive(Clone, Debug)]
struct CursorState {
    offset: usize,
    snapshot_epoch: Epoch,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SignedCursorPayload {
    version: u32,
    kind: String,
    tenant_id: String,
    database_id: String,
    branch_id: String,
    actor_digest: String,
    query_digest: String,
    snapshot_epoch: u64,
    limit: usize,
    order_key: String,
    offset: usize,
    expires_at: u64,
}

fn parse_legacy_cursor_offset(cursor: Option<&str>) -> Result<usize> {
    match cursor {
        Some(value) if value.trim().is_empty() => Err(TraceDbError::InvalidCommand(
            "cursor must be a non-empty offset".to_string(),
        )),
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| TraceDbError::InvalidCommand(format!("invalid cursor offset: {value}"))),
        None => Ok(0),
    }
}

fn cursor_scope_for_scan(
    actor: &ActorContext,
    request: &RecordScanRequest,
    limit: usize,
) -> CursorScope {
    CursorScope {
        kind: "scan",
        tenant_id: request.tenant_id.clone(),
        database_id: actor.database_id.clone(),
        branch_id: actor.branch_id.clone(),
        actor_digest: cursor_actor_digest(actor),
        query_digest: stable_body_hash(
            serde_json::json!({
                "kind": "scan",
                "table": &request.table,
                "tenant_id": &request.tenant_id,
                "order": "record_id_asc",
            })
            .to_string()
            .as_bytes(),
        ),
        limit,
        order_key: "record_id_asc".to_string(),
    }
}

fn cursor_scope_for_query(actor: &ActorContext, query: &HybridQuery, limit: usize) -> CursorScope {
    let mut digest_query = query.clone();
    digest_query.cursor = None;
    CursorScope {
        kind: "query",
        tenant_id: query.tenant_id.clone(),
        database_id: actor.database_id.clone(),
        branch_id: actor.branch_id.clone(),
        actor_digest: cursor_actor_digest(actor),
        query_digest: stable_body_hash(
            serde_json::to_string(&digest_query)
                .unwrap_or_else(|_| "{}".to_string())
                .as_bytes(),
        ),
        limit,
        order_key: "ranked".to_string(),
    }
}

fn cursor_actor_digest(actor: &ActorContext) -> String {
    stable_body_hash(
        serde_json::json!({
            "tenant_id": &actor.tenant_id,
            "database_id": &actor.database_id,
            "branch_id": &actor.branch_id,
            "token_identity": &actor.token_identity,
            "policy_epoch": actor.policy_epoch,
            "scopes": &actor.scopes,
        })
        .to_string()
        .as_bytes(),
    )
}

fn resolve_cursor_state(
    scope: &CursorScope,
    cursor: Option<&str>,
    latest_epoch: Epoch,
) -> Result<CursorState> {
    let Some(cursor) = cursor else {
        return Ok(CursorState {
            offset: 0,
            snapshot_epoch: latest_epoch,
        });
    };
    if cursor.starts_with(CURSOR_TOKEN_PREFIX) {
        let payload = decode_signed_cursor(scope, cursor)?;
        return Ok(CursorState {
            offset: payload.offset,
            snapshot_epoch: Epoch::new(payload.snapshot_epoch),
        });
    }
    if cursor_signing_key()?.is_some() {
        return Err(TraceDbError::InvalidCommand(
            "invalid cursor: signed cursor token is required".to_string(),
        ));
    }
    Ok(CursorState {
        offset: parse_legacy_cursor_offset(Some(cursor))?,
        snapshot_epoch: latest_epoch,
    })
}

fn encode_next_cursor(scope: &CursorScope, offset: usize, snapshot_epoch: Epoch) -> Result<String> {
    if cursor_signing_key()?.is_none() {
        return Ok(offset.to_string());
    }
    let payload = SignedCursorPayload {
        version: CURSOR_VERSION,
        kind: scope.kind.to_string(),
        tenant_id: scope.tenant_id.clone(),
        database_id: scope.database_id.clone(),
        branch_id: scope.branch_id.clone(),
        actor_digest: scope.actor_digest.clone(),
        query_digest: scope.query_digest.clone(),
        snapshot_epoch: snapshot_epoch.get(),
        limit: scope.limit,
        order_key: scope.order_key.clone(),
        offset,
        expires_at: now_unix_secs().saturating_add(cursor_ttl_secs()),
    };
    encode_signed_cursor(&payload)
}

fn encode_signed_cursor(payload: &SignedCursorPayload) -> Result<String> {
    let key = cursor_signing_key()?.ok_or_else(|| {
        TraceDbError::InvalidCommand("TRACEDB_CURSOR_SIGNING_KEY_B64 is required".to_string())
    })?;
    let payload_json = serde_json::to_vec(payload)?;
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload_json);
    let mut mac = HmacSha256::new_from_slice(&key)
        .map_err(|_| TraceDbError::InvalidCommand("invalid cursor signing key".to_string()))?;
    mac.update(payload_b64.as_bytes());
    let signature = mac.finalize().into_bytes();
    Ok(format!(
        "{CURSOR_TOKEN_PREFIX}.{payload_b64}.{}",
        URL_SAFE_NO_PAD.encode(signature)
    ))
}

fn decode_signed_cursor(scope: &CursorScope, token: &str) -> Result<SignedCursorPayload> {
    let key = cursor_signing_key()?.ok_or_else(|| {
        TraceDbError::InvalidCommand(
            "TRACEDB_CURSOR_SIGNING_KEY_B64 is required to verify cursor".to_string(),
        )
    })?;
    let mut parts = token.split('.');
    if parts.next() != Some(CURSOR_TOKEN_PREFIX) {
        return Err(invalid_cursor("malformed token"));
    }
    let Some(payload_b64) = parts.next() else {
        return Err(invalid_cursor("malformed token"));
    };
    let Some(signature_b64) = parts.next() else {
        return Err(invalid_cursor("malformed token"));
    };
    if parts.next().is_some() {
        return Err(invalid_cursor("malformed token"));
    }
    let signature = URL_SAFE_NO_PAD
        .decode(signature_b64)
        .map_err(|_| invalid_cursor("malformed signature"))?;
    let mut mac = HmacSha256::new_from_slice(&key)
        .map_err(|_| TraceDbError::InvalidCommand("invalid cursor signing key".to_string()))?;
    mac.update(payload_b64.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| invalid_cursor("signature mismatch"))?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| invalid_cursor("malformed payload"))?;
    let payload: SignedCursorPayload = serde_json::from_slice(&payload_bytes)?;
    validate_signed_cursor_payload(scope, &payload)?;
    Ok(payload)
}

fn validate_signed_cursor_payload(
    scope: &CursorScope,
    payload: &SignedCursorPayload,
) -> Result<()> {
    if payload.version != CURSOR_VERSION {
        return Err(invalid_cursor("unsupported version"));
    }
    if payload.expires_at < now_unix_secs() {
        return Err(invalid_cursor("expired"));
    }
    if payload.kind != scope.kind {
        return Err(invalid_cursor("kind mismatch"));
    }
    if payload.tenant_id != scope.tenant_id {
        return Err(invalid_cursor("tenant mismatch"));
    }
    if payload.database_id != scope.database_id {
        return Err(invalid_cursor("database mismatch"));
    }
    if payload.branch_id != scope.branch_id {
        return Err(invalid_cursor("branch mismatch"));
    }
    if payload.actor_digest != scope.actor_digest {
        return Err(invalid_cursor("actor mismatch"));
    }
    if payload.query_digest != scope.query_digest {
        return Err(invalid_cursor("query mismatch"));
    }
    if payload.limit != scope.limit {
        return Err(invalid_cursor("limit mismatch"));
    }
    if payload.order_key != scope.order_key {
        return Err(invalid_cursor("ordering mismatch"));
    }
    Ok(())
}

fn cursor_signing_key() -> Result<Option<[u8; 32]>> {
    match env::var("TRACEDB_CURSOR_SIGNING_KEY_B64") {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => {
            if value.chars().any(char::is_whitespace) {
                return Err(TraceDbError::InvalidCommand(
                    "TRACEDB_CURSOR_SIGNING_KEY_B64 must be strict base64 without whitespace"
                        .to_string(),
                ));
            }
            let decoded = BASE64_STANDARD.decode(value).map_err(|_| {
                TraceDbError::InvalidCommand(
                    "TRACEDB_CURSOR_SIGNING_KEY_B64 must be strict base64".to_string(),
                )
            })?;
            decoded.try_into().map(Some).map_err(|decoded: Vec<u8>| {
                TraceDbError::InvalidCommand(format!(
                    "TRACEDB_CURSOR_SIGNING_KEY_B64 must decode to exactly 32 bytes, got {}",
                    decoded.len()
                ))
            })
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(TraceDbError::InvalidCommand(format!(
            "failed to read TRACEDB_CURSOR_SIGNING_KEY_B64: {error}"
        ))),
    }
}

fn cursor_ttl_secs() -> u64 {
    env::var("TRACEDB_CURSOR_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CURSOR_TTL_SECS)
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn invalid_cursor(reason: impl Into<String>) -> TraceDbError {
    TraceDbError::InvalidCommand(format!("invalid cursor: {}", reason.into()))
}

fn validate_actor_tenant(actor: &ActorContext, tenant_id: &str) -> Result<()> {
    if actor.tenant_id == tenant_id {
        return Ok(());
    }
    Err(TraceDbError::InvalidCommand(format!(
        "actor tenant {} cannot query tenant {}",
        actor.tenant_id, tenant_id
    )))
}

pub trait BackupRestore {
    fn backup(&self, target: impl AsRef<Path>) -> Result<()>;
    fn restore(source: impl AsRef<Path>, target: impl AsRef<Path>) -> Result<TraceDb>;
}

#[derive(Clone, Debug)]
/// Main TraceDB engine handle.
pub struct TraceDb {
    dir: PathBuf,
    manifest: TraceDbManifest,
    store: RecordStore,
    wal: Wal,
    encryption: Option<EncryptionContext>,
    idempotency_receipts: Vec<IdempotencyReceipt>,
    job_catalog: JobCatalog,
    last_recovery_torn_tail: Option<TornWalTail>,
    lexical_cache: Arc<Mutex<LexicalCorpusCache>>,
    _engine_lock: EngineLock,
}

#[derive(Clone, Debug)]
struct LexicalCorpusCache {
    entries: BTreeMap<LexicalCacheKey, LexicalCacheEntry>,
    capacity: usize,
    tick: u64,
}

impl Default for LexicalCorpusCache {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity: DEFAULT_LEXICAL_CACHE_CAPACITY,
            tick: 0,
        }
    }
}

impl LexicalCorpusCache {
    fn get(&mut self, key: &LexicalCacheKey) -> Option<tracedb_text::PreparedTextCorpus> {
        let next_tick = self.next_tick();
        let entry = self.entries.get_mut(key)?;
        entry.last_used = next_tick;
        Some(entry.corpus.clone())
    }

    fn insert(&mut self, key: LexicalCacheKey, corpus: tracedb_text::PreparedTextCorpus) {
        let next_tick = self.next_tick();
        self.entries.insert(
            key,
            LexicalCacheEntry {
                corpus,
                last_used: next_tick,
            },
        );
        self.evict_lru();
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.tick = 0;
    }

    fn next_tick(&mut self) -> u64 {
        self.tick = self.tick.saturating_add(1);
        self.tick
    }

    fn evict_lru(&mut self) {
        while self.entries.len() > self.capacity {
            let Some(key) = self
                .entries
                .iter()
                .min_by(|left, right| {
                    left.1
                        .last_used
                        .cmp(&right.1.last_used)
                        .then_with(|| left.0.cmp(right.0))
                })
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.entries.remove(&key);
        }
    }
}

#[derive(Clone, Debug)]
struct LexicalCacheEntry {
    corpus: tracedb_text::PreparedTextCorpus,
    last_used: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct LexicalCacheKey {
    table: String,
    tenant_id: String,
    read_epoch: u64,
    manifest_generation: u64,
    scalar_eq: String,
    text_columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointFile {
    format_version: u32,
    epoch: Epoch,
    schemas: Vec<TableSchema>,
    records: Vec<StoredRecord>,
    #[serde(default)]
    idempotency_receipts: Vec<IdempotencyReceipt>,
    #[serde(default)]
    job_catalog: JobCatalog,
    checksum: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointPayload {
    format_version: u32,
    epoch: Epoch,
    schemas: Vec<TableSchema>,
    records: Vec<StoredRecord>,
    #[serde(default)]
    idempotency_receipts: Vec<IdempotencyReceipt>,
    #[serde(default)]
    job_catalog: JobCatalog,
}

#[derive(Clone, Debug, Default)]
struct ManifestWriteTiming {
    total_ms: f64,
    bytes: u64,
    checksum_ms: f64,
    serialize_ms: f64,
    write_ms: f64,
    sync_file_ms: f64,
    rename_ms: f64,
    sync_dir_ms: f64,
}

#[derive(Clone, Debug, Default)]
struct ManifestBumpTiming {
    total_ms: f64,
    clone_ms: f64,
    write: ManifestWriteTiming,
}

#[derive(Clone, Debug, Default)]
struct RefreshTiming {
    total_ms: f64,
    manifest_read_ms: f64,
    wal_tail_ms: f64,
    reopen_ms: f64,
    performed: bool,
}

impl WritePathTiming {
    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        total_ms: f64,
        lock_ms: f64,
        refresh: RefreshTiming,
        schema_lookup_ms: f64,
        store_clone_ms: f64,
        store_delta_plan_ms: f64,
        store_delta_apply_ms: f64,
        store_apply_ms: f64,
        store_apply_timing: ReplacementApplyTiming,
        feature_invalidation_ms: f64,
        commit_build_ms: f64,
        wal: WalAppendTiming,
        store_install_ms: f64,
        manifest: ManifestBumpTiming,
        cache_clear_ms: f64,
    ) -> Self {
        Self {
            total_ms,
            lock_ms,
            refresh_total_ms: refresh.total_ms,
            refresh_manifest_read_ms: refresh.manifest_read_ms,
            refresh_wal_tail_ms: refresh.wal_tail_ms,
            refresh_reopen_ms: refresh.reopen_ms,
            refresh_performed: refresh.performed,
            schema_lookup_ms,
            store_clone_ms,
            store_delta_plan_ms,
            store_delta_apply_ms,
            store_apply_ms,
            store_apply_validate_identity_ms: store_apply_timing.validate_identity_ms,
            store_apply_validate_vector_ms: store_apply_timing.validate_vector_ms,
            store_apply_key_ms: store_apply_timing.key_ms,
            store_apply_fields_ms: store_apply_timing.fields_ms,
            store_apply_finalize_identity_ms: store_apply_timing.finalize_identity_ms,
            store_apply_features_ms: store_apply_timing.features_ms,
            store_apply_install_ms: store_apply_timing.install_ms,
            feature_invalidation_ms,
            commit_build_ms,
            wal_total_ms: wal.total_ms,
            wal_lock_tail_ms: wal.lock_tail_ms,
            wal_frame_build_ms: wal.frame_build_ms,
            wal_commit_prepare_ms: wal.commit_prepare_ms,
            wal_serialize_ms: wal.serialize_ms,
            wal_payload_checksum_ms: wal.payload_checksum_ms,
            wal_frame_assembly_ms: wal.frame_assembly_ms,
            wal_payload_bytes: wal.payload_bytes,
            wal_frame_bytes: wal.frame_bytes,
            wal_write_ms: wal.write_ms,
            wal_sync_data_ms: wal.sync_data_ms,
            wal_tail_update_ms: wal.tail_update_ms,
            store_install_ms,
            manifest_total_ms: manifest.total_ms,
            manifest_clone_ms: manifest.clone_ms,
            manifest_write_total_ms: manifest.write.total_ms,
            manifest_bytes: manifest.write.bytes,
            manifest_checksum_ms: manifest.write.checksum_ms,
            manifest_serialize_ms: manifest.write.serialize_ms,
            manifest_write_ms: manifest.write.write_ms,
            manifest_sync_file_ms: manifest.write.sync_file_ms,
            manifest_rename_ms: manifest.write.rename_ms,
            manifest_sync_dir_ms: manifest.write.sync_dir_ms,
            cache_clear_ms,
        }
    }
}

impl TraceDb {
    /// Open a TraceDB at the given directory.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(dir, TraceDbOpenOptions::from_env())
    }

    /// Open a TraceDB with explicit options.
    pub fn open_with_options(dir: impl AsRef<Path>, options: TraceDbOpenOptions) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        initialize_layout(&dir)?;
        let _engine_lock = EngineLock::acquire(&dir)?;
        let manifest_path = dir.join("manifest.tdb");
        let master_key = options.master_key()?;
        if !manifest_path.exists() && !manifest_path.with_extension("tdb.bak").exists() {
            let mut manifest = TraceDbManifest::empty(database_id_from_path(&dir));
            if let Some(master_key) = master_key.as_ref() {
                let (_, metadata) = EncryptionContext::create(master_key, &manifest.database_id)?;
                manifest.encryption = Some(metadata);
            }
            write_manifest(&manifest_path, &mut manifest)?;
        }

        let mut manifest = read_manifest(&manifest_path)?;
        if manifest.modules.is_empty() {
            manifest.modules = builtin_module_manifests();
        }
        let mut encryption = match manifest.encryption.as_ref() {
            Some(metadata) => {
                if let Some(context) = options.encryption_context.as_ref() {
                    if context.key_id() == metadata.key_id {
                        Some(context.clone())
                    } else {
                        return Err(TraceDbError::Crypto(format!(
                            "failed to unwrap database encryption key for key_id {}",
                            metadata.key_id
                        )));
                    }
                } else {
                    let master_key = master_key.as_ref().ok_or_else(|| {
                        TraceDbError::Crypto(
                            "TRACEDB_MASTER_KEY_B64 is required to open encrypted TraceDB data"
                                .to_string(),
                        )
                    })?;
                    Some(EncryptionContext::from_metadata(
                        master_key,
                        &manifest.database_id,
                        metadata,
                    )?)
                }
            }
            None => None,
        };
        if encryption.is_none() {
            if let Some(master_key) = master_key.as_ref() {
                let (context, metadata) =
                    EncryptionContext::create(master_key, &manifest.database_id)?;
                manifest.encryption = Some(metadata);
                write_manifest(&manifest_path, &mut manifest)?;
                encryption = Some(context);
            }
        }
        let wal = Wal::open_with_encryption(&dir, encryption.clone())?;
        let wal_scan = wal.scan_with_metadata()?;
        let entries = wal_scan.entries;
        let all_commits = entries
            .iter()
            .map(|entry| entry.commit.clone())
            .collect::<Vec<_>>();
        if manifest.checkpoint_epoch > manifest.latest_epoch {
            return Err(TraceDbError::ManifestCorruption(format!(
                "checkpoint epoch {} exceeds latest epoch {}",
                manifest.checkpoint_epoch, manifest.latest_epoch
            )));
        }
        let mut checkpoint_receipts = Vec::new();
        let mut job_catalog = JobCatalog::default();
        let mut store = if manifest.checkpoint_epoch.get() > 0 {
            let checkpoint =
                read_checkpoint_file(&dir, manifest.checkpoint_epoch, encryption.as_ref())?;
            if checkpoint.epoch != manifest.checkpoint_epoch {
                return Err(TraceDbError::ManifestCorruption(format!(
                    "checkpoint epoch mismatch: manifest {}, file {}",
                    manifest.checkpoint_epoch, checkpoint.epoch
                )));
            }
            if manifest.schemas.is_empty() {
                manifest.schemas = checkpoint.schemas.clone();
            }
            checkpoint_receipts = checkpoint.idempotency_receipts.clone();
            job_catalog = checkpoint.job_catalog.clone();
            RecordStore::from_checkpoint_records(checkpoint.records)?
        } else {
            RecordStore::default()
        };
        let commits = all_commits
            .into_iter()
            .filter(|commit| commit.epoch > manifest.checkpoint_epoch)
            .collect::<Vec<_>>();
        for commit in &commits {
            for schema in &commit.schema_changes {
                upsert_schema(&mut manifest.schemas, schema.clone());
            }
        }
        let mut idempotency_receipts = checkpoint_receipts;
        idempotency_receipts.extend(
            commits
                .iter()
                .flat_map(|commit| commit.idempotency_receipts.clone()),
        );
        for event in commits.iter().flat_map(|commit| commit.job_events.clone()) {
            job_catalog
                .apply_event(event)
                .map_err(TraceDbError::InvalidCommand)?;
        }
        store.apply_commits(&manifest.schemas, &commits)?;
        if let Some(last_commit) = commits.last() {
            if last_commit.epoch > manifest.latest_epoch {
                manifest.latest_epoch = last_commit.epoch;
                manifest.durable_epoch = last_commit.epoch;
                manifest.manifest_generation += 1;
                write_manifest(&manifest_path, &mut manifest)?;
            }
        }

        Ok(Self {
            dir,
            manifest,
            store,
            wal,
            encryption,
            idempotency_receipts,
            job_catalog,
            last_recovery_torn_tail: wal_scan.torn_tail,
            lexical_cache: Arc::new(Mutex::new(LexicalCorpusCache::default())),
            _engine_lock,
        })
    }

    pub fn apply_schema(&mut self, schema: TableSchema) -> Result<Epoch> {
        self.apply_schema_with_idempotency_receipt(schema, None)
    }

    pub fn apply_schema_with_idempotency_receipt(
        &mut self,
        schema: TableSchema,
        receipt: Option<IdempotencyReceipt>,
    ) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        schema.validate()?;
        self.validate_schema_compatible(&schema)?;
        let epoch = self.manifest.latest_epoch.next();
        let idempotency_receipts =
            idempotency_receipts_for_response(receipt, serde_json::json!({ "epoch": epoch.get() }));
        let mut commit = CommitRecord::empty(epoch.get(), epoch).for_database(
            self.manifest.database_id.clone(),
            self.manifest.branch_id.clone(),
        );
        commit.idempotency_receipts = idempotency_receipts;
        commit.schema_changes.push(schema.clone());
        commit.module_events.push(ModuleCommitEvent {
            module_id: "tracedb-kernel".to_string(),
            event: "schema.apply".to_string(),
        });
        commit
            .module_events
            .extend(module_events_for_schema("schema.apply", &schema));
        self.wal.append_commit(&commit)?;
        upsert_schema(&mut self.manifest.schemas, schema);
        self.idempotency_receipts
            .extend(commit.idempotency_receipts.clone());
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    /// Insert a record.
    pub fn insert(&mut self, input: RecordInput) -> Result<Epoch> {
        self.insert_with_idempotency_receipt(input, None)
    }

    pub fn insert_with_idempotency_receipt(
        &mut self,
        input: RecordInput,
        receipt: Option<IdempotencyReceipt>,
    ) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let schema = self
            .manifest
            .table(&input.table)
            .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let delta = self.store.plan_mutation(&schema, &input, epoch)?;
        let feature_invalidations = feature_invalidations_for_mutation(&schema, &input);
        let idempotency_receipts =
            idempotency_receipts_for_response(receipt, serde_json::json!({ "epoch": epoch.get() }));
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            mutations: vec![input.clone()],
            feature_invalidations,
            module_events: module_events_for_schema("insert.index", &schema),
            idempotency_receipts,
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store.apply_delta(delta);
        self.idempotency_receipts
            .extend(commit.idempotency_receipts.clone());
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    /// Put (upsert) a single record.
    pub fn put(&mut self, request: RecordPutRequest) -> Result<Epoch> {
        self.put_with_idempotency_receipt(request, None)
    }

    pub fn put_with_idempotency_receipt(
        &mut self,
        request: RecordPutRequest,
        receipt: Option<IdempotencyReceipt>,
    ) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let input = request.record;
        let schema = self
            .manifest
            .table(&input.table)
            .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let delta = self.store.plan_replacement(&schema, &input, epoch)?;
        let feature_invalidations = feature_invalidations_for_mutation(&schema, &input);
        let idempotency_receipts =
            idempotency_receipts_for_response(receipt, serde_json::json!({ "epoch": epoch.get() }));
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: vec![input.clone()],
            mutations: Vec::new(),
            deletions: Vec::new(),
            feature_invalidations,
            module_events: module_events_for_schema("record.put", &schema),
            idempotency_receipts,
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store.apply_delta(delta);
        self.idempotency_receipts
            .extend(commit.idempotency_receipts.clone());
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn idempotency_receipts(&self) -> Result<Vec<IdempotencyReceipt>> {
        Ok(self.idempotency_receipts.clone())
    }

    pub fn record_idempotency_receipt(&mut self, receipt: IdempotencyReceipt) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let epoch = self.manifest.latest_epoch.next();
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: Vec::new(),
            mutations: Vec::new(),
            deletions: Vec::new(),
            feature_invalidations: Vec::new(),
            module_events: vec![ModuleCommitEvent {
                module_id: "tracedb-kernel".to_string(),
                event: "idempotency.receipt".to_string(),
            }],
            idempotency_receipts: vec![receipt],
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.idempotency_receipts
            .extend(commit.idempotency_receipts.clone());
        self.bump_manifest(epoch)?;
        Ok(epoch)
    }

    pub fn jobs(&self) -> Result<Vec<TraceJob>> {
        Ok(self.job_catalog.jobs())
    }

    pub fn enqueue_job(
        &mut self,
        kind: JobKind,
        target: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> Result<TraceJob> {
        let mut planned = self.job_catalog.clone();
        let job = planned
            .enqueue(kind, target, idempotency_key)
            .map_err(TraceDbError::InvalidCommand)?;
        self.append_job_event(JobEvent::enqueued(job.clone()), planned)?;
        Ok(job)
    }

    pub fn lease_job(
        &mut self,
        worker_id: WorkerId,
        kind: JobKind,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<TraceJob>> {
        let mut planned = self.job_catalog.clone();
        let Some(job) = planned
            .lease_next_at(worker_id.clone(), kind, now_ms, lease_ms)
            .map_err(TraceDbError::InvalidCommand)?
        else {
            return Ok(None);
        };
        let event = JobEvent::leased(
            job.job_id.clone(),
            worker_id,
            job.lease_token.clone().unwrap_or_default(),
            job.lease_expires_at_ms
                .unwrap_or(now_ms.saturating_add(lease_ms)),
        );
        self.append_job_event(event, planned)?;
        Ok(Some(job))
    }

    pub fn heartbeat_job(
        &mut self,
        job_id: &str,
        lease_token: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<TraceJob> {
        let mut planned = self.job_catalog.clone();
        let job = planned
            .heartbeat(job_id, lease_token, now_ms, lease_ms)
            .map_err(TraceDbError::InvalidCommand)?;
        let event = JobEvent::Heartbeat {
            job_id: job_id.to_string(),
            lease_token: lease_token.to_string(),
            lease_expires_at_ms: job
                .lease_expires_at_ms
                .unwrap_or(now_ms.saturating_add(lease_ms)),
        };
        self.append_job_event(event, planned)?;
        Ok(job)
    }

    pub fn complete_job(&mut self, job_id: &str, lease_token: &str) -> Result<TraceJob> {
        let mut planned = self.job_catalog.clone();
        let job = planned
            .complete(job_id, Some(lease_token))
            .map_err(TraceDbError::InvalidCommand)?;
        self.append_job_event(
            JobEvent::completed(job_id.to_string(), lease_token.to_string()),
            planned,
        )?;
        Ok(job)
    }

    pub fn fail_job(
        &mut self,
        job_id: &str,
        lease_token: Option<&str>,
        error: impl Into<String>,
        permanent: bool,
        now_ms: u64,
    ) -> Result<TraceJob> {
        let error = error.into();
        let mut planned = self.job_catalog.clone();
        let job = planned
            .fail(job_id, lease_token, error.clone(), permanent, now_ms)
            .map_err(TraceDbError::InvalidCommand)?;
        self.append_job_event(
            JobEvent::Failed {
                job_id: job_id.to_string(),
                lease_token: lease_token.map(str::to_string),
                error,
                permanent,
                next_attempt_after_ms: now_ms,
            },
            planned,
        )?;
        Ok(job)
    }

    fn append_job_event(&mut self, event: JobEvent, planned: JobCatalog) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let epoch = self.manifest.latest_epoch.next();
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: Vec::new(),
            mutations: Vec::new(),
            deletions: Vec::new(),
            feature_invalidations: Vec::new(),
            module_events: vec![ModuleCommitEvent {
                module_id: "tracedb-jobs".to_string(),
                event: "job.event".to_string(),
            }],
            job_events: vec![event],
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.job_catalog = planned;
        self.bump_manifest(epoch)?;
        write_job_catalog_file(&self.dir, &self.job_catalog, self.encryption.as_ref())?;
        Ok(epoch)
    }

    pub fn put_batch(&mut self, request: RecordPutBatchRequest) -> Result<Epoch> {
        self.put_batch_with_idempotency_receipt(request, None)
    }

    pub fn put_batch_with_idempotency_receipt(
        &mut self,
        request: RecordPutBatchRequest,
        receipt: Option<IdempotencyReceipt>,
    ) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        if request.records.is_empty() {
            return Err(TraceDbError::InvalidCommand(
                "batch put requires at least one record".to_string(),
            ));
        }

        let epoch = self.manifest.latest_epoch.next();
        let mut delta = RecordStoreDelta::default();
        let mut feature_invalidations = Vec::new();
        let mut module_events = Vec::new();
        let mut seen_module_events = BTreeSet::new();
        for input in &request.records {
            let schema = self
                .manifest
                .table(&input.table)
                .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
                .clone();
            self.store
                .plan_replacement_into(&mut delta, &schema, input, epoch)?;
            feature_invalidations.extend(feature_invalidations_for_mutation(&schema, input));
            for event in module_events_for_schema("record.put", &schema) {
                if seen_module_events.insert((event.module_id.clone(), event.event.clone())) {
                    module_events.push(event);
                }
            }
        }

        let record_count = request.records.len();
        let idempotency_receipts = idempotency_receipts_for_response(
            receipt,
            serde_json::json!({ "epoch": epoch.get(), "record_count": record_count }),
        );
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: request.records.clone(),
            mutations: Vec::new(),
            deletions: Vec::new(),
            feature_invalidations,
            module_events,
            idempotency_receipts,
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store.apply_delta(delta);
        self.idempotency_receipts
            .extend(commit.idempotency_receipts.clone());
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn put_batch_with_write_timing(
        &mut self,
        request: RecordPutBatchRequest,
    ) -> Result<(Epoch, WritePathTiming)> {
        let total_started = Instant::now();
        let lock_started = Instant::now();
        let _guard = WriteLock::acquire(&self.dir)?;
        let lock_ms = elapsed_ms(lock_started);
        let refresh_timing = self.refresh_from_disk_if_stale_with_timing()?;
        if request.records.is_empty() {
            return Err(TraceDbError::InvalidCommand(
                "batch put requires at least one record".to_string(),
            ));
        }

        let schema_lookup_started = Instant::now();
        let schemas = request
            .records
            .iter()
            .map(|input| {
                self.manifest
                    .table(&input.table)
                    .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))
                    .cloned()
            })
            .collect::<Result<Vec<_>>>()?;
        let schema_lookup_ms = elapsed_ms(schema_lookup_started);

        let epoch = self.manifest.latest_epoch.next();
        let store_clone_ms = 0.0;

        let mut store_apply_timing = ReplacementApplyTiming::default();
        let store_delta_plan_started = Instant::now();
        let mut delta = RecordStoreDelta::default();
        for (input, schema) in request.records.iter().zip(schemas.iter()) {
            let timing = self
                .store
                .plan_replacement_into_with_timing(&mut delta, schema, input, epoch)?;
            store_apply_timing.validate_identity_ms += timing.validate_identity_ms;
            store_apply_timing.validate_vector_ms += timing.validate_vector_ms;
            store_apply_timing.key_ms += timing.key_ms;
            store_apply_timing.fields_ms += timing.fields_ms;
            store_apply_timing.finalize_identity_ms += timing.finalize_identity_ms;
            store_apply_timing.features_ms += timing.features_ms;
            store_apply_timing.install_ms += timing.install_ms;
        }
        let store_delta_plan_ms = elapsed_ms(store_delta_plan_started);

        let feature_invalidation_started = Instant::now();
        let mut feature_invalidations = Vec::new();
        for (input, schema) in request.records.iter().zip(schemas.iter()) {
            feature_invalidations.extend(feature_invalidations_for_mutation(schema, input));
        }
        let feature_invalidation_ms = elapsed_ms(feature_invalidation_started);

        let commit_build_started = Instant::now();
        let mut module_events = Vec::new();
        let mut seen_module_events = BTreeSet::new();
        for schema in &schemas {
            for event in module_events_for_schema("record.put", schema) {
                if seen_module_events.insert((event.module_id.clone(), event.event.clone())) {
                    module_events.push(event);
                }
            }
        }
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: request.records.clone(),
            mutations: Vec::new(),
            deletions: Vec::new(),
            feature_invalidations,
            module_events,
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        let commit_build_ms = elapsed_ms(commit_build_started);

        let (_lsn, wal_timing) = self.wal.append_commit_with_timing(&commit)?;
        let store_install_started = Instant::now();
        self.store.apply_delta(delta);
        let store_delta_apply_ms = elapsed_ms(store_install_started);
        let store_apply_ms = store_delta_plan_ms + store_delta_apply_ms;
        let store_install_ms = store_delta_apply_ms;
        let manifest_timing = self.bump_manifest_with_timing(epoch)?;
        let cache_clear_started = Instant::now();
        self.clear_lexical_cache();
        let cache_clear_ms = elapsed_ms(cache_clear_started);

        Ok((
            epoch,
            WritePathTiming::from_parts(
                elapsed_ms(total_started),
                lock_ms,
                refresh_timing,
                schema_lookup_ms,
                store_clone_ms,
                store_delta_plan_ms,
                store_delta_apply_ms,
                store_apply_ms,
                store_apply_timing,
                feature_invalidation_ms,
                commit_build_ms,
                wal_timing,
                store_install_ms,
                manifest_timing,
                cache_clear_ms,
            ),
        ))
    }

    pub fn put_with_write_timing(
        &mut self,
        request: RecordPutRequest,
    ) -> Result<(Epoch, WritePathTiming)> {
        let total_started = Instant::now();
        let lock_started = Instant::now();
        let _guard = WriteLock::acquire(&self.dir)?;
        let lock_ms = elapsed_ms(lock_started);
        let refresh_timing = self.refresh_from_disk_if_stale_with_timing()?;

        let input = request.record;
        let schema_lookup_started = Instant::now();
        let schema = self
            .manifest
            .table(&input.table)
            .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
            .clone();
        let schema_lookup_ms = elapsed_ms(schema_lookup_started);

        let epoch = self.manifest.latest_epoch.next();
        let store_clone_ms = 0.0;
        let store_delta_plan_started = Instant::now();
        let (delta, store_apply_timing) = self
            .store
            .plan_replacement_with_timing(&schema, &input, epoch)?;
        let store_delta_plan_ms = elapsed_ms(store_delta_plan_started);
        let feature_invalidation_started = Instant::now();
        let feature_invalidations = feature_invalidations_for_mutation(&schema, &input);
        let feature_invalidation_ms = elapsed_ms(feature_invalidation_started);

        let commit_build_started = Instant::now();
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: vec![input.clone()],
            mutations: Vec::new(),
            deletions: Vec::new(),
            feature_invalidations,
            module_events: module_events_for_schema("record.put", &schema),
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        let commit_build_ms = elapsed_ms(commit_build_started);

        let (_lsn, wal_timing) = self.wal.append_commit_with_timing(&commit)?;
        let store_install_started = Instant::now();
        self.store.apply_delta(delta);
        let store_delta_apply_ms = elapsed_ms(store_install_started);
        let store_apply_ms = store_delta_plan_ms + store_delta_apply_ms;
        let store_install_ms = store_delta_apply_ms;
        let manifest_timing = self.bump_manifest_with_timing(epoch)?;
        let cache_clear_started = Instant::now();
        self.clear_lexical_cache();
        let cache_clear_ms = elapsed_ms(cache_clear_started);

        Ok((
            epoch,
            WritePathTiming::from_parts(
                elapsed_ms(total_started),
                lock_ms,
                refresh_timing,
                schema_lookup_ms,
                store_clone_ms,
                store_delta_plan_ms,
                store_delta_apply_ms,
                store_apply_ms,
                store_apply_timing,
                feature_invalidation_ms,
                commit_build_ms,
                wal_timing,
                store_install_ms,
                manifest_timing,
                cache_clear_ms,
            ),
        ))
    }

    pub fn replace(&mut self, request: RecordPutRequest) -> Result<Epoch> {
        self.put(request)
    }

    pub fn patch(&mut self, request: RecordPatchRequest) -> Result<Epoch> {
        self.patch_with_idempotency_receipt(request, None)
    }

    pub fn patch_with_idempotency_receipt(
        &mut self,
        request: RecordPatchRequest,
        receipt: Option<IdempotencyReceipt>,
    ) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let input = request.into_record_input();
        let schema = self
            .manifest
            .table(&input.table)
            .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let delta = self.store.plan_mutation(&schema, &input, epoch)?;
        let feature_invalidations = feature_invalidations_for_mutation(&schema, &input);
        let idempotency_receipts =
            idempotency_receipts_for_response(receipt, serde_json::json!({ "epoch": epoch.get() }));
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: Vec::new(),
            mutations: vec![input.clone()],
            deletions: Vec::new(),
            feature_invalidations,
            module_events: module_events_for_schema("record.patch", &schema),
            idempotency_receipts,
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store.apply_delta(delta);
        self.idempotency_receipts
            .extend(commit.idempotency_receipts.clone());
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn delete(&mut self, request: RecordDeleteRequest) -> Result<Epoch> {
        self.delete_with_idempotency_receipt(request, None)
    }

    pub fn delete_with_idempotency_receipt(
        &mut self,
        request: RecordDeleteRequest,
        receipt: Option<IdempotencyReceipt>,
    ) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let deletion = request.into_deletion();
        let schema = self
            .manifest
            .table(&deletion.table)
            .ok_or_else(|| TraceDbError::UnknownTable(deletion.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let delta = self.store.plan_delete(&schema, &deletion, epoch)?;
        let idempotency_receipts = idempotency_receipts_for_response(
            receipt,
            serde_json::json!({ "deleted": true, "epoch": epoch.get() }),
        );
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: Vec::new(),
            mutations: Vec::new(),
            deletions: vec![deletion],
            feature_invalidations: Vec::new(),
            module_events: module_events_for_schema("record.delete", &schema),
            idempotency_receipts,
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store.apply_delta(delta);
        self.idempotency_receipts
            .extend(commit.idempotency_receipts.clone());
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn get(&self, request: RecordGetRequest) -> Result<Option<RecordOutput>> {
        self.manifest
            .table(&request.table)
            .ok_or_else(|| TraceDbError::UnknownTable(request.table.clone()))?;
        Ok(self
            .store
            .get_record(
                &request.table,
                &request.tenant_id,
                &request.id,
                self.manifest.latest_epoch,
            )
            .map(record_output))
    }

    pub fn get_as(
        &self,
        actor: &ActorContext,
        request: RecordGetRequest,
    ) -> Result<Option<RecordOutput>> {
        validate_actor_tenant(actor, &request.tenant_id)?;
        self.get(request)
    }

    /// Scan records with optional pagination.
    pub fn scan(&self, request: RecordScanRequest) -> Result<RecordScanOutput> {
        self.manifest
            .table(&request.table)
            .ok_or_else(|| TraceDbError::UnknownTable(request.table.clone()))?;
        let limit = request.limit.max(1);
        let cursor_offset = parse_legacy_cursor_offset(request.cursor.as_deref())?;
        let mut records = self.store.visible_records_at(
            &request.table,
            &request.tenant_id,
            self.manifest.latest_epoch,
        );
        records.sort_by(|left, right| left.header.record_id.cmp(&right.header.record_id));
        let page = records
            .into_iter()
            .skip(cursor_offset)
            .take(limit.saturating_add(1))
            .map(record_output)
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        let records = page.into_iter().take(limit).collect::<Vec<_>>();
        Ok(RecordScanOutput {
            next_cursor: has_more.then(|| (cursor_offset + records.len()).to_string()),
            returned_count: records.len(),
            records,
        })
    }

    pub fn scan_as(
        &self,
        actor: &ActorContext,
        request: RecordScanRequest,
    ) -> Result<RecordScanOutput> {
        validate_actor_tenant(actor, &request.tenant_id)?;
        self.manifest
            .table(&request.table)
            .ok_or_else(|| TraceDbError::UnknownTable(request.table.clone()))?;
        let limit = request.limit.max(1);
        let scope = cursor_scope_for_scan(actor, &request, limit);
        let cursor = resolve_cursor_state(
            &scope,
            request.cursor.as_deref(),
            self.manifest.latest_epoch,
        )?;
        let mut records = self.store.visible_records_at(
            &request.table,
            &request.tenant_id,
            cursor.snapshot_epoch,
        );
        records.sort_by(|left, right| left.header.record_id.cmp(&right.header.record_id));
        let page = records
            .into_iter()
            .skip(cursor.offset)
            .take(limit.saturating_add(1))
            .map(record_output)
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        let records = page.into_iter().take(limit).collect::<Vec<_>>();
        Ok(RecordScanOutput {
            next_cursor: if has_more {
                Some(encode_next_cursor(
                    &scope,
                    cursor.offset + records.len(),
                    cursor.snapshot_epoch,
                )?)
            } else {
                None
            },
            returned_count: records.len(),
            records,
        })
    }

    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        Ok(self.store.snapshot(self.manifest.latest_epoch))
    }

    /// Execute a hybrid query.
    pub fn query(&self, query: HybridQuery) -> Result<QueryOutput> {
        Ok(self.query_with_timing(query)?.output)
    }

    pub fn query_as(&self, actor: &ActorContext, query: HybridQuery) -> Result<QueryOutput> {
        Ok(self.query_with_timing_as(actor, query)?.output)
    }

    pub fn query_with_timing_as(
        &self,
        actor: &ActorContext,
        query: HybridQuery,
    ) -> Result<TimedQueryOutput> {
        validate_actor_tenant(actor, &query.tenant_id)?;
        let limit = query.top_k.max(1);
        let scope = cursor_scope_for_query(actor, &query, limit);
        let cursor =
            resolve_cursor_state(&scope, query.cursor.as_deref(), self.manifest.latest_epoch)?;
        self.query_with_timing_internal(query, cursor, Some(scope), Some(actor))
    }

    pub fn query_with_timing(&self, query: HybridQuery) -> Result<TimedQueryOutput> {
        let cursor = CursorState {
            offset: parse_legacy_cursor_offset(query.cursor.as_deref())?,
            snapshot_epoch: self.manifest.latest_epoch,
        };
        self.query_with_timing_internal(query, cursor, None, None)
    }

    fn query_with_timing_internal(
        &self,
        query: HybridQuery,
        cursor: CursorState,
        cursor_scope: Option<CursorScope>,
        actor: Option<&ActorContext>,
    ) -> Result<TimedQueryOutput> {
        let total_started = Instant::now();
        let mut timing = QueryExecutionTiming::default();
        let include_explain = query.explain;
        let read_epoch = cursor.snapshot_epoch;
        let schema = self
            .manifest
            .table(&query.table)
            .ok_or_else(|| TraceDbError::UnknownTable(query.table.clone()))?;
        validate_text_query_field(schema, query.text.as_ref(), query.text_field.as_deref())?;
        validate_vector_query_dimensions(
            schema,
            query.vector.as_deref(),
            query.vector_field.as_deref(),
        )?;
        validate_scalar_eq_predicates(schema, &query.scalar_eq)?;
        let cursor_offset = cursor.offset;
        if query.top_k == 0 {
            timing.total_ms = elapsed_ms(total_started);
            timing.engine_core_ms = timing.total_ms;
            return Ok(TimedQueryOutput {
                timing,
                output: QueryOutput {
                    results: Vec::new(),
                    explain: ExplainOutput {
                        read_epoch,
                        schema_epoch: self.manifest.latest_epoch,
                        policy_epoch: read_epoch,
                        scalar_filter_applied: !query.scalar_eq.is_empty(),
                        scalar_filter_predicates: scalar_filter_predicates(&query.scalar_eq),
                        freshness_mode: query.freshness.as_str().to_string(),
                        fusion_method: "RRF".to_string(),
                        ..ExplainOutput::default()
                    },
                    next_cursor: None,
                },
            });
        }
        let tenant_visibility_started = Instant::now();
        let default_actor = actor
            .is_none()
            .then(|| default_query_actor(&query.tenant_id, read_epoch));
        let actor = actor.or(default_actor.as_ref()).expect("actor resolved");
        let visibility_oracle = VisibilityOracle;
        let visible = self
            .store
            .visible_records_at(&query.table, &query.tenant_id, read_epoch)
            .into_iter()
            .filter(|record| stored_record_visible(record, actor, &visibility_oracle))
            .collect::<Vec<_>>();
        let sealed_records = self
            .sealed_segment_records(&query.table, &query.tenant_id)?
            .into_iter()
            .filter(|record| record.version_id <= read_epoch.get())
            .filter(|record| segment_record_visible(record, actor, &visibility_oracle))
            .collect::<Vec<_>>();
        let tenant_mask_visible_records = visible.len();
        let tenant_visibility_ms = elapsed_ms(tenant_visibility_started);
        let scalar_filter_started = Instant::now();
        let scalar_filter_applied = !query.scalar_eq.is_empty();
        let visible = filter_records_by_scalar_eq(visible, &query.scalar_eq);
        let sealed_records = filter_segment_records_by_scalar_eq(sealed_records, &query.scalar_eq);
        let scalar_filter_ms = elapsed_ms(scalar_filter_started);
        let page_end = cursor_offset.saturating_add(query.top_k);
        let requested_window = page_end.saturating_add(1);
        let candidate_budget = requested_window
            .saturating_mul(4)
            .max(requested_window)
            .max(1);
        let mut explain = ExplainOutput {
            read_epoch,
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: read_epoch,
            tenant_mask_visible_records,
            scalar_filter_applied,
            scalar_filter_predicates: scalar_filter_predicates(&query.scalar_eq),
            scalar_filter_visible_records: visible.len(),
            scalar_filter_removed_records: tenant_mask_visible_records
                .saturating_sub(visible.len()),
            candidate_budget,
            hot_overlay_searched: true,
            segments_scanned: sealed_records.len(),
            freshness_mode: query.freshness.as_str().to_string(),
            fusion_method: "RRF".to_string(),
            ..ExplainOutput::default()
        };
        if include_explain {
            let explain_build_started = Instant::now();
            explain.phase_timings.push(query_phase_timing(
                "tenant_visibility",
                tenant_visibility_ms,
            ));
            explain
                .phase_timings
                .push(query_phase_timing("scalar_filter", scalar_filter_ms));
            let mut trace_query = TraceQuery::hybrid(
                &query.table,
                &query.tenant_id,
                query.text_field.as_deref(),
                query.text.as_deref(),
                query.vector_field.as_deref(),
                query.vector.as_ref().map(Vec::len),
                query.top_k,
            );
            trace_query.scalar_eq = query.scalar_eq.clone();
            if let Some(seed) = &query.graph_seed {
                trace_query.graph_seeds.push(seed.clone());
            }
            trace_query.temporal_as_of = query.temporal_as_of;
            let physical_plan = plan_trace_query(&trace_query, visible.len());
            explain.selected_strategy = Some(format!("{:?}", physical_plan.strategy));
            explain.skipped_access_paths = physical_plan
                .skipped_paths
                .iter()
                .map(|path| format!("{}: {}", path.access_path_id, path.reason))
                .collect();
            explain.exact_fallback_triggered = physical_plan.exact_fallback;
            explain.early_stop_reason =
                Some("all opened streams exhausted under fixed budget".to_string());
            explain.module_versions = self
                .registered_module_catalog()
                .into_iter()
                .map(|module| format!("{}@{}", module.module_id, module.version))
                .collect();

            for record in &visible {
                for vector in &schema.vector_columns {
                    match record.features.get(&vector.name).map(|state| &state.status) {
                        Some(FeatureStatus::Dirty) => explain.dirty_feature_count += 1,
                        Some(FeatureStatus::Pending) => explain.pending_feature_count += 1,
                        Some(FeatureStatus::Failed) => explain.failed_feature_count += 1,
                        Some(FeatureStatus::Missing) | None => explain.missing_feature_count += 1,
                        _ => {}
                    }
                }
            }
            timing.explain_build_ms += elapsed_ms(explain_build_started);
        }

        let query_fragment = QueryFragment {
            text_field: query.text_field.clone(),
            text: query.text.clone(),
            vector_field: query.vector_field.clone(),
            vector_dimensions: query.vector.as_ref().map(Vec::len),
        };
        let visibility = visible
            .iter()
            .map(|record| record.header.record_id.clone())
            .chain(sealed_records.iter().map(|record| record.record_id.clone()))
            .collect::<Vec<_>>();
        let access_path_build_started = Instant::now();
        let access_paths = query_access_paths(
            self,
            QueryAccessInput {
                schema,
                visible: &visible,
                sealed_records: &sealed_records,
                tenant_id: &query.tenant_id,
                scalar_eq_key: scalar_eq_cache_key(&query.scalar_eq),
                text_field: query.text_field.clone(),
                text: query.text.clone(),
                vector_field: query.vector_field.clone(),
                vector_query: query.vector.clone(),
                graph_seed: query.graph_seed.clone(),
                temporal_as_of: query.temporal_as_of,
                freshness: &query.freshness,
                fallback_candidate_limit: query_has_evidence(&query).then_some(candidate_budget),
            },
        )?;
        let access_path_build_ms = elapsed_ms(access_path_build_started);
        let mut planner_candidates = Vec::<Candidate>::new();
        let mut streams = Vec::<RankedStream>::new();
        let mut access_path_timings = access_paths.timings;
        let access_path_open_started = Instant::now();
        for (index, access_path) in access_paths.paths.iter().enumerate() {
            let descriptor = access_path.descriptor();
            let access_path_open_started = Instant::now();
            let batch = access_path.open(
                query_fragment.clone(),
                &visibility,
                CandidateBudget {
                    max_candidates: candidate_budget,
                },
            );
            if let Some(timing) = access_path_timings.get_mut(index) {
                timing.open_ms = elapsed_ms(access_path_open_started);
            }
            let source = descriptor.access_path_id.as_str();
            if source == "LexicalPath" {
                if include_explain {
                    let explain_build_started = Instant::now();
                    explain.text_candidates = batch.candidates.len();
                    explain.opened_candidate_streams.push("text".to_string());
                    timing.explain_build_ms += elapsed_ms(explain_build_started);
                }
                streams.push(ranked_stream_from_candidates("text", &batch.candidates));
            } else if source == "VectorPath" {
                if include_explain {
                    let explain_build_started = Instant::now();
                    explain.vector_candidates = batch.candidates.len();
                    explain.opened_candidate_streams.push("vector".to_string());
                    timing.explain_build_ms += elapsed_ms(explain_build_started);
                }
                streams.push(ranked_stream_from_candidates("vector", &batch.candidates));
            } else {
                if include_explain {
                    let explain_build_started = Instant::now();
                    explain
                        .opened_candidate_streams
                        .push(candidate_stream_name(source).to_string());
                    timing.explain_build_ms += elapsed_ms(explain_build_started);
                }
                streams.push(ranked_stream_from_candidates(
                    candidate_stream_name(source),
                    &batch.candidates,
                ));
            }
            if include_explain {
                let explain_build_started = Instant::now();
                planner_candidates.extend(batch.candidates);
                explain.access_paths.push(access_path.explain());
                timing.explain_build_ms += elapsed_ms(explain_build_started);
            }
        }
        let access_path_open_ms = elapsed_ms(access_path_open_started);
        if include_explain {
            let explain_build_started = Instant::now();
            explain.phase_timings.push(query_phase_timing(
                "access_path_build",
                access_path_build_ms,
            ));
            explain
                .phase_timings
                .push(query_phase_timing("access_path_open", access_path_open_ms));
            explain.access_path_timings = access_path_timings;
            explain.lexical_cache_hits = access_paths.lexical_cache_hits;
            explain.lexical_cache_misses = access_paths.lexical_cache_misses;
            explain.lexical_indexed_documents = access_paths.lexical_indexed_documents;
            explain.lexical_scored_documents = access_paths.lexical_scored_documents;
            explain.exact_fallback_triggered |= access_paths.vector_exact_fallback_used;
            explain.planner_candidates = planner_candidates;
            timing.explain_build_ms += elapsed_ms(explain_build_started);
        }

        let fusion_started = Instant::now();
        let mut fused = fuse_query_streams(&streams);
        if query.text.is_some() && !lexical_scores_are_tied(&fused) {
            fused.sort_by(lexical_first_order);
        } else if query.vector.is_some() {
            fused.sort_by(vector_first_order);
        }
        if include_explain {
            let explain_build_started = Instant::now();
            explain.deduped_candidate_count = fused.len();
            explain
                .phase_timings
                .push(query_phase_timing("fusion", elapsed_ms(fusion_started)));
            timing.explain_build_ms += elapsed_ms(explain_build_started);
        }
        let materialization_started = Instant::now();
        let sealed_visible_records = sealed_records
            .iter()
            .filter(|record| {
                !self.store.is_tombstoned_at(
                    &record.table,
                    &record.tenant_id,
                    &record.record_id,
                    read_epoch,
                )
            })
            .collect::<Vec<_>>();
        let visible_ids = visible
            .iter()
            .map(|record| record.header.record_id.clone())
            .chain(
                sealed_visible_records
                    .iter()
                    .map(|record| record.record_id.clone()),
            )
            .collect::<BTreeSet<_>>();
        let records_by_id = visible
            .iter()
            .map(|record| (record.header.record_id.clone(), record.clone()))
            .collect::<BTreeMap<_, _>>();
        let sealed_records_by_id = sealed_visible_records
            .into_iter()
            .map(|record| (record.record_id.clone(), record))
            .collect::<BTreeMap<_, _>>();

        let mut materialized = Vec::new();
        let mut removed = 0usize;
        let mut checked = 0usize;
        for candidate in fused {
            checked += 1;
            if !visible_ids.contains(&candidate.record_id) {
                removed += 1;
                continue;
            }
            if materialized.len() >= requested_window {
                continue;
            }
            if let Some(record) = records_by_id.get(&candidate.record_id) {
                materialized.push(query_row_from_stored(record, &candidate));
            } else if let Some(record) = sealed_records_by_id.get(&candidate.record_id) {
                materialized.push(query_row_from_segment(record, &candidate));
            }
        }
        let materialize_ms = elapsed_ms(materialization_started);
        timing.materialize_ms = materialize_ms;

        let has_more = materialized.len() > page_end;
        let page = materialized
            .into_iter()
            .skip(cursor_offset)
            .take(query.top_k)
            .collect::<Vec<_>>();

        explain.materialized_count = page.len();
        explain.final_visibility_guard_count = checked;
        explain.final_visibility_guard_removed = removed;
        explain.returned_count = page.len();
        if include_explain {
            let explain_build_started = Instant::now();
            explain
                .phase_timings
                .push(query_phase_timing("materialization", materialize_ms));
            timing.explain_build_ms += elapsed_ms(explain_build_started);
        }
        timing.total_ms = elapsed_ms(total_started);
        timing.engine_core_ms =
            (timing.total_ms - timing.explain_build_ms - timing.materialize_ms).max(0.0);
        Ok(TimedQueryOutput {
            timing,
            output: QueryOutput {
                next_cursor: if has_more {
                    Some(match cursor_scope.as_ref() {
                        Some(scope) => {
                            encode_next_cursor(scope, cursor_offset + page.len(), read_epoch)?
                        }
                        None => (cursor_offset + page.len()).to_string(),
                    })
                } else {
                    None
                },
                results: page,
                explain,
            },
        })
    }

    pub fn feature_state(
        &self,
        table: &str,
        tenant_id: &str,
        record_id: &str,
        feature: &str,
    ) -> Result<DerivedFeatureState> {
        self.store
            .feature_state(
                table,
                tenant_id,
                record_id,
                feature,
                self.manifest.latest_epoch,
            )
            .ok_or_else(|| TraceDbError::NotFound(format!("feature {table}.{record_id}.{feature}")))
    }

    pub fn set_feature_status(
        &mut self,
        table: &str,
        tenant_id: &str,
        record_id: &str,
        feature: &str,
        status: FeatureStatus,
    ) -> Result<Epoch> {
        if tenant_id.trim().is_empty() {
            return Err(TraceDbError::InvalidRecord(
                "tenant id cannot be empty".to_string(),
            ));
        }

        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let schema = self
            .manifest
            .table(table)
            .ok_or_else(|| TraceDbError::UnknownTable(table.to_string()))?
            .clone();
        if !schema
            .vector_columns
            .iter()
            .any(|vector| vector.name == feature)
        {
            return Err(TraceDbError::NotFound(format!(
                "feature {table}.{record_id}.{feature}"
            )));
        }

        let epoch = self.manifest.latest_epoch.next();
        let invalidation = FeatureInvalidation {
            table: table.to_string(),
            tenant_id: tenant_id.to_string(),
            record_id: record_id.to_string(),
            feature: feature.to_string(),
            status,
        };
        let delta = self.store.plan_feature_invalidation(&invalidation, epoch)?;
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: Vec::new(),
            mutations: Vec::new(),
            deletions: Vec::new(),
            feature_invalidations: vec![invalidation],
            module_events: vec![ModuleCommitEvent {
                module_id: "tracedb-features".to_string(),
                event: "feature.status.set".to_string(),
            }],
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store.apply_delta(delta);
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn checkpoint(&mut self) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        let epoch = self.manifest.latest_epoch;
        write_checkpoint_file(
            &self.dir,
            epoch,
            self.manifest.schemas.clone(),
            self.store.checkpoint_records(epoch),
            self.idempotency_receipts.clone(),
            self.job_catalog.clone(),
            self.encryption.as_ref(),
        )?;
        let mut manifest = read_manifest(self.dir.join("manifest.tdb"))?;
        if manifest.latest_epoch != epoch {
            return Err(TraceDbError::ManifestCorruption(format!(
                "checkpoint handle is stale: handle epoch {}, manifest epoch {}",
                epoch, manifest.latest_epoch
            )));
        }
        manifest.checkpoint_epoch = epoch;
        manifest.manifest_generation += 1;
        let previous_checksum = manifest.checksums.manifest_checksum;
        manifest.checksums.parent_checksum = previous_checksum;
        write_manifest(self.dir.join("manifest.tdb"), &mut manifest)?;
        truncate_active_wal(&self.dir)?;
        self.wal = Wal::open_with_encryption(&self.dir, self.encryption.clone())?;
        self.manifest = manifest;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn inspect_manifest(&self) -> Result<TraceDbManifest> {
        Ok(self.manifest.clone())
    }

    pub fn inspect_wal(&self) -> Result<Vec<tracedb_log::WalEntry>> {
        self.wal.scan()
    }

    pub fn last_recovery_torn_tail(&self) -> Option<TornWalTail> {
        self.last_recovery_torn_tail.clone()
    }

    pub fn registered_modules(&self) -> Vec<String> {
        self.registered_module_catalog()
            .into_iter()
            .map(|module| module.module_id)
            .collect()
    }

    pub fn registered_module_catalog(&self) -> Vec<RegisteredModule> {
        let mut registry = ModuleRegistry::default();
        registry.register(Box::new(tracedb_text::TextModule)).ok();
        registry
            .register(Box::new(tracedb_vector::VectorModule))
            .ok();
        registry.register(Box::new(tracedb_graph::GraphModule)).ok();
        registry
            .register(Box::new(tracedb_temporal::TemporalModule))
            .ok();
        registry
            .register(Box::new(tracedb_policy::PolicyModule))
            .ok();
        registry
            .register(Box::new(tracedb_provenance::ProvenanceModule))
            .ok();
        registry
            .register(Box::new(tracedb_features::FeaturesModule))
            .ok();
        registry
            .register(Box::new(tracedb_retrieval_core::RetrievalCoreModule))
            .ok();
        registry.modules().to_vec()
    }

    pub fn publish_segment(&mut self, segment_id: impl Into<String>) -> Result<()> {
        self.publish_segment_with_parent_generation(segment_id, self.manifest.manifest_generation)
    }

    pub fn compact(&mut self) -> Result<()> {
        let segment_id = format!(
            "compact-{}-{}",
            self.manifest.latest_epoch.get(),
            self.manifest.manifest_generation + 1
        );
        self.publish_compacted_segment(segment_id, self.manifest.manifest_generation)
    }

    pub fn vacuum(&mut self) -> Result<usize> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let referenced_segments = self
            .manifest
            .segments
            .iter()
            .map(|segment| format!("{}.tseg", segment.segment_id))
            .collect::<BTreeSet<_>>();
        let referenced_indexes = self
            .manifest
            .indexes
            .iter()
            .filter_map(|index| Path::new(&index.object_path).file_name())
            .map(|file_name| file_name.to_string_lossy().to_string())
            .collect::<BTreeSet<_>>();
        let removed_segments =
            vacuum_artifact_dir(&self.dir.join("segments"), &referenced_segments)?;
        let removed_indexes = vacuum_artifact_dir(&self.dir.join("indexes"), &referenced_indexes)?;
        Ok(removed_segments + removed_indexes)
    }

    pub fn publish_segment_with_parent_generation(
        &mut self,
        segment_id: impl Into<String>,
        parent_manifest_generation: u64,
    ) -> Result<()> {
        let _guard = WriteLock::acquire(&self.dir)?;
        let segment_id = segment_id.into();
        let manifest_path = self.dir.join("manifest.tdb");
        let durable_manifest = read_manifest(&manifest_path)?;
        if parent_manifest_generation != durable_manifest.manifest_generation {
            return Err(TraceDbError::ManifestCorruption(format!(
                "parent manifest generation mismatch: expected {}, got {parent_manifest_generation}",
                durable_manifest.manifest_generation
            )));
        }
        if let Some(existing) = durable_manifest
            .segments
            .iter()
            .find(|segment| segment.segment_id == segment_id)
        {
            if existing.state != tracedb_core::SegmentState::Published {
                return Err(TraceDbError::ManifestCorruption(format!(
                    "segment {} already exists in state {:?}",
                    existing.segment_id, existing.state
                )));
            }
            let object_path = self.dir.join("segments").join(format!("{segment_id}.tseg"));
            let object = tracedb_segment::read_segment_object_with_encryption(
                &object_path,
                self.encryption.as_ref(),
            )?;
            if object.generation != existing.generation {
                return Err(TraceDbError::ManifestCorruption(format!(
                    "segment object generation mismatch: manifest {}, object {}",
                    existing.generation, object.generation
                )));
            }
            self.manifest = durable_manifest;
            self.clear_lexical_cache();
            return Ok(());
        }
        let generation = durable_manifest.segments.len() as u64 + 1;
        let object_path = self.dir.join("segments").join(format!("{segment_id}.tseg"));
        let records = self.segment_records_for_snapshot()?;
        let object = tracedb_segment::publish_segment_records_with_encryption(
            &object_path,
            &segment_id,
            generation,
            records,
            self.encryption.as_ref(),
        )?;
        let index_manifests = self.build_segment_indexes(&object, parent_manifest_generation)?;
        let mut manifest = durable_manifest;
        manifest.segments.push(object.manifest());
        manifest.indexes.extend(index_manifests);
        manifest.manifest_generation += 1;
        let previous_checksum = manifest.checksums.manifest_checksum;
        manifest.checksums.parent_checksum = previous_checksum;
        write_manifest(manifest_path, &mut manifest)?;
        self.manifest = manifest;
        self.clear_lexical_cache();
        Ok(())
    }

    fn publish_compacted_segment(
        &mut self,
        segment_id: impl Into<String>,
        parent_manifest_generation: u64,
    ) -> Result<()> {
        let _guard = WriteLock::acquire(&self.dir)?;
        let segment_id = segment_id.into();
        let manifest_path = self.dir.join("manifest.tdb");
        let durable_manifest = read_manifest(&manifest_path)?;
        if parent_manifest_generation != durable_manifest.manifest_generation {
            return Err(TraceDbError::ManifestCorruption(format!(
                "parent manifest generation mismatch: expected {}, got {parent_manifest_generation}",
                durable_manifest.manifest_generation
            )));
        }
        if durable_manifest
            .segments
            .iter()
            .any(|segment| segment.segment_id == segment_id)
        {
            return Err(TraceDbError::ManifestCorruption(format!(
                "compacted segment {} already exists",
                segment_id
            )));
        }

        let source_segment_ids = durable_manifest
            .segments
            .iter()
            .filter(|segment| segment.state == tracedb_core::SegmentState::Published)
            .map(|segment| segment.segment_id.clone())
            .collect::<BTreeSet<_>>();
        let generation = durable_manifest.manifest_generation + 1;
        let object_path = self.dir.join("segments").join(format!("{segment_id}.tseg"));
        let records = self.segment_records_for_snapshot()?;
        let object = tracedb_segment::publish_segment_records_with_encryption(
            &object_path,
            &segment_id,
            generation,
            records,
            self.encryption.as_ref(),
        )?;
        let index_manifests = self.build_segment_indexes(&object, parent_manifest_generation)?;
        let mut manifest = durable_manifest;
        manifest
            .segments
            .retain(|segment| !source_segment_ids.contains(&segment.segment_id));
        manifest
            .indexes
            .retain(|index| !source_segment_ids.contains(&index.segment_id));
        manifest.segments.push(object.manifest());
        manifest.indexes.extend(index_manifests);
        manifest.manifest_generation += 1;
        let previous_checksum = manifest.checksums.manifest_checksum;
        manifest.checksums.parent_checksum = previous_checksum;
        write_manifest(manifest_path, &mut manifest)?;
        self.manifest = manifest;
        self.clear_lexical_cache();
        Ok(())
    }

    pub fn backup(&self, target: impl AsRef<Path>) -> Result<()> {
        backup_dir(&self.dir, target.as_ref())
    }

    pub fn create_snapshot(&self, target: impl AsRef<Path>) -> Result<()> {
        snapshot_dir(&self.dir, target.as_ref())
    }

    pub fn restore(source: impl AsRef<Path>, target: impl AsRef<Path>) -> Result<Self> {
        restore_dir(source.as_ref(), target.as_ref())?;
        Self::open(target)
    }

    pub fn restore_snapshot(source: impl AsRef<Path>, target: impl AsRef<Path>) -> Result<Self> {
        restore_dir(source.as_ref(), target.as_ref())?;
        Self::open(target)
    }

    fn bump_manifest(&mut self, epoch: Epoch) -> Result<()> {
        self.manifest.latest_epoch = epoch;
        self.manifest.durable_epoch = epoch;
        self.manifest.manifest_generation += 1;
        let mut manifest = self.manifest.clone();
        let previous_checksum = manifest.checksums.manifest_checksum;
        manifest.checksums.parent_checksum = previous_checksum;
        write_manifest(self.dir.join("manifest.tdb"), &mut manifest)?;
        self.manifest = manifest;
        Ok(())
    }

    fn bump_manifest_with_timing(&mut self, epoch: Epoch) -> Result<ManifestBumpTiming> {
        let total_started = Instant::now();
        self.manifest.latest_epoch = epoch;
        self.manifest.durable_epoch = epoch;
        self.manifest.manifest_generation += 1;
        let clone_started = Instant::now();
        let mut manifest = self.manifest.clone();
        let previous_checksum = manifest.checksums.manifest_checksum;
        manifest.checksums.parent_checksum = previous_checksum;
        let clone_ms = elapsed_ms(clone_started);
        let write = write_manifest_with_timing(self.dir.join("manifest.tdb"), &mut manifest)?;
        self.manifest = manifest;
        Ok(ManifestBumpTiming {
            total_ms: elapsed_ms(total_started),
            clone_ms,
            write,
        })
    }

    fn refresh_from_disk_if_stale(&mut self) -> Result<()> {
        self.refresh_from_disk_if_stale_with_timing().map(|_| ())
    }

    fn refresh_from_disk_if_stale_with_timing(&mut self) -> Result<RefreshTiming> {
        let total_started = Instant::now();
        let manifest_read_started = Instant::now();
        let durable_manifest = read_manifest(self.dir.join("manifest.tdb"))?;
        let manifest_read_ms = elapsed_ms(manifest_read_started);
        let wal_tail_started = Instant::now();
        let wal_last_epoch = self.wal.last_commit_epoch()?;
        let wal_tail_ms = elapsed_ms(wal_tail_started);
        let wal_is_newer = wal_last_epoch
            .map(|last_epoch| last_epoch > self.manifest.latest_epoch)
            .unwrap_or(false);
        let manifest_is_newer = durable_manifest.latest_epoch > self.manifest.latest_epoch
            || durable_manifest.checkpoint_epoch > self.manifest.checkpoint_epoch
            || durable_manifest.manifest_generation > self.manifest.manifest_generation;
        if !wal_is_newer && !manifest_is_newer {
            return Ok(RefreshTiming {
                total_ms: elapsed_ms(total_started),
                manifest_read_ms,
                wal_tail_ms,
                reopen_ms: 0.0,
                performed: false,
            });
        }

        let reopen_started = Instant::now();
        let TraceDb {
            manifest,
            store,
            wal,
            encryption,
            idempotency_receipts,
            last_recovery_torn_tail,
            ..
        } = TraceDb::open_with_options(
            &self.dir,
            TraceDbOpenOptions::with_existing_encryption_context(self.encryption.clone()),
        )?;
        self.manifest = manifest;
        self.store = store;
        self.wal = wal;
        self.encryption = encryption;
        self.idempotency_receipts = idempotency_receipts;
        self.last_recovery_torn_tail = last_recovery_torn_tail;
        self.clear_lexical_cache();
        Ok(RefreshTiming {
            total_ms: elapsed_ms(total_started),
            manifest_read_ms,
            wal_tail_ms,
            reopen_ms: elapsed_ms(reopen_started),
            performed: true,
        })
    }

    fn validate_schema_compatible(&self, schema: &TableSchema) -> Result<()> {
        let Some(existing) = self.manifest.table(&schema.name) else {
            return Ok(());
        };
        let has_committed_rows = !self
            .store
            .snapshot(self.manifest.latest_epoch)
            .all_visible_records(&schema.name)
            .is_empty();
        if !has_committed_rows {
            return Ok(());
        }
        if schema.primary_id_column != existing.primary_id_column {
            return Err(TraceDbError::InvalidSchema(format!(
                "incompatible schema change for table {}: primary id column cannot change after committed rows exist",
                schema.name
            )));
        }
        if schema.tenant_id_column != existing.tenant_id_column {
            return Err(TraceDbError::InvalidSchema(format!(
                "incompatible schema change for table {}: tenant id column cannot change after committed rows exist",
                schema.name
            )));
        }
        for existing_vector in &existing.vector_columns {
            if let Some(new_vector) = schema
                .vector_columns
                .iter()
                .find(|vector| vector.name == existing_vector.name)
            {
                if new_vector.dimensions != existing_vector.dimensions {
                    return Err(TraceDbError::InvalidSchema(format!(
                        "incompatible schema change for vector column {}: existing committed rows use {} dimensions, new schema uses {}",
                        existing_vector.name, existing_vector.dimensions, new_vector.dimensions
                    )));
                }
            }
        }
        Ok(())
    }

    fn segment_records_for_snapshot(&self) -> Result<Vec<SegmentRecord>> {
        let snapshot = self.store.snapshot(self.manifest.latest_epoch);
        let mut out = Vec::new();
        for schema in &self.manifest.schemas {
            for record in snapshot.all_visible_records(&schema.name) {
                let mut text = BTreeMap::new();
                for column in &schema.text_indexed_columns {
                    if let Some(Value::String(value)) = record.fields.get(column) {
                        text.insert(column.clone(), value.clone());
                    }
                }
                let mut vectors = BTreeMap::new();
                for vector in &schema.vector_columns {
                    if let Some(values) = record.fields.get(&vector.name).and_then(value_as_f32_vec)
                    {
                        vectors.insert(vector.name.clone(), values);
                    }
                }
                out.push(SegmentRecord {
                    table: record.header.table_id.clone(),
                    record_id: record.header.record_id.clone(),
                    tenant_id: record.header.tenant_id.clone(),
                    version_id: record.header.version_id.get(),
                    fields: record.fields.clone().into_iter().collect(),
                    text,
                    vectors,
                });
            }
        }
        Ok(out)
    }

    fn sealed_segment_records(&self, table: &str, tenant_id: &str) -> Result<Vec<SegmentRecord>> {
        let mut out = Vec::new();
        for segment in &self.manifest.segments {
            if segment.state != tracedb_core::SegmentState::Published {
                continue;
            }
            if !segment.table_set.is_empty()
                && !segment.table_set.iter().any(|entry| entry == table)
            {
                continue;
            }
            if !segment.tenant_set.is_empty()
                && !segment.tenant_set.iter().any(|entry| entry == tenant_id)
            {
                continue;
            }
            let path = self
                .dir
                .join("segments")
                .join(format!("{}.tseg", segment.segment_id));
            if !path.exists() {
                continue;
            }
            let object = tracedb_segment::read_segment_object_with_encryption(
                path,
                self.encryption.as_ref(),
            )?;
            self.validate_ready_indexes_for_segment(&segment.segment_id)?;
            out.extend(
                object
                    .records
                    .into_iter()
                    .filter(|record| record.table == table && record.tenant_id == tenant_id),
            );
        }
        Ok(out)
    }

    fn validate_ready_indexes_for_segment(&self, segment_id: &str) -> Result<()> {
        for index in self
            .manifest
            .indexes
            .iter()
            .filter(|index| index.segment_id == segment_id && index.state == IndexState::Ready)
        {
            let artifact = tracedb_index::read_index_artifact(
                self.dir.join(&index.object_path),
                self.encryption.as_ref(),
            )
            .map_err(|error| {
                TraceDbError::ArtifactCorruption(format!(
                    "index artifact {} failed verification: {error}",
                    index.index_id
                ))
            })?;
            let payload_checksum = artifact.payload_checksum()?;
            if index.payload_checksum != [0u8; 32] && payload_checksum != index.payload_checksum {
                return Err(TraceDbError::ArtifactCorruption(format!(
                    "index artifact {} payload checksum mismatch: manifest {:?}, artifact {payload_checksum:?}",
                    index.index_id, index.payload_checksum
                )));
            }
            if index.source_segment_checksum != [0u8; 32]
                && artifact.source_segment_checksum != index.source_segment_checksum
            {
                return Err(TraceDbError::ArtifactCorruption(format!(
                    "index artifact {} source segment checksum mismatch",
                    index.index_id
                )));
            }
        }
        Ok(())
    }

    fn build_segment_indexes(
        &self,
        object: &tracedb_segment::SegmentObject,
        parent_manifest_generation: u64,
    ) -> Result<Vec<IndexManifest>> {
        let records = object
            .records
            .iter()
            .map(index_record_from_segment)
            .collect::<Vec<_>>();
        let artifacts = tracedb_index::build_segment_index_artifacts(
            &object.segment_id,
            object.generation,
            parent_manifest_generation,
            object.object_checksum,
            &records,
        )?;
        let mut manifests = Vec::new();
        for artifact in artifacts {
            let index_id = artifact.index_id.clone();
            let object_path = format!("indexes/{index_id}.tidx");
            let index_path = self.dir.join(&object_path);
            let checksum = tracedb_index::write_index_artifact(
                &index_path,
                &artifact,
                self.encryption.as_ref(),
            )?;
            let payload_checksum = artifact.payload_checksum()?;

            manifests.push(IndexManifest {
                index_id,
                segment_id: object.segment_id.clone(),
                generation: object.generation,
                kind: artifact.kind,
                state: IndexState::Ready,
                policy_aware: true,
                parent_manifest_generation,
                object_path,
                checksum,
                source_segment_checksum: object.object_checksum,
                payload_checksum,
                artifact_format_version: 1,
                codec: "bincode".to_string(),
                created_epoch: self.manifest.latest_epoch,
                ready_epoch: Some(self.manifest.latest_epoch),
            });
        }
        Ok(manifests)
    }
}

impl BackupRestore for TraceDb {
    fn backup(&self, target: impl AsRef<Path>) -> Result<()> {
        TraceDb::backup(self, target)
    }

    fn restore(source: impl AsRef<Path>, target: impl AsRef<Path>) -> Result<TraceDb> {
        TraceDb::restore(source, target)
    }
}

#[derive(Clone, Debug)]
struct StreamCandidate {
    record_id: String,
    score: f32,
    freshness_penalty: f32,
}

#[derive(Clone, Debug)]
struct RankedStream {
    name: &'static str,
    candidates: Vec<StreamCandidate>,
}

#[derive(Clone, Debug)]
struct MemoryAccessPath {
    descriptor: PlannerAccessPathDescriptor,
    candidates: Vec<Candidate>,
}

impl AccessPath for MemoryAccessPath {
    fn descriptor(&self) -> PlannerAccessPathDescriptor {
        self.descriptor.clone()
    }

    fn estimate(&self, _predicates: &Predicates, _ctx: &Stats) -> CostEstimate {
        CostEstimate {
            startup_cost: 1.0,
            per_candidate_cost: 1.0,
            expected_candidates: self.candidates.len(),
        }
    }

    fn open(
        &self,
        query: QueryFragment,
        visibility: &[String],
        budget: CandidateBudget,
    ) -> CandidateBatch {
        let visible = visibility.iter().collect::<BTreeSet<_>>();
        let query_allows_path = match self.descriptor.access_path_id.as_str() {
            "LexicalPath" => query
                .text
                .as_ref()
                .is_some_and(|text| !text.trim().is_empty()),
            "VectorPath" => query.vector_dimensions.is_some(),
            _ => true,
        };
        CandidateBatch {
            candidates: self
                .candidates
                .iter()
                .filter(|candidate| {
                    query_allows_path
                        && (!self.descriptor.policy_aware || visible.contains(&candidate.record_id))
                })
                .take(budget.max_candidates)
                .cloned()
                .collect(),
        }
    }

    fn next_batch(&mut self, budget: WorkBudget) -> CandidateBatch {
        CandidateBatch {
            candidates: self
                .candidates
                .iter()
                .take(budget.max_work_units)
                .cloned()
                .collect(),
        }
    }

    fn refine(&mut self, _feedback: PlannerFeedback) {}

    fn explain(&self) -> AccessPathExplain {
        AccessPathExplain {
            access_path_id: self.descriptor.access_path_id.clone(),
            opened: true,
            visibility_checked_before_open: self.descriptor.policy_aware,
            candidates: self.candidates.len(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct FusedCandidate {
    record_id: String,
    vector: Option<f32>,
    lexical: Option<f32>,
    relational: Option<f32>,
    freshness_penalty: f32,
    final_score: f32,
}

fn fuse_rrf(streams: &[RankedStream]) -> Vec<FusedCandidate> {
    let mut fused = BTreeMap::<String, FusedCandidate>::new();
    for stream in streams {
        for (idx, candidate) in stream.candidates.iter().enumerate() {
            let rank = idx as f32 + 1.0;
            let entry =
                fused
                    .entry(candidate.record_id.clone())
                    .or_insert_with(|| FusedCandidate {
                        record_id: candidate.record_id.clone(),
                        ..FusedCandidate::default()
                    });
            entry.final_score += 1.0 / (60.0 + rank);
            entry.freshness_penalty = entry.freshness_penalty.max(candidate.freshness_penalty);
            match stream.name {
                "text" => entry.lexical = Some(candidate.score),
                "vector" => entry.vector = Some(candidate.score),
                _ => entry.relational = Some(candidate.score),
            }
        }
    }
    let mut values = fused
        .into_values()
        .map(|mut candidate| {
            candidate.final_score -= candidate.freshness_penalty;
            candidate
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        score_order(left.final_score, right.final_score)
            .then_with(|| left.record_id.cmp(&right.record_id))
    });
    values
}

fn fuse_query_streams(streams: &[RankedStream]) -> Vec<FusedCandidate> {
    let evidence_streams = streams
        .iter()
        .filter(|stream| is_evidence_stream(stream.name) && !stream.candidates.is_empty())
        .cloned()
        .collect::<Vec<_>>();
    if evidence_streams.is_empty() {
        return fuse_rrf(streams);
    }

    let mut fused = fuse_rrf(&evidence_streams);
    let mut seen = fused
        .iter()
        .map(|candidate| candidate.record_id.clone())
        .collect::<BTreeSet<_>>();
    let fallback_streams = streams
        .iter()
        .filter(|stream| !is_evidence_stream(stream.name) && !stream.candidates.is_empty())
        .cloned()
        .collect::<Vec<_>>();
    for candidate in fuse_rrf(&fallback_streams) {
        if seen.insert(candidate.record_id.clone()) {
            fused.push(candidate);
        }
    }
    fused
}

fn query_row_from_stored(record: &StoredRecord, candidate: &FusedCandidate) -> QueryRow {
    QueryRow {
        record_id: record.header.record_id.clone(),
        version_id: record.header.version_id.get(),
        tenant_id: record.header.tenant_id.clone(),
        fields: record.fields.clone(),
        score: score_components_from_candidate(candidate),
    }
}

fn query_row_from_segment(record: &SegmentRecord, candidate: &FusedCandidate) -> QueryRow {
    QueryRow {
        record_id: record.record_id.clone(),
        version_id: record.version_id,
        tenant_id: record.tenant_id.clone(),
        fields: record.fields.clone().into_iter().collect(),
        score: score_components_from_candidate(candidate),
    }
}

fn score_components_from_candidate(candidate: &FusedCandidate) -> ScoreComponents {
    ScoreComponents {
        vector: candidate.vector,
        lexical: candidate.lexical,
        relational: candidate.relational,
        freshness_penalty: (candidate.freshness_penalty > 0.0)
            .then_some(candidate.freshness_penalty),
        final_score: candidate.final_score,
    }
}

fn is_evidence_stream(name: &str) -> bool {
    matches!(name, "text" | "vector" | "graph" | "temporal")
}

fn lexical_first_order(left: &FusedCandidate, right: &FusedCandidate) -> Ordering {
    match (left.lexical, right.lexical) {
        (Some(left_lexical), Some(right_lexical)) => score_order(left_lexical, right_lexical)
            .then_with(|| match (left.vector, right.vector) {
                (Some(left_vector), Some(right_vector)) => score_order(left_vector, right_vector),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            })
            .then_with(|| score_order(left.final_score, right.final_score))
            .then_with(|| left.record_id.cmp(&right.record_id)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => score_order(left.final_score, right.final_score)
            .then_with(|| left.record_id.cmp(&right.record_id)),
    }
}

fn vector_first_order(left: &FusedCandidate, right: &FusedCandidate) -> Ordering {
    match (left.vector, right.vector) {
        (Some(left_vector), Some(right_vector)) => score_order(left_vector, right_vector)
            .then_with(|| score_order(left.final_score, right.final_score))
            .then_with(|| left.record_id.cmp(&right.record_id)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => score_order(left.final_score, right.final_score)
            .then_with(|| left.record_id.cmp(&right.record_id)),
    }
}

fn query_has_evidence(query: &HybridQuery) -> bool {
    query
        .text
        .as_ref()
        .is_some_and(|text| !text.trim().is_empty())
        || query.vector.is_some()
        || query.graph_seed.is_some()
        || query.temporal_as_of.is_some()
}

fn query_phase_timing(phase: impl Into<String>, elapsed_ms: f64) -> QueryPhaseTiming {
    QueryPhaseTiming {
        phase: phase.into(),
        elapsed_ms,
    }
}

fn access_path_timing(access_path_id: impl Into<String>, build_ms: f64) -> AccessPathTiming {
    AccessPathTiming {
        access_path_id: access_path_id.into(),
        build_ms,
        open_ms: 0.0,
    }
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn http_ok_json(value: &Value) -> String {
    let body = value.to_string();
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn idempotency_receipts_for_response(
    receipt: Option<IdempotencyReceipt>,
    response: Value,
) -> Vec<IdempotencyReceipt> {
    receipt
        .map(|mut receipt| {
            receipt.response = http_ok_json(&response);
            receipt
        })
        .into_iter()
        .collect()
}

fn lexical_scores_are_tied(candidates: &[FusedCandidate]) -> bool {
    let mut scores = candidates
        .iter()
        .filter_map(|candidate| candidate.lexical)
        .filter(|score| score.is_finite());
    let Some(first) = scores.next() else {
        return true;
    };
    let (min, max) = scores.fold((first, first), |(min, max), score| {
        (min.min(score), max.max(score))
    });
    (max - min).abs() <= 1e-6
}

fn ranked_stream_from_candidates(name: &'static str, candidates: &[Candidate]) -> RankedStream {
    RankedStream {
        name,
        candidates: candidates
            .iter()
            .map(|candidate| StreamCandidate {
                record_id: candidate.record_id.clone(),
                score: candidate.score_components.final_score,
                freshness_penalty: candidate
                    .score_components
                    .freshness_penalty
                    .unwrap_or_default(),
            })
            .collect(),
    }
}

fn candidate_stream_name(access_path_id: &str) -> &'static str {
    match access_path_id {
        "PolicyPath" => "policy",
        "RelationalPath" => "relational",
        "HotOverlayPath" => "hot_overlay",
        "GraphPath" => "graph",
        "TemporalPath" => "temporal",
        _ => "relational",
    }
}

struct QueryAccessInput<'a> {
    schema: &'a TableSchema,
    visible: &'a [StoredRecord],
    sealed_records: &'a [SegmentRecord],
    tenant_id: &'a str,
    scalar_eq_key: String,
    text_field: Option<String>,
    text: Option<String>,
    vector_field: Option<String>,
    vector_query: Option<Vec<f32>>,
    graph_seed: Option<String>,
    temporal_as_of: Option<u64>,
    freshness: &'a FreshnessMode,
    fallback_candidate_limit: Option<usize>,
}

struct QueryAccessPaths {
    paths: Vec<MemoryAccessPath>,
    timings: Vec<AccessPathTiming>,
    lexical_cache_hits: usize,
    lexical_cache_misses: usize,
    lexical_indexed_documents: usize,
    lexical_scored_documents: usize,
    vector_exact_fallback_used: bool,
}

struct VectorIndexCandidateReport {
    candidates: Vec<Candidate>,
    exact_fallback_used: bool,
}

struct LexicalQueryReport {
    cache_hit: bool,
    cache_miss: bool,
    indexed_documents: usize,
    score_report: tracedb_text::TextScoreReport,
}

fn query_access_paths(db: &TraceDb, input: QueryAccessInput<'_>) -> Result<QueryAccessPaths> {
    let QueryAccessInput {
        schema,
        visible,
        sealed_records,
        tenant_id,
        scalar_eq_key,
        text_field,
        text,
        vector_field,
        vector_query,
        graph_seed,
        temporal_as_of,
        freshness,
        fallback_candidate_limit,
    } = input;
    let mut paths = Vec::new();
    let mut timings = Vec::new();
    let fallback_candidate_limit = fallback_candidate_limit.unwrap_or(usize::MAX);
    let path_started = Instant::now();
    paths.push(MemoryAccessPath {
        descriptor: planner_descriptor("PolicyPath", None),
        candidates: visible
            .iter()
            .take(fallback_candidate_limit)
            .map(|record| {
                planner_candidate(record, "PolicyPath", 1.0, |score| ScoreComponents {
                    relational: Some(score),
                    final_score: score,
                    ..ScoreComponents::default()
                })
            })
            .collect(),
    });
    timings.push(access_path_timing("PolicyPath", elapsed_ms(path_started)));
    let path_started = Instant::now();
    paths.push(MemoryAccessPath {
        descriptor: planner_descriptor("RelationalPath", None),
        candidates: visible
            .iter()
            .take(fallback_candidate_limit)
            .map(|record| {
                planner_candidate(record, "RelationalPath", 0.75, |score| ScoreComponents {
                    relational: Some(score),
                    final_score: score,
                    ..ScoreComponents::default()
                })
            })
            .collect(),
    });
    timings.push(access_path_timing(
        "RelationalPath",
        elapsed_ms(path_started),
    ));
    let path_started = Instant::now();
    paths.push(MemoryAccessPath {
        descriptor: planner_descriptor("HotOverlayPath", None),
        candidates: visible
            .iter()
            .take(fallback_candidate_limit)
            .map(|record| {
                planner_candidate(record, "HotOverlayPath", 1.0, |score| ScoreComponents {
                    relational: Some(score),
                    final_score: score,
                    ..ScoreComponents::default()
                })
            })
            .collect(),
    });
    timings.push(access_path_timing(
        "HotOverlayPath",
        elapsed_ms(path_started),
    ));

    let path_started = Instant::now();
    let mut lexical_candidates = Vec::new();
    let mut lexical_cache_hits = 0;
    let mut lexical_cache_misses = 0;
    let mut lexical_indexed_documents = 0;
    let mut lexical_scored_documents = 0;
    if let Some(text) = text {
        let records_by_id = visible
            .iter()
            .map(|record| (record.header.record_id.as_str(), record))
            .collect::<BTreeMap<_, _>>();
        let sealed_by_id = sealed_records
            .iter()
            .map(|record| (record.record_id.as_str(), record))
            .collect::<BTreeMap<_, _>>();
        let lexical_report = db.score_prepared_lexical_corpus(
            schema,
            tenant_id,
            &scalar_eq_key,
            visible,
            sealed_records,
            text_field.as_deref(),
            &text,
        );
        lexical_cache_hits = usize::from(lexical_report.cache_hit);
        lexical_cache_misses = usize::from(lexical_report.cache_miss);
        lexical_indexed_documents = lexical_report.indexed_documents;
        lexical_scored_documents = lexical_report.score_report.scored_documents;
        let mut scored = lexical_report.score_report.scores;
        scored.sort_by(|left, right| score_order(left.1, right.1));
        lexical_candidates = scored
            .into_iter()
            .filter_map(|(record_id, score)| {
                records_by_id
                    .get(record_id.as_str())
                    .map(|record| {
                        planner_candidate(record, "LexicalPath", score, |score| ScoreComponents {
                            lexical: Some(score),
                            final_score: score,
                            ..ScoreComponents::default()
                        })
                    })
                    .or_else(|| {
                        sealed_by_id.get(record_id.as_str()).map(|record| {
                            segment_candidate(record, "LexicalPath", score, |score| {
                                ScoreComponents {
                                    lexical: Some(score),
                                    final_score: score,
                                    ..ScoreComponents::default()
                                }
                            })
                        })
                    })
            })
            .collect();
    }
    paths.push(MemoryAccessPath {
        descriptor: planner_descriptor("LexicalPath", Some("tracedb-text")),
        candidates: lexical_candidates,
    });
    timings.push(access_path_timing("LexicalPath", elapsed_ms(path_started)));

    let path_started = Instant::now();
    let mut vector_candidates = Vec::new();
    let mut vector_exact_fallback_used = false;
    if let Some(vector_query) = vector_query {
        vector_candidates = visible
            .iter()
            .filter_map(|record| {
                vector_score(
                    schema,
                    record,
                    vector_field.as_deref(),
                    &vector_query,
                    freshness,
                )
                .map(|(score, penalty)| {
                    let freshness =
                        vector_feature_freshness(schema, record, vector_field.as_deref());
                    Candidate {
                        record_id: record.header.record_id.clone(),
                        version_id: record.header.version_id.get(),
                        score_components: ScoreComponents {
                            vector: Some(score),
                            freshness_penalty: (penalty > 0.0).then_some(penalty),
                            final_score: score - penalty,
                            ..ScoreComponents::default()
                        },
                        score_upper_bound: Some(1.0),
                        source: "VectorPath".to_string(),
                        freshness,
                        visibility_checked: true,
                    }
                })
            })
            .collect();
        if let Some(index_report) = db.sealed_vector_candidates_from_index_artifacts(
            schema,
            sealed_records,
            vector_field.as_deref(),
            &vector_query,
            fallback_candidate_limit,
        )? {
            vector_exact_fallback_used |= index_report.exact_fallback_used;
            vector_candidates.extend(index_report.candidates);
        } else {
            vector_exact_fallback_used = !sealed_records.is_empty();
            vector_candidates.extend(sealed_records.iter().filter_map(|record| {
                segment_vector_score(schema, record, vector_field.as_deref(), &vector_query).map(
                    |score| {
                        segment_candidate(record, "VectorPath", score, |score| ScoreComponents {
                            vector: Some(score),
                            final_score: score,
                            ..ScoreComponents::default()
                        })
                    },
                )
            }));
        }
        vector_candidates.sort_by(|left, right| {
            score_order(
                left.score_components.final_score,
                right.score_components.final_score,
            )
            .then_with(|| left.record_id.cmp(&right.record_id))
            .then_with(|| left.version_id.cmp(&right.version_id))
        });
    }
    paths.push(MemoryAccessPath {
        descriptor: planner_descriptor("VectorPath", Some("tracedb-vector")),
        candidates: vector_candidates,
    });
    timings.push(access_path_timing("VectorPath", elapsed_ms(path_started)));

    if let Some(seed) = graph_seed {
        let path_started = Instant::now();
        let candidates = visible
            .iter()
            .filter(|record| record_has_graph_seed(record, &seed))
            .map(|record| {
                planner_candidate(record, "GraphPath", 0.6, |score| ScoreComponents {
                    relational: Some(score),
                    final_score: score,
                    ..ScoreComponents::default()
                })
            })
            .collect();
        paths.push(MemoryAccessPath {
            descriptor: planner_descriptor("GraphPath", Some("tracedb-graph")),
            candidates,
        });
        timings.push(access_path_timing("GraphPath", elapsed_ms(path_started)));
    }

    if let Some(as_of) = temporal_as_of {
        let path_started = Instant::now();
        let candidates = visible
            .iter()
            .filter(|record| record_valid_as_of(record, as_of))
            .map(|record| {
                planner_candidate(record, "TemporalPath", 0.65, |score| ScoreComponents {
                    relational: Some(score),
                    final_score: score,
                    ..ScoreComponents::default()
                })
            })
            .collect();
        paths.push(MemoryAccessPath {
            descriptor: planner_descriptor("TemporalPath", Some("tracedb-temporal")),
            candidates,
        });
        timings.push(access_path_timing("TemporalPath", elapsed_ms(path_started)));
    }

    Ok(QueryAccessPaths {
        paths,
        timings,
        lexical_cache_hits,
        lexical_cache_misses,
        lexical_indexed_documents,
        lexical_scored_documents,
        vector_exact_fallback_used,
    })
}

impl TraceDb {
    fn sealed_vector_candidates_from_index_artifacts(
        &self,
        schema: &TableSchema,
        sealed_records: &[SegmentRecord],
        vector_field: Option<&str>,
        query: &[f32],
        limit: usize,
    ) -> Result<Option<VectorIndexCandidateReport>> {
        if sealed_records.is_empty() {
            return Ok(Some(VectorIndexCandidateReport {
                candidates: Vec::new(),
                exact_fallback_used: false,
            }));
        }
        let Some(vector) = selected_vector_column(schema, vector_field) else {
            return Ok(None);
        };
        let eligible = sealed_records
            .iter()
            .map(|record| ((record.record_id.clone(), record.version_id), record))
            .collect::<BTreeMap<_, _>>();
        let allowed = eligible.keys().cloned().collect::<BTreeSet<_>>();
        let eligible_segment_ids = self
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.state == tracedb_core::SegmentState::Published)
            .filter(|segment| {
                segment.table_set.is_empty()
                    || segment.table_set.iter().any(|entry| entry == &schema.name)
            })
            .map(|segment| segment.segment_id.clone())
            .collect::<BTreeSet<_>>();

        let mut used_artifact = false;
        let mut used_exact_fallback = false;
        let mut candidates = Vec::new();
        let mut seen = BTreeSet::new();
        for index in self.manifest.indexes.iter().filter(|index| {
            index.kind == "vector"
                && index.state == IndexState::Ready
                && eligible_segment_ids.contains(&index.segment_id)
        }) {
            let artifact = tracedb_index::read_index_artifact(
                self.dir.join(&index.object_path),
                self.encryption.as_ref(),
            )
            .map_err(|error| {
                TraceDbError::ArtifactCorruption(format!(
                    "index artifact {} failed verification: {error}",
                    index.index_id
                ))
            })?;
            let Some(vector_artifact) = artifact.as_vector() else {
                continue;
            };
            used_artifact = true;
            let report = vector_artifact.search_vector_with_report_filtered(
                &vector.name,
                query,
                limit,
                &allowed,
            );
            used_exact_fallback |= report.exact_fallback_used;
            for score in report.scores {
                let key = (score.record_id.clone(), score.version_id);
                let Some(record) = eligible.get(&key).copied() else {
                    continue;
                };
                if seen.insert(key) {
                    candidates.push(segment_candidate(
                        record,
                        "VectorPath",
                        score.score,
                        |score| ScoreComponents {
                            vector: Some(score),
                            final_score: score,
                            ..ScoreComponents::default()
                        },
                    ));
                }
            }
        }

        if !used_artifact {
            return Ok(None);
        }

        if used_exact_fallback {
            candidates.extend(sealed_records.iter().filter_map(|record| {
                let key = (record.record_id.clone(), record.version_id);
                if seen.contains(&key) {
                    return None;
                }
                segment_vector_score(schema, record, vector_field, query).map(|score| {
                    segment_candidate(record, "VectorPath", score, |score| ScoreComponents {
                        vector: Some(score),
                        final_score: score,
                        ..ScoreComponents::default()
                    })
                })
            }));
        }
        candidates.sort_by(|left, right| {
            score_order(
                left.score_components.final_score,
                right.score_components.final_score,
            )
            .then_with(|| left.record_id.cmp(&right.record_id))
            .then_with(|| left.version_id.cmp(&right.version_id))
        });
        Ok(Some(VectorIndexCandidateReport {
            candidates,
            exact_fallback_used: used_exact_fallback,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn score_prepared_lexical_corpus(
        &self,
        schema: &TableSchema,
        tenant_id: &str,
        scalar_eq_key: &str,
        visible: &[StoredRecord],
        sealed_records: &[SegmentRecord],
        text_field: Option<&str>,
        query: &str,
    ) -> LexicalQueryReport {
        let indexed_documents = visible.len() + sealed_records.len();
        if indexed_documents < MIN_LEXICAL_CACHE_DOCUMENTS {
            let docs = lexical_documents(schema, visible, sealed_records, text_field);
            return LexicalQueryReport {
                cache_hit: false,
                cache_miss: false,
                indexed_documents,
                score_report: tracedb_text::score_corpus_with_stats(query, &docs),
            };
        }

        let key = LexicalCacheKey {
            table: schema.name.clone(),
            tenant_id: tenant_id.to_string(),
            read_epoch: self.manifest.latest_epoch.get(),
            manifest_generation: self.manifest.manifest_generation,
            scalar_eq: scalar_eq_key.to_string(),
            text_columns: selected_text_columns(schema, text_field),
        };

        if let Some(corpus) = self
            .lexical_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
        {
            return LexicalQueryReport {
                cache_hit: true,
                cache_miss: false,
                indexed_documents: corpus.document_count(),
                score_report: corpus.score_query_with_stats(query),
            };
        }

        let (corpus, score_report) =
            tracedb_text::PreparedTextCorpus::from_documents_with_initial_score(
                query,
                &lexical_documents(schema, visible, sealed_records, text_field),
            );
        let indexed_documents = corpus.document_count();
        self.lexical_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, corpus);
        LexicalQueryReport {
            cache_hit: false,
            cache_miss: true,
            indexed_documents,
            score_report,
        }
    }

    fn clear_lexical_cache(&self) {
        self.lexical_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

fn planner_descriptor(
    access_path_id: impl Into<String>,
    module_id: Option<&str>,
) -> PlannerAccessPathDescriptor {
    PlannerAccessPathDescriptor {
        access_path_id: access_path_id.into(),
        module_id: module_id.map(str::to_string),
        policy_aware: true,
    }
}

fn planner_candidate(
    record: &StoredRecord,
    source: &str,
    score_upper_bound: f32,
    score: impl FnOnce(f32) -> ScoreComponents,
) -> Candidate {
    Candidate {
        record_id: record.header.record_id.clone(),
        version_id: record.header.version_id.get(),
        score_components: score(score_upper_bound),
        score_upper_bound: Some(score_upper_bound),
        source: source.to_string(),
        freshness: FeatureFreshness::Ready,
        visibility_checked: true,
    }
}

fn segment_candidate(
    record: &SegmentRecord,
    source: &str,
    score_upper_bound: f32,
    score: impl FnOnce(f32) -> ScoreComponents,
) -> Candidate {
    Candidate {
        record_id: record.record_id.clone(),
        version_id: record.version_id,
        score_components: score(score_upper_bound),
        score_upper_bound: Some(score_upper_bound),
        source: source.to_string(),
        freshness: FeatureFreshness::Ready,
        visibility_checked: true,
    }
}

fn index_record_from_segment(record: &SegmentRecord) -> tracedb_index::IndexRecord {
    tracedb_index::IndexRecord {
        table: record.table.clone(),
        record_id: record.record_id.clone(),
        tenant_id: record.tenant_id.clone(),
        version_id: record.version_id,
        fields: record.fields.clone(),
        text: record.text.clone(),
        vectors: record.vectors.clone(),
    }
}

fn record_output(record: StoredRecord) -> RecordOutput {
    RecordOutput {
        table: record.header.table_id,
        id: record.header.record_id,
        tenant_id: record.header.tenant_id,
        version_id: record.header.version_id.get(),
        fields: record.fields,
    }
}

fn vector_feature_freshness(
    schema: &TableSchema,
    record: &StoredRecord,
    vector_field: Option<&str>,
) -> FeatureFreshness {
    selected_vector_column(schema, vector_field)
        .and_then(|vector| record.features.get(&vector.name))
        .map(|state| match state.status {
            FeatureStatus::Ready => FeatureFreshness::Ready,
            FeatureStatus::Dirty => FeatureFreshness::Dirty,
            FeatureStatus::Pending => FeatureFreshness::Pending,
            FeatureStatus::Failed => FeatureFreshness::Failed,
            FeatureStatus::Missing => FeatureFreshness::Missing,
        })
        .unwrap_or(FeatureFreshness::Missing)
}

fn vector_score(
    schema: &TableSchema,
    record: &StoredRecord,
    vector_field: Option<&str>,
    query: &[f32],
    freshness: &FreshnessMode,
) -> Option<(f32, f32)> {
    let vector = selected_vector_column(schema, vector_field)?;
    let state = record.features.get(&vector.name)?;
    let penalty = match (&state.status, freshness) {
        (FeatureStatus::Ready, _) => 0.0,
        (FeatureStatus::Dirty, FreshnessMode::AllowDirty) => 0.05,
        _ => return None,
    };
    let value = record.fields.get(&vector.name).and_then(value_as_f32_vec)?;
    let score = tracedb_vector::cosine_similarity(query, &value)?;
    Some((score, penalty))
}

fn segment_vector_score(
    schema: &TableSchema,
    record: &SegmentRecord,
    vector_field: Option<&str>,
    query: &[f32],
) -> Option<f32> {
    let vector = selected_vector_column(schema, vector_field)?;
    let value = record.vectors.get(&vector.name)?;
    let score = tracedb_vector::cosine_similarity(query, value)?;
    Some(score)
}

fn validate_text_query_field(
    schema: &TableSchema,
    query: Option<&String>,
    text_field: Option<&str>,
) -> Result<()> {
    if query.is_none() {
        return Ok(());
    }
    let Some(text_field) = text_field else {
        return Ok(());
    };
    if schema
        .text_indexed_columns
        .iter()
        .any(|column| column == text_field)
    {
        return Ok(());
    }
    Err(TraceDbError::InvalidCommand(format!(
        "invalid text query column {text_field}: not in schema text indexed columns"
    )))
}

fn validate_vector_query_dimensions(
    schema: &TableSchema,
    query: Option<&[f32]>,
    vector_field: Option<&str>,
) -> Result<()> {
    let Some(query) = query else {
        return Ok(());
    };
    let Some(vector) = selected_vector_column(schema, vector_field) else {
        if let Some(vector_field) = vector_field {
            return Err(TraceDbError::InvalidCommand(format!(
                "invalid vector query column {vector_field}: not in schema vector columns"
            )));
        }
        return Err(TraceDbError::InvalidSchema(format!(
            "table {} has no vector columns",
            schema.name
        )));
    };
    if query.len() != vector.dimensions {
        return Err(TraceDbError::InvalidVectorDimensions {
            column: vector.name.clone(),
            expected: vector.dimensions,
            actual: query.len(),
        });
    }
    Ok(())
}

fn selected_text_columns(schema: &TableSchema, text_field: Option<&str>) -> Vec<String> {
    text_field
        .map(|field| vec![field.to_string()])
        .unwrap_or_else(|| schema.text_indexed_columns.clone())
}

fn selected_vector_column<'a>(
    schema: &'a TableSchema,
    vector_field: Option<&str>,
) -> Option<&'a VectorColumnSchema> {
    match vector_field {
        Some(field) => schema
            .vector_columns
            .iter()
            .find(|vector| vector.name == field),
        None => schema.vector_columns.first(),
    }
}

fn validate_scalar_eq_predicates(
    schema: &TableSchema,
    scalar_eq: &Map<String, Value>,
) -> Result<()> {
    for column in scalar_eq.keys() {
        if !schema
            .scalar_columns
            .iter()
            .any(|scalar_column| scalar_column == column)
        {
            return Err(TraceDbError::InvalidCommand(format!(
                "invalid scalar predicate column {column}: not in schema scalar columns"
            )));
        }
    }
    Ok(())
}

fn filter_records_by_scalar_eq(
    records: Vec<StoredRecord>,
    scalar_eq: &Map<String, Value>,
) -> Vec<StoredRecord> {
    if scalar_eq.is_empty() {
        return records;
    }
    records
        .into_iter()
        .filter(|record| record_matches_scalar_eq(&record.fields, scalar_eq))
        .collect()
}

fn filter_segment_records_by_scalar_eq(
    records: Vec<SegmentRecord>,
    scalar_eq: &Map<String, Value>,
) -> Vec<SegmentRecord> {
    if scalar_eq.is_empty() {
        return records;
    }
    records
        .into_iter()
        .filter(|record| record_matches_scalar_eq(&record.fields, scalar_eq))
        .collect()
}

fn record_matches_scalar_eq(fields: &impl ScalarFields, scalar_eq: &Map<String, Value>) -> bool {
    scalar_eq
        .iter()
        .all(|(column, expected)| fields.scalar_value(column) == Some(expected))
}

trait ScalarFields {
    fn scalar_value(&self, column: &str) -> Option<&Value>;
}

impl ScalarFields for Map<String, Value> {
    fn scalar_value(&self, column: &str) -> Option<&Value> {
        self.get(column)
    }
}

impl ScalarFields for BTreeMap<String, Value> {
    fn scalar_value(&self, column: &str) -> Option<&Value> {
        self.get(column)
    }
}

fn default_query_actor(tenant_id: &str, policy_epoch: Epoch) -> ActorContext {
    ActorContext::managed_request(
        tenant_id,
        "local",
        "main",
        "legacy-query",
        "query",
        policy_epoch.get(),
        vec!["records:read".to_string()],
    )
}

fn stored_record_visible(
    record: &StoredRecord,
    actor: &ActorContext,
    oracle: &VisibilityOracle,
) -> bool {
    let policy = policy_from_fields(&record.fields, &record.header.tenant_id);
    oracle
        .visible(
            &record.header.record_id,
            record.header.version_id.get(),
            &policy,
            actor,
        )
        .allowed
}

fn segment_record_visible(
    record: &SegmentRecord,
    actor: &ActorContext,
    oracle: &VisibilityOracle,
) -> bool {
    let policy = policy_from_fields(&record.fields, &record.tenant_id);
    oracle
        .visible(&record.record_id, record.version_id, &policy, actor)
        .allowed
}

fn policy_from_fields(fields: &impl ScalarFields, tenant_id: &str) -> Policy {
    fields
        .scalar_value(POLICY_FIELD_NAME)
        .and_then(|value| serde_json::from_value::<Policy>(value.clone()).ok())
        .unwrap_or_else(|| Policy::tenant(tenant_id))
}

fn scalar_filter_predicates(scalar_eq: &Map<String, Value>) -> Vec<String> {
    scalar_eq
        .iter()
        .map(|(column, value)| format!("{column} = {}", scalar_value_label(value)))
        .collect()
}

fn scalar_eq_cache_key(scalar_eq: &Map<String, Value>) -> String {
    let ordered = scalar_eq
        .iter()
        .map(|(column, value)| (column.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&ordered).unwrap_or_else(|_| "{}".to_string())
}

fn scalar_value_label(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn lexical_documents(
    schema: &TableSchema,
    visible: &[StoredRecord],
    sealed_records: &[SegmentRecord],
    text_field: Option<&str>,
) -> Vec<(String, String)> {
    let mut docs = visible
        .iter()
        .map(|record| {
            (
                record.header.record_id.clone(),
                text_body(schema, record, text_field).unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    docs.extend(sealed_records.iter().map(|record| {
        (
            record.record_id.clone(),
            segment_text_body(record, text_field).unwrap_or_default(),
        )
    }));
    docs
}

fn text_body(
    schema: &TableSchema,
    record: &StoredRecord,
    text_field: Option<&str>,
) -> Option<String> {
    let mut parts = Vec::new();
    for column in selected_text_columns(schema, text_field) {
        if let Some(Value::String(value)) = record.fields.get(&column) {
            parts.push(value.clone());
        }
    }
    (!parts.is_empty()).then(|| parts.join(" "))
}

fn segment_text_body(record: &SegmentRecord, text_field: Option<&str>) -> Option<String> {
    let parts = match text_field {
        Some(field) => record
            .text
            .get(field)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>(),
        None => record.text.values().cloned().collect::<Vec<_>>(),
    };
    (!parts.is_empty()).then(|| parts.join(" "))
}

fn record_has_graph_seed(record: &StoredRecord, seed: &str) -> bool {
    record
        .fields
        .get("edges")
        .and_then(Value::as_array)
        .map(|edges| edges.iter().any(|edge| edge.as_str() == Some(seed)))
        .unwrap_or(false)
}

fn record_valid_as_of(record: &StoredRecord, as_of: u64) -> bool {
    let start = record
        .fields
        .get("valid_from")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let end = record
        .fields
        .get("valid_to")
        .and_then(Value::as_u64)
        .unwrap_or(u64::MAX);
    start <= as_of && as_of <= end
}

fn feature_invalidations_for_mutation(
    schema: &TableSchema,
    input: &RecordInput,
) -> Vec<FeatureInvalidation> {
    schema
        .vector_columns
        .iter()
        .filter(|vector| {
            !input.fields.contains_key(&vector.name)
                && vector
                    .source_columns
                    .iter()
                    .any(|source| input.fields.contains_key(source))
        })
        .map(|vector| FeatureInvalidation {
            table: input.table.clone(),
            tenant_id: input.tenant_id.clone(),
            record_id: input.id.clone(),
            feature: vector.name.clone(),
            status: FeatureStatus::Dirty,
        })
        .collect()
}

fn module_events_for_schema(event: &str, schema: &TableSchema) -> Vec<ModuleCommitEvent> {
    let mut events = Vec::new();
    if !schema.text_indexed_columns.is_empty() {
        events.push(ModuleCommitEvent {
            module_id: "tracedb-text".to_string(),
            event: event.to_string(),
        });
    }
    if !schema.vector_columns.is_empty() {
        events.push(ModuleCommitEvent {
            module_id: "tracedb-vector".to_string(),
            event: event.to_string(),
        });
    }
    events
}

fn upsert_schema(schemas: &mut Vec<TableSchema>, schema: TableSchema) {
    if let Some(existing) = schemas
        .iter_mut()
        .find(|existing| existing.name == schema.name)
    {
        *existing = schema;
    } else {
        schemas.push(schema);
    }
}

fn score_order(left: f32, right: f32) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}

fn initialize_layout(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    for subdir in [
        "wal",
        "hot/rows",
        "hot/text",
        "hot/vectors",
        "hot/policy",
        "hot/features",
        "segments",
        "indexes",
        "checkpoints",
        "snapshots",
        "jobs",
    ] {
        fs::create_dir_all(dir.join(subdir))?;
    }
    Ok(())
}

fn write_job_catalog_file(
    dir: &Path,
    catalog: &JobCatalog,
    encryption: Option<&EncryptionContext>,
) -> Result<()> {
    let path = dir.join("jobs/catalog.tjobs");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec(catalog)?;
    let body = match encryption {
        Some(encryption) => encryption.encrypt_artifact("jobs", &body)?,
        None => body,
    };
    let tmp_path = path.with_extension("tjobs.tmp");
    let mut file = File::create(&tmp_path)?;
    file.write_all(&body)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, &path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

struct WriteLock {
    _file: std::fs::File,
}

impl WriteLock {
    fn acquire(dir: &std::path::Path) -> Result<Self> {
        let path = dir.join("engine.write.lock");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(TraceDbError::Io)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            TraceDbError::Io(std::io::Error::new(
                error.kind(),
                format!(
                    "engine write lock contention on {}: {error}",
                    path.display()
                ),
            ))
        })?;
        Ok(Self { _file: file })
    }
}

struct EngineLock {
    file: Arc<std::fs::File>,
}

impl Clone for EngineLock {
    fn clone(&self) -> Self {
        Self {
            file: Arc::clone(&self.file),
        }
    }
}

impl Drop for EngineLock {
    fn drop(&mut self) {
        if Arc::strong_count(&self.file) == 1 {
            let _ = fs2::FileExt::unlock(&*self.file);
        }
    }
}

impl std::fmt::Debug for EngineLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineLock").finish_non_exhaustive()
    }
}

impl EngineLock {
    fn acquire(dir: &std::path::Path) -> Result<Self> {
        let path = dir.join("engine.lock");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(TraceDbError::Io)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|_error| {
            TraceDbError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "Engine directory is already locked by another process. Check {} for the PID.",
                    path.display()
                ),
            ))
        })?;
        file.set_len(0).map_err(TraceDbError::Io)?;
        file.write_all(std::process::id().to_string().as_bytes())
            .map_err(TraceDbError::Io)?;
        file.write_all(b"\n").map_err(TraceDbError::Io)?;
        file.sync_all().map_err(TraceDbError::Io)?;
        Ok(Self {
            file: Arc::new(file),
        })
    }
}

fn read_manifest(path: impl AsRef<Path>) -> Result<TraceDbManifest> {
    let path = path.as_ref();
    match read_manifest_inner(path) {
        Ok(manifest) => Ok(manifest),
        Err(err) if manifest_read_error_can_fallback(&err) => {
            let bak_path = path.with_extension("tdb.bak");
            if bak_path.exists() {
                tracing::warn!(
                    "manifest {} failed validation/read, falling back to {}",
                    path.display(),
                    bak_path.display()
                );
                read_manifest_inner(&bak_path)
            } else {
                Err(err)
            }
        }
        Err(err) => Err(err),
    }
}

fn manifest_read_error_can_fallback(err: &TraceDbError) -> bool {
    let readable_corruption_io = matches!(
        err,
        TraceDbError::Io(io)
            if matches!(
                io.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::InvalidData
                    | std::io::ErrorKind::UnexpectedEof
            )
    );
    matches!(err, TraceDbError::Json(_)) || readable_corruption_io
}

fn read_manifest_inner(path: &Path) -> Result<TraceDbManifest> {
    let mut file = File::open(path)?;
    let mut body = String::new();
    file.read_to_string(&mut body)?;
    let manifest: TraceDbManifest = serde_json::from_str(&body)?;
    let expected = manifest.checksums.manifest_checksum;
    if expected == [0u8; 32] {
        return Err(TraceDbError::ManifestCorruption(
            "missing manifest checksum".to_string(),
        ));
    }
    let actual = compute_manifest_checksum(&manifest)?;
    if actual != expected {
        return Err(TraceDbError::ManifestCorruption(format!(
            "manifest checksum mismatch: expected {expected:?}, got {actual:?}",
        )));
    }
    Ok(manifest)
}

fn write_manifest(path: impl AsRef<Path>, manifest: &mut TraceDbManifest) -> Result<()> {
    manifest.checksums.manifest_checksum = [0u8; 32];
    manifest.checksums.manifest_checksum = compute_manifest_checksum(manifest)?;
    let body = serde_json::to_vec_pretty(manifest)?;
    let path = path.as_ref();
    copy_manifest_backup(path)?;
    let tmp_path = path.with_extension("tdb.tmp");
    let mut file = File::create(&tmp_path)?;
    file.write_all(&body)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn write_manifest_with_timing(
    path: impl AsRef<Path>,
    manifest: &mut TraceDbManifest,
) -> Result<ManifestWriteTiming> {
    let total_started = Instant::now();
    let checksum_started = Instant::now();
    manifest.checksums.manifest_checksum = [0u8; 32];
    manifest.checksums.manifest_checksum = compute_manifest_checksum(manifest)?;
    let checksum_ms = elapsed_ms(checksum_started);
    let serialize_started = Instant::now();
    let body = serde_json::to_vec_pretty(manifest)?;
    let bytes = body.len() as u64;
    let serialize_ms = elapsed_ms(serialize_started);
    let path = path.as_ref();
    copy_manifest_backup(path)?;
    let tmp_path = path.with_extension("tdb.tmp");
    let write_started = Instant::now();
    let mut file = File::create(&tmp_path)?;
    file.write_all(&body)?;
    let write_ms = elapsed_ms(write_started);
    let sync_file_started = Instant::now();
    file.sync_all()?;
    let sync_file_ms = elapsed_ms(sync_file_started);
    drop(file);
    let rename_started = Instant::now();
    fs::rename(&tmp_path, path)?;
    let rename_ms = elapsed_ms(rename_started);
    let sync_dir_started = Instant::now();
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    let sync_dir_ms = elapsed_ms(sync_dir_started);
    Ok(ManifestWriteTiming {
        total_ms: elapsed_ms(total_started),
        bytes,
        checksum_ms,
        serialize_ms,
        write_ms,
        sync_file_ms,
        rename_ms,
        sync_dir_ms,
    })
}

fn copy_manifest_backup(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    if let Err(err) = read_manifest_inner(path) {
        if manifest_read_error_can_fallback(&err) {
            tracing::warn!(
                "not replacing manifest backup because {} is not valid: {}",
                path.display(),
                err
            );
            return Ok(());
        }
        return Err(err);
    }

    let bak_path = path.with_extension("tdb.bak");
    fs::copy(path, &bak_path)?;
    File::open(&bak_path)?.sync_all()?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn checkpoint_relative_path(epoch: Epoch) -> String {
    checkpoint_binary_relative_path(epoch)
}

fn checkpoint_binary_relative_path(epoch: Epoch) -> String {
    format!("checkpoints/checkpoint-{}.tchk", epoch.get())
}

fn checkpoint_json_relative_path(epoch: Epoch) -> String {
    format!("checkpoints/checkpoint-{}.json", epoch.get())
}

fn compute_checkpoint_checksum(checkpoint: &CheckpointFile) -> Result<[u8; 32]> {
    let mut normalized = checkpoint.clone();
    normalized.checksum = [0u8; 32];
    Ok(checksum_bytes(&serde_json::to_vec(&normalized)?))
}

fn write_checkpoint_file(
    dir: &Path,
    epoch: Epoch,
    schemas: Vec<TableSchema>,
    records: Vec<StoredRecord>,
    idempotency_receipts: Vec<IdempotencyReceipt>,
    job_catalog: JobCatalog,
    encryption: Option<&EncryptionContext>,
) -> Result<String> {
    let relative = checkpoint_relative_path(epoch);
    let path = dir.join(&relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = CheckpointPayload {
        format_version: CHECKPOINT_FORMAT_VERSION,
        epoch,
        schemas,
        records,
        idempotency_receipts,
        job_catalog,
    };
    let payload = serde_json::to_vec(&payload)?;
    let payload = match encryption {
        Some(encryption) => encryption.encrypt_artifact("checkpoint", &payload)?,
        None => payload,
    };
    let checksum = checksum_bytes(&payload);
    let mut body = Vec::with_capacity(CHECKPOINT_MAGIC_V3.len() + 32 + payload.len());
    body.extend_from_slice(CHECKPOINT_MAGIC_V3);
    body.extend_from_slice(&checksum);
    body.extend_from_slice(&payload);
    let tmp_path = path.with_extension("tchk.tmp");
    let mut file = File::create(&tmp_path)?;
    file.write_all(&body)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, &path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(relative)
}

fn read_checkpoint_file(
    dir: &Path,
    epoch: Epoch,
    encryption: Option<&EncryptionContext>,
) -> Result<CheckpointFile> {
    let path = dir.join(checkpoint_binary_relative_path(epoch));
    if path.exists() {
        return read_binary_checkpoint_file(&path, encryption);
    }
    read_json_checkpoint_file(&dir.join(checkpoint_json_relative_path(epoch)))
}

fn read_binary_checkpoint_file(
    path: &Path,
    encryption: Option<&EncryptionContext>,
) -> Result<CheckpointFile> {
    let body = fs::read(path).map_err(|err| {
        TraceDbError::ManifestCorruption(format!(
            "failed to read checkpoint file at {}: {err}",
            path.display()
        ))
    })?;
    if body.starts_with(CHECKPOINT_MAGIC_V3) {
        return read_framed_checkpoint_file(path, &body, encryption);
    }
    if body.starts_with(CHECKPOINT_MAGIC_V2) {
        return read_legacy_binary_checkpoint_file(path, &body);
    }
    Err(TraceDbError::ManifestCorruption(format!(
        "checkpoint magic mismatch at {}",
        path.display()
    )))
}

fn read_framed_checkpoint_file(
    path: &Path,
    body: &[u8],
    encryption: Option<&EncryptionContext>,
) -> Result<CheckpointFile> {
    if body.len() <= CHECKPOINT_MAGIC_V3.len() + 32 {
        return Err(TraceDbError::ManifestCorruption(format!(
            "checkpoint payload missing at {}",
            path.display()
        )));
    }
    let expected = body[CHECKPOINT_MAGIC_V3.len()..CHECKPOINT_MAGIC_V3.len() + 32]
        .try_into()
        .map_err(|_| {
            TraceDbError::ManifestCorruption(format!(
                "checkpoint checksum frame is invalid at {}",
                path.display()
            ))
        })?;
    if expected == [0u8; 32] {
        return Err(TraceDbError::ManifestCorruption(format!(
            "missing checkpoint checksum at {}",
            path.display()
        )));
    }
    let payload = &body[CHECKPOINT_MAGIC_V3.len() + 32..];
    let actual = checksum_bytes(payload);
    if actual != expected {
        return Err(TraceDbError::ManifestCorruption(format!(
            "checkpoint checksum mismatch at {}: expected {expected:?}, got {actual:?}",
            path.display()
        )));
    }
    let payload = decrypt_artifact_if_needed(encryption, "checkpoint", payload)?;
    let payload: CheckpointPayload = serde_json::from_slice(&payload).map_err(|err| {
        TraceDbError::ManifestCorruption(format!(
            "failed to parse checkpoint file at {}: {err}",
            path.display()
        ))
    })?;
    if payload.format_version != CHECKPOINT_FORMAT_VERSION {
        return Err(TraceDbError::ManifestCorruption(format!(
            "unsupported checkpoint format version {}",
            payload.format_version
        )));
    }
    Ok(CheckpointFile {
        format_version: payload.format_version,
        epoch: payload.epoch,
        schemas: payload.schemas,
        records: payload.records,
        idempotency_receipts: payload.idempotency_receipts,
        job_catalog: payload.job_catalog,
        checksum: expected,
    })
}

fn read_legacy_binary_checkpoint_file(path: &Path, body: &[u8]) -> Result<CheckpointFile> {
    if body.len() <= CHECKPOINT_MAGIC_V2.len() {
        return Err(TraceDbError::ManifestCorruption(format!(
            "checkpoint payload missing at {}",
            path.display()
        )));
    }
    let checkpoint: CheckpointFile = serde_json::from_slice(&body[CHECKPOINT_MAGIC_V2.len()..])
        .map_err(|err| {
            TraceDbError::ManifestCorruption(format!(
                "failed to parse checkpoint file at {}: {err}",
                path.display()
            ))
        })?;
    verify_checkpoint_file(path, checkpoint, CHECKPOINT_LEGACY_COMPACT_FORMAT_VERSION)
}

fn read_json_checkpoint_file(path: &Path) -> Result<CheckpointFile> {
    let body = fs::read(path).map_err(|err| {
        TraceDbError::ManifestCorruption(format!(
            "failed to read checkpoint file at {}: {err}",
            path.display()
        ))
    })?;
    let checkpoint: CheckpointFile = serde_json::from_slice(&body).map_err(|err| {
        TraceDbError::ManifestCorruption(format!(
            "failed to parse checkpoint file at {}: {err}",
            path.display()
        ))
    })?;
    verify_checkpoint_file(path, checkpoint, CHECKPOINT_LEGACY_JSON_FORMAT_VERSION)
}

fn verify_checkpoint_file(
    path: &Path,
    checkpoint: CheckpointFile,
    expected_format_version: u32,
) -> Result<CheckpointFile> {
    if checkpoint.format_version != expected_format_version {
        return Err(TraceDbError::ManifestCorruption(format!(
            "unsupported checkpoint format version {}",
            checkpoint.format_version
        )));
    }
    let expected = checkpoint.checksum;
    if expected == [0u8; 32] {
        return Err(TraceDbError::ManifestCorruption(format!(
            "missing checkpoint checksum at {}",
            path.display()
        )));
    }
    let actual = compute_checkpoint_checksum(&checkpoint)?;
    if actual != expected {
        return Err(TraceDbError::ManifestCorruption(format!(
            "checkpoint checksum mismatch at {}: expected {expected:?}, got {actual:?}",
            path.display(),
        )));
    }
    Ok(checkpoint)
}

fn truncate_active_wal(dir: &Path) -> Result<()> {
    let path = dir.join("wal").join("000001.twal");
    let file = File::create(&path)?;
    file.sync_all()?;
    drop(file);
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn backup_dir(source: &Path, target: &Path) -> Result<()> {
    validate_copy_paths(source, target)?;
    if target.exists() {
        fs::remove_dir_all(target)?;
    }
    fs::create_dir_all(target)?;
    for entry in [
        "manifest.tdb",
        "engine.lock",
        "wal",
        "hot",
        "segments",
        "indexes",
        "checkpoints",
        "snapshots",
        "jobs",
    ] {
        let from = source.join(entry);
        let to = target.join(entry);
        if from.is_file() {
            fs::copy(from, to)?;
        } else if from.is_dir() {
            copy_dir(&from, &to)?;
        }
    }
    Ok(())
}

fn snapshot_dir(source: &Path, target: &Path) -> Result<()> {
    validate_admin_snapshot_path("snapshot target", target)?;
    let canonical_source = source.canonicalize()?;
    if target.exists() && target.canonicalize()? == canonical_source {
        return Err(TraceDbError::InvalidCommand(
            "source and target directories must differ".to_string(),
        ));
    }
    if target.exists() {
        fs::remove_dir_all(target)?;
    }
    fs::create_dir_all(target)?;
    for entry in [
        "manifest.tdb",
        "engine.lock",
        "wal",
        "hot",
        "segments",
        "indexes",
        "checkpoints",
        "jobs",
    ] {
        let from = source.join(entry);
        let to = target.join(entry);
        if from.is_file() {
            fs::copy(from, to)?;
        } else if from.is_dir() {
            copy_dir(&from, &to)?;
        }
    }
    Ok(())
}

fn restore_dir(source: &Path, target: &Path) -> Result<()> {
    validate_admin_snapshot_path("restore source", source)?;
    validate_admin_snapshot_path("restore target", target)?;
    validate_copy_paths(source, target)?;
    if target.exists() {
        fs::remove_dir_all(target)?;
    }
    copy_dir(source, target)
}

fn validate_admin_snapshot_path(label: &str, path: &Path) -> Result<()> {
    let Some(root) = admin_snapshot_root()? else {
        return Ok(());
    };
    let resolved = resolve_existing_or_parent(path)?;
    if !resolved.starts_with(&root) {
        return Err(TraceDbError::InvalidCommand(format!(
            "{label} must be under TRACEDB_ADMIN_SNAPSHOT_ROOT ({})",
            root.display()
        )));
    }
    Ok(())
}

fn admin_snapshot_root() -> Result<Option<PathBuf>> {
    let Some(value) = std::env::var("TRACEDB_ADMIN_SNAPSHOT_ROOT")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let root = PathBuf::from(value);
    if !root.is_absolute() {
        return Err(TraceDbError::InvalidCommand(
            "TRACEDB_ADMIN_SNAPSHOT_ROOT must be an absolute path".to_string(),
        ));
    }
    Ok(Some(root.canonicalize()?))
}

fn resolve_existing_or_parent(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Ok(path.canonicalize()?);
    }
    let parent = path.parent().ok_or_else(|| {
        TraceDbError::InvalidCommand(format!("path {} has no parent directory", path.display()))
    })?;
    let canonical_parent = parent.canonicalize()?;
    Ok(canonical_parent.join(path.file_name().ok_or_else(|| {
        TraceDbError::InvalidCommand(format!("path {} has no final component", path.display()))
    })?))
}

fn vacuum_artifact_dir(dir: &Path, referenced_files: &BTreeSet<String>) -> Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut removed = 0usize;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().to_string();
        let extension = path.extension().and_then(|value| value.to_str());
        let is_artifact = matches!(extension, Some("tseg" | "tidx"));
        let is_staged = extension == Some("tmp");
        if is_staged || (is_artifact && !referenced_files.contains(&file_name)) {
            fs::remove_file(&path)?;
            removed += 1;
        }
    }
    File::open(dir)?.sync_all()?;
    Ok(removed)
}

fn validate_copy_paths(source: &Path, target: &Path) -> Result<()> {
    let canonical_source = source.canonicalize()?;
    if target.exists() {
        let canonical_target = target.canonicalize()?;
        if canonical_source == canonical_target {
            return Err(TraceDbError::InvalidCommand(
                "source and target directories must differ".to_string(),
            ));
        }
        if canonical_target.starts_with(&canonical_source) {
            return Err(TraceDbError::InvalidCommand(
                "target directory cannot be inside source directory".to_string(),
            ));
        }
    } else if let Some(parent) = target.parent() {
        let canonical_parent = parent
            .canonicalize()
            .unwrap_or_else(|_| parent.to_path_buf());
        if canonical_parent.starts_with(&canonical_source) {
            return Err(TraceDbError::InvalidCommand(
                "target directory cannot be inside source directory".to_string(),
            ));
        }
    }
    Ok(())
}

fn copy_dir(source: &Path, target: &Path) -> Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        if from.is_dir() {
            copy_dir(&from, &to)?;
        } else {
            fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

pub fn schema_from_json(value: Value) -> Result<TableSchema> {
    Ok(serde_json::from_value(value)?)
}

pub fn record_from_json(value: Value) -> Result<RecordInput> {
    Ok(serde_json::from_value(value)?)
}

pub fn query_from_json(value: Value) -> Result<HybridQuery> {
    Ok(serde_json::from_value(value)?)
}

/// Parses TraceDB's native line-oriented TraceQL v0 query form, plus the
/// bounded SQL-ish `SELECT` adapter form, into the shared `HybridQuery` model.
/// The SQL-ish adapter is intentionally not SQL or PostgreSQL compatibility.
pub fn traceql_query_from_str(input: &str) -> Result<HybridQuery> {
    let input = input.trim();
    if starts_with_sqlish_select(input) {
        return traceql_sqlish_select_from_str(input);
    }

    let mut table = None;
    let mut tenant_id = None;
    let mut text_field = None;
    let mut text = None;
    let mut vector_field = None;
    let mut vector = None;
    let mut scalar_eq = Map::new();
    let mut seen_scalar_eq = BTreeSet::new();
    let mut top_k = 10;
    let mut top_k_seen = false;
    let mut freshness = FreshnessMode::Strict;
    let mut freshness_seen = false;
    let mut explain = false;
    let mut explain_seen = false;

    for statement in split_traceql_statements(input)? {
        let line_number = statement.line_number;
        let (directive, body) = split_traceql_directive(&statement.text);
        match directive.to_ascii_uppercase().as_str() {
            "FROM" => {
                set_traceql_once(
                    &mut table,
                    "FROM",
                    parse_traceql_single_argument("FROM", body, line_number)?,
                    line_number,
                )?;
            }
            "TENANT" => {
                set_traceql_once(
                    &mut tenant_id,
                    "TENANT",
                    parse_traceql_single_argument("TENANT", body, line_number)?,
                    line_number,
                )?;
            }
            "WHERE" => {
                let (field, value) = parse_traceql_where(body, line_number)?;
                if !seen_scalar_eq.insert(field.clone()) {
                    return Err(invalid_traceql(
                        line_number,
                        format!("WHERE field {field:?} cannot be specified more than once"),
                    ));
                }
                scalar_eq.insert(field, parse_traceql_value(value, line_number)?);
            }
            "MATCH" => {
                let (column, value) = parse_traceql_column_value("MATCH", body, line_number)?;
                set_traceql_once(
                    &mut text_field,
                    "MATCH field",
                    column.to_string(),
                    line_number,
                )?;
                text = Some(parse_traceql_string_value(value, line_number)?);
            }
            "NEAR" => {
                let (column, value) = parse_traceql_column_value("NEAR", body, line_number)?;
                set_traceql_once(
                    &mut vector_field,
                    "NEAR field",
                    column.to_string(),
                    line_number,
                )?;
                let parsed_vector =
                    serde_json::from_str::<Vec<f32>>(value.trim()).map_err(|err| {
                        invalid_traceql(
                            line_number,
                            format!("NEAR vector must be a JSON number array: {err}"),
                        )
                    })?;
                if parsed_vector.is_empty() {
                    return Err(invalid_traceql(line_number, "NEAR vector cannot be empty"));
                }
                vector = Some(parsed_vector);
            }
            "FRESHNESS" => {
                if freshness_seen {
                    return Err(invalid_traceql(
                        line_number,
                        "FRESHNESS cannot be specified more than once",
                    ));
                }
                freshness_seen = true;
                let value = parse_traceql_single_argument("FRESHNESS", body, line_number)?;
                freshness = match value.to_ascii_uppercase().as_str() {
                    "STRICT" => FreshnessMode::Strict,
                    "LAZY" => FreshnessMode::Lazy,
                    "ALLOW_DIRTY" | "ALLOWDIRTY" | "ALLOW-DIRTY" => FreshnessMode::AllowDirty,
                    _ => {
                        return Err(invalid_traceql(
                            line_number,
                            "FRESHNESS must be STRICT, LAZY, or ALLOW_DIRTY",
                        ));
                    }
                };
            }
            "LIMIT" => {
                if top_k_seen {
                    return Err(invalid_traceql(
                        line_number,
                        "LIMIT cannot be specified more than once",
                    ));
                }
                top_k_seen = true;
                let value = parse_traceql_single_argument("LIMIT", body, line_number)?;
                top_k = value.parse::<usize>().map_err(|err| {
                    invalid_traceql(
                        line_number,
                        format!("LIMIT must be a positive integer: {err}"),
                    )
                })?;
                if top_k == 0 {
                    return Err(invalid_traceql(
                        line_number,
                        "LIMIT must be greater than zero",
                    ));
                }
            }
            "EXPLAIN" => {
                if explain_seen {
                    return Err(invalid_traceql(
                        line_number,
                        "EXPLAIN cannot be specified more than once",
                    ));
                }
                explain_seen = true;
                if !body.trim().is_empty() {
                    return Err(invalid_traceql(
                        line_number,
                        "EXPLAIN does not accept arguments",
                    ));
                }
                explain = true;
            }
            _ => {
                return Err(invalid_traceql(
                    line_number,
                    format!("unknown directive {directive:?}"),
                ));
            }
        }
    }

    Ok(HybridQuery {
        table: table.ok_or_else(|| invalid_traceql(0, "FROM is required"))?,
        tenant_id: tenant_id.ok_or_else(|| invalid_traceql(0, "TENANT is required"))?,
        cursor: None,
        text_field,
        text,
        vector_field,
        vector,
        scalar_eq,
        graph_seed: None,
        temporal_as_of: None,
        top_k,
        freshness,
        explain,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TraceQlStatement {
    line_number: usize,
    text: String,
}

fn split_traceql_statements(input: &str) -> Result<Vec<TraceQlStatement>> {
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut line_number = 1usize;
    let mut statement_line = 1usize;
    let mut paren_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            if ch == '\n' {
                line_number += 1;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '(' => paren_depth += 1,
            ')' => {
                paren_depth = paren_depth.checked_sub(1).ok_or_else(|| {
                    invalid_traceql(line_number, "unexpected closing parenthesis")
                })?;
            }
            '{' => brace_depth += 1,
            '}' => {
                brace_depth = brace_depth
                    .checked_sub(1)
                    .ok_or_else(|| invalid_traceql(line_number, "unexpected closing brace"))?;
            }
            '[' => bracket_depth += 1,
            ']' => {
                bracket_depth = bracket_depth
                    .checked_sub(1)
                    .ok_or_else(|| invalid_traceql(line_number, "unexpected closing bracket"))?;
            }
            '\n' if paren_depth == 0 && brace_depth == 0 && bracket_depth == 0 => {
                push_traceql_statement(&mut statements, &input[start..index], statement_line);
                line_number += 1;
                start = index + ch.len_utf8();
                statement_line = line_number;
            }
            '\n' => line_number += 1,
            _ => {}
        }
    }

    if in_string {
        return Err(invalid_traceql(
            statement_line,
            "unterminated string literal in statement",
        ));
    }
    if paren_depth > 0 {
        return Err(invalid_traceql(
            statement_line,
            "unterminated opening parenthesis in statement",
        ));
    }
    if brace_depth > 0 {
        return Err(invalid_traceql(
            statement_line,
            "unterminated opening brace in statement",
        ));
    }
    if bracket_depth > 0 {
        return Err(invalid_traceql(
            statement_line,
            "unterminated opening bracket in statement",
        ));
    }

    push_traceql_statement(&mut statements, &input[start..], statement_line);
    Ok(statements)
}

fn push_traceql_statement(statements: &mut Vec<TraceQlStatement>, raw: &str, line_number: usize) {
    let text = raw.trim();
    if text.is_empty() || text.starts_with('#') {
        return;
    }
    statements.push(TraceQlStatement {
        line_number,
        text: text.to_string(),
    });
}

/// Parses TraceDB's bounded GraphQL adapter query form into `HybridQuery`.
/// This is a compiler primitive only, not a resolver runtime or GraphQL server.
pub fn graphql_query_from_str(input: &str) -> Result<HybridQuery> {
    let input = strip_graphql_comments(input);
    let body = graphql_operation_body(&input)?;
    let (table, arguments) = graphql_root_selection(body)?;
    let argument_pairs = split_graphql_top_level(arguments, ',')?;

    let mut tenant_id = None;
    let mut text_field = None;
    let mut text = None;
    let mut vector_field = None;
    let mut vector = None;
    let mut scalar_eq = Map::new();
    let mut top_k = 10;
    let mut freshness = FreshnessMode::Strict;
    let mut explain = false;
    let mut seen = BTreeSet::new();

    for argument in argument_pairs {
        let argument = argument.trim();
        if argument.is_empty() {
            continue;
        }
        let (name, value) = split_graphql_name_value(argument)?;
        let canonical_name = match name {
            "tenant_id" | "tenant" => "tenant_id",
            "where" | "filter" => "where",
            "match" | "text" => "match",
            "near" | "vector" => "near",
            other => other,
        };
        if !seen.insert(canonical_name.to_string()) {
            return Err(invalid_graphql_adapter(format!(
                "argument {canonical_name:?} cannot be specified more than once"
            )));
        }
        match name {
            "tenant_id" | "tenant" => {
                tenant_id = Some(parse_graphql_string(value, "tenant_id")?);
            }
            "where" | "filter" => {
                let predicates = parse_graphql_scalar_object(value, "where")?;
                for (key, value) in predicates {
                    if scalar_eq.insert(key.clone(), value).is_some() {
                        return Err(invalid_graphql_adapter(format!(
                            "where field {key:?} cannot be specified more than once"
                        )));
                    }
                }
            }
            "match" | "text" => {
                text = Some(parse_graphql_string(value, name)?);
            }
            "match_field" | "text_field" => {
                text_field = Some(parse_graphql_string(value, name)?);
            }
            "near" | "vector" => {
                vector = Some(parse_graphql_vector(value, name)?);
            }
            "near_field" | "vector_field" => {
                vector_field = Some(parse_graphql_string(value, name)?);
            }
            "limit" => {
                top_k = parse_graphql_limit(value)?;
            }
            "freshness" => {
                freshness = parse_graphql_freshness(value)?;
            }
            "explain" => {
                explain = parse_graphql_bool(value, "explain")?;
            }
            _ => {
                return Err(invalid_graphql_adapter(format!(
                    "unknown root argument {name:?}"
                )));
            }
        }
    }

    Ok(HybridQuery {
        table: table.to_string(),
        tenant_id: tenant_id.ok_or_else(|| invalid_graphql_adapter("tenant_id is required"))?,
        cursor: None,
        text_field,
        text,
        vector_field,
        vector,
        scalar_eq,
        graph_seed: None,
        temporal_as_of: None,
        top_k,
        freshness,
        explain,
    })
}

fn strip_graphql_comments(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut in_comment = false;

    for ch in input.chars() {
        if in_comment {
            if ch == '\n' || ch == '\r' {
                in_comment = false;
                output.push(ch);
            }
            continue;
        }
        if in_string {
            output.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => {
                in_string = true;
                output.push(ch);
            }
            '#' => in_comment = true,
            _ => output.push(ch),
        }
    }
    output
}

/// Generates a bounded GraphQL adapter SDL view from TraceDB table schemas.
/// This describes the adapter query shape only; execution still returns the
/// canonical TraceDB `QueryResponse` envelope from `POST /v1/graphql`.
pub fn graphql_schema_sdl_from_tables(schemas: &[TableSchema]) -> Result<String> {
    let mut output = String::new();
    output.push_str("scalar TraceDBJSON\n\n");
    output.push_str("enum TraceDBFreshness {\n  STRICT\n  LAZY\n  ALLOW_DIRTY\n}\n\n");
    output.push_str("type TraceDBScore {\n");
    output.push_str("  vector: Float\n");
    output.push_str("  lexical: Float\n");
    output.push_str("  relational: Float\n");
    output.push_str("  freshness_penalty: Float\n");
    output.push_str("  final_score: Float!\n");
    output.push_str("}\n\n");

    output.push_str("type Query {\n");
    if schemas.is_empty() {
        output.push_str("  _empty: Boolean!\n");
    }
    for schema in schemas {
        validate_graphql_schema_identifier(&schema.name, "table name")?;
        let type_prefix = graphql_table_type_prefix(&schema.name);
        output.push_str(&format!("  {}(\n", schema.name));
        output.push_str("    tenant_id: ID!\n");
        output.push_str("    tenant: ID\n");
        output.push_str(&format!("    where: {type_prefix}Where\n"));
        output.push_str(&format!("    filter: {type_prefix}Where\n"));
        output.push_str("    match: String\n");
        output.push_str("    match_field: String\n");
        output.push_str("    text: String\n");
        output.push_str("    text_field: String\n");
        output.push_str("    near: [Float!]\n");
        output.push_str("    near_field: String\n");
        output.push_str("    vector: [Float!]\n");
        output.push_str("    vector_field: String\n");
        output.push_str("    limit: Int\n");
        output.push_str("    freshness: TraceDBFreshness\n");
        output.push_str("    explain: Boolean\n");
        output.push_str(&format!("  ): [{type_prefix}Row!]!\n"));
    }
    output.push_str("}\n");

    for schema in schemas {
        let type_prefix = graphql_table_type_prefix(&schema.name);
        output.push('\n');
        output.push_str(&format!("input {type_prefix}Where {{\n"));
        let mut where_fields = BTreeSet::new();
        for field in &schema.scalar_columns {
            validate_graphql_schema_identifier(field, "scalar column")?;
            where_fields.insert(field.as_str());
        }
        if where_fields.is_empty() {
            output.push_str("  _empty: Boolean\n");
        } else {
            for field in where_fields {
                output.push_str(&format!("  {field}: TraceDBJSON\n"));
            }
        }
        output.push_str("}\n\n");
        output.push_str(&format!("type {type_prefix}Row {{\n"));
        output.push_str("  record_id: ID!\n");
        output.push_str("  tenant_id: ID!\n");
        output.push_str("  version_id: Int!\n");
        output.push_str("  fields: TraceDBJSON!\n");
        output.push_str("  score: TraceDBScore!\n");
        let mut row_fields = BTreeSet::new();
        push_graphql_row_field(
            &mut output,
            &mut row_fields,
            &schema.primary_id_column,
            "ID",
        )?;
        push_graphql_row_field(&mut output, &mut row_fields, &schema.tenant_id_column, "ID")?;
        for field in &schema.scalar_columns {
            push_graphql_row_field(&mut output, &mut row_fields, field, "TraceDBJSON")?;
        }
        for field in &schema.text_indexed_columns {
            push_graphql_row_field(&mut output, &mut row_fields, field, "String")?;
        }
        for vector in &schema.vector_columns {
            push_graphql_row_field(&mut output, &mut row_fields, &vector.name, "[Float!]")?;
        }
        output.push_str("}\n");
    }

    Ok(output)
}

fn validate_graphql_schema_identifier(name: &str, context: &str) -> Result<()> {
    let valid = take_graphql_identifier(name).is_some_and(|(_, rest)| rest.trim().is_empty());
    if valid && !name.starts_with("__") {
        return Ok(());
    }
    Err(invalid_graphql_adapter(format!(
        "{context} {name:?} cannot be exported as a GraphQL schema identifier"
    )))
}

fn graphql_table_type_prefix(table: &str) -> String {
    let mut output = String::new();
    let mut capitalize_next = true;
    for ch in table.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            output.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            output.push(ch);
        }
    }
    if output.is_empty() {
        "TraceDBTable".to_string()
    } else {
        output
    }
}

fn push_graphql_row_field(
    output: &mut String,
    seen: &mut BTreeSet<String>,
    name: &str,
    field_type: &str,
) -> Result<()> {
    validate_graphql_schema_identifier(name, "column")?;
    if seen.insert(name.to_string()) {
        output.push_str(&format!("  {name}: {field_type}\n"));
    }
    Ok(())
}

fn graphql_operation_body(input: &str) -> Result<&str> {
    let input = input.trim();
    if input.is_empty() {
        return Err(invalid_graphql_adapter("query cannot be empty"));
    }
    let Some(open_index) = input.find('{') else {
        return Err(invalid_graphql_adapter("query selection set is required"));
    };
    let prefix = input[..open_index].trim();
    if !prefix.is_empty() {
        let mut tokens = prefix.split_whitespace();
        let operation = tokens.next().unwrap_or_default();
        if operation != "query" {
            return Err(invalid_graphql_adapter(
                "only query operations are supported; mutations and subscriptions are not adapters",
            ));
        }
        if prefix.contains('(') {
            return Err(invalid_graphql_adapter(
                "operation variables are not supported by the bounded adapter",
            ));
        }
        if let Some(operation_name) = tokens.next() {
            let valid_name = take_graphql_identifier(operation_name)
                .is_some_and(|(_, rest)| rest.trim().is_empty());
            if !valid_name {
                return Err(invalid_graphql_adapter(
                    "operation name must be a GraphQL identifier",
                ));
            }
        }
        if tokens.next().is_some() {
            return Err(invalid_graphql_adapter(
                "operation directives are not supported by the bounded adapter",
            ));
        }
    }
    let close_index = matching_graphql_delimiter(input, open_index, '{', '}')
        .ok_or_else(|| invalid_graphql_adapter("query selection set is not closed"))?;
    if !input[close_index + 1..].trim().is_empty() {
        return Err(invalid_graphql_adapter(
            "unexpected content after query selection set",
        ));
    }
    Ok(&input[open_index + 1..close_index])
}

fn graphql_root_selection(body: &str) -> Result<(&str, &str)> {
    let body = body.trim();
    if body.is_empty() {
        return Err(invalid_graphql_adapter("one root field is required"));
    }
    if body.starts_with("...") {
        return Err(invalid_graphql_adapter("fragments are not supported"));
    }

    let (table, rest) = take_graphql_identifier(body)
        .ok_or_else(|| invalid_graphql_adapter("root field is required"))?;
    if table.starts_with("__") {
        return Err(invalid_graphql_adapter(
            "introspection fields are not supported",
        ));
    }
    let rest = rest.trim_start();
    if rest.starts_with(':') {
        return Err(invalid_graphql_adapter("root aliases are not supported"));
    }
    if !rest.starts_with('(') {
        return Err(invalid_graphql_adapter("root field arguments are required"));
    }
    let argument_offset = body.len() - rest.len();
    let argument_end = matching_graphql_delimiter(body, argument_offset, '(', ')')
        .ok_or_else(|| invalid_graphql_adapter("root field arguments are not closed"))?;
    let arguments = &body[argument_offset + 1..argument_end];
    let rest = body[argument_end + 1..].trim_start();
    if !rest.starts_with('{') {
        return Err(invalid_graphql_adapter(
            "root field selection set is required",
        ));
    }
    let selection_offset = body.len() - rest.len();
    let selection_end = matching_graphql_delimiter(body, selection_offset, '{', '}')
        .ok_or_else(|| invalid_graphql_adapter("root field selection set is not closed"))?;
    let selection_body = body[selection_offset + 1..selection_end].trim();
    if selection_body.is_empty() {
        return Err(invalid_graphql_adapter(
            "root field selection set cannot be empty",
        ));
    }
    if selection_body.contains("...") {
        return Err(invalid_graphql_adapter("fragments are not supported"));
    }
    if !body[selection_end + 1..].trim().is_empty() {
        return Err(invalid_graphql_adapter(
            "exactly one root field is supported",
        ));
    }
    Ok((table, arguments))
}

fn take_graphql_identifier(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    let mut chars = input.char_indices();
    let (_, first) = chars.next()?;
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return None;
    }
    let mut end = first.len_utf8();
    for (index, ch) in chars {
        if ch == '_' || ch.is_ascii_alphanumeric() {
            end = index + ch.len_utf8();
        } else {
            break;
        }
    }
    Some((&input[..end], &input[end..]))
}

fn split_graphql_top_level(input: &str, separator: char) -> Result<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut paren_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' => paren_depth += 1,
            ')' => {
                paren_depth = paren_depth.checked_sub(1).ok_or_else(|| {
                    invalid_graphql_adapter("unexpected closing parenthesis in argument value")
                })?;
            }
            '{' => brace_depth += 1,
            '}' => {
                brace_depth = brace_depth.checked_sub(1).ok_or_else(|| {
                    invalid_graphql_adapter("unexpected closing brace in argument value")
                })?;
            }
            '[' => bracket_depth += 1,
            ']' => {
                bracket_depth = bracket_depth.checked_sub(1).ok_or_else(|| {
                    invalid_graphql_adapter("unexpected closing bracket in argument value")
                })?;
            }
            _ if ch == separator && paren_depth == 0 && brace_depth == 0 && bracket_depth == 0 => {
                parts.push(input[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    if in_string || paren_depth != 0 || brace_depth != 0 || bracket_depth != 0 {
        return Err(invalid_graphql_adapter("unterminated argument value"));
    }
    parts.push(input[start..].trim());
    Ok(parts)
}

fn split_graphql_name_value(input: &str) -> Result<(&str, &str)> {
    let Some(index) = top_level_graphql_separator(input, ':') else {
        return Err(invalid_graphql_adapter("arguments must use name: value"));
    };
    let name = input[..index].trim();
    let value = input[index + 1..].trim();
    if take_graphql_identifier(name).is_none_or(|(_, rest)| !rest.trim().is_empty()) {
        return Err(invalid_graphql_adapter(
            "argument name must be an identifier",
        ));
    }
    if value.is_empty() {
        return Err(invalid_graphql_adapter(format!(
            "argument {name:?} value cannot be empty"
        )));
    }
    Ok((name, value))
}

fn top_level_graphql_separator(input: &str, separator: char) -> Option<usize> {
    let mut paren_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '{' => brace_depth += 1,
            '}' => brace_depth = brace_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            _ if ch == separator && paren_depth == 0 && brace_depth == 0 && bracket_depth == 0 => {
                return Some(index);
            }
            _ => {}
        }
    }
    None
}

fn matching_graphql_delimiter(
    input: &str,
    open_index: usize,
    open: char,
    close: char,
) -> Option<usize> {
    if !input[open_index..].starts_with(open) {
        return None;
    }
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut in_comment = false;

    for (index, ch) in input[open_index..].char_indices() {
        let index = open_index + index;
        if in_comment {
            if ch == '\n' || ch == '\r' {
                in_comment = false;
            }
            continue;
        }
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
        } else if ch == '#' {
            in_comment = true;
        } else if ch == open {
            depth += 1;
        } else if ch == close {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

fn parse_graphql_scalar_object(input: &str, name: &str) -> Result<Map<String, Value>> {
    let input = input.trim();
    if !input.starts_with('{') {
        return Err(invalid_graphql_adapter(format!("{name} must be an object")));
    }
    let close_index = matching_graphql_delimiter(input, 0, '{', '}')
        .ok_or_else(|| invalid_graphql_adapter(format!("{name} object is not closed")))?;
    if !input[close_index + 1..].trim().is_empty() {
        return Err(invalid_graphql_adapter(format!(
            "unexpected content after {name} object"
        )));
    }
    let mut output = Map::new();
    for entry in split_graphql_top_level(&input[1..close_index], ',')? {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (field, value) = split_graphql_name_value(entry)?;
        if output
            .insert(field.to_string(), parse_graphql_value(value)?)
            .is_some()
        {
            return Err(invalid_graphql_adapter(format!(
                "{name} field {field:?} cannot be specified more than once"
            )));
        }
    }
    Ok(output)
}

fn parse_graphql_string(input: &str, name: &str) -> Result<String> {
    let input = input.trim();
    if !input.starts_with('"') {
        return Err(invalid_graphql_adapter(format!(
            "{name} must be a quoted non-empty string"
        )));
    }
    match serde_json::from_str::<Value>(input).map_err(|error| {
        invalid_graphql_adapter(format!("{name} must be a valid JSON string: {error}"))
    })? {
        Value::String(value) if !value.is_empty() => Ok(value),
        _ => Err(invalid_graphql_adapter(format!(
            "{name} must be a quoted non-empty string"
        ))),
    }
}

fn parse_graphql_vector(input: &str, name: &str) -> Result<Vec<f32>> {
    let value = parse_graphql_value(input)?;
    let vector = value_as_f32_vec(&value)
        .ok_or_else(|| invalid_graphql_adapter(format!("{name} must be a numeric vector array")))?;
    if vector.is_empty() {
        return Err(invalid_graphql_adapter(format!(
            "{name} vector cannot be empty"
        )));
    }
    Ok(vector)
}

fn parse_graphql_limit(input: &str) -> Result<usize> {
    let value = parse_graphql_value(input)?;
    let limit = match value {
        Value::Number(number) => number
            .as_u64()
            .and_then(|value| usize::try_from(value).ok()),
        _ => None,
    }
    .ok_or_else(|| invalid_graphql_adapter("limit must be a positive integer"))?;
    if limit == 0 {
        return Err(invalid_graphql_adapter("limit must be greater than zero"));
    }
    Ok(limit)
}

fn parse_graphql_bool(input: &str, name: &str) -> Result<bool> {
    match parse_graphql_value(input)? {
        Value::Bool(value) => Ok(value),
        _ => Err(invalid_graphql_adapter(format!("{name} must be a boolean"))),
    }
}

fn parse_graphql_freshness(input: &str) -> Result<FreshnessMode> {
    let raw = match parse_graphql_value(input)? {
        Value::String(value) => value,
        _ => {
            return Err(invalid_graphql_adapter(
                "freshness must be STRICT, LAZY, or ALLOW_DIRTY",
            ));
        }
    };
    match raw.to_ascii_uppercase().as_str() {
        "STRICT" => Ok(FreshnessMode::Strict),
        "LAZY" => Ok(FreshnessMode::Lazy),
        "ALLOW_DIRTY" | "ALLOWDIRTY" | "ALLOW-DIRTY" => Ok(FreshnessMode::AllowDirty),
        _ => Err(invalid_graphql_adapter(
            "freshness must be STRICT, LAZY, or ALLOW_DIRTY",
        )),
    }
}

fn parse_graphql_value(input: &str) -> Result<Value> {
    let input = input.trim();
    if input.is_empty() {
        return Err(invalid_graphql_adapter("value cannot be empty"));
    }
    if input.starts_with('{') {
        return Err(invalid_graphql_adapter(
            "nested objects are only supported for where/filter",
        ));
    }
    match serde_json::from_str::<Value>(input) {
        Ok(value) => Ok(value),
        Err(error) => {
            if input.starts_with('"') || input.ends_with('"') {
                Err(invalid_graphql_adapter(format!(
                    "quoted value must be valid JSON: {error}"
                )))
            } else {
                Ok(Value::String(input.to_string()))
            }
        }
    }
}

fn starts_with_sqlish_select(input: &str) -> bool {
    strip_sqlish_keyword_prefix(input, "SELECT").is_some()
        || strip_sqlish_keyword_prefix(input, "EXPLAIN")
            .and_then(|rest| strip_sqlish_keyword_prefix(rest, "SELECT"))
            .is_some()
}

fn traceql_sqlish_select_from_str(input: &str) -> Result<HybridQuery> {
    let mut query = normalize_sqlish_whitespace(input);
    query = query.trim().trim_end_matches(';').trim().to_string();

    reject_unsupported_sqlish_keywords(&query)?;

    let mut explain = false;
    let mut rest = query.as_str();
    if let Some(after_explain) = strip_sqlish_keyword_prefix(rest, "EXPLAIN") {
        explain = true;
        rest = after_explain;
    }
    rest = strip_sqlish_keyword_prefix(rest, "SELECT")
        .ok_or_else(|| invalid_sqlish("SELECT is required"))?;

    rest = rest.trim_start();
    let Some(after_star) = rest.strip_prefix('*') else {
        return Err(invalid_sqlish("only SELECT * is supported"));
    };
    rest = after_star.trim_start();
    rest = strip_sqlish_keyword_prefix(rest, "FROM")
        .ok_or_else(|| invalid_sqlish("FROM is required after SELECT *"))?;

    let (table, after_table) =
        take_sqlish_identifier(rest).ok_or_else(|| invalid_sqlish("FROM requires a table"))?;
    rest = strip_sqlish_keyword_prefix(after_table, "WHERE")
        .ok_or_else(|| invalid_sqlish("WHERE tenant_id = value is required"))?;

    let (where_clause, limit_clause) = if let Some(limit_index) = find_sqlish_keyword(rest, "LIMIT")
    {
        (
            &rest[..limit_index],
            Some(&rest[limit_index + "LIMIT".len()..]),
        )
    } else {
        (rest, None)
    };
    let top_k = if let Some(limit_clause) = limit_clause {
        let value = limit_clause.trim();
        if value.is_empty() {
            return Err(invalid_sqlish("LIMIT requires a positive integer"));
        }
        if value.split_whitespace().count() != 1 {
            return Err(invalid_sqlish("LIMIT accepts exactly one positive integer"));
        }
        let parsed = value
            .parse::<usize>()
            .map_err(|err| invalid_sqlish(format!("LIMIT must be a positive integer: {err}")))?;
        if parsed == 0 {
            return Err(invalid_sqlish("LIMIT must be greater than zero"));
        }
        parsed
    } else {
        10
    };

    let mut tenant_id = None;
    let mut scalar_eq = Map::new();
    for condition in split_sqlish_and_conditions(where_clause) {
        let condition = condition.trim();
        if condition.is_empty() {
            return Err(invalid_sqlish("WHERE contains an empty condition"));
        }
        let (field, raw_value) = condition
            .split_once('=')
            .ok_or_else(|| invalid_sqlish("WHERE conditions must use field = value"))?;
        let field = field.trim();
        if field.is_empty() || field.split_whitespace().count() != 1 {
            return Err(invalid_sqlish("WHERE field must be one identifier"));
        }
        let value = parse_sqlish_value(raw_value.trim())?;
        if field.eq_ignore_ascii_case("tenant_id") || field.eq_ignore_ascii_case("tenant") {
            match value {
                Value::String(value) if !value.is_empty() => {
                    if tenant_id.replace(value).is_some() {
                        return Err(invalid_sqlish(
                            "tenant_id cannot be specified more than once",
                        ));
                    }
                }
                _ => return Err(invalid_sqlish("tenant_id must be a string value")),
            }
        } else {
            if scalar_eq.insert(field.to_string(), value).is_some() {
                return Err(invalid_sqlish(format!(
                    "WHERE field {field:?} cannot be specified more than once"
                )));
            }
        }
    }

    Ok(HybridQuery {
        table: table.to_string(),
        tenant_id: tenant_id.ok_or_else(|| invalid_sqlish("tenant_id is required"))?,
        cursor: None,
        text_field: None,
        text: None,
        vector_field: None,
        vector: None,
        scalar_eq,
        graph_seed: None,
        temporal_as_of: None,
        top_k,
        freshness: FreshnessMode::Strict,
        explain,
    })
}

fn reject_unsupported_sqlish_keywords(input: &str) -> Result<()> {
    for keyword in [
        "JOIN", "GROUP", "ORDER", "HAVING", "UNION", "OR", "INSERT", "UPDATE", "DELETE", "DROP",
        "CREATE", "ALTER",
    ] {
        if find_sqlish_keyword(input, keyword).is_some() {
            return Err(invalid_sqlish(format!(
                "{keyword} is not supported by the bounded SELECT adapter"
            )));
        }
    }
    Ok(())
}

fn normalize_sqlish_whitespace(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_double_quote = false;
    let mut in_single_quote = false;
    let mut escaped = false;
    let mut pending_space = false;

    for ch in input.chars() {
        if in_double_quote {
            if pending_space {
                output.push(' ');
                pending_space = false;
            }
            output.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }
        if in_single_quote {
            if pending_space {
                output.push(' ');
                pending_space = false;
            }
            output.push(ch);
            if ch == '\'' {
                in_single_quote = false;
            }
            continue;
        }
        if ch.is_whitespace() {
            pending_space = !output.is_empty();
            continue;
        }
        if pending_space {
            output.push(' ');
            pending_space = false;
        }
        if ch == '"' {
            in_double_quote = true;
        } else if ch == '\'' {
            in_single_quote = true;
        }
        output.push(ch);
    }

    output
}

fn strip_sqlish_keyword_prefix<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let input = input.trim_start();
    let prefix = input.get(..keyword.len())?;
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &input[keyword.len()..];
    if rest.is_empty() || rest.chars().next().is_some_and(char::is_whitespace) {
        Some(rest.trim_start())
    } else {
        None
    }
}

fn take_sqlish_identifier(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    let end = input.find(char::is_whitespace).unwrap_or(input.len());
    if end == 0 {
        None
    } else {
        Some((&input[..end], &input[end..]))
    }
}

fn find_sqlish_keyword(input: &str, keyword: &str) -> Option<usize> {
    let mut in_double_quote = false;
    let mut in_single_quote = false;
    let mut escaped = false;

    for (index, ch) in input.char_indices() {
        if in_double_quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }
        if in_single_quote {
            if ch == '\'' {
                in_single_quote = false;
            }
            continue;
        }
        if ch == '"' {
            in_double_quote = true;
            continue;
        }
        if ch == '\'' {
            in_single_quote = true;
            continue;
        }

        let Some(candidate) = input[index..].get(..keyword.len()) else {
            continue;
        };
        if !candidate.eq_ignore_ascii_case(keyword) {
            continue;
        }
        let before_is_boundary = input[..index]
            .chars()
            .next_back()
            .is_none_or(|before| !is_sqlish_identifier_char(before));
        let after_is_boundary = input[index + keyword.len()..]
            .chars()
            .next()
            .is_none_or(|after| !is_sqlish_identifier_char(after));
        if before_is_boundary && after_is_boundary {
            return Some(index);
        }
    }

    None
}

fn is_sqlish_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}

fn split_sqlish_and_conditions(where_clause: &str) -> Vec<&str> {
    let mut conditions = Vec::new();
    let mut rest = where_clause;
    while let Some(index) = find_sqlish_keyword(rest, "AND") {
        conditions.push(&rest[..index]);
        rest = &rest[index + "AND".len()..];
    }
    conditions.push(rest);
    conditions
}

fn parse_sqlish_value(raw: &str) -> Result<Value> {
    let value = raw.trim();
    if value.starts_with('\'') || value.ends_with('\'') {
        if !(value.starts_with('\'') && value.ends_with('\'')) || value.len() < 2 {
            return Err(invalid_sqlish("single-quoted value is not closed"));
        }
        return Ok(Value::String(value[1..value.len() - 1].replace("''", "'")));
    }
    parse_traceql_value(value, 1).map_err(|err| match err {
        TraceDbError::InvalidCommand(message) => TraceDbError::InvalidCommand(
            message.replace("invalid TraceQL at line 1", "invalid SQL-ish"),
        ),
        other => other,
    })
}

fn split_traceql_directive(line: &str) -> (&str, &str) {
    if let Some(index) = line.find(char::is_whitespace) {
        (&line[..index], line[index..].trim())
    } else {
        (line, "")
    }
}

fn parse_traceql_single_argument(
    directive: &str,
    body: &str,
    line_number: usize,
) -> Result<String> {
    let value = body.trim();
    if value.is_empty() {
        return Err(invalid_traceql(
            line_number,
            format!("{directive} requires one argument"),
        ));
    }
    if value.split_whitespace().count() != 1 {
        return Err(invalid_traceql(
            line_number,
            format!("{directive} accepts exactly one argument"),
        ));
    }
    Ok(value.to_string())
}

fn set_traceql_once(
    target: &mut Option<String>,
    directive: &str,
    value: String,
    line_number: usize,
) -> Result<()> {
    if target.replace(value).is_some() {
        return Err(invalid_traceql(
            line_number,
            format!("{directive} cannot be specified more than once"),
        ));
    }
    Ok(())
}

fn parse_traceql_where(body: &str, line_number: usize) -> Result<(String, &str)> {
    let (field, value) = body
        .split_once('=')
        .ok_or_else(|| invalid_traceql(line_number, "WHERE must use field = value"))?;
    let field = field.trim();
    if field.is_empty() || field.split_whitespace().count() != 1 {
        return Err(invalid_traceql(
            line_number,
            "WHERE field must be one identifier",
        ));
    }
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_traceql(line_number, "WHERE value cannot be empty"));
    }
    Ok((field.to_string(), value))
}

fn parse_traceql_column_value<'a>(
    directive: &str,
    body: &'a str,
    line_number: usize,
) -> Result<(&'a str, &'a str)> {
    let body = body.trim();
    let Some(index) = body.find(char::is_whitespace) else {
        return Err(invalid_traceql(
            line_number,
            format!("{directive} requires a column and value"),
        ));
    };
    let column = body[..index].trim();
    let value = body[index..].trim();
    if column.is_empty() || value.is_empty() {
        return Err(invalid_traceql(
            line_number,
            format!("{directive} requires a column and value"),
        ));
    }
    Ok((column, value))
}

fn parse_traceql_string_value(raw: &str, line_number: usize) -> Result<String> {
    match parse_traceql_value(raw, line_number)? {
        Value::String(value) => Ok(value),
        _ => Err(invalid_traceql(
            line_number,
            "MATCH value must be a string literal or bare string",
        )),
    }
}

fn parse_traceql_value(raw: &str, line_number: usize) -> Result<Value> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(invalid_traceql(line_number, "value cannot be empty"));
    }
    match serde_json::from_str::<Value>(value) {
        Ok(parsed) => Ok(parsed),
        Err(err) => {
            if value.starts_with('"') || value.ends_with('"') {
                Err(invalid_traceql(
                    line_number,
                    format!("quoted value must be valid JSON: {err}"),
                ))
            } else {
                Ok(Value::String(value.to_string()))
            }
        }
    }
}

fn invalid_traceql(line_number: usize, message: impl Into<String>) -> TraceDbError {
    let message = message.into();
    if line_number == 0 {
        TraceDbError::InvalidCommand(format!("invalid TraceQL: {message}"))
    } else {
        TraceDbError::InvalidCommand(format!("invalid TraceQL at line {line_number}: {message}"))
    }
}

fn invalid_sqlish(message: impl Into<String>) -> TraceDbError {
    TraceDbError::InvalidCommand(format!("invalid SQL-ish: {}", message.into()))
}

fn invalid_graphql_adapter(message: impl Into<String>) -> TraceDbError {
    TraceDbError::InvalidCommand(format!("invalid GraphQL adapter: {}", message.into()))
}

pub fn value_object(fields: &[(&str, Value)]) -> Map<String, Value> {
    fields
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex as TestMutex;
    use tracedb_core::VersionId;
    use tracedb_store::RecordHeader;

    const GOOD_MASTER_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    const OTHER_MASTER_KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
    const GOOD_CURSOR_KEY: &str = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=";
    static CURSOR_ENV_LOCK: TestMutex<()> = TestMutex::new(());

    fn with_cursor_key(test: impl FnOnce()) {
        let _guard = CURSOR_ENV_LOCK.lock().expect("cursor env lock");
        std::env::set_var("TRACEDB_CURSOR_SIGNING_KEY_B64", GOOD_CURSOR_KEY);
        std::env::set_var("TRACEDB_CURSOR_TTL_SECS", "3600");
        test();
        std::env::remove_var("TRACEDB_CURSOR_SIGNING_KEY_B64");
        std::env::remove_var("TRACEDB_CURSOR_TTL_SECS");
    }

    fn tenant_actor(token_identity: &str) -> ActorContext {
        ActorContext::managed_request(
            "tenant-a",
            "local",
            "main",
            token_identity,
            "request-cursor",
            1,
            vec!["records:read".to_string()],
        )
    }

    fn schema() -> TableSchema {
        TableSchema {
            name: "docs".to_string(),
            primary_id_column: "id".to_string(),
            tenant_id_column: "tenant".to_string(),
            scalar_columns: vec!["category".to_string()],
            text_indexed_columns: vec!["body".to_string()],
            vector_columns: vec![VectorColumnSchema {
                name: "embedding".to_string(),
                dimensions: 3,
                source_columns: vec!["body".to_string()],
            }],
        }
    }

    fn record(id: &str, body: &str) -> RecordInput {
        RecordInput {
            table: "docs".to_string(),
            id: id.to_string(),
            tenant_id: "tenant-a".to_string(),
            fields: json!({
                "id": id,
                "tenant": "tenant-a",
                "category": "code",
                "body": body,
                "embedding": [1.0, 0.0, 0.0],
            })
            .as_object()
            .expect("object")
            .clone(),
        }
    }

    fn vector_record(id: &str, body: &str, embedding: [f32; 3]) -> RecordInput {
        RecordInput {
            table: "docs".to_string(),
            id: id.to_string(),
            tenant_id: "tenant-a".to_string(),
            fields: json!({
                "id": id,
                "tenant": "tenant-a",
                "category": "code",
                "body": body,
                "embedding": embedding,
            })
            .as_object()
            .expect("object")
            .clone(),
        }
    }

    fn stored_record(id: &str, body: &str) -> StoredRecord {
        StoredRecord {
            header: RecordHeader {
                record_id: id.to_string(),
                table_id: "docs".to_string(),
                tenant_id: "tenant-a".to_string(),
                schema_version: 1,
                begin_epoch: Epoch::new(1),
                end_epoch: None,
                version_id: VersionId::new(1),
                tombstone: None,
            },
            fields: json!({
                "id": id,
                "tenant": "tenant-a",
                "category": "code",
                "body": body,
            })
            .as_object()
            .expect("object")
            .clone(),
            features: BTreeMap::new(),
        }
    }

    fn fielded_schema() -> TableSchema {
        TableSchema {
            name: "fielded_docs".to_string(),
            primary_id_column: "id".to_string(),
            tenant_id_column: "tenant".to_string(),
            scalar_columns: vec!["category".to_string()],
            text_indexed_columns: vec!["title".to_string(), "body".to_string()],
            vector_columns: vec![
                VectorColumnSchema {
                    name: "title_embedding".to_string(),
                    dimensions: 2,
                    source_columns: vec!["title".to_string()],
                },
                VectorColumnSchema {
                    name: "body_embedding".to_string(),
                    dimensions: 2,
                    source_columns: vec!["body".to_string()],
                },
            ],
        }
    }

    fn fielded_record(
        id: &str,
        title: &str,
        body: &str,
        title_embedding: [f32; 2],
        body_embedding: [f32; 2],
    ) -> RecordInput {
        RecordInput {
            table: "fielded_docs".to_string(),
            id: id.to_string(),
            tenant_id: "tenant-a".to_string(),
            fields: json!({
                "id": id,
                "tenant": "tenant-a",
                "category": "code",
                "title": title,
                "body": body,
                "title_embedding": title_embedding,
                "body_embedding": body_embedding,
            })
            .as_object()
            .expect("object")
            .clone(),
        }
    }

    fn fielded_query() -> HybridQuery {
        HybridQuery {
            table: "fielded_docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            cursor: None,
            text_field: None,
            text: None,
            vector_field: None,
            vector: None,
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        }
    }

    #[test]
    fn traceql_query_string_compiles_to_hybrid_query() {
        let query = traceql_query_from_str(
            r#"
            # Native TraceQL, not SQL.
            FROM docs
            TENANT tenant-a
            WHERE category = "code"
            MATCH body "agent memory"
            NEAR embedding [1.0, 0.0, 0.0]
            FRESHNESS lazy
            LIMIT 20
            EXPLAIN
            "#,
        )
        .expect("traceql query");

        assert_eq!(query.table, "docs");
        assert_eq!(query.tenant_id, "tenant-a");
        assert_eq!(query.scalar_eq.get("category"), Some(&json!("code")));
        assert_eq!(query.text_field.as_deref(), Some("body"));
        assert_eq!(query.text.as_deref(), Some("agent memory"));
        assert_eq!(query.vector_field.as_deref(), Some("embedding"));
        assert_eq!(query.vector, Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(query.freshness, FreshnessMode::Lazy);
        assert_eq!(query.top_k, 20);
        assert!(query.explain);
    }

    #[test]
    fn traceql_query_string_keeps_json_newlines_inside_statement_values() {
        let query = traceql_query_from_str(
            r#"
            FROM docs
            TENANT tenant-a
            WHERE metadata = {
              "kind": "guide",
              "tags": ["api", "traceql"]
            }
            NEAR embedding [
              1.0,
              0.0,
              0.0
            ]
            MATCH body "line one\nline two"
            LIMIT 3
            "#,
        )
        .expect("traceql query with multi-line values");

        assert_eq!(query.scalar_eq["metadata"]["kind"], json!("guide"));
        assert_eq!(query.scalar_eq["metadata"]["tags"][1], json!("traceql"));
        assert_eq!(query.vector, Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(query.text.as_deref(), Some("line one\nline two"));
        assert_eq!(query.top_k, 3);
    }

    #[test]
    fn traceql_query_rejects_unbalanced_statement_delimiters_instead_of_swallowing_lines() {
        let error = traceql_query_from_str(
            r#"
            FROM docs
            TENANT tenant-a
            MATCH body foo(
            LIMIT 3
            "#,
        )
        .expect_err("unbalanced TraceQL statement should be rejected");

        assert!(
            matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("unterminated opening parenthesis")),
            "unexpected TraceQL delimiter error: {error}"
        );
    }

    #[test]
    fn traceql_query_string_defaults_to_strict_limit_ten_without_explain() {
        let query = traceql_query_from_str(
            r#"
            FROM docs
            TENANT tenant-a
            MATCH body "exact freshness defaults"
            "#,
        )
        .expect("traceql query");

        assert_eq!(query.freshness, FreshnessMode::Strict);
        assert_eq!(query.top_k, 10);
        assert!(!query.explain);
    }

    #[test]
    fn traceql_query_string_rejects_unknown_directives() {
        let error = traceql_query_from_str(
            r#"
            FROM docs
            TENANT tenant-a
            DROP TABLE docs
            "#,
        )
        .expect_err("unknown directive");

        assert!(
            matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("invalid TraceQL"))
        );
    }

    #[test]
    fn traceql_query_string_rejects_duplicate_predicates_and_directives() {
        for query in [
            r#"
            FROM docs
            TENANT tenant-a
            WHERE category = "code"
            WHERE category = "notes"
            "#,
            r#"
            FROM docs
            TENANT tenant-a
            FRESHNESS strict
            FRESHNESS lazy
            "#,
            r#"
            FROM docs
            TENANT tenant-a
            LIMIT 2
            LIMIT 3
            "#,
            r#"
            FROM docs
            TENANT tenant-a
            EXPLAIN
            EXPLAIN
            "#,
        ] {
            let error = traceql_query_from_str(query).expect_err("duplicate TraceQL rejected");

            assert!(
                matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("cannot be specified more than once")),
                "unexpected duplicate TraceQL error: {error}"
            );
        }
    }

    #[test]
    fn traceql_query_string_requires_table_and_tenant() {
        let error = traceql_query_from_str(
            r#"
            FROM docs
            MATCH body "missing tenant"
            "#,
        )
        .expect_err("missing tenant");

        assert!(
            matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("TENANT"))
        );
    }

    #[test]
    fn traceql_sqlish_select_compiles_to_hybrid_query() {
        let query = traceql_query_from_str(
            r#"
            EXPLAIN SELECT * FROM docs
            WHERE tenant_id = "tenant-a" AND category = "code"
            LIMIT 7
            "#,
        )
        .expect("sql-ish query");

        assert_eq!(query.table, "docs");
        assert_eq!(query.tenant_id, "tenant-a");
        assert_eq!(query.scalar_eq.get("category"), Some(&json!("code")));
        assert_eq!(query.top_k, 7);
        assert!(query.explain);
        assert!(query.text.is_none());
        assert!(query.vector.is_none());
    }

    #[test]
    fn traceql_sqlish_select_rejects_duplicate_scalar_predicates() {
        let error = traceql_query_from_str(
            r#"
            SELECT * FROM docs
            WHERE tenant_id = "tenant-a" AND category = "code" AND category = "notes"
            "#,
        )
        .expect_err("duplicate scalar predicate should fail");

        assert!(
            matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("category") && message.contains("more than once")),
            "unexpected duplicate SQL-ish error: {error}"
        );
    }

    #[test]
    fn traceql_sqlish_select_rejects_join_compatibility_claims() {
        let error = traceql_query_from_str(
            r#"
            SELECT * FROM docs JOIN users ON docs.user_id = users.id
            WHERE tenant_id = "tenant-a"
            "#,
        )
        .expect_err("join should not be accepted");

        assert!(
            matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("SQL-ish"))
        );
    }

    #[test]
    fn traceql_sqlish_select_rejects_or_instead_of_dropping_predicate() {
        let error = traceql_query_from_str(
            r#"
            SELECT * FROM docs
            WHERE tenant_id = "tenant-a" OR status = "published"
            "#,
        )
        .expect_err("OR should not be accepted by bounded SQL-ish adapter");

        assert!(
            matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("OR") && message.contains("SQL-ish")),
            "unexpected OR SQL-ish error: {error}"
        );
    }

    #[test]
    fn graphql_query_string_compiles_to_hybrid_query() {
        let query = graphql_query_from_str(
            r#"
            query SearchDocs {
              docs(
                tenant_id: "tenant-a",
                where: { category: "code", status: "reviewed" },
                match_field: "body",
                match: "TraceDB",
                near_field: "embedding",
                near: [1.0, 0.0, 0.0],
                limit: 7,
                freshness: ALLOW_DIRTY,
                explain: true
              ) {
                record_id
                fields
                score
              }
            }
            "#,
        )
        .expect("bounded GraphQL query");

        assert_eq!(query.table, "docs");
        assert_eq!(query.tenant_id, "tenant-a");
        assert_eq!(query.scalar_eq.get("category"), Some(&json!("code")));
        assert_eq!(query.scalar_eq.get("status"), Some(&json!("reviewed")));
        assert_eq!(query.text_field.as_deref(), Some("body"));
        assert_eq!(query.text.as_deref(), Some("TraceDB"));
        assert_eq!(query.vector_field.as_deref(), Some("embedding"));
        assert_eq!(query.vector, Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(query.top_k, 7);
        assert_eq!(query.freshness, FreshnessMode::AllowDirty);
        assert!(query.explain);
    }

    #[test]
    fn graphql_query_ignores_comments_with_delimiter_characters() {
        let query = graphql_query_from_str(
            r#"
            query SearchDocs {
              # Comments can contain GraphQL-looking delimiters: { ( [ ] ) }
              docs(
                tenant_id: "tenant-a",
                # The parser should not count these braces: { }
                where: { category: "code" },
                match_field: "body",
                match: "TraceDB",
                limit: 2
              ) {
                record_id
                # More fake delimiters: { } ) ]
              }
            }
            "#,
        )
        .expect("bounded GraphQL query with comments");

        assert_eq!(query.table, "docs");
        assert_eq!(query.tenant_id, "tenant-a");
        assert_eq!(query.scalar_eq.get("category"), Some(&json!("code")));
        assert_eq!(query.text.as_deref(), Some("TraceDB"));
        assert_eq!(query.top_k, 2);
    }

    #[test]
    fn graphql_query_rejects_unquoted_string_arguments() {
        let error = graphql_query_from_str(
            r#"
            query {
              docs(tenant_id: tenant-a, match: TraceDB) {
                record_id
              }
            }
            "#,
        )
        .expect_err("unquoted strings should not be accepted");

        assert!(
            matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("invalid GraphQL adapter")),
            "unexpected GraphQL string error: {error}"
        );
    }

    #[test]
    fn graphql_query_rejects_mutation_resolver_model() {
        let error = graphql_query_from_str(
            r#"
            mutation {
              putDoc(table: "docs", tenant_id: "tenant-a", id: "intro") {
                record_id
              }
            }
            "#,
        )
        .expect_err("mutations should not be accepted by the query adapter");

        assert!(
            matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("invalid GraphQL adapter"))
        );
    }

    #[test]
    fn hybrid_query_text_field_limits_lexical_candidate_stream() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(fielded_schema()).expect("schema");
        db.insert(fielded_record(
            "title-hit",
            "needle in selected title",
            "ordinary body",
            [1.0, 0.0],
            [0.0, 1.0],
        ))
        .expect("insert title-hit");
        db.insert(fielded_record(
            "body-hit",
            "ordinary title",
            "needle in unselected body",
            [0.0, 1.0],
            [1.0, 0.0],
        ))
        .expect("insert body-hit");

        let mut query = fielded_query();
        query.text_field = Some("title".to_string());
        query.text = Some("needle".to_string());

        let output = db.query(query).expect("query");
        let lexical_candidates = output
            .explain
            .planner_candidates
            .iter()
            .filter(|candidate| candidate.source == "LexicalPath")
            .map(|candidate| candidate.record_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(output.explain.text_candidates, 1);
        assert_eq!(lexical_candidates, vec!["title-hit"]);
    }

    #[test]
    fn hybrid_query_vector_field_selects_named_vector_column() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(fielded_schema()).expect("schema");
        db.insert(fielded_record(
            "title-vector-hit",
            "title evidence",
            "body evidence",
            [1.0, 0.0],
            [0.0, 1.0],
        ))
        .expect("insert title-vector-hit");
        db.insert(fielded_record(
            "body-vector-hit",
            "title evidence",
            "body evidence",
            [0.0, 1.0],
            [1.0, 0.0],
        ))
        .expect("insert body-vector-hit");

        let mut query = fielded_query();
        query.vector_field = Some("body_embedding".to_string());
        query.vector = Some(vec![1.0, 0.0]);

        let output = db.query(query).expect("query");
        let vector_candidates = output
            .explain
            .planner_candidates
            .iter()
            .filter(|candidate| candidate.source == "VectorPath")
            .map(|candidate| {
                (
                    candidate.record_id.as_str(),
                    candidate.score_components.vector.unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(output.explain.vector_candidates, 2);
        assert_eq!(vector_candidates[0].0, "body-vector-hit");
        assert!(vector_candidates[0].1 > vector_candidates[1].1);
    }

    #[test]
    fn hybrid_query_rejects_unknown_text_or_vector_field() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(fielded_schema()).expect("schema");

        let mut text_query = fielded_query();
        text_query.text_field = Some("missing_text".to_string());
        text_query.text = Some("needle".to_string());
        let text_error = db.query(text_query).expect_err("unknown text field");
        assert!(
            matches!(text_error, TraceDbError::InvalidCommand(message) if message.contains("invalid text query column"))
        );

        let mut vector_query = fielded_query();
        vector_query.vector_field = Some("missing_vector".to_string());
        vector_query.vector = Some(vec![1.0, 0.0]);
        let vector_error = db.query(vector_query).expect_err("unknown vector field");
        assert!(
            matches!(vector_error, TraceDbError::InvalidCommand(message) if message.contains("invalid vector query column"))
        );
    }

    #[test]
    fn record_scan_paginates_with_stable_cursor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(schema()).expect("schema");
        for id in ["a", "b", "c"] {
            db.put(RecordPutRequest::new(record(id, "cursor scan")))
                .expect("put");
        }

        let first = db
            .scan(RecordScanRequest::new("docs", "tenant-a").limit(2))
            .expect("first page");
        assert_eq!(
            first
                .records
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(first.next_cursor.as_deref(), Some("2"));

        let second = db
            .scan(
                RecordScanRequest::new("docs", "tenant-a")
                    .limit(2)
                    .cursor(first.next_cursor.expect("cursor")),
            )
            .expect("second page");
        assert_eq!(
            second
                .records
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            vec!["c"]
        );
        assert_eq!(second.next_cursor, None);
    }

    #[test]
    fn hybrid_query_paginates_ranked_results_with_cursor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(schema()).expect("schema");
        for id in ["a", "b", "c"] {
            db.put(RecordPutRequest::new(record(id, "cursor query")))
                .expect("put");
        }

        let mut query = HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            cursor: None,
            text_field: Some("body".to_string()),
            text: Some("cursor".to_string()),
            vector_field: None,
            vector: None,
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 2,
            freshness: FreshnessMode::Strict,
            explain: true,
        };
        let first = db.query(query.clone()).expect("first page");
        assert_eq!(first.results.len(), 2);
        assert_eq!(first.next_cursor.as_deref(), Some("2"));

        query.cursor = first.next_cursor;
        let second = db.query(query).expect("second page");
        assert_eq!(second.results.len(), 1);
        assert_eq!(second.next_cursor, None);
    }

    #[test]
    fn signed_scan_cursor_binds_actor_and_snapshot_epoch() {
        with_cursor_key(|| {
            let temp = tempfile::tempdir().expect("tempdir");
            let mut db = TraceDb::open(temp.path()).expect("open");
            db.apply_schema(schema()).expect("schema");
            for id in ["a", "b", "c"] {
                db.put(RecordPutRequest::new(record(id, "signed cursor scan")))
                    .expect("put");
            }
            let actor = tenant_actor("token:cursor-owner");

            let first = db
                .scan_as(&actor, RecordScanRequest::new("docs", "tenant-a").limit(2))
                .expect("first signed page");
            let cursor = first.next_cursor.clone().expect("signed cursor");
            assert!(
                cursor.starts_with("tdbc1."),
                "signed cursor should be opaque: {cursor}"
            );
            db.put(RecordPutRequest::new(record("d", "newer than cursor")))
                .expect("post-cursor put");

            let second = db
                .scan_as(
                    &actor,
                    RecordScanRequest::new("docs", "tenant-a")
                        .limit(2)
                        .cursor(cursor.clone()),
                )
                .expect("second signed page");
            assert_eq!(
                second
                    .records
                    .iter()
                    .map(|record| record.id.as_str())
                    .collect::<Vec<_>>(),
                vec!["c"],
                "cursor should page against the original snapshot epoch"
            );

            let wrong_actor = tenant_actor("token:other");
            let error = db
                .scan_as(
                    &wrong_actor,
                    RecordScanRequest::new("docs", "tenant-a")
                        .limit(2)
                        .cursor(cursor),
                )
                .expect_err("wrong actor should fail signed cursor");
            assert!(
                error.to_string().contains("invalid cursor: actor mismatch"),
                "unexpected wrong-actor cursor error: {error}"
            );
        });
    }

    #[test]
    fn signed_query_cursor_rejects_tamper_and_wrong_query() {
        with_cursor_key(|| {
            let temp = tempfile::tempdir().expect("tempdir");
            let mut db = TraceDb::open(temp.path()).expect("open");
            db.apply_schema(schema()).expect("schema");
            for id in ["a", "b", "c"] {
                db.put(RecordPutRequest::new(record(id, "signed cursor query")))
                    .expect("put");
            }
            let actor = tenant_actor("token:query-owner");
            let mut query = HybridQuery {
                table: "docs".to_string(),
                tenant_id: "tenant-a".to_string(),
                cursor: None,
                text_field: Some("body".to_string()),
                text: Some("signed".to_string()),
                vector_field: None,
                vector: None,
                scalar_eq: Default::default(),
                graph_seed: None,
                temporal_as_of: None,
                top_k: 2,
                freshness: FreshnessMode::Strict,
                explain: true,
            };

            let first = db
                .query_as(&actor, query.clone())
                .expect("first signed query page");
            let cursor = first.next_cursor.clone().expect("signed query cursor");
            assert!(cursor.starts_with("tdbc1."));

            let mut tampered = cursor.clone();
            tampered.push('x');
            query.cursor = Some(tampered);
            let error = db
                .query_as(&actor, query.clone())
                .expect_err("tampered cursor should fail");
            assert!(
                error.to_string().contains("invalid cursor"),
                "unexpected tampered cursor error: {error}"
            );

            query.cursor = Some(cursor);
            query.text = Some("different query".to_string());
            let error = db
                .query_as(&actor, query)
                .expect_err("wrong query should fail cursor");
            assert!(
                error.to_string().contains("invalid cursor: query mismatch"),
                "unexpected wrong-query cursor error: {error}"
            );
        });
    }

    #[test]
    fn actor_aware_query_rejects_tenant_mismatch_below_route_layer() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(schema()).expect("schema");
        db.put(RecordPutRequest::new(record("a", "actor scoped query")))
            .expect("put");
        let actor = tracedb_policy::ActorContext::managed_request(
            "tenant-b",
            "local",
            "main",
            "token:other",
            "request-tenant-mismatch",
            1,
            vec!["records:read".to_string()],
        );
        let query = HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            cursor: None,
            text_field: Some("body".to_string()),
            text: Some("actor".to_string()),
            vector_field: None,
            vector: None,
            scalar_eq: Default::default(),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 2,
            freshness: FreshnessMode::Strict,
            explain: true,
        };

        let error = db
            .query_as(&actor, query)
            .expect_err("actor tenant mismatch should be rejected");

        assert!(
            error
                .to_string()
                .contains("actor tenant tenant-b cannot query tenant tenant-a"),
            "unexpected actor mismatch error: {error}"
        );
    }

    #[test]
    fn query_as_filters_policy_hidden_records_before_candidate_generation() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(schema()).expect("schema");
        db.put(RecordPutRequest::new(record(
            "visible",
            "policy oracle candidate",
        )))
        .expect("put visible");
        let mut hidden = record("hidden", "policy oracle candidate");
        hidden.fields.insert(
            "__tracedb_policy".to_string(),
            serde_json::to_value(
                tracedb_policy::Policy::tenant("tenant-a")
                    .with_visibility(tracedb_policy::VisibilityMode::Hidden),
            )
            .expect("policy json"),
        );
        db.put(RecordPutRequest::new(hidden)).expect("put hidden");

        let actor = tracedb_policy::ActorContext::tenant_user("tenant-a", "user-a");
        let output = db
            .query_as(
                &actor,
                HybridQuery {
                    table: "docs".to_string(),
                    tenant_id: "tenant-a".to_string(),
                    cursor: None,
                    text_field: Some("body".to_string()),
                    text: Some("policy oracle".to_string()),
                    vector_field: None,
                    vector: None,
                    scalar_eq: Default::default(),
                    graph_seed: None,
                    temporal_as_of: None,
                    top_k: 5,
                    freshness: FreshnessMode::Strict,
                    explain: true,
                },
            )
            .expect("query");

        assert_eq!(
            output
                .results
                .iter()
                .map(|row| row.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["visible"]
        );
        assert!(
            output
                .explain
                .planner_candidates
                .iter()
                .all(|candidate| candidate.record_id != "hidden"),
            "hidden policy record reached planner candidates: {:?}",
            output.explain.planner_candidates
        );
    }

    #[test]
    fn graphql_query_rejects_resolver_specific_features() {
        for query in [
            r#"
            query {
              docsAlias: docs(tenant_id: "tenant-a") {
                record_id
              }
            }
            "#,
            r#"
            query {
              docs(tenant_id: "tenant-a") {
                ...DocFields
              }
            }
            "#,
            r#"
            query {
              docs(tenant_id: "tenant-a", tenant: "tenant-b") {
                record_id
              }
            }
            "#,
        ] {
            let error =
                graphql_query_from_str(query).expect_err("resolver-specific GraphQL rejected");

            assert!(
                matches!(&error, TraceDbError::InvalidCommand(message) if message.contains("invalid GraphQL adapter"))
            );
        }
    }

    #[test]
    fn graphql_schema_sdl_is_generated_from_table_schema() {
        let sdl = graphql_schema_sdl_from_tables(&[schema()]).expect("graphql schema sdl");

        assert!(sdl.contains("scalar TraceDBJSON"), "SDL: {sdl}");
        assert!(sdl.contains("enum TraceDBFreshness"), "SDL: {sdl}");
        assert!(sdl.contains("type TraceDBScore"), "SDL: {sdl}");
        assert!(sdl.contains("type Query"), "SDL: {sdl}");
        assert!(sdl.contains("docs("), "SDL: {sdl}");
        assert!(sdl.contains("tenant_id: ID!"), "SDL: {sdl}");
        assert!(sdl.contains("where: DocsWhere"), "SDL: {sdl}");
        assert!(sdl.contains("match_field: String"), "SDL: {sdl}");
        assert!(sdl.contains("near_field: String"), "SDL: {sdl}");
        assert!(sdl.contains("type DocsRow"), "SDL: {sdl}");
        assert!(sdl.contains("record_id: ID!"), "SDL: {sdl}");
        assert!(sdl.contains("category: TraceDBJSON"), "SDL: {sdl}");
        assert!(sdl.contains("body: String"), "SDL: {sdl}");
        assert!(sdl.contains("embedding: [Float!]"), "SDL: {sdl}");
    }

    #[test]
    fn lexical_cache_admits_only_large_prepared_corpora() {
        let temp = tempfile::tempdir().expect("tempdir");
        let db = TraceDb::open(temp.path()).expect("open");
        let schema = schema();
        let small_records = (0..4)
            .map(|idx| stored_record(&format!("small-{idx}"), "agent memory lexical cache"))
            .collect::<Vec<_>>();

        let first_small = db.score_prepared_lexical_corpus(
            &schema,
            "tenant-a",
            "{}",
            &small_records,
            &[],
            None,
            "lexical cache",
        );
        let second_small = db.score_prepared_lexical_corpus(
            &schema,
            "tenant-a",
            "{}",
            &small_records,
            &[],
            None,
            "lexical cache",
        );

        assert!(!first_small.cache_hit);
        assert!(!first_small.cache_miss);
        assert_eq!(first_small.indexed_documents, small_records.len());
        assert!(!second_small.cache_hit);
        assert!(!second_small.cache_miss);

        let large_records = (0..MIN_LEXICAL_CACHE_DOCUMENTS)
            .map(|idx| stored_record(&format!("large-{idx}"), "agent memory lexical cache"))
            .collect::<Vec<_>>();
        let first_large = db.score_prepared_lexical_corpus(
            &schema,
            "tenant-a",
            "large",
            &large_records,
            &[],
            None,
            "lexical cache",
        );
        let second_large = db.score_prepared_lexical_corpus(
            &schema,
            "tenant-a",
            "large",
            &large_records,
            &[],
            None,
            "lexical cache",
        );

        assert!(!first_large.cache_hit);
        assert!(first_large.cache_miss);
        assert_eq!(first_large.indexed_documents, MIN_LEXICAL_CACHE_DOCUMENTS);
        assert!(second_large.cache_hit);
        assert!(!second_large.cache_miss);
        assert_eq!(second_large.indexed_documents, MIN_LEXICAL_CACHE_DOCUMENTS);
    }

    #[test]
    fn lexical_cache_evicts_least_recently_used_entry_at_default_capacity() {
        let temp = tempfile::tempdir().expect("tempdir");
        let db = TraceDb::open(temp.path()).expect("open");
        let schema = schema();
        let large_records = (0..MIN_LEXICAL_CACHE_DOCUMENTS)
            .map(|idx| stored_record(&format!("large-{idx}"), "agent memory lexical cache"))
            .collect::<Vec<_>>();

        for idx in 0..65 {
            let report = db.score_prepared_lexical_corpus(
                &schema,
                "tenant-a",
                &format!("key-{idx}"),
                &large_records,
                &[],
                None,
                "lexical cache",
            );
            assert!(report.cache_miss, "key-{idx} should populate cache");
        }

        let first_again = db.score_prepared_lexical_corpus(
            &schema,
            "tenant-a",
            "key-0",
            &large_records,
            &[],
            None,
            "lexical cache",
        );

        assert_eq!(db.lexical_cache.lock().unwrap().entries.len(), 64);
        assert!(
            first_again.cache_miss,
            "oldest cache entry should have been evicted"
        );
    }

    #[test]
    fn sealed_segment_records_materialize_without_hot_store_copy() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(schema()).expect("schema");
        db.insert(record("sealed-only", "rare sealed segment evidence"))
            .expect("insert");
        db.compact().expect("compact");
        db.store = RecordStore::default();

        let output = db
            .query(HybridQuery {
                table: "docs".to_string(),
                tenant_id: "tenant-a".to_string(),
                cursor: None,
                text_field: Some("body".to_string()),
                text: Some("sealed evidence".to_string()),
                vector_field: Some("embedding".to_string()),
                vector: Some(vec![1.0, 0.0, 0.0]),
                scalar_eq: Default::default(),
                graph_seed: None,
                temporal_as_of: None,
                top_k: 5,
                freshness: FreshnessMode::Strict,
                explain: true,
            })
            .expect("query");

        assert_eq!(
            output
                .results
                .iter()
                .map(|row| row.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sealed-only"]
        );
        assert_eq!(output.explain.final_visibility_guard_removed, 0);
    }

    #[test]
    fn sealed_vector_query_uses_ready_vector_index_artifact() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(schema()).expect("schema");
        db.insert(vector_record(
            "segment-nearest",
            "sealed vector candidate",
            [1.0, 0.0, 0.0],
        ))
        .expect("insert segment-nearest");
        db.insert(vector_record(
            "artifact-nearest",
            "sealed vector candidate",
            [0.0, 1.0, 0.0],
        ))
        .expect("insert artifact-nearest");
        db.compact().expect("compact");

        let vector_index = db
            .manifest
            .indexes
            .iter()
            .find(|index| index.kind == "vector")
            .expect("vector index manifest")
            .clone();
        let index_path = temp.path().join(&vector_index.object_path);
        let mut artifact =
            tracedb_index::read_index_artifact(&index_path, None).expect("read vector index");
        let tracedb_index::IndexPayload::Vector(vector) = &mut artifact.payload else {
            panic!("vector payload expected");
        };
        for entry in &mut vector.entries {
            if entry.record_id == "segment-nearest" {
                entry.vector = vec![0.0, 1.0, 0.0];
            } else if entry.record_id == "artifact-nearest" {
                entry.vector = vec![1.0, 0.0, 0.0];
            }
        }
        vector.neighbors = BTreeMap::new();
        let checksum =
            tracedb_index::write_index_artifact(&index_path, &artifact, None).expect("rewrite");
        let payload_checksum = artifact.payload_checksum().expect("payload checksum");
        let manifest_index = db
            .manifest
            .indexes
            .iter_mut()
            .find(|index| index.index_id == vector_index.index_id)
            .expect("manifest vector index");
        manifest_index.checksum = checksum;
        manifest_index.payload_checksum = payload_checksum;
        write_manifest(temp.path().join("manifest.tdb"), &mut db.manifest).expect("manifest");
        db.store = RecordStore::default();

        let output = db
            .query(HybridQuery {
                table: "docs".to_string(),
                tenant_id: "tenant-a".to_string(),
                cursor: None,
                text_field: None,
                text: None,
                vector_field: Some("embedding".to_string()),
                vector: Some(vec![1.0, 0.0, 0.0]),
                scalar_eq: Default::default(),
                graph_seed: None,
                temporal_as_of: None,
                top_k: 1,
                freshness: FreshnessMode::Strict,
                explain: true,
            })
            .expect("query");

        assert_eq!(
            output.results.first().map(|row| row.record_id.as_str()),
            Some("artifact-nearest")
        );
    }

    #[test]
    fn compaction_replaces_source_segments_and_vacuum_removes_superseded_artifacts() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(schema()).expect("schema");
        db.insert(record("a", "first compacted segment"))
            .expect("insert a");

        db.compact().expect("first compact");
        let first_manifest = db.inspect_manifest().expect("first manifest");
        assert_eq!(first_manifest.segments.len(), 1);
        let old_segment_id = first_manifest.segments[0].segment_id.clone();
        let old_segment_path = temp
            .path()
            .join("segments")
            .join(format!("{old_segment_id}.tseg"));
        let old_index_paths = first_manifest
            .indexes
            .iter()
            .map(|index| temp.path().join(&index.object_path))
            .collect::<Vec<_>>();
        assert!(old_segment_path.exists());
        assert!(old_index_paths.iter().all(|path| path.exists()));

        db.insert(record("b", "second compacted segment"))
            .expect("insert b");
        db.compact().expect("second compact");
        let compacted_manifest = db.inspect_manifest().expect("compacted manifest");
        assert_eq!(
            compacted_manifest.segments.len(),
            1,
            "compaction should replace source segments in the manifest"
        );
        assert_ne!(compacted_manifest.segments[0].segment_id, old_segment_id);
        assert!(
            compacted_manifest
                .indexes
                .iter()
                .all(|index| index.segment_id == compacted_manifest.segments[0].segment_id),
            "manifest indexes should only reference the compacted segment"
        );

        let removed = db.vacuum().expect("vacuum");
        assert!(
            removed >= 1 + old_index_paths.len(),
            "vacuum should remove the old segment and its indexes, removed {removed}"
        );
        assert!(!old_segment_path.exists());
        assert!(old_index_paths.iter().all(|path| !path.exists()));
        let new_segment_path = temp.path().join("segments").join(format!(
            "{}.tseg",
            compacted_manifest.segments[0].segment_id
        ));
        assert!(new_segment_path.exists());
        assert!(compacted_manifest
            .indexes
            .iter()
            .all(|index| temp.path().join(&index.object_path).exists()));
    }

    #[test]
    fn tde_encrypted_artifacts_reopen_with_correct_key() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open_with_options(
            temp.path(),
            TraceDbOpenOptions::with_master_key_b64(GOOD_MASTER_KEY),
        )
        .expect("open encrypted db");
        db.apply_schema(schema()).expect("schema");
        db.put(RecordPutRequest::new(record(
            "secret",
            "secret encrypted wal checkpoint segment index",
        )))
        .expect("put");
        let wal_bytes = fs::read(temp.path().join("wal/000001.twal")).expect("wal bytes");
        let wal_text = String::from_utf8_lossy(&wal_bytes);
        assert!(
            !wal_text.contains("secret encrypted wal checkpoint segment index"),
            "encrypted WAL must not expose plaintext record body"
        );
        assert!(
            !wal_text.contains("\"mutations\""),
            "encrypted WAL must not expose plaintext commit JSON"
        );
        let checkpoint_epoch = db.checkpoint().expect("checkpoint");
        db.compact().expect("compact");

        let manifest = db.inspect_manifest().expect("manifest");
        let encryption = manifest.encryption.expect("manifest encryption metadata");
        assert_eq!(encryption.algorithm, "XChaCha20Poly1305");
        assert!(encryption.key_id.starts_with("tracedb-root:"));
        assert!(!encryption.wrapped_dek_b64.is_empty());

        let checkpoint_bytes =
            fs::read(temp.path().join(checkpoint_relative_path(checkpoint_epoch)))
                .expect("checkpoint bytes");
        assert_eq!(
            &checkpoint_bytes[..CHECKPOINT_MAGIC_V3.len()],
            CHECKPOINT_MAGIC_V3
        );
        assert!(
            !String::from_utf8_lossy(&checkpoint_bytes)
                .contains("secret encrypted wal checkpoint segment index"),
            "encrypted checkpoint must not expose plaintext record body"
        );

        let segment = manifest.segments.first().expect("segment manifest");
        let segment_bytes = fs::read(
            temp.path()
                .join("segments")
                .join(format!("{}.tseg", segment.segment_id)),
        )
        .expect("segment bytes");
        assert!(
            !String::from_utf8_lossy(&segment_bytes)
                .contains("secret encrypted wal checkpoint segment index"),
            "encrypted segment must not expose plaintext record body"
        );
        let index = manifest.indexes.first().expect("index manifest");
        let index_bytes = fs::read(temp.path().join(&index.object_path)).expect("index bytes");
        assert!(
            !String::from_utf8_lossy(&index_bytes).contains("secret"),
            "encrypted index must not expose plaintext tokens"
        );

        drop(db);
        let reopened = TraceDb::open_with_options(
            temp.path(),
            TraceDbOpenOptions::with_master_key_b64(GOOD_MASTER_KEY),
        )
        .expect("reopen encrypted db");
        let record = reopened
            .get(RecordGetRequest::new("docs", "tenant-a", "secret"))
            .expect("get encrypted record")
            .expect("record exists");
        assert_eq!(
            record.fields["body"],
            json!("secret encrypted wal checkpoint segment index")
        );
    }

    #[test]
    fn tde_wrong_or_missing_master_key_fails_open() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open_with_options(
            temp.path(),
            TraceDbOpenOptions::with_master_key_b64(GOOD_MASTER_KEY),
        )
        .expect("open encrypted db");
        db.apply_schema(schema()).expect("schema");
        db.put(RecordPutRequest::new(record("a", "wrong key protected")))
            .expect("put");
        drop(db);

        let wrong = TraceDb::open_with_options(
            temp.path(),
            TraceDbOpenOptions::with_master_key_b64(OTHER_MASTER_KEY),
        )
        .expect_err("wrong key must fail open");
        assert!(
            wrong
                .to_string()
                .contains("failed to unwrap database encryption key"),
            "unexpected wrong-key error: {wrong}"
        );

        let missing = TraceDb::open_with_options(temp.path(), TraceDbOpenOptions::without_tde())
            .expect_err("missing key must fail open");
        assert!(
            missing
                .to_string()
                .contains("TRACEDB_MASTER_KEY_B64 is required to open encrypted TraceDB data"),
            "unexpected missing-key error: {missing}"
        );
    }

    #[test]
    fn legacy_plaintext_artifacts_remain_readable_when_tde_is_enabled() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut plain = TraceDb::open_with_options(temp.path(), TraceDbOpenOptions::without_tde())
            .expect("open plaintext db");
        plain.apply_schema(schema()).expect("schema");
        plain
            .put(RecordPutRequest::new(record(
                "legacy",
                "legacy plaintext body",
            )))
            .expect("put");
        let checkpoint_epoch = plain.checkpoint().expect("checkpoint");
        let wal_before = fs::read(temp.path().join("wal/000001.twal")).expect("wal before");
        let checkpoint_before =
            fs::read(temp.path().join(checkpoint_relative_path(checkpoint_epoch)))
                .expect("checkpoint before");
        drop(plain);

        let mut encrypted = TraceDb::open_with_options(
            temp.path(),
            TraceDbOpenOptions::with_master_key_b64(GOOD_MASTER_KEY),
        )
        .expect("open legacy db with tde configured");
        let legacy = encrypted
            .get(RecordGetRequest::new("docs", "tenant-a", "legacy"))
            .expect("get legacy")
            .expect("legacy record exists");
        assert_eq!(legacy.fields["body"], json!("legacy plaintext body"));
        assert_eq!(
            fs::read(temp.path().join("wal/000001.twal")).expect("wal after tde open"),
            wal_before,
            "opening legacy plaintext with TDE must not rewrite existing WAL bytes"
        );
        assert_eq!(
            fs::read(temp.path().join(checkpoint_relative_path(checkpoint_epoch)))
                .expect("checkpoint after tde open"),
            checkpoint_before,
            "opening legacy plaintext with TDE must not rewrite existing checkpoint bytes"
        );

        encrypted
            .put(RecordPutRequest::new(record("new", "new encrypted body")))
            .expect("new encrypted put");
        let mixed_wal = fs::read(temp.path().join("wal/000001.twal")).expect("mixed wal");
        assert!(
            mixed_wal.starts_with(&wal_before),
            "legacy WAL prefix should remain intact after encrypted append"
        );
        assert!(
            !String::from_utf8_lossy(&mixed_wal[wal_before.len()..]).contains("new encrypted body"),
            "new TDE-configured WAL frame must be encrypted"
        );
    }

    #[test]
    fn opening_same_data_directory_twice_fails_with_lock_error() {
        let temp = tempfile::tempdir().expect("tempdir");
        let db = TraceDb::open(temp.path()).expect("first open");

        let error = TraceDb::open(temp.path()).expect_err("second open should fail");
        let message = error.to_string();
        assert!(
            message.contains("already locked"),
            "lock error should explain contention: {message}"
        );
        assert!(
            message.contains("engine.lock"),
            "lock error should identify the lock file: {message}"
        );
        assert!(
            message.contains("PID"),
            "lock error should point at the PID marker: {message}"
        );

        drop(db);
        TraceDb::open(temp.path()).expect("open after first handle drops");
    }

    #[test]
    fn engine_lock_writes_pid_and_preserves_existing_marker_on_contention() {
        let temp = tempfile::tempdir().expect("tempdir");
        let db = TraceDb::open(temp.path()).expect("open");
        let lock_path = temp.path().join("engine.lock");
        let expected_pid_marker = format!("{}\n", std::process::id());

        assert_eq!(
            fs::read_to_string(&lock_path).expect("pid marker"),
            expected_pid_marker,
            "engine lock should write the owning process PID after acquisition"
        );

        let existing_marker = "existing-owner-pid=424242\n";
        fs::write(&lock_path, existing_marker).expect("overwrite marker while lock is held");
        let error = TraceDb::open(temp.path()).expect_err("contended open should fail");
        assert!(
            error.to_string().contains("already locked"),
            "unexpected contention error: {error}"
        );
        assert_eq!(
            fs::read_to_string(&lock_path).expect("marker after failed open"),
            existing_marker,
            "failed lock acquisition must not truncate the existing lock marker"
        );

        drop(db);
        TraceDb::open(temp.path()).expect("open after lock release");
        assert_eq!(
            fs::read_to_string(&lock_path).expect("pid marker after reacquire"),
            expected_pid_marker,
            "successful reacquisition should refresh the PID marker"
        );
    }

    #[test]
    fn open_uses_valid_manifest_backup_when_primary_is_corrupted() {
        let temp = tempfile::tempdir().expect("tempdir");
        {
            let mut db = TraceDb::open(temp.path()).expect("open");
            db.apply_schema(schema()).expect("schema");
            db.put(RecordPutRequest::new(record(
                "backup-fallback",
                "from backup fallback",
            )))
            .expect("put");
        }

        let manifest_path = temp.path().join("manifest.tdb");
        let backup_path = manifest_path.with_extension("tdb.bak");
        let backup_before = fs::read(&backup_path).expect("manifest backup exists");
        read_manifest_inner(&backup_path).expect("manifest backup is valid");
        fs::write(&manifest_path, b"{not valid json").expect("corrupt primary manifest");

        let db = TraceDb::open(temp.path()).expect("open using backup manifest");
        let restored = db
            .get(RecordGetRequest::new("docs", "tenant-a", "backup-fallback"))
            .expect("get restored record")
            .expect("record restored from WAL after backup manifest fallback");
        assert_eq!(restored.fields["body"], json!("from backup fallback"));
        read_manifest_inner(&manifest_path).expect("primary manifest is rewritten valid");
        assert_eq!(
            fs::read(&backup_path).expect("backup after recovery"),
            backup_before,
            "recovery must not overwrite the only valid backup with the corrupt primary"
        );
        read_manifest_inner(&backup_path).expect("backup remains valid after recovery");
    }

    #[test]
    fn open_rejects_checksum_corrupted_manifest_even_with_valid_backup() {
        let temp = tempfile::tempdir().expect("tempdir");
        {
            let mut db = TraceDb::open(temp.path()).expect("open");
            db.apply_schema(schema()).expect("schema");
            db.put(RecordPutRequest::new(record(
                "checksum-corruption",
                "checksum protected body",
            )))
            .expect("put");
        }

        let manifest_path = temp.path().join("manifest.tdb");
        let backup_path = manifest_path.with_extension("tdb.bak");
        read_manifest_inner(&manifest_path).expect("primary manifest is valid before corruption");
        read_manifest_inner(&backup_path).expect("manifest backup is valid");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("primary manifest"))
                .expect("manifest json");
        manifest["checksums"]["manifest_checksum"] = json!([
            1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0
        ]);
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).expect("corrupt manifest json"),
        )
        .expect("write checksum-corrupted manifest");

        let error = TraceDb::open(temp.path()).expect_err("checksum corruption must fail open");
        assert!(
            error.to_string().contains("manifest checksum mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn manifest_write_keeps_primary_when_temp_write_fails() {
        let temp = tempfile::tempdir().expect("tempdir");
        let db = TraceDb::open(temp.path()).expect("open");
        let manifest_path = temp.path().join("manifest.tdb");
        let original_manifest = fs::read(&manifest_path).expect("primary manifest");
        fs::create_dir(manifest_path.with_extension("tdb.tmp")).expect("block temp manifest path");

        let mut manifest = db.inspect_manifest().expect("inspect manifest");
        write_manifest(&manifest_path, &mut manifest).expect_err("temp write should fail");

        assert_eq!(
            fs::read(&manifest_path).expect("primary manifest after failed write"),
            original_manifest,
            "failed manifest write must leave the current primary manifest in place"
        );
        read_manifest_inner(&manifest_path).expect("primary manifest remains valid");
        read_manifest_inner(&manifest_path.with_extension("tdb.bak"))
            .expect("backup manifest remains valid");
    }

    #[test]
    fn stale_engine_and_wal_lock_files_are_recovered_with_owner_checks() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        fs::write(temp.path().join("engine.write.lock"), "999999999").expect("engine lock");
        fs::write(temp.path().join("wal/000001.twal.lock"), "999999999").expect("wal lock");

        db.apply_schema(schema())
            .expect("stale engine and WAL locks should be recovered");

        // With fs2 advisory locks, a stale lock file (not held by any process)
        // is acquired directly; the file may persist after release.
        assert!(
            temp.path().join("engine.write.lock").exists(),
            "engine advisory lock file persists after release"
        );
        assert!(
            temp.path().join("wal/000001.twal.lock").exists(),
            "WAL advisory lock file persists after release"
        );
    }

    #[test]
    fn checkpoint_file_uses_framed_payload_checksum() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut db = TraceDb::open(temp.path()).expect("open");
        db.apply_schema(schema()).expect("schema");
        db.insert(record("a", "checkpoint payload"))
            .expect("insert");

        let epoch = db.checkpoint().expect("checkpoint");
        let relative = checkpoint_relative_path(epoch);
        let body = fs::read(temp.path().join(relative)).expect("checkpoint bytes");

        assert_eq!(&body[..CHECKPOINT_MAGIC_V3.len()], CHECKPOINT_MAGIC_V3);
        assert!(body.len() > CHECKPOINT_MAGIC_V3.len() + 4);
        let expected: [u8; 32] = body[CHECKPOINT_MAGIC_V3.len()..CHECKPOINT_MAGIC_V3.len() + 32]
            .try_into()
            .expect("checksum bytes");
        let payload = &body[CHECKPOINT_MAGIC_V3.len() + 32..];
        assert_eq!(expected, checksum_bytes(payload));
    }
}
