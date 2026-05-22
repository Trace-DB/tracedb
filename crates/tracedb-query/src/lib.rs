#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tracedb_core::{
    builtin_module_manifests, checksum_bytes, compute_manifest_checksum, database_id_from_path,
    value_as_f32_vec, DerivedFeatureState, Epoch, FeatureStatus, IndexManifest, IndexState,
    ModuleCommitEvent, Result, TraceDbError, TraceDbManifest,
};
use tracedb_log::{CommitRecord, TornWalTail, Wal, WalAppendTiming};
use tracedb_modules::{ModuleRegistry, RegisteredModule};
use tracedb_planner::{
    plan_trace_query, AccessPath, AccessPathDescriptor as PlannerAccessPathDescriptor,
    AccessPathExplain, AccessPathTiming, Candidate, CandidateBatch, CandidateBudget, CostEstimate,
    ExplainOutput, FeatureFreshness, PlannerFeedback, Predicates, QueryFragment, QueryOutput,
    QueryPhaseTiming, QueryRow, ScoreComponents, Stats, TraceQuery, WorkBudget,
};
use tracedb_segment::SegmentRecord;
use tracedb_store::{ReadSnapshot, RecordStore, ReplacementApplyTiming, StoredRecord};

const CHECKPOINT_MAGIC_V2: &[u8; 8] = b"TDBCHK01";
const CHECKPOINT_MAGIC_V3: &[u8; 8] = b"TDBCHK02";
const CHECKPOINT_FORMAT_VERSION: u32 = 3;
const CHECKPOINT_LEGACY_COMPACT_FORMAT_VERSION: u32 = 2;
const CHECKPOINT_LEGACY_JSON_FORMAT_VERSION: u32 = 1;
const MIN_LEXICAL_CACHE_DOCUMENTS: usize = 2_048;

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
    pub text: Option<String>,
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
}

impl RecordScanRequest {
    pub fn new(table: impl Into<String>, tenant_id: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            tenant_id: tenant_id.into(),
            limit: 100,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
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

pub trait BackupRestore {
    fn backup(&self, target: impl AsRef<Path>) -> Result<()>;
    fn restore(source: impl AsRef<Path>, target: impl AsRef<Path>) -> Result<TraceDb>;
}

#[derive(Clone, Debug)]
pub struct TraceDb {
    dir: PathBuf,
    manifest: TraceDbManifest,
    store: RecordStore,
    wal: Wal,
    last_recovery_torn_tail: Option<TornWalTail>,
    lexical_cache: RefCell<LexicalCorpusCache>,
}

#[derive(Clone, Debug, Default)]
struct LexicalCorpusCache {
    entries: BTreeMap<LexicalCacheKey, tracedb_text::PreparedTextCorpus>,
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
    checksum: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointPayload {
    format_version: u32,
    epoch: Epoch,
    schemas: Vec<TableSchema>,
    records: Vec<StoredRecord>,
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
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        initialize_layout(&dir)?;
        let manifest_path = dir.join("manifest.tdb");
        if !manifest_path.exists() {
            let mut manifest = TraceDbManifest::empty(database_id_from_path(&dir));
            write_manifest(&manifest_path, &mut manifest)?;
        }

        let mut manifest = read_manifest(&manifest_path)?;
        if manifest.modules.is_empty() {
            manifest.modules = builtin_module_manifests();
        }
        let wal = Wal::open(&dir)?;
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
        let mut store = if manifest.checkpoint_epoch.get() > 0 {
            let checkpoint = read_checkpoint_file(&dir, manifest.checkpoint_epoch)?;
            if checkpoint.epoch != manifest.checkpoint_epoch {
                return Err(TraceDbError::ManifestCorruption(format!(
                    "checkpoint epoch mismatch: manifest {}, file {}",
                    manifest.checkpoint_epoch, checkpoint.epoch
                )));
            }
            if manifest.schemas.is_empty() {
                manifest.schemas = checkpoint.schemas.clone();
            }
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
            last_recovery_torn_tail: wal_scan.torn_tail,
            lexical_cache: RefCell::new(LexicalCorpusCache::default()),
        })
    }

    pub fn apply_schema(&mut self, schema: TableSchema) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        schema.validate()?;
        self.validate_schema_compatible(&schema)?;
        let epoch = self.manifest.latest_epoch.next();
        let mut commit = CommitRecord::empty(epoch.get(), epoch).for_database(
            self.manifest.database_id.clone(),
            self.manifest.branch_id.clone(),
        );
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
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn insert(&mut self, input: RecordInput) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let schema = self
            .manifest
            .table(&input.table)
            .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let mut staged = self.store.clone();
        staged.apply_mutation(&schema, &input, epoch)?;
        let feature_invalidations = feature_invalidations_for_mutation(&schema, &input);
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            mutations: vec![input.clone()],
            feature_invalidations,
            module_events: module_events_for_schema("insert.index", &schema),
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store = staged;
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn put(&mut self, request: RecordPutRequest) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let input = request.record;
        let schema = self
            .manifest
            .table(&input.table)
            .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let mut staged = self.store.clone();
        staged.apply_replacement_without_return(&schema, &input, epoch)?;
        let feature_invalidations = feature_invalidations_for_mutation(&schema, &input);
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
        self.wal.append_commit(&commit)?;
        self.store = staged;
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn put_batch(&mut self, request: RecordPutBatchRequest) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        if request.records.is_empty() {
            return Err(TraceDbError::InvalidCommand(
                "batch put requires at least one record".to_string(),
            ));
        }

        let epoch = self.manifest.latest_epoch.next();
        let mut staged = self.store.clone();
        let mut feature_invalidations = Vec::new();
        let mut module_events = Vec::new();
        let mut seen_module_events = BTreeSet::new();
        for input in &request.records {
            let schema = self
                .manifest
                .table(&input.table)
                .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
                .clone();
            staged.apply_replacement_without_return(&schema, input, epoch)?;
            feature_invalidations.extend(feature_invalidations_for_mutation(&schema, input));
            for event in module_events_for_schema("record.put", &schema) {
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
        self.wal.append_commit(&commit)?;
        self.store = staged;
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
        let store_clone_started = Instant::now();
        let mut staged = self.store.clone();
        let store_clone_ms = elapsed_ms(store_clone_started);

        let mut store_apply_timing = ReplacementApplyTiming::default();
        let store_apply_started = Instant::now();
        for (input, schema) in request.records.iter().zip(schemas.iter()) {
            let timing =
                staged.apply_replacement_without_return_with_timing(schema, input, epoch)?;
            store_apply_timing.validate_identity_ms += timing.validate_identity_ms;
            store_apply_timing.validate_vector_ms += timing.validate_vector_ms;
            store_apply_timing.key_ms += timing.key_ms;
            store_apply_timing.fields_ms += timing.fields_ms;
            store_apply_timing.finalize_identity_ms += timing.finalize_identity_ms;
            store_apply_timing.features_ms += timing.features_ms;
            store_apply_timing.install_ms += timing.install_ms;
        }
        let store_apply_ms = elapsed_ms(store_apply_started);

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
        self.store = staged;
        let store_install_ms = elapsed_ms(store_install_started);
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
        let store_clone_started = Instant::now();
        let mut staged = self.store.clone();
        let store_clone_ms = elapsed_ms(store_clone_started);
        let store_apply_started = Instant::now();
        let store_apply_timing =
            staged.apply_replacement_without_return_with_timing(&schema, &input, epoch)?;
        let store_apply_ms = elapsed_ms(store_apply_started);
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
        self.store = staged;
        let store_install_ms = elapsed_ms(store_install_started);
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
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let input = request.into_record_input();
        let schema = self
            .manifest
            .table(&input.table)
            .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let mut staged = self.store.clone();
        staged.apply_mutation(&schema, &input, epoch)?;
        let feature_invalidations = feature_invalidations_for_mutation(&schema, &input);
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: Vec::new(),
            mutations: vec![input.clone()],
            deletions: Vec::new(),
            feature_invalidations,
            module_events: module_events_for_schema("record.patch", &schema),
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store = staged;
        self.bump_manifest(epoch)?;
        self.clear_lexical_cache();
        Ok(epoch)
    }

    pub fn delete(&mut self, request: RecordDeleteRequest) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        self.refresh_from_disk_if_stale()?;
        let deletion = request.into_deletion();
        let schema = self
            .manifest
            .table(&deletion.table)
            .ok_or_else(|| TraceDbError::UnknownTable(deletion.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let mut staged = self.store.clone();
        staged.apply_delete(&schema, &deletion, epoch)?;
        let commit = CommitRecord {
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
            schema_changes: Vec::new(),
            replacements: Vec::new(),
            mutations: Vec::new(),
            deletions: vec![deletion],
            feature_invalidations: Vec::new(),
            module_events: module_events_for_schema("record.delete", &schema),
            ..CommitRecord::empty(epoch.get(), epoch).for_database(
                self.manifest.database_id.clone(),
                self.manifest.branch_id.clone(),
            )
        };
        self.wal.append_commit(&commit)?;
        self.store = staged;
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

    pub fn scan(&self, request: RecordScanRequest) -> Result<RecordScanOutput> {
        self.manifest
            .table(&request.table)
            .ok_or_else(|| TraceDbError::UnknownTable(request.table.clone()))?;
        let limit = request.limit.max(1);
        let records = self
            .store
            .scan_records(
                &request.table,
                &request.tenant_id,
                limit,
                self.manifest.latest_epoch,
            )
            .into_iter()
            .map(record_output)
            .collect::<Vec<_>>();
        Ok(RecordScanOutput {
            returned_count: records.len(),
            records,
        })
    }

    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        Ok(self.store.snapshot(self.manifest.latest_epoch))
    }

    pub fn query(&self, query: HybridQuery) -> Result<QueryOutput> {
        Ok(self.query_with_timing(query)?.output)
    }

    pub fn query_with_timing(&self, query: HybridQuery) -> Result<TimedQueryOutput> {
        let total_started = Instant::now();
        let mut timing = QueryExecutionTiming::default();
        let include_explain = query.explain;
        let schema = self
            .manifest
            .table(&query.table)
            .ok_or_else(|| TraceDbError::UnknownTable(query.table.clone()))?;
        validate_vector_query_dimensions(schema, query.vector.as_deref())?;
        validate_scalar_eq_predicates(schema, &query.scalar_eq)?;
        if query.top_k == 0 {
            timing.total_ms = elapsed_ms(total_started);
            timing.engine_core_ms = timing.total_ms;
            return Ok(TimedQueryOutput {
                timing,
                output: QueryOutput {
                    results: Vec::new(),
                    explain: ExplainOutput {
                        read_epoch: self.manifest.latest_epoch,
                        schema_epoch: self.manifest.latest_epoch,
                        policy_epoch: self.manifest.latest_epoch,
                        scalar_filter_applied: !query.scalar_eq.is_empty(),
                        scalar_filter_predicates: scalar_filter_predicates(&query.scalar_eq),
                        freshness_mode: query.freshness.as_str().to_string(),
                        fusion_method: "RRF".to_string(),
                        ..ExplainOutput::default()
                    },
                },
            });
        }
        let tenant_visibility_started = Instant::now();
        let visible = self.store.visible_records_at(
            &query.table,
            &query.tenant_id,
            self.manifest.latest_epoch,
        );
        let sealed_records = self.sealed_segment_records(&query.table, &query.tenant_id)?;
        let tenant_mask_visible_records = visible.len();
        let tenant_visibility_ms = elapsed_ms(tenant_visibility_started);
        let scalar_filter_started = Instant::now();
        let scalar_filter_applied = !query.scalar_eq.is_empty();
        let visible = filter_records_by_scalar_eq(visible, &query.scalar_eq);
        let sealed_records = filter_segment_records_by_scalar_eq(sealed_records, &query.scalar_eq);
        let scalar_filter_ms = elapsed_ms(scalar_filter_started);
        let candidate_budget = query.top_k.saturating_mul(4).max(query.top_k).max(1);
        let mut explain = ExplainOutput {
            read_epoch: self.manifest.latest_epoch,
            schema_epoch: self.manifest.latest_epoch,
            policy_epoch: self.manifest.latest_epoch,
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
                query.text.as_deref(),
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
            text: query.text.clone(),
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
                text: query.text.clone(),
                vector_query: query.vector.clone(),
                graph_seed: query.graph_seed.clone(),
                temporal_as_of: query.temporal_as_of,
                freshness: &query.freshness,
                fallback_candidate_limit: query_has_evidence(&query).then_some(candidate_budget),
            },
        );
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
                    self.manifest.latest_epoch,
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
            if materialized.len() >= query.top_k {
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

        explain.materialized_count = materialized.len();
        explain.final_visibility_guard_count = checked;
        explain.final_visibility_guard_removed = removed;
        explain.returned_count = materialized.len();
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
                results: materialized,
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
        let mut staged = self.store.clone();
        staged.apply_feature_invalidation(&invalidation, epoch)?;
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
        self.store = staged;
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
        self.wal = Wal::open(&self.dir)?;
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
        self.publish_segment(segment_id)
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
            let object = tracedb_segment::read_segment_object(&object_path)?;
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
        let object = tracedb_segment::publish_segment_records(
            &object_path,
            &segment_id,
            generation,
            records,
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
            last_recovery_torn_tail,
            ..
        } = TraceDb::open(&self.dir)?;
        self.manifest = manifest;
        self.store = store;
        self.wal = wal;
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
            let object = tracedb_segment::read_segment_object(path)?;
            out.extend(
                object
                    .records
                    .into_iter()
                    .filter(|record| record.table == table && record.tenant_id == tenant_id),
            );
        }
        Ok(out)
    }

    fn build_segment_indexes(
        &self,
        object: &tracedb_segment::SegmentObject,
        parent_manifest_generation: u64,
    ) -> Result<Vec<IndexManifest>> {
        let mut kinds = BTreeSet::from(["primary".to_string(), "policy".to_string()]);
        for record in &object.records {
            if !record.text.is_empty() {
                kinds.insert("text".to_string());
            }
            if !record.vectors.is_empty() {
                kinds.insert("vector".to_string());
            }
        }

        let mut manifests = Vec::new();
        for kind in kinds {
            let index_id = format!("{}:{kind}:{}", object.segment_id, object.generation);
            let object_path = format!("indexes/{index_id}.tidx");
            let body = serde_json::json!({
                "index_id": index_id,
                "segment_id": object.segment_id,
                "segment_generation": object.generation,
                "kind": kind,
                "state_history": ["PENDING", "BUILDING", "READY"],
                "policy_aware": true,
                "source_segment_checksum": object.object_checksum,
                "record_count": object.records.len(),
            });
            let bytes = serde_json::to_vec_pretty(&body)?;
            let checksum = checksum_bytes(&bytes);
            let index_path = self.dir.join(&object_path);
            if let Some(parent) = index_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let tmp_path = index_path.with_extension("tidx.tmp");
            let mut file = File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&tmp_path, &index_path)?;

            manifests.push(IndexManifest {
                index_id,
                segment_id: object.segment_id.clone(),
                generation: object.generation,
                kind,
                state: IndexState::Ready,
                policy_aware: true,
                parent_manifest_generation,
                object_path,
                checksum,
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
    text: Option<String>,
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
}

struct LexicalQueryReport {
    cache_hit: bool,
    cache_miss: bool,
    indexed_documents: usize,
    score_report: tracedb_text::TextScoreReport,
}

fn query_access_paths(db: &TraceDb, input: QueryAccessInput<'_>) -> QueryAccessPaths {
    let QueryAccessInput {
        schema,
        visible,
        sealed_records,
        tenant_id,
        scalar_eq_key,
        text,
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
    if let Some(vector_query) = vector_query {
        vector_candidates = visible
            .iter()
            .filter_map(|record| {
                vector_score(schema, record, &vector_query, freshness).map(|(score, penalty)| {
                    let freshness = vector_feature_freshness(schema, record);
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
        vector_candidates.extend(sealed_records.iter().filter_map(|record| {
            segment_vector_score(schema, record, &vector_query).map(|score| {
                segment_candidate(record, "VectorPath", score, |score| ScoreComponents {
                    vector: Some(score),
                    final_score: score,
                    ..ScoreComponents::default()
                })
            })
        }));
        vector_candidates.sort_by(|left, right| {
            score_order(
                left.score_components.final_score,
                right.score_components.final_score,
            )
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

    QueryAccessPaths {
        paths,
        timings,
        lexical_cache_hits,
        lexical_cache_misses,
        lexical_indexed_documents,
        lexical_scored_documents,
    }
}

impl TraceDb {
    fn score_prepared_lexical_corpus(
        &self,
        schema: &TableSchema,
        tenant_id: &str,
        scalar_eq_key: &str,
        visible: &[StoredRecord],
        sealed_records: &[SegmentRecord],
        query: &str,
    ) -> LexicalQueryReport {
        let indexed_documents = visible.len() + sealed_records.len();
        if indexed_documents < MIN_LEXICAL_CACHE_DOCUMENTS {
            let docs = lexical_documents(schema, visible, sealed_records);
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
            text_columns: schema.text_indexed_columns.clone(),
        };

        if let Some(corpus) = self.lexical_cache.borrow().entries.get(&key) {
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
                &lexical_documents(schema, visible, sealed_records),
            );
        let indexed_documents = corpus.document_count();
        self.lexical_cache.borrow_mut().entries.insert(key, corpus);
        LexicalQueryReport {
            cache_hit: false,
            cache_miss: true,
            indexed_documents,
            score_report,
        }
    }

    fn clear_lexical_cache(&self) {
        self.lexical_cache.borrow_mut().entries.clear();
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

fn record_output(record: StoredRecord) -> RecordOutput {
    RecordOutput {
        table: record.header.table_id,
        id: record.header.record_id,
        tenant_id: record.header.tenant_id,
        version_id: record.header.version_id.get(),
        fields: record.fields,
    }
}

fn vector_feature_freshness(schema: &TableSchema, record: &StoredRecord) -> FeatureFreshness {
    schema
        .vector_columns
        .iter()
        .find_map(|vector| record.features.get(&vector.name))
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
    query: &[f32],
    freshness: &FreshnessMode,
) -> Option<(f32, f32)> {
    for vector in &schema.vector_columns {
        let state = record.features.get(&vector.name)?;
        let penalty = match (&state.status, freshness) {
            (FeatureStatus::Ready, _) => 0.0,
            (FeatureStatus::Dirty, FreshnessMode::AllowDirty) => 0.05,
            _ => continue,
        };
        let value = record.fields.get(&vector.name).and_then(value_as_f32_vec)?;
        let score = tracedb_vector::cosine_similarity(query, &value)?;
        return Some((score, penalty));
    }
    None
}

fn segment_vector_score(
    schema: &TableSchema,
    record: &SegmentRecord,
    query: &[f32],
) -> Option<f32> {
    if let Some(vector) = schema.vector_columns.first() {
        let value = record.vectors.get(&vector.name)?;
        let score = tracedb_vector::cosine_similarity(query, value)?;
        return Some(score);
    }
    None
}

fn validate_vector_query_dimensions(schema: &TableSchema, query: Option<&[f32]>) -> Result<()> {
    let Some(query) = query else {
        return Ok(());
    };
    let Some(vector) = schema.vector_columns.first() else {
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
) -> Vec<(String, String)> {
    let mut docs = visible
        .iter()
        .map(|record| {
            (
                record.header.record_id.clone(),
                text_body(schema, record).unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    docs.extend(sealed_records.iter().map(|record| {
        (
            record.record_id.clone(),
            segment_text_body(record).unwrap_or_default(),
        )
    }));
    docs
}

fn text_body(schema: &TableSchema, record: &StoredRecord) -> Option<String> {
    let mut parts = Vec::new();
    for column in &schema.text_indexed_columns {
        if let Some(Value::String(value)) = record.fields.get(column) {
            parts.push(value.clone());
        }
    }
    (!parts.is_empty()).then(|| parts.join(" "))
}

fn segment_text_body(record: &SegmentRecord) -> Option<String> {
    let parts = record.text.values().cloned().collect::<Vec<_>>();
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
    let engine_lock = dir.join("engine.lock");
    if !engine_lock.exists() {
        File::create(engine_lock)?;
    }
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

struct WriteLock {
    path: PathBuf,
}

impl WriteLock {
    fn acquire(dir: &Path) -> Result<Self> {
        let path = dir.join("engine.write.lock");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    file.write_all(std::process::id().to_string().as_bytes())?;
                    file.sync_all()?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if Instant::now() >= deadline {
                        return Err(TraceDbError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("timed out waiting for write lock {}", path.display()),
                        )));
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(TraceDbError::Io(error)),
            }
        }
    }
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn read_manifest(path: impl AsRef<Path>) -> Result<TraceDbManifest> {
    let mut file = File::open(path)?;
    let mut body = String::new();
    file.read_to_string(&mut body)?;
    let manifest: TraceDbManifest = serde_json::from_str(&body)?;
    let expected = manifest.checksums.manifest_checksum;
    if expected == 0 {
        return Err(TraceDbError::ManifestCorruption(
            "missing manifest checksum".to_string(),
        ));
    }
    let actual = compute_manifest_checksum(&manifest)?;
    if actual != expected {
        return Err(TraceDbError::ManifestCorruption(format!(
            "manifest checksum mismatch: expected {expected}, got {actual}"
        )));
    }
    Ok(manifest)
}

fn write_manifest(path: impl AsRef<Path>, manifest: &mut TraceDbManifest) -> Result<()> {
    manifest.checksums.manifest_checksum = 0;
    manifest.checksums.manifest_checksum = compute_manifest_checksum(manifest)?;
    let body = serde_json::to_vec_pretty(manifest)?;
    let path = path.as_ref();
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
    manifest.checksums.manifest_checksum = 0;
    manifest.checksums.manifest_checksum = compute_manifest_checksum(manifest)?;
    let checksum_ms = elapsed_ms(checksum_started);
    let serialize_started = Instant::now();
    let body = serde_json::to_vec_pretty(manifest)?;
    let bytes = body.len() as u64;
    let serialize_ms = elapsed_ms(serialize_started);
    let path = path.as_ref();
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

fn checkpoint_relative_path(epoch: Epoch) -> String {
    checkpoint_binary_relative_path(epoch)
}

fn checkpoint_binary_relative_path(epoch: Epoch) -> String {
    format!("checkpoints/checkpoint-{}.tchk", epoch.get())
}

fn checkpoint_json_relative_path(epoch: Epoch) -> String {
    format!("checkpoints/checkpoint-{}.json", epoch.get())
}

fn compute_checkpoint_checksum(checkpoint: &CheckpointFile) -> Result<u32> {
    let mut normalized = checkpoint.clone();
    normalized.checksum = 0;
    Ok(checksum_bytes(&serde_json::to_vec(&normalized)?))
}

fn write_checkpoint_file(
    dir: &Path,
    epoch: Epoch,
    schemas: Vec<TableSchema>,
    records: Vec<StoredRecord>,
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
    };
    let payload = serde_json::to_vec(&payload)?;
    let checksum = checksum_bytes(&payload);
    let mut body = Vec::with_capacity(CHECKPOINT_MAGIC_V3.len() + 4 + payload.len());
    body.extend_from_slice(CHECKPOINT_MAGIC_V3);
    body.extend_from_slice(&checksum.to_le_bytes());
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

fn read_checkpoint_file(dir: &Path, epoch: Epoch) -> Result<CheckpointFile> {
    let path = dir.join(checkpoint_binary_relative_path(epoch));
    if path.exists() {
        return read_binary_checkpoint_file(&path);
    }
    read_json_checkpoint_file(&dir.join(checkpoint_json_relative_path(epoch)))
}

fn read_binary_checkpoint_file(path: &Path) -> Result<CheckpointFile> {
    let body = fs::read(path).map_err(|err| {
        TraceDbError::ManifestCorruption(format!(
            "failed to read checkpoint file at {}: {err}",
            path.display()
        ))
    })?;
    if body.starts_with(CHECKPOINT_MAGIC_V3) {
        return read_framed_checkpoint_file(path, &body);
    }
    if body.starts_with(CHECKPOINT_MAGIC_V2) {
        return read_legacy_binary_checkpoint_file(path, &body);
    }
    Err(TraceDbError::ManifestCorruption(format!(
        "checkpoint magic mismatch at {}",
        path.display()
    )))
}

fn read_framed_checkpoint_file(path: &Path, body: &[u8]) -> Result<CheckpointFile> {
    if body.len() <= CHECKPOINT_MAGIC_V3.len() + 4 {
        return Err(TraceDbError::ManifestCorruption(format!(
            "checkpoint payload missing at {}",
            path.display()
        )));
    }
    let expected = u32::from_le_bytes(
        body[CHECKPOINT_MAGIC_V3.len()..CHECKPOINT_MAGIC_V3.len() + 4]
            .try_into()
            .map_err(|_| {
                TraceDbError::ManifestCorruption(format!(
                    "checkpoint checksum frame is invalid at {}",
                    path.display()
                ))
            })?,
    );
    if expected == 0 {
        return Err(TraceDbError::ManifestCorruption(format!(
            "missing checkpoint checksum at {}",
            path.display()
        )));
    }
    let payload = &body[CHECKPOINT_MAGIC_V3.len() + 4..];
    let actual = checksum_bytes(payload);
    if actual != expected {
        return Err(TraceDbError::ManifestCorruption(format!(
            "checkpoint checksum mismatch at {}: expected {expected}, got {actual}",
            path.display()
        )));
    }
    let payload: CheckpointPayload = serde_json::from_slice(payload).map_err(|err| {
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
    if expected == 0 {
        return Err(TraceDbError::ManifestCorruption(format!(
            "missing checkpoint checksum at {}",
            path.display()
        )));
    }
    let actual = compute_checkpoint_checksum(&checkpoint)?;
    if actual != expected {
        return Err(TraceDbError::ManifestCorruption(format!(
            "checkpoint checksum mismatch at {}: expected {expected}, got {actual}",
            path.display()
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
    validate_copy_paths(source, target)?;
    if target.exists() {
        fs::remove_dir_all(target)?;
    }
    copy_dir(source, target)
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
    let mut text = None;
    let mut vector = None;
    let mut scalar_eq = Map::new();
    let mut top_k = 10;
    let mut freshness = FreshnessMode::Strict;
    let mut explain = false;

    for (line_idx, raw_line) in input.lines().enumerate() {
        let line_number = line_idx + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (directive, body) = split_traceql_directive(line);
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
                scalar_eq.insert(field, parse_traceql_value(value, line_number)?);
            }
            "MATCH" => {
                let (_column, value) = parse_traceql_column_value("MATCH", body, line_number)?;
                text = Some(parse_traceql_string_value(value, line_number)?);
            }
            "NEAR" => {
                let (_column, value) = parse_traceql_column_value("NEAR", body, line_number)?;
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
        text,
        vector,
        scalar_eq,
        graph_seed: None,
        temporal_as_of: None,
        top_k,
        freshness,
        explain,
    })
}

/// Parses TraceDB's bounded GraphQL adapter query form into `HybridQuery`.
/// This is a compiler primitive only, not a resolver runtime or GraphQL server.
pub fn graphql_query_from_str(input: &str) -> Result<HybridQuery> {
    let body = graphql_operation_body(input)?;
    let (table, arguments) = graphql_root_selection(body)?;
    let argument_pairs = split_graphql_top_level(arguments, ',')?;

    let mut tenant_id = None;
    let mut text = None;
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
            "near" | "vector" => {
                vector = Some(parse_graphql_vector(value, name)?);
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
        text,
        vector,
        scalar_eq,
        graph_seed: None,
        temporal_as_of: None,
        top_k,
        freshness,
        explain,
    })
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
        output.push_str("    text: String\n");
        output.push_str("    near: [Float!]\n");
        output.push_str("    vector: [Float!]\n");
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

    for (index, ch) in input[open_index..].char_indices() {
        let index = open_index + index;
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
    match parse_graphql_value(input)? {
        Value::String(value) if !value.is_empty() => Ok(value),
        _ => Err(invalid_graphql_adapter(format!(
            "{name} must be a non-empty string"
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
            scalar_eq.insert(field.to_string(), value);
        }
    }

    Ok(HybridQuery {
        table: table.to_string(),
        tenant_id: tenant_id.ok_or_else(|| invalid_sqlish("tenant_id is required"))?,
        text: None,
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
        "JOIN", "GROUP", "ORDER", "HAVING", "UNION", "INSERT", "UPDATE", "DELETE", "DROP",
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
    use tracedb_core::VersionId;
    use tracedb_store::RecordHeader;

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
        assert_eq!(query.text.as_deref(), Some("agent memory"));
        assert_eq!(query.vector, Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(query.freshness, FreshnessMode::Lazy);
        assert_eq!(query.top_k, 20);
        assert!(query.explain);
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
    fn graphql_query_string_compiles_to_hybrid_query() {
        let query = graphql_query_from_str(
            r#"
            query SearchDocs {
              docs(
                tenant_id: "tenant-a",
                where: { category: "code", status: "reviewed" },
                match: "TraceDB",
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
        assert_eq!(query.text.as_deref(), Some("TraceDB"));
        assert_eq!(query.vector, Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(query.top_k, 7);
        assert_eq!(query.freshness, FreshnessMode::AllowDirty);
        assert!(query.explain);
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
            "lexical cache",
        );
        let second_small = db.score_prepared_lexical_corpus(
            &schema,
            "tenant-a",
            "{}",
            &small_records,
            &[],
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
            "lexical cache",
        );
        let second_large = db.score_prepared_lexical_corpus(
            &schema,
            "tenant-a",
            "large",
            &large_records,
            &[],
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
                text: Some("sealed evidence".to_string()),
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
        let expected = u32::from_le_bytes(
            body[CHECKPOINT_MAGIC_V3.len()..CHECKPOINT_MAGIC_V3.len() + 4]
                .try_into()
                .expect("checksum bytes"),
        );
        let payload = &body[CHECKPOINT_MAGIC_V3.len() + 4..];
        assert_eq!(expected, checksum_bytes(payload));
    }
}
