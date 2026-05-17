#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
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
use tracedb_log::{CommitRecord, TornWalTail, Wal};
use tracedb_modules::{ModuleRegistry, RegisteredModule};
use tracedb_planner::{
    plan_trace_query, AccessPath, AccessPathDescriptor as PlannerAccessPathDescriptor,
    AccessPathExplain, Candidate, CandidateBatch, CandidateBudget, CostEstimate, ExplainOutput,
    FeatureFreshness, PlannerFeedback, Predicates, QueryFragment, QueryOutput, QueryRow,
    ScoreComponents, Stats, TraceQuery, WorkBudget,
};
use tracedb_segment::SegmentRecord;
use tracedb_store::{ReadSnapshot, RecordStore, StoredRecord};

const CHECKPOINT_MAGIC_V2: &[u8; 8] = b"TDBCHK01";
const CHECKPOINT_MAGIC_V3: &[u8; 8] = b"TDBCHK02";
const CHECKPOINT_FORMAT_VERSION: u32 = 3;
const CHECKPOINT_LEGACY_COMPACT_FORMAT_VERSION: u32 = 2;
const CHECKPOINT_LEGACY_JSON_FORMAT_VERSION: u32 = 1;

pub use tracedb_core::{
    FeatureInvalidation, ModuleManifest, RecordDeletion, RecordInput, TableSchema,
    VectorColumnSchema,
};
pub use tracedb_planner::{ExplainOutput as HybridExplain, QueryOutput as HybridQueryOutput};

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
        })
    }

    pub fn apply_schema(&mut self, schema: TableSchema) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
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
        Ok(epoch)
    }

    pub fn insert(&mut self, input: RecordInput) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
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
        Ok(epoch)
    }

    pub fn put(&mut self, request: RecordPutRequest) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
        let input = request.record;
        let schema = self
            .manifest
            .table(&input.table)
            .ok_or_else(|| TraceDbError::UnknownTable(input.table.clone()))?
            .clone();
        let epoch = self.manifest.latest_epoch.next();
        let mut staged = self.store.clone();
        staged.apply_replacement(&schema, &input, epoch)?;
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
        Ok(epoch)
    }

    pub fn replace(&mut self, request: RecordPutRequest) -> Result<Epoch> {
        self.put(request)
    }

    pub fn patch(&mut self, request: RecordPatchRequest) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
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
        Ok(epoch)
    }

    pub fn delete(&mut self, request: RecordDeleteRequest) -> Result<Epoch> {
        let _guard = WriteLock::acquire(&self.dir)?;
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
        let schema = self
            .manifest
            .table(&query.table)
            .ok_or_else(|| TraceDbError::UnknownTable(query.table.clone()))?;
        validate_vector_query_dimensions(schema, query.vector.as_deref())?;
        validate_scalar_eq_predicates(schema, &query.scalar_eq)?;
        if query.top_k == 0 {
            return Ok(QueryOutput {
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
            });
        }
        let visible = self.store.visible_records_at(
            &query.table,
            &query.tenant_id,
            self.manifest.latest_epoch,
        );
        let sealed_records = self.sealed_segment_records(&query.table, &query.tenant_id)?;
        let tenant_mask_visible_records = visible.len();
        let scalar_filter_applied = !query.scalar_eq.is_empty();
        let visible = filter_records_by_scalar_eq(visible, &query.scalar_eq);
        let sealed_records = filter_segment_records_by_scalar_eq(sealed_records, &query.scalar_eq);
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
            candidate_budget: query.top_k.saturating_mul(4).max(query.top_k).max(1),
            hot_overlay_searched: true,
            segments_scanned: sealed_records.len(),
            freshness_mode: query.freshness.as_str().to_string(),
            fusion_method: "RRF".to_string(),
            ..ExplainOutput::default()
        };
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

        let query_fragment = QueryFragment {
            text: query.text.clone(),
            vector_dimensions: query.vector.as_ref().map(Vec::len),
        };
        let visibility = visible
            .iter()
            .map(|record| record.header.record_id.clone())
            .chain(sealed_records.iter().map(|record| record.record_id.clone()))
            .collect::<Vec<_>>();
        let access_paths = query_access_paths(QueryAccessInput {
            schema,
            visible: &visible,
            sealed_records: &sealed_records,
            text: query.text.clone(),
            vector_query: query.vector.clone(),
            graph_seed: query.graph_seed.clone(),
            temporal_as_of: query.temporal_as_of,
            freshness: &query.freshness,
            fallback_candidate_limit: query_has_evidence(&query)
                .then_some(explain.candidate_budget),
        });
        let mut planner_candidates = Vec::<Candidate>::new();
        let mut streams = Vec::<RankedStream>::new();
        for access_path in &access_paths {
            let descriptor = access_path.descriptor();
            let batch = access_path.open(
                query_fragment.clone(),
                &visibility,
                CandidateBudget {
                    max_candidates: explain.candidate_budget,
                },
            );
            let source = descriptor.access_path_id.as_str();
            if source == "LexicalPath" {
                explain.text_candidates = batch.candidates.len();
                explain.opened_candidate_streams.push("text".to_string());
                streams.push(ranked_stream_from_candidates("text", &batch.candidates));
            } else if source == "VectorPath" {
                explain.vector_candidates = batch.candidates.len();
                explain.opened_candidate_streams.push("vector".to_string());
                streams.push(ranked_stream_from_candidates("vector", &batch.candidates));
            } else {
                explain
                    .opened_candidate_streams
                    .push(candidate_stream_name(source).to_string());
                streams.push(ranked_stream_from_candidates(
                    candidate_stream_name(source),
                    &batch.candidates,
                ));
            }
            planner_candidates.extend(batch.candidates);
            explain.access_paths.push(access_path.explain());
        }
        explain.planner_candidates = planner_candidates;

        let mut fused = fuse_query_streams(&streams);
        if query.text.is_some() && !lexical_scores_are_tied(&fused) {
            fused.sort_by(lexical_first_order);
        } else if query.vector.is_some() {
            fused.sort_by(vector_first_order);
        }
        explain.deduped_candidate_count = fused.len();
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

        explain.materialized_count = materialized.len();
        explain.final_visibility_guard_count = checked;
        explain.final_visibility_guard_removed = removed;
        explain.returned_count = materialized.len();
        Ok(QueryOutput {
            results: materialized,
            explain,
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
    text: Option<String>,
    vector_query: Option<Vec<f32>>,
    graph_seed: Option<String>,
    temporal_as_of: Option<u64>,
    freshness: &'a FreshnessMode,
    fallback_candidate_limit: Option<usize>,
}

fn query_access_paths(input: QueryAccessInput<'_>) -> Vec<MemoryAccessPath> {
    let QueryAccessInput {
        schema,
        visible,
        sealed_records,
        text,
        vector_query,
        graph_seed,
        temporal_as_of,
        freshness,
        fallback_candidate_limit,
    } = input;
    let mut paths = Vec::new();
    let fallback_candidate_limit = fallback_candidate_limit.unwrap_or(usize::MAX);
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

    let mut lexical_candidates = Vec::new();
    if let Some(text) = text {
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
        let records_by_id = visible
            .iter()
            .map(|record| (record.header.record_id.as_str(), record))
            .collect::<BTreeMap<_, _>>();
        let sealed_by_id = sealed_records
            .iter()
            .map(|record| (record.record_id.as_str(), record))
            .collect::<BTreeMap<_, _>>();
        let mut scored = tracedb_text::score_corpus(&text, &docs);
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

    if let Some(seed) = graph_seed {
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
    }

    if let Some(as_of) = temporal_as_of {
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
    }

    paths
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

fn scalar_value_label(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
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
