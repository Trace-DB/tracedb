#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::time::Instant;
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordInput, TableSchema, TraceDb, VectorColumnSchema,
    WritePathTiming,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkloadKind {
    AiChatMemory,
    MultiTenantSemanticSearch,
    CodeSearch,
    GraphRag,
    FilteredHybridSearch,
    SearchRag6,
    PostgresRelational,
    PgVectorHybrid,
    MongoDocument,
    OpenSearchLexical,
    QdrantVector,
    TraceDbFalsification,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BaselineKind {
    TraceDb,
    Postgres,
    PgVector,
    MongoDb,
    Qdrant,
    OpenSearch,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkTarget {
    pub workload: WorkloadKind,
    pub records: usize,
}

impl BenchmarkTarget {
    pub fn new(workload: WorkloadKind, records: usize) -> Self {
        Self { workload, records }
    }

    pub fn name(&self) -> String {
        let workload = match self.workload {
            WorkloadKind::AiChatMemory => "ai_chat_memory",
            WorkloadKind::MultiTenantSemanticSearch => "multi_tenant_semantic_search",
            WorkloadKind::CodeSearch => "code_search",
            WorkloadKind::GraphRag => "graph_rag",
            WorkloadKind::FilteredHybridSearch => "filtered_hybrid_search",
            WorkloadKind::SearchRag6 => "search_rag_6",
            WorkloadKind::PostgresRelational => "postgres_relational",
            WorkloadKind::PgVectorHybrid => "pgvector_hybrid",
            WorkloadKind::MongoDocument => "mongo_document",
            WorkloadKind::OpenSearchLexical => "opensearch_lexical",
            WorkloadKind::QdrantVector => "qdrant_vector",
            WorkloadKind::TraceDbFalsification => "tracedb_falsification",
        };
        format!("{workload}_{}", self.records)
    }

    pub fn baselines(&self) -> Vec<BaselineKind> {
        match self.workload {
            WorkloadKind::SearchRag6 => vec![
                BaselineKind::TraceDb,
                BaselineKind::Postgres,
                BaselineKind::PgVector,
                BaselineKind::MongoDb,
                BaselineKind::Qdrant,
                BaselineKind::OpenSearch,
            ],
            WorkloadKind::PostgresRelational => vec![BaselineKind::TraceDb, BaselineKind::Postgres],
            WorkloadKind::PgVectorHybrid => vec![BaselineKind::TraceDb, BaselineKind::PgVector],
            WorkloadKind::MongoDocument => vec![BaselineKind::TraceDb, BaselineKind::MongoDb],
            WorkloadKind::OpenSearchLexical => {
                vec![BaselineKind::TraceDb, BaselineKind::OpenSearch]
            }
            WorkloadKind::QdrantVector => vec![BaselineKind::TraceDb, BaselineKind::Qdrant],
            _ => vec![BaselineKind::TraceDb],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InProcessScalingConfig {
    pub record_targets: Vec<usize>,
    pub open_repetitions: usize,
    pub query_repetitions: usize,
    pub checkpoint_at_points: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InProcessScalingReport {
    pub benchmark: String,
    pub point_count: usize,
    pub max_records: usize,
    pub points: Vec<InProcessScalingPoint>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimingP95 {
    pub name: String,
    pub p95_ms: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InProcessScalingPoint {
    pub records: usize,
    pub latest_epoch: u64,
    pub wal_bytes: u64,
    pub data_dir_bytes: u64,
    pub insert_p95_ms: f64,
    pub recent_insert_p95_ms: f64,
    #[serde(default)]
    pub engine_query_p50_ms: f64,
    pub engine_query_p95_ms: f64,
    #[serde(default)]
    pub engine_query_p99_ms: f64,
    #[serde(default)]
    pub rare_lexical_query_p50_ms: f64,
    #[serde(default)]
    pub rare_lexical_query_p95_ms: f64,
    #[serde(default)]
    pub rare_lexical_query_p99_ms: f64,
    #[serde(default)]
    pub common_lexical_query_p50_ms: f64,
    #[serde(default)]
    pub common_lexical_query_p95_ms: f64,
    #[serde(default)]
    pub common_lexical_query_p99_ms: f64,
    #[serde(default)]
    pub query_phase_p95_ms: Vec<TimingP95>,
    #[serde(default)]
    pub query_access_path_build_p95_ms: Vec<TimingP95>,
    #[serde(default)]
    pub query_access_path_open_p95_ms: Vec<TimingP95>,
    #[serde(default)]
    pub put_phase_p95_ms: Vec<TimingP95>,
    #[serde(default)]
    pub recent_put_phase_p95_ms: Vec<TimingP95>,
    pub query_returned_count: usize,
    pub lexical_cache_hits: usize,
    pub lexical_cache_misses: usize,
    pub lexical_indexed_documents: usize,
    pub lexical_scored_documents: usize,
    pub engine_open_p95_ms: Option<f64>,
    pub checkpoint_latency_ms: Option<f64>,
    pub checkpoint_wal_bytes: Option<u64>,
    pub checkpoint_data_dir_bytes: Option<u64>,
    pub checkpoint_engine_open_p95_ms: Option<f64>,
    pub checkpoint_engine_query_p50_ms: Option<f64>,
    pub checkpoint_engine_query_p95_ms: Option<f64>,
    pub checkpoint_engine_query_p99_ms: Option<f64>,
    pub checkpoint_rare_lexical_query_p50_ms: Option<f64>,
    pub checkpoint_rare_lexical_query_p95_ms: Option<f64>,
    pub checkpoint_rare_lexical_query_p99_ms: Option<f64>,
    pub checkpoint_common_lexical_query_p50_ms: Option<f64>,
    pub checkpoint_common_lexical_query_p95_ms: Option<f64>,
    pub checkpoint_common_lexical_query_p99_ms: Option<f64>,
    pub checkpoint_lexical_cache_hits: Option<usize>,
    pub checkpoint_lexical_cache_misses: Option<usize>,
    pub checkpoint_lexical_indexed_documents: Option<usize>,
    pub checkpoint_lexical_scored_documents: Option<usize>,
    #[serde(default)]
    pub checkpoint_query_phase_p95_ms: Option<Vec<TimingP95>>,
    #[serde(default)]
    pub checkpoint_access_path_build_p95_ms: Option<Vec<TimingP95>>,
    #[serde(default)]
    pub checkpoint_access_path_open_p95_ms: Option<Vec<TimingP95>>,
}

pub fn run_inprocess_scaling(
    config: InProcessScalingConfig,
) -> Result<InProcessScalingReport, Box<dyn Error>> {
    if config.record_targets.is_empty() {
        return Err("at least one record target is required".into());
    }
    if config.open_repetitions == 0 || config.query_repetitions == 0 {
        return Err("open and query repetitions must be positive".into());
    }
    let mut targets = config.record_targets.clone();
    targets.sort_unstable();
    targets.dedup();
    if targets.contains(&0) {
        return Err("record targets must be positive".into());
    }

    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().to_path_buf();
    let mut db = TraceDb::open(&data_dir)?;
    db.apply_schema(scaling_schema())?;

    let mut insert_latencies = Vec::new();
    let mut put_phase_samples = BTreeMap::<String, Vec<f64>>::new();
    let mut points = Vec::new();
    let mut next_target = 0;
    let max_records = *targets.last().expect("target");
    for index in 1..=max_records {
        let record = scaling_record(index);
        let ((_, write_timing), elapsed_ms) = timed_value_ms(|| {
            db.put_with_write_timing(tracedb_query::RecordPutRequest::new(record))
        })?;
        insert_latencies.push(elapsed_ms);
        record_write_timing_samples(&write_timing, &mut put_phase_samples);

        while next_target < targets.len() && index == targets[next_target] {
            points.push(measure_inprocess_point(
                &mut db,
                &data_dir,
                index,
                &insert_latencies,
                &put_phase_samples,
                &config,
            )?);
            next_target += 1;
        }
    }

    Ok(InProcessScalingReport {
        benchmark: "tracedb-inprocess-scaling".to_string(),
        point_count: points.len(),
        max_records,
        points,
    })
}

fn measure_inprocess_point(
    db: &mut TraceDb,
    data_dir: &std::path::Path,
    records: usize,
    insert_latencies: &[f64],
    put_phase_samples: &BTreeMap<String, Vec<f64>>,
    config: &InProcessScalingConfig,
) -> Result<InProcessScalingPoint, Box<dyn Error>> {
    let query = scaling_query(records);
    let rare_lexical_query = rare_lexical_scaling_query(records);
    let common_lexical_query = common_lexical_scaling_query();
    let mut query_latencies = Vec::new();
    let mut rare_lexical_query_latencies = Vec::new();
    let mut common_lexical_query_latencies = Vec::new();
    let mut query_phase_samples = BTreeMap::<String, Vec<f64>>::new();
    let mut query_access_path_build_samples = BTreeMap::<String, Vec<f64>>::new();
    let mut query_access_path_open_samples = BTreeMap::<String, Vec<f64>>::new();
    let mut returned_count = 0;
    let mut lexical_cache_hits = 0;
    let mut lexical_cache_misses = 0;
    let mut lexical_indexed_documents = 0;
    let mut lexical_scored_documents = 0;
    for _ in 0..config.query_repetitions {
        let output = timed_value_ms(|| db.query(query.clone()))?;
        query_latencies.push(output.1);
        record_explain_timing_samples(
            &output.0.explain,
            &mut query_phase_samples,
            &mut query_access_path_build_samples,
            &mut query_access_path_open_samples,
        );
        returned_count = output.0.results.len();
        lexical_cache_hits += output.0.explain.lexical_cache_hits;
        lexical_cache_misses += output.0.explain.lexical_cache_misses;
        lexical_indexed_documents =
            lexical_indexed_documents.max(output.0.explain.lexical_indexed_documents);
        lexical_scored_documents =
            lexical_scored_documents.max(output.0.explain.lexical_scored_documents);
    }
    for _ in 0..config.query_repetitions {
        rare_lexical_query_latencies.push(timed_ms(|| db.query(rare_lexical_query.clone()))?);
    }
    for _ in 0..config.query_repetitions {
        common_lexical_query_latencies.push(timed_ms(|| db.query(common_lexical_query.clone()))?);
    }

    let mut open_latencies = Vec::new();
    for _ in 0..config.open_repetitions {
        open_latencies.push(timed_ms(|| TraceDb::open(data_dir))?);
    }

    let latest_epoch = db.inspect_manifest()?.latest_epoch.get();
    let current_wal_bytes = wal_bytes(data_dir);
    let data_dir_bytes = directory_size(data_dir);
    let recent_start = insert_latencies.len().saturating_sub(64);
    let mut point = InProcessScalingPoint {
        records,
        latest_epoch,
        wal_bytes: current_wal_bytes,
        data_dir_bytes,
        insert_p95_ms: round_ms(percentile(insert_latencies, 95.0)),
        recent_insert_p95_ms: round_ms(percentile(&insert_latencies[recent_start..], 95.0)),
        engine_query_p50_ms: round_ms(percentile(&query_latencies, 50.0)),
        engine_query_p95_ms: round_ms(percentile(&query_latencies, 95.0)),
        engine_query_p99_ms: round_ms(percentile(&query_latencies, 99.0)),
        rare_lexical_query_p50_ms: round_ms(percentile(&rare_lexical_query_latencies, 50.0)),
        rare_lexical_query_p95_ms: round_ms(percentile(&rare_lexical_query_latencies, 95.0)),
        rare_lexical_query_p99_ms: round_ms(percentile(&rare_lexical_query_latencies, 99.0)),
        common_lexical_query_p50_ms: round_ms(percentile(&common_lexical_query_latencies, 50.0)),
        common_lexical_query_p95_ms: round_ms(percentile(&common_lexical_query_latencies, 95.0)),
        common_lexical_query_p99_ms: round_ms(percentile(&common_lexical_query_latencies, 99.0)),
        query_phase_p95_ms: timing_p95_samples(&query_phase_samples),
        query_access_path_build_p95_ms: timing_p95_samples(&query_access_path_build_samples),
        query_access_path_open_p95_ms: timing_p95_samples(&query_access_path_open_samples),
        put_phase_p95_ms: timing_p95_samples(put_phase_samples),
        recent_put_phase_p95_ms: timing_p95_recent_samples(put_phase_samples, 64),
        query_returned_count: returned_count,
        lexical_cache_hits,
        lexical_cache_misses,
        lexical_indexed_documents,
        lexical_scored_documents,
        engine_open_p95_ms: Some(round_ms(percentile(&open_latencies, 95.0))),
        checkpoint_latency_ms: None,
        checkpoint_wal_bytes: None,
        checkpoint_data_dir_bytes: None,
        checkpoint_engine_open_p95_ms: None,
        checkpoint_engine_query_p50_ms: None,
        checkpoint_engine_query_p95_ms: None,
        checkpoint_engine_query_p99_ms: None,
        checkpoint_rare_lexical_query_p50_ms: None,
        checkpoint_rare_lexical_query_p95_ms: None,
        checkpoint_rare_lexical_query_p99_ms: None,
        checkpoint_common_lexical_query_p50_ms: None,
        checkpoint_common_lexical_query_p95_ms: None,
        checkpoint_common_lexical_query_p99_ms: None,
        checkpoint_lexical_cache_hits: None,
        checkpoint_lexical_cache_misses: None,
        checkpoint_lexical_indexed_documents: None,
        checkpoint_lexical_scored_documents: None,
        checkpoint_query_phase_p95_ms: None,
        checkpoint_access_path_build_p95_ms: None,
        checkpoint_access_path_open_p95_ms: None,
    };

    if config.checkpoint_at_points {
        let checkpoint_latency = timed_ms(|| db.checkpoint())?;
        let mut checkpoint_open_latencies = Vec::new();
        let mut checkpoint_query_latencies = Vec::new();
        let mut checkpoint_rare_lexical_query_latencies = Vec::new();
        let mut checkpoint_common_lexical_query_latencies = Vec::new();
        let mut checkpoint_phase_samples = BTreeMap::<String, Vec<f64>>::new();
        let mut checkpoint_access_path_build_samples = BTreeMap::<String, Vec<f64>>::new();
        let mut checkpoint_access_path_open_samples = BTreeMap::<String, Vec<f64>>::new();
        let mut checkpoint_lexical_cache_hits = 0;
        let mut checkpoint_lexical_cache_misses = 0;
        let mut checkpoint_lexical_indexed_documents = 0;
        let mut checkpoint_lexical_scored_documents = 0;
        for _ in 0..config.open_repetitions {
            let opened = timed_value_ms(|| TraceDb::open(data_dir))?;
            checkpoint_open_latencies.push(opened.1);
            let opened_db = opened.0;
            for _ in 0..config.query_repetitions {
                let output = timed_value_ms(|| opened_db.query(query.clone()))?;
                checkpoint_query_latencies.push(output.1);
                record_explain_timing_samples(
                    &output.0.explain,
                    &mut checkpoint_phase_samples,
                    &mut checkpoint_access_path_build_samples,
                    &mut checkpoint_access_path_open_samples,
                );
                checkpoint_lexical_cache_hits += output.0.explain.lexical_cache_hits;
                checkpoint_lexical_cache_misses += output.0.explain.lexical_cache_misses;
                checkpoint_lexical_indexed_documents = checkpoint_lexical_indexed_documents
                    .max(output.0.explain.lexical_indexed_documents);
                checkpoint_lexical_scored_documents = checkpoint_lexical_scored_documents
                    .max(output.0.explain.lexical_scored_documents);
            }
            for _ in 0..config.query_repetitions {
                checkpoint_rare_lexical_query_latencies
                    .push(timed_ms(|| opened_db.query(rare_lexical_query.clone()))?);
            }
            for _ in 0..config.query_repetitions {
                checkpoint_common_lexical_query_latencies
                    .push(timed_ms(|| opened_db.query(common_lexical_query.clone()))?);
            }
        }
        point.checkpoint_latency_ms = Some(round_ms(checkpoint_latency));
        point.checkpoint_wal_bytes = Some(wal_bytes(data_dir));
        point.checkpoint_data_dir_bytes = Some(directory_size(data_dir));
        point.checkpoint_engine_open_p95_ms =
            Some(round_ms(percentile(&checkpoint_open_latencies, 95.0)));
        point.checkpoint_engine_query_p50_ms =
            Some(round_ms(percentile(&checkpoint_query_latencies, 50.0)));
        point.checkpoint_engine_query_p95_ms =
            Some(round_ms(percentile(&checkpoint_query_latencies, 95.0)));
        point.checkpoint_engine_query_p99_ms =
            Some(round_ms(percentile(&checkpoint_query_latencies, 99.0)));
        point.checkpoint_rare_lexical_query_p50_ms = Some(round_ms(percentile(
            &checkpoint_rare_lexical_query_latencies,
            50.0,
        )));
        point.checkpoint_rare_lexical_query_p95_ms = Some(round_ms(percentile(
            &checkpoint_rare_lexical_query_latencies,
            95.0,
        )));
        point.checkpoint_rare_lexical_query_p99_ms = Some(round_ms(percentile(
            &checkpoint_rare_lexical_query_latencies,
            99.0,
        )));
        point.checkpoint_common_lexical_query_p50_ms = Some(round_ms(percentile(
            &checkpoint_common_lexical_query_latencies,
            50.0,
        )));
        point.checkpoint_common_lexical_query_p95_ms = Some(round_ms(percentile(
            &checkpoint_common_lexical_query_latencies,
            95.0,
        )));
        point.checkpoint_common_lexical_query_p99_ms = Some(round_ms(percentile(
            &checkpoint_common_lexical_query_latencies,
            99.0,
        )));
        point.checkpoint_lexical_cache_hits = Some(checkpoint_lexical_cache_hits);
        point.checkpoint_lexical_cache_misses = Some(checkpoint_lexical_cache_misses);
        point.checkpoint_lexical_indexed_documents = Some(checkpoint_lexical_indexed_documents);
        point.checkpoint_lexical_scored_documents = Some(checkpoint_lexical_scored_documents);
        point.checkpoint_query_phase_p95_ms = Some(timing_p95_samples(&checkpoint_phase_samples));
        point.checkpoint_access_path_build_p95_ms =
            Some(timing_p95_samples(&checkpoint_access_path_build_samples));
        point.checkpoint_access_path_open_p95_ms =
            Some(timing_p95_samples(&checkpoint_access_path_open_samples));
    }

    Ok(point)
}

fn scaling_schema() -> TableSchema {
    TableSchema {
        name: "scaling_records".to_string(),
        primary_id_column: "id".to_string(),
        tenant_id_column: "tenant".to_string(),
        scalar_columns: vec!["category".to_string(), "status".to_string()],
        text_indexed_columns: vec!["title".to_string(), "body".to_string()],
        vector_columns: vec![VectorColumnSchema {
            name: "embedding".to_string(),
            dimensions: 8,
            source_columns: vec!["body".to_string()],
        }],
    }
}

fn scaling_record(index: usize) -> RecordInput {
    let tenant = if index.is_multiple_of(2) {
        "tenant-a"
    } else {
        "tenant-b"
    };
    let category = if index.is_multiple_of(3) {
        "memory"
    } else {
        "retrieval"
    };
    let record_id = format!("rec-{index:06}");
    RecordInput {
        table: "scaling_records".to_string(),
        id: record_id.clone(),
        tenant_id: tenant.to_string(),
        fields: json!({
            "id": record_id,
            "tenant": tenant,
            "title": format!("TraceDB scaling record {index}"),
            "body": format!("agent memory vector retrieval policy freshness record {index}"),
            "category": category,
            "status": "active",
            "embedding": [0.1, 0.2, 0.3, 0.4, 0.1, 0.2, 0.3, 0.4],
        })
        .as_object()
        .expect("record object")
        .clone(),
    }
}

fn scaling_query(records: usize) -> HybridQuery {
    HybridQuery {
        table: "scaling_records".to_string(),
        tenant_id: "tenant-a".to_string(),
        text: Some(format!(
            "agent memory vector retrieval policy freshness record {records}"
        )),
        vector: Some(vec![0.1, 0.2, 0.3, 0.4, 0.1, 0.2, 0.3, 0.4]),
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 5,
        freshness: FreshnessMode::AllowDirty,
        explain: true,
    }
}

fn rare_lexical_scaling_query(records: usize) -> HybridQuery {
    HybridQuery {
        table: "scaling_records".to_string(),
        tenant_id: "tenant-a".to_string(),
        text: Some(records.to_string()),
        vector: None,
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 5,
        freshness: FreshnessMode::AllowDirty,
        explain: true,
    }
}

fn common_lexical_scaling_query() -> HybridQuery {
    HybridQuery {
        table: "scaling_records".to_string(),
        tenant_id: "tenant-a".to_string(),
        text: Some("agent memory vector retrieval policy freshness".to_string()),
        vector: None,
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 5,
        freshness: FreshnessMode::AllowDirty,
        explain: true,
    }
}

fn timed_ms<T, E>(f: impl FnOnce() -> Result<T, E>) -> Result<f64, E> {
    let start = Instant::now();
    f().map(|_| start.elapsed().as_secs_f64() * 1000.0)
}

fn timed_value_ms<T, E>(f: impl FnOnce() -> Result<T, E>) -> Result<(T, f64), E> {
    let start = Instant::now();
    f().map(|value| (value, start.elapsed().as_secs_f64() * 1000.0))
}

fn record_explain_timing_samples(
    explain: &tracedb_query::HybridExplain,
    phase_samples: &mut BTreeMap<String, Vec<f64>>,
    access_path_build_samples: &mut BTreeMap<String, Vec<f64>>,
    access_path_open_samples: &mut BTreeMap<String, Vec<f64>>,
) {
    for timing in &explain.phase_timings {
        phase_samples
            .entry(timing.phase.clone())
            .or_default()
            .push(timing.elapsed_ms);
    }
    for timing in &explain.access_path_timings {
        access_path_build_samples
            .entry(timing.access_path_id.clone())
            .or_default()
            .push(timing.build_ms);
        access_path_open_samples
            .entry(timing.access_path_id.clone())
            .or_default()
            .push(timing.open_ms);
    }
}

fn record_write_timing_samples(timing: &WritePathTiming, samples: &mut BTreeMap<String, Vec<f64>>) {
    let total_without_manifest = (timing.total_ms - timing.manifest_total_ms).max(0.0);
    for (name, value) in [
        ("total", timing.total_ms),
        ("total_without_manifest", total_without_manifest),
        ("lock", timing.lock_ms),
        ("schema_lookup", timing.schema_lookup_ms),
        ("store_clone", timing.store_clone_ms),
        ("store_apply", timing.store_apply_ms),
        ("feature_invalidation", timing.feature_invalidation_ms),
        ("commit_build", timing.commit_build_ms),
        ("wal_total", timing.wal_total_ms),
        ("wal_lock_tail", timing.wal_lock_tail_ms),
        ("wal_frame_build", timing.wal_frame_build_ms),
        ("wal_write", timing.wal_write_ms),
        ("wal_sync_data", timing.wal_sync_data_ms),
        ("wal_tail_update", timing.wal_tail_update_ms),
        ("store_install", timing.store_install_ms),
        ("manifest_total", timing.manifest_total_ms),
        ("manifest_clone", timing.manifest_clone_ms),
        ("manifest_write_total", timing.manifest_write_total_ms),
        ("manifest_checksum", timing.manifest_checksum_ms),
        ("manifest_serialize", timing.manifest_serialize_ms),
        ("manifest_write", timing.manifest_write_ms),
        ("manifest_sync_file", timing.manifest_sync_file_ms),
        ("manifest_rename", timing.manifest_rename_ms),
        ("manifest_sync_dir", timing.manifest_sync_dir_ms),
        ("cache_clear", timing.cache_clear_ms),
    ] {
        samples.entry(name.to_string()).or_default().push(value);
    }
}

fn timing_p95_samples(samples: &BTreeMap<String, Vec<f64>>) -> Vec<TimingP95> {
    samples
        .iter()
        .map(|(name, values)| TimingP95 {
            name: name.clone(),
            p95_ms: round_ms(percentile(values, 95.0)),
        })
        .collect()
}

fn timing_p95_recent_samples(
    samples: &BTreeMap<String, Vec<f64>>,
    sample_count: usize,
) -> Vec<TimingP95> {
    samples
        .iter()
        .map(|(name, values)| {
            let recent_start = values.len().saturating_sub(sample_count);
            TimingP95 {
                name: name.clone(),
                p95_ms: round_ms(percentile(&values[recent_start..], 95.0)),
            }
        })
        .collect()
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let rank = ((p / 100.0) * ((sorted.len() - 1) as f64)).round() as usize;
    sorted[rank]
}

fn round_ms(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn wal_bytes(data_dir: &std::path::Path) -> u64 {
    data_dir
        .join("wal")
        .join("000001.twal")
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn directory_size(path: &std::path::Path) -> u64 {
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if let Ok(metadata) = entry.metadata() {
            if metadata.is_dir() {
                total += directory_size(&path);
            } else {
                total += metadata.len();
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inprocess_scaling_report_separates_engine_open_and_query_costs() {
        let config = InProcessScalingConfig {
            record_targets: vec![10],
            open_repetitions: 2,
            query_repetitions: 2,
            checkpoint_at_points: true,
        };

        let report = run_inprocess_scaling(config).expect("inprocess scaling report");

        assert_eq!(report.benchmark, "tracedb-inprocess-scaling");
        assert_eq!(report.points.len(), 1);
        let point = &report.points[0];
        assert_eq!(point.records, 10);
        assert_eq!(point.query_returned_count, 5);
        assert_eq!(point.checkpoint_wal_bytes, Some(0));
        assert!(point.engine_open_p95_ms.is_some());
        assert!(point.engine_query_p50_ms > 0.0);
        assert!(point.engine_query_p95_ms > 0.0);
        assert!(point.engine_query_p99_ms >= point.engine_query_p95_ms);
        assert!(point.rare_lexical_query_p50_ms > 0.0);
        assert!(point.rare_lexical_query_p95_ms > 0.0);
        assert!(point.rare_lexical_query_p99_ms >= point.rare_lexical_query_p95_ms);
        assert!(point.common_lexical_query_p50_ms > 0.0);
        assert!(point.common_lexical_query_p95_ms > 0.0);
        assert!(point.common_lexical_query_p99_ms >= point.common_lexical_query_p95_ms);
        assert!(point.checkpoint_engine_query_p50_ms.unwrap() > 0.0);
        assert!(
            point.checkpoint_engine_query_p99_ms.unwrap()
                >= point.checkpoint_engine_query_p95_ms.unwrap()
        );
        assert!(point.checkpoint_rare_lexical_query_p50_ms.unwrap() > 0.0);
        assert!(
            point.checkpoint_rare_lexical_query_p99_ms.unwrap()
                >= point.checkpoint_rare_lexical_query_p95_ms.unwrap()
        );
        assert!(point.checkpoint_common_lexical_query_p50_ms.unwrap() > 0.0);
        assert!(
            point.checkpoint_common_lexical_query_p99_ms.unwrap()
                >= point.checkpoint_common_lexical_query_p95_ms.unwrap()
        );
        assert!(point
            .query_phase_p95_ms
            .iter()
            .any(|timing| timing.name == "access_path_build" && timing.p95_ms >= 0.0));
        assert!(point
            .query_access_path_build_p95_ms
            .iter()
            .any(|timing| timing.name == "LexicalPath" && timing.p95_ms >= 0.0));
        assert!(point
            .query_access_path_build_p95_ms
            .iter()
            .any(|timing| timing.name == "VectorPath" && timing.p95_ms >= 0.0));
        assert!(point
            .put_phase_p95_ms
            .iter()
            .any(|timing| timing.name == "store_clone" && timing.p95_ms >= 0.0));
        assert!(point
            .put_phase_p95_ms
            .iter()
            .any(|timing| timing.name == "wal_sync_data" && timing.p95_ms >= 0.0));
        assert!(point
            .put_phase_p95_ms
            .iter()
            .any(|timing| timing.name == "manifest_total" && timing.p95_ms >= 0.0));
        assert!(point
            .recent_put_phase_p95_ms
            .iter()
            .any(|timing| timing.name == "store_clone" && timing.p95_ms >= 0.0));
        let recent_total = point
            .recent_put_phase_p95_ms
            .iter()
            .find(|timing| timing.name == "total")
            .expect("recent total write p95")
            .p95_ms;
        let recent_without_manifest = point
            .recent_put_phase_p95_ms
            .iter()
            .find(|timing| timing.name == "total_without_manifest")
            .expect("recent total_without_manifest write p95")
            .p95_ms;
        assert!(
            recent_without_manifest <= recent_total,
            "manifest deferral headroom estimate should not exceed total write latency"
        );
        assert_eq!(point.lexical_cache_hits, 0);
        assert_eq!(point.lexical_cache_misses, 0);
        assert_eq!(point.lexical_indexed_documents, 5);
        assert!(point.checkpoint_engine_open_p95_ms.unwrap() > 0.0);
        assert!(point.checkpoint_engine_query_p95_ms.unwrap() > 0.0);
        assert_eq!(point.checkpoint_lexical_cache_hits.unwrap(), 0);
        assert_eq!(point.checkpoint_lexical_cache_misses.unwrap(), 0);
        assert!(point
            .checkpoint_query_phase_p95_ms
            .as_ref()
            .expect("checkpoint query phase timings")
            .iter()
            .any(|timing| timing.name == "access_path_build" && timing.p95_ms >= 0.0));
    }
}
