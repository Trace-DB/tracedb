#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;
use std::fs;
use std::time::Instant;
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordInput, TableSchema, TraceDb, VectorColumnSchema,
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
pub struct InProcessScalingPoint {
    pub records: usize,
    pub latest_epoch: u64,
    pub wal_bytes: u64,
    pub data_dir_bytes: u64,
    pub insert_p95_ms: f64,
    pub recent_insert_p95_ms: f64,
    pub engine_query_p95_ms: f64,
    pub query_returned_count: usize,
    pub engine_open_p95_ms: Option<f64>,
    pub checkpoint_latency_ms: Option<f64>,
    pub checkpoint_wal_bytes: Option<u64>,
    pub checkpoint_data_dir_bytes: Option<u64>,
    pub checkpoint_engine_open_p95_ms: Option<f64>,
    pub checkpoint_engine_query_p95_ms: Option<f64>,
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
    let mut points = Vec::new();
    let mut next_target = 0;
    let max_records = *targets.last().expect("target");
    for index in 1..=max_records {
        let record = scaling_record(index);
        insert_latencies.push(timed_ms(|| {
            db.put(tracedb_query::RecordPutRequest::new(record))
        })?);

        while next_target < targets.len() && index == targets[next_target] {
            points.push(measure_inprocess_point(
                &mut db,
                &data_dir,
                index,
                &insert_latencies,
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
    config: &InProcessScalingConfig,
) -> Result<InProcessScalingPoint, Box<dyn Error>> {
    let query = scaling_query(records);
    let mut query_latencies = Vec::new();
    let mut returned_count = 0;
    for _ in 0..config.query_repetitions {
        let output = timed_value_ms(|| db.query(query.clone()))?;
        query_latencies.push(output.1);
        returned_count = output.0.results.len();
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
        engine_query_p95_ms: round_ms(percentile(&query_latencies, 95.0)),
        query_returned_count: returned_count,
        engine_open_p95_ms: Some(round_ms(percentile(&open_latencies, 95.0))),
        checkpoint_latency_ms: None,
        checkpoint_wal_bytes: None,
        checkpoint_data_dir_bytes: None,
        checkpoint_engine_open_p95_ms: None,
        checkpoint_engine_query_p95_ms: None,
    };

    if config.checkpoint_at_points {
        let checkpoint_latency = timed_ms(|| db.checkpoint())?;
        let mut checkpoint_open_latencies = Vec::new();
        let mut checkpoint_query_latencies = Vec::new();
        for _ in 0..config.open_repetitions {
            let opened = timed_value_ms(|| TraceDb::open(data_dir))?;
            checkpoint_open_latencies.push(opened.1);
            let opened_db = opened.0;
            for _ in 0..config.query_repetitions {
                let output = timed_value_ms(|| opened_db.query(query.clone()))?;
                checkpoint_query_latencies.push(output.1);
            }
        }
        point.checkpoint_latency_ms = Some(round_ms(checkpoint_latency));
        point.checkpoint_wal_bytes = Some(wal_bytes(data_dir));
        point.checkpoint_data_dir_bytes = Some(directory_size(data_dir));
        point.checkpoint_engine_open_p95_ms =
            Some(round_ms(percentile(&checkpoint_open_latencies, 95.0)));
        point.checkpoint_engine_query_p95_ms =
            Some(round_ms(percentile(&checkpoint_query_latencies, 95.0)));
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

fn timed_ms<T, E>(f: impl FnOnce() -> Result<T, E>) -> Result<f64, E> {
    let start = Instant::now();
    f().map(|_| start.elapsed().as_secs_f64() * 1000.0)
}

fn timed_value_ms<T, E>(f: impl FnOnce() -> Result<T, E>) -> Result<(T, f64), E> {
    let start = Instant::now();
    f().map(|value| (value, start.elapsed().as_secs_f64() * 1000.0))
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
        assert!(point.engine_query_p95_ms > 0.0);
        assert!(point.checkpoint_engine_open_p95_ms.unwrap() > 0.0);
        assert!(point.checkpoint_engine_query_p95_ms.unwrap() > 0.0);
    }
}
