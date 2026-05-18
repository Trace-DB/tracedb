#![forbid(unsafe_code)]

use tracedb_bench::{
    run_batch_write_attribution, run_inprocess_scaling, BatchWriteAttributionConfig,
    BenchmarkTarget, InProcessScalingConfig, WorkloadKind,
};

fn main() {
    if std::env::var("TRACEDB_BENCH_MODE").as_deref() == Ok("batch-write-attribution") {
        let report = run_batch_write_attribution(BatchWriteAttributionConfig {
            record_targets: parse_record_targets(
                &std::env::var("TRACEDB_BENCH_RECORD_TARGETS")
                    .unwrap_or_else(|_| "1024,4096".to_string()),
            ),
            repetitions: parse_usize_env("TRACEDB_BENCH_BATCH_REPETITIONS", 3),
        })
        .expect("batch write attribution benchmark");
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("json report")
        );
        return;
    }

    if std::env::var("TRACEDB_BENCH_MODE").as_deref() == Ok("inprocess-scaling") {
        let report = run_inprocess_scaling(InProcessScalingConfig {
            record_targets: parse_record_targets(
                &std::env::var("TRACEDB_BENCH_RECORD_TARGETS")
                    .unwrap_or_else(|_| "128,512,1024".to_string()),
            ),
            open_repetitions: parse_usize_env("TRACEDB_BENCH_OPEN_REPETITIONS", 5),
            query_repetitions: parse_usize_env("TRACEDB_BENCH_QUERY_REPETITIONS", 3),
            checkpoint_at_points: std::env::var("TRACEDB_BENCH_CHECKPOINT_AT_POINTS")
                .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        })
        .expect("inprocess scaling benchmark");
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("json report")
        );
        return;
    }

    let workload =
        std::env::var("TRACEDB_BENCH_WORKLOAD").unwrap_or_else(|_| "ai-chat-memory".to_string());
    let records = std::env::var("TRACEDB_BENCH_RECORDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100_000);
    let target = BenchmarkTarget::new(workload_kind(&workload), records);
    println!(
        "{}",
        serde_json::json!({
            "ok": true,
            "benchmark": target.name(),
            "records": records,
            "baselines": target.baselines(),
        })
    );
}

fn parse_record_targets(raw: &str) -> Vec<usize> {
    raw.split(',')
        .filter_map(|part| part.trim().parse::<usize>().ok())
        .collect()
}

fn parse_usize_env(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn workload_kind(workload: &str) -> WorkloadKind {
    match workload {
        "search-rag-6" => WorkloadKind::SearchRag6,
        "postgres-relational" => WorkloadKind::PostgresRelational,
        "pgvector-hybrid" => WorkloadKind::PgVectorHybrid,
        "mongo-document" => WorkloadKind::MongoDocument,
        "opensearch-lexical" => WorkloadKind::OpenSearchLexical,
        "qdrant-vector" => WorkloadKind::QdrantVector,
        "tracedb-falsification" => WorkloadKind::TraceDbFalsification,
        "code-search" => WorkloadKind::CodeSearch,
        "graph-rag" => WorkloadKind::GraphRag,
        "filtered-hybrid-search" => WorkloadKind::FilteredHybridSearch,
        "multi-tenant-semantic-search" => WorkloadKind::MultiTenantSemanticSearch,
        _ => WorkloadKind::AiChatMemory,
    }
}
