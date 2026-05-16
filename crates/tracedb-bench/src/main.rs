#![forbid(unsafe_code)]

use tracedb_bench::{BenchmarkTarget, WorkloadKind};

fn main() {
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
