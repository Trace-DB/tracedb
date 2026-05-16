#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

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
