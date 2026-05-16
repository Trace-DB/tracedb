use serde_json::json;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use tracedb_bench::{BenchmarkTarget, WorkloadKind};
use tracedb_catalog::{BranchState, Catalog};
use tracedb_features::{FeatureFreshnessMode, FeatureLifecycle};
use tracedb_gateway::{Gateway, GatewayRequest, GatewayServerConfig};
use tracedb_graph::{Edge, GraphStore};
use tracedb_jobs::{JobCatalog, JobKind, JobStatus, WorkerId};
use tracedb_keeper::{BranchWalService, CommitRequest};
use tracedb_metering::{MeterKind, UsageMeter};
use tracedb_policy::{AclEntry, ActorContext, Policy, VisibilityMode, VisibilityOracle};
use tracedb_provenance::{Provenance, RetrievalAudit};
use tracedb_query::TraceDb;
use tracedb_query::{FreshnessMode, HybridQuery, RecordInput, TableSchema, VectorColumnSchema};
use tracedb_retrieval_core::{RetrievalMode, RetrievalOverlay};
use tracedb_schema::{
    ColumnDescriptor, EdgeTableDescriptor, FeatureDescriptor, LogicalType, ModuleRequirement,
    TableDescriptor,
};
use tracedb_sdk::{TraceDbClient, TraceDbClientConfig};
use tracedb_segment::SegmentObject;
use tracedb_segment_server::{ObjectRef, SegmentServer};
use tracedb_std::standard_module_manifest_ids;
use tracedb_temporal::{TemporalIndex, TemporalRange};

#[test]
fn full_standard_module_bundle_is_registered_as_native_not_sidecars() {
    let modules = standard_module_manifest_ids();
    for expected in [
        "tracedb-text",
        "tracedb-vector",
        "tracedb-graph",
        "tracedb-temporal",
        "tracedb-policy",
        "tracedb-provenance",
        "tracedb-features",
        "tracedb-retrieval-core",
    ] {
        assert!(
            modules.contains(&expected.to_string()),
            "missing {expected}"
        );
    }

    let temp = tempfile::tempdir().expect("tempdir");
    let db = TraceDb::open(temp.path()).expect("open db");
    let registered = db.registered_modules();
    for expected in modules {
        assert!(
            registered.contains(&expected),
            "engine did not register {expected}"
        );
    }
}

#[test]
fn typed_table_descriptor_covers_ai_native_data_model() {
    let descriptor = TableDescriptor::new("messages")
        .column(ColumnDescriptor::primary("id", LogicalType::Id))
        .column(ColumnDescriptor::tenant("tenant_id"))
        .column(ColumnDescriptor::new("content", LogicalType::TextIndexed))
        .column(ColumnDescriptor::new(
            "embedding",
            LogicalType::Vector {
                element: "F32".to_string(),
                dimensions: 1536,
                metric: "COSINE".to_string(),
            },
        ))
        .column(ColumnDescriptor::new("valid", LogicalType::TemporalRange))
        .feature(FeatureDescriptor::embedding(
            "embedding",
            vec!["content".to_string()],
            "text-embedding-3-large",
        ))
        .edge_table(EdgeTableDescriptor::new("mentions", "messages"))
        .module_requirement(ModuleRequirement::new("tracedb-vector", "0.1.0"));

    descriptor.validate().expect("descriptor validates");
    assert!(descriptor.requires_module("tracedb-vector"));
    assert_eq!(descriptor.schema_version, 1);
}

#[test]
fn local_manifest_and_wal_carry_branch_and_managed_authority_fields() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut db = TraceDb::open(temp.path()).expect("open db");
    db.apply_schema(TableSchema {
        name: "docs".to_string(),
        primary_id_column: "id".to_string(),
        tenant_id_column: "tenant".to_string(),
        scalar_columns: Vec::new(),
        text_indexed_columns: vec!["body".to_string()],
        vector_columns: vec![VectorColumnSchema {
            name: "embedding".to_string(),
            dimensions: 2,
            source_columns: vec!["body".to_string()],
        }],
    })
    .expect("schema");
    db.insert(RecordInput {
        table: "docs".to_string(),
        id: "a".to_string(),
        tenant_id: "tenant-a".to_string(),
        fields: json!({
            "id": "a",
            "tenant": "tenant-a",
            "body": "branch aware wal",
            "embedding": [1.0, 0.0],
        })
        .as_object()
        .unwrap()
        .clone(),
    })
    .expect("insert");

    let manifest = db.inspect_manifest().expect("manifest");
    assert_eq!(manifest.branch_id, "main");
    assert_eq!(manifest.checkpoint_epoch, manifest.latest_epoch);
    assert!(manifest
        .job_queues
        .contains(&"tracedb.embedding.generate".to_string()));

    let commits = db.inspect_wal().expect("wal");
    assert_eq!(commits[0].commit.database_id, manifest.database_id);
    assert_eq!(commits[0].commit.branch_id, "main");
    assert_eq!(commits[1].commit.parent_epoch.get(), 1);
    assert_eq!(commits[1].commit.commit_marker, "COMMITTED");

    let output = db
        .query(HybridQuery {
            table: "docs".to_string(),
            tenant_id: "tenant-a".to_string(),
            text: Some("branch".to_string()),
            vector: Some(vec![1.0, 0.0]),
            graph_seed: None,
            temporal_as_of: None,
            top_k: 1,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .expect("query");
    assert!(output.explain.exact_fallback_triggered);
    assert_eq!(
        output.explain.selected_strategy.as_deref(),
        Some("PolicyPartitionFirst")
    );
    assert!(output
        .explain
        .module_versions
        .iter()
        .any(|module| module == "tracedb-graph@0.1.0"));
    assert!(output
        .explain
        .skipped_access_paths
        .iter()
        .any(|path| path.starts_with("GraphPath")));
}

#[test]
fn policy_oracle_blocks_forbidden_retrieval_before_candidate_generation() {
    let actor = ActorContext::tenant_user("tenant-a", "user-a");
    let allowed = Policy::tenant("tenant-a")
        .with_visibility(VisibilityMode::Acl)
        .with_acl(vec![AclEntry::allow_user("user-a")]);
    let denied = Policy::tenant("tenant-a")
        .with_visibility(VisibilityMode::Acl)
        .with_acl(vec![AclEntry::allow_user("user-b")])
        .suppress_from_ai(true);

    let oracle = VisibilityOracle;
    assert!(oracle.visible("record-a", 1, &allowed, &actor).allowed);
    let decision = oracle.visible("record-b", 1, &denied, &actor);
    assert!(!decision.allowed);
    assert!(decision.reasons.contains(&"suppress_from_ai".to_string()));
}

#[test]
fn feature_lifecycle_schedules_durable_idempotent_jobs() {
    let mut jobs = JobCatalog::default();
    let mut lifecycle = FeatureLifecycle::new("embedding", vec!["content".to_string()]);
    lifecycle.mark_dirty(2, 44);
    let job = lifecycle
        .enqueue_recompute(&mut jobs, "messages/a")
        .expect("job");
    let duplicate = lifecycle
        .enqueue_recompute(&mut jobs, "messages/a")
        .expect("same job");

    assert_eq!(job.job_id, duplicate.job_id);
    assert_eq!(jobs.depth_by_status(JobStatus::Queued), 1);

    let leased = jobs
        .lease_next(WorkerId::new("worker-1"), JobKind::GenerateEmbedding)
        .expect("leased")
        .expect("job available");
    assert_eq!(leased.status, JobStatus::Leased);
}

#[test]
fn graph_temporal_provenance_and_retrieval_state_are_policy_safe() {
    let actor = ActorContext::tenant_user("tenant-a", "user-a");
    let policy = Policy::tenant("tenant-a");
    let hidden = Policy::tenant("tenant-b");
    let mut graph = GraphStore::default();
    graph.add_edge(Edge::new("e1", "a", "b", "mentions", 0.9, policy.clone()));
    graph.add_edge(Edge::new("e2", "a", "hidden", "mentions", 1.0, hidden));

    let visible = graph.visible_neighbors("a", &actor, &VisibilityOracle);
    assert_eq!(visible, vec!["b".to_string()]);

    let mut temporal = TemporalIndex::default();
    temporal.insert("a", TemporalRange::closed(10, 20));
    assert!(temporal.as_of("a", 15));
    assert!(!temporal.as_of("a", 25));

    let provenance = Provenance::source_uri("docs://trace", "actor-a");
    let audit = RetrievalAudit::new("q1", 4).with_returned(vec!["a".to_string()]);
    assert_eq!(provenance.source_uri.as_deref(), Some("docs://trace"));
    assert_eq!(audit.returned_ids, vec!["a"]);

    let mut overlay = RetrievalOverlay::default();
    overlay.record("a", RetrievalMode::Suppress, "obsolete");
    assert!(overlay.suppression_penalty("a") > 0.0);
}

#[test]
fn managed_plane_contracts_route_through_gateway_keeper_worker_and_metering() {
    let temp = tempfile::tempdir().expect("tempdir");
    let catalog_path = temp.path().join("catalog/catalog.json");
    let mut catalog = Catalog::default();
    let database = catalog
        .create_database("org-a", "project-a", "memory", "us-west")
        .expect("database");
    let branch = catalog
        .create_branch(&database.database_id, "main", None)
        .expect("branch");
    assert_eq!(branch.state, BranchState::Active);
    catalog.save(&catalog_path).expect("save catalog");
    let reloaded = Catalog::load(&catalog_path).expect("load catalog");
    assert_eq!(
        reloaded.branch(&branch.branch_id).expect("durable branch"),
        &branch
    );

    let keeper_path = temp.path().join("keeper/main.json");
    let mut keeper = BranchWalService::open(&keeper_path).expect("keeper");
    let first = keeper
        .commit(CommitRequest::new(&branch.branch_id, "idem-1", "insert a"))
        .expect("first commit");
    let retry = keeper
        .commit(CommitRequest::new(&branch.branch_id, "idem-1", "insert a"))
        .expect("idempotent retry");
    assert_eq!(first.epoch, retry.epoch);
    assert_eq!(keeper.commit_log().len(), 1);
    let mut reopened_keeper = BranchWalService::open(&keeper_path).expect("reopen keeper");
    let durable_retry = reopened_keeper
        .commit(CommitRequest::new(&branch.branch_id, "idem-1", "insert a"))
        .expect("durable idempotent retry");
    assert_eq!(first, durable_retry);
    assert_eq!(reopened_keeper.commit_log().len(), 1);

    let mut meter = UsageMeter::default();
    let gateway = Gateway::new(catalog.clone(), "secret-token");
    let response = gateway
        .route(
            GatewayRequest::query(&database.database_id, &branch.branch_id)
                .with_bearer_token("secret-token"),
            &mut meter,
        )
        .expect("route");
    assert_eq!(response.engine_target.service_name, "tracedb-engine");
    assert_eq!(meter.total(MeterKind::Request), 1);
    let mismatch = gateway
        .route(
            GatewayRequest::query("other-db", &branch.branch_id).with_bearer_token("secret-token"),
            &mut meter,
        )
        .unwrap_err();
    assert!(mismatch.contains("does not belong"));

    let engine_url = spawn_engine_stub();
    let proxied = tracedb_gateway::proxy_engine_request(
        &engine_url,
        "POST",
        "/v1/query",
        br#"{"table":"docs"}"#,
        "application/json",
    )
    .expect("gateway proxy");
    assert_eq!(proxied.status_code, 200);
    assert!(String::from_utf8_lossy(&proxied.body).contains("\"engine\":true"));
    let runtime_meter = Arc::new(Mutex::new(UsageMeter::default()));
    let runtime_config = GatewayServerConfig {
        bind: "127.0.0.1:0".to_string(),
        engine_url: engine_url.clone(),
        required_token: Some("secret-token".to_string()),
        catalog: reloaded,
        meter: Arc::clone(&runtime_meter),
        rate_limit_enabled: true,
        rate_limit_requests: 10,
    };
    let runtime_body = json!({
        "database_id": database.database_id,
        "branch_id": branch.branch_id,
        "table": "docs",
        "tenant_id": "tenant-a",
        "top_k": 1,
        "freshness": "Strict"
    })
    .to_string();
    let runtime_request = format!(
        "POST /v1/query HTTP/1.1\r\ncontent-type: application/json\r\nauthorization: Bearer secret-token\r\ncontent-length: {}\r\n\r\n{}",
        runtime_body.len(),
        runtime_body
    );
    let runtime_response =
        tracedb_gateway::handle_gateway_request_text(&runtime_request, runtime_config);
    assert!(runtime_response.starts_with("HTTP/1.1 200 OK"));
    assert!(runtime_response.contains("\"engine\":true"));
    assert_eq!(runtime_meter.lock().unwrap().total(MeterKind::Request), 1);

    let mut jobs = JobCatalog::default();
    jobs.enqueue(JobKind::VerifyDatabase, "branch/main", "verify-main")
        .expect("enqueue");
    let report = tracedb_worker::run_once_through_engine_api(
        &mut jobs,
        WorkerId::new("worker-1"),
        &engine_url,
    )
    .expect("worker");
    assert!(report.used_private_engine_api);
    assert!(report.engine_health_checked);
    assert_eq!(report.engine_status_code, 200);
}

#[test]
fn graph_and_temporal_query_paths_are_executable_not_only_registered() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut db = TraceDb::open(temp.path()).expect("open db");
    db.apply_schema(TableSchema {
        name: "events".to_string(),
        primary_id_column: "id".to_string(),
        tenant_id_column: "tenant".to_string(),
        scalar_columns: vec![
            "edges".to_string(),
            "valid_from".to_string(),
            "valid_to".to_string(),
        ],
        text_indexed_columns: Vec::new(),
        vector_columns: Vec::new(),
    })
    .unwrap();
    db.insert(RecordInput {
        table: "events".to_string(),
        id: "event-a".to_string(),
        tenant_id: "tenant-a".to_string(),
        fields: json!({
            "id": "event-a",
            "tenant": "tenant-a",
            "edges": ["seed"],
            "valid_from": 10,
            "valid_to": 20,
        })
        .as_object()
        .unwrap()
        .clone(),
    })
    .unwrap();
    let output = db
        .query(HybridQuery {
            table: "events".to_string(),
            tenant_id: "tenant-a".to_string(),
            text: None,
            vector: None,
            graph_seed: Some("seed".to_string()),
            temporal_as_of: Some(15),
            top_k: 5,
            freshness: FreshnessMode::Strict,
            explain: true,
        })
        .unwrap();
    assert!(output
        .explain
        .access_paths
        .iter()
        .any(|path| path.access_path_id == "GraphPath" && path.candidates == 1));
    assert!(output
        .explain
        .access_paths
        .iter()
        .any(|path| path.access_path_id == "TemporalPath" && path.candidates == 1));
    assert!(output
        .explain
        .opened_candidate_streams
        .contains(&"graph".to_string()));
    assert!(output
        .explain
        .opened_candidate_streams
        .contains(&"temporal".to_string()));
}

#[test]
fn segment_server_cache_sdk_and_bench_surfaces_are_executable_contracts() {
    let segment = SegmentObject::minimal("s1", 1).expect("segment");
    assert!(segment
        .module_blocks
        .iter()
        .any(|block| block.module_id == "tracedb-policy"));

    let mut server = SegmentServer::default();
    let object = ObjectRef::new("segments/s1.tseg", 55);
    server.publish(object.clone()).expect("publish");

    let mut cache = tracedb_cache::SegmentCache::new(1);
    cache.insert(object.clone());
    assert!(cache.get("segments/s1.tseg").is_some());

    let client = TraceDbClient::new(TraceDbClientConfig::managed(
        "https://example.tracedb",
        "token",
    ));
    let request = client
        .table("messages")
        .tenant("tenant-a")
        .match_text("content", "hybrid retrieval")
        .near("embedding", vec![1.0, 0.0])
        .freshness(FeatureFreshnessMode::Lazy)
        .limit(10)
        .build();
    assert_eq!(request.top_k, 10);
    assert_eq!(request.text.as_deref(), Some("hybrid retrieval"));
    assert_eq!(request.freshness, "Lazy");

    let target = BenchmarkTarget::new(WorkloadKind::AiChatMemory, 100_000);
    assert_eq!(target.name(), "ai_chat_memory_100000");
}

#[test]
fn railway_deploy_artifacts_exist_for_gateway_engine_worker_and_bench() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    for path in [
        "deploy/railway/README.md",
        "deploy/railway/env.example",
        "deploy/railway/railway.gateway.toml",
        "deploy/railway/railway.engine.toml",
        "deploy/railway/railway.worker.toml",
        "deploy/railway/railway.bench.toml",
        "apps/gateway/README.md",
        "apps/engine/README.md",
        "apps/worker/README.md",
        "apps/bench/README.md",
    ] {
        assert!(root.join(path).exists(), "missing {path}");
    }
}

fn spawn_engine_stub() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind engine stub");
    let addr = listener.local_addr().expect("engine stub address");
    thread::spawn(move || {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().expect("accept engine request");
            let mut buffer = [0u8; 4096];
            let read = stream.read(&mut buffer).expect("read engine request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/");
            let body = if path == "/internal/health" {
                json!({ "ok": true, "service": "tracedb-engine" }).to_string()
            } else {
                json!({ "ok": true, "engine": true, "path": path }).to_string()
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write engine response");
        }
    });
    format!("http://{addr}")
}
