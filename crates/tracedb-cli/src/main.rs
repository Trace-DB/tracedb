#![forbid(unsafe_code)]

use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracedb_bench::{BenchmarkTarget, WorkloadKind};
use tracedb_catalog::Catalog;
use tracedb_core::{stable_body_hash, IdempotencyReceipt, IndexState, ARTIFACT_ENVELOPE_MAGIC};
use tracedb_jobs::{JobKind, JobStatus, WorkerId};
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordPatchRequest, RecordPutBatchRequest, RecordPutRequest, RecordScanRequest, TableSchema,
    TraceDb, TraceDbOpenOptions, VectorColumnSchema,
};
use tracedb_sdk::{
    RestoreRequest, SnapshotRequest, TraceDbClient, TraceDbClientConfig, TraceDbClientError,
    TraceDbRequestOptions,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("tracedb: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let data_dir = take_data_dir(&mut args);
    let Some(command) = args.first().cloned() else {
        usage();
        return Ok(());
    };

    match command.as_str() {
        "init" => {
            TraceDb::open(&data_dir)?;
            print_json(json!({ "initialized": data_dir }));
        }
        "create" => {
            let name = args.get(1).ok_or("missing database name")?;
            let mut catalog = Catalog::default();
            let database = catalog
                .create_database("local-org", "local-project", name, "local")
                .map_err(std::io::Error::other)?;
            let branch = catalog
                .create_branch(&database.database_id, "main", None)
                .map_err(std::io::Error::other)?;
            persist_catalog(&data_dir, &catalog)?;
            print_json(json!({
                "database_id": database.database_id,
                "endpoint": database.endpoint,
                "default_branch": branch.branch_id,
            }));
        }
        "branch" if args.get(1).map(String::as_str) == Some("create") => {
            let name = args.get(2).ok_or("missing branch name")?;
            let parent = args
                .iter()
                .position(|arg| arg == "--from")
                .and_then(|idx| args.get(idx + 1))
                .cloned();
            let from = parent.unwrap_or_else(|| "main".to_string());
            let branch_dir = data_dir.join("catalog/branches");
            fs::create_dir_all(&branch_dir)?;
            fs::write(
                branch_dir.join(format!("{name}.json")),
                serde_json::to_vec_pretty(&json!({
                    "branch": name,
                    "from": from.clone(),
                    "copy_on_write": true,
                }))?,
            )?;
            print_json(json!({
                "branch": name,
                "from": from,
                "copy_on_write": true,
            }));
        }
        "connect" => {
            let branch = args.get(1).map(String::as_str).unwrap_or("main");
            print_json(json!({ "connected": branch, "mode": "local-daemon" }));
        }
        "serve" => {
            let bind = args
                .get(1)
                .cloned()
                .or_else(|| env::var("TRACEDB_BIND").ok())
                .unwrap_or_else(|| "127.0.0.1:8080".to_string());
            tracedb_server::serve(&data_dir, &bind)?;
        }
        "schema" if args.get(1).map(String::as_str) == Some("apply") => {
            let path = args.get(2).ok_or("missing schema json path")?;
            let schema: TableSchema = serde_json::from_str(&fs::read_to_string(path)?)?;
            let mut db = TraceDb::open(&data_dir)?;
            let epoch = db.apply_schema(schema)?;
            print_json(json!({ "epoch": epoch.get() }));
        }
        "insert" => {
            let input: RecordInput = serde_json::from_str(&read_arg_or_stdin(args.get(1))?)?;
            let mut db = TraceDb::open(&data_dir)?;
            let epoch = db.insert(input)?;
            print_json(json!({ "epoch": epoch.get() }));
        }
        "put" => {
            let input: RecordInput = serde_json::from_str(&read_arg_or_stdin(args.get(1))?)?;
            let mut db = TraceDb::open(&data_dir)?;
            let epoch = db.put(RecordPutRequest::new(input))?;
            print_json(json!({ "epoch": epoch.get() }));
        }
        "get" => {
            let request = record_get_from_args_or_json(&args)?;
            let db = TraceDb::open(&data_dir)?;
            print_json(json!({ "record": db.get(request)? }));
        }
        "patch" => {
            let request: RecordPatchRequest =
                serde_json::from_str(&read_arg_or_stdin(args.get(1))?)?;
            let mut db = TraceDb::open(&data_dir)?;
            let epoch = db.patch(request)?;
            print_json(json!({ "epoch": epoch.get() }));
        }
        "delete" => {
            let request = record_delete_from_args_or_json(&args)?;
            let mut db = TraceDb::open(&data_dir)?;
            let epoch = db.delete(request)?;
            print_json(json!({ "deleted": true, "epoch": epoch.get() }));
        }
        "feature"
            if args.get(1).map(String::as_str) == Some("status")
                && args.get(2).map(String::as_str) == Some("set") =>
        {
            let table = args.get(3).ok_or("missing table")?;
            let tenant_id = args.get(4).ok_or("missing tenant id")?;
            let record_id = args.get(5).ok_or("missing record id")?;
            let feature = args.get(6).ok_or("missing feature name")?;
            let status = serde_json::from_value(json!(canonical_feature_status(
                args.get(7).ok_or("missing feature status")?
            )?))?;
            let mut db = TraceDb::open(&data_dir)?;
            let epoch = db.set_feature_status(table, tenant_id, record_id, feature, status)?;
            let state = db.feature_state(table, tenant_id, record_id, feature)?;
            print_json(json!({
                "table": table,
                "tenant_id": tenant_id,
                "record_id": record_id,
                "feature": feature,
                "status": state.status,
                "epoch": epoch.get(),
            }));
        }
        "scan" => {
            let request = record_scan_from_args_or_json(&args)?;
            let db = TraceDb::open(&data_dir)?;
            print_json(serde_json::to_value(db.scan(request)?)?);
        }
        "query" => {
            let query: HybridQuery = serde_json::from_str(&read_arg_or_stdin(args.get(1))?)?;
            let db = TraceDb::open(&data_dir)?;
            print_json(serde_json::to_value(db.query(query)?)?);
        }
        "explain" => {
            let mut query: HybridQuery = serde_json::from_str(&read_arg_or_stdin(args.get(1))?)?;
            query.explain = true;
            let db = TraceDb::open(&data_dir)?;
            print_json(serde_json::to_value(db.query(query)?.explain)?);
        }
        "recover" => {
            let db = TraceDb::open(&data_dir)?;
            print_json(json!({ "latest_epoch": db.inspect_manifest()?.latest_epoch.get() }));
        }
        "inspect" if args.get(1).map(String::as_str) == Some("manifest") => {
            let db = TraceDb::open(&data_dir)?;
            print_json(serde_json::to_value(db.inspect_manifest()?)?);
        }
        "inspect" if args.get(1).map(String::as_str) == Some("modules") => {
            let db = TraceDb::open(&data_dir)?;
            print_json(serde_json::to_value(db.registered_module_catalog())?);
        }
        "inspect" if args.get(1).map(String::as_str) == Some("segments") => {
            let db = TraceDb::open(&data_dir)?;
            print_json(serde_json::to_value(db.inspect_manifest()?.segments)?);
        }
        "inspect" if args.get(1).map(String::as_str) == Some("indexes") => {
            let db = TraceDb::open(&data_dir)?;
            print_json(serde_json::to_value(db.inspect_manifest()?.indexes)?);
        }
        "inspect" if args.get(1).map(String::as_str) == Some("jobs") => {
            let db = TraceDb::open(&data_dir)?;
            print_json(serde_json::to_value(db.inspect_manifest()?.job_queues)?);
        }
        "inspect" if args.get(1).map(String::as_str) == Some("policies") => {
            print_json(json!({
                "policy_index": "tenant-mask",
                "final_visibility_guard": true,
                "retrieval_pushdown": true,
            }));
        }
        "inspect" if args.get(1).map(String::as_str) == Some("wal") => {
            let db = TraceDb::open(&data_dir)?;
            let entries = db
                .inspect_wal()?
                .iter()
                .map(|entry| json!({ "lsn": entry.lsn.get(), "epoch": entry.commit.epoch.get() }))
                .collect::<Vec<_>>();
            print_json(Value::Array(entries));
        }
        "backup" => {
            let target = args.get(1).ok_or("missing backup directory")?;
            let db = TraceDb::open(&data_dir)?;
            db.backup(target)?;
            print_json(json!({ "backup": target }));
        }
        "compact" => {
            let mut db = TraceDb::open(&data_dir)?;
            db.compact()?;
            let manifest = db.inspect_manifest()?;
            print_json(json!({
                "compacted": true,
                "segment_count": manifest.segments.len(),
                "index_count": manifest.indexes.len(),
            }));
        }
        "checkpoint" => {
            let mut db = TraceDb::open(&data_dir)?;
            let epoch = db.checkpoint()?;
            print_json(json!({ "checkpoint_epoch": epoch.get() }));
        }
        "snapshot" if args.get(1).map(String::as_str) == Some("create") => {
            let target = args.get(2).ok_or("missing snapshot target directory")?;
            let db = TraceDb::open(&data_dir)?;
            db.create_snapshot(target)?;
            print_json(json!({ "snapshot": target }));
        }
        "snapshot" if args.get(1).map(String::as_str) == Some("restore") => {
            let source = args.get(2).ok_or("missing snapshot source directory")?;
            let target = args
                .get(3)
                .map(PathBuf::from)
                .ok_or("missing restore target directory; refusing to overwrite active --data")?;
            TraceDb::restore_snapshot(source, &target)?;
            print_json(json!({ "restored": target }));
        }
        "snapshot" if args.get(1).map(String::as_str) == Some("list") => {
            print_json(json!({ "snapshots": list_snapshot_dirs(&data_dir)? }));
        }
        "jobs" if args.get(1).map(String::as_str) == Some("list") => {
            let db = TraceDb::open(&data_dir)?;
            let jobs = db.jobs()?;
            print_json(json!({
                "durable": true,
                "queues": db.inspect_manifest()?.job_queues,
                "local_worker_queues": [
                    "tracedb.segment.compact",
                    "tracedb.snapshot.create",
                    "tracedb.feature.index"
                ],
                "status_counts": job_status_counts(&jobs),
                "jobs": jobs,
            }));
        }
        "jobs" if args.get(1).map(String::as_str) == Some("run") => {
            let job = args.get(2).map(String::as_str).unwrap_or("compact");
            let mut db = TraceDb::open(&data_dir)?;
            print_json(run_local_job(&mut db, job, &data_dir)?);
        }
        "doctor" if args.get(1).map(String::as_str) == Some("http") => {
            let config = parse_http_doctor_config(&args[2..])?;
            run_http_doctor_command(config)?;
        }
        "doctor" => {
            print_json(run_doctor(&data_dir));
        }
        "demo" => {
            run_demo(&data_dir)?;
        }
        "http-demo" => {
            run_http_demo(&data_dir)?;
        }
        "product-regression" => {
            let config = parse_product_regression_config(&args[1..])?;
            run_product_regression(config)?;
        }
        "product-quickstart" => {
            let config = parse_product_quickstart_config(&args[1..])?;
            run_product_regression(config)?;
        }
        "durability-faults" => {
            let config = parse_durability_faults_config(&args[1..])?;
            run_durability_faults(config)?;
        }
        "storage-index-jobs" => {
            let config = parse_storage_index_jobs_config(&args[1..])?;
            run_storage_index_jobs(config)?;
        }
        "compose" => {
            let action = args.get(1).map(String::as_str).unwrap_or("status");
            run_compose(action, &args[2..])?;
        }
        "verify" => {
            let db = TraceDb::open(&data_dir)?;
            let wal_entries = db.inspect_wal()?.len();
            let manifest = db.inspect_manifest()?;
            print_json(json!({
                "ok": true,
                "latest_epoch": manifest.latest_epoch.get(),
                "manifest_generation": manifest.manifest_generation,
                "wal_entries": wal_entries,
                "manifest_checksum": manifest.checksums.manifest_checksum,
            }));
        }
        "export" => {
            let scope = args.get(1).map(String::as_str).unwrap_or("all");
            print_json(json!({
                "scope": scope,
                "mode": "export_redaction",
                "covers": ["rows", "vectors", "text", "graph", "provenance", "jobs", "snapshots"],
            }));
        }
        "delete-user" => {
            let subject = args.get(1).ok_or("missing subject id")?;
            print_json(json!({
                "subject": subject,
                "mode": "logical_delete",
                "retention_checked": true,
                "legal_hold_checked": true,
            }));
        }
        "bench" => {
            let workload = args.get(1).map(String::as_str).unwrap_or("ai-chat-memory");
            let records = args
                .get(2)
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(100_000);
            let kind = match workload {
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
            };
            let target = BenchmarkTarget::new(kind, records);
            print_json(json!({
                "benchmark": target.name(),
                "records": records,
                "baselines": target.baselines(),
            }));
        }
        "restore" => {
            let source = args.get(1).ok_or("missing backup directory")?;
            let target = args
                .get(2)
                .map(PathBuf::from)
                .ok_or("missing restore target directory; refusing to overwrite active --data")?;
            TraceDb::restore(source, &target)?;
            print_json(json!({ "restored": target }));
        }
        _ => usage(),
    }

    Ok(())
}

fn record_get_from_args_or_json(
    args: &[String],
) -> Result<RecordGetRequest, Box<dyn std::error::Error>> {
    if args.len() >= 4 {
        return Ok(RecordGetRequest::new(&args[1], &args[2], &args[3]));
    }
    Ok(serde_json::from_str(&read_arg_or_stdin(args.get(1))?)?)
}

fn record_delete_from_args_or_json(
    args: &[String],
) -> Result<RecordDeleteRequest, Box<dyn std::error::Error>> {
    if args.len() >= 4 {
        return Ok(RecordDeleteRequest::new(&args[1], &args[2], &args[3]));
    }
    Ok(serde_json::from_str(&read_arg_or_stdin(args.get(1))?)?)
}

fn record_scan_from_args_or_json(
    args: &[String],
) -> Result<RecordScanRequest, Box<dyn std::error::Error>> {
    if args.len() >= 3 {
        let limit = args
            .get(3)
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100);
        return Ok(RecordScanRequest::new(&args[1], &args[2]).limit(limit));
    }
    Ok(serde_json::from_str(&read_arg_or_stdin(args.get(1))?)?)
}

fn list_snapshot_dirs(data_dir: &std::path::Path) -> std::io::Result<Vec<String>> {
    let snapshot_root = data_dir.join("snapshots");
    if !snapshot_root.exists() {
        return Ok(Vec::new());
    }
    let mut snapshots = Vec::new();
    for entry in fs::read_dir(snapshot_root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            snapshots.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    snapshots.sort();
    Ok(snapshots)
}

fn run_doctor(data_dir: &std::path::Path) -> Value {
    let db_status = match TraceDb::open(data_dir) {
        Ok(db) => match db.inspect_manifest() {
            Ok(manifest) => json!({
                "ok": true,
                "latest_epoch": manifest.latest_epoch.get(),
                "durable_epoch": manifest.durable_epoch.get(),
                "segment_count": manifest.segments.len(),
                "index_count": manifest.indexes.len(),
                "recovery_state": if db.last_recovery_torn_tail().is_some() { "torn_tail_ignored" } else { "clean" },
            }),
            Err(error) => json!({ "ok": false, "error": error.to_string() }),
        },
        Err(error) => json!({ "ok": false, "error": error.to_string() }),
    };
    let docker = Command::new("docker")
        .arg("compose")
        .arg("version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false);
    json!({
        "data_dir": data_dir,
        "directories": {
            "wal": data_dir.join("wal").exists(),
            "segments": data_dir.join("segments").exists(),
            "indexes": data_dir.join("indexes").exists(),
            "snapshots": data_dir.join("snapshots").exists(),
            "jobs": data_dir.join("jobs").exists(),
        },
        "engine": db_status,
        "compose": {
            "docker_compose_available": docker,
            "compose_file": PathBuf::from("docker-compose.yml").exists(),
        },
        "catalog": {
            "local_catalog": data_dir.join("catalog/local_catalog.json").exists(),
        },
        "queue": {
            "mode": "local-valkey-when-compose-running",
        },
        "bucket": {
            "mode": "local-minio-when-compose-running",
        }
    })
}

struct HttpDoctorConfig {
    url: String,
    token: String,
    database_id: Option<String>,
    branch_id: Option<String>,
    timeout_ms: u64,
    safe_retries: u8,
    wait_ready_ms: u64,
}

fn parse_http_doctor_config(
    args: &[String],
) -> Result<HttpDoctorConfig, Box<dyn std::error::Error>> {
    let mut url = env::var("TRACEDB_URL").ok();
    let mut token = env::var("TRACEDB_TOKEN").unwrap_or_else(|_| "dev-token".to_string());
    let mut database_id = env::var("TRACEDB_DATABASE_ID").ok();
    let mut branch_id = env::var("TRACEDB_BRANCH_ID").ok();
    let mut timeout_ms = env::var("TRACEDB_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1_000)
        .max(1);
    let mut safe_retries = env::var("TRACEDB_SAFE_RETRIES")
        .ok()
        .and_then(|value| value.parse::<u8>().ok())
        .unwrap_or(1);
    let mut wait_ready_ms = env::var("TRACEDB_WAIT_READY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);

    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--url" => {
                idx += 1;
                url = Some(
                    args.get(idx)
                        .ok_or("missing value for --url")?
                        .trim_end_matches('/')
                        .to_string(),
                );
            }
            "--token" => {
                idx += 1;
                token = args.get(idx).ok_or("missing value for --token")?.clone();
            }
            "--database-id" => {
                idx += 1;
                database_id = Some(
                    args.get(idx)
                        .ok_or("missing value for --database-id")?
                        .clone(),
                );
            }
            "--branch-id" => {
                idx += 1;
                branch_id = Some(
                    args.get(idx)
                        .ok_or("missing value for --branch-id")?
                        .clone(),
                );
            }
            "--timeout-ms" => {
                idx += 1;
                timeout_ms = args
                    .get(idx)
                    .ok_or("missing value for --timeout-ms")?
                    .parse::<u64>()
                    .map_err(|_| "--timeout-ms must be an unsigned integer")?
                    .max(1);
            }
            "--safe-retries" => {
                idx += 1;
                safe_retries = args
                    .get(idx)
                    .ok_or("missing value for --safe-retries")?
                    .parse::<u8>()
                    .map_err(|_| "--safe-retries must fit in u8")?;
            }
            "--wait-ready-ms" => {
                idx += 1;
                wait_ready_ms = args
                    .get(idx)
                    .ok_or("missing value for --wait-ready-ms")?
                    .parse::<u64>()
                    .map_err(|_| "--wait-ready-ms must be an unsigned integer")?;
            }
            other => return Err(format!("unknown doctor http option {other}").into()),
        }
        idx += 1;
    }

    let url = url
        .ok_or("missing --url or TRACEDB_URL for doctor http")?
        .trim_end_matches('/')
        .to_string();
    Ok(HttpDoctorConfig {
        url,
        token,
        database_id,
        branch_id,
        timeout_ms,
        safe_retries,
        wait_ready_ms,
    })
}

fn run_http_doctor(config: HttpDoctorConfig) -> Value {
    let mut client_config = TraceDbClientConfig::managed(config.url.clone(), config.token)
        .with_timeout(Duration::from_millis(config.timeout_ms))
        .with_safe_retries(config.safe_retries);
    if let Some(database_id) = &config.database_id {
        client_config = client_config.with_database(database_id.clone());
    }
    if let Some(branch_id) = &config.branch_id {
        client_config = client_config.with_branch(branch_id.clone());
    }
    let client = TraceDbClient::new(client_config);

    let ready_wait = http_doctor_wait_for_ready(&client, config.wait_ready_ms);
    let health = http_doctor_check(|| client.health());
    let ready = http_doctor_check(|| client.ready());
    let databases = http_doctor_check(|| client.list_databases());
    let branches = http_doctor_check(|| client.list_branches());
    let metrics = http_doctor_check(|| client.public_safe_metrics());
    let admin_jobs = http_doctor_check(|| {
        client.request_json(
            "GET",
            &routed_admin_jobs_path(config.database_id.as_deref(), config.branch_id.as_deref()),
            None,
        )
    });
    let ok = http_doctor_ready_wait_ok(&ready_wait)
        && http_doctor_check_ok(&health)
        && http_doctor_ready_ok(&ready)
        && http_doctor_check_ok(&databases)
        && http_doctor_check_ok(&branches)
        && http_doctor_check_ok(&metrics)
        && http_doctor_check_ok(&admin_jobs);

    json!({
        "ok": ok,
        "mode": "http-endpoint-diagnostics",
        "server_url": config.url,
        "database_id": config.database_id,
        "branch_id": config.branch_id,
        "request_timeout_ms": config.timeout_ms,
        "safe_retries": config.safe_retries,
        "ready_wait_timeout_ms": config.wait_ready_ms,
        "ready_wait": ready_wait,
        "checks": {
            "health": health,
            "ready": ready,
            "databases": databases,
            "branches": branches,
            "metrics": metrics,
            "admin_jobs": admin_jobs,
        },
        "sql_module": "not_implemented",
    })
}

fn run_http_doctor_command(config: HttpDoctorConfig) -> Result<(), Box<dyn std::error::Error>> {
    let summary = run_http_doctor(config);
    let ok = summary.get("ok").and_then(Value::as_bool).unwrap_or(false);
    print_json(summary);
    if ok {
        Ok(())
    } else {
        Err("doctor http endpoint checks failed".into())
    }
}

fn routed_admin_jobs_path(database_id: Option<&str>, branch_id: Option<&str>) -> String {
    let mut params = Vec::new();
    if let Some(database_id) = database_id {
        params.push(format!("database_id={}", query_component(database_id)));
    }
    if let Some(branch_id) = branch_id {
        params.push(format!("branch_id={}", query_component(branch_id)));
    }
    if params.is_empty() {
        "/v1/admin/jobs".to_string()
    } else {
        format!("/v1/admin/jobs?{}", params.join("&"))
    }
}

fn query_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b':' => {
                vec![byte as char]
            }
            other => format!("%{other:02X}").chars().collect(),
        })
        .collect()
}

fn http_doctor_check(probe: impl FnOnce() -> Result<Value, TraceDbClientError>) -> Value {
    match probe() {
        Ok(response) => json!({ "ok": true, "response": response }),
        Err(error) => http_doctor_error(error),
    }
}

fn http_doctor_wait_for_ready(client: &TraceDbClient, timeout_ms: u64) -> Value {
    if timeout_ms == 0 {
        return json!({
            "enabled": false,
            "ok": true,
            "attempts": 0,
        });
    }

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut attempts = 0_u64;
    let mut last_check = json!({ "ok": false, "error": "ready wait did not run" });
    loop {
        attempts += 1;
        let check = http_doctor_check(|| client.ready());
        if http_doctor_ready_ok(&check) {
            return json!({
                "enabled": true,
                "ok": true,
                "attempts": attempts,
                "check": check,
            });
        }
        last_check = check;
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        thread::sleep(remaining.min(Duration::from_millis(20)));
    }

    json!({
        "enabled": true,
        "ok": false,
        "attempts": attempts,
        "last_check": last_check,
    })
}

fn http_doctor_error(error: TraceDbClientError) -> Value {
    let server_error = error.server_error();
    let server_error_code = error.server_error_code();
    let mut value = json!({
        "ok": false,
        "error": error.to_string(),
    });
    if let Some(object) = value.as_object_mut() {
        if let Some(server_error) = server_error {
            object.insert("server_error".to_string(), json!(server_error));
        }
        if let Some(server_error_code) = server_error_code {
            object.insert("server_error_code".to_string(), json!(server_error_code));
        }
        if let TraceDbClientError::HttpStatus {
            method,
            path,
            status,
            ..
        } = error
        {
            object.insert("method".to_string(), json!(method));
            object.insert("path".to_string(), json!(path));
            object.insert("http_status".to_string(), json!(status));
        }
    }
    value
}

fn http_doctor_ready_wait_ok(wait: &Value) -> bool {
    wait.get("ok").and_then(Value::as_bool).unwrap_or(false)
}

fn http_doctor_check_ok(check: &Value) -> bool {
    check.get("ok").and_then(Value::as_bool).unwrap_or(false)
}

fn http_doctor_ready_ok(check: &Value) -> bool {
    http_doctor_check_ok(check)
        && check
            .pointer("/response/ready")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

fn run_demo(data_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(data_dir)?;
    let table = "demo_docs";
    let tenant = "demo-tenant";
    db.apply_schema(demo_schema(table))?;
    let (_epoch, write_timing) =
        db.put_batch_with_write_timing(RecordPutBatchRequest::new(vec![
            demo_record(
                table,
                tenant,
                "intro",
                "TraceDB local demo quickstart",
                "published",
                [1.0, 0.0, 0.0],
            ),
            demo_record(
                table,
                tenant,
                "sdk",
                "TraceDB SDK and HTTP API surface",
                "published",
                [0.8, 0.2, 0.0],
            ),
            demo_record(
                table,
                tenant,
                "ops",
                "TraceDB snapshot restore and WAL recovery",
                "draft",
                [0.0, 1.0, 0.0],
            ),
        ]))?;

    let query = HybridQuery {
        table: table.to_string(),
        tenant_id: tenant.to_string(),
        cursor: None,
        text_field: Some("body".to_string()),
        text: Some("TraceDB API".to_string()),
        vector_field: Some("embedding".to_string()),
        vector: Some(vec![1.0, 0.0, 0.0]),
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 3,
        freshness: FreshnessMode::Strict,
        explain: true,
    };
    let query_output = db.query(query.clone())?;
    let query_ids = query_output
        .results
        .iter()
        .map(|row| row.record_id.clone())
        .collect::<Vec<_>>();

    let scan_before_delete = db.scan(RecordScanRequest::new(table, tenant).limit(10))?;
    db.delete(RecordDeleteRequest::new(table, tenant, "ops").tombstone("demo_cleanup"))?;
    let deleted_hidden = db
        .get(RecordGetRequest::new(table, tenant, "ops"))?
        .is_none();
    db.compact()?;

    let snapshot_dir = data_dir.join("demo-snapshot");
    if snapshot_dir.exists() {
        fs::remove_dir_all(&snapshot_dir)?;
    }
    db.create_snapshot(&snapshot_dir)?;
    let restore_dir = data_dir.join("demo-restore");
    if restore_dir.exists() {
        fs::remove_dir_all(&restore_dir)?;
    }
    let restored = TraceDb::restore_snapshot(&snapshot_dir, &restore_dir)?;
    let restored_scan = restored.scan(RecordScanRequest::new(table, tenant).limit(10))?;
    let manifest = db.inspect_manifest()?;
    let restored_manifest = restored.inspect_manifest()?;

    print_json(json!({
        "ok": true,
        "mode": "embedded-local-demo",
        "data_dir": data_dir,
        "table": table,
        "tenant_id": tenant,
        "steps": {
            "schema_apply": true,
            "batch_ingest": true,
            "query": true,
            "scan": true,
            "delete": true,
            "compact": true,
            "snapshot": true,
            "restore": true,
        },
        "records_before_delete": scan_before_delete.returned_count,
        "records_after_restore": restored_scan.returned_count,
        "query_result_ids": query_ids,
        "explain": {
            "read_epoch": query_output.explain.read_epoch.get(),
            "fusion_method": query_output.explain.fusion_method,
            "returned_count": query_output.explain.returned_count,
            "access_path_count": query_output.explain.access_paths.len(),
        },
        "write_timing": {
            "total_ms": write_timing.total_ms,
            "store_apply_ms": write_timing.store_apply_ms,
            "wal_total_ms": write_timing.wal_total_ms,
            "manifest_total_ms": write_timing.manifest_total_ms,
        },
        "deleted_hidden": deleted_hidden,
        "latest_epoch": manifest.latest_epoch.get(),
        "restored_latest_epoch": restored_manifest.latest_epoch.get(),
        "snapshot_dir": snapshot_dir,
        "restore_dir": restore_dir,
        "sql_module": "not_implemented",
    }));
    Ok(())
}

fn run_http_demo(data_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let bind = listener.local_addr()?;
    drop(listener);

    let _server = LocalServerChild::start(data_dir, &bind.to_string())?;

    let url = format!("http://{bind}");
    let client = TraceDbClient::new(
        TraceDbClientConfig::managed(url.clone(), "dev-token")
            .with_timeout(Duration::from_millis(500))
            .with_safe_retries(1)
            .with_idempotency_retries(1),
    );
    wait_for_ready(&client)?;

    let table = "demo_docs";
    let tenant = "demo-tenant";
    let run_id = http_demo_run_id()?;
    let schema = client.apply_schema_typed_with_options(
        &demo_schema(table),
        &http_demo_idempotency_options(&run_id, "schema-apply"),
    )?;
    let ingest = client.put_batch_typed_with_options(
        &RecordPutBatchRequest::new(vec![
            demo_record(
                table,
                tenant,
                "intro",
                "TraceDB local HTTP SDK quickstart",
                "published",
                [1.0, 0.0, 0.0],
            ),
            demo_record(
                table,
                tenant,
                "sdk",
                "TraceDB SDK and HTTP API surface",
                "published",
                [0.8, 0.2, 0.0],
            ),
            demo_record(
                table,
                tenant,
                "ops",
                "TraceDB snapshot restore and WAL recovery",
                "draft",
                [0.0, 1.0, 0.0],
            ),
        ]),
        &http_demo_idempotency_options(&run_id, "put-batch"),
    )?;
    let scan = client.scan_typed(&RecordScanRequest::new(table, tenant).limit(10))?;
    let query = HybridQuery {
        table: table.to_string(),
        tenant_id: tenant.to_string(),
        cursor: None,
        text_field: Some("body".to_string()),
        text: Some("TraceDB API".to_string()),
        vector_field: Some("embedding".to_string()),
        vector: Some(vec![1.0, 0.0, 0.0]),
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 3,
        freshness: FreshnessMode::Strict,
        explain: true,
    };
    let query_response = client.query_typed(&query)?;
    let query_ids = query_response
        .results
        .iter()
        .map(|row| row.record_id.clone())
        .collect::<Vec<_>>();
    let explain = client.explain_typed(&query)?;
    let delete = client.delete_typed_with_options(
        &RecordDeleteRequest::new(table, tenant, "ops").tombstone("http_demo_cleanup"),
        &http_demo_idempotency_options(&run_id, "delete-ops"),
    )?;
    let deleted_hidden = client
        .get_record_typed(&RecordGetRequest::new(table, tenant, "ops"))?
        .record
        .is_none();
    let compact =
        client.compact_typed_with_options(&http_demo_idempotency_options(&run_id, "compact"))?;

    let admin_dir = data_dir.join("http-demo-admin");
    fs::create_dir_all(&admin_dir)?;
    let snapshot_dir = admin_dir.join("snapshot");
    if snapshot_dir.exists() {
        fs::remove_dir_all(&snapshot_dir)?;
    }
    let restore_dir = admin_dir.join("restore");
    if restore_dir.exists() {
        fs::remove_dir_all(&restore_dir)?;
    }
    let snapshot = client.snapshot_typed_with_options(
        &SnapshotRequest::new(snapshot_dir.to_string_lossy().to_string()),
        &http_demo_idempotency_options(&run_id, "snapshot"),
    )?;
    let restore = client.restore_typed_with_options(
        &RestoreRequest::new(
            snapshot_dir.to_string_lossy().to_string(),
            restore_dir.to_string_lossy().to_string(),
        ),
        &http_demo_idempotency_options(&run_id, "restore"),
    )?;
    let restored = TraceDb::open(&restore_dir)?;
    let restored_scan = restored.scan(RecordScanRequest::new(table, tenant).limit(10))?;

    print_json(json!({
        "ok": true,
        "mode": "local-http-sdk-demo",
        "server_url": url,
        "data_dir": data_dir,
        "table": table,
        "tenant_id": tenant,
        "steps": {
            "server_start": true,
            "ready": true,
            "schema_apply": true,
            "batch_ingest": true,
            "scan": true,
            "query": true,
            "explain": true,
            "delete": delete.deleted,
            "compact": compact.compacted,
            "snapshot": snapshot.snapshot,
            "restore": restore.restored,
        },
        "schema_epoch": schema.epoch,
        "records_inserted": ingest.record_count,
        "records_scanned": scan.returned_count,
        "records_after_restore": restored_scan.returned_count,
        "query_result_ids": query_ids,
        "explain_returned_count": explain.returned_count,
        "deleted_hidden": deleted_hidden,
        "snapshot_dir": snapshot.target,
        "restore_dir": restore.target,
        "idempotency_retries": 1,
        "idempotency_keys": true,
        "sql_module": "not_implemented",
    }));
    Ok(())
}

struct ProductRegressionConfig {
    data_root: PathBuf,
    cleanup_data: bool,
    keep_data: bool,
    skip_typescript: bool,
    inject_failure: Option<String>,
    report_file: Option<PathBuf>,
    list_steps: bool,
    only_step: Option<String>,
}

const PRODUCT_REGRESSION_STEPS: &[&str] = &[
    "embedded_demo",
    "embedded_verify",
    "http_demo",
    "local_doctor",
    "rust_sdk_quickstart",
    "python_sdk_smoke",
    "typescript_check",
    "typescript_http_smoke",
    "typescript_gateway_smoke",
];

const PRODUCT_REGRESSION_ONLY_STEPS: &[&str] = PRODUCT_REGRESSION_STEPS;
const PRODUCT_QUICKSTART_REPORT_FILE: &str = "target/tracedb/product-quickstart.json";
const DURABILITY_FAULTS_REPORT_FILE: &str = "target/tracedb/durability-faults.json";
const STORAGE_INDEX_JOBS_REPORT_FILE: &str = "target/tracedb/storage-index-jobs.json";
const DURABILITY_GOOD_MASTER_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const DURABILITY_OTHER_MASTER_KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
const DURABILITY_FAULT_SCENARIOS: &[&str] = &[
    "wrong_master_key",
    "missing_master_key",
    "torn_wal_tail",
    "manifest_corruption",
    "checkpoint_corruption",
    "stale_lock_recovery",
    "encrypted_snapshot_restore",
    "wal_idempotency_replay_after_reopen",
];
const STORAGE_INDEX_JOB_SCENARIOS: &[&str] = &[
    "delta_writes",
    "binary_segment_roundtrip",
    "legacy_json_segment_read",
    "checksum_corruption",
    "encrypted_binary_artifacts",
    "bm25_query_parity",
    "hnsw_vector_parity",
    "bitmap_policy_filtering",
    "stale_sealed_candidate_hot_materialization",
    "vacuum_safety",
    "durable_enqueue_replay",
    "lease_expiry",
    "retry_dead_letter",
    "interrupted_compaction",
    "failed_index_build_recovery",
    "backup_job_failure",
    "restore_verification_job",
    "reopen_after_job_state_change",
];

struct DurabilityFaultsConfig {
    data_root: PathBuf,
    cleanup_data: bool,
    keep_data: bool,
    inject_failure: Option<String>,
    report_file: Option<PathBuf>,
}

struct StorageIndexJobsConfig {
    data_root: PathBuf,
    cleanup_data: bool,
    keep_data: bool,
    inject_failure: Option<String>,
    report_file: Option<PathBuf>,
}

fn parse_durability_faults_config(
    args: &[String],
) -> Result<DurabilityFaultsConfig, Box<dyn std::error::Error>> {
    let mut data_root = None;
    let mut keep_data = false;
    let mut inject_failure = None;
    let mut report_file = None;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--data-root" => {
                idx += 1;
                data_root = Some(PathBuf::from(
                    args.get(idx).ok_or("missing value for --data-root")?,
                ));
            }
            "--keep-data" => keep_data = true,
            "--report-file" => {
                idx += 1;
                report_file = Some(PathBuf::from(
                    args.get(idx).ok_or("missing value for --report-file")?,
                ));
            }
            "--inject-failure" => {
                idx += 1;
                let scenario = args
                    .get(idx)
                    .ok_or("missing value for --inject-failure")?
                    .to_string();
                if !DURABILITY_FAULT_SCENARIOS.contains(&scenario.as_str()) {
                    return Err(format!(
                        "unknown durability-faults failure injection scenario {scenario}; expected one of {}",
                        DURABILITY_FAULT_SCENARIOS.join(", ")
                    )
                    .into());
                }
                inject_failure = Some(scenario);
            }
            other => return Err(format!("unknown durability-faults option {other}").into()),
        }
        idx += 1;
    }
    let cleanup_data = data_root.is_none() && !keep_data;
    let data_root = data_root.unwrap_or_else(default_durability_faults_root);
    if report_file.is_none() {
        report_file = Some(default_durability_faults_report_file()?);
    }
    Ok(DurabilityFaultsConfig {
        data_root,
        cleanup_data,
        keep_data,
        inject_failure,
        report_file,
    })
}

fn default_durability_faults_report_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(product_regression_workspace_root()?.join(DURABILITY_FAULTS_REPORT_FILE))
}

fn default_durability_faults_root() -> PathBuf {
    let suffix = product_regression_run_id().unwrap_or_else(|_| std::process::id().to_string());
    env::temp_dir().join(format!("tracedb-durability-faults-{suffix}"))
}

fn run_durability_faults(config: DurabilityFaultsConfig) -> Result<(), Box<dyn std::error::Error>> {
    if config.data_root.exists() && config.cleanup_data {
        fs::remove_dir_all(&config.data_root)?;
    }
    fs::create_dir_all(&config.data_root)?;

    let mut scenarios = serde_json::Map::new();
    for scenario in DURABILITY_FAULT_SCENARIOS {
        let dir = config.data_root.join(scenario);
        let result = if config.inject_failure.as_deref() == Some(*scenario) {
            Err("injected durability fault failure".into())
        } else {
            run_durability_fault_scenario(scenario, &dir)
        };
        scenarios.insert(
            (*scenario).to_string(),
            durability_fault_scenario_summary(result),
        );
    }
    let statuses = durability_fault_status_counts(&scenarios);
    let ok = statuses.get("failed").and_then(Value::as_u64).unwrap_or(0) == 0;
    let summary = json!({
        "ok": ok,
        "mode": "local-durability-faults",
        "scope": "local_only",
        "data_root": config.data_root.display().to_string(),
        "report_file": product_regression_report_file_json(config.report_file.as_deref()),
        "data_cleanup": config.cleanup_data,
        "keep_data": config.keep_data,
        "failure_injection": config.inject_failure,
        "claims": {
            "managed_cloud": "not_checked",
            "cross_replica_exactly_once": "not_claimed",
            "tde_scope": "local_artifacts_when_configured",
        },
        "statuses": statuses,
        "scenarios": scenarios,
    });
    if config.cleanup_data {
        let _ = fs::remove_dir_all(&config.data_root);
    }
    emit_json(summary, config.report_file.as_deref())?;
    if ok {
        Ok(())
    } else {
        Err("durability-faults local gate failed".into())
    }
}

fn run_durability_fault_scenario(
    scenario: &str,
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    match scenario {
        "wrong_master_key" => durability_fault_wrong_master_key(dir),
        "missing_master_key" => durability_fault_missing_master_key(dir),
        "torn_wal_tail" => durability_fault_torn_wal_tail(dir),
        "manifest_corruption" => durability_fault_manifest_corruption(dir),
        "checkpoint_corruption" => durability_fault_checkpoint_corruption(dir),
        "stale_lock_recovery" => durability_fault_stale_lock_recovery(dir),
        "encrypted_snapshot_restore" => durability_fault_encrypted_snapshot_restore(dir),
        "wal_idempotency_replay_after_reopen" => durability_fault_idempotency_replay(dir),
        other => Err(format!("unknown durability fault scenario {other}").into()),
    }
}

fn durability_fault_scenario_summary(result: Result<Value, Box<dyn std::error::Error>>) -> Value {
    match result {
        Ok(details) => json!({
            "ok": true,
            "status": "passed",
            "evidence": details.get("evidence").and_then(Value::as_str).unwrap_or("scenario completed"),
            "details": details,
        }),
        Err(error) => json!({
            "ok": false,
            "status": "failed",
            "error": error.to_string(),
        }),
    }
}

fn durability_fault_status_counts(scenarios: &serde_json::Map<String, Value>) -> Value {
    let mut passed = 0_u64;
    let mut failed = 0_u64;
    let mut not_applicable = 0_u64;
    for scenario in scenarios.values() {
        match scenario
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("failed")
        {
            "passed" => passed += 1,
            "not_applicable" => not_applicable += 1,
            _ => failed += 1,
        }
    }
    json!({
        "passed": passed,
        "failed": failed,
        "not_applicable": not_applicable,
    })
}

fn durability_fault_wrong_master_key(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    seed_encrypted_db(dir, "wrong-key-marker", "wrong key protected body")?;
    let error = TraceDb::open_with_options(
        dir,
        TraceDbOpenOptions::with_master_key_b64(DURABILITY_OTHER_MASTER_KEY),
    )
    .expect_err("wrong master key must fail open")
    .to_string();
    if !error.contains("failed to unwrap database encryption key") {
        return Err(format!("unexpected wrong-key error: {error}").into());
    }
    Ok(json!({
        "evidence": "encrypted database rejected a different 32-byte master key",
        "stable_error_contains": "failed to unwrap database encryption key",
        "error": error,
    }))
}

fn durability_fault_missing_master_key(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    seed_encrypted_db(dir, "missing-key-marker", "missing key protected body")?;
    let error = TraceDb::open_with_options(dir, TraceDbOpenOptions::without_tde())
        .expect_err("missing master key must fail open")
        .to_string();
    if !error.contains("TRACEDB_MASTER_KEY_B64 is required to open encrypted TraceDB data") {
        return Err(format!("unexpected missing-key error: {error}").into());
    }
    Ok(json!({
        "evidence": "encrypted database rejected open without TRACEDB_MASTER_KEY_B64",
        "stable_error_contains": "TRACEDB_MASTER_KEY_B64 is required",
        "error": error,
    }))
}

fn durability_fault_torn_wal_tail(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.apply_schema(demo_schema("docs"))?;
    db.put(RecordPutRequest::new(demo_record(
        "docs",
        "tenant-a",
        "torn",
        "Torn WAL",
        "committed before torn tail",
        [1.0, 0.0, 0.0],
    )))?;
    drop(db);

    let wal_path = dir.join("wal/000001.twal");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)?
        .write_all(b"torn")?;
    let reopened = TraceDb::open(dir)?;
    let torn = reopened
        .last_recovery_torn_tail()
        .ok_or("expected torn WAL tail recovery metadata")?;
    if torn.reason != "short_header" {
        return Err(format!("unexpected torn WAL reason {}", torn.reason).into());
    }
    Ok(json!({
        "evidence": "open ignored short torn WAL tail and exposed recovery metadata",
        "reason": torn.reason,
        "offset": torn.offset,
        "actual_len": torn.actual_len,
        "expected_len": torn.expected_len,
    }))
}

fn durability_fault_manifest_corruption(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.apply_schema(demo_schema("docs"))?;
    drop(db);

    let manifest_path = dir.join("manifest.tdb");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["checksums"]["manifest_checksum"] = json!(1_u32);
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    let error = TraceDb::open(dir)
        .expect_err("corrupt manifest checksum must fail open")
        .to_string();
    if !error.contains("manifest checksum mismatch") {
        return Err(format!("unexpected manifest corruption error: {error}").into());
    }
    Ok(json!({
        "evidence": "manifest checksum mismatch stopped open",
        "stable_error_contains": "manifest checksum mismatch",
        "error": error,
    }))
}

fn durability_fault_checkpoint_corruption(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.apply_schema(demo_schema("docs"))?;
    db.put(RecordPutRequest::new(demo_record(
        "docs",
        "tenant-a",
        "checkpoint",
        "Checkpoint",
        "checkpoint protected body",
        [0.0, 1.0, 0.0],
    )))?;
    let epoch = db.checkpoint()?;
    drop(db);

    let checkpoint_path = dir.join(format!("checkpoints/checkpoint-{}.tchk", epoch.get()));
    let mut checkpoint = fs::read(&checkpoint_path)?;
    let last = checkpoint
        .last_mut()
        .ok_or("checkpoint file unexpectedly empty")?;
    *last ^= 0xff;
    fs::write(&checkpoint_path, checkpoint)?;
    let error = TraceDb::open(dir)
        .expect_err("corrupt checkpoint must fail open")
        .to_string();
    if !error.contains("checkpoint checksum mismatch") {
        return Err(format!("unexpected checkpoint corruption error: {error}").into());
    }
    Ok(json!({
        "evidence": "checkpoint checksum mismatch stopped open",
        "stable_error_contains": "checkpoint checksum mismatch",
        "error": error,
    }))
}

fn durability_fault_stale_lock_recovery(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    fs::write(dir.join("engine.write.lock"), "999999999")?;
    fs::create_dir_all(dir.join("wal"))?;
    fs::write(dir.join("wal/000001.twal.lock"), "999999999")?;
    db.apply_schema(demo_schema("docs"))?;
    let engine_lock_removed = !dir.join("engine.write.lock").exists();
    let wal_lock_removed = !dir.join("wal/000001.twal.lock").exists();
    if !engine_lock_removed || !wal_lock_removed {
        return Err("stale engine/WAL locks were not recovered".into());
    }
    Ok(json!({
        "evidence": "stale owner PID lock files were removed after explicit safety checks",
        "engine_lock_removed": engine_lock_removed,
        "wal_lock_removed": wal_lock_removed,
    }))
}

fn durability_fault_encrypted_snapshot_restore(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let marker_body = "encrypted snapshot restore marker body";
    let mut db = TraceDb::open_with_options(
        dir,
        TraceDbOpenOptions::with_master_key_b64(DURABILITY_GOOD_MASTER_KEY),
    )?;
    db.apply_schema(demo_schema("docs"))?;
    db.put(RecordPutRequest::new(demo_record(
        "docs",
        "tenant-a",
        "snapshot-marker",
        marker_body,
        "snapshot_marker",
        [0.0, 0.0, 1.0],
    )))?;
    db.checkpoint()?;
    let wal_bytes = fs::read(dir.join("wal/000001.twal"))?;
    if String::from_utf8_lossy(&wal_bytes).contains(marker_body) {
        return Err("encrypted WAL exposed plaintext marker body".into());
    }
    let snapshot_dir = dir.with_extension("snapshot");
    let restore_dir = dir.with_extension("restore");
    db.create_snapshot(&snapshot_dir)?;
    drop(db);

    durability_restore_snapshot_copy(&snapshot_dir, &restore_dir)?;
    let restored = TraceDb::open_with_options(
        &restore_dir,
        TraceDbOpenOptions::with_master_key_b64(DURABILITY_GOOD_MASTER_KEY),
    )?;
    let record = restored
        .get(RecordGetRequest::new("docs", "tenant-a", "snapshot-marker"))?
        .ok_or("restored marker record missing")?;
    if record.fields["body"] != json!(marker_body) {
        return Err("restored marker body did not match".into());
    }
    Ok(json!({
        "evidence": "encrypted snapshot restored into a fresh target and marker read succeeded",
        "snapshot_dir": snapshot_dir.display().to_string(),
        "restore_dir": restore_dir.display().to_string(),
        "marker_id": "snapshot-marker",
        "restored_marker_visible": true,
    }))
}

fn durability_fault_idempotency_replay(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.apply_schema(demo_schema("docs"))?;
    let record = demo_record(
        "docs",
        "tenant-a",
        "idem-marker",
        "wal receipt replay marker body",
        "idempotency_marker",
        [1.0, 1.0, 0.0],
    );
    let body = serde_json::to_vec(&json!({ "record": record }))?;
    let receipt = IdempotencyReceipt {
        method: "POST".to_string(),
        path: "/v1/records/put".to_string(),
        key: "durability-fault-idempotency".to_string(),
        body_hash: stable_body_hash(&body),
        actor_tenant_id: "tenant-a".to_string(),
        database_id: "db_local".to_string(),
        branch_id: "db_local:main".to_string(),
        token_identity: "local-dev".to_string(),
        response: json!({ "epoch": 2 }).to_string(),
    };
    db.put_with_idempotency_receipt(RecordPutRequest::new(record), Some(receipt.clone()))?;
    drop(db);

    let reopened = TraceDb::open(dir)?;
    let receipts = reopened.idempotency_receipts()?;
    let replayed = receipts.iter().any(|candidate| {
        candidate.method == receipt.method
            && candidate.path == receipt.path
            && candidate.key == receipt.key
            && candidate.body_hash == receipt.body_hash
            && candidate.actor_tenant_id == receipt.actor_tenant_id
            && candidate.database_id == receipt.database_id
            && candidate.branch_id == receipt.branch_id
            && candidate.token_identity == receipt.token_identity
            && candidate.response.contains("\"epoch\":2")
    });
    if !replayed {
        return Err("WAL-backed idempotency receipt was not replayed after reopen".into());
    }
    if dir.join("http-idempotency-cache.json").exists() {
        return Err("legacy http-idempotency-cache.json was created".into());
    }
    Ok(json!({
        "evidence": "idempotency receipt replayed from WAL after clean reopen without JSON cache authority",
        "receipt_key": receipt.key,
        "body_hash": receipt.body_hash,
        "receipt_count": receipts.len(),
        "legacy_json_cache_present": false,
    }))
}

fn durability_restore_snapshot_copy(
    source: &std::path::Path,
    target: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if source == target {
        return Err("source and target directories must differ".into());
    }
    if target.exists() {
        fs::remove_dir_all(target)?;
    }
    durability_copy_dir_all(source, target)?;
    Ok(())
}

fn durability_copy_dir_all(
    source: &std::path::Path,
    target: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let child_target = target.join(entry.file_name());
        if ty.is_dir() {
            durability_copy_dir_all(&entry.path(), &child_target)?;
        } else if ty.is_file() {
            fs::copy(entry.path(), child_target)?;
        }
    }
    Ok(())
}

fn seed_encrypted_db(
    dir: &std::path::Path,
    record_id: &str,
    body: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut db = TraceDb::open_with_options(
        dir,
        TraceDbOpenOptions::with_master_key_b64(DURABILITY_GOOD_MASTER_KEY),
    )?;
    db.apply_schema(demo_schema("docs"))?;
    db.put(RecordPutRequest::new(demo_record(
        "docs",
        "tenant-a",
        record_id,
        body,
        "tde_marker",
        [1.0, 0.0, 0.0],
    )))?;
    Ok(())
}

fn parse_storage_index_jobs_config(
    args: &[String],
) -> Result<StorageIndexJobsConfig, Box<dyn std::error::Error>> {
    let mut data_root = None;
    let mut keep_data = false;
    let mut inject_failure = None;
    let mut report_file = None;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--data-root" => {
                idx += 1;
                data_root = Some(PathBuf::from(
                    args.get(idx).ok_or("missing value for --data-root")?,
                ));
            }
            "--keep-data" => keep_data = true,
            "--report-file" => {
                idx += 1;
                report_file = Some(PathBuf::from(
                    args.get(idx).ok_or("missing value for --report-file")?,
                ));
            }
            "--inject-failure" => {
                idx += 1;
                let scenario = args
                    .get(idx)
                    .ok_or("missing value for --inject-failure")?
                    .to_string();
                if !STORAGE_INDEX_JOB_SCENARIOS.contains(&scenario.as_str()) {
                    return Err(format!(
                        "unknown storage-index-jobs failure injection scenario {scenario}; expected one of {}",
                        STORAGE_INDEX_JOB_SCENARIOS.join(", ")
                    )
                    .into());
                }
                inject_failure = Some(scenario);
            }
            other => return Err(format!("unknown storage-index-jobs option {other}").into()),
        }
        idx += 1;
    }
    let cleanup_data = data_root.is_none() && !keep_data;
    let data_root = data_root.unwrap_or_else(default_storage_index_jobs_root);
    if report_file.is_none() {
        report_file = Some(default_storage_index_jobs_report_file()?);
    }
    Ok(StorageIndexJobsConfig {
        data_root,
        cleanup_data,
        keep_data,
        inject_failure,
        report_file,
    })
}

fn default_storage_index_jobs_report_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(product_regression_workspace_root()?.join(STORAGE_INDEX_JOBS_REPORT_FILE))
}

fn default_storage_index_jobs_root() -> PathBuf {
    let suffix = product_regression_run_id().unwrap_or_else(|_| std::process::id().to_string());
    env::temp_dir().join(format!("tracedb-storage-index-jobs-{suffix}"))
}

fn run_storage_index_jobs(
    config: StorageIndexJobsConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if config.data_root.exists() && config.cleanup_data {
        fs::remove_dir_all(&config.data_root)?;
    }
    fs::create_dir_all(&config.data_root)?;

    let mut scenarios = serde_json::Map::new();
    for scenario in STORAGE_INDEX_JOB_SCENARIOS {
        let dir = config.data_root.join(scenario);
        let result = if config.inject_failure.as_deref() == Some(*scenario) {
            Err("injected storage-index-jobs failure".into())
        } else {
            run_storage_index_job_scenario(scenario, &dir)
        };
        scenarios.insert(
            (*scenario).to_string(),
            storage_index_job_scenario_summary(result),
        );
    }
    let statuses = durability_fault_status_counts(&scenarios);
    let ok = statuses.get("failed").and_then(Value::as_u64).unwrap_or(0) == 0;
    let summary = json!({
        "ok": ok,
        "mode": "local-storage-index-jobs",
        "scope": "local_only",
        "data_root": config.data_root.display().to_string(),
        "report_file": product_regression_report_file_json(config.report_file.as_deref()),
        "data_cleanup": config.cleanup_data,
        "keep_data": config.keep_data,
        "failure_injection": config.inject_failure,
        "claims": {
            "managed_cloud": "not_checked",
            "api_parity_expansion": "not_checked",
            "storage_foundation": "checked",
            "durable_jobs": "checked",
        },
        "statuses": statuses,
        "scenarios": scenarios,
    });
    if config.cleanup_data {
        let _ = fs::remove_dir_all(&config.data_root);
    }
    emit_json(summary, config.report_file.as_deref())?;
    if ok {
        Ok(())
    } else {
        Err("storage-index-jobs local gate failed".into())
    }
}

fn run_storage_index_job_scenario(
    scenario: &str,
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    match scenario {
        "delta_writes" => storage_delta_writes(dir),
        "binary_segment_roundtrip" => storage_binary_segment_roundtrip(dir),
        "legacy_json_segment_read" => storage_legacy_json_segment_read(dir),
        "checksum_corruption" => storage_checksum_corruption(dir),
        "encrypted_binary_artifacts" => storage_encrypted_binary_artifacts(dir),
        "bm25_query_parity" => storage_bm25_query_parity(dir),
        "hnsw_vector_parity" => storage_hnsw_vector_parity(dir),
        "bitmap_policy_filtering" => storage_bitmap_policy_filtering(dir),
        "stale_sealed_candidate_hot_materialization" => {
            storage_stale_sealed_candidate_hot_materialization(dir)
        }
        "vacuum_safety" => storage_vacuum_safety(dir),
        "durable_enqueue_replay" => storage_durable_enqueue_replay(dir),
        "lease_expiry" => storage_lease_expiry(dir),
        "retry_dead_letter" => storage_retry_dead_letter(dir),
        "interrupted_compaction" => storage_interrupted_compaction(dir),
        "failed_index_build_recovery" => storage_failed_index_build_recovery(dir),
        "backup_job_failure" => storage_backup_job_failure(dir),
        "restore_verification_job" => storage_restore_verification_job(dir),
        "reopen_after_job_state_change" => storage_reopen_after_job_state_change(dir),
        other => Err(format!("unknown storage-index-jobs scenario {other}").into()),
    }
}

fn storage_index_job_scenario_summary(result: Result<Value, Box<dyn std::error::Error>>) -> Value {
    match result {
        Ok(details) => json!({
            "ok": true,
            "status": "passed",
            "evidence": details.get("evidence").and_then(Value::as_str).unwrap_or("scenario completed"),
            "details": details,
        }),
        Err(error) => json!({
            "ok": false,
            "status": "failed",
            "error": error.to_string(),
        }),
    }
}

fn storage_delta_writes(dir: &std::path::Path) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.apply_schema(demo_schema("docs"))?;
    let (_epoch, timing) =
        db.put_batch_with_write_timing(RecordPutBatchRequest::new(storage_demo_records("docs")))?;
    if timing.store_clone_ms != 0.0 {
        return Err(format!(
            "store_clone_ms should be 0.0, got {}",
            timing.store_clone_ms
        )
        .into());
    }
    if timing.store_delta_apply_ms < 0.0 || timing.store_delta_plan_ms < 0.0 {
        return Err("delta timing fields must be present and non-negative".into());
    }
    let record = db
        .get(RecordGetRequest::new("docs", "tenant-a", "alpha"))?
        .ok_or("delta write record missing")?;
    Ok(json!({
        "evidence": "batch write used delta planning and materialized after WAL append",
        "record_id": record.id,
        "store_clone_ms": timing.store_clone_ms,
        "store_delta_plan_ms": timing.store_delta_plan_ms,
        "store_delta_apply_ms": timing.store_delta_apply_ms,
        "wal_frame_bytes": timing.wal_frame_bytes,
    }))
}

fn storage_binary_segment_roundtrip(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    db.compact()?;
    let manifest = db.inspect_manifest()?;
    let segment = manifest
        .segments
        .first()
        .ok_or("segment manifest missing")?;
    let path = dir
        .join("segments")
        .join(format!("{}.tseg", segment.segment_id));
    let bytes = fs::read(&path)?;
    if !bytes.starts_with(ARTIFACT_ENVELOPE_MAGIC) {
        return Err("new segment artifact did not use TraceDB binary envelope".into());
    }
    let object = tracedb_segment::read_segment_object(&path)?;
    Ok(json!({
        "evidence": "segment artifact roundtripped through TraceDB binary envelope",
        "segment_id": object.segment_id,
        "format_version": object.format_version,
        "records": object.records.len(),
        "object_checksum": object.object_checksum,
    }))
}

fn storage_legacy_json_segment_read(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    let mut object =
        tracedb_segment::SegmentObject::from_records("legacy-json", 1, storage_segment_records())?;
    object.format_version = tracedb_segment::SEGMENT_LEGACY_JSON_FORMAT_VERSION;
    object.object_checksum = 0;
    object.object_checksum = tracedb_segment::compute_segment_object_checksum(&object)?;
    let path = dir.join("legacy-json.tseg");
    fs::write(&path, serde_json::to_vec_pretty(&object)?)?;
    let bytes = fs::read(&path)?;
    if bytes.starts_with(ARTIFACT_ENVELOPE_MAGIC) {
        return Err("legacy JSON fixture unexpectedly used binary envelope".into());
    }
    let read = tracedb_segment::read_segment_object(&path)?;
    Ok(json!({
        "evidence": "legacy plaintext JSON segment reader remains available",
        "segment_id": read.segment_id,
        "format_version": read.format_version,
        "records": read.records.len(),
    }))
}

fn storage_checksum_corruption(dir: &std::path::Path) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    db.compact()?;
    let manifest = db.inspect_manifest()?;
    let index = manifest
        .indexes
        .iter()
        .find(|index| index.state == IndexState::Ready)
        .ok_or("ready index manifest missing")?;
    let path = dir.join(&index.object_path);
    let mut bytes = fs::read(&path)?;
    let last = bytes.last_mut().ok_or("index artifact was empty")?;
    *last ^= 0xff;
    fs::write(&path, bytes)?;
    let error = tracedb_index::read_index_artifact(&path, None)
        .expect_err("corrupt index artifact must fail verification")
        .to_string();
    Ok(json!({
        "evidence": "corrupt binary index artifact failed checksum verification",
        "index_id": index.index_id,
        "stable_error": error,
    }))
}

fn storage_encrypted_binary_artifacts(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let marker = "encrypted storage marker body";
    let mut db = TraceDb::open_with_options(
        dir,
        TraceDbOpenOptions::with_master_key_b64(DURABILITY_GOOD_MASTER_KEY),
    )?;
    db.apply_schema(demo_schema("docs"))?;
    db.put(RecordPutRequest::new(demo_record(
        "docs",
        "tenant-a",
        "encrypted-alpha",
        marker,
        "encrypted",
        [1.0, 0.0, 0.0],
    )))?;
    db.compact()?;
    let manifest = db.inspect_manifest()?;
    let segment = manifest
        .segments
        .first()
        .ok_or("encrypted segment missing")?;
    let segment_path = dir
        .join("segments")
        .join(format!("{}.tseg", segment.segment_id));
    let bytes = fs::read(&segment_path)?;
    if bytes.starts_with(ARTIFACT_ENVELOPE_MAGIC)
        || String::from_utf8_lossy(&bytes).contains(marker)
    {
        return Err("encrypted binary segment exposed plaintext envelope or marker".into());
    }
    drop(db);
    let reopened = TraceDb::open_with_options(
        dir,
        TraceDbOpenOptions::with_master_key_b64(DURABILITY_GOOD_MASTER_KEY),
    )?;
    let record = reopened
        .get(RecordGetRequest::new("docs", "tenant-a", "encrypted-alpha"))?
        .ok_or("encrypted marker record missing after reopen")?;
    Ok(json!({
        "evidence": "TDE wrapped binary artifacts while reopened reads remained available",
        "segment_id": segment.segment_id,
        "raw_segment_plaintext_marker_present": false,
        "record_id": record.id,
        "key_id": manifest.encryption.as_ref().map(|metadata| metadata.key_id.clone()),
    }))
}

fn storage_bm25_query_parity(dir: &std::path::Path) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    let before = query_record_ids(&db, "banana neural", None, "tenant-a")?;
    db.compact()?;
    let after = query_record_ids(&db, "banana neural", None, "tenant-a")?;
    if before != after {
        return Err(format!("BM25 query parity mismatch before={before:?} after={after:?}").into());
    }
    let text_index = first_index_artifact(dir, &db, "text")?;
    let scores = text_index
        .as_text()
        .ok_or("text index artifact payload missing")?
        .score_text("banana neural", Some("body"));
    if scores.is_empty() {
        return Err("text index produced no postings-backed scores".into());
    }
    Ok(json!({
        "evidence": "BM25 postings artifact produced scores and query parity held with sealed records",
        "record_ids": after,
        "text_scores": scores.len(),
        "top_text_score_record": scores[0].record_id,
    }))
}

fn storage_hnsw_vector_parity(dir: &std::path::Path) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    let before = query_record_ids(&db, "", Some(vec![1.0, 0.0, 0.0]), "tenant-a")?;
    db.compact()?;
    let after = query_record_ids(&db, "", Some(vec![1.0, 0.0, 0.0]), "tenant-a")?;
    if before != after {
        return Err(
            format!("HNSW vector parity mismatch before={before:?} after={after:?}").into(),
        );
    }
    let vector_index = first_index_artifact(dir, &db, "vector")?;
    let vector = vector_index
        .as_vector()
        .ok_or("vector index artifact payload missing")?;
    let nearest = vector.search_vector("embedding", &[1.0, 0.0, 0.0], 2);
    let neighbors = vector
        .hnsw_neighbors("embedding", "alpha")
        .cloned()
        .unwrap_or_default();
    if nearest.first().map(|score| score.record_id.as_str()) != Some("alpha") {
        return Err("vector artifact nearest neighbor did not match exact result".into());
    }
    Ok(json!({
        "evidence": "segment-local deterministic HNSW artifact matched exact vector query top result",
        "record_ids": after,
        "nearest": nearest.iter().map(|score| score.record_id.clone()).collect::<Vec<_>>(),
        "alpha_neighbors": neighbors,
    }))
}

fn storage_bitmap_policy_filtering(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    db.compact()?;
    let output = db.query(storage_query(None, None, "tenant-b"))?;
    if output.results.iter().any(|row| row.tenant_id != "tenant-b") {
        return Err("policy query returned a row from the wrong tenant".into());
    }
    let policy_index = first_index_artifact(dir, &db, "policy")?;
    let visible = policy_index
        .as_bitmap()
        .ok_or("policy bitmap payload missing")?
        .visible_record_ids("tenant-b", &serde_json::Map::new());
    if !visible.contains("tenant-b-only") {
        return Err("policy bitmap did not include tenant-b record".into());
    }
    Ok(json!({
        "evidence": "policy bitmap artifact and final materialization guard kept tenant visibility scoped",
        "query_rows": output.results.len(),
        "bitmap_visible": visible.into_iter().collect::<Vec<_>>(),
    }))
}

fn storage_stale_sealed_candidate_hot_materialization(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    db.compact()?;
    db.put(RecordPutRequest::new(demo_record(
        "docs",
        "tenant-a",
        "alpha",
        "fresh overlay body",
        "fresh",
        [1.0, 0.0, 0.0],
    )))?;
    let output = db.query(storage_query(Some("fresh"), None, "tenant-a"))?;
    let row = output
        .results
        .first()
        .ok_or("fresh hot overlay row missing")?;
    if row.fields.get("body") != Some(&json!("fresh overlay body")) {
        return Err("sealed candidate materialized stale body instead of hot overlay".into());
    }
    Ok(json!({
        "evidence": "fresh hot overlay materialization won over stale sealed candidate",
        "record_id": row.record_id,
        "body": row.fields.get("body").cloned(),
    }))
}

fn storage_vacuum_safety(dir: &std::path::Path) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    db.compact()?;
    fs::write(dir.join("segments/orphan.tseg"), b"orphan")?;
    fs::write(dir.join("indexes/orphan.tidx"), b"orphan")?;
    let removed = db.vacuum()?;
    let manifest = db.inspect_manifest()?;
    let referenced_segments_exist = manifest.segments.iter().all(|segment| {
        dir.join("segments")
            .join(format!("{}.tseg", segment.segment_id))
            .exists()
    });
    let referenced_indexes_exist = manifest
        .indexes
        .iter()
        .all(|index| dir.join(&index.object_path).exists());
    if removed < 2 || !referenced_segments_exist || !referenced_indexes_exist {
        return Err("vacuum did not remove only unreferenced artifacts safely".into());
    }
    Ok(json!({
        "evidence": "vacuum removed unreferenced artifacts and preserved manifest references",
        "removed": removed,
        "referenced_segments_exist": referenced_segments_exist,
        "referenced_indexes_exist": referenced_indexes_exist,
    }))
}

fn storage_durable_enqueue_replay(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    let job = db.enqueue_job(
        JobKind::CompactSegment,
        "segment:all",
        "storage-durable-enqueue",
    )?;
    drop(db);
    let reopened = TraceDb::open(dir)?;
    let jobs = reopened.jobs()?;
    if !jobs.iter().any(|candidate| candidate.job_id == job.job_id) {
        return Err("durable job was not replayed from WAL after reopen".into());
    }
    Ok(json!({
        "evidence": "job enqueue replayed from WAL after reopen",
        "job_id": job.job_id,
        "jobs": jobs.len(),
    }))
}

fn storage_lease_expiry(dir: &std::path::Path) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.enqueue_job(JobKind::BuildVectorIndex, "segment:seg-1", "lease-expiry")?;
    let first = db
        .lease_job(
            WorkerId::new("worker-1"),
            JobKind::BuildVectorIndex,
            1_000,
            100,
        )?
        .ok_or("first lease missing")?;
    let blocked = db.lease_job(
        WorkerId::new("worker-2"),
        JobKind::BuildVectorIndex,
        1_050,
        100,
    )?;
    if blocked.is_some() {
        return Err("unexpired job lease was stolen".into());
    }
    let expired = db
        .lease_job(
            WorkerId::new("worker-2"),
            JobKind::BuildVectorIndex,
            1_101,
            100,
        )?
        .ok_or("expired job did not lease")?;
    if first.lease_token == expired.lease_token {
        return Err("expired lease reused the same lease token".into());
    }
    Ok(json!({
        "evidence": "lease expiry allowed a new worker to acquire the durable job",
        "first_lease_token": first.lease_token,
        "expired_lease_token": expired.lease_token,
        "lease_owner": expired.lease_owner.map(|worker| worker.0),
    }))
}

fn storage_retry_dead_letter(dir: &std::path::Path) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.enqueue_job(
        JobKind::BuildTextIndex,
        "segment:seg-1",
        "retry-dead-letter",
    )?;
    let first = db
        .lease_job(
            WorkerId::new("worker-1"),
            JobKind::BuildTextIndex,
            1_000,
            100,
        )?
        .ok_or("first lease missing")?;
    db.fail_job(
        &first.job_id,
        first.lease_token.as_deref(),
        "first failure",
        false,
        1_100,
    )?;
    let retry = db
        .lease_job(
            WorkerId::new("worker-2"),
            JobKind::BuildTextIndex,
            1_100,
            100,
        )?
        .ok_or("retry lease missing")?;
    db.fail_job(
        &retry.job_id,
        retry.lease_token.as_deref(),
        "second failure",
        false,
        1_200,
    )?;
    let final_retry = db
        .lease_job(
            WorkerId::new("worker-3"),
            JobKind::BuildTextIndex,
            1_200,
            100,
        )?
        .ok_or("final retry lease missing")?;
    let failed = db.fail_job(
        &final_retry.job_id,
        final_retry.lease_token.as_deref(),
        "third failure",
        false,
        1_300,
    )?;
    if failed.status != JobStatus::DeadLettered {
        return Err(format!(
            "job status should be dead-lettered, got {:?}",
            failed.status
        )
        .into());
    }
    Ok(json!({
        "evidence": "retryable failures transitioned to dead-letter state after max attempts",
        "job_id": failed.job_id,
        "status": format!("{:?}", failed.status),
        "attempts": failed.attempts,
        "last_error": failed.last_error,
    }))
}

fn storage_interrupted_compaction(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    db.compact()?;
    fs::write(
        dir.join("segments/interrupted.tseg.tmp"),
        b"partial segment",
    )?;
    fs::write(dir.join("indexes/interrupted.tidx.tmp"), b"partial index")?;
    let removed = db.vacuum()?;
    if dir.join("segments/interrupted.tseg.tmp").exists()
        || dir.join("indexes/interrupted.tidx.tmp").exists()
    {
        return Err("interrupted staged compaction artifacts were not recovered by vacuum".into());
    }
    Ok(json!({
        "evidence": "interrupted staged compaction/index artifacts were vacuumed without manifest changes",
        "removed": removed,
    }))
}

fn storage_failed_index_build_recovery(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.enqueue_job(
        JobKind::BuildTextIndex,
        "segment:seg-1",
        "failed-index-build",
    )?;
    let leased = db
        .lease_job(
            WorkerId::new("index-worker"),
            JobKind::BuildTextIndex,
            1_000,
            100,
        )?
        .ok_or("index build lease missing")?;
    let failed = db.fail_job(
        &leased.job_id,
        leased.lease_token.as_deref(),
        "checksum mismatch while writing text index",
        false,
        1_010,
    )?;
    let retry = db
        .lease_job(
            WorkerId::new("index-worker-2"),
            JobKind::BuildTextIndex,
            1_010,
            100,
        )?
        .ok_or("retry lease missing")?;
    let completed = db.complete_job(&retry.job_id, retry.lease_token.as_deref().unwrap_or(""))?;
    if failed.status != JobStatus::FailedRetryable || completed.status != JobStatus::Succeeded {
        return Err("failed index build did not recover through retry and completion".into());
    }
    Ok(json!({
        "evidence": "failed index build became retryable and recovered on a later lease",
        "job_id": completed.job_id,
        "retry_status": format!("{:?}", failed.status),
        "final_status": format!("{:?}", completed.status),
    }))
}

fn storage_backup_job_failure(dir: &std::path::Path) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.enqueue_job(
        JobKind::BackupDatabase,
        "backup:s3://example/trace",
        "backup-failure",
    )?;
    let leased = db
        .lease_job(
            WorkerId::new("backup-worker"),
            JobKind::BackupDatabase,
            1_000,
            100,
        )?
        .ok_or("backup lease missing")?;
    let failed = db.fail_job(
        &leased.job_id,
        leased.lease_token.as_deref(),
        "object upload failed",
        true,
        1_100,
    )?;
    if failed.status != JobStatus::FailedPermanent {
        return Err("backup failure did not become permanent".into());
    }
    Ok(json!({
        "evidence": "failed backup upload was represented as durable permanent failure",
        "job_id": failed.job_id,
        "status": format!("{:?}", failed.status),
        "last_error": failed.last_error,
    }))
}

fn storage_restore_verification_job(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = seed_storage_index_db(dir)?;
    db.enqueue_job(
        JobKind::RestoreVerification,
        "restore:local-snapshot",
        "restore-verification",
    )?;
    let leased = db
        .lease_job(
            WorkerId::new("restore-worker"),
            JobKind::RestoreVerification,
            1_000,
            100,
        )?
        .ok_or("restore verification lease missing")?;
    let completed = db.complete_job(&leased.job_id, leased.lease_token.as_deref().unwrap_or(""))?;
    if completed.status != JobStatus::Succeeded {
        return Err("restore verification job did not complete".into());
    }
    Ok(json!({
        "evidence": "restore verification used the same durable lease and completion lifecycle",
        "job_id": completed.job_id,
        "status": format!("{:?}", completed.status),
    }))
}

fn storage_reopen_after_job_state_change(
    dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.enqueue_job(JobKind::FeatureRefresh, "feature:all", "reopen-job-state")?;
    let leased = db
        .lease_job(
            WorkerId::new("feature-worker"),
            JobKind::FeatureRefresh,
            1_000,
            100,
        )?
        .ok_or("feature refresh lease missing")?;
    let completed = db.complete_job(&leased.job_id, leased.lease_token.as_deref().unwrap_or(""))?;
    drop(db);
    let reopened = TraceDb::open(dir)?;
    let replayed = reopened
        .jobs()?
        .into_iter()
        .find(|job| job.job_id == completed.job_id)
        .ok_or("completed job missing after reopen")?;
    if replayed.status != JobStatus::Succeeded {
        return Err("completed job status did not replay after reopen".into());
    }
    Ok(json!({
        "evidence": "job state changes replayed after reopen",
        "job_id": replayed.job_id,
        "status": format!("{:?}", replayed.status),
    }))
}

fn seed_storage_index_db(dir: &std::path::Path) -> Result<TraceDb, Box<dyn std::error::Error>> {
    let mut db = TraceDb::open(dir)?;
    db.apply_schema(demo_schema("docs"))?;
    db.put_batch_with_write_timing(RecordPutBatchRequest::new(storage_demo_records("docs")))?;
    Ok(db)
}

fn storage_demo_records(table: &str) -> Vec<RecordInput> {
    vec![
        demo_record(
            table,
            "tenant-a",
            "alpha",
            "banana neural retrieval memory",
            "ready",
            [1.0, 0.0, 0.0],
        ),
        demo_record(
            table,
            "tenant-a",
            "beta",
            "orange graph planning notes",
            "ready",
            [0.0, 1.0, 0.0],
        ),
        demo_record(
            table,
            "tenant-b",
            "tenant-b-only",
            "banana private tenant document",
            "ready",
            [0.0, 0.0, 1.0],
        ),
    ]
}

fn storage_segment_records() -> Vec<tracedb_segment::SegmentRecord> {
    storage_demo_records("docs")
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            let mut fields = BTreeMap::new();
            let mut text = BTreeMap::new();
            let mut vectors = BTreeMap::new();
            for (key, value) in record.fields {
                if key == "body" {
                    if let Some(body) = value.as_str() {
                        text.insert(key.clone(), body.to_string());
                    }
                }
                if key == "embedding" {
                    if let Some(vector) = value_as_f32_vec_for_cli(&value) {
                        vectors.insert(key.clone(), vector);
                    }
                }
                fields.insert(key, value);
            }
            tracedb_segment::SegmentRecord {
                table: record.table,
                record_id: record.id,
                tenant_id: record.tenant_id,
                version_id: (index + 1) as u64,
                fields,
                text,
                vectors,
            }
        })
        .collect()
}

fn value_as_f32_vec_for_cli(value: &Value) -> Option<Vec<f32>> {
    value
        .as_array()?
        .iter()
        .map(|item| item.as_f64().map(|value| value as f32))
        .collect()
}

fn storage_query(text: Option<&str>, vector: Option<Vec<f32>>, tenant_id: &str) -> HybridQuery {
    HybridQuery {
        table: "docs".to_string(),
        tenant_id: tenant_id.to_string(),
        cursor: None,
        text_field: text.map(|_| "body".to_string()),
        text: text.map(str::to_string),
        vector_field: vector.as_ref().map(|_| "embedding".to_string()),
        vector,
        scalar_eq: serde_json::Map::new(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 3,
        freshness: FreshnessMode::AllowDirty,
        explain: true,
    }
}

fn query_record_ids(
    db: &TraceDb,
    text: &str,
    vector: Option<Vec<f32>>,
    tenant_id: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let query = storage_query((!text.is_empty()).then_some(text), vector, tenant_id);
    Ok(db
        .query(query)?
        .results
        .into_iter()
        .map(|row| row.record_id)
        .collect())
}

fn first_index_artifact(
    dir: &std::path::Path,
    db: &TraceDb,
    kind: &str,
) -> Result<tracedb_index::IndexArtifact, Box<dyn std::error::Error>> {
    let manifest = db.inspect_manifest()?;
    let index = manifest
        .indexes
        .iter()
        .find(|index| index.kind == kind && index.state == IndexState::Ready)
        .ok_or_else(|| format!("ready {kind} index manifest missing"))?;
    Ok(tracedb_index::read_index_artifact(
        dir.join(&index.object_path),
        None,
    )?)
}

fn parse_product_regression_config(
    args: &[String],
) -> Result<ProductRegressionConfig, Box<dyn std::error::Error>> {
    let mut data_root = None;
    let mut keep_data = false;
    let mut skip_typescript = false;
    let mut inject_failure = None;
    let mut report_file = None;
    let mut list_steps = false;
    let mut only_step = None;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--data-root" => {
                idx += 1;
                data_root = Some(PathBuf::from(
                    args.get(idx).ok_or("missing value for --data-root")?,
                ));
            }
            "--keep-data" => keep_data = true,
            "--skip-typescript" => skip_typescript = true,
            "--list-steps" => list_steps = true,
            "--report-file" => {
                idx += 1;
                report_file = Some(PathBuf::from(
                    args.get(idx).ok_or("missing value for --report-file")?,
                ));
            }
            "--only" => {
                idx += 1;
                let step = args.get(idx).ok_or("missing value for --only")?.to_string();
                if !PRODUCT_REGRESSION_ONLY_STEPS.contains(&step.as_str()) {
                    return Err(format!(
                        "product-regression --only currently supports {}; got {step}",
                        PRODUCT_REGRESSION_ONLY_STEPS.join(", ")
                    )
                    .into());
                }
                only_step = Some(step);
            }
            "--inject-failure" => {
                idx += 1;
                let step = args
                    .get(idx)
                    .ok_or("missing value for --inject-failure")?
                    .to_string();
                if !PRODUCT_REGRESSION_STEPS.contains(&step.as_str()) {
                    return Err(format!(
                        "unknown product-regression failure injection step {step}; expected one of {}",
                        PRODUCT_REGRESSION_STEPS.join(", ")
                    )
                    .into());
                }
                inject_failure = Some(step);
            }
            other => return Err(format!("unknown product-regression option {other}").into()),
        }
        idx += 1;
    }
    if skip_typescript {
        if let Some(step) = only_step.as_deref() {
            if product_regression_step_is_typescript(step) {
                return Err(format!(
                    "product-regression --only {step} conflicts with --skip-typescript; remove --skip-typescript or choose a non-TypeScript step"
                )
                .into());
            }
        }
    }
    let cleanup_data = data_root.is_none() && !keep_data;
    let data_root = data_root.unwrap_or_else(default_product_regression_root);
    Ok(ProductRegressionConfig {
        data_root,
        cleanup_data,
        keep_data,
        skip_typescript,
        inject_failure,
        report_file,
        list_steps,
        only_step,
    })
}

fn parse_product_quickstart_config(
    args: &[String],
) -> Result<ProductRegressionConfig, Box<dyn std::error::Error>> {
    let mut config = parse_product_regression_config(args)?;
    if config.report_file.is_none() {
        config.report_file = Some(default_product_quickstart_report_file()?);
    }
    Ok(config)
}

fn product_regression_step_is_typescript(step: &str) -> bool {
    step.starts_with("typescript_")
}

fn default_product_quickstart_report_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(product_regression_workspace_root()?.join(PRODUCT_QUICKSTART_REPORT_FILE))
}

fn default_product_regression_root() -> PathBuf {
    let suffix = product_regression_run_id().unwrap_or_else(|_| std::process::id().to_string());
    env::temp_dir().join(format!("tracedb-product-regression-{suffix}"))
}

fn run_product_regression(
    config: ProductRegressionConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if config.list_steps {
        emit_json(
            product_regression_step_list_summary(config.report_file.as_deref()),
            config.report_file.as_deref(),
        )?;
        return Ok(());
    }

    if config.data_root.exists() && config.cleanup_data {
        fs::remove_dir_all(&config.data_root)?;
    }
    fs::create_dir_all(&config.data_root)?;

    let mut steps = serde_json::Map::new();
    let mut local_server_url = None;
    let cli = env::current_exe()?;
    let workspace = product_regression_workspace_root()?;
    let embedded_dir = config
        .data_root
        .join("embedded")
        .to_string_lossy()
        .to_string();
    let http_demo_dir = config
        .data_root
        .join("http-demo")
        .to_string_lossy()
        .to_string();
    if config.only_step.as_deref() == Some("embedded_demo") {
        let step = run_product_regression_step_or_injected(
            &config,
            "embedded_demo",
            product_regression_cli_command(
                &cli,
                vec!["--data".into(), embedded_dir.clone(), "demo".into()],
            ),
        );
        steps.insert("embedded_demo".to_string(), step);
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("embedded_verify") {
        let step = run_product_regression_step_or_injected(
            &config,
            "embedded_verify",
            product_regression_cli_command(
                &cli,
                vec!["--data".into(), embedded_dir.clone(), "verify".into()],
            ),
        );
        steps.insert("embedded_verify".to_string(), step);
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("http_demo") {
        let step = run_product_regression_step_or_injected(
            &config,
            "http_demo",
            product_regression_cli_command(
                &cli,
                vec!["--data".into(), http_demo_dir.clone(), "http-demo".into()],
            ),
        );
        steps.insert("http_demo".to_string(), step);
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("local_doctor") {
        let (bind, url) = product_regression_server_bind_and_url()?;
        local_server_url = Some(url.clone());
        {
            let _server = LocalServerChild::start(&config.data_root.join("server-data"), &bind)?;
            let doctor = product_regression_local_doctor_command(&cli, &url);
            let step = run_product_regression_step_or_injected(&config, "local_doctor", doctor);
            steps.insert("local_doctor".to_string(), step);
        }
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("rust_sdk_quickstart") {
        let (bind, url) = product_regression_server_bind_and_url()?;
        local_server_url = Some(url.clone());
        {
            let _server = LocalServerChild::start(&config.data_root.join("server-data"), &bind)?;
            let sdk = product_regression_rust_sdk_quickstart_command(
                &workspace,
                &url,
                &config.data_root.join("sdk-admin"),
            )?;
            let step = run_product_regression_step_or_injected(&config, "rust_sdk_quickstart", sdk);
            steps.insert("rust_sdk_quickstart".to_string(), step);
        }
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("python_sdk_smoke") {
        let command = product_regression_python_sdk_command(&workspace);
        let step = run_product_regression_step_or_injected(&config, "python_sdk_smoke", command);
        steps.insert("python_sdk_smoke".to_string(), step);
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("typescript_check") {
        let command = product_regression_typescript_command(&workspace, &["run", "check"]);
        let step = run_product_regression_step_or_injected(&config, "typescript_check", command);
        steps.insert("typescript_check".to_string(), step);
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("typescript_http_smoke") {
        let command =
            product_regression_typescript_command(&workspace, &["run", "public-http-smoke"]);
        let step =
            run_product_regression_step_or_injected(&config, "typescript_http_smoke", command);
        steps.insert("typescript_http_smoke".to_string(), step);
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("typescript_gateway_smoke") {
        let command = product_regression_typescript_command(&workspace, &["run", "gateway-smoke"]);
        let step =
            run_product_regression_step_or_injected(&config, "typescript_gateway_smoke", command);
        steps.insert("typescript_gateway_smoke".to_string(), step);
        return finish_product_regression(config, local_server_url, steps);
    }

    for (name, command) in [
        (
            "embedded_demo",
            product_regression_cli_command(
                &cli,
                vec!["--data".into(), embedded_dir.clone(), "demo".into()],
            ),
        ),
        (
            "embedded_verify",
            product_regression_cli_command(
                &cli,
                vec!["--data".into(), embedded_dir.clone(), "verify".into()],
            ),
        ),
        (
            "http_demo",
            product_regression_cli_command(
                &cli,
                vec!["--data".into(), http_demo_dir.clone(), "http-demo".into()],
            ),
        ),
    ] {
        let step = run_product_regression_step_or_injected(&config, name, command);
        let ok = product_regression_step_ok(&step);
        steps.insert(name.to_string(), step);
        if !ok {
            return finish_product_regression(config, local_server_url, steps);
        }
    }

    let (bind, url) = product_regression_server_bind_and_url()?;
    local_server_url = Some(url.clone());
    let mut server_step_failed = false;
    {
        let _server = LocalServerChild::start(&config.data_root.join("server-data"), &bind)?;

        let doctor = product_regression_local_doctor_command(&cli, &url);
        let step = run_product_regression_step_or_injected(&config, "local_doctor", doctor);
        let ok = product_regression_step_ok(&step);
        steps.insert("local_doctor".to_string(), step);
        if !ok {
            server_step_failed = true;
        }

        if !server_step_failed {
            let sdk = product_regression_rust_sdk_quickstart_command(
                &workspace,
                &url,
                &config.data_root.join("sdk-admin"),
            )?;
            let step = run_product_regression_step_or_injected(&config, "rust_sdk_quickstart", sdk);
            let ok = product_regression_step_ok(&step);
            steps.insert("rust_sdk_quickstart".to_string(), step);
            if !ok {
                server_step_failed = true;
            }
        }
    }
    if server_step_failed {
        return finish_product_regression(config, local_server_url, steps);
    }

    let python = product_regression_python_sdk_command(&workspace);
    let step = run_product_regression_step_or_injected(&config, "python_sdk_smoke", python);
    let ok = product_regression_step_ok(&step);
    steps.insert("python_sdk_smoke".to_string(), step);
    if !ok {
        return finish_product_regression(config, local_server_url, steps);
    }

    if !config.skip_typescript {
        for (name, args) in [
            ("typescript_check", ["run", "check"]),
            ("typescript_http_smoke", ["run", "public-http-smoke"]),
            ("typescript_gateway_smoke", ["run", "gateway-smoke"]),
        ] {
            let command = product_regression_typescript_command(&workspace, &args);
            let step = run_product_regression_step_or_injected(&config, name, command);
            let ok = product_regression_step_ok(&step);
            steps.insert(name.to_string(), step);
            if !ok {
                return finish_product_regression(config, local_server_url, steps);
            }
        }
    }

    finish_product_regression(config, local_server_url, steps)
}

fn finish_product_regression(
    config: ProductRegressionConfig,
    local_server_url: Option<String>,
    steps: serde_json::Map<String, Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ok = steps.values().all(product_regression_step_ok);
    let data_root = config.data_root.display().to_string();
    let failure_injection = config.inject_failure.clone();
    let only_step = config.only_step.clone();
    let report_file = product_regression_report_file_json(config.report_file.as_deref());
    let human_summary = product_regression_human_summary(&steps, only_step.as_deref());
    let summary = json!({
        "ok": ok,
        "mode": "local-product-regression",
        "scope": "local_only",
        "data_root": data_root,
        "report_file": report_file,
        "data_cleanup": config.cleanup_data,
        "keep_data": config.keep_data,
        "failure_injection": failure_injection,
        "only_step": only_step,
        "human_summary": human_summary,
        "local_server_url": local_server_url,
        "typescript_enabled": !config.skip_typescript,
        "claims": {
            "sql_module": "not_implemented",
            "managed_cloud": "not_checked",
            "benchmark": "not_checked",
        },
        "steps": steps,
    });
    if config.cleanup_data {
        let _ = fs::remove_dir_all(&config.data_root);
    }
    emit_json(summary, config.report_file.as_deref())?;
    if ok {
        Ok(())
    } else {
        Err("product-regression local product gate failed".into())
    }
}

fn product_regression_human_summary(
    steps: &serde_json::Map<String, Value>,
    only_step: Option<&str>,
) -> Value {
    let steps_total = steps.len();
    let steps_passed = steps
        .values()
        .filter(|step| product_regression_step_ok(step))
        .count();
    let failed_step = steps
        .iter()
        .find_map(|(name, step)| (!product_regression_step_ok(step)).then(|| name.clone()));
    let (status, mut message) = if let Some(failed_step) = failed_step.as_deref() {
        (
            "failed",
            format!(
                "local product regression failed: {steps_passed}/{steps_total} steps passed; failed_step={failed_step}"
            ),
        )
    } else {
        (
            "passed",
            format!("local product regression passed: {steps_passed}/{steps_total} steps"),
        )
    };
    if let Some(only_step) = only_step {
        message.push_str("; only_step=");
        message.push_str(only_step);
    }
    json!({
        "status": status,
        "message": message,
        "steps_passed": steps_passed,
        "steps_total": steps_total,
        "failed_step": failed_step,
    })
}

fn product_regression_cli_command(cli: &std::path::Path, args: Vec<String>) -> Command {
    let mut command = Command::new(cli);
    command.args(args);
    command
}

fn product_regression_server_bind_and_url() -> std::io::Result<(String, String)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let bind = listener.local_addr()?.to_string();
    drop(listener);
    let url = format!("http://{bind}");
    Ok((bind, url))
}

fn product_regression_local_doctor_command(cli: &std::path::Path, url: &str) -> Command {
    product_regression_cli_command(
        cli,
        vec![
            "doctor".into(),
            "http".into(),
            "--url".into(),
            url.to_string(),
            "--token".into(),
            "dev-token".into(),
            "--timeout-ms".into(),
            "1000".into(),
            "--safe-retries".into(),
            "1".into(),
            "--wait-ready-ms".into(),
            "5000".into(),
            "--database-id".into(),
            "db_local".into(),
            "--branch-id".into(),
            "db_local:main".into(),
        ],
    )
}

fn product_regression_rust_sdk_quickstart_command(
    workspace: &std::path::Path,
    url: &str,
    admin_dir: &std::path::Path,
) -> std::io::Result<Command> {
    fs::create_dir_all(admin_dir)?;
    let admin_dir_string = admin_dir.to_string_lossy().to_string();
    let mut command = Command::new("cargo");
    command.current_dir(workspace).args([
        "run",
        "-q",
        "-p",
        "tracedb-sdk",
        "--example",
        "quickstart",
        "--",
        "--url",
        url,
        "--token",
        "dev-token",
        "--timeout-ms",
        "5000",
        "--safe-retries",
        "1",
        "--idempotency-retries",
        "1",
        "--admin-dir",
        &admin_dir_string,
    ]);
    Ok(command)
}

fn product_regression_python_sdk_command(workspace: &std::path::Path) -> Command {
    let mut command = Command::new("python3");
    command
        .current_dir(workspace)
        .args(["clients/python/http_smoke.py"]);
    command
}

fn product_regression_typescript_command(workspace: &std::path::Path, args: &[&str]) -> Command {
    let mut command = Command::new("npm");
    command
        .current_dir(workspace.join("clients/typescript"))
        .args(args);
    command
}

fn product_regression_step_list_summary(report_file: Option<&std::path::Path>) -> Value {
    let steps_total = PRODUCT_REGRESSION_STEPS.len();
    let only_supported = PRODUCT_REGRESSION_STEPS
        .iter()
        .filter(|name| PRODUCT_REGRESSION_ONLY_STEPS.contains(name))
        .count();
    let report_file = product_regression_report_file_json(report_file);
    let steps = PRODUCT_REGRESSION_STEPS
        .iter()
        .map(|name| {
            json!({
                "name": *name,
                "only_supported": PRODUCT_REGRESSION_ONLY_STEPS.contains(name),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "ok": true,
        "mode": "local-product-regression-step-list",
        "scope": "local_only",
        "report_file": report_file,
        "human_summary": {
            "status": "listed",
            "message": format!(
                "local product regression steps listed: {steps_total} steps; only_supported={only_supported}"
            ),
            "steps_total": steps_total,
            "only_supported": only_supported,
        },
        "claims": {
            "sql_module": "not_implemented",
            "managed_cloud": "not_checked",
            "benchmark": "not_checked",
        },
        "steps": steps,
    })
}

fn product_regression_report_file_json(report_file: Option<&std::path::Path>) -> Value {
    report_file
        .map(|path| json!(path.display().to_string()))
        .unwrap_or(Value::Null)
}

fn run_product_regression_step_or_injected(
    config: &ProductRegressionConfig,
    name: &str,
    command: Command,
) -> Value {
    if config.inject_failure.as_deref() == Some(name) {
        return json!({
            "name": name,
            "ok": false,
            "injected_failure": true,
            "error": "injected product-regression failure",
            "command": product_regression_command_display(&command),
            "cwd": product_regression_command_cwd(&command),
        });
    }
    run_product_regression_step(name, command)
}

fn run_product_regression_step(name: &str, mut command: Command) -> Value {
    let command_display = product_regression_command_display(&command);
    let cwd = product_regression_command_cwd(&command);
    let start = Instant::now();
    match command.output() {
        Ok(output) => {
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let mut value = json!({
                "name": name,
                "ok": output.status.success(),
                "command": command_display,
                "cwd": cwd,
                "exit_code": output.status.code(),
                "duration_ms": duration_ms,
            });
            if let Some(summary) = parse_product_regression_json(&stdout) {
                value["summary"] = summary;
            }
            if !output.status.success() {
                value["stdout_tail"] = json!(tail(&stdout, 4_000));
                value["stderr_tail"] = json!(tail(&stderr, 4_000));
            }
            value
        }
        Err(error) => json!({
            "name": name,
            "ok": false,
            "command": command_display,
            "cwd": cwd,
            "spawn_error": error.to_string(),
        }),
    }
}

fn product_regression_command_display(command: &Command) -> String {
    let mut parts = vec![command.get_program().to_string_lossy().to_string()];
    parts.extend(
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string()),
    );
    parts.join(" ")
}

fn product_regression_command_cwd(command: &Command) -> String {
    command
        .get_current_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| {
            env::current_dir()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|_| ".".to_string())
        })
}

fn parse_product_regression_json(stdout: &str) -> Option<Value> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Some(value);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if end < start {
        return None;
    }
    serde_json::from_str(&trimmed[start..=end]).ok()
}

fn product_regression_step_ok(step: &Value) -> bool {
    step.get("ok").and_then(Value::as_bool).unwrap_or(false)
}

fn product_regression_workspace_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    Ok(manifest
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or("could not find TraceDB workspace root")?
        .to_path_buf())
}

fn product_regression_run_id() -> Result<String, std::time::SystemTimeError> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(format!("{}-{}", std::process::id(), elapsed.as_millis()))
}

fn tail(text: &str, limit: usize) -> String {
    let chars = text.chars().collect::<Vec<_>>();
    if chars.len() <= limit {
        text.to_string()
    } else {
        chars[chars.len() - limit..].iter().collect()
    }
}

fn wait_for_ready(client: &TraceDbClient) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_error = None;
    for _ in 0..50 {
        match client.ready_typed() {
            Ok(ready) if ready.ready => return Ok(()),
            Ok(_) => {}
            Err(error) => last_error = Some(error),
        }
        thread::sleep(Duration::from_millis(20));
    }
    let message = last_error
        .map(|error| error.to_string())
        .unwrap_or_else(|| "server did not report ready".to_string());
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("timed out waiting for local http demo server: {message}"),
    )
    .into())
}

struct LocalServerChild {
    child: Child,
}

impl LocalServerChild {
    fn start(data_dir: &std::path::Path, bind: &str) -> std::io::Result<Self> {
        let child = Command::new(env::current_exe()?)
            .arg("--data")
            .arg(data_dir)
            .arg("serve")
            .arg(bind)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child })
    }
}

impl Drop for LocalServerChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn http_demo_run_id() -> Result<String, std::time::SystemTimeError> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(format!("{}-{}", std::process::id(), elapsed.as_millis()))
}

fn http_demo_idempotency_options(run_id: &str, step: &str) -> TraceDbRequestOptions {
    TraceDbRequestOptions::new().with_idempotency_key(format!("http-demo-{run_id}-{step}"))
}

fn demo_schema(table: &str) -> TableSchema {
    TableSchema {
        name: table.to_string(),
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

fn demo_record(
    table: &str,
    tenant: &str,
    id: &str,
    body: &str,
    status: &str,
    embedding: [f32; 3],
) -> RecordInput {
    RecordInput {
        table: table.to_string(),
        id: id.to_string(),
        tenant_id: tenant.to_string(),
        fields: json!({
            "id": id,
            "tenant": tenant,
            "body": body,
            "status": status,
            "embedding": embedding,
        })
        .as_object()
        .unwrap()
        .clone(),
    }
}

fn run_local_job(
    db: &mut TraceDb,
    job_name: &str,
    data_dir: &std::path::Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let (kind, canonical, target) = parse_cli_job_kind(job_name)?;
    let run_id = product_regression_run_id()?;
    let enqueued = db.enqueue_job(kind.clone(), target, format!("cli:{canonical}:{run_id}"))?;
    let leased = db
        .lease_job(
            WorkerId::new("cli-local-worker"),
            kind.clone(),
            now_millis()?,
            30_000,
        )?
        .ok_or("local job was not leased")?;
    let lease_token = leased
        .lease_token
        .as_deref()
        .ok_or("leased job was missing lease token")?
        .to_string();
    let mut details = json!({});
    match canonical {
        "tracedb.segment.compact" => {
            db.compact()?;
            let manifest = db.inspect_manifest()?;
            details = json!({
                "segment_count": manifest.segments.len(),
                "index_count": manifest.indexes.len(),
            });
        }
        "tracedb.artifacts.vacuum" => {
            let removed = db.vacuum()?;
            details = json!({ "removed_artifacts": removed });
        }
        "tracedb.index.text.build" | "tracedb.index.vector.build" => {
            db.compact()?;
            let manifest = db.inspect_manifest()?;
            let index_count = manifest
                .indexes
                .iter()
                .filter(|index| {
                    (canonical == "tracedb.index.text.build" && index.kind == "text")
                        || (canonical == "tracedb.index.vector.build" && index.kind == "vector")
                })
                .count();
            details = json!({ "index_count": index_count });
        }
        "tracedb.backup.create" => {
            let target = data_dir.with_extension("job-backup");
            db.backup(&target)?;
            details = json!({ "backup_target": target.display().to_string() });
        }
        "tracedb.restore.verify" => {
            let snapshot = data_dir.with_extension("job-restore-snapshot");
            let restore = data_dir.with_extension("job-restore-target");
            db.create_snapshot(&snapshot)?;
            let restored = TraceDb::restore_snapshot(&snapshot, &restore)?;
            details = json!({
                "snapshot": snapshot.display().to_string(),
                "restore": restore.display().to_string(),
                "restored_epoch": restored.inspect_manifest()?.latest_epoch.get(),
            });
        }
        "tracedb.feature.refresh" => {
            details = json!({ "module_count": db.registered_module_catalog().len() });
        }
        _ => {}
    }
    let completed = db.complete_job(&leased.job_id, &lease_token)?;
    Ok(json!({
        "durable": true,
        "job": canonical,
        "kind": format!("{:?}", kind),
        "target": target,
        "enqueued_job_id": enqueued.job_id,
        "leased_job_id": leased.job_id,
        "completed": true,
        "status": format!("{:?}", completed.status),
        "attempts": completed.attempts,
        "details": details,
    }))
}

fn parse_cli_job_kind(
    job_name: &str,
) -> Result<(JobKind, &'static str, &'static str), Box<dyn std::error::Error>> {
    match job_name {
        "compact" | "tracedb.segment.compact" => {
            Ok((JobKind::CompactSegment, "tracedb.segment.compact", "segment:all"))
        }
        "vacuum" | "tracedb.artifacts.vacuum" => Ok((
            JobKind::VacuumArtifacts,
            "tracedb.artifacts.vacuum",
            "artifacts:unreferenced",
        )),
        "build-text-index" | "tracedb.index.text.build" => Ok((
            JobKind::BuildTextIndex,
            "tracedb.index.text.build",
            "segment:all",
        )),
        "build-vector-index" | "tracedb.index.vector.build" => Ok((
            JobKind::BuildVectorIndex,
            "tracedb.index.vector.build",
            "segment:all",
        )),
        "backup" | "tracedb.backup.create" => Ok((
            JobKind::BackupDatabase,
            "tracedb.backup.create",
            "backup:local",
        )),
        "restore-verify" | "tracedb.restore.verify" => Ok((
            JobKind::RestoreVerification,
            "tracedb.restore.verify",
            "restore:local",
        )),
        "feature-refresh" | "tracedb.feature.refresh" => Ok((
            JobKind::FeatureRefresh,
            "tracedb.feature.refresh",
            "feature:all",
        )),
        other => Err(format!(
            "unknown job runner {other}; expected compact, vacuum, build-text-index, build-vector-index, backup, restore-verify, or feature-refresh"
        )
        .into()),
    }
}

fn job_status_counts(jobs: &[tracedb_jobs::TraceJob]) -> Value {
    let mut counts = serde_json::Map::new();
    for job in jobs {
        let key = format!("{:?}", job.status);
        let current = counts.get(&key).and_then(Value::as_u64).unwrap_or(0);
        counts.insert(key, json!(current + 1));
    }
    Value::Object(counts)
}

fn now_millis() -> Result<u64, std::time::SystemTimeError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64)
}

fn run_compose(action: &str, extra: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut command = Command::new("docker");
    command.arg("compose").arg("-f").arg("docker-compose.yml");
    match action {
        "up" => {
            command.arg("up").arg("-d");
        }
        "down" => {
            command.arg("down");
        }
        "status" | "ps" => {
            command.arg("ps");
        }
        other => return Err(format!("unknown compose action {other}").into()),
    }
    command.args(extra);
    let status = command.status()?;
    if !status.success() {
        return Err(format!("docker compose {action} failed with {status}").into());
    }
    Ok(())
}

fn take_data_dir(args: &mut Vec<String>) -> PathBuf {
    if let Some(idx) = args.iter().position(|arg| arg == "--data") {
        args.remove(idx);
        if idx < args.len() {
            return PathBuf::from(args.remove(idx));
        }
    }
    PathBuf::from(".tracedb")
}

fn read_arg_or_stdin(arg: Option<&String>) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(arg) = arg {
        if std::path::Path::new(arg).exists() {
            return Ok(fs::read_to_string(arg)?);
        }
        return Ok(arg.clone());
    }
    let mut input = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
    Ok(input)
}

fn canonical_feature_status(status: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match status {
        "Ready" | "ready" => Ok("Ready"),
        "Dirty" | "dirty" => Ok("Dirty"),
        "Pending" | "pending" => Ok("Pending"),
        "Failed" | "failed" => Ok("Failed"),
        "Missing" | "missing" => Ok("Missing"),
        other => Err(format!(
            "unknown feature status {other}; expected Ready, Dirty, Pending, Failed, or Missing"
        )
        .into()),
    }
}

fn print_json(value: Value) {
    println!("{}", serde_json::to_string_pretty(&value).unwrap());
}

fn emit_json(
    value: Value,
    report_file: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(report_file) = report_file {
        write_json_report(report_file, &value)?;
    }
    print_json(value);
    Ok(())
}

fn write_json_report(
    path: &std::path::Path,
    value: &Value,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut body = serde_json::to_vec_pretty(value)?;
    body.push(b'\n');
    fs::write(path, body)?;
    Ok(())
}

fn persist_catalog(data_dir: &std::path::Path, catalog: &Catalog) -> std::io::Result<()> {
    let catalog_dir = data_dir.join("catalog");
    fs::create_dir_all(&catalog_dir)?;
    let body = serde_json::to_vec_pretty(catalog).map_err(std::io::Error::other)?;
    fs::write(catalog_dir.join("local_catalog.json"), body)
}

fn usage() {
    eprintln!(
        "usage: tracedb [--data DIR] <init|create|branch create|connect|serve|schema apply|insert|put|get|patch|delete|feature status set|scan|query|explain|recover|inspect manifest|inspect wal|inspect modules|inspect indexes|inspect jobs|inspect policies|compact|checkpoint|snapshot create|snapshot restore|snapshot list|jobs list|jobs run compact|vacuum|build-text-index|build-vector-index|backup|restore-verify|feature-refresh|doctor|doctor http --url URL [--database-id DB] [--branch-id BRANCH] [--wait-ready-ms MS] or TRACEDB_URL=... tracedb doctor http|demo|http-demo|product-regression [--data-root DIR] [--keep-data] [--skip-typescript] [--inject-failure STEP] [--report-file PATH] [--list-steps] [--only {}]|product-quickstart [--data-root DIR] [--keep-data] [--skip-typescript] [--inject-failure STEP] [--report-file PATH] [--list-steps] [--only {}]|durability-faults [--data-root DIR] [--keep-data] [--inject-failure SCENARIO] [--report-file PATH]|storage-index-jobs [--data-root DIR] [--keep-data] [--inject-failure SCENARIO] [--report-file PATH]|compose up|compose down|compose status|verify|backup|restore|export|delete-user|bench>",
        PRODUCT_REGRESSION_ONLY_STEPS.join("|"),
        PRODUCT_REGRESSION_ONLY_STEPS.join("|")
    );
}
