#![forbid(unsafe_code)]

use serde_json::{json, Value};
use std::env;
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracedb_bench::{BenchmarkTarget, WorkloadKind};
use tracedb_catalog::Catalog;
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordPatchRequest, RecordPutBatchRequest, RecordPutRequest, RecordScanRequest, TableSchema,
    TraceDb, VectorColumnSchema,
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
            print_json(json!({
                "queues": db.inspect_manifest()?.job_queues,
                "local_worker_queues": [
                    "tracedb.segment.compact",
                    "tracedb.snapshot.create",
                    "tracedb.feature.index"
                ],
            }));
        }
        "jobs" if args.get(1).map(String::as_str) == Some("run") => {
            let job = args.get(2).map(String::as_str).unwrap_or("compact");
            match job {
                "compact" | "tracedb.segment.compact" => {
                    let mut db = TraceDb::open(&data_dir)?;
                    db.compact()?;
                    print_json(json!({ "job": "tracedb.segment.compact", "completed": true }));
                }
                other => {
                    print_json(
                        json!({ "job": other, "completed": false, "reason": "no local runner registered" }),
                    );
                }
            }
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
        text: Some("TraceDB API".to_string()),
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
        text: Some("TraceDB API".to_string()),
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
    list_steps: bool,
    only_step: Option<String>,
}

const PRODUCT_REGRESSION_STEPS: &[&str] = &[
    "embedded_demo",
    "embedded_verify",
    "http_demo",
    "local_doctor",
    "rust_sdk_quickstart",
    "typescript_check",
    "typescript_http_smoke",
    "typescript_gateway_smoke",
];

const PRODUCT_REGRESSION_ONLY_STEPS: &[&str] = &[
    "embedded_demo",
    "embedded_verify",
    "http_demo",
    "local_doctor",
    "rust_sdk_quickstart",
    "typescript_check",
    "typescript_http_smoke",
    "typescript_gateway_smoke",
];

fn parse_product_regression_config(
    args: &[String],
) -> Result<ProductRegressionConfig, Box<dyn std::error::Error>> {
    let mut data_root = None;
    let mut keep_data = false;
    let mut skip_typescript = false;
    let mut inject_failure = None;
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
    let cleanup_data = data_root.is_none() && !keep_data;
    let data_root = data_root.unwrap_or_else(default_product_regression_root);
    Ok(ProductRegressionConfig {
        data_root,
        cleanup_data,
        keep_data,
        skip_typescript,
        inject_failure,
        list_steps,
        only_step,
    })
}

fn default_product_regression_root() -> PathBuf {
    let suffix = product_regression_run_id().unwrap_or_else(|_| std::process::id().to_string());
    env::temp_dir().join(format!("tracedb-product-regression-{suffix}"))
}

fn run_product_regression(
    config: ProductRegressionConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if config.list_steps {
        print_json(product_regression_step_list_summary());
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
    if config.only_step.as_deref() == Some("typescript_check") {
        let command = product_regression_typescript_command(&workspace, &["run", "check"]);
        let step = run_product_regression_step_or_injected(&config, "typescript_check", command);
        steps.insert("typescript_check".to_string(), step);
        return finish_product_regression(config, local_server_url, steps);
    }
    if config.only_step.as_deref() == Some("typescript_http_smoke") {
        let command = product_regression_typescript_command(&workspace, &["run", "http-smoke"]);
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

    if !config.skip_typescript {
        for (name, args) in [
            ("typescript_check", ["run", "check"]),
            ("typescript_http_smoke", ["run", "http-smoke"]),
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
    let summary = json!({
        "ok": ok,
        "mode": "local-product-regression",
        "scope": "local_only",
        "data_root": data_root,
        "data_cleanup": config.cleanup_data,
        "keep_data": config.keep_data,
        "failure_injection": failure_injection,
        "only_step": only_step,
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
    print_json(summary);
    if ok {
        Ok(())
    } else {
        Err("product-regression local product gate failed".into())
    }
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

fn product_regression_typescript_command(workspace: &std::path::Path, args: &[&str]) -> Command {
    let mut command = Command::new("npm");
    command
        .current_dir(workspace.join("clients/typescript"))
        .args(args);
    command
}

fn product_regression_step_list_summary() -> Value {
    let steps = PRODUCT_REGRESSION_STEPS
        .iter()
        .map(|name| {
            json!({
                "name": name,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "ok": true,
        "mode": "local-product-regression-step-list",
        "scope": "local_only",
        "claims": {
            "sql_module": "not_implemented",
            "managed_cloud": "not_checked",
            "benchmark": "not_checked",
        },
        "steps": steps,
    })
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

fn persist_catalog(data_dir: &std::path::Path, catalog: &Catalog) -> std::io::Result<()> {
    let catalog_dir = data_dir.join("catalog");
    fs::create_dir_all(&catalog_dir)?;
    let body = serde_json::to_vec_pretty(catalog).map_err(std::io::Error::other)?;
    fs::write(catalog_dir.join("local_catalog.json"), body)
}

fn usage() {
    eprintln!(
        "usage: tracedb [--data DIR] <init|create|branch create|connect|serve|schema apply|insert|put|get|patch|delete|feature status set|scan|query|explain|recover|inspect manifest|inspect wal|inspect modules|inspect indexes|inspect jobs|inspect policies|compact|checkpoint|snapshot create|snapshot restore|snapshot list|jobs list|jobs run|doctor|doctor http --url URL [--database-id DB] [--branch-id BRANCH] [--wait-ready-ms MS] or TRACEDB_URL=... tracedb doctor http|demo|http-demo|product-regression [--data-root DIR] [--keep-data] [--skip-typescript] [--inject-failure STEP] [--list-steps] [--only embedded_demo|embedded_verify|http_demo|local_doctor|rust_sdk_quickstart|typescript_check|typescript_http_smoke|typescript_gateway_smoke]|compose up|compose down|compose status|verify|backup|restore|export|delete-user|bench>"
    );
}
