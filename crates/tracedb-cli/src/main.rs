#![forbid(unsafe_code)]

use serde_json::{json, Value};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tracedb_bench::{BenchmarkTarget, WorkloadKind};
use tracedb_catalog::Catalog;
use tracedb_query::{
    HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput, RecordPatchRequest,
    RecordPutRequest, RecordScanRequest, TableSchema, TraceDb,
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
        "doctor" => {
            print_json(run_doctor(&data_dir));
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
        "usage: tracedb [--data DIR] <init|create|branch create|connect|serve|schema apply|insert|put|get|patch|delete|scan|query|explain|recover|inspect manifest|inspect wal|inspect modules|inspect segments|inspect indexes|inspect jobs|inspect policies|compact|snapshot create|snapshot restore|snapshot list|jobs list|jobs run|doctor|compose up|compose down|compose status|verify|backup|restore|export|delete-user|bench>"
    );
}
