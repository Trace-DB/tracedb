use serde_json::json;
use std::env;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordPutBatchRequest, RecordScanRequest, TableSchema, VectorColumnSchema,
};
use tracedb_sdk::{RestoreRequest, SnapshotRequest, TraceDbClient, TraceDbClientConfig};

fn main() {
    if let Err(error) = run() {
        eprintln!("tracedb-sdk quickstart: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = QuickstartArgs::from_env()?;
    if args.help {
        println!("{}", QuickstartArgs::usage());
        return Ok(());
    }

    let mut config = TraceDbClientConfig::managed(args.url, args.token);
    if let Some(database_id) = args.database_id {
        config = config.with_database(database_id);
    }
    if let Some(branch_id) = args.branch_id {
        config = config.with_branch(branch_id);
    }
    if let Some(timeout_ms) = args.timeout_ms {
        config = config.with_timeout(Duration::from_millis(timeout_ms));
    }
    if let Some(safe_retries) = args.safe_retries {
        config = config.with_safe_retries(safe_retries);
    }
    let client = TraceDbClient::new(config);

    let ready = client.ready_typed()?;
    let schema = client.apply_schema_typed(&schema())?;
    let batch = RecordPutBatchRequest::new(vec![
        record(
            "intro",
            "tenant-a",
            "rust database api quickstart",
            [1.0, 0.0, 0.0],
        ),
        record("ops", "tenant-a", "snapshot restore flow", [0.0, 1.0, 0.0]),
    ]);
    let ingest = client.put_batch_typed(&batch)?;
    let scan = client.scan_typed(&RecordScanRequest::new("docs", "tenant-a").limit(10))?;
    let query_response = client.query_typed(&query(false))?;
    let explain = client.explain_typed(&query(false))?;
    let delete = client.delete_typed(&RecordDeleteRequest::new("docs", "tenant-a", "ops"))?;
    let deleted = client.get_record_typed(&RecordGetRequest::new("docs", "tenant-a", "ops"))?;
    let admin = args
        .admin_dir
        .as_ref()
        .map(|admin_dir| run_admin_smoke(&client, admin_dir))
        .transpose()?;

    let summary = json!({
        "ok": true,
        "server_ready": ready.ready,
        "schema_epoch": schema.epoch,
        "records_inserted": ingest.record_count,
        "records_scanned": scan.returned_count,
        "query_result_count": query_response.results.len(),
        "explain_returned_count": explain.returned_count,
        "deleted": delete.deleted,
        "deleted_hidden": deleted.record.is_none(),
        "snapshot_target": admin.as_ref().map(|admin| admin.snapshot_target.as_str()),
        "restore_target": admin.as_ref().map(|admin| admin.restore_target.as_str()),
        "sql_module": "not_implemented",
        "steps": {
            "ready": true,
            "schema_apply": true,
            "batch_ingest": true,
            "scan": true,
            "query": true,
            "explain": true,
            "delete": true,
            "compact": admin.as_ref().map(|admin| admin.compacted).unwrap_or(false),
            "snapshot": admin.as_ref().map(|admin| admin.snapshot).unwrap_or(false),
            "restore": admin.as_ref().map(|admin| admin.restored).unwrap_or(false),
        },
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);

    Ok(())
}

#[derive(Debug)]
struct QuickstartArgs {
    url: String,
    token: String,
    database_id: Option<String>,
    branch_id: Option<String>,
    timeout_ms: Option<u64>,
    safe_retries: Option<u8>,
    admin_dir: Option<PathBuf>,
    help: bool,
}

impl QuickstartArgs {
    fn from_env() -> Result<Self, String> {
        let mut args = Self {
            url: env::var("TRACEDB_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string()),
            token: env::var("TRACEDB_TOKEN").unwrap_or_default(),
            database_id: env::var("TRACEDB_DATABASE_ID").ok(),
            branch_id: env::var("TRACEDB_BRANCH_ID").ok(),
            timeout_ms: env::var("TRACEDB_TIMEOUT_MS")
                .ok()
                .map(|value| parse_timeout_ms(&value))
                .transpose()?,
            safe_retries: env::var("TRACEDB_SAFE_RETRIES")
                .ok()
                .map(|value| parse_safe_retries(&value))
                .transpose()?,
            admin_dir: env::var("TRACEDB_ADMIN_DIR")
                .ok()
                .map(|value| parse_admin_dir(&value, "TRACEDB_ADMIN_DIR"))
                .transpose()?,
            help: false,
        };
        let mut cli = env::args().skip(1);
        while let Some(arg) = cli.next() {
            match arg.as_str() {
                "--url" => args.url = next_value(&mut cli, "--url")?,
                "--token" => args.token = next_value(&mut cli, "--token")?,
                "--database-id" => args.database_id = Some(next_value(&mut cli, "--database-id")?),
                "--branch-id" => args.branch_id = Some(next_value(&mut cli, "--branch-id")?),
                "--timeout-ms" => {
                    args.timeout_ms =
                        Some(parse_timeout_ms(&next_value(&mut cli, "--timeout-ms")?)?)
                }
                "--safe-retries" => {
                    args.safe_retries = Some(parse_safe_retries(&next_value(
                        &mut cli,
                        "--safe-retries",
                    )?)?)
                }
                "--admin-dir" => {
                    args.admin_dir = Some(parse_admin_dir(
                        &next_value(&mut cli, "--admin-dir")?,
                        "--admin-dir",
                    )?)
                }
                "--help" | "-h" => args.help = true,
                unknown => return Err(format!("unknown argument {unknown}\n{}", Self::usage())),
            }
        }
        Ok(args)
    }

    fn usage() -> &'static str {
        "Usage: cargo run -p tracedb-sdk --example quickstart -- --url http://127.0.0.1:8080 [--token TOKEN] [--database-id DB] [--branch-id BRANCH] [--timeout-ms MS] [--safe-retries N] [--admin-dir SERVER_SIDE_DIR]"
    }
}

struct AdminSmokeSummary {
    compacted: bool,
    snapshot: bool,
    restored: bool,
    snapshot_target: String,
    restore_target: String,
}

fn run_admin_smoke(
    client: &TraceDbClient,
    admin_dir: &Path,
) -> Result<AdminSmokeSummary, Box<dyn Error>> {
    let suffix = quickstart_run_suffix()?;
    let snapshot_target = admin_dir.join(format!("quickstart-snapshot-{suffix}"));
    let restore_target = admin_dir.join(format!("quickstart-restore-{suffix}"));
    let snapshot_target = snapshot_target.to_string_lossy().to_string();
    let restore_target = restore_target.to_string_lossy().to_string();

    let compact = client.compact_typed()?;
    let snapshot = client.snapshot_typed(&SnapshotRequest::new(snapshot_target.clone()))?;
    let restore = client.restore_typed(&RestoreRequest::new(
        snapshot_target.clone(),
        restore_target.clone(),
    ))?;

    Ok(AdminSmokeSummary {
        compacted: compact.compacted,
        snapshot: snapshot.snapshot,
        restored: restore.restored,
        snapshot_target: snapshot.target,
        restore_target: restore.target,
    })
}

fn quickstart_run_suffix() -> Result<String, std::time::SystemTimeError> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(format!("{}-{}", std::process::id(), elapsed.as_millis()))
}

fn next_value(cli: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    cli.next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} requires a value"))
}

fn parse_timeout_ms(value: &str) -> Result<u64, String> {
    let timeout_ms = value
        .parse::<u64>()
        .map_err(|_| format!("--timeout-ms must be a positive integer, got {value}"))?;
    if timeout_ms == 0 {
        return Err("--timeout-ms must be greater than 0".to_string());
    }
    Ok(timeout_ms)
}

fn parse_safe_retries(value: &str) -> Result<u8, String> {
    value
        .parse::<u8>()
        .map_err(|_| format!("--safe-retries must fit in 0..=255, got {value}"))
}

fn parse_admin_dir(value: &str, name: &str) -> Result<PathBuf, String> {
    let path = Path::new(value);
    if value.is_empty() || !path.is_absolute() {
        return Err(format!("{name} must be an absolute server-side path"));
    }
    Ok(path.to_path_buf())
}

fn schema() -> TableSchema {
    TableSchema {
        name: "docs".to_string(),
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

fn record(id: &str, tenant: &str, body: &str, embedding: [f32; 3]) -> RecordInput {
    RecordInput {
        table: "docs".to_string(),
        id: id.to_string(),
        tenant_id: tenant.to_string(),
        fields: json!({
            "id": id,
            "tenant": tenant,
            "status": "published",
            "body": body,
            "embedding": embedding,
        })
        .as_object()
        .expect("object fields")
        .clone(),
    }
}

fn query(explain: bool) -> HybridQuery {
    HybridQuery {
        table: "docs".to_string(),
        tenant_id: "tenant-a".to_string(),
        text: Some("rust api".to_string()),
        vector: Some(vec![1.0, 0.0, 0.0]),
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 5,
        freshness: FreshnessMode::Strict,
        explain,
    }
}
