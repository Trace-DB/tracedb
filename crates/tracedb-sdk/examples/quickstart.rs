use serde_json::{json, Value};
use std::env;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tracedb_query::{
    FreshnessMode, HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput,
    RecordPatchRequest, RecordScanRequest, TableSchema, VectorColumnSchema,
};
use tracedb_sdk::{
    ErrorResponse, RestoreRequest, SnapshotRequest, TraceDbClient, TraceDbClientConfig,
    TraceDbClientError, TraceDbRequestOptions,
};

fn main() {
    if let Err(error) = run() {
        println!("{}", error.to_pretty_json());
        eprintln!("tracedb-sdk quickstart: {}", error.message);
        std::process::exit(1);
    }
}

fn run() -> Result<(), QuickstartFailure> {
    let args = QuickstartArgs::from_env().map_err(|message| {
        QuickstartFailure::configuration(message, QuickstartFailureContext::from_env_and_cli())
    })?;
    if args.help {
        println!("{}", QuickstartArgs::usage());
        return Ok(());
    }
    validate_quickstart_url(&args.url)
        .map_err(|message| QuickstartFailure::configuration(message, (&args).into()))?;
    run_quickstart(&args)
        .map_err(|error| QuickstartFailure::execution(error.to_string(), (&args).into()))
}

fn run_quickstart(args: &QuickstartArgs) -> Result<(), Box<dyn Error>> {
    let mut config = TraceDbClientConfig::managed(args.url.clone(), args.token.clone());
    if let Some(database_id) = args.database_id.as_ref() {
        config = config.with_database(database_id.clone());
    }
    if let Some(branch_id) = args.branch_id.as_ref() {
        config = config.with_branch(branch_id.clone());
    }
    if let Some(timeout_ms) = args.timeout_ms {
        config = config.with_timeout(Duration::from_millis(timeout_ms));
    }
    if let Some(safe_retries) = args.safe_retries {
        config = config.with_safe_retries(safe_retries);
    }
    if let Some(idempotency_retries) = args.idempotency_retries {
        config = config.with_idempotency_retries(idempotency_retries);
    }
    let client = TraceDbClient::new(config);
    let idempotency_keys_enabled = args.idempotency_retries.unwrap_or(0) > 0;
    let idempotency_run_id = if idempotency_keys_enabled {
        Some(quickstart_run_suffix()?)
    } else {
        None
    };

    let ready = client.ready_typed()?;
    let health = client.health_typed()?;
    let databases = client.list_databases_typed()?;
    let branches = client.list_branches_typed()?;
    let metrics = client.public_safe_metrics_typed()?;
    let jobs = client.list_admin_jobs_typed()?;
    let schema_request = schema();
    let schema_options = idempotency_options(idempotency_run_id.as_deref(), "schema-apply");
    let schema = match schema_options.as_ref() {
        Some(options) => client.apply_schema_typed_with_options(&schema_request, options)?,
        None => client.apply_schema_typed(&schema_request)?,
    };
    let put_record = record("put", "tenant-a", "single record put path", [0.6, 0.4, 0.0]);
    let put_options = idempotency_options(idempotency_run_id.as_deref(), "put-single");
    let put = match put_options.as_ref() {
        Some(options) => client.put_typed_with_options(&put_record, options)?,
        None => client.put_typed(&put_record)?,
    };
    let docs = client.table("docs").tenant("tenant-a");
    let batch_rows = vec![
        json!({
            "id": "intro",
            "body": "rust database api quickstart",
            "embedding": [1.0, 0.0, 0.0],
        })
        .as_object()
        .expect("row literal is an object")
        .clone(),
        json!({
            "id": "ops",
            "body": "snapshot restore flow",
            "embedding": [0.0, 1.0, 0.0],
        })
        .as_object()
        .expect("row literal is an object")
        .clone(),
    ];
    let ingest_options = idempotency_options(idempotency_run_id.as_deref(), "put-batch");
    let ingest = match ingest_options.as_ref() {
        Some(options) => docs.insert_rows_with_options(batch_rows, options)?,
        None => docs.insert_rows(batch_rows)?,
    };
    let patch_request = patch_request();
    let patch_options = idempotency_options(idempotency_run_id.as_deref(), "patch-intro");
    let patch = match patch_options.as_ref() {
        Some(options) => client.patch_typed_with_options(&patch_request, options)?,
        None => client.patch_typed(&patch_request)?,
    };
    let patched = client.get_record_typed(&RecordGetRequest::new("docs", "tenant-a", "intro"))?;
    let scan = client.scan_typed(&RecordScanRequest::new("docs", "tenant-a").limit(10))?;
    let query_response = client.query_typed(&query(false))?;
    let traceql_response = client.traceql_typed(traceql_query(false))?;
    let traceql_explain = client.traceql_typed(traceql_query(true))?;
    let explain = client.explain_typed(&query(false))?;
    let delete_request = RecordDeleteRequest::new("docs", "tenant-a", "ops");
    let delete_options = idempotency_options(idempotency_run_id.as_deref(), "delete-ops");
    let delete = match delete_options.as_ref() {
        Some(options) => client.delete_typed_with_options(&delete_request, options)?,
        None => client.delete_typed(&delete_request)?,
    };
    let deleted = client.get_record_typed(&RecordGetRequest::new("docs", "tenant-a", "ops"))?;
    let error_envelope = error_envelope_smoke(&client)?;
    let admin = args
        .admin_dir
        .as_ref()
        .map(|admin_dir| run_admin_smoke(&client, admin_dir, idempotency_run_id.as_deref()))
        .transpose()?;
    let admin_summary = match admin.as_ref() {
        Some(admin) => json!({
            "requested": true,
            "compact": admin.compacted,
            "snapshot": admin.snapshot,
            "restore": admin.restored,
        }),
        None => json!({
            "requested": false,
            "compact": "skipped",
            "snapshot": "skipped",
            "restore": "skipped",
        }),
    };
    let steps = json!({
        "ready": true,
        "health": health.ok,
        "catalog": true,
        "metrics": metrics.latest_epoch.is_some(),
        "schema_apply": true,
        "put": true,
        "batch_ingest": true,
        "row_batch_ingest": true,
        "patch": true,
        "scan": true,
        "query": true,
        "traceql_string_execution": true,
        "explain": true,
        "delete": true,
        "error_envelope": true,
        "jobs": true,
        "compact": admin.as_ref().map(|admin| admin.compacted).unwrap_or(false),
        "snapshot": admin.as_ref().map(|admin| admin.snapshot).unwrap_or(false),
        "restore": admin.as_ref().map(|admin| admin.restored).unwrap_or(false),
    });

    let summary = json!({
        "ok": true,
        "mode": "rust-sdk-quickstart",
        "server_url": args.url.as_str(),
        "database_id": args.database_id.as_deref(),
        "branch_id": args.branch_id.as_deref(),
        "table": "docs",
        "tenant_id": "tenant-a",
        "admin": admin_summary,
        "server_ready": ready.ready,
        "health_ok": health.ok,
        "database_count": databases.databases.len(),
        "branch_count": branches.branches.len(),
        "metrics_latest_epoch": metrics.latest_epoch,
        "admin_job_count": jobs.jobs.len(),
        "schema_epoch": schema.epoch,
        "put_epoch": put.epoch,
        "records_put": 1,
        "records_batched": ingest.record_count,
        "records_row_batched": ingest.record_count,
        "records_inserted": ingest.record_count + 1,
        "patched": patch.epoch > schema.epoch,
        "patched_status": patched
            .record
            .as_ref()
            .and_then(|record| record.fields.get("status"))
            .and_then(serde_json::Value::as_str),
        "records_scanned": scan.returned_count,
        "query_result_count": query_response.results.len(),
        "traceql_result_count": traceql_response.results.len(),
        "traceql_explain": traceql_explain.explain.is_some(),
        "explain_returned_count": explain.returned_count,
        "deleted": delete.deleted,
        "deleted_hidden": deleted.record.is_none(),
        "error_envelope": error_envelope,
        "snapshot_target": admin.as_ref().map(|admin| admin.snapshot_target.as_str()),
        "restore_target": admin.as_ref().map(|admin| admin.restore_target.as_str()),
        "idempotency_retries": args.idempotency_retries.unwrap_or(0),
        "idempotency_keys": idempotency_keys_enabled,
        "sql_module": "not_implemented",
        "steps": steps,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);

    Ok(())
}

struct QuickstartFailure {
    kind: &'static str,
    phase: &'static str,
    message: String,
    context: QuickstartFailureContext,
}

impl QuickstartFailure {
    fn configuration(message: String, context: QuickstartFailureContext) -> Self {
        Self {
            kind: "configuration",
            phase: "config",
            message,
            context,
        }
    }

    fn execution(message: String, context: QuickstartFailureContext) -> Self {
        Self {
            kind: "execution",
            phase: "execution",
            message,
            context,
        }
    }

    fn summary(&self) -> serde_json::Value {
        let admin_state = if self.context.admin_requested {
            "not_started"
        } else {
            "skipped"
        };
        let admin = json!({
            "requested": self.context.admin_requested,
            "compact": admin_state,
            "snapshot": admin_state,
            "restore": admin_state,
        });
        let steps = json!({
            "ready": false,
            "health": false,
            "catalog": false,
            "metrics": false,
            "schema_apply": false,
            "put": false,
            "batch_ingest": false,
            "row_batch_ingest": false,
            "patch": false,
            "scan": false,
            "query": false,
            "traceql_string_execution": false,
            "explain": false,
            "delete": false,
            "error_envelope": false,
            "jobs": false,
            "compact": false,
            "snapshot": false,
            "restore": false,
        });
        json!({
            "ok": false,
            "mode": "rust-sdk-quickstart",
            "server_url": self.context.url.as_str(),
            "database_id": self.context.database_id.as_deref(),
            "branch_id": self.context.branch_id.as_deref(),
            "table": "docs",
            "tenant_id": "tenant-a",
            "admin": admin,
            "idempotency_retries": self.context.idempotency_retries.unwrap_or(0),
            "idempotency_keys": false,
            "phase": self.phase,
            "error": {
                "kind": self.kind,
                "message": self.message.as_str(),
            },
            "sql_module": "not_implemented",
            "steps": steps,
        })
    }

    fn to_pretty_json(&self) -> String {
        serde_json::to_string_pretty(&self.summary()).unwrap_or_else(|_| {
            format!(
                "{{\"ok\":false,\"mode\":\"rust-sdk-quickstart\",\"error\":{{\"kind\":\"{}\",\"message\":\"{}\"}},\"sql_module\":\"not_implemented\"}}",
                self.kind,
                self.message.replace('"', "\\\"")
            )
        })
    }
}

struct QuickstartFailureContext {
    url: String,
    database_id: Option<String>,
    branch_id: Option<String>,
    admin_requested: bool,
    idempotency_retries: Option<u8>,
}

impl QuickstartFailureContext {
    fn from_env_and_cli() -> Self {
        let mut context = Self {
            url: env::var("TRACEDB_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string()),
            database_id: env::var("TRACEDB_DATABASE_ID").ok(),
            branch_id: env::var("TRACEDB_BRANCH_ID").ok(),
            admin_requested: env::var("TRACEDB_ADMIN_DIR")
                .ok()
                .is_some_and(|value| !value.is_empty()),
            idempotency_retries: env::var("TRACEDB_IDEMPOTENCY_RETRIES")
                .ok()
                .and_then(|value| value.parse::<u8>().ok()),
        };
        let mut cli = env::args().skip(1);
        while let Some(arg) = cli.next() {
            match arg.as_str() {
                "--url" => {
                    if let Some(value) = cli.next() {
                        context.url = value;
                    }
                }
                "--database-id" => context.database_id = cli.next(),
                "--branch-id" => context.branch_id = cli.next(),
                "--admin-dir" => {
                    if let Some(value) = cli.next() {
                        context.admin_requested = !value.is_empty();
                    }
                }
                "--idempotency-retries" => {
                    context.idempotency_retries =
                        cli.next().and_then(|value| value.parse::<u8>().ok());
                }
                "--token" | "--timeout-ms" | "--safe-retries" => {
                    let _ = cli.next();
                }
                _ => {}
            }
        }
        context
    }
}

impl From<&QuickstartArgs> for QuickstartFailureContext {
    fn from(args: &QuickstartArgs) -> Self {
        Self {
            url: args.url.clone(),
            database_id: args.database_id.clone(),
            branch_id: args.branch_id.clone(),
            admin_requested: args.admin_dir.is_some(),
            idempotency_retries: args.idempotency_retries,
        }
    }
}

#[derive(Debug)]
struct QuickstartArgs {
    url: String,
    token: String,
    database_id: Option<String>,
    branch_id: Option<String>,
    timeout_ms: Option<u64>,
    safe_retries: Option<u8>,
    idempotency_retries: Option<u8>,
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
            idempotency_retries: env::var("TRACEDB_IDEMPOTENCY_RETRIES")
                .ok()
                .map(|value| parse_idempotency_retries(&value))
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
                "--idempotency-retries" => {
                    args.idempotency_retries = Some(parse_idempotency_retries(&next_value(
                        &mut cli,
                        "--idempotency-retries",
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
        "Usage: cargo run -p tracedb-sdk --example quickstart -- --url http://127.0.0.1:8080 [--token TOKEN] [--database-id DB] [--branch-id BRANCH] [--timeout-ms MS] [--safe-retries N] [--idempotency-retries N] [--admin-dir SERVER_SIDE_DIR]"
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
    idempotency_run_id: Option<&str>,
) -> Result<AdminSmokeSummary, Box<dyn Error>> {
    let suffix = quickstart_run_suffix()?;
    let snapshot_target = admin_dir.join(format!("quickstart-snapshot-{suffix}"));
    let restore_target = admin_dir.join(format!("quickstart-restore-{suffix}"));
    let snapshot_target = snapshot_target.to_string_lossy().to_string();
    let restore_target = restore_target.to_string_lossy().to_string();

    let compact_options = idempotency_options(idempotency_run_id, "compact");
    let compact = match compact_options.as_ref() {
        Some(options) => client.compact_typed_with_options(options)?,
        None => client.compact_typed()?,
    };
    let snapshot_request = SnapshotRequest::new(snapshot_target.clone());
    let snapshot_options = idempotency_options(idempotency_run_id, "snapshot");
    let snapshot = match snapshot_options.as_ref() {
        Some(options) => client.snapshot_typed_with_options(&snapshot_request, options)?,
        None => client.snapshot_typed(&snapshot_request)?,
    };
    let restore_request = RestoreRequest::new(snapshot_target.clone(), restore_target.clone());
    let restore_options = idempotency_options(idempotency_run_id, "restore");
    let restore = match restore_options.as_ref() {
        Some(options) => client.restore_typed_with_options(&restore_request, options)?,
        None => client.restore_typed(&restore_request)?,
    };

    Ok(AdminSmokeSummary {
        compacted: compact.compacted,
        snapshot: snapshot.snapshot,
        restored: restore.restored,
        snapshot_target: snapshot.target,
        restore_target: restore.target,
    })
}

fn idempotency_options(run_id: Option<&str>, step: &str) -> Option<TraceDbRequestOptions> {
    run_id.map(|run_id| {
        TraceDbRequestOptions::new().with_idempotency_key(format!("quickstart-{run_id}-{step}"))
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

fn validate_quickstart_url(url: &str) -> Result<(), String> {
    let without_scheme = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("invalid TraceDB URL {url}; expected http://host[:port][/path]"))?;
    let authority = without_scheme
        .split_once('/')
        .map(|(authority, _)| authority)
        .unwrap_or(without_scheme);
    if authority.is_empty() {
        return Err(format!(
            "invalid TraceDB URL {url}; expected http://host[:port][/path]"
        ));
    }
    let (host, port) = authority
        .rsplit_once(':')
        .map(|(host, port)| (host, Some(port)))
        .unwrap_or((authority, None));
    if host.is_empty() {
        return Err(format!(
            "invalid TraceDB URL {url}; expected http://host[:port][/path]"
        ));
    }
    if let Some(port) = port {
        port.parse::<u16>()
            .map_err(|_| format!("invalid TraceDB URL {url}; port must fit in 0..=65535"))?;
    }
    Ok(())
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

fn parse_idempotency_retries(value: &str) -> Result<u8, String> {
    value
        .parse::<u8>()
        .map_err(|_| format!("--idempotency-retries must fit in 0..=255, got {value}"))
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

fn error_envelope_smoke(client: &TraceDbClient) -> Result<Value, Box<dyn Error>> {
    match client.request_json("POST", "/v1/records/get", Some(&json!({}))) {
        Ok(value) => Err(quickstart_error(format!(
            "expected /v1/records/get error envelope, got success response {value}"
        ))),
        Err(TraceDbClientError::HttpStatus {
            method,
            path,
            status,
            body,
        }) if status == 400 => {
            let envelope: ErrorResponse = serde_json::from_str(&body)?;
            if envelope.error.trim().is_empty() {
                return Err(quickstart_error("error envelope had an empty error field"));
            }
            Ok(json!({
                "status": status,
                "method": method,
                "path": path,
                "error": envelope.error,
                "code": envelope.code,
            }))
        }
        Err(error) => Err(Box::new(error)),
    }
}

fn quickstart_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        message.into(),
    ))
}

fn patch_request() -> RecordPatchRequest {
    RecordPatchRequest::new(
        "docs",
        "tenant-a",
        "intro",
        json!({ "status": "reviewed" })
            .as_object()
            .expect("object fields")
            .clone(),
    )
}

fn query(explain: bool) -> HybridQuery {
    HybridQuery {
        table: "docs".to_string(),
        tenant_id: "tenant-a".to_string(),
        cursor: None,
        text_field: None,
        text: Some("rust api".to_string()),
        vector_field: None,
        vector: Some(vec![1.0, 0.0, 0.0]),
        scalar_eq: Default::default(),
        graph_seed: None,
        temporal_as_of: None,
        top_k: 5,
        freshness: FreshnessMode::Strict,
        explain,
    }
}

fn traceql_query(explain: bool) -> String {
    let mut lines = vec![
        "FROM docs",
        "TENANT tenant-a",
        "WHERE status = \"reviewed\"",
        "MATCH body \"rust\"",
        "NEAR embedding [1.0, 0.0, 0.0]",
        "FRESHNESS ALLOW_DIRTY",
        "LIMIT 5",
    ];
    if explain {
        lines.push("EXPLAIN");
    }
    lines.join("\n")
}
