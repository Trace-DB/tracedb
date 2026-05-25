#![forbid(unsafe_code)]

use axum::body::{to_bytes, Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, Uri};
use axum::routing::any;
use axum::Router;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracedb_policy::ActorContext;
use tracedb_query::{
    graphql_query_from_str, graphql_schema_sdl_from_tables, traceql_query_from_str, HybridQuery,
    RecordDeleteRequest, RecordGetRequest, RecordInput, RecordPatchRequest, RecordPutBatchRequest,
    RecordPutRequest, RecordScanRequest, TableSchema, TraceDb,
};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

type IdempotencyCache = Arc<Mutex<IdempotencyCacheState>>;

#[derive(Clone)]
struct EngineAppState {
    db: Arc<Mutex<TraceDb>>,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EngineServerConfig {
    pub internal_token: Option<String>,
}

impl EngineServerConfig {
    pub fn from_env() -> Self {
        Self {
            internal_token: std::env::var("TRACEDB_ENGINE_INTERNAL_TOKEN")
                .ok()
                .or_else(|| std::env::var("TRACEDB_ENGINE_TOKEN").ok())
                .filter(|token| !token.trim().is_empty()),
        }
    }

    pub fn with_internal_token(mut self, token: impl Into<String>) -> Self {
        self.internal_token = Some(token.into());
        self
    }

    fn authorizes_private_request(&self, request: &str) -> bool {
        let Some(required) = self.internal_token.as_deref() else {
            return true;
        };
        header_value(request, "x-tracedb-engine-token") == Some(required)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IdempotencyCacheKey {
    method: String,
    path: String,
    key: String,
}

impl IdempotencyCacheKey {
    fn new(method: &str, path: &str, key: &str) -> Self {
        Self {
            method: method.to_string(),
            path: path.to_string(),
            key: key.to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct IdempotencyEntry {
    body: String,
    response: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DurableIdempotencyEntry {
    method: String,
    path: String,
    key: String,
    body: String,
    response: String,
}

#[derive(Debug, Deserialize)]
struct TraceQlQueryRequest {
    query: String,
}

#[derive(Debug, Deserialize)]
struct GraphQlQueryRequest {
    query: String,
}

#[derive(Debug)]
struct IdempotencyCacheState {
    path: PathBuf,
    entries: HashMap<IdempotencyCacheKey, IdempotencyEntry>,
}

impl IdempotencyCacheState {
    fn load(path: PathBuf) -> std::io::Result<Self> {
        if !path.exists() {
            return Ok(Self {
                path,
                entries: HashMap::new(),
            });
        }
        let entries =
            serde_json::from_str::<Vec<DurableIdempotencyEntry>>(&fs::read_to_string(&path)?)
                .map_err(to_io_error)?
                .into_iter()
                .map(|entry| {
                    (
                        IdempotencyCacheKey {
                            method: entry.method,
                            path: entry.path,
                            key: entry.key,
                        },
                        IdempotencyEntry {
                            body: entry.body,
                            response: entry.response,
                        },
                    )
                })
                .collect();
        Ok(Self { path, entries })
    }

    fn get(&self, key: &IdempotencyCacheKey) -> Option<IdempotencyEntry> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: IdempotencyCacheKey, entry: IdempotencyEntry) -> std::io::Result<()> {
        self.entries.insert(key.clone(), entry);
        if let Err(error) = self.persist() {
            self.entries.remove(&key);
            return Err(error);
        }
        Ok(())
    }

    fn persist(&self) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let entries = self
            .entries
            .iter()
            .map(|(key, entry)| DurableIdempotencyEntry {
                method: key.method.clone(),
                path: key.path.clone(),
                key: key.key.clone(),
                body: entry.body.clone(),
                response: entry.response.clone(),
            })
            .collect::<Vec<_>>();
        let tmp = self.path.with_extension("json.tmp");
        fs::write(
            &tmp,
            serde_json::to_vec_pretty(&entries).map_err(to_io_error)?,
        )?;
        fs::rename(tmp, &self.path)
    }
}

pub fn serve(db_path: impl AsRef<Path>, bind: &str) -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(serve_async(
        db_path.as_ref().to_path_buf(),
        bind.to_string(),
        EngineServerConfig::from_env(),
    ))
}

pub async fn serve_async(
    db_path: impl AsRef<Path>,
    bind: impl AsRef<str>,
    config: EngineServerConfig,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind.as_ref()).await?;
    serve_tokio_listener(db_path, listener, config).await
}

pub async fn serve_tokio_listener(
    db_path: impl AsRef<Path>,
    listener: tokio::net::TcpListener,
    config: EngineServerConfig,
) -> std::io::Result<()> {
    let (db, idempotency_cache) = open_server_state(db_path)?;
    let state = EngineAppState {
        db,
        idempotency_cache,
        config,
    };
    let app = Router::new()
        .fallback(any(handle_axum_request))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(state);
    axum::serve(listener, app).await
}

pub fn serve_listener(db_path: impl AsRef<Path>, listener: TcpListener) -> std::io::Result<()> {
    serve_listener_with_config(db_path, listener, EngineServerConfig::from_env())
}

pub fn serve_listener_with_config(
    db_path: impl AsRef<Path>,
    listener: TcpListener,
    config: EngineServerConfig,
) -> std::io::Result<()> {
    let (db, idempotency_cache) = open_server_state(db_path)?;
    for stream in listener.incoming() {
        spawn_handler_with_config(
            stream?,
            Arc::clone(&db),
            Arc::clone(&idempotency_cache),
            config.clone(),
        );
    }
    Ok(())
}

pub fn serve_with_shutdown(
    db_path: impl AsRef<Path>,
    bind: &str,
    should_shutdown: impl Fn() -> bool,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    serve_listener_with_shutdown(db_path, listener, should_shutdown)
}

pub fn serve_listener_with_shutdown(
    db_path: impl AsRef<Path>,
    listener: TcpListener,
    should_shutdown: impl Fn() -> bool,
) -> std::io::Result<()> {
    serve_listener_with_shutdown_and_config(
        db_path,
        listener,
        should_shutdown,
        EngineServerConfig::from_env(),
    )
}

pub fn serve_listener_with_shutdown_and_config(
    db_path: impl AsRef<Path>,
    listener: TcpListener,
    should_shutdown: impl Fn() -> bool,
    config: EngineServerConfig,
) -> std::io::Result<()> {
    let (db, idempotency_cache) = open_server_state(db_path)?;
    listener.set_nonblocking(true)?;
    while !should_shutdown() {
        match listener.accept() {
            Ok((stream, _)) => {
                spawn_handler_with_config(
                    stream,
                    Arc::clone(&db),
                    Arc::clone(&idempotency_cache),
                    config.clone(),
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn open_server_state(
    db_path: impl AsRef<Path>,
) -> std::io::Result<(Arc<Mutex<TraceDb>>, IdempotencyCache)> {
    let db_path = db_path.as_ref().to_path_buf();
    let db = TraceDb::open(&db_path).map_err(to_io_error)?;
    let db = Arc::new(Mutex::new(db));
    let idempotency_cache = Arc::new(Mutex::new(IdempotencyCacheState::load(
        db_path.join("http-idempotency-cache.json"),
    )?));
    Ok((db, idempotency_cache))
}

fn spawn_handler_with_config(
    stream: TcpStream,
    db: Arc<Mutex<TraceDb>>,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) {
    thread::spawn(move || {
        let _ = handle(stream, db, idempotency_cache, config);
    });
}

fn handle(
    mut stream: TcpStream,
    db: Arc<Mutex<TraceDb>>,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) -> std::io::Result<()> {
    let response = match handle_inner(&mut stream, db, idempotency_cache, config) {
        Ok(response) => response,
        Err(error) => bad_request(error.to_string()),
    };
    stream.write_all(response.as_bytes())?;
    stream.flush()
}

fn route_path(target: &str) -> &str {
    target
        .split_once('?')
        .map(|(path, _)| path)
        .unwrap_or(target)
}

fn handle_inner(
    stream: &mut TcpStream,
    db: Arc<Mutex<TraceDb>>,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) -> std::io::Result<String> {
    let request = read_request(stream)?;
    handle_request_text(&request, db, idempotency_cache, config)
}

async fn handle_axum_request(
    State(state): State<EngineAppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response<Body>, Infallible> {
    let body = match to_bytes(body, 16 * 1024 * 1024).await {
        Ok(body) => body,
        Err(error) => {
            return Ok(response_from_http_text(bad_request(format!(
                "failed to read request body: {error}"
            ))));
        }
    };
    let request = request_text_from_parts(&method, &uri, &headers, &body);
    let response =
        match handle_request_text(&request, state.db, state.idempotency_cache, state.config) {
            Ok(response) => response,
            Err(error) => bad_request(error.to_string()),
        };
    Ok(response_from_http_text(response))
}

fn handle_request_text(
    request: &str,
    db: Arc<Mutex<TraceDb>>,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) -> std::io::Result<String> {
    let request_start = Instant::now();
    let read_ms = elapsed_ms(request_start);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let path = route_path(target);
    let request_id = header_value(&request, "x-request-id")
        .map(str::to_string)
        .unwrap_or_else(next_request_id);
    log_request("tracedb-engine", &request_id, method, path);
    if !is_public_engine_route(method, path) && !config.authorizes_private_request(request) {
        return Ok(unauthorized("missing or invalid private engine token"));
    }
    let body = request
        .split("\r\n\r\n")
        .nth(1)
        .or_else(|| request.split("\n\n").nth(1))
        .unwrap_or_default();
    let actor = actor_context_from_request(request, body, &request_id);
    let idempotency_cache_key = header_value(&request, "idempotency-key")
        .filter(|key| !key.is_empty())
        .filter(|_| supports_http_idempotency(method, path))
        .map(|key| IdempotencyCacheKey::new(method, path, key));
    let mut idempotency_cache_guard = idempotency_cache_key
        .as_ref()
        .map(|_| idempotency_cache.lock().unwrap());
    if let (Some(cache_key), Some(cache)) = (
        idempotency_cache_key.as_ref(),
        idempotency_cache_guard.as_ref(),
    ) {
        if let Some(entry) = cache.get(cache_key) {
            if entry.body == body {
                return Ok(entry.response);
            }
            return Ok(conflict(
                "idempotency key reused with different request body",
            ));
        }
    }

    let response = match (method, path) {
        ("GET", "/health") | ("GET", "/internal/health") => {
            let db = db.lock().unwrap();
            let manifest = db.inspect_manifest().map_err(to_io_error)?;
            ok(json!({
                "ok": true,
                "data_dir_available": true,
                "latest_epoch": manifest.latest_epoch.get(),
                "durable_epoch": manifest.durable_epoch.get(),
                "branch_id": manifest.branch_id,
                "active_wal": manifest.active_wal,
                "wal_state": "open",
                "catalog_connection": "configured-or-local",
                "queue_connection": "configured-or-local",
            }))
        }
        ("GET", "/v1/health") => ok(json!({ "ok": true, "service": "tracedb-engine" })),
        ("GET", "/ready") | ("GET", "/v1/ready") => {
            let db = db.lock().unwrap();
            let manifest = db.inspect_manifest().map_err(to_io_error)?;
            ok(json!({
                "ready": true,
                "service": "tracedb-engine",
                "latest_epoch": manifest.latest_epoch.get(),
                "durable_epoch": manifest.durable_epoch.get(),
                "recovery_state": if db.last_recovery_torn_tail().is_some() { "torn_tail_ignored" } else { "clean" },
            }))
        }
        ("GET", "/v1/databases") => ok(json!({
            "mode": "local",
            "databases": [{
                "database_id": "local",
                "endpoint": "local-daemon",
            }]
        })),
        ("GET", "/v1/branches") => {
            let db = db.lock().unwrap();
            let manifest = db.inspect_manifest().map_err(to_io_error)?;
            ok(json!({
                "branches": [{
                    "branch_id": manifest.branch_id,
                    "state": "ACTIVE",
                    "latest_epoch": manifest.latest_epoch.get(),
                }]
            }))
        }
        ("GET", "/metrics") | ("GET", "/v1/metrics/public-safe") => {
            let db = db.lock().unwrap();
            let manifest = db.inspect_manifest().map_err(to_io_error)?;
            ok(json!({
                "service": "tracedb-engine",
                "latest_epoch": manifest.latest_epoch.get(),
                "durable_epoch": manifest.durable_epoch.get(),
                "segment_count": manifest.segments.len(),
                "index_count": manifest.indexes.len(),
                "module_count": manifest.modules.len(),
                "schema_count": manifest.schemas.len(),
                "recovery_state": if db.last_recovery_torn_tail().is_some() { "torn_tail_ignored" } else { "clean" },
            }))
        }
        ("POST", "/v1/schema/apply") => {
            let schema: TableSchema = serde_json::from_str(body).map_err(to_io_error)?;
            let epoch = db
                .lock()
                .unwrap()
                .apply_schema(schema)
                .map_err(to_io_error)?;
            ok(json!({ "epoch": epoch.get() }))
        }
        ("POST", "/v1/insert") => {
            let input: RecordInput = serde_json::from_str(body).map_err(to_io_error)?;
            let epoch = db.lock().unwrap().insert(input).map_err(to_io_error)?;
            ok(json!({ "epoch": epoch.get() }))
        }
        ("POST", "/v1/records/put") => {
            let input = parse_record_put_body(body)?;
            let epoch = db
                .lock()
                .unwrap()
                .put(RecordPutRequest::new(input))
                .map_err(to_io_error)?;
            ok(json!({ "epoch": epoch.get() }))
        }
        ("POST", "/v1/records/put-batch") => {
            let request: RecordPutBatchRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let record_count = request.records.len();
            if request.include_write_timing {
                let (epoch, timing) = db
                    .lock()
                    .unwrap()
                    .put_batch_with_write_timing(request)
                    .map_err(to_io_error)?;
                ok(json!({
                    "epoch": epoch.get(),
                    "record_count": record_count,
                    "write_timing": timing,
                }))
            } else {
                let epoch = db.lock().unwrap().put_batch(request).map_err(to_io_error)?;
                ok(json!({ "epoch": epoch.get(), "record_count": record_count }))
            }
        }
        ("POST", "/v1/records/patch") => {
            let request: RecordPatchRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let epoch = db.lock().unwrap().patch(request).map_err(to_io_error)?;
            ok(json!({ "epoch": epoch.get() }))
        }
        ("POST", "/v1/records/delete") => {
            let request: RecordDeleteRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let epoch = db.lock().unwrap().delete(request).map_err(to_io_error)?;
            ok(json!({ "deleted": true, "epoch": epoch.get() }))
        }
        ("POST", "/v1/records/get") => {
            let request: RecordGetRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let record = db
                .lock()
                .unwrap()
                .get_as(&actor, request)
                .map_err(to_io_error)?;
            ok(json!({ "record": record }))
        }
        ("POST", "/v1/records/scan") => {
            let request: RecordScanRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let output = db
                .lock()
                .unwrap()
                .scan_as(&actor, request)
                .map_err(to_io_error)?;
            ok(serde_json::to_value(output).map_err(to_io_error)?)
        }
        ("POST", "/v1/query") => {
            let parse_start = Instant::now();
            let query: HybridQuery = serde_json::from_str(body).map_err(to_io_error)?;
            let parse_ms = elapsed_ms(parse_start);
            query_response(&db, actor, query, request_start, read_ms, parse_ms)?
        }
        ("POST", "/v1/traceql") => {
            let parse_start = Instant::now();
            let request: TraceQlQueryRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let query = traceql_query_from_str(&request.query).map_err(to_io_error)?;
            let parse_ms = elapsed_ms(parse_start);
            query_response(&db, actor, query, request_start, read_ms, parse_ms)?
        }
        ("POST", "/v1/graphql") => {
            let parse_start = Instant::now();
            let request: GraphQlQueryRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let query = graphql_query_from_str(&request.query).map_err(to_io_error)?;
            let parse_ms = elapsed_ms(parse_start);
            query_response(&db, actor, query, request_start, read_ms, parse_ms)?
        }
        ("GET", "/v1/graphql/schema") => {
            let db = db.lock().unwrap();
            let manifest = db.inspect_manifest().map_err(to_io_error)?;
            let schema = graphql_schema_sdl_from_tables(&manifest.schemas).map_err(to_io_error)?;
            let tables = manifest
                .schemas
                .iter()
                .map(|schema| schema.name.clone())
                .collect::<Vec<_>>();
            ok(json!({
                "adapter": "bounded_graphql_query_adapter",
                "schema": schema,
                "tables": tables,
                "execution": "POST /v1/graphql returns TraceDB QueryResponse, not a GraphQL data envelope",
            }))
        }
        ("POST", "/v1/explain") => {
            let parse_start = Instant::now();
            let mut query: HybridQuery = serde_json::from_str(body).map_err(to_io_error)?;
            query.explain = true;
            let parse_ms = elapsed_ms(parse_start);
            let mut actor = actor.clone();
            if actor.tenant_id == "local" {
                actor.tenant_id = query.tenant_id.clone();
            }
            let lock_start = Instant::now();
            let guard = db.lock().unwrap();
            let lock_wait_ms = elapsed_ms(lock_start);
            let engine_start = Instant::now();
            let timed_output = guard
                .query_with_timing_as(&actor, query)
                .map_err(to_io_error)?;
            let engine_ms = elapsed_ms(engine_start);
            let query_timing = timed_output.timing;
            drop(guard);
            let response_shape_start = Instant::now();
            let value = serde_json::to_value(timed_output.output.explain).map_err(to_io_error)?;
            let response_shape_ms = elapsed_ms(response_shape_start);
            ok_timed(
                value,
                request_start,
                response_shape_ms,
                &[
                    ("read", read_ms),
                    ("parse", parse_ms),
                    ("lock_wait", lock_wait_ms),
                    ("engine", engine_ms),
                    ("engine_core", query_timing.engine_core_ms),
                    ("explain_build", query_timing.explain_build_ms),
                    ("materialize", query_timing.materialize_ms),
                ],
            )
        }
        ("POST", "/v1/admin/compact") => {
            db.lock().unwrap().compact().map_err(to_io_error)?;
            ok(json!({ "compacted": true }))
        }
        ("POST", "/v1/admin/snapshot") => {
            let value: Value = serde_json::from_str(body).map_err(to_io_error)?;
            let target = value.get("target").and_then(Value::as_str).ok_or_else(|| {
                to_io_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "snapshot target is required",
                ))
            })?;
            db.lock()
                .unwrap()
                .create_snapshot(target)
                .map_err(to_io_error)?;
            ok(json!({ "snapshot": true, "target": target }))
        }
        ("POST", "/v1/admin/restore") => {
            let value: Value = serde_json::from_str(body).map_err(to_io_error)?;
            let source = value.get("source").and_then(Value::as_str).ok_or_else(|| {
                to_io_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "restore source is required",
                ))
            })?;
            let target = value.get("target").and_then(Value::as_str).ok_or_else(|| {
                to_io_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "restore target is required",
                ))
            })?;
            let restored = TraceDb::restore_snapshot(source, target).map_err(to_io_error)?;
            let mut response = json!({ "restored": true, "source": source, "target": target });
            if let Some(verify_record) = value.get("verify_record") {
                let request: RecordGetRequest =
                    serde_json::from_value(verify_record.clone()).map_err(to_io_error)?;
                let record = restored.get(request.clone()).map_err(to_io_error)?;
                response["verification"] = json!({
                    "status": if record.is_some() { "passed" } else { "failed" },
                    "record_visible": record.is_some(),
                    "request": request,
                    "record": record,
                });
            }
            ok(response)
        }
        ("GET", "/v1/admin/jobs") => ok(json!({
            "jobs": [
                { "queue": "tracedb.segment.compact", "state": "idle" },
                { "queue": "tracedb.snapshot.create", "state": "idle" },
                { "queue": "tracedb.feature.index", "state": "idle" }
            ]
        })),
        _ => not_found(),
    };
    if let (Some(cache_key), Some(cache)) =
        (idempotency_cache_key, idempotency_cache_guard.as_mut())
    {
        if response.starts_with("HTTP/1.1 200 OK") {
            if let Err(error) = cache.insert(
                cache_key,
                IdempotencyEntry {
                    body: body.to_string(),
                    response: response.clone(),
                },
            ) {
                eprintln!("tracedb-server: failed to persist idempotency response: {error}");
            }
        }
    }
    Ok(response)
}

#[cfg(test)]
fn handle_request_text_for_test(
    request: &str,
    db: Arc<Mutex<TraceDb>>,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) -> std::io::Result<String> {
    handle_request_text(request, db, idempotency_cache, config)
}

fn supports_http_idempotency(method: &str, path: &str) -> bool {
    matches!(
        (method, path),
        ("POST", "/v1/schema/apply")
            | ("POST", "/v1/insert")
            | ("POST", "/v1/records/put")
            | ("POST", "/v1/records/put-batch")
            | ("POST", "/v1/records/patch")
            | ("POST", "/v1/records/delete")
            | ("POST", "/v1/admin/compact")
            | ("POST", "/v1/admin/snapshot")
            | ("POST", "/v1/admin/restore")
    )
}

fn is_public_engine_route(method: &str, path: &str) -> bool {
    matches!((method, path), ("GET", "/health") | ("GET", "/v1/health"))
}

fn parse_record_put_body(body: &str) -> std::io::Result<RecordInput> {
    let value: Value = serde_json::from_str(body).map_err(to_io_error)?;
    if value.get("record").is_some() {
        let request: RecordPutRequest = serde_json::from_value(value).map_err(to_io_error)?;
        Ok(request.record)
    } else {
        serde_json::from_value(value).map_err(to_io_error)
    }
}

fn query_response(
    db: &Arc<Mutex<TraceDb>>,
    mut actor: ActorContext,
    query: HybridQuery,
    request_start: Instant,
    read_ms: f64,
    parse_ms: f64,
) -> std::io::Result<String> {
    let include_explain = query.explain;
    if actor.tenant_id == "local" {
        actor.tenant_id = query.tenant_id.clone();
    }
    let lock_start = Instant::now();
    let guard = db.lock().unwrap();
    let lock_wait_ms = elapsed_ms(lock_start);
    let engine_start = Instant::now();
    let timed_output = guard
        .query_with_timing_as(&actor, query)
        .map_err(to_io_error)?;
    let engine_ms = elapsed_ms(engine_start);
    let query_timing = timed_output.timing;
    drop(guard);
    let output = timed_output.output;
    let response_shape_start = Instant::now();
    let value = if include_explain {
        serde_json::to_value(output).map_err(to_io_error)?
    } else {
        let mut value = json!({ "results": output.results });
        if let Some(next_cursor) = output.next_cursor {
            value["next_cursor"] = json!(next_cursor);
        }
        value
    };
    let response_shape_ms = elapsed_ms(response_shape_start);
    Ok(ok_timed(
        value,
        request_start,
        response_shape_ms,
        &[
            ("read", read_ms),
            ("parse", parse_ms),
            ("lock_wait", lock_wait_ms),
            ("engine", engine_ms),
            ("engine_core", query_timing.engine_core_ms),
            ("explain_build", query_timing.explain_build_ms),
            ("materialize", query_timing.materialize_ms),
        ],
    ))
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let (header_end, delimiter_len);
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before headers",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some((end, len)) = find_header_end(&buffer) {
            header_end = end;
            delimiter_len = len;
            break;
        }
        if buffer.len() > 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request headers exceed 1MiB",
            ));
        }
    }
    let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let body_start = header_end + delimiter_len;
    if content_length > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "request body exceeds 16MiB",
        ));
    }
    let expected_len = body_start
        .checked_add(content_length)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "request too large"))?;
    while buffer.len() < expected_len {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before full body",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    Ok(String::from_utf8_lossy(&buffer[..expected_len]).to_string())
}

fn find_header_end(buffer: &[u8]) -> Option<(usize, usize)> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| (pos, 4))
        .or_else(|| {
            buffer
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|pos| (pos, 2))
        })
}

fn request_text_from_parts(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> String {
    let mut request = format!("{method} {uri} HTTP/1.1\r\n");
    for (name, value) in headers {
        if name.as_str().eq_ignore_ascii_case("content-length") {
            continue;
        }
        if let Ok(value) = value.to_str() {
            request.push_str(name.as_str());
            request.push_str(": ");
            request.push_str(value);
            request.push_str("\r\n");
        }
    }
    request.push_str("content-length: ");
    request.push_str(&body.len().to_string());
    request.push_str("\r\n\r\n");
    request.push_str(&String::from_utf8_lossy(body));
    request
}

fn response_from_http_text(response: String) -> Response<Body> {
    let Some((head, body)) = response
        .split_once("\r\n\r\n")
        .or_else(|| response.split_once("\n\n"))
    else {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(response))
            .expect("internal response");
    };
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .and_then(|status| StatusCode::from_u16(status).ok())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder().status(status);
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let Ok(name) = HeaderName::from_bytes(name.trim().as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_str(value.trim()) else {
            continue;
        };
        builder = builder.header(name, value);
    }
    builder
        .header("connection", "close")
        .body(Body::from(body.to_string()))
        .expect("response builder")
}

fn header_value<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().skip(1).find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

fn actor_context_from_request(request: &str, body: &str, request_id: &str) -> ActorContext {
    let body_json = serde_json::from_str::<Value>(body).ok();
    let body_tenant = body_json
        .as_ref()
        .and_then(|value| json_string(value, "tenant_id"))
        .or_else(|| first_record_json_string(body_json.as_ref(), "tenant_id"))
        .or_else(|| graphql_tenant_from_body(body_json.as_ref()))
        .or_else(|| traceql_tenant_from_body(body_json.as_ref()));
    let database_id = header_value(request, "x-tracedb-database-id")
        .map(str::to_string)
        .or_else(|| {
            body_json
                .as_ref()
                .and_then(|value| json_string(value, "database_id"))
        })
        .unwrap_or_else(|| "local".to_string());
    let branch_id = header_value(request, "x-tracedb-branch-id")
        .map(str::to_string)
        .or_else(|| {
            body_json
                .as_ref()
                .and_then(|value| json_string(value, "branch_id"))
        })
        .unwrap_or_else(|| "main".to_string());
    let tenant_id = header_value(request, "x-tracedb-tenant-id")
        .map(str::to_string)
        .or(body_tenant)
        .unwrap_or_else(|| "local".to_string());
    let token_identity = header_value(request, "x-tracedb-token-identity")
        .map(str::to_string)
        .or_else(|| {
            header_value(request, "authorization")
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(|_| "bearer".to_string())
        })
        .unwrap_or_else(|| "anonymous".to_string());
    let actor_request_id = header_value(request, "x-tracedb-request-id")
        .unwrap_or(request_id)
        .to_string();
    let policy_epoch = header_value(request, "x-tracedb-policy-epoch")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let scopes = header_value(request, "x-tracedb-scopes")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    ActorContext::managed_request(
        tenant_id,
        database_id,
        branch_id,
        token_identity,
        actor_request_id,
        policy_epoch,
        scopes,
    )
}

fn json_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn first_record_json_string(value: Option<&Value>, field: &str) -> Option<String> {
    value?
        .get("records")
        .and_then(Value::as_array)
        .and_then(|records| records.first())
        .and_then(|record| record.get(field))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn graphql_tenant_from_body(value: Option<&Value>) -> Option<String> {
    let query = value?.get("query")?.as_str()?;
    let marker = "tenant_id:";
    let after_marker = query.split(marker).nth(1)?.trim_start();
    quoted_prefix(after_marker)
}

fn traceql_tenant_from_body(value: Option<&Value>) -> Option<String> {
    let query = value?.get("query")?.as_str()?;
    query.lines().find_map(|line| {
        let line = line.trim();
        line.strip_prefix("TENANT ")
            .map(str::trim)
            .filter(|tenant| !tenant.is_empty())
            .map(str::to_string)
    })
}

fn quoted_prefix(value: &str) -> Option<String> {
    let value = value.strip_prefix('"')?;
    let end = value.find('"')?;
    Some(value[..end].to_string())
}

fn next_request_id() -> String {
    format!(
        "engine-{}-{}",
        std::process::id(),
        NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn log_request(service: &str, request_id: &str, method: &str, path: &str) {
    println!(
        "{}",
        json!({
            "service": service,
            "request_id": request_id,
            "method": method,
            "path": path,
        })
    );
}

fn ok(value: Value) -> String {
    let body = value.to_string();
    ok_body_with_headers(body, &[])
}

fn ok_timed(
    value: Value,
    request_start: Instant,
    response_shape_ms: f64,
    timings: &[(&str, f64)],
) -> String {
    let encode_start = Instant::now();
    let body = value.to_string();
    let body_encode_ms = elapsed_ms(encode_start);
    let encode_ms = response_shape_ms + body_encode_ms;
    let prewrite_total_ms = elapsed_ms(request_start);
    let mut all_timings = timings.to_vec();
    all_timings.push(("response_shape", response_shape_ms));
    all_timings.push(("body_encode", body_encode_ms));
    all_timings.push(("encode", encode_ms));
    all_timings.push(("prewrite_total", prewrite_total_ms));
    ok_body_with_headers(
        body,
        &[("server-timing", server_timing_header(&all_timings))],
    )
}

fn ok_body_with_headers(body: String, extra_headers: &[(&str, String)]) -> String {
    let mut response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    response.push_str(&body);
    response
}

fn server_timing_header(timings: &[(&str, f64)]) -> String {
    timings
        .iter()
        .map(|(name, value)| format!("{name};dur={:.3}", value.max(0.0)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn not_found() -> String {
    let body = json!({ "error": "not found", "code": "not_found" }).to_string();
    format!(
        "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bad_request(message: String) -> String {
    let body = json!({ "error": message, "code": "bad_request" }).to_string();
    format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn unauthorized(message: impl Into<String>) -> String {
    let body = json!({ "error": message.into(), "code": "unauthorized" }).to_string();
    format!(
        "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn conflict(message: impl Into<String>) -> String {
    let body = json!({ "error": message.into(), "code": "idempotency_conflict" }).to_string();
    format!(
        "HTTP/1.1 409 Conflict\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn to_io_error(error: impl std::error::Error) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_health_requires_private_token_when_configured() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (db, idempotency_cache) = open_server_state(temp.path()).expect("server state");
        let config = EngineServerConfig::default().with_internal_token("engine-secret");

        let missing = handle_request_text_for_test(
            "GET /internal/health HTTP/1.1\r\ncontent-length: 0\r\n\r\n",
            Arc::clone(&db),
            Arc::clone(&idempotency_cache),
            config.clone(),
        )
        .expect("missing token response");
        assert!(
            missing.starts_with("HTTP/1.1 401 Unauthorized"),
            "missing token should be rejected: {missing}"
        );

        let authorized = handle_request_text_for_test(
            "GET /internal/health HTTP/1.1\r\nx-tracedb-engine-token: engine-secret\r\ncontent-length: 0\r\n\r\n",
            db,
            idempotency_cache,
            config,
        )
        .expect("authorized token response");
        assert!(
            authorized.starts_with("HTTP/1.1 200 OK"),
            "authorized token should pass: {authorized}"
        );
    }

    #[test]
    fn stateful_v1_routes_require_private_token_when_configured() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (db, idempotency_cache) = open_server_state(temp.path()).expect("server state");
        let config = EngineServerConfig::default().with_internal_token("engine-secret");
        let schema = json!({
            "name": "docs",
            "primary_id_column": "id",
            "tenant_id_column": "tenant",
            "scalar_columns": ["status"],
            "text_indexed_columns": ["body"],
            "vector_columns": [{
                "name": "embedding",
                "dimensions": 2,
                "source_columns": ["body"]
            }]
        })
        .to_string();

        let missing = handle_request_text_for_test(
            &format!(
                "POST /v1/schema/apply HTTP/1.1\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                schema.len(),
                schema
            ),
            Arc::clone(&db),
            Arc::clone(&idempotency_cache),
            config.clone(),
        )
        .expect("missing token response");
        assert!(
            missing.starts_with("HTTP/1.1 401 Unauthorized"),
            "stateful route without private token should be rejected: {missing}"
        );

        let authorized = handle_request_text_for_test(
            &format!(
                "POST /v1/schema/apply HTTP/1.1\r\ncontent-type: application/json\r\nx-tracedb-engine-token: engine-secret\r\ncontent-length: {}\r\n\r\n{}",
                schema.len(),
                schema
            ),
            db,
            idempotency_cache,
            config,
        )
        .expect("authorized token response");
        assert!(
            authorized.starts_with("HTTP/1.1 200 OK"),
            "stateful route with private token should pass: {authorized}"
        );
    }

    #[tokio::test]
    async fn axum_entrypoint_rejects_oversized_body() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (db, idempotency_cache) = open_server_state(temp.path()).expect("server state");
        let state = EngineAppState {
            db,
            idempotency_cache,
            config: EngineServerConfig::default(),
        };
        let response = handle_axum_request(
            State(state),
            Method::POST,
            "/v1/query".parse().expect("uri"),
            HeaderMap::new(),
            Body::from(vec![b'x'; 16 * 1024 * 1024 + 1]),
        )
        .await
        .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 4096)
            .await
            .expect("body bytes");
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.contains("failed to read request body"),
            "oversized Axum body should be rejected by production path: {body}"
        );
    }
}
