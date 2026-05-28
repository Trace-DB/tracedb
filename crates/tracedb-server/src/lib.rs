#![forbid(unsafe_code)]

use axum::body::{to_bytes, Body, Bytes};
use axum::error_handling::HandleErrorLayer;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, Uri};
use axum::routing::any;
use axum::{BoxError, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
#[cfg(test)]
use tokio::sync::RwLockReadGuard;
use tower::limit::ConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tracedb_core::{stable_body_hash, Epoch, IdempotencyReceipt, TraceDbManifest};
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
    engine: EngineHandle,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineServerConfig {
    pub internal_token: Option<String>,
    pub request_timeout: Duration,
    pub max_concurrent_requests: usize,
}

impl Default for EngineServerConfig {
    fn default() -> Self {
        Self {
            internal_token: None,
            request_timeout: Duration::from_secs(30),
            max_concurrent_requests: 1024,
        }
    }
}

impl EngineServerConfig {
    pub fn from_env() -> Self {
        Self {
            internal_token: std::env::var("TRACEDB_ENGINE_INTERNAL_TOKEN")
                .ok()
                .or_else(|| std::env::var("TRACEDB_ENGINE_TOKEN").ok())
                .filter(|token| !token.trim().is_empty()),
            request_timeout: env_duration_ms("TRACEDB_REQUEST_TIMEOUT_MS", 30_000),
            max_concurrent_requests: env_usize("TRACEDB_MAX_CONCURRENT_REQUESTS", 1024).max(1),
        }
    }

    pub fn with_internal_token(mut self, token: impl Into<String>) -> Self {
        self.internal_token = Some(token.into());
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_max_concurrent_requests(mut self, limit: usize) -> Self {
        self.max_concurrent_requests = limit.max(1);
        self
    }

    fn authorizes_private_request(&self, request: &str) -> bool {
        let Some(required) = self.internal_token.as_deref() else {
            return true;
        };
        header_value(request, "x-tracedb-engine-token") == Some(required)
    }
}

#[derive(Clone)]
pub struct EngineHandle {
    db: Arc<RwLock<TraceDb>>,
}

impl EngineHandle {
    pub fn open(db_path: impl AsRef<Path>) -> std::io::Result<Self> {
        Ok(Self {
            db: Arc::new(RwLock::new(TraceDb::open(db_path).map_err(to_io_error)?)),
        })
    }

    async fn inspect_manifest(&self) -> std::io::Result<(TraceDbManifest, bool)> {
        let db = self.db.read().await;
        Ok((
            db.inspect_manifest().map_err(to_io_error)?,
            db.last_recovery_torn_tail().is_some(),
        ))
    }

    pub async fn ready_response(&self) -> std::io::Result<String> {
        let (manifest, torn_tail) = self.inspect_manifest().await?;
        Ok(ok(json!({
            "ready": true,
            "service": "tracedb-engine",
            "latest_epoch": manifest.latest_epoch.get(),
            "durable_epoch": manifest.durable_epoch.get(),
            "recovery_state": if torn_tail { "torn_tail_ignored" } else { "clean" },
        })))
    }

    pub async fn apply_schema(&self, schema: TableSchema) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .apply_schema(schema)
            .map_err(to_io_error)
    }

    pub async fn apply_schema_with_idempotency_receipt(
        &self,
        schema: TableSchema,
        receipt: Option<IdempotencyReceipt>,
    ) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .apply_schema_with_idempotency_receipt(schema, receipt)
            .map_err(to_io_error)
    }

    pub async fn insert(&self, input: RecordInput) -> std::io::Result<Epoch> {
        self.db.write().await.insert(input).map_err(to_io_error)
    }

    pub async fn insert_with_idempotency_receipt(
        &self,
        input: RecordInput,
        receipt: Option<IdempotencyReceipt>,
    ) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .insert_with_idempotency_receipt(input, receipt)
            .map_err(to_io_error)
    }

    pub async fn put(&self, request: RecordPutRequest) -> std::io::Result<Epoch> {
        self.db.write().await.put(request).map_err(to_io_error)
    }

    pub async fn put_with_idempotency_receipt(
        &self,
        request: RecordPutRequest,
        receipt: Option<IdempotencyReceipt>,
    ) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .put_with_idempotency_receipt(request, receipt)
            .map_err(to_io_error)
    }

    pub async fn put_batch(&self, request: RecordPutBatchRequest) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .put_batch(request)
            .map_err(to_io_error)
    }

    pub async fn put_batch_with_idempotency_receipt(
        &self,
        request: RecordPutBatchRequest,
        receipt: Option<IdempotencyReceipt>,
    ) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .put_batch_with_idempotency_receipt(request, receipt)
            .map_err(to_io_error)
    }

    pub async fn put_batch_with_write_timing(
        &self,
        request: RecordPutBatchRequest,
    ) -> std::io::Result<(Epoch, tracedb_query::WritePathTiming)> {
        self.db
            .write()
            .await
            .put_batch_with_write_timing(request)
            .map_err(to_io_error)
    }

    pub async fn patch(&self, request: RecordPatchRequest) -> std::io::Result<Epoch> {
        self.db.write().await.patch(request).map_err(to_io_error)
    }

    pub async fn patch_with_idempotency_receipt(
        &self,
        request: RecordPatchRequest,
        receipt: Option<IdempotencyReceipt>,
    ) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .patch_with_idempotency_receipt(request, receipt)
            .map_err(to_io_error)
    }

    pub async fn delete(&self, request: RecordDeleteRequest) -> std::io::Result<Epoch> {
        self.db.write().await.delete(request).map_err(to_io_error)
    }

    pub async fn delete_with_idempotency_receipt(
        &self,
        request: RecordDeleteRequest,
        receipt: Option<IdempotencyReceipt>,
    ) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .delete_with_idempotency_receipt(request, receipt)
            .map_err(to_io_error)
    }

    pub async fn get_as(
        &self,
        actor: &ActorContext,
        request: RecordGetRequest,
    ) -> std::io::Result<Option<tracedb_query::RecordOutput>> {
        self.db
            .read()
            .await
            .get_as(actor, request)
            .map_err(to_io_error)
    }

    pub async fn scan_as(
        &self,
        actor: &ActorContext,
        request: RecordScanRequest,
    ) -> std::io::Result<tracedb_query::RecordScanOutput> {
        self.db
            .read()
            .await
            .scan_as(actor, request)
            .map_err(to_io_error)
    }

    pub async fn query_with_timing_as(
        &self,
        actor: &ActorContext,
        query: HybridQuery,
    ) -> std::io::Result<(tracedb_query::TimedQueryOutput, f64)> {
        let lock_start = Instant::now();
        let db = self.db.read().await;
        let lock_wait_ms = elapsed_ms(lock_start);
        let output = db.query_with_timing_as(actor, query).map_err(to_io_error)?;
        Ok((output, lock_wait_ms))
    }

    async fn graphql_schema_response(&self) -> std::io::Result<String> {
        let db = self.db.read().await;
        let manifest = db.inspect_manifest().map_err(to_io_error)?;
        let schema = graphql_schema_sdl_from_tables(&manifest.schemas).map_err(to_io_error)?;
        let tables = manifest
            .schemas
            .iter()
            .map(|schema| schema.name.clone())
            .collect::<Vec<_>>();
        Ok(ok(json!({
            "adapter": "bounded_graphql_query_adapter",
            "schema": schema,
            "tables": tables,
            "execution": "POST /v1/graphql returns TraceDB QueryResponse, not a GraphQL data envelope",
        })))
    }

    pub async fn compact(&self) -> std::io::Result<()> {
        self.db.write().await.compact().map_err(to_io_error)
    }

    pub async fn create_snapshot(&self, target: impl AsRef<Path>) -> std::io::Result<()> {
        self.db
            .read()
            .await
            .create_snapshot(target)
            .map_err(to_io_error)
    }

    pub async fn idempotency_receipts(&self) -> std::io::Result<Vec<IdempotencyReceipt>> {
        self.db
            .read()
            .await
            .idempotency_receipts()
            .map_err(to_io_error)
    }

    pub async fn record_idempotency_receipt(
        &self,
        receipt: IdempotencyReceipt,
    ) -> std::io::Result<Epoch> {
        self.db
            .write()
            .await
            .record_idempotency_receipt(receipt)
            .map_err(to_io_error)
    }

    pub async fn enqueue_job(
        &self,
        kind: tracedb_jobs::JobKind,
        target: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        self.db
            .write()
            .await
            .enqueue_job(kind, target, idempotency_key)
            .map_err(to_io_error)
    }

    pub async fn jobs(&self) -> std::io::Result<Vec<tracedb_jobs::TraceJob>> {
        self.db.read().await.jobs().map_err(to_io_error)
    }

    pub async fn lease_job(
        &self,
        worker_id: tracedb_jobs::WorkerId,
        kind: tracedb_jobs::JobKind,
        lease_ms: u64,
    ) -> std::io::Result<Option<tracedb_jobs::TraceJob>> {
        self.db
            .write()
            .await
            .lease_job(worker_id, kind, now_ms(), lease_ms)
            .map_err(to_io_error)
    }

    pub async fn heartbeat_job(
        &self,
        job_id: &str,
        lease_token: &str,
        lease_ms: u64,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        self.db
            .write()
            .await
            .heartbeat_job(job_id, lease_token, now_ms(), lease_ms)
            .map_err(to_io_error)
    }

    pub async fn complete_job(
        &self,
        job_id: &str,
        lease_token: &str,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        self.db
            .write()
            .await
            .complete_job(job_id, lease_token)
            .map_err(to_io_error)
    }

    pub async fn fail_job(
        &self,
        job_id: &str,
        lease_token: Option<&str>,
        error: &str,
        permanent: bool,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        self.db
            .write()
            .await
            .fail_job(job_id, lease_token, error, permanent, now_ms())
            .map_err(to_io_error)
    }

    #[cfg(test)]
    async fn hold_read_snapshot_for_test(&self) -> RwLockReadGuard<'_, TraceDb> {
        self.db.read().await
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IdempotencyCacheKey {
    method: String,
    path: String,
    key: String,
    actor_tenant_id: String,
    database_id: String,
    branch_id: String,
    token_identity: String,
}

impl IdempotencyCacheKey {
    fn new(method: &str, path: &str, key: &str, actor: &ActorContext) -> Self {
        Self {
            method: method.to_string(),
            path: path.to_string(),
            key: key.to_string(),
            actor_tenant_id: actor.tenant_id.clone(),
            database_id: actor.database_id.clone(),
            branch_id: actor.branch_id.clone(),
            token_identity: actor.token_identity.clone(),
        }
    }

    fn from_receipt(receipt: &IdempotencyReceipt) -> Self {
        Self {
            method: receipt.method.clone(),
            path: receipt.path.clone(),
            key: receipt.key.clone(),
            actor_tenant_id: receipt.actor_tenant_id.clone(),
            database_id: receipt.database_id.clone(),
            branch_id: receipt.branch_id.clone(),
            token_identity: receipt.token_identity.clone(),
        }
    }

    fn to_receipt(&self, body_hash: String) -> IdempotencyReceipt {
        IdempotencyReceipt {
            method: self.method.clone(),
            path: self.path.clone(),
            key: self.key.clone(),
            body_hash,
            actor_tenant_id: self.actor_tenant_id.clone(),
            database_id: self.database_id.clone(),
            branch_id: self.branch_id.clone(),
            token_identity: self.token_identity.clone(),
            response: String::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct IdempotencyEntry {
    body_hash: String,
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
    entries: HashMap<IdempotencyCacheKey, IdempotencyEntry>,
}

impl IdempotencyCacheState {
    fn from_receipts(receipts: Vec<IdempotencyReceipt>) -> Self {
        let entries = receipts
            .into_iter()
            .map(|receipt| {
                (
                    IdempotencyCacheKey::from_receipt(&receipt),
                    IdempotencyEntry {
                        body_hash: receipt.body_hash,
                        response: receipt.response,
                    },
                )
            })
            .collect();
        Self { entries }
    }

    fn get(&self, key: &IdempotencyCacheKey) -> Option<IdempotencyEntry> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: IdempotencyCacheKey, entry: IdempotencyEntry) {
        self.entries.insert(key, entry);
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
    let (engine, idempotency_cache) = open_server_state(db_path)?;
    let state = EngineAppState {
        engine,
        idempotency_cache,
        config: config.clone(),
    };
    let app = Router::new()
        .fallback(any(handle_axum_request))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(runtime_handle_error))
                .layer(LoadShedLayer::new())
                .layer(ConcurrencyLimitLayer::new(config.max_concurrent_requests))
                .layer(TimeoutLayer::new(config.request_timeout)),
        )
        .with_state(state);
    axum::serve(listener, app).await
}

async fn runtime_handle_error(error: BoxError) -> Response<Body> {
    if error.is::<tower::timeout::error::Elapsed>() {
        return response_from_http_text(runtime_error_response(RuntimeRouteError::Timeout));
    }
    if error.is::<tower::load_shed::error::Overloaded>() {
        return response_from_http_text(runtime_error_response(RuntimeRouteError::Overloaded));
    }
    response_from_http_text(runtime_error_response(RuntimeRouteError::Internal(
        error.to_string(),
    )))
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
            db.clone(),
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
                    db.clone(),
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
) -> std::io::Result<(EngineHandle, IdempotencyCache)> {
    let db_path = db_path.as_ref().to_path_buf();
    let receipts = TraceDb::open(&db_path)
        .and_then(|db| db.idempotency_receipts())
        .map_err(to_io_error)?;
    let engine = EngineHandle::open(&db_path)?;
    let idempotency_cache = Arc::new(Mutex::new(IdempotencyCacheState::from_receipts(receipts)));
    Ok((engine, idempotency_cache))
}

fn spawn_handler_with_config(
    stream: TcpStream,
    engine: EngineHandle,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) {
    thread::spawn(move || {
        let _ = handle(stream, engine, idempotency_cache, config);
    });
}

fn handle(
    mut stream: TcpStream,
    engine: EngineHandle,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) -> std::io::Result<()> {
    let response = match handle_inner(&mut stream, engine, idempotency_cache, config) {
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
    engine: EngineHandle,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) -> std::io::Result<String> {
    let request = read_request(stream)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(handle_request_text(
        &request,
        engine,
        idempotency_cache,
        config,
    ))
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
    let response = match handle_request_text(
        &request,
        state.engine,
        state.idempotency_cache,
        state.config,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => bad_request(error.to_string()),
    };
    Ok(response_from_http_text(response))
}

async fn handle_request_text(
    request: &str,
    engine: EngineHandle,
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
    let body_hash = stable_body_hash(body.as_bytes());
    let idempotency_cache_key = header_value(&request, "idempotency-key")
        .filter(|key| !key.is_empty())
        .filter(|_| supports_http_idempotency(method, path))
        .map(|key| IdempotencyCacheKey::new(method, path, key, &actor));
    if let Some(cache_key) = idempotency_cache_key.as_ref() {
        let cached_entry = idempotency_cache.lock().unwrap().get(cache_key);
        if let Some(entry) = cached_entry {
            if entry.body_hash == body_hash {
                return Ok(entry.response);
            }
            return Ok(conflict(
                "idempotency key reused with different request body",
            ));
        }
    }

    let response = match (method, path) {
        ("GET", "/health") | ("GET", "/internal/health") => {
            let (manifest, _) = engine.inspect_manifest().await?;
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
        ("GET", "/ready") | ("GET", "/v1/ready") => engine.ready_response().await?,
        ("GET", "/v1/databases") => ok(json!({
            "mode": "local",
            "databases": [{
                "database_id": "local",
                "endpoint": "local-daemon",
            }]
        })),
        ("GET", "/v1/branches") => {
            let (manifest, _) = engine.inspect_manifest().await?;
            ok(json!({
                "branches": [{
                    "branch_id": manifest.branch_id,
                    "state": "ACTIVE",
                    "latest_epoch": manifest.latest_epoch.get(),
                }]
            }))
        }
        ("GET", "/metrics") | ("GET", "/v1/metrics/public-safe") => {
            let (manifest, torn_tail) = engine.inspect_manifest().await?;
            ok(json!({
                "service": "tracedb-engine",
                "latest_epoch": manifest.latest_epoch.get(),
                "durable_epoch": manifest.durable_epoch.get(),
                "segment_count": manifest.segments.len(),
                "index_count": manifest.indexes.len(),
                "module_count": manifest.modules.len(),
                "schema_count": manifest.schemas.len(),
                "recovery_state": if torn_tail { "torn_tail_ignored" } else { "clean" },
            }))
        }
        ("POST", "/v1/schema/apply") => {
            let schema: TableSchema = serde_json::from_str(body).map_err(to_io_error)?;
            let receipt = idempotency_cache_key
                .as_ref()
                .map(|key| key.to_receipt(body_hash.clone()));
            let epoch = engine
                .apply_schema_with_idempotency_receipt(schema, receipt)
                .await?;
            ok(json!({ "epoch": epoch.get() }))
        }
        ("POST", "/v1/insert") => {
            let input: RecordInput = serde_json::from_str(body).map_err(to_io_error)?;
            let receipt = idempotency_cache_key
                .as_ref()
                .map(|key| key.to_receipt(body_hash.clone()));
            let epoch = engine
                .insert_with_idempotency_receipt(input, receipt)
                .await?;
            ok(json!({ "epoch": epoch.get() }))
        }
        ("POST", "/v1/records/put") => {
            let input = parse_record_put_body(body)?;
            let receipt = idempotency_cache_key
                .as_ref()
                .map(|key| key.to_receipt(body_hash.clone()));
            let epoch = engine
                .put_with_idempotency_receipt(RecordPutRequest::new(input), receipt)
                .await?;
            ok(json!({ "epoch": epoch.get() }))
        }
        ("POST", "/v1/records/put-batch") => {
            let request: RecordPutBatchRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let record_count = request.records.len();
            if request.include_write_timing {
                let (epoch, timing) = engine.put_batch_with_write_timing(request).await?;
                ok(json!({
                    "epoch": epoch.get(),
                    "record_count": record_count,
                    "write_timing": timing,
                }))
            } else {
                let receipt = idempotency_cache_key
                    .as_ref()
                    .map(|key| key.to_receipt(body_hash.clone()));
                let epoch = engine
                    .put_batch_with_idempotency_receipt(request, receipt)
                    .await?;
                ok(json!({ "epoch": epoch.get(), "record_count": record_count }))
            }
        }
        ("POST", "/v1/records/patch") => {
            let request: RecordPatchRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let receipt = idempotency_cache_key
                .as_ref()
                .map(|key| key.to_receipt(body_hash.clone()));
            let epoch = engine
                .patch_with_idempotency_receipt(request, receipt)
                .await?;
            ok(json!({ "epoch": epoch.get() }))
        }
        ("POST", "/v1/records/delete") => {
            let request: RecordDeleteRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let receipt = idempotency_cache_key
                .as_ref()
                .map(|key| key.to_receipt(body_hash.clone()));
            let epoch = engine
                .delete_with_idempotency_receipt(request, receipt)
                .await?;
            ok(json!({ "deleted": true, "epoch": epoch.get() }))
        }
        ("POST", "/v1/records/get") => {
            let request: RecordGetRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let record = engine.get_as(&actor, request).await?;
            ok(json!({ "record": record }))
        }
        ("POST", "/v1/records/scan") => {
            let request: RecordScanRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let output = engine.scan_as(&actor, request).await?;
            ok(serde_json::to_value(output).map_err(to_io_error)?)
        }
        ("POST", "/v1/query") => {
            let parse_start = Instant::now();
            let query: HybridQuery = serde_json::from_str(body).map_err(to_io_error)?;
            let parse_ms = elapsed_ms(parse_start);
            query_response(&engine, actor, query, request_start, read_ms, parse_ms).await?
        }
        ("POST", "/v1/traceql") => {
            let parse_start = Instant::now();
            let request: TraceQlQueryRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let query = traceql_query_from_str(&request.query).map_err(to_io_error)?;
            let parse_ms = elapsed_ms(parse_start);
            query_response(&engine, actor, query, request_start, read_ms, parse_ms).await?
        }
        ("POST", "/v1/graphql") => {
            let parse_start = Instant::now();
            let request: GraphQlQueryRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let query = graphql_query_from_str(&request.query).map_err(to_io_error)?;
            let parse_ms = elapsed_ms(parse_start);
            query_response(&engine, actor, query, request_start, read_ms, parse_ms).await?
        }
        ("GET", "/v1/graphql/schema") => engine.graphql_schema_response().await?,
        ("POST", "/v1/explain") => {
            let parse_start = Instant::now();
            let mut query: HybridQuery = serde_json::from_str(body).map_err(to_io_error)?;
            query.explain = true;
            let parse_ms = elapsed_ms(parse_start);
            let mut actor = actor.clone();
            if actor.tenant_id == "local" {
                actor.tenant_id = query.tenant_id.clone();
            }
            let engine_start = Instant::now();
            let (timed_output, lock_wait_ms) = engine.query_with_timing_as(&actor, query).await?;
            let engine_ms = elapsed_ms(engine_start);
            let query_timing = timed_output.timing;
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
            engine.compact().await?;
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
            engine.create_snapshot(target).await?;
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
        ("POST", "/internal/jobs/lease") => {
            let value: Value = serde_json::from_str(body).map_err(to_io_error)?;
            let worker_id = value
                .get("worker_id")
                .and_then(Value::as_str)
                .unwrap_or("worker")
                .to_string();
            let kind = value
                .get("kind")
                .and_then(Value::as_str)
                .and_then(parse_job_kind)
                .unwrap_or(tracedb_jobs::JobKind::VerifyDatabase);
            let lease_ms = value
                .get("lease_ms")
                .and_then(Value::as_u64)
                .unwrap_or(30_000);
            let job = engine
                .lease_job(tracedb_jobs::WorkerId::new(worker_id), kind, lease_ms)
                .await?;
            ok(json!({ "leased": job.is_some(), "job": job }))
        }
        ("POST", "/internal/jobs/heartbeat") => {
            let value: Value = serde_json::from_str(body).map_err(to_io_error)?;
            let job_id = required_str(&value, "job_id")?;
            let lease_token = required_str(&value, "lease_token")?;
            let lease_ms = value
                .get("lease_ms")
                .and_then(Value::as_u64)
                .unwrap_or(30_000);
            let job = engine.heartbeat_job(job_id, lease_token, lease_ms).await?;
            ok(json!({ "heartbeat": true, "job": job }))
        }
        ("POST", "/internal/jobs/complete") => {
            let value: Value = serde_json::from_str(body).map_err(to_io_error)?;
            let job_id = required_str(&value, "job_id")?;
            let lease_token = required_str(&value, "lease_token")?;
            let job = engine.complete_job(job_id, lease_token).await?;
            ok(json!({ "completed": true, "job": job }))
        }
        ("POST", "/internal/jobs/fail") => {
            let value: Value = serde_json::from_str(body).map_err(to_io_error)?;
            let job_id = required_str(&value, "job_id")?;
            let lease_token = value.get("lease_token").and_then(Value::as_str);
            let error = value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("job failed");
            let permanent = value
                .get("permanent")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let job = engine
                .fail_job(job_id, lease_token, error, permanent)
                .await?;
            ok(json!({ "failed": true, "job": job }))
        }
        ("GET", "/v1/admin/jobs") => {
            let durable_jobs = engine.jobs().await?;
            ok(json!({
                "durable": true,
                "queues": [
                    "tracedb.segment.compact",
                    "tracedb.snapshot.create",
                    "tracedb.feature.index"
                ],
                "jobs": [
                    { "queue": "tracedb.segment.compact", "state": "idle" },
                    { "queue": "tracedb.snapshot.create", "state": "idle" },
                    { "queue": "tracedb.feature.index", "state": "idle" }
                ],
                "durable_jobs": durable_jobs,
            }))
        }
        _ => not_found(),
    };
    if let Some(cache_key) = idempotency_cache_key {
        if response.starts_with("HTTP/1.1 200 OK") {
            if records_receipt_after_response(method, path) {
                let mut receipt = cache_key.to_receipt(body_hash.clone());
                receipt.response = response.clone();
                engine.record_idempotency_receipt(receipt).await?;
            }
            idempotency_cache.lock().unwrap().insert(
                cache_key,
                IdempotencyEntry {
                    body_hash,
                    response: response.clone(),
                },
            );
        }
    }
    Ok(response)
}

#[cfg(test)]
fn handle_request_text_for_test(
    request: &str,
    engine: EngineHandle,
    idempotency_cache: IdempotencyCache,
    config: EngineServerConfig,
) -> std::io::Result<String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(handle_request_text(
        request,
        engine,
        idempotency_cache,
        config,
    ))
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

fn required_str<'a>(value: &'a Value, field: &str) -> std::io::Result<&'a str> {
    value.get(field).and_then(Value::as_str).ok_or_else(|| {
        to_io_error(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{field} is required"),
        ))
    })
}

fn parse_job_kind(value: &str) -> Option<tracedb_jobs::JobKind> {
    match value {
        "GenerateEmbedding" | "generate_embedding" => {
            Some(tracedb_jobs::JobKind::GenerateEmbedding)
        }
        "RegenerateEmbedding" | "regenerate_embedding" => {
            Some(tracedb_jobs::JobKind::RegenerateEmbedding)
        }
        "BuildTextIndex" | "build_text_index" => Some(tracedb_jobs::JobKind::BuildTextIndex),
        "BuildVectorIndex" | "build_vector_index" => Some(tracedb_jobs::JobKind::BuildVectorIndex),
        "CompactSegment" | "compact_segment" => Some(tracedb_jobs::JobKind::CompactSegment),
        "VacuumArtifacts" | "vacuum_artifacts" => Some(tracedb_jobs::JobKind::VacuumArtifacts),
        "ReindexTable" | "reindex_table" => Some(tracedb_jobs::JobKind::ReindexTable),
        "ValidatePolicy" | "validate_policy" => Some(tracedb_jobs::JobKind::ValidatePolicy),
        "RefreshSummary" | "refresh_summary" => Some(tracedb_jobs::JobKind::RefreshSummary),
        "FeatureRefresh" | "feature_refresh" => Some(tracedb_jobs::JobKind::FeatureRefresh),
        "ExportSubject" | "export_subject" => Some(tracedb_jobs::JobKind::ExportSubject),
        "PurgeSubject" | "purge_subject" => Some(tracedb_jobs::JobKind::PurgeSubject),
        "BackupDatabase" | "backup_database" => Some(tracedb_jobs::JobKind::BackupDatabase),
        "RestoreVerification" | "restore_verification" => {
            Some(tracedb_jobs::JobKind::RestoreVerification)
        }
        "VerifyDatabase" | "verify_database" => Some(tracedb_jobs::JobKind::VerifyDatabase),
        _ => None,
    }
}

fn records_receipt_after_response(method: &str, path: &str) -> bool {
    matches!(
        (method, path),
        ("POST", "/v1/admin/compact")
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

async fn query_response(
    engine: &EngineHandle,
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
    let engine_start = Instant::now();
    let (timed_output, lock_wait_ms) = engine.query_with_timing_as(&actor, query).await?;
    let engine_ms = elapsed_ms(engine_start);
    let query_timing = timed_output.timing;
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

pub fn init_json_tracing(default_filter: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new(default_filter))
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .try_init();
}

#[cfg(test)]
fn request_log_fields(service: &str, request_id: &str, method: &str, path: &str) -> Value {
    json!({
        "service": service,
        "request_id": request_id,
        "method": method,
        "path": path,
    })
}

fn log_request(service: &str, request_id: &str, method: &str, path: &str) {
    tracing::info!(
        service = service,
        request_id = request_id,
        method = method,
        path = path,
        "request"
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum RuntimeRouteError {
    Timeout,
    Overloaded,
    Internal(String),
}

fn runtime_error_response(error: RuntimeRouteError) -> String {
    match error {
        RuntimeRouteError::Timeout => http_json_response(
            "504 Gateway Timeout",
            json!({
                "error": "request timed out",
                "code": "timeout",
            }),
        ),
        RuntimeRouteError::Overloaded => http_json_response(
            "503 Service Unavailable",
            json!({
                "error": "request capacity exceeded",
                "code": "overloaded",
            }),
        ),
        RuntimeRouteError::Internal(message) => http_json_response(
            "500 Internal Server Error",
            json!({
                "error": message,
                "code": "internal_error",
            }),
        ),
    }
}

fn http_json_response(status: &str, body: Value) -> String {
    let body = body.to_string();
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn env_duration_ms(name: &str, default_ms: u64) -> Duration {
    Duration::from_millis(
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(default_ms),
    )
}

fn env_usize(name: &str, default_value: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default_value)
}

fn to_io_error(error: impl std::error::Error) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_test_schema() -> TableSchema {
        TableSchema {
            name: "docs".to_string(),
            primary_id_column: "id".to_string(),
            tenant_id_column: "tenant".to_string(),
            scalar_columns: vec!["status".to_string()],
            text_indexed_columns: vec!["body".to_string()],
            vector_columns: vec![],
        }
    }

    fn runtime_test_record(id: &str, body: &str) -> RecordInput {
        RecordInput {
            table: "docs".to_string(),
            id: id.to_string(),
            tenant_id: "tenant-a".to_string(),
            fields: json!({
                "id": id,
                "tenant": "tenant-a",
                "status": "active",
                "body": body,
            })
            .as_object()
            .expect("record fields")
            .clone(),
        }
    }

    #[tokio::test]
    async fn engine_handle_readiness_does_not_wait_behind_read_snapshot() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = EngineHandle::open(temp.path()).expect("engine handle");
        let _read_snapshot = engine.hold_read_snapshot_for_test().await;

        let ready = tokio::time::timeout(Duration::from_millis(50), engine.ready_response())
            .await
            .expect("ready should not wait behind an active read snapshot")
            .expect("ready response");

        assert!(
            ready.starts_with("HTTP/1.1 200 OK"),
            "ready response should be OK while a reader is active: {ready}"
        );
        assert!(ready.contains("\"ready\":true"));
    }

    #[tokio::test]
    async fn engine_handle_serializes_write_commits_and_keeps_reads_visible() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = EngineHandle::open(temp.path()).expect("engine handle");
        engine
            .apply_schema(runtime_test_schema())
            .await
            .expect("schema");

        let left = {
            let engine = engine.clone();
            tokio::spawn(async move {
                engine
                    .put(RecordPutRequest::new(runtime_test_record("a", "left")))
                    .await
                    .expect("left put")
                    .get()
            })
        };
        let right = {
            let engine = engine.clone();
            tokio::spawn(async move {
                engine
                    .put(RecordPutRequest::new(runtime_test_record("b", "right")))
                    .await
                    .expect("right put")
                    .get()
            })
        };
        let mut epochs = vec![
            left.await.expect("left task"),
            right.await.expect("right task"),
        ];
        epochs.sort_unstable();
        assert_eq!(epochs, vec![2, 3]);

        let actor = ActorContext::tenant_user("tenant-a", "runtime-test");
        let scanned = engine
            .scan_as(&actor, RecordScanRequest::new("docs", "tenant-a"))
            .await
            .expect("scan");
        assert_eq!(scanned.returned_count, 2);
    }

    #[tokio::test]
    async fn engine_handle_read_outputs_are_stable_across_later_writes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = EngineHandle::open(temp.path()).expect("engine handle");
        let actor = ActorContext::tenant_user("tenant-a", "runtime-test");
        engine
            .apply_schema(runtime_test_schema())
            .await
            .expect("schema");
        engine
            .put(RecordPutRequest::new(runtime_test_record("a", "first")))
            .await
            .expect("first put");

        let before = engine
            .scan_as(&actor, RecordScanRequest::new("docs", "tenant-a"))
            .await
            .expect("scan before");
        engine
            .put(RecordPutRequest::new(runtime_test_record("b", "second")))
            .await
            .expect("second put");
        let after = engine
            .scan_as(&actor, RecordScanRequest::new("docs", "tenant-a"))
            .await
            .expect("scan after");

        assert_eq!(before.returned_count, 1);
        assert_eq!(after.returned_count, 2);
    }

    #[test]
    fn runtime_timeout_and_overload_errors_are_stable_json() {
        let timeout = runtime_error_response(RuntimeRouteError::Timeout);
        assert!(timeout.starts_with("HTTP/1.1 504 Gateway Timeout"));
        assert!(timeout.contains("\"code\":\"timeout\""));

        let overload = runtime_error_response(RuntimeRouteError::Overloaded);
        assert!(overload.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(overload.contains("\"code\":\"overloaded\""));
    }

    #[test]
    fn request_log_fields_are_structured_for_tracing() {
        let fields = request_log_fields("tracedb-engine", "request-1", "POST", "/v1/query");
        assert_eq!(fields["service"], json!("tracedb-engine"));
        assert_eq!(fields["request_id"], json!("request-1"));
        assert_eq!(fields["method"], json!("POST"));
        assert_eq!(fields["path"], json!("/v1/query"));
    }

    #[test]
    fn internal_health_requires_private_token_when_configured() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (db, idempotency_cache) = open_server_state(temp.path()).expect("server state");
        let config = EngineServerConfig::default().with_internal_token("engine-secret");

        let missing = handle_request_text_for_test(
            "GET /internal/health HTTP/1.1\r\ncontent-length: 0\r\n\r\n",
            db.clone(),
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
            db.clone(),
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
    async fn admin_jobs_lists_persisted_job_catalog_state() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (engine, idempotency_cache) = open_server_state(temp.path()).expect("server state");
        engine
            .enqueue_job(
                tracedb_jobs::JobKind::CompactSegment,
                "segment:seg-1",
                "compact:seg-1",
            )
            .await
            .expect("enqueue job");
        let config = EngineServerConfig::default().with_internal_token("engine-secret");

        let response = handle_request_text(
            "GET /v1/admin/jobs HTTP/1.1\r\nx-tracedb-engine-token: engine-secret\r\ncontent-length: 0\r\n\r\n",
            engine,
            idempotency_cache,
            config,
        )
        .await
        .expect("admin jobs response");

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"job_id\":\"job:compact_segment:compact:seg-1\""));
        assert!(response.contains("\"status\":\"Queued\""));
        assert!(response.contains("\"durable\":true"));
    }

    #[tokio::test]
    async fn private_worker_job_endpoints_lease_heartbeat_complete_and_fail() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (engine, idempotency_cache) = open_server_state(temp.path()).expect("server state");
        engine
            .enqueue_job(
                tracedb_jobs::JobKind::BuildTextIndex,
                "segment:seg-1",
                "text:seg-1",
            )
            .await
            .expect("enqueue job");
        let config = EngineServerConfig::default().with_internal_token("engine-secret");
        let lease_body = r#"{"worker_id":"worker-1","kind":"BuildTextIndex","lease_ms":5000}"#;

        let lease = handle_request_text(
            &format!(
                "POST /internal/jobs/lease HTTP/1.1\r\ncontent-type: application/json\r\nx-tracedb-engine-token: engine-secret\r\ncontent-length: {}\r\n\r\n{}",
                lease_body.len(),
                lease_body
            ),
            engine.clone(),
            Arc::clone(&idempotency_cache),
            config.clone(),
        )
        .await
        .expect("lease response");

        assert!(lease.starts_with("HTTP/1.1 200 OK"));
        assert!(lease.contains("\"leased\":true"));
        assert!(lease.contains("\"lease_token\""));

        let body = lease.split("\r\n\r\n").nth(1).expect("lease body");
        let parsed: Value = serde_json::from_str(body).expect("lease json");
        let job_id = parsed["job"]["job_id"].as_str().expect("job id");
        let lease_token = parsed["job"]["lease_token"].as_str().expect("lease token");
        let heartbeat_body =
            json!({ "job_id": job_id, "lease_token": lease_token, "lease_ms": 5000 }).to_string();
        let heartbeat = handle_request_text(
            &format!(
                "POST /internal/jobs/heartbeat HTTP/1.1\r\ncontent-type: application/json\r\nx-tracedb-engine-token: engine-secret\r\ncontent-length: {}\r\n\r\n{}",
                heartbeat_body.len(),
                heartbeat_body
            ),
            engine.clone(),
            Arc::clone(&idempotency_cache),
            config.clone(),
        )
        .await
        .expect("heartbeat response");
        assert!(heartbeat.contains("\"heartbeat\":true"));

        let complete_body = json!({ "job_id": job_id, "lease_token": lease_token }).to_string();
        let complete = handle_request_text(
            &format!(
                "POST /internal/jobs/complete HTTP/1.1\r\ncontent-type: application/json\r\nx-tracedb-engine-token: engine-secret\r\ncontent-length: {}\r\n\r\n{}",
                complete_body.len(),
                complete_body
            ),
            engine,
            idempotency_cache,
            config,
        )
        .await
        .expect("complete response");
        assert!(complete.contains("\"completed\":true"));
    }

    #[tokio::test]
    async fn axum_entrypoint_rejects_oversized_body() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (engine, idempotency_cache) = open_server_state(temp.path()).expect("server state");
        let state = EngineAppState {
            engine,
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
