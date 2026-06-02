#![forbid(unsafe_code)]
//! HTTP server and request handler for the TraceDB engine.

use async_graphql::parser::parse_query;
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
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use tokio::sync::RwLockReadGuard;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
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
        let internal_token = std::env::var("TRACEDB_ENGINE_INTERNAL_TOKEN")
            .ok()
            .or_else(|| std::env::var("TRACEDB_ENGINE_TOKEN").ok())
            .filter(|token| !token.trim().is_empty());
        let require_internal_token = bool_env("TRACEDB_REQUIRE_ENGINE_TOKEN", false)
            || bool_env("TRACEDB_HOSTED_ALPHA", false);
        assert!(
            internal_token.is_some() || !require_internal_token,
            "TRACEDB_ENGINE_INTERNAL_TOKEN must be set when hosted/private engine mode is enabled"
        );
        Self {
            internal_token,
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
/// Handle to an opened TraceDB engine shard.
pub struct EngineHandle {
    db: Arc<RwLock<TraceDb>>,
    shard_key: ShardKey,
    root_dir: Arc<PathBuf>,
    shards: Arc<RwLock<HashMap<ShardKey, Arc<RwLock<TraceDb>>>>>,
    job_catalogs: Arc<RwLock<HashMap<ShardKey, Arc<AsyncMutex<ServerJobRuntime>>>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ShardKey {
    database_id: String,
    branch_id: String,
}

fn default_shard_key() -> ShardKey {
    ShardKey {
        database_id: "local".to_string(),
        branch_id: "main".to_string(),
    }
}

fn is_default_shard_key(key: &ShardKey) -> bool {
    (key.database_id == "local" && key.branch_id == "main")
        || (key.database_id == "db_local" && key.branch_id == "db_local:main")
}

fn shard_path(root_dir: &Path, key: &ShardKey) -> PathBuf {
    root_dir
        .join("shards")
        .join(safe_shard_component(&key.database_id))
        .join(safe_shard_component(&key.branch_id))
}

fn safe_shard_component(value: &str) -> String {
    if value == "." || value == ".." {
        return "_invalid".to_string();
    }
    let mut output = String::new();
    for byte in value.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                output.push(byte as char)
            }
            _ => output.push_str(&format!("_{byte:02x}")),
        }
    }
    if output.is_empty() {
        "_empty".to_string()
    } else {
        output
    }
}

fn write_shard_receipt(shard_dir: &Path, key: &ShardKey) -> std::io::Result<()> {
    let receipt = json!({
        "format_version": 1,
        "database_id": key.database_id,
        "branch_id": key.branch_id,
        "path_encoding": "tracedb-shard-component-v1",
    })
    .to_string();
    fs::write(shard_dir.join("shard.receipt.json"), receipt)
}

impl EngineHandle {
    pub fn open(db_path: impl AsRef<Path>) -> std::io::Result<Self> {
        let root_dir = db_path.as_ref().to_path_buf();
        let db = Arc::new(RwLock::new(TraceDb::open(&root_dir).map_err(to_io_error)?));
        let default_key = default_shard_key();
        let mut shards = HashMap::new();
        shards.insert(default_key.clone(), Arc::clone(&db));
        shards.insert(
            ShardKey {
                database_id: "db_local".to_string(),
                branch_id: "db_local:main".to_string(),
            },
            Arc::clone(&db),
        );
        Ok(Self {
            db,
            shard_key: default_key,
            root_dir: Arc::new(root_dir),
            shards: Arc::new(RwLock::new(shards)),
            job_catalogs: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn for_actor(&self, actor: &ActorContext) -> std::io::Result<Self> {
        let key = ShardKey {
            database_id: actor.database_id.clone(),
            branch_id: actor.branch_id.clone(),
        };
        if is_default_shard_key(&key) {
            return Ok(self.clone_with_db(key, Arc::clone(&self.db)));
        }
        if let Some(db) = self.shards.read().await.get(&key).cloned() {
            return Ok(self.clone_with_db(key, db));
        }
        let mut shards = self.shards.write().await;
        if let Some(db) = shards.get(&key).cloned() {
            return Ok(self.clone_with_db(key, db));
        }
        let shard_dir = shard_path(&self.root_dir, &key);
        fs::create_dir_all(&shard_dir)?;
        write_shard_receipt(&shard_dir, &key)?;
        let db = Arc::new(RwLock::new(TraceDb::open(&shard_dir).map_err(to_io_error)?));
        shards.insert(key.clone(), Arc::clone(&db));
        Ok(self.clone_with_db(key, db))
    }

    fn clone_with_db(&self, shard_key: ShardKey, db: Arc<RwLock<TraceDb>>) -> Self {
        Self {
            db,
            shard_key,
            root_dir: Arc::clone(&self.root_dir),
            shards: Arc::clone(&self.shards),
            job_catalogs: Arc::clone(&self.job_catalogs),
        }
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
            "execution": "POST /v1/graphql/bounded returns TraceDB QueryResponse; POST /v1/graphql returns GraphQL data/errors",
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

    async fn job_runtime(&self) -> std::io::Result<Arc<AsyncMutex<ServerJobRuntime>>> {
        if let Some(runtime) = self.job_catalogs.read().await.get(&self.shard_key).cloned() {
            return Ok(runtime);
        }
        let mut runtimes = self.job_catalogs.write().await;
        if let Some(runtime) = runtimes.get(&self.shard_key).cloned() {
            return Ok(runtime);
        }
        let runtime = Arc::new(AsyncMutex::new(ServerJobRuntime::open(
            &self.root_dir,
            &self.shard_key,
        )?));
        runtimes.insert(self.shard_key.clone(), Arc::clone(&runtime));
        Ok(runtime)
    }

    pub async fn enqueue_job(
        &self,
        kind: tracedb_jobs::JobKind,
        target: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        let runtime = self.job_runtime().await?;
        let mut runtime = runtime.lock().await;
        runtime.enqueue(kind, target, idempotency_key)
    }

    pub async fn jobs(&self) -> std::io::Result<Vec<tracedb_jobs::TraceJob>> {
        let runtime = self.job_runtime().await?;
        let runtime = runtime.lock().await;
        Ok(runtime.jobs())
    }

    pub async fn lease_job(
        &self,
        worker_id: tracedb_jobs::WorkerId,
        kind: tracedb_jobs::JobKind,
        lease_ms: u64,
    ) -> std::io::Result<Option<tracedb_jobs::TraceJob>> {
        let runtime = self.job_runtime().await?;
        let mut runtime = runtime.lock().await;
        runtime.lease(worker_id, kind, now_ms(), lease_ms)
    }

    pub async fn heartbeat_job(
        &self,
        job_id: &str,
        lease_token: &str,
        lease_ms: u64,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        let runtime = self.job_runtime().await?;
        let mut runtime = runtime.lock().await;
        runtime.heartbeat(job_id, lease_token, now_ms(), lease_ms)
    }

    pub async fn complete_job(
        &self,
        job_id: &str,
        lease_token: &str,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        let runtime = self.job_runtime().await?;
        let mut runtime = runtime.lock().await;
        runtime.complete(job_id, lease_token)
    }

    pub async fn fail_job(
        &self,
        job_id: &str,
        lease_token: Option<&str>,
        error: &str,
        permanent: bool,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        let runtime = self.job_runtime().await?;
        let mut runtime = runtime.lock().await;
        runtime.fail(job_id, lease_token, error, permanent, now_ms())
    }

    #[cfg(test)]
    async fn hold_read_snapshot_for_test(&self) -> RwLockReadGuard<'_, TraceDb> {
        self.db.read().await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ServerJobCheckpoint {
    format_version: u32,
    shard: ServerJobShardReceipt,
    catalog: tracedb_jobs::JobCatalog,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ServerJobShardReceipt {
    database_id: String,
    branch_id: String,
}

#[derive(Clone, Debug)]
struct ServerJobRuntime {
    catalog: tracedb_jobs::JobCatalog,
    checkpoint_path: PathBuf,
    events_path: PathBuf,
    shard: ShardKey,
}

impl ServerJobRuntime {
    fn open(root_dir: &Path, shard: &ShardKey) -> std::io::Result<Self> {
        let dir = job_runtime_dir(root_dir, shard);
        fs::create_dir_all(&dir)?;
        let checkpoint_path = dir.join("catalog.json");
        let events_path = dir.join("events.jsonl");
        let catalog = if checkpoint_path.exists() {
            let body = fs::read_to_string(&checkpoint_path)?;
            serde_json::from_str::<ServerJobCheckpoint>(&body)
                .map_err(to_io_error)?
                .catalog
        } else {
            tracedb_jobs::JobCatalog::default()
        };
        let mut runtime = Self {
            catalog,
            checkpoint_path,
            events_path,
            shard: shard.clone(),
        };
        runtime.replay_events()?;
        runtime.persist_checkpoint()?;
        Ok(runtime)
    }

    fn enqueue(
        &mut self,
        kind: tracedb_jobs::JobKind,
        target: impl Into<String>,
        idempotency_key: impl Into<String>,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        let job = self
            .catalog
            .enqueue(kind, target, idempotency_key)
            .map_err(invalid_job_command)?;
        self.persist_event(tracedb_jobs::JobEvent::enqueued(job.clone()))?;
        self.persist_checkpoint()?;
        Ok(job)
    }

    fn jobs(&self) -> Vec<tracedb_jobs::TraceJob> {
        self.catalog.jobs()
    }

    fn lease(
        &mut self,
        worker_id: tracedb_jobs::WorkerId,
        kind: tracedb_jobs::JobKind,
        now_ms: u64,
        lease_ms: u64,
    ) -> std::io::Result<Option<tracedb_jobs::TraceJob>> {
        let Some(job) = self
            .catalog
            .lease_next_at(worker_id.clone(), kind, now_ms, lease_ms)
            .map_err(invalid_job_command)?
        else {
            return Ok(None);
        };
        self.persist_event(tracedb_jobs::JobEvent::leased(
            job.job_id.clone(),
            worker_id,
            job.lease_token.clone().unwrap_or_default(),
            job.lease_expires_at_ms
                .unwrap_or(now_ms.saturating_add(lease_ms)),
        ))?;
        self.persist_checkpoint()?;
        Ok(Some(job))
    }

    fn heartbeat(
        &mut self,
        job_id: &str,
        lease_token: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        let job = self
            .catalog
            .heartbeat(job_id, lease_token, now_ms, lease_ms)
            .map_err(invalid_job_command)?;
        self.persist_event(tracedb_jobs::JobEvent::Heartbeat {
            job_id: job_id.to_string(),
            lease_token: lease_token.to_string(),
            lease_expires_at_ms: job
                .lease_expires_at_ms
                .unwrap_or(now_ms.saturating_add(lease_ms)),
        })?;
        self.persist_checkpoint()?;
        Ok(job)
    }

    fn complete(
        &mut self,
        job_id: &str,
        lease_token: &str,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        let job = self
            .catalog
            .complete(job_id, Some(lease_token))
            .map_err(invalid_job_command)?;
        self.persist_event(tracedb_jobs::JobEvent::completed(
            job_id.to_string(),
            lease_token.to_string(),
        ))?;
        self.persist_checkpoint()?;
        Ok(job)
    }

    fn fail(
        &mut self,
        job_id: &str,
        lease_token: Option<&str>,
        error: &str,
        permanent: bool,
        now_ms: u64,
    ) -> std::io::Result<tracedb_jobs::TraceJob> {
        let job = self
            .catalog
            .fail(job_id, lease_token, error, permanent, now_ms)
            .map_err(invalid_job_command)?;
        self.persist_event(tracedb_jobs::JobEvent::Failed {
            job_id: job_id.to_string(),
            lease_token: lease_token.map(str::to_string),
            error: error.to_string(),
            permanent,
            next_attempt_after_ms: now_ms,
        })?;
        self.persist_checkpoint()?;
        Ok(job)
    }

    fn replay_events(&mut self) -> std::io::Result<()> {
        if !self.events_path.exists() {
            return Ok(());
        }
        let body = fs::read_to_string(&self.events_path)?;
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            let event =
                serde_json::from_str::<tracedb_jobs::JobEvent>(line).map_err(to_io_error)?;
            self.catalog
                .apply_event(event)
                .map_err(invalid_job_command)?;
        }
        Ok(())
    }

    fn persist_event(&self, event: tracedb_jobs::JobEvent) -> std::io::Result<()> {
        use std::io::Write as _;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.events_path)?;
        let body = serde_json::to_string(&event).map_err(to_io_error)?;
        writeln!(file, "{body}")?;
        file.sync_all()
    }

    fn persist_checkpoint(&self) -> std::io::Result<()> {
        let checkpoint = ServerJobCheckpoint {
            format_version: 1,
            shard: ServerJobShardReceipt {
                database_id: self.shard.database_id.clone(),
                branch_id: self.shard.branch_id.clone(),
            },
            catalog: self.catalog.clone(),
        };
        let body = serde_json::to_vec_pretty(&checkpoint).map_err(to_io_error)?;
        let tmp_path = self.checkpoint_path.with_extension("json.tmp");
        fs::write(&tmp_path, body)?;
        let tmp = fs::File::open(&tmp_path)?;
        tmp.sync_all()?;
        fs::rename(&tmp_path, &self.checkpoint_path)?;
        if let Some(parent) = self.checkpoint_path.parent() {
            let parent = fs::File::open(parent)?;
            parent.sync_all()?;
        }
        Ok(())
    }
}

fn job_runtime_dir(root_dir: &Path, shard: &ShardKey) -> PathBuf {
    if is_default_shard_key(shard) {
        root_dir.join("jobs")
    } else {
        shard_path(root_dir, shard).join("jobs")
    }
}

fn invalid_job_command(error: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
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
    #[serde(default)]
    variables: Value,
    #[serde(default, alias = "operationName")]
    operation_name: Option<String>,
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

/// Start a blocking HTTP server.
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

/// Start an async HTTP server.
pub async fn serve_async(
    db_path: impl AsRef<Path>,
    bind: impl AsRef<str>,
    config: EngineServerConfig,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind.as_ref()).await?;
    serve_tokio_listener(db_path, listener, config).await
}

/// Start an async HTTP server on an existing Tokio listener.
async fn security_headers<B>(mut response: Response<B>) -> Response<B> {
    response.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-content-type-options"),
        axum::http::HeaderValue::from_static("nosniff"),
    );
    response.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-frame-options"),
        axum::http::HeaderValue::from_static("DENY"),
    );
    response.headers_mut().insert(
        axum::http::header::HeaderName::from_static("referrer-policy"),
        axum::http::HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    response
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
        .layer(axum::middleware::map_response(security_headers))
        .with_state(state);
    let shutdown = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("failed to install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = sigint.recv() => {},
        }
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
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
    let request_id = header_value(request, "x-request-id")
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
    let engine = engine.for_actor(&actor).await?;
    let body_hash = stable_body_hash(body.as_bytes());
    let idempotency_cache_key = header_value(request, "idempotency-key")
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
            traceql_response(&engine, actor, request, request_start, read_ms, parse_start).await?
        }
        ("POST", "/v1/graphql") => {
            let parse_start = Instant::now();
            let request: GraphQlQueryRequest = serde_json::from_str(body).map_err(to_io_error)?;
            native_graphql_response(&engine, actor, request, request_start, read_ms, parse_start)
                .await?
        }
        ("POST", "/v1/graphql/bounded") => {
            let parse_start = Instant::now();
            let request: GraphQlQueryRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let query = graphql_query_from_str(&request.query).map_err(to_io_error)?;
            let parse_ms = elapsed_ms(parse_start);
            query_response(&engine, actor, query, request_start, read_ms, parse_ms).await?
        }
        ("GET", "/v1/graphql/schema") => engine.graphql_schema_response().await?,
        ("GET", "/v1/graphql/bounded/schema") => engine.graphql_schema_response().await?,
        ("POST", "/v1/explain") => {
            let parse_start = Instant::now();
            let mut query: HybridQuery = serde_json::from_str(body).map_err(to_io_error)?;
            query.explain = true;
            let parse_ms = elapsed_ms(parse_start);
            let actor = actor.clone();
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
            if target.contains("..") {
                return Ok(bad_request(
                    "snapshot target must not contain parent directory references".to_string(),
                ));
            }
            if let Err(error) = engine.create_snapshot(target).await {
                return Ok(bad_request(error.to_string()));
            }
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
            if source.contains("..") {
                return Ok(bad_request(
                    "restore source must not contain parent directory references".to_string(),
                ));
            }
            if target.contains("..") {
                return Ok(bad_request(
                    "restore target must not contain parent directory references".to_string(),
                ));
            }
            let restored = match TraceDb::restore_snapshot(source, target) {
                Ok(restored) => restored,
                Err(error) => return Ok(bad_request(error.to_string())),
            };
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
            | ("POST", "/v1/graphql")
            | ("POST", "/v1/traceql")
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
            | ("POST", "/v1/graphql")
            | ("POST", "/v1/traceql")
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
    actor: ActorContext,
    query: HybridQuery,
    request_start: Instant,
    read_ms: f64,
    parse_ms: f64,
) -> std::io::Result<String> {
    let include_explain = query.explain;
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

async fn traceql_response(
    engine: &EngineHandle,
    actor: ActorContext,
    request: TraceQlQueryRequest,
    request_start: Instant,
    read_ms: f64,
    parse_start: Instant,
) -> std::io::Result<String> {
    if let Some((command, payload)) = split_traceql_command(&request.query) {
        let value = execute_traceql_command(engine, &actor, command, payload).await?;
        return Ok(ok(value));
    }
    let query = traceql_query_from_str(&request.query).map_err(to_io_error)?;
    let parse_ms = elapsed_ms(parse_start);
    query_response(engine, actor, query, request_start, read_ms, parse_ms).await
}

async fn execute_traceql_command(
    engine: &EngineHandle,
    actor: &ActorContext,
    command: &str,
    payload: &str,
) -> std::io::Result<Value> {
    match command {
        "SCHEMA APPLY" => {
            let schema: TableSchema = serde_json::from_str(payload).map_err(to_io_error)?;
            let epoch = engine.apply_schema(schema).await?;
            Ok(json!({ "epoch": epoch.get() }))
        }
        "RECORD PUT" | "PUT" => {
            let input = parse_record_put_body(payload)?;
            let epoch = engine.put(RecordPutRequest::new(input)).await?;
            Ok(json!({ "epoch": epoch.get() }))
        }
        "RECORD BATCH" | "BATCH" => {
            let request: RecordPutBatchRequest =
                serde_json::from_str(payload).map_err(to_io_error)?;
            let record_count = request.records.len();
            let epoch = engine.put_batch(request).await?;
            Ok(json!({ "epoch": epoch.get(), "record_count": record_count }))
        }
        "RECORD PATCH" | "PATCH" => {
            let request: RecordPatchRequest = serde_json::from_str(payload).map_err(to_io_error)?;
            let epoch = engine.patch(request).await?;
            Ok(json!({ "epoch": epoch.get() }))
        }
        "RECORD DELETE" | "DELETE" => {
            let request: RecordDeleteRequest =
                serde_json::from_str(payload).map_err(to_io_error)?;
            let epoch = engine.delete(request).await?;
            Ok(json!({ "deleted": true, "epoch": epoch.get() }))
        }
        "RECORD GET" | "GET" => {
            let request: RecordGetRequest = serde_json::from_str(payload).map_err(to_io_error)?;
            let record = engine.get_as(actor, request).await?;
            Ok(json!({ "record": record }))
        }
        "RECORD SCAN" | "SCAN" => {
            let request: RecordScanRequest = serde_json::from_str(payload).map_err(to_io_error)?;
            let output = engine.scan_as(actor, request).await?;
            serde_json::to_value(output).map_err(to_io_error)
        }
        "QUERY" => {
            let query: HybridQuery = serde_json::from_str(payload).map_err(to_io_error)?;
            query_output_value(engine, actor, query).await
        }
        "EXPLAIN" => {
            let mut query: HybridQuery = serde_json::from_str(payload).map_err(to_io_error)?;
            query.explain = true;
            let value = query_output_value(engine, actor, query).await?;
            Ok(value
                .get("explain")
                .cloned()
                .unwrap_or_else(|| json!({ "explain": null })))
        }
        "ADMIN COMPACT" | "COMPACT" => {
            engine.compact().await?;
            Ok(json!({ "compacted": true }))
        }
        "ADMIN SNAPSHOT" | "SNAPSHOT" => {
            let value: Value = serde_json::from_str(payload).map_err(to_io_error)?;
            let target = required_str(&value, "target")?;
            if let Err(error) = engine.create_snapshot(target).await {
                return Ok(
                    json!({ "ok": false, "error": error.to_string(), "code": "bad_request" }),
                );
            }
            Ok(json!({ "snapshot": true, "target": target }))
        }
        "ADMIN RESTORE" | "RESTORE" => restore_value_from_payload(payload),
        "JOBS LIST" => {
            let jobs = engine.jobs().await?;
            Ok(json!({ "durable": true, "durable_jobs": jobs }))
        }
        "JOBS RUN" => {
            let value: Value = serde_json::from_str(payload).map_err(to_io_error)?;
            let kind = value
                .get("kind")
                .and_then(Value::as_str)
                .and_then(parse_job_kind)
                .unwrap_or(tracedb_jobs::JobKind::VerifyDatabase);
            let target = value
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("traceql-job")
                .to_string();
            let idempotency_key = value
                .get("idempotency_key")
                .and_then(Value::as_str)
                .unwrap_or(&target)
                .to_string();
            let job = engine.enqueue_job(kind, target, idempotency_key).await?;
            Ok(json!({ "job": job }))
        }
        _ => Err(to_io_error(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid TraceQL command: {command}"),
        ))),
    }
}

async fn native_graphql_response(
    engine: &EngineHandle,
    actor: ActorContext,
    request: GraphQlQueryRequest,
    _request_start: Instant,
    _read_ms: f64,
    _parse_start: Instant,
) -> std::io::Result<String> {
    let response = match execute_native_graphql(engine, &actor, &request).await {
        Ok((field, value)) => json!({ "data": { field: value } }),
        Err(message) => json!({
            "data": Value::Null,
            "errors": [graphql_error(message)],
        }),
    };
    Ok(ok(response))
}

async fn execute_native_graphql(
    engine: &EngineHandle,
    actor: &ActorContext,
    request: &GraphQlQueryRequest,
) -> std::result::Result<(String, Value), String> {
    parse_query(&request.query).map_err(|error| format!("invalid GraphQL: {error}"))?;
    let _operation_name = request.operation_name.as_deref();
    let field = graphql_root_field(&request.query).ok_or_else(|| {
        "invalid GraphQL: exactly one root field is required for TraceDB operations".to_string()
    })?;
    let value = match field.as_str() {
        "schemaApply" | "applySchema" => {
            let schema: TableSchema = graphql_input_json(request, &field).and_then(|value| {
                serde_json::from_value(value).map_err(|error| error.to_string())
            })?;
            let epoch = engine
                .apply_schema(schema)
                .await
                .map_err(|error| error.to_string())?;
            json!({ "epoch": epoch.get() })
        }
        "put" => {
            let input = graphql_input_string(request, &field)
                .and_then(|body| parse_record_put_body(&body).map_err(|error| error.to_string()))?;
            let epoch = engine
                .put(RecordPutRequest::new(input))
                .await
                .map_err(|error| error.to_string())?;
            json!({ "epoch": epoch.get() })
        }
        "batch" => {
            let request_body: RecordPutBatchRequest = graphql_input_json(request, &field)
                .and_then(|value| {
                    serde_json::from_value(value).map_err(|error| error.to_string())
                })?;
            let record_count = request_body.records.len();
            let epoch = engine
                .put_batch(request_body)
                .await
                .map_err(|error| error.to_string())?;
            json!({ "epoch": epoch.get(), "record_count": record_count })
        }
        "patch" => {
            let request_body: RecordPatchRequest =
                graphql_input_json(request, &field).and_then(|value| {
                    serde_json::from_value(value).map_err(|error| error.to_string())
                })?;
            let epoch = engine
                .patch(request_body)
                .await
                .map_err(|error| error.to_string())?;
            json!({ "epoch": epoch.get() })
        }
        "delete" => {
            let request_body: RecordDeleteRequest =
                graphql_input_json(request, &field).and_then(|value| {
                    serde_json::from_value(value).map_err(|error| error.to_string())
                })?;
            let epoch = engine
                .delete(request_body)
                .await
                .map_err(|error| error.to_string())?;
            json!({ "deleted": true, "epoch": epoch.get() })
        }
        "get" => {
            let request_body: RecordGetRequest =
                graphql_input_json(request, &field).and_then(|value| {
                    serde_json::from_value(value).map_err(|error| error.to_string())
                })?;
            let record = engine
                .get_as(actor, request_body)
                .await
                .map_err(|error| error.to_string())?;
            json!({ "record": record })
        }
        "scan" => {
            let request_body: RecordScanRequest =
                graphql_input_json(request, &field).and_then(|value| {
                    serde_json::from_value(value).map_err(|error| error.to_string())
                })?;
            serde_json::to_value(
                engine
                    .scan_as(actor, request_body)
                    .await
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?
        }
        "query" => {
            let query: HybridQuery = graphql_input_json(request, &field).and_then(|value| {
                serde_json::from_value(value).map_err(|error| error.to_string())
            })?;
            query_output_value(engine, actor, query)
                .await
                .map_err(|error| error.to_string())?
        }
        "explain" => {
            let mut query: HybridQuery = graphql_input_json(request, &field).and_then(|value| {
                serde_json::from_value(value).map_err(|error| error.to_string())
            })?;
            query.explain = true;
            query_output_value(engine, actor, query)
                .await
                .map_err(|error| error.to_string())?
                .get("explain")
                .cloned()
                .unwrap_or_else(|| json!({ "explain": null }))
        }
        "compact" => {
            engine.compact().await.map_err(|error| error.to_string())?;
            json!({ "compacted": true })
        }
        "snapshot" => {
            let value = graphql_input_json(request, &field)?;
            let target = value
                .get("target")
                .and_then(Value::as_str)
                .ok_or_else(|| "snapshot target is required".to_string())?;
            engine
                .create_snapshot(target)
                .await
                .map_err(|error| error.to_string())?;
            json!({ "snapshot": true, "target": target })
        }
        "restore" => {
            let body = graphql_input_string(request, &field)?;
            restore_value_from_payload(&body).map_err(|error| error.to_string())?
        }
        "jobs" => {
            let jobs = engine.jobs().await.map_err(|error| error.to_string())?;
            json!({ "durable": true, "durable_jobs": jobs })
        }
        "jobRun" | "runJob" => {
            let value = graphql_input_json(request, &field)?;
            let kind = value
                .get("kind")
                .and_then(Value::as_str)
                .and_then(parse_job_kind)
                .unwrap_or(tracedb_jobs::JobKind::VerifyDatabase);
            let target = value
                .get("target")
                .and_then(Value::as_str)
                .unwrap_or("graphql-job")
                .to_string();
            let idempotency_key = value
                .get("idempotency_key")
                .and_then(Value::as_str)
                .unwrap_or(&target)
                .to_string();
            let job = engine
                .enqueue_job(kind, target, idempotency_key)
                .await
                .map_err(|error| error.to_string())?;
            json!({ "job": job })
        }
        _ => return Err(format!("unsupported TraceDB GraphQL field {field}")),
    };
    Ok((field, value))
}

async fn query_output_value(
    engine: &EngineHandle,
    actor: &ActorContext,
    query: HybridQuery,
) -> std::io::Result<Value> {
    let include_explain = query.explain;
    let (timed_output, _) = engine.query_with_timing_as(actor, query).await?;
    let output = timed_output.output;
    if include_explain {
        serde_json::to_value(output).map_err(to_io_error)
    } else {
        let mut value = json!({ "results": output.results });
        if let Some(next_cursor) = output.next_cursor {
            value["next_cursor"] = json!(next_cursor);
        }
        Ok(value)
    }
}

fn split_traceql_command(input: &str) -> Option<(&str, &str)> {
    let trimmed = input.trim();
    for command in [
        "SCHEMA APPLY",
        "RECORD PUT",
        "RECORD BATCH",
        "RECORD PATCH",
        "RECORD DELETE",
        "RECORD GET",
        "RECORD SCAN",
        "ADMIN COMPACT",
        "ADMIN SNAPSHOT",
        "ADMIN RESTORE",
        "JOBS LIST",
        "JOBS RUN",
        "EXPLAIN",
        "QUERY",
        "PUT",
        "BATCH",
        "PATCH",
        "DELETE",
        "GET",
        "SCAN",
        "COMPACT",
        "SNAPSHOT",
        "RESTORE",
    ] {
        if trimmed.eq_ignore_ascii_case(command) {
            return Some((command, "{}"));
        }
        if trimmed.len() > command.len()
            && trimmed[..command.len()].eq_ignore_ascii_case(command)
            && trimmed.as_bytes()[command.len()].is_ascii_whitespace()
        {
            let payload = trimmed[command.len()..].trim();
            if command == "EXPLAIN" && starts_with_sqlish_select_payload(payload) {
                continue;
            }
            return Some((command, payload));
        }
    }
    None
}

fn starts_with_sqlish_select_payload(input: &str) -> bool {
    let trimmed = input.trim_start();
    trimmed
        .get(.."SELECT".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT"))
        && trimmed["SELECT".len()..]
            .chars()
            .next()
            .is_none_or(char::is_whitespace)
}

fn restore_value_from_payload(payload: &str) -> std::io::Result<Value> {
    let value: Value = serde_json::from_str(payload).map_err(to_io_error)?;
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
    Ok(response)
}

fn graphql_error(message: impl Into<String>) -> Value {
    json!({
        "message": message.into(),
        "extensions": {
            "code": "TRACEDB_GRAPHQL_ERROR",
        },
    })
}

fn graphql_root_field(query: &str) -> Option<String> {
    let body_start = query.find('{')?;
    let body = &query[body_start + 1..];
    let mut chars = body.trim_start().chars().peekable();
    let mut field = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            field.push(ch);
            let _ = chars.next();
        } else {
            break;
        }
    }
    if field.is_empty() {
        None
    } else {
        Some(field)
    }
}

fn graphql_input_json(
    request: &GraphQlQueryRequest,
    field: &str,
) -> std::result::Result<Value, String> {
    let input = graphql_input_string(request, field)?;
    serde_json::from_str(&input).map_err(|error| format!("invalid {field} input JSON: {error}"))
}

fn graphql_input_string(
    request: &GraphQlQueryRequest,
    field: &str,
) -> std::result::Result<String, String> {
    let raw = graphql_arg_raw(&request.query, field, "input")
        .ok_or_else(|| format!("{field} input argument is required"))?;
    if let Some(variable_name) = raw.strip_prefix('$') {
        let variable = request
            .variables
            .get(variable_name)
            .ok_or_else(|| format!("GraphQL variable ${variable_name} is required"))?;
        return match variable {
            Value::String(value) => Ok(value.clone()),
            other => Ok(other.to_string()),
        };
    }
    serde_json::from_str::<String>(&raw)
        .map_err(|error| format!("{field} input must be a GraphQL string: {error}"))
}

fn graphql_arg_raw(query: &str, field: &str, arg_name: &str) -> Option<String> {
    let field_index = query.find(field)?;
    let after_field = &query[field_index + field.len()..];
    let open = after_field.find('(')?;
    let args = matching_delimited(&after_field[open..], '(', ')')?;
    split_top_level(&args, ',').into_iter().find_map(|part| {
        let (name, value) = part.split_once(':')?;
        (name.trim() == arg_name).then(|| value.trim().to_string())
    })
}

fn matching_delimited(input: &str, open: char, close: char) -> Option<String> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut started = false;
    let mut out = String::new();
    for ch in input.chars() {
        if !started {
            if ch == open {
                started = true;
                depth = 1;
            }
            continue;
        }
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            out.push(ch);
            continue;
        }
        if ch == open {
            depth += 1;
            out.push(ch);
            continue;
        }
        if ch == close {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(out);
            }
            out.push(ch);
            continue;
        }
        out.push(ch);
    }
    None
}

fn split_top_level(input: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ if ch == separator && depth == 0 => {
                parts.push(input[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(input[start..].trim());
    parts
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
    if let Some(field) = graphql_root_field(query) {
        let request = GraphQlQueryRequest {
            query: query.to_string(),
            variables: value
                .and_then(|body| body.get("variables"))
                .cloned()
                .unwrap_or(Value::Null),
            operation_name: value
                .and_then(|body| {
                    body.get("operation_name")
                        .or_else(|| body.get("operationName"))
                })
                .and_then(Value::as_str)
                .map(str::to_string),
        };
        if let Ok(input) = graphql_input_json(&request, &field) {
            if let Some(tenant_id) = json_string(&input, "tenant_id")
                .or_else(|| json_string(&input, "tenant"))
                .or_else(|| {
                    input
                        .get("record")
                        .and_then(|record| json_string(record, "tenant_id"))
                })
                .or_else(|| first_record_json_string(Some(&input), "tenant_id"))
            {
                return Some(tenant_id);
            }
        }
    }
    let marker = "tenant_id:";
    let after_marker = query.split(marker).nth(1)?.trim_start();
    quoted_prefix(after_marker)
}

fn traceql_tenant_from_body(value: Option<&Value>) -> Option<String> {
    let query = value?.get("query")?.as_str()?;
    if let Some((_, payload)) = split_traceql_command(query) {
        if let Ok(value) = serde_json::from_str::<Value>(payload) {
            if let Some(tenant_id) = json_string(&value, "tenant_id")
                .or_else(|| json_string(&value, "tenant"))
                .or_else(|| {
                    value
                        .get("record")
                        .and_then(|record| json_string(record, "tenant_id"))
                })
                .or_else(|| first_record_json_string(Some(&value), "tenant_id"))
            {
                return Some(tenant_id);
            }
        }
    }
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

fn bool_env(name: &str, default_value: bool) -> bool {
    std::env::var(name)
        .map(|value| matches!(value.as_str(), "true" | "1" | "yes" | "on"))
        .unwrap_or(default_value)
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

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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

    fn test_idempotency_cache() -> IdempotencyCache {
        Arc::new(Mutex::new(IdempotencyCacheState {
            entries: HashMap::new(),
        }))
    }

    fn json_body(response: &str) -> Value {
        let body = response
            .split("\r\n\r\n")
            .nth(1)
            .or_else(|| response.split("\n\n").nth(1))
            .expect("response body");
        serde_json::from_str(body).expect("json response body")
    }

    fn http_post(path: &str, body: &str) -> String {
        format!(
            "POST {path} HTTP/1.1\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn http_post_with_engine_token(path: &str, body: &str) -> String {
        format!(
            "POST {path} HTTP/1.1\r\ncontent-type: application/json\r\nx-tracedb-engine-token: engine-secret\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        )
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
    async fn engine_handle_physically_isolates_database_branch_shards() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = EngineHandle::open(temp.path()).expect("engine handle");
        let actor_a = ActorContext::managed_request(
            "tenant-a",
            "db_a",
            "db_a:main",
            "token-a",
            "request-a",
            0,
            vec!["records:read".to_string(), "records:write".to_string()],
        );
        let actor_b = ActorContext::managed_request(
            "tenant-a",
            "db_b",
            "db_b:main",
            "token-b",
            "request-b",
            0,
            vec!["records:read".to_string(), "records:write".to_string()],
        );
        let shard_a = root.for_actor(&actor_a).await.expect("shard a");
        let shard_b = root.for_actor(&actor_b).await.expect("shard b");

        shard_a
            .apply_schema(runtime_test_schema())
            .await
            .expect("schema a");
        shard_b
            .apply_schema(runtime_test_schema())
            .await
            .expect("schema b");
        shard_a
            .put(RecordPutRequest::new(runtime_test_record(
                "same",
                "from shard a",
            )))
            .await
            .expect("put a");
        shard_b
            .put(RecordPutRequest::new(runtime_test_record(
                "same",
                "from shard b",
            )))
            .await
            .expect("put b");

        let read_a = shard_a
            .get_as(&actor_a, RecordGetRequest::new("docs", "tenant-a", "same"))
            .await
            .expect("get a")
            .expect("record a");
        let read_b = shard_b
            .get_as(&actor_b, RecordGetRequest::new("docs", "tenant-a", "same"))
            .await
            .expect("get b")
            .expect("record b");

        assert_eq!(read_a.fields["body"], json!("from shard a"));
        assert_eq!(read_b.fields["body"], json!("from shard b"));
        assert!(temp
            .path()
            .join("shards")
            .join("db_a")
            .join("db_a_3amain")
            .join("shard.receipt.json")
            .exists());
        assert!(temp
            .path()
            .join("shards")
            .join("db_b")
            .join("db_b_3amain")
            .join("shard.receipt.json")
            .exists());
    }

    #[tokio::test]
    async fn job_catalog_does_not_wait_on_data_plane_read_lock_and_replays() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = EngineHandle::open(temp.path()).expect("engine handle");
        let _read_snapshot = engine.hold_read_snapshot_for_test().await;

        let job = tokio::time::timeout(
            Duration::from_millis(50),
            engine.enqueue_job(
                tracedb_jobs::JobKind::CompactSegment,
                "segment:seg-1",
                "compact:seg-1",
            ),
        )
        .await
        .expect("job enqueue should not wait behind data-plane read lock")
        .expect("enqueue");
        assert_eq!(job.status, tracedb_jobs::JobStatus::Queued);
        drop(_read_snapshot);

        let leased = engine
            .lease_job(
                tracedb_jobs::WorkerId::new("worker-1"),
                tracedb_jobs::JobKind::CompactSegment,
                30_000,
            )
            .await
            .expect("lease")
            .expect("leased job");
        assert_eq!(leased.job_id, job.job_id);
        assert_eq!(leased.status, tracedb_jobs::JobStatus::Leased);
        drop(engine);

        let reopened = EngineHandle::open(temp.path()).expect("reopen engine");
        let jobs = reopened.jobs().await.expect("jobs after reopen");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, job.job_id);
        assert_eq!(jobs[0].status, tracedb_jobs::JobStatus::Leased);
        assert_eq!(
            jobs[0].lease_owner,
            Some(tracedb_jobs::WorkerId::new("worker-1"))
        );
        assert!(temp.path().join("jobs").join("catalog.json").exists());
    }

    #[test]
    fn native_graphql_returns_data_errors_and_preserves_bounded_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = EngineHandle::open(temp.path()).expect("engine handle");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            engine
                .apply_schema(runtime_test_schema())
                .await
                .expect("schema");
            engine
                .put(RecordPutRequest::new(runtime_test_record(
                    "intro",
                    "native graphql body",
                )))
                .await
                .expect("put");
        });
        let query_input = json!({
            "table": "docs",
            "tenant_id": "tenant-a",
            "text_field": "body",
            "text": "native",
            "top_k": 2,
            "freshness": "Strict",
            "explain": false,
        })
        .to_string();
        let graphql = format!(
            "query {{ query(input: {}) {{ results }} }}",
            serde_json::to_string(&query_input).expect("quoted gql input")
        );
        let body = json!({ "query": graphql }).to_string();

        let response = handle_request_text_for_test(
            &http_post("/v1/graphql", &body),
            engine.clone(),
            test_idempotency_cache(),
            EngineServerConfig::default(),
        )
        .expect("native graphql response");
        let payload = json_body(&response);
        assert!(payload.get("errors").is_none(), "payload: {payload}");
        assert_eq!(
            payload["data"]["query"]["results"][0]["record_id"],
            json!("intro")
        );

        let bounded_query =
            r#"query { docs(tenant_id: "tenant-a", text: "native", limit: 2) { id } }"#;
        let bounded_body = json!({ "query": bounded_query }).to_string();
        let bounded = handle_request_text_for_test(
            &http_post("/v1/graphql/bounded", &bounded_body),
            engine.clone(),
            test_idempotency_cache(),
            EngineServerConfig::default(),
        )
        .expect("bounded graphql response");
        let bounded_payload = json_body(&bounded);
        assert!(
            bounded_payload.get("results").is_some(),
            "bounded path should keep TraceDB QueryResponse shape: {bounded_payload}"
        );
        assert!(
            bounded_payload.get("data").is_none(),
            "bounded path should not be native GraphQL envelope: {bounded_payload}"
        );

        let invalid_body =
            json!({ "query": "query { unsupported(input: \"{}\") { ok } }" }).to_string();
        let invalid = handle_request_text_for_test(
            &http_post("/v1/graphql", &invalid_body),
            engine,
            test_idempotency_cache(),
            EngineServerConfig::default(),
        )
        .expect("graphql errors response");
        let invalid_payload = json_body(&invalid);
        assert!(invalid_payload["data"].is_null());
        assert!(invalid_payload["errors"][0]["message"]
            .as_str()
            .expect("message")
            .contains("unsupported TraceDB GraphQL field"));
    }

    #[test]
    fn traceql_command_statements_execute_record_operations() {
        let temp = tempfile::tempdir().expect("tempdir");
        let engine = EngineHandle::open(temp.path()).expect("engine handle");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            engine
                .apply_schema(runtime_test_schema())
                .await
                .expect("schema");
            engine
                .put(RecordPutRequest::new(runtime_test_record(
                    "traceql",
                    "command body",
                )))
                .await
                .expect("put");
        });
        let command = format!(
            "RECORD GET {}",
            json!({
                "table": "docs",
                "tenant_id": "tenant-a",
                "id": "traceql",
            })
        );
        let body = json!({ "query": command }).to_string();

        let response = handle_request_text_for_test(
            &http_post("/v1/traceql", &body),
            engine,
            test_idempotency_cache(),
            EngineServerConfig::default(),
        )
        .expect("traceql command response");
        let payload = json_body(&response);
        assert_eq!(payload["record"]["id"], json!("traceql"));
    }

    #[test]
    fn snapshot_and_restore_require_managed_root_when_configured() {
        let _env_guard = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::tempdir().expect("tempdir");
        let admin_root = temp.path().join("admin-snapshots");
        std::fs::create_dir_all(&admin_root).expect("admin root");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&outside).expect("outside root");
        std::env::set_var("TRACEDB_ADMIN_SNAPSHOT_ROOT", &admin_root);
        let engine = EngineHandle::open(temp.path().join("db")).expect("engine handle");
        let config = EngineServerConfig::default().with_internal_token("engine-secret");

        let bad_snapshot = json!({ "target": outside.join("snapshot") }).to_string();
        let snapshot_response = handle_request_text_for_test(
            &http_post_with_engine_token("/v1/admin/snapshot", &bad_snapshot),
            engine.clone(),
            test_idempotency_cache(),
            config.clone(),
        )
        .expect("snapshot response");
        assert!(
            snapshot_response.starts_with("HTTP/1.1 400 Bad Request"),
            "snapshot outside managed root must fail: {snapshot_response}"
        );

        let source = admin_root.join("snapshot-ok");
        let snapshot_ok = json!({ "target": source }).to_string();
        let snapshot_ok_response = handle_request_text_for_test(
            &http_post_with_engine_token("/v1/admin/snapshot", &snapshot_ok),
            engine.clone(),
            test_idempotency_cache(),
            config.clone(),
        )
        .expect("snapshot ok response");
        assert!(
            snapshot_ok_response.starts_with("HTTP/1.1 200 OK"),
            "snapshot inside managed root must pass: {snapshot_ok_response}"
        );

        let bad_restore = json!({
            "source": source,
            "target": outside.join("restore")
        })
        .to_string();
        let restore_response = handle_request_text_for_test(
            &http_post_with_engine_token("/v1/admin/restore", &bad_restore),
            engine.clone(),
            test_idempotency_cache(),
            config.clone(),
        )
        .expect("restore response");
        assert!(
            restore_response.starts_with("HTTP/1.1 400 Bad Request"),
            "restore outside managed root must fail: {restore_response}"
        );

        let traceql_snapshot = json!({
            "query": format!(
                "ADMIN SNAPSHOT {}",
                json!({ "target": outside.join("traceql-snapshot") })
            )
        })
        .to_string();
        let traceql_response = handle_request_text_for_test(
            &http_post_with_engine_token("/v1/traceql", &traceql_snapshot),
            engine.clone(),
            test_idempotency_cache(),
            config.clone(),
        )
        .expect("traceql snapshot response");
        assert!(
            traceql_response.contains("TRACEDB_ADMIN_SNAPSHOT_ROOT"),
            "TraceQL snapshot should use shared snapshot-root policy: {traceql_response}"
        );

        let graphql_input = json!({ "target": outside.join("graphql-snapshot") }).to_string();
        let graphql_body = json!({
            "query": format!(
                "mutation {{ snapshot(input: {}) {{ snapshot }} }}",
                serde_json::to_string(&graphql_input).expect("quoted input")
            )
        })
        .to_string();
        let graphql_response = handle_request_text_for_test(
            &http_post_with_engine_token("/v1/graphql", &graphql_body),
            engine,
            test_idempotency_cache(),
            config,
        )
        .expect("graphql snapshot response");
        let graphql_payload = json_body(&graphql_response);
        assert!(
            graphql_payload["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("TRACEDB_ADMIN_SNAPSHOT_ROOT"),
            "GraphQL snapshot should use shared snapshot-root policy: {graphql_payload}"
        );
        std::env::remove_var("TRACEDB_ADMIN_SNAPSHOT_ROOT");
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
