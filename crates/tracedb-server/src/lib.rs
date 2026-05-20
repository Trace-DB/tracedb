#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracedb_query::{
    HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput, RecordPatchRequest,
    RecordPutBatchRequest, RecordPutRequest, RecordScanRequest, TableSchema, TraceDb,
};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

type IdempotencyCache = Arc<Mutex<IdempotencyCacheState>>;

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
    let (db, idempotency_cache) = open_server_state(db_path)?;
    let listener = TcpListener::bind(bind)?;
    for stream in listener.incoming() {
        spawn_handler(stream?, Arc::clone(&db), Arc::clone(&idempotency_cache));
    }
    Ok(())
}

pub fn serve_with_shutdown(
    db_path: impl AsRef<Path>,
    bind: &str,
    should_shutdown: impl Fn() -> bool,
) -> std::io::Result<()> {
    let (db, idempotency_cache) = open_server_state(db_path)?;
    let listener = TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;
    while !should_shutdown() {
        match listener.accept() {
            Ok((stream, _)) => {
                spawn_handler(stream, Arc::clone(&db), Arc::clone(&idempotency_cache));
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

fn spawn_handler(stream: TcpStream, db: Arc<Mutex<TraceDb>>, idempotency_cache: IdempotencyCache) {
    thread::spawn(move || {
        let _ = handle(stream, db, idempotency_cache);
    });
}

fn handle(
    mut stream: TcpStream,
    db: Arc<Mutex<TraceDb>>,
    idempotency_cache: IdempotencyCache,
) -> std::io::Result<()> {
    let response = match handle_inner(&mut stream, db, idempotency_cache) {
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
) -> std::io::Result<String> {
    let request_start = Instant::now();
    let request = read_request(stream)?;
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
    let body = request
        .split("\r\n\r\n")
        .nth(1)
        .or_else(|| request.split("\n\n").nth(1))
        .unwrap_or_default();
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
            let record = db.lock().unwrap().get(request).map_err(to_io_error)?;
            ok(json!({ "record": record }))
        }
        ("POST", "/v1/records/scan") => {
            let request: RecordScanRequest = serde_json::from_str(body).map_err(to_io_error)?;
            let output = db.lock().unwrap().scan(request).map_err(to_io_error)?;
            ok(serde_json::to_value(output).map_err(to_io_error)?)
        }
        ("POST", "/v1/query") => {
            let parse_start = Instant::now();
            let query: HybridQuery = serde_json::from_str(body).map_err(to_io_error)?;
            let include_explain = query.explain;
            let parse_ms = elapsed_ms(parse_start);
            let lock_start = Instant::now();
            let guard = db.lock().unwrap();
            let lock_wait_ms = elapsed_ms(lock_start);
            let engine_start = Instant::now();
            let timed_output = guard.query_with_timing(query).map_err(to_io_error)?;
            let engine_ms = elapsed_ms(engine_start);
            let query_timing = timed_output.timing;
            drop(guard);
            let output = timed_output.output;
            let response_shape_start = Instant::now();
            let value = if include_explain {
                serde_json::to_value(output).map_err(to_io_error)?
            } else {
                json!({ "results": output.results })
            };
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
        ("POST", "/v1/explain") => {
            let parse_start = Instant::now();
            let mut query: HybridQuery = serde_json::from_str(body).map_err(to_io_error)?;
            query.explain = true;
            let parse_ms = elapsed_ms(parse_start);
            let lock_start = Instant::now();
            let guard = db.lock().unwrap();
            let lock_wait_ms = elapsed_ms(lock_start);
            let engine_start = Instant::now();
            let timed_output = guard.query_with_timing(query).map_err(to_io_error)?;
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
            TraceDb::restore_snapshot(source, target).map_err(to_io_error)?;
            ok(json!({ "restored": true, "source": source, "target": target }))
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

fn parse_record_put_body(body: &str) -> std::io::Result<RecordInput> {
    let value: Value = serde_json::from_str(body).map_err(to_io_error)?;
    if value.get("record").is_some() {
        let request: RecordPutRequest = serde_json::from_value(value).map_err(to_io_error)?;
        Ok(request.record)
    } else {
        serde_json::from_value(value).map_err(to_io_error)
    }
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

fn header_value<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().skip(1).find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then(|| value.trim())
    })
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
