#![forbid(unsafe_code)]

use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use tracedb_query::{
    HybridQuery, RecordDeleteRequest, RecordGetRequest, RecordInput, RecordPatchRequest,
    RecordPutBatchRequest, RecordPutRequest, RecordScanRequest, TableSchema, TraceDb,
};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub fn serve(db_path: impl AsRef<Path>, bind: &str) -> std::io::Result<()> {
    let db = TraceDb::open(db_path).map_err(to_io_error)?;
    let db = Arc::new(Mutex::new(db));
    let listener = TcpListener::bind(bind)?;
    for stream in listener.incoming() {
        let stream = stream?;
        let db = Arc::clone(&db);
        thread::spawn(move || {
            let _ = handle(stream, db);
        });
    }
    Ok(())
}

fn handle(mut stream: TcpStream, db: Arc<Mutex<TraceDb>>) -> std::io::Result<()> {
    let response = match handle_inner(&mut stream, db) {
        Ok(response) => response,
        Err(error) => bad_request(error.to_string()),
    };
    stream.write_all(response.as_bytes())?;
    stream.flush()
}

fn handle_inner(stream: &mut TcpStream, db: Arc<Mutex<TraceDb>>) -> std::io::Result<String> {
    let request_start = Instant::now();
    let request = read_request(stream)?;
    let read_ms = elapsed_ms(request_start);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let request_id = header_value(&request, "x-request-id")
        .map(str::to_string)
        .unwrap_or_else(next_request_id);
    log_request("tracedb-engine", &request_id, method, path);
    let body = request
        .split("\r\n\r\n")
        .nth(1)
        .or_else(|| request.split("\n\n").nth(1))
        .unwrap_or_default();

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
            let epoch = db.lock().unwrap().put_batch(request).map_err(to_io_error)?;
            ok(json!({ "epoch": epoch.get(), "record_count": record_count }))
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
            let parse_ms = elapsed_ms(parse_start);
            let lock_start = Instant::now();
            let guard = db.lock().unwrap();
            let lock_wait_ms = elapsed_ms(lock_start);
            let engine_start = Instant::now();
            let output = guard.query(query).map_err(to_io_error)?;
            let engine_ms = elapsed_ms(engine_start);
            drop(guard);
            let encode_start = Instant::now();
            let value = serde_json::to_value(output).map_err(to_io_error)?;
            let value_encode_ms = elapsed_ms(encode_start);
            ok_timed(
                value,
                request_start,
                value_encode_ms,
                &[
                    ("read", read_ms),
                    ("parse", parse_ms),
                    ("lock_wait", lock_wait_ms),
                    ("engine", engine_ms),
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
            let output = guard.query(query).map_err(to_io_error)?;
            let engine_ms = elapsed_ms(engine_start);
            drop(guard);
            let encode_start = Instant::now();
            let value = serde_json::to_value(output.explain).map_err(to_io_error)?;
            let value_encode_ms = elapsed_ms(encode_start);
            ok_timed(
                value,
                request_start,
                value_encode_ms,
                &[
                    ("read", read_ms),
                    ("parse", parse_ms),
                    ("lock_wait", lock_wait_ms),
                    ("engine", engine_ms),
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
    Ok(response)
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
    prior_encode_ms: f64,
    timings: &[(&str, f64)],
) -> String {
    let encode_start = Instant::now();
    let body = value.to_string();
    let encode_ms = prior_encode_ms + elapsed_ms(encode_start);
    let prewrite_total_ms = elapsed_ms(request_start);
    let mut all_timings = timings.to_vec();
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
    let body = json!({ "error": "not found" }).to_string();
    format!(
        "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn bad_request(message: String) -> String {
    let body = json!({ "error": message }).to_string();
    format!(
        "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
}

fn to_io_error(error: impl std::error::Error) -> std::io::Error {
    std::io::Error::other(error.to_string())
}
