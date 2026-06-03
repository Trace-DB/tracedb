#![forbid(unsafe_code)]

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use tracedb_jobs::{JobCatalog, JobKind, WorkerId};

#[tokio::main]
async fn main() {
    init_json_tracing("info");
    if std::env::var("TRACEDB_WORKER_ONCE").as_deref() != Ok("true") {
        if let Err(error) = serve_healthcheck_worker().await {
            eprintln!("tracedb-worker: {error}");
            std::process::exit(1);
        }
        return;
    }
    run_once_or_exit().await;
}

async fn run_once_or_exit() {
    let engine_url = std::env::var("TRACEDB_ENGINE_URL")
        .or_else(|_| std::env::var("TRACEDB_WORKER_ENGINE_URL"))
        .unwrap_or_else(|_| "http://tracedb-engine.railway.internal:8080".to_string());
    let mut jobs = JobCatalog::default();
    let _ = jobs.enqueue(JobKind::VerifyDatabase, "startup", "startup-verify");
    match tracedb_worker::run_once_through_engine_api_async(
        &mut jobs,
        WorkerId::new("worker-main"),
        &engine_url,
    )
    .await
    {
        Ok(report) => println!(
            "{}",
            json!({
                "ok": true,
                "engine_url": report.engine_url,
                "used_private_engine_api": report.used_private_engine_api,
                "engine_health_checked": report.engine_health_checked,
                "engine_status_code": report.engine_status_code,
                "leased_job_id": report.leased_job_id,
                "heartbeat_job_id": report.heartbeat_job_id,
                "completed_job_id": report.completed_job_id,
                "failed_job_id": report.failed_job_id,
            })
        ),
        Err(error) => {
            eprintln!("tracedb-worker: {error}");
            std::process::exit(1);
        }
    }
}

async fn serve_healthcheck_worker() -> std::io::Result<()> {
    let bind = bind_addr_from_env();
    let engine_url = std::env::var("TRACEDB_ENGINE_URL")
        .or_else(|_| std::env::var("TRACEDB_WORKER_ENGINE_URL"))
        .unwrap_or_else(|_| "http://tracedb-engine.railway.internal:8080".to_string());
    let state = WorkerHttpState { engine_url };
    let app = Router::new()
        .route("/health", get(worker_health))
        .route("/ready", get(worker_health))
        .route("/v1/health", get(worker_health))
        .route("/v1/ready", get(worker_health))
        .route("/metrics", get(worker_metrics))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app)
        .await
        .map_err(std::io::Error::other)
}

#[derive(Clone)]
struct WorkerHttpState {
    engine_url: String,
}

async fn worker_health(State(state): State<WorkerHttpState>) -> (StatusCode, Json<Value>) {
    tracing::info!(service = "tracedb-worker", path = "/health", "request");
    match tracedb_worker::probe_private_engine_health_async(&state.engine_url).await {
        Ok(status) => (
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "ready": true,
                "service": "tracedb-worker",
                "mode": "queue-worker",
                "mutates_through_private_engine_api": true,
                "engine_health_checked": true,
                "engine_status_code": status,
            })),
        ),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ok": false,
                "service": "tracedb-worker",
                "mode": "queue-worker",
                "mutates_through_private_engine_api": true,
                "engine_health_checked": true,
                "error": error,
            })),
        ),
    }
}

async fn worker_metrics(State(state): State<WorkerHttpState>) -> Json<Value> {
    tracing::info!(service = "tracedb-worker", path = "/metrics", "request");
    Json(json!({
        "service": "tracedb-worker",
        "worker_loops": 0,
        "leased_jobs": 0,
        "completed_jobs": 0,
        "engine_url": state.engine_url,
    }))
}

fn bind_addr_from_env() -> String {
    std::env::var("TRACEDB_BIND").unwrap_or_else(|_| {
        std::env::var("PORT")
            .map(|port| format!("[::]:{port}"))
            .unwrap_or_else(|_| "[::]:8080".to_string())
    })
}

fn init_json_tracing(default_filter: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new(default_filter))
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .try_init();
}
