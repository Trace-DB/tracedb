use serde_json::Value;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use tracedb_bench::{BaselineKind, BenchmarkTarget, WorkloadKind};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn generated_dataset_has_relevance_labels_aligned_to_scoring_baseline() {
    let root = repo_root();
    let lab = root.join("benchmarks/realworld");
    let output = Command::new("python3")
        .arg("-c")
        .arg(
            r#"
from runner.datasets import generated_dataset
from runner.adapters.base import in_memory_search_metrics
d = generated_dataset(1000, 42)
m = in_memory_search_metrics(d)
print(m["recall_at_5"])
raise SystemExit(0 if m["recall_at_5"] >= 0.95 else 1)
"#,
        )
        .current_dir(&lab)
        .output()
        .expect("run relevance check");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn tracedb_http_surface_runs_real_record_query_and_delete_semantics() {
    let root = repo_root();
    let lab = root.join("benchmarks/realworld");
    let reports = tempfile::tempdir().expect("reports tempdir");
    let json_report = reports.path().join("tracedb-http.json");
    let markdown_report = reports.path().join("tracedb-http.md");
    let db_dir = tempfile::tempdir().expect("db tempdir");

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    let data_dir = db_dir.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let output = Command::new("python3")
        .arg("-m")
        .arg("runner")
        .arg("run")
        .arg("--profile")
        .arg("smoke")
        .arg("--dataset")
        .arg("generated")
        .arg("--records")
        .arg("24")
        .arg("--target")
        .arg("tracedb")
        .arg("--surface")
        .arg("http,curl")
        .arg("--openrouter-mode")
        .arg("off")
        .arg("--output-json")
        .arg(&json_report)
        .arg("--output-md")
        .arg(&markdown_report)
        .env("TRACEDB_HTTP_URL", format!("http://{addr}"))
        .current_dir(&lab)
        .output()
        .expect("run tracedb http benchmark");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value =
        serde_json::from_str(&std::fs::read_to_string(&json_report).unwrap()).unwrap();
    let tracedb = report["baselines"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == "tracedb")
        .expect("tracedb baseline");
    assert_eq!(tracedb["available"], true);
    assert_eq!(tracedb["metrics"]["failure_count"], 0);
    assert!(tracedb["metrics"]["min_recall_at_5"].is_number());
    assert!(tracedb["metrics"]["min_ndcg_at_5"].is_number());
    assert!(tracedb["metrics"]["ingest_latency_p95_ms"].is_number());
    assert!(tracedb["metrics"]["query_latency_p95_ms"].is_number());
    assert!(tracedb["metrics"]["admin_latency_p95_ms"].is_number());
    assert!(tracedb["metrics"]["queries_below_full_recall_count"].is_number());
    assert!(tracedb["metrics"]["queries_with_zero_recall_count"].is_number());
    assert!(tracedb["metrics"]["category_filter_applied"].is_boolean());
    assert!(tracedb["metrics"]["off_category_result_count"].is_number());
    assert!(tracedb["metrics"]["queries_with_off_category_results_count"].is_number());
    assert_eq!(tracedb["metrics"]["category_filter_applied"], true);
    assert_eq!(tracedb["metrics"]["off_category_result_count"], 0);
    assert_eq!(
        tracedb["metrics"]["queries_with_off_category_results_count"],
        0
    );
    assert_eq!(
        tracedb["metrics"]["query_output_probe_explain_false_explain_returned_count"],
        0
    );
    assert!(
        tracedb["metrics"]["query_output_probe_explain_false_body_bytes_p95"]
            .as_f64()
            .unwrap_or_default()
            < tracedb["metrics"]["query_output_probe_explain_true_body_bytes_p95"]
                .as_f64()
                .unwrap_or_default(),
        "explain=false body should be leaner than explain=true: {} vs {}",
        tracedb["metrics"]["query_output_probe_explain_false_body_bytes_p95"],
        tracedb["metrics"]["query_output_probe_explain_true_body_bytes_p95"]
    );
    assert!(
        tracedb["metrics"]["recall_at_5"]
            .as_f64()
            .unwrap_or_default()
            >= 0.8,
        "recall_at_5 should meet scalar-filter KPI floor: {}",
        tracedb["metrics"]["recall_at_5"]
    );
    assert!(tracedb["notes"].as_array().unwrap().iter().any(|note| note
        .as_str()
        .unwrap_or_default()
        .contains("TraceDB HTTP/curl records/query/delete smoke passed")));
    assert!(tracedb["notes"].as_array().unwrap().iter().any(|note| note
        .as_str()
        .unwrap_or_default()
        .contains("TraceDB HTTP retrieval diagnostics")));
    assert!(tracedb["notes"].as_array().unwrap().iter().any(|note| note
        .as_str()
        .unwrap_or_default()
        .contains("TraceDB HTTP filter parity diagnostics")));
}

#[test]
fn realworld_lab_declares_search_rag_6_services_and_runner_contracts() {
    let target = BenchmarkTarget::new(WorkloadKind::SearchRag6, 1_000);
    assert_eq!(
        target.baselines(),
        vec![
            BaselineKind::TraceDb,
            BaselineKind::Postgres,
            BaselineKind::PgVector,
            BaselineKind::MongoDb,
            BaselineKind::Qdrant,
            BaselineKind::OpenSearch,
        ]
    );

    let root = repo_root();
    let lab = root.join("benchmarks/realworld");
    assert!(lab.join("docker-compose.yml").exists());
    assert!(lab.join("README.md").exists());
    assert!(lab.join("requirements.txt").exists());
    assert!(lab.join("runner/__main__.py").exists());
    assert!(lab.join("runner/adapters/tracedb.py").exists());
    assert!(lab.join("workloads/search_rag_6.json").exists());

    let compose = std::fs::read_to_string(lab.join("docker-compose.yml")).unwrap();
    for service in [
        "bench-tracedb",
        "bench-postgres",
        "bench-pgvector",
        "bench-mongo",
        "bench-qdrant",
        "bench-opensearch",
        "bench-runner",
    ] {
        assert!(compose.contains(service), "missing service {service}");
    }
    assert!(compose.contains("pgvector/pgvector"));
    assert!(compose.contains("opensearchproject/opensearch"));
    assert!(compose.contains("qdrant/qdrant"));
    assert!(compose.contains("mongo:"));
}

#[test]
fn generated_smoke_benchmark_emits_json_and_markdown_for_all_baselines() {
    let root = repo_root();
    let lab = root.join("benchmarks/realworld");
    let reports = tempfile::tempdir().expect("reports tempdir");
    let json_report = reports.path().join("smoke.json");
    let markdown_report = reports.path().join("smoke.md");

    let output = Command::new("python3")
        .arg("-m")
        .arg("runner")
        .arg("run")
        .arg("--profile")
        .arg("smoke")
        .arg("--dataset")
        .arg("generated")
        .arg("--records")
        .arg("48")
        .arg("--target")
        .arg("all")
        .arg("--surface")
        .arg("sdk,cli,http,curl")
        .arg("--openrouter-mode")
        .arg("off")
        .arg("--output-json")
        .arg(&json_report)
        .arg("--output-md")
        .arg(&markdown_report)
        .current_dir(&lab)
        .output()
        .expect("run smoke benchmark");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let report: Value =
        serde_json::from_str(&std::fs::read_to_string(&json_report).unwrap()).unwrap();
    assert_eq!(report["profile"], "smoke");
    assert_eq!(report["dataset"]["kind"], "generated");
    assert_eq!(report["summary"]["baseline_count"], 6);
    assert_eq!(report["summary"]["record_count"], 48);

    let baselines = report["baselines"].as_array().expect("baselines array");
    for baseline in [
        "tracedb",
        "postgres",
        "pgvector",
        "mongodb",
        "qdrant",
        "opensearch",
    ] {
        let entry = baselines
            .iter()
            .find(|entry| entry["name"] == baseline)
            .unwrap_or_else(|| panic!("missing baseline {baseline}"));
        assert!(entry["metrics"]["ingest_count"].as_u64().is_some());
        assert!(entry["metrics"]["query_count"].as_u64().is_some());
        assert!(!entry["notes"].as_array().expect("notes array").is_empty());
    }

    let markdown = std::fs::read_to_string(&markdown_report).unwrap();
    assert!(markdown.contains("# TraceDB Real-World Benchmark Report"));
    assert!(markdown.contains("| tracedb |"));
    assert!(markdown.contains("| pgvector |"));
    assert!(markdown.contains("| mongodb |"));
    assert!(markdown.contains("sdk, cli, http, curl"));
}
