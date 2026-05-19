use serde_json::Value;
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

#[test]
fn sdk_quickstart_example_runs_against_real_http_server() {
    let temp = tempfile::tempdir().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let data_dir = temp.path().to_path_buf();
    std::thread::spawn(move || {
        let _ = tracedb_server::serve(data_dir, &addr.to_string());
    });
    std::thread::sleep(Duration::from_millis(100));

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace_root)
        .args([
            "run",
            "-q",
            "-p",
            "tracedb-sdk",
            "--example",
            "quickstart",
            "--",
            "--url",
            &format!("http://{addr}"),
            "--token",
            "dev-token",
            "--timeout-ms",
            "5000",
        ])
        .output()
        .expect("run quickstart example");

    assert!(
        output.status.success(),
        "quickstart failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&output.stdout).expect("quickstart emits json summary");

    assert_eq!(summary["ok"], true);
    assert_eq!(summary["steps"]["schema_apply"], true);
    assert_eq!(summary["steps"]["batch_ingest"], true);
    assert_eq!(summary["steps"]["query"], true);
    assert_eq!(summary["steps"]["scan"], true);
    assert_eq!(summary["steps"]["delete"], true);
    assert_eq!(summary["sql_module"], "not_implemented");
}
