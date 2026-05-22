from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


SOURCE_DIR = Path(__file__).resolve().parent
REPO_ROOT = SOURCE_DIR.parents[1]


def _configure_import_path() -> None:
    if os.environ.get("TRACEDB_PYTHON_IMPORT_MODE") != "installed":
        return
    source_dir = SOURCE_DIR.resolve()
    sys.path = [
        entry
        for entry in sys.path
        if Path(entry or os.getcwd()).resolve() != source_dir
    ]


_configure_import_path()

from tracedb import TraceDB, TraceDBHTTPError  # noqa: E402 - import mode must be configured first.


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def start_server(data_dir: Path, bind: str) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env.update(
        {
            "TRACEDB_BIND": bind,
            "TRACEDB_DATA_DIR": str(data_dir),
            "TRACEDB_SERVICE_MODE": "engine",
            "CARGO_TERM_COLOR": "never",
            "CARGO_INCREMENTAL": "0",
        }
    )
    return subprocess.Popen(
        ["cargo", "run", "-q", "-p", "tracedb-server"],
        cwd=REPO_ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def wait_for_ready(db: TraceDB, process: subprocess.Popen[str]) -> None:
    last_error = "not ready"
    for _ in range(900):
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise RuntimeError(f"tracedb-server exited before ready; stdout={stdout}; stderr={stderr}")
        try:
            if db.ready().get("ready") is True:
                return
        except Exception as error:  # noqa: BLE001 - readiness loops report the last failure.
            last_error = str(error)
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for tracedb-server readiness: {last_error}")


def stop_server(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def schema() -> dict[str, Any]:
    return {
        "name": "docs",
        "primary_id_column": "id",
        "tenant_id_column": "tenant",
        "scalar_columns": ["status"],
        "text_indexed_columns": ["body"],
        "vector_columns": [{"name": "embedding", "dimensions": 3, "source_columns": ["body"]}],
    }


def run_smoke(summary_json: Path | None = None) -> dict[str, Any]:
    run_id = f"{os.getpid()}-{int(time.time() * 1000)}"
    with tempfile.TemporaryDirectory(prefix="tracedb-python-sdk-smoke-") as temp_dir:
        root = Path(temp_dir)
        data_dir = root / "data"
        admin_dir = root / "admin"
        admin_dir.mkdir(parents=True, exist_ok=True)
        bind = f"127.0.0.1:{free_port()}"
        base_url = f"http://{bind}"
        process = start_server(data_dir, bind)
        try:
            db = TraceDB(base_url, token="dev-token", safe_retries=1)
            assert db.safe_retries == 1
            wait_for_ready(db, process)
            assert db.health()["ok"] is True
            databases = db.list_databases()
            branches = db.list_branches()
            metrics = db.public_safe_metrics()
            assert metrics["service"] == "tracedb-engine"

            db.apply_schema(schema(), idempotency_key=f"python-{run_id}-schema")
            docs = db.table("docs").tenant("tenant-a")

            put_response = docs.insert(
                "intro",
                {
                    "body": "TraceDB Python SDK smoke",
                    "embedding": [1, 0, 0],
                    "status": "published",
                },
                idempotency_key=f"python-{run_id}-put",
            )
            replay_response = docs.insert(
                "intro",
                {
                    "body": "TraceDB Python SDK smoke",
                    "embedding": [1, 0, 0],
                    "status": "published",
                },
                idempotency_key=f"python-{run_id}-put",
            )
            conflict_status = None
            try:
                docs.insert(
                    "intro",
                    {
                        "body": "TraceDB Python SDK smoke changed",
                        "embedding": [1, 0, 0],
                        "status": "published",
                    },
                    idempotency_key=f"python-{run_id}-put",
                )
            except TraceDBHTTPError as error:
                conflict_status = error.status
            assert conflict_status == 409
            assert replay_response["epoch"] == put_response["epoch"]

            batch = docs.insert_batch(
                [
                    {
                        "id": "sdk",
                        "fields": {
                            "body": "TraceDB Python sync SDK table handle",
                            "embedding": [0.8, 0.2, 0],
                            "status": "published",
                        },
                    },
                    {
                        "id": "ops",
                        "fields": {
                            "body": "TraceDB Python SDK snapshot restore path",
                            "embedding": [0, 1, 0],
                            "status": "published",
                        },
                    },
                ],
                idempotency_key=f"python-{run_id}-batch",
            )
            assert batch["record_count"] == 2

            docs.patch("sdk", {"status": "reviewed", "reviewer": "python-sdk-smoke"}, idempotency_key=f"python-{run_id}-patch")
            patched = docs.get("sdk")
            assert patched["record"]["fields"]["status"] == "reviewed"
            scan = docs.limit(10).scan()
            assert scan["returned_count"] == 3

            query = (
                db.table("docs")
                .where({"tenant_id": "tenant-a", "status": "published"})
                .match_text("body", "TraceDB Python")
                .near("embedding", [1, 0, 0])
                .with_options(explain=True, freshness="lazy")
                .limit(3)
                .all()
            )
            assert isinstance(query["results"], list)
            explain = (
                db.table("docs")
                .where({"tenant_id": "tenant-a", "status": "published"})
                .match_text("body", "TraceDB Python")
                .near("embedding", [1, 0, 0])
                .limit(3)
                .explain_plan()
            )
            assert isinstance(explain["returned_count"], int)

            deleted = docs.delete("ops", tombstone="python_sdk_smoke", idempotency_key=f"python-{run_id}-delete")
            assert deleted["deleted"] is True
            deleted_get = docs.get("ops")
            assert deleted_get["record"] is None

            compact = db.compact(idempotency_key=f"python-{run_id}-compact")
            snapshot = db.snapshot(str(admin_dir / "snapshot"), idempotency_key=f"python-{run_id}-snapshot")
            restore = db.restore(
                str(admin_dir / "snapshot"),
                str(admin_dir / "restore"),
                idempotency_key=f"python-{run_id}-restore",
            )
            jobs = db.list_admin_jobs()
            assert compact["compacted"] is True
            assert snapshot["snapshot"] is True
            assert restore["restored"] is True
            assert any(job.get("queue") == "tracedb.snapshot.create" for job in jobs.get("jobs", []))

            error_envelope: dict[str, Any]
            try:
                db.request_json("POST", "/v1/records/get", {})
            except TraceDBHTTPError as error:
                error_envelope = {
                    "status": error.status,
                    "error": error.response_error,
                    "code": error.response_code,
                    "method": error.method,
                    "path": error.path,
                }
            else:
                raise AssertionError("expected error envelope request to fail")

            summary = {
                "ok": True,
                "mode": "python-sdk-http-smoke",
                "server_url": base_url,
                "sdk_surface": "python_sync",
                "safe_retries": db.safe_retries,
                "steps": {
                    "ready": True,
                    "health": True,
                    "catalog": True,
                    "metrics": True,
                    "schema_apply": True,
                    "put": True,
                    "batch_ingest": True,
                    "patch": True,
                    "get": True,
                    "scan": True,
                    "query": True,
                    "explain": True,
                    "delete": True,
                    "idempotency": True,
                    "error_envelope": True,
                    "compact": True,
                    "snapshot": True,
                    "restore": True,
                    "jobs": True,
                },
                "records_put": 1,
                "records_inserted": 3,
                "records_scanned": scan["returned_count"],
                "catalog_databases": len(databases.get("databases", [])),
                "catalog_branches": len(branches.get("branches", [])),
                "put_epoch": put_response["epoch"],
                "idempotency_replay_epoch": replay_response["epoch"],
                "idempotency_conflict_status": conflict_status,
                "patched_status": patched["record"]["fields"]["status"],
                "deleted_hidden": deleted_get["record"] is None,
                "error_envelope": error_envelope,
                "snapshot_target": snapshot["target"],
                "restore_target": restore["target"],
                "sql_module": "not_implemented",
                "python_import_mode": os.environ.get("TRACEDB_PYTHON_IMPORT_MODE", "source"),
            }
            if summary_json is not None:
                summary_json.parent.mkdir(parents=True, exist_ok=True)
                summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
            return summary
        finally:
            stop_server(process)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TraceDB Python sync SDK HTTP smoke.")
    parser.add_argument("--summary-json", help="Optional path to write the JSON summary.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    summary = run_smoke(Path(args.summary_json) if args.summary_json else None)
    print(json.dumps(summary, indent=2, sort_keys=True))
    print("python sdk http smoke ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
