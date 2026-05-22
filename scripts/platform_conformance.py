from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT = Path("docs/platform-contract-v0.json")
DEFAULT_SURFACES = ["http_direct", "rust_sdk"]
PYTHON_SDK_CONFORMANCE_EVIDENCE = "installed package + clients/python/http_smoke.py"


def load_contract(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def contract_scenario_ids(manifest: dict[str, Any]) -> list[str]:
    return [scenario["id"] for scenario in manifest["conformance_scenarios"]]


def contract_surface_ids(manifest: dict[str, Any]) -> list[str]:
    return [surface["id"] for surface in manifest["surfaces"]]


def scenario_result(
    scenario_id: str,
    status: str,
    *,
    evidence: str | None = None,
    reason: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"id": scenario_id, "status": status}
    if evidence is not None:
        result["evidence"] = evidence
    if reason is not None:
        result["reason"] = reason
    if details:
        result["details"] = details
    return result


def passed(scenario_id: str, evidence: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return scenario_result(scenario_id, "passed", evidence=evidence, details=details)


def failed(scenario_id: str, error: Exception) -> dict[str, Any]:
    return scenario_result(scenario_id, "failed", reason=f"{type(error).__name__}: {error}")


def not_checked(scenario_id: str, reason: str) -> dict[str, Any]:
    return scenario_result(scenario_id, "not_checked", reason=reason)


def empty_surface_report(
    manifest: dict[str, Any],
    surface_id: str,
    status: str,
    reason: str,
) -> dict[str, Any]:
    return finalize_surface(
        surface_id,
        status,
        [scenario_result(scenario_id, status, reason=reason) for scenario_id in contract_scenario_ids(manifest)],
        evidence=[],
    )


def finalize_surface(
    surface_id: str,
    status: str,
    scenarios: list[dict[str, Any]],
    *,
    evidence: list[str],
) -> dict[str, Any]:
    failed_count = sum(1 for scenario in scenarios if scenario["status"] == "failed")
    passed_count = sum(1 for scenario in scenarios if scenario["status"] == "passed")
    complete = len(scenarios) > 0 and passed_count == len(scenarios)
    ok = failed_count == 0 and status not in {"not_run", "failed"}
    return {
        "surface": surface_id,
        "status": status,
        "ok": ok,
        "complete": complete,
        "passed": passed_count,
        "required": len(scenarios),
        "not_checked": sum(1 for scenario in scenarios if scenario["status"] == "not_checked"),
        "failed": failed_count,
        "evidence": evidence,
        "scenarios": scenarios,
    }


def ordered_surface_scenarios(
    manifest: dict[str, Any],
    scenario_map: dict[str, dict[str, Any]],
    *,
    default_reason: str,
) -> list[dict[str, Any]]:
    return [
        scenario_map.get(scenario_id, not_checked(scenario_id, default_reason))
        for scenario_id in contract_scenario_ids(manifest)
    ]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    headers: dict[str, str] | None = None,
    expected_status: int = 200,
) -> tuple[int, dict[str, Any]]:
    request_headers = {
        "Authorization": "Bearer dev-token",
        "Accept": "application/json",
    }
    data = None
    if body is not None:
        data = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = response.read().decode("utf-8")
            status = response.status
    except urllib.error.HTTPError as error:
        payload = error.read().decode("utf-8")
        status = error.code
    if status != expected_status:
        raise RuntimeError(f"{method} {path} returned {status}, expected {expected_status}: {payload}")
    try:
        return status, json.loads(payload)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{method} {path} returned non-JSON body: {payload}") from error


def wait_for_ready(base_url: str, process: subprocess.Popen[str]) -> None:
    last_error = "not ready"
    for _ in range(300):
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise RuntimeError(
                f"tracedb-server exited before ready; stdout={stdout}; stderr={stderr}"
            )
        try:
            _, ready = request_json(base_url, "GET", "/v1/ready")
            if ready.get("ready") is True:
                return
        except Exception as error:  # noqa: BLE001 - readiness loops report the last failure.
            last_error = str(error)
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for tracedb-server readiness: {last_error}")


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def table_schema() -> dict[str, Any]:
    return {
        "name": "docs",
        "primary_id_column": "id",
        "tenant_id_column": "tenant",
        "scalar_columns": ["status"],
        "text_indexed_columns": ["body"],
        "vector_columns": [
            {"name": "embedding", "dimensions": 3, "source_columns": ["body"]},
        ],
    }


def record(record_id: str, body: str, status: str, embedding: list[float]) -> dict[str, Any]:
    return {
        "table": "docs",
        "id": record_id,
        "tenant_id": "tenant-a",
        "fields": {
            "body": body,
            "embedding": embedding,
            "id": record_id,
            "status": status,
            "tenant": "tenant-a",
        },
    }


def query_body(*, explain: bool) -> dict[str, Any]:
    return {
        "table": "docs",
        "tenant_id": "tenant-a",
        "text": "TraceDB",
        "vector": [1.0, 0.0, 0.0],
        "top_k": 3,
        "freshness": "Strict",
        "explain": explain,
    }


def error_envelope_scenario(base_url: str) -> dict[str, Any]:
    status, payload = request_json(
        base_url,
        "POST",
        "/v1/records/get",
        {},
        expected_status=400,
    )
    if not isinstance(payload.get("error"), str):
        raise RuntimeError(f"expected JSON error envelope with string error field, got {payload}")
    return passed(
        "errors",
        "current JSON error envelope",
        {
            "status": status,
            "error": payload["error"],
            "code": payload.get("code"),
        },
    )


def traceql_string_execution_scenario(base_url: str) -> dict[str, Any]:
    traceql = "\n".join(
        [
            "FROM docs",
            "TENANT tenant-a",
            'WHERE status = "reviewed"',
            'MATCH body "TraceDB"',
            "NEAR embedding [1.0, 0.0, 0.0]",
            "FRESHNESS allow_dirty",
            "LIMIT 3",
        ]
    )
    _, payload = request_json(base_url, "POST", "/v1/traceql", {"query": traceql})
    results = payload.get("results")
    if not isinstance(results, list):
        raise RuntimeError(f"TraceQL response missing results list: {payload}")
    result_ids = [row.get("record_id") for row in results if isinstance(row, dict)]
    if "intro" not in result_ids:
        raise RuntimeError(f"TraceQL did not return expected record intro: {payload}")
    if "explain" in payload:
        raise RuntimeError(f"TraceQL response should be lean without EXPLAIN: {payload}")

    _, explain_payload = request_json(
        base_url,
        "POST",
        "/v1/traceql",
        {"query": f"{traceql}\nEXPLAIN"},
    )
    if not isinstance(explain_payload.get("results"), list) or not isinstance(
        explain_payload.get("explain"),
        dict,
    ):
        raise RuntimeError(f"TraceQL EXPLAIN response missing results or explain: {explain_payload}")

    invalid_status, invalid_payload = request_json(
        base_url,
        "POST",
        "/v1/traceql",
        {"query": "FROM docs\nTENANT tenant-a\nDROP TABLE docs"},
        expected_status=400,
    )
    error = invalid_payload.get("error")
    if invalid_payload.get("code") != "bad_request" or not (
        isinstance(error, str) and "invalid TraceQL" in error
    ):
        raise RuntimeError(f"invalid TraceQL did not preserve bad-request envelope: {invalid_payload}")

    return passed(
        "traceql_string_execution",
        "POST /v1/traceql",
        {
            "result_ids": result_ids,
            "explain": True,
            "invalid_status": invalid_status,
            "invalid_code": invalid_payload.get("code"),
        },
    )


def run_http_direct_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    scenarios: dict[str, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory(prefix="tracedb-http-conformance-") as temp_dir:
        temp = Path(temp_dir)
        data_dir = temp / "data"
        admin_dir = temp / "admin"
        admin_dir.mkdir(parents=True, exist_ok=True)
        bind = f"127.0.0.1:{free_port()}"
        base_url = f"http://{bind}"
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
        process = subprocess.Popen(
            ["cargo", "run", "-q", "-p", "tracedb-server"],
            cwd=repo_root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_for_ready(base_url, process)

            def run_step(scenario_id: str, action: Any) -> None:
                try:
                    scenarios[scenario_id] = action()
                except Exception as error:  # noqa: BLE001 - report per-scenario failure details.
                    scenarios[scenario_id] = failed(scenario_id, error)

            schema = table_schema()
            intro = record("intro", "TraceDB direct HTTP conformance", "draft", [1.0, 0.0, 0.0])
            intro_changed = record(
                "intro",
                "TraceDB direct HTTP conformance changed",
                "draft",
                [1.0, 0.0, 0.0],
            )

            run_step(
                "schema_apply",
                lambda: passed(
                    "schema_apply",
                    "POST /v1/schema/apply",
                    {"epoch": request_json(base_url, "POST", "/v1/schema/apply", schema)[1]["epoch"]},
                ),
            )
            run_step(
                "put",
                lambda: passed(
                    "put",
                    "POST /v1/records/put",
                    {
                        "epoch": request_json(
                            base_url,
                            "POST",
                            "/v1/records/put",
                            intro,
                            headers={"Idempotency-Key": "http-direct-put-intro"},
                        )[1]["epoch"]
                    },
                ),
            )
            run_step(
                "batch",
                lambda: passed(
                    "batch",
                    "POST /v1/records/put-batch",
                    {
                        "record_count": request_json(
                            base_url,
                            "POST",
                            "/v1/records/put-batch",
                            {
                                "records": [
                                    record("sdk", "TraceDB SDK conformance", "published", [0.8, 0.2, 0.0]),
                                    record("ops", "TraceDB snapshot restore", "published", [0.0, 1.0, 0.0]),
                                ]
                            },
                            headers={"Idempotency-Key": "http-direct-batch"},
                        )[1]["record_count"]
                    },
                ),
            )
            run_step(
                "patch",
                lambda: passed(
                    "patch",
                    "POST /v1/records/patch",
                    {
                        "epoch": request_json(
                            base_url,
                            "POST",
                            "/v1/records/patch",
                            {
                                "table": "docs",
                                "tenant_id": "tenant-a",
                                "id": "intro",
                                "fields": {"status": "reviewed"},
                            },
                            headers={"Idempotency-Key": "http-direct-patch"},
                        )[1]["epoch"]
                    },
                ),
            )
            run_step(
                "get",
                lambda: passed(
                    "get",
                    "POST /v1/records/get",
                    {
                        "status": request_json(
                            base_url,
                            "POST",
                            "/v1/records/get",
                            {"table": "docs", "tenant_id": "tenant-a", "id": "intro"},
                        )[1]["record"]["fields"]["status"]
                    },
                ),
            )
            run_step(
                "scan",
                lambda: passed(
                    "scan",
                    "POST /v1/records/scan",
                    {
                        "returned_count": request_json(
                            base_url,
                            "POST",
                            "/v1/records/scan",
                            {"table": "docs", "tenant_id": "tenant-a", "limit": 10},
                        )[1]["returned_count"]
                    },
                ),
            )
            run_step(
                "query",
                lambda: passed(
                    "query",
                    "POST /v1/query",
                    {
                        "result_count": len(
                            request_json(base_url, "POST", "/v1/query", query_body(explain=True))[1][
                                "results"
                            ]
                        )
                    },
                ),
            )
            run_step(
                "traceql_string_execution",
                lambda: traceql_string_execution_scenario(base_url),
            )
            run_step(
                "explain",
                lambda: passed(
                    "explain",
                    "POST /v1/explain",
                    {
                        "returned_count": request_json(
                            base_url,
                            "POST",
                            "/v1/explain",
                            query_body(explain=True),
                        )[1]["returned_count"]
                    },
                ),
            )
            run_step(
                "delete",
                lambda: passed(
                    "delete",
                    "POST /v1/records/delete",
                    {
                        "deleted": request_json(
                            base_url,
                            "POST",
                            "/v1/records/delete",
                            {
                                "table": "docs",
                                "tenant_id": "tenant-a",
                                "id": "ops",
                                "tombstone": "platform_conformance",
                            },
                            headers={"Idempotency-Key": "http-direct-delete"},
                        )[1]["deleted"],
                        "hidden": request_json(
                            base_url,
                            "POST",
                            "/v1/records/get",
                            {"table": "docs", "tenant_id": "tenant-a", "id": "ops"},
                        )[1]["record"]
                        is None,
                    },
                ),
            )
            run_step(
                "idempotency",
                lambda: passed(
                    "idempotency",
                    "Idempotency-Key replay and conflict",
                    {
                        "replay_epoch": request_json(
                            base_url,
                            "POST",
                            "/v1/records/put",
                            intro,
                            headers={"Idempotency-Key": "http-direct-put-intro"},
                        )[1]["epoch"],
                        "conflict_status": request_json(
                            base_url,
                            "POST",
                            "/v1/records/put",
                            intro_changed,
                            headers={"Idempotency-Key": "http-direct-put-intro"},
                            expected_status=409,
                        )[0],
                    },
                ),
            )
            run_step(
                "errors",
                lambda: error_envelope_scenario(base_url),
            )
            snapshot_dir = admin_dir / "snapshot"
            restore_dir = admin_dir / "restore"
            run_step(
                "snapshot_restore",
                lambda: passed(
                    "snapshot_restore",
                    "POST /v1/admin/snapshot and POST /v1/admin/restore",
                    {
                        "snapshot": request_json(
                            base_url,
                            "POST",
                            "/v1/admin/snapshot",
                            {"target": str(snapshot_dir)},
                            headers={"Idempotency-Key": "http-direct-snapshot"},
                        )[1]["snapshot"],
                        "restored": request_json(
                            base_url,
                            "POST",
                            "/v1/admin/restore",
                            {"source": str(snapshot_dir), "target": str(restore_dir)},
                            headers={"Idempotency-Key": "http-direct-restore"},
                        )[1]["restored"],
                    },
                ),
            )
        finally:
            stop_process(process)

    ordered = [
        scenarios.get(scenario_id, not_checked(scenario_id, "scenario did not run"))
        for scenario_id in contract_scenario_ids(manifest)
    ]
    return finalize_surface(
        "http_direct",
        "checked",
        ordered,
        evidence=["raw stdlib HTTP requests against tracedb-server"],
    )


def run_command(
    argv: list[str],
    cwd: Path,
    *,
    env_extra: dict[str, str] | None = None,
) -> dict[str, Any]:
    env = {**os.environ, "CARGO_TERM_COLOR": "never", "CARGO_INCREMENTAL": "0"}
    if env_extra:
        env.update(env_extra)
    started = time.monotonic()
    process = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return {
        "argv": argv,
        "cwd": str(cwd),
        "duration_s": round(time.monotonic() - started, 3),
        "ok": process.returncode == 0,
        "returncode": process.returncode,
        "stdout": process.stdout,
        "stderr_tail": process.stderr[-12_000:],
    }


def map_rust_sdk_product_summary(
    manifest: dict[str, Any],
    product_summary: dict[str, Any],
) -> dict[str, Any]:
    step = product_summary.get("steps", {}).get("rust_sdk_quickstart", {})
    quickstart = step.get("summary", {})
    quickstart_steps = quickstart.get("steps", {})
    error_envelope = quickstart.get("error_envelope", {})
    records_put = quickstart.get("records_put")

    def step_passed(name: str) -> bool:
        return step.get("ok") is True and quickstart.get("ok") is True and quickstart_steps.get(name) is True

    def error_envelope_passed() -> bool:
        return (
            step_passed("error_envelope")
            and isinstance(error_envelope, dict)
            and isinstance(error_envelope.get("status"), int)
            and error_envelope["status"] >= 400
            and isinstance(error_envelope.get("error"), str)
            and bool(error_envelope["error"])
        )

    scenario_map = {
        "schema_apply": passed("schema_apply", "rust_sdk_quickstart steps.schema_apply")
        if step_passed("schema_apply")
        else failed("schema_apply", RuntimeError("Rust SDK quickstart schema_apply did not pass")),
        "put": passed(
            "put",
            "rust_sdk_quickstart steps.put",
            {"records_put": records_put, "put_epoch": quickstart.get("put_epoch")},
        )
        if step_passed("put") and isinstance(records_put, int) and records_put >= 1
        else failed("put", RuntimeError("Rust SDK quickstart single-record put evidence missing")),
        "batch": passed("batch", "rust_sdk_quickstart steps.batch_ingest")
        if step_passed("batch_ingest")
        else failed("batch", RuntimeError("Rust SDK quickstart batch_ingest did not pass")),
        "patch": passed("patch", "rust_sdk_quickstart steps.patch")
        if step_passed("patch")
        else failed("patch", RuntimeError("Rust SDK quickstart patch did not pass")),
        "get": passed("get", "rust_sdk_quickstart patched get and deleted get checks")
        if quickstart.get("patched_status") == "reviewed" and quickstart.get("deleted_hidden") is True
        else failed("get", RuntimeError("Rust SDK quickstart get evidence missing")),
        "scan": passed("scan", "rust_sdk_quickstart steps.scan")
        if step_passed("scan")
        else failed("scan", RuntimeError("Rust SDK quickstart scan did not pass")),
        "query": passed("query", "rust_sdk_quickstart steps.query")
        if step_passed("query")
        else failed("query", RuntimeError("Rust SDK quickstart query did not pass")),
        "explain": passed("explain", "rust_sdk_quickstart steps.explain")
        if step_passed("explain")
        else failed("explain", RuntimeError("Rust SDK quickstart explain did not pass")),
        "delete": passed("delete", "rust_sdk_quickstart steps.delete and deleted_hidden")
        if step_passed("delete") and quickstart.get("deleted_hidden") is True
        else failed("delete", RuntimeError("Rust SDK quickstart delete did not pass")),
        "idempotency": passed("idempotency", "rust_sdk_quickstart idempotency_keys")
        if quickstart.get("idempotency_keys") is True and quickstart.get("idempotency_retries", 0) >= 1
        else failed("idempotency", RuntimeError("Rust SDK quickstart idempotency evidence missing")),
        "errors": passed("errors", "rust_sdk_quickstart error_envelope", dict(error_envelope))
        if error_envelope_passed()
        else failed("errors", RuntimeError("Rust SDK quickstart error envelope evidence missing")),
        "snapshot_restore": passed("snapshot_restore", "rust_sdk_quickstart admin snapshot/restore")
        if step_passed("snapshot") and step_passed("restore")
        else failed("snapshot_restore", RuntimeError("Rust SDK quickstart snapshot/restore did not pass")),
    }
    scenarios = ordered_surface_scenarios(
        manifest,
        scenario_map,
        default_reason="Rust SDK public surface does not expose native TraceQL execution yet",
    )
    return finalize_surface(
        "rust_sdk",
        "checked",
        scenarios,
        evidence=["cargo run -q -p tracedb-cli -- product-regression --only rust_sdk_quickstart"],
    )


def map_python_sdk_smoke_summary(
    manifest: dict[str, Any],
    smoke_summary: dict[str, Any],
) -> dict[str, Any]:
    steps = smoke_summary.get("steps", {})
    error_envelope = smoke_summary.get("error_envelope", {})

    def step_passed(name: str) -> bool:
        return smoke_summary.get("ok") is True and steps.get(name) is True

    def error_envelope_passed() -> bool:
        return (
            step_passed("error_envelope")
            and isinstance(error_envelope, dict)
            and isinstance(error_envelope.get("status"), int)
            and error_envelope["status"] >= 400
            and isinstance(error_envelope.get("error"), str)
            and bool(error_envelope["error"])
        )

    scenario_map = {
        "schema_apply": passed("schema_apply", "python sdk smoke steps.schema_apply")
        if step_passed("schema_apply")
        else failed("schema_apply", RuntimeError("Python SDK smoke schema_apply did not pass")),
        "put": passed(
            "put",
            "python sdk smoke steps.put",
            {"records_put": smoke_summary.get("records_put"), "put_epoch": smoke_summary.get("put_epoch")},
        )
        if step_passed("put") and smoke_summary.get("records_put") == 1
        else failed("put", RuntimeError("Python SDK smoke single-record put evidence missing")),
        "batch": passed(
            "batch",
            "python sdk smoke steps.batch_ingest",
            {"records_inserted": smoke_summary.get("records_inserted")},
        )
        if step_passed("batch_ingest")
        else failed("batch", RuntimeError("Python SDK smoke batch_ingest did not pass")),
        "patch": passed("patch", "python sdk smoke steps.patch")
        if step_passed("patch")
        else failed("patch", RuntimeError("Python SDK smoke patch did not pass")),
        "get": passed("get", "python sdk smoke patched get and deleted get checks")
        if step_passed("get") and smoke_summary.get("patched_status") == "reviewed"
        else failed("get", RuntimeError("Python SDK smoke get evidence missing")),
        "scan": passed(
            "scan",
            "python sdk smoke steps.scan",
            {"records_scanned": smoke_summary.get("records_scanned")},
        )
        if step_passed("scan")
        else failed("scan", RuntimeError("Python SDK smoke scan did not pass")),
        "query": passed("query", "python sdk smoke steps.query")
        if step_passed("query")
        else failed("query", RuntimeError("Python SDK smoke query did not pass")),
        "explain": passed("explain", "python sdk smoke steps.explain")
        if step_passed("explain")
        else failed("explain", RuntimeError("Python SDK smoke explain did not pass")),
        "delete": passed("delete", "python sdk smoke steps.delete and deleted_hidden")
        if step_passed("delete") and smoke_summary.get("deleted_hidden") is True
        else failed("delete", RuntimeError("Python SDK smoke delete did not pass")),
        "idempotency": passed(
            "idempotency",
            "python sdk smoke idempotency replay and conflict",
            {
                "replay_epoch": smoke_summary.get("idempotency_replay_epoch"),
                "conflict_status": smoke_summary.get("idempotency_conflict_status"),
            },
        )
        if step_passed("idempotency") and smoke_summary.get("idempotency_conflict_status") == 409
        else failed("idempotency", RuntimeError("Python SDK smoke idempotency evidence missing")),
        "errors": passed("errors", "python sdk smoke error_envelope", dict(error_envelope))
        if error_envelope_passed()
        else failed("errors", RuntimeError("Python SDK smoke error envelope evidence missing")),
        "snapshot_restore": passed("snapshot_restore", "python sdk smoke admin snapshot/restore")
        if step_passed("snapshot") and step_passed("restore")
        else failed("snapshot_restore", RuntimeError("Python SDK smoke snapshot/restore did not pass")),
    }
    scenarios = ordered_surface_scenarios(
        manifest,
        scenario_map,
        default_reason="Python SDK public surface does not expose native TraceQL execution yet",
    )
    return finalize_surface(
        "python_sdk",
        "checked",
        scenarios,
        evidence=[PYTHON_SDK_CONFORMANCE_EVIDENCE],
    )


def map_typescript_sdk_smoke_summary(
    manifest: dict[str, Any],
    smoke_summary: dict[str, Any],
) -> dict[str, Any]:
    steps = smoke_summary.get("steps", {})
    error_envelope = smoke_summary.get("error_envelope", {})

    def step_passed(name: str) -> bool:
        return smoke_summary.get("ok") is True and steps.get(name) is True

    def error_envelope_passed() -> bool:
        return (
            step_passed("error_envelope")
            and isinstance(error_envelope, dict)
            and isinstance(error_envelope.get("status"), int)
            and error_envelope["status"] >= 400
            and isinstance(error_envelope.get("error"), str)
            and bool(error_envelope["error"])
        )

    scenario_map = {
        "schema_apply": passed("schema_apply", "typescript public sdk smoke steps.schema_apply")
        if step_passed("schema_apply")
        else failed("schema_apply", RuntimeError("TypeScript SDK smoke schema_apply did not pass")),
        "put": passed(
            "put",
            "typescript public sdk smoke steps.put",
            {"records_put": smoke_summary.get("records_put"), "put_epoch": smoke_summary.get("put_epoch")},
        )
        if step_passed("put") and smoke_summary.get("records_put") == 1
        else failed("put", RuntimeError("TypeScript SDK smoke single-record put evidence missing")),
        "batch": passed(
            "batch",
            "typescript public sdk smoke steps.batch_ingest",
            {"records_inserted": smoke_summary.get("records_inserted")},
        )
        if step_passed("batch_ingest")
        else failed("batch", RuntimeError("TypeScript SDK smoke batch_ingest did not pass")),
        "patch": passed("patch", "typescript public sdk smoke steps.patch")
        if step_passed("patch")
        else failed("patch", RuntimeError("TypeScript SDK smoke patch did not pass")),
        "get": passed("get", "typescript public sdk smoke patched get and deleted get checks")
        if step_passed("get") and smoke_summary.get("patched_status") == "reviewed"
        else failed("get", RuntimeError("TypeScript SDK smoke get evidence missing")),
        "scan": passed(
            "scan",
            "typescript public sdk smoke steps.scan",
            {"records_scanned": smoke_summary.get("records_scanned")},
        )
        if step_passed("scan")
        else failed("scan", RuntimeError("TypeScript SDK smoke scan did not pass")),
        "query": passed("query", "typescript public sdk smoke steps.query")
        if step_passed("query")
        else failed("query", RuntimeError("TypeScript SDK smoke query did not pass")),
        "explain": passed("explain", "typescript public sdk smoke steps.explain")
        if step_passed("explain")
        else failed("explain", RuntimeError("TypeScript SDK smoke explain did not pass")),
        "delete": passed("delete", "typescript public sdk smoke steps.delete and deleted_hidden")
        if step_passed("delete") and smoke_summary.get("deleted_hidden") is True
        else failed("delete", RuntimeError("TypeScript SDK smoke delete did not pass")),
        "idempotency": passed(
            "idempotency",
            "typescript public sdk smoke idempotency replay and conflict",
            {
                "replay_observed": smoke_summary.get("idempotency_replay_observed"),
                "conflict_status": smoke_summary.get("idempotency_conflict_status"),
            },
        )
        if step_passed("idempotency")
        and smoke_summary.get("idempotency_replay_observed") is True
        and smoke_summary.get("idempotency_conflict_status") == 409
        else failed("idempotency", RuntimeError("TypeScript SDK smoke idempotency evidence missing")),
        "errors": passed("errors", "typescript public sdk smoke error_envelope", dict(error_envelope))
        if error_envelope_passed()
        else failed("errors", RuntimeError("TypeScript SDK smoke error envelope evidence missing")),
        "snapshot_restore": passed("snapshot_restore", "typescript public sdk smoke admin snapshot/restore")
        if step_passed("snapshot") and step_passed("restore")
        else failed("snapshot_restore", RuntimeError("TypeScript SDK smoke snapshot/restore did not pass")),
    }
    scenarios = ordered_surface_scenarios(
        manifest,
        scenario_map,
        default_reason="TypeScript public SDK surface does not expose native TraceQL execution yet",
    )
    return finalize_surface(
        "typescript_sdk",
        "checked",
        scenarios,
        evidence=["npm run public-http-smoke -- --summary-json"],
    )


def run_rust_sdk_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="tracedb-rust-sdk-conformance-") as temp_dir:
        command = run_command(
            [
                "cargo",
                "run",
                "-q",
                "-p",
                "tracedb-cli",
                "--",
                "product-regression",
                "--only",
                "rust_sdk_quickstart",
                "--data-root",
                temp_dir,
            ],
            repo_root,
        )
    if not command["ok"]:
        return finalize_surface(
            "rust_sdk",
            "failed",
            [
                scenario_result(
                    scenario_id,
                    "failed",
                    reason=f"rust_sdk product-regression failed: {command['stderr_tail']}",
                )
                for scenario_id in contract_scenario_ids(manifest)
            ],
            evidence=[json.dumps({"command": command["argv"], "returncode": command["returncode"]})],
        )
    product_summary = json.loads(command["stdout"])
    surface = map_rust_sdk_product_summary(manifest, product_summary)
    surface["command"] = {
        "argv": command["argv"],
        "duration_s": command["duration_s"],
        "returncode": command["returncode"],
    }
    return surface


def run_typescript_sdk_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="tracedb-typescript-sdk-conformance-") as temp_dir:
        summary_path = Path(temp_dir) / "typescript-sdk-smoke.json"
        command = run_command(
            [
                "npm",
                "run",
                "public-http-smoke",
                "--",
                "--summary-json",
                str(summary_path),
            ],
            repo_root / "clients" / "typescript",
        )
        if not command["ok"]:
            return finalize_surface(
                "typescript_sdk",
                "failed",
                [
                    scenario_result(
                        scenario_id,
                        "failed",
                        reason=(
                            "typescript_sdk public HTTP smoke failed: "
                            f"stdout={command['stdout'][-12_000:]} stderr={command['stderr_tail']}"
                        ),
                    )
                    for scenario_id in contract_scenario_ids(manifest)
                ],
                evidence=[json.dumps({"command": command["argv"], "returncode": command["returncode"]})],
            )
        smoke_summary = json.loads(summary_path.read_text())
    surface = map_typescript_sdk_smoke_summary(manifest, smoke_summary)
    surface["command"] = {
        "argv": command["argv"],
        "cwd": str(repo_root / "clients" / "typescript"),
        "duration_s": command["duration_s"],
        "returncode": command["returncode"],
    }
    return surface


def install_python_sdk_package_for_conformance(repo_root: Path, temp_dir: Path) -> tuple[Path, dict[str, Any]]:
    source_dir = repo_root / "clients" / "python"
    package_dir = temp_dir / "python-sdk-package"
    target_dir = temp_dir / "python-sdk-site"
    shutil.copytree(
        source_dir,
        package_dir,
        ignore=shutil.ignore_patterns("build", "*.egg-info", "__pycache__"),
    )
    command = run_command(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            "--target",
            str(target_dir),
            str(package_dir),
        ],
        temp_dir,
    )
    return target_dir, command


def run_python_sdk_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="tracedb-python-sdk-conformance-") as temp_dir:
        temp_path = Path(temp_dir)
        summary_path = Path(temp_dir) / "python-sdk-smoke.json"
        target_dir, install_command = install_python_sdk_package_for_conformance(repo_root, temp_path)
        if not install_command["ok"]:
            return finalize_surface(
                "python_sdk",
                "failed",
                [
                    scenario_result(
                        scenario_id,
                        "failed",
                        reason=(
                            "python_sdk package install failed before HTTP smoke: "
                            f"stdout={install_command['stdout'][-12_000:]} stderr={install_command['stderr_tail']}"
                        ),
                    )
                    for scenario_id in contract_scenario_ids(manifest)
                ],
                evidence=[
                    PYTHON_SDK_CONFORMANCE_EVIDENCE,
                    json.dumps({"command": install_command["argv"], "returncode": install_command["returncode"]}),
                ],
            )
        command = run_command(
            [
                sys.executable,
                "clients/python/http_smoke.py",
                "--summary-json",
                str(summary_path),
            ],
            repo_root,
            env_extra={
                "PYTHONPATH": str(target_dir),
                "TRACEDB_PYTHON_IMPORT_MODE": "installed",
            },
        )
        if not command["ok"]:
            return finalize_surface(
                "python_sdk",
                "failed",
                [
                    scenario_result(
                        scenario_id,
                        "failed",
                        reason=(
                            "python_sdk smoke failed: "
                            f"stdout={command['stdout'][-12_000:]} stderr={command['stderr_tail']}"
                        ),
                    )
                    for scenario_id in contract_scenario_ids(manifest)
                ],
                evidence=[
                    PYTHON_SDK_CONFORMANCE_EVIDENCE,
                    json.dumps({"command": install_command["argv"], "returncode": install_command["returncode"]}),
                    json.dumps({"command": command["argv"], "returncode": command["returncode"]}),
                ],
            )
        smoke_summary = json.loads(summary_path.read_text())
    surface = map_python_sdk_smoke_summary(manifest, smoke_summary)
    surface["install_command"] = {
        "argv": install_command["argv"],
        "duration_s": install_command["duration_s"],
        "returncode": install_command["returncode"],
    }
    surface["command"] = {
        "argv": command["argv"],
        "env": {
            "PYTHONPATH": str(target_dir),
            "TRACEDB_PYTHON_IMPORT_MODE": "installed",
        },
        "duration_s": command["duration_s"],
        "returncode": command["returncode"],
    }
    return surface


def build_report(manifest: dict[str, Any], surfaces: list[dict[str, Any]]) -> dict[str, Any]:
    totals = {
        "surfaces": len(surfaces),
        "complete_surfaces": sum(1 for surface in surfaces if surface["complete"]),
        "passed_scenarios": sum(surface["passed"] for surface in surfaces),
        "required_scenarios": sum(surface["required"] for surface in surfaces),
        "not_checked_scenarios": sum(surface["not_checked"] for surface in surfaces),
        "failed_scenarios": sum(surface["failed"] for surface in surfaces),
    }
    return {
        "ok": all(surface["ok"] for surface in surfaces),
        "complete": all(surface["complete"] for surface in surfaces),
        "mode": "platform-conformance",
        "contract": manifest["contract"],
        "version": manifest.get("version"),
        "sql_compatibility": manifest.get("sql_compatibility"),
        "postgres_compatibility": manifest.get("postgres_compatibility"),
        "totals": totals,
        "surfaces": surfaces,
    }


def write_summary(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def run_selected_surfaces(
    manifest: dict[str, Any],
    repo_root: Path,
    selected_surfaces: list[str],
) -> list[dict[str, Any]]:
    unknown = sorted(set(selected_surfaces) - set(contract_surface_ids(manifest)))
    if unknown:
        raise ValueError(f"unknown platform conformance surface(s): {', '.join(unknown)}")
    surfaces = []
    for surface_id in selected_surfaces:
        if surface_id == "http_direct":
            surfaces.append(run_http_direct_surface(manifest, repo_root))
        elif surface_id == "rust_sdk":
            surfaces.append(run_rust_sdk_surface(manifest, repo_root))
        elif surface_id == "typescript_sdk":
            surfaces.append(run_typescript_sdk_surface(manifest, repo_root))
        elif surface_id == "python_sdk":
            surfaces.append(run_python_sdk_surface(manifest, repo_root))
        else:
            surfaces.append(
                empty_surface_report(
                    manifest,
                    surface_id,
                    "not_run",
                    "surface is declared in Platform Contract v0 but not executable in this runner yet",
                )
            )
    return surfaces


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TraceDB Platform Contract v0 conformance lanes.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT))
    parser.add_argument(
        "--surface",
        action="append",
        dest="surfaces",
        help="Surface id to run. Defaults to http_direct and rust_sdk.",
    )
    parser.add_argument("--summary-json", help="Optional path to write the JSON report.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    contract_path = Path(args.contract)
    if not contract_path.is_absolute():
        contract_path = repo_root / contract_path
    manifest = load_contract(contract_path)
    selected_surfaces = args.surfaces or DEFAULT_SURFACES
    report = build_report(manifest, run_selected_surfaces(manifest, repo_root, selected_surfaces))
    output = json.dumps(report, indent=2, sort_keys=True)
    print(output)
    if args.summary_json:
        write_summary(report, Path(args.summary_json))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
