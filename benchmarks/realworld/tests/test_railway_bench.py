from __future__ import annotations

import os
import unittest
import json
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import sys


LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from railway_bench import (
    build_railway_artifact_manifest,
    build_railway_backup_receipt,
    build_railway_backup_verdict,
    build_railway_operation_receipt,
    build_railway_operation_plan,
    build_railway_operator_runbook,
    build_railway_manifest,
    build_railway_persistence_verdict,
    build_railway_runbook_verification,
    load_railway_config,
    railway_runbook_verification_markdown,
    redact_env,
    run_railway_endpoint_health,
    run_railway_snapshot_restore_check,
    run_railway_stateful_smoke,
    validate_railway_backup_receipt,
    validate_railway_config,
    validate_railway_operation_receipt,
)


class ReadyHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/ready":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ready":true}')
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


class FailingReadyHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/ready":
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ready":false}')
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


class StatefulSmokeHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/ready":
            self._send_json(200, {"ready": True})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        body = self._read_body()
        self.server.requests.append({"path": self.path, "body": body})
        if self.path == "/v1/schema/apply":
            self.server.schema = body
            self._send_json(200, {"epoch": 1})
            return
        if self.path == "/v1/records/put":
            record = body.get("record", body)
            key = (record["table"], record["tenant_id"], record["id"])
            self.server.records[key] = {
                "table": record["table"],
                "id": record["id"],
                "tenant_id": record["tenant_id"],
                "fields": record["fields"],
            }
            self._send_json(200, {"epoch": 2})
            return
        if self.path == "/v1/records/get":
            key = (body["table"], body["tenant_id"], body["id"])
            self._send_json(200, {"record": self.server.records.get(key)})
            return
        self._send_json(404, {"error": "not found"})

    def _read_body(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length else b"{}"
        return json.loads(raw_body.decode("utf-8"))

    def _send_json(self, status: int, payload: dict) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        return


class MissingMarkerHandler(StatefulSmokeHandler):
    def do_POST(self) -> None:
        body = self._read_body()
        self.server.requests.append({"path": self.path, "body": body})
        if self.path == "/v1/schema/apply":
            self._send_json(200, {"epoch": 1})
            return
        if self.path == "/v1/records/put":
            self._send_json(200, {"epoch": 2})
            return
        if self.path == "/v1/records/get":
            self._send_json(200, {"record": None})
            return
        self._send_json(404, {"error": "not found"})


class SnapshotRestoreHandler(StatefulSmokeHandler):
    def do_POST(self) -> None:
        body = self._read_body()
        self.server.requests.append({"path": self.path, "body": body})
        if self.path == "/v1/admin/snapshot":
            self._send_json(200, {"snapshot": True, "target": body["target"]})
            return
        if self.path == "/v1/admin/restore":
            payload = {
                "restored": True,
                "source": body["source"],
                "target": body["target"],
            }
            if "verify_record" in body:
                payload["verification"] = {
                    "status": "passed",
                    "record_visible": True,
                    "record": {
                        "table": body["verify_record"]["table"],
                        "tenant_id": body["verify_record"]["tenant_id"],
                        "id": body["verify_record"]["id"],
                    },
                }
            self._send_json(200, payload)
            return
        return super().do_POST()


class TestHttpServer:
    def __init__(self, handler: type[BaseHTTPRequestHandler]) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.server.records = {}
        self.server.requests = []
        self.server.schema = None
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self) -> "TestHttpServer":
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class RailwayBenchTests(unittest.TestCase):
    def test_config_validation_accepts_dedicated_trace_service(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        result = validate_railway_config(config)

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["missing"], [])

    def test_manifest_redacts_tokens_and_lists_services(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                "POSTGRES_RAILWAY_SERVICE_ID": "service_postgres",
            }
        )

        manifest = build_railway_manifest(config, suite_id="railway-test")
        encoded = repr(manifest)

        self.assertEqual(manifest["status"], "configured")
        self.assertNotIn("railway-token-secret", encoded)
        self.assertEqual(manifest["services"][0]["role"], "tracedb")
        self.assertEqual(manifest["services"][0]["service_id"], "service_tracedb")
        self.assertEqual(manifest["services"][0]["volume_mount_path"], "/data/tracedb")
        self.assertIn("postgres", [service["role"] for service in manifest["services"]])

    def test_redact_env_removes_sensitive_values(self) -> None:
        redacted = redact_env(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "PATH": "/usr/bin",
            }
        )

        self.assertEqual(redacted["RAILWAY_API_TOKEN"], "<redacted>")
        self.assertEqual(redacted["TRACEDB_RAILWAY_SERVICE_ID"], "service_tracedb")
        self.assertEqual(redacted["PATH"], "/usr/bin")

    def test_endpoint_health_records_ready_probe_without_leaking_token(self) -> None:
        with TestHttpServer(ReadyHandler) as server:
            config = load_railway_config(
                {
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )

            health = run_railway_endpoint_health(config, timeout_seconds=1.0)

        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["base_url"], server.base_url)
        self.assertEqual(health["checks"][0]["name"], "ready")
        self.assertEqual(health["checks"][0]["status_code"], 200)
        self.assertTrue(health["checks"][0]["ok"])
        self.assertNotIn("railway-token-secret", repr(health))

    def test_endpoint_health_returns_unhealthy_for_non_2xx_ready_probe(self) -> None:
        with TestHttpServer(FailingReadyHandler) as server:
            config = load_railway_config(
                {
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )

            health = run_railway_endpoint_health(config, timeout_seconds=1.0)

        self.assertEqual(health["status"], "unhealthy")
        self.assertEqual(health["checks"][0]["status_code"], 503)
        self.assertFalse(health["checks"][0]["ok"])

    def test_manifest_can_include_endpoint_health_result(self) -> None:
        endpoint_health = {
            "status": "healthy",
            "base_url": "http://tracedb.railway.internal:8080",
            "checks": [{"name": "ready", "ok": True, "status_code": 200}],
        }
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        manifest = build_railway_manifest(
            config,
            suite_id="railway-test",
            endpoint_health=endpoint_health,
        )

        self.assertEqual(manifest["endpoint_health"], endpoint_health)

    def test_stateful_smoke_writes_and_reads_marker_without_leaking_token(self) -> None:
        with TestHttpServer(StatefulSmokeHandler) as server:
            config = load_railway_config(
                {
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )

            smoke = run_railway_stateful_smoke(
                config,
                marker_id="marker-123",
                run_id="suite-run-123",
                timeout_seconds=1.0,
                bearer_token="gateway-secret-token",
            )

        self.assertEqual(smoke["status"], "passed")
        self.assertEqual(smoke["marker"]["id"], "marker-123")
        self.assertEqual(smoke["marker"]["table"], "railway_stateful_markers")
        self.assertEqual([op["name"] for op in smoke["operations"]], ["schema_apply", "record_put", "record_get"])
        self.assertTrue(all(op["ok"] for op in smoke["operations"]))
        self.assertNotIn("gateway-secret-token", repr(smoke))
        self.assertNotIn("railway-token-secret", repr(smoke))

    def test_stateful_read_only_probe_does_not_rewrite_marker(self) -> None:
        with TestHttpServer(StatefulSmokeHandler) as server:
            server.server.records[
                ("railway_stateful_markers", "railway-smoke", "marker-123")
            ] = {
                "table": "railway_stateful_markers",
                "id": "marker-123",
                "tenant_id": "railway-smoke",
                "fields": {
                    "id": "marker-123",
                    "tenant": "railway-smoke",
                    "kind": "railway_stateful_smoke",
                    "run_id": "pre-restart-run",
                    "status": "written",
                    "marker_id": "marker-123",
                    "body": "TraceDB Railway stateful smoke marker marker-123",
                },
            }
            config = load_railway_config(
                {
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )

            smoke = run_railway_stateful_smoke(
                config,
                marker_id="marker-123",
                run_id="post-restart-run",
                timeout_seconds=1.0,
                write_marker=False,
            )
            paths = [request["path"] for request in server.server.requests]

        self.assertEqual(smoke["status"], "passed")
        self.assertEqual(smoke["mode"], "read_only")
        self.assertEqual([op["name"] for op in smoke["operations"]], ["record_get"])
        self.assertEqual(paths, ["/v1/records/get"])
        self.assertNotIn("railway-token-secret", repr(smoke))

    def test_stateful_read_only_probe_requires_existing_marker_id(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        smoke = run_railway_stateful_smoke(
            config,
            run_id="post-restart-run",
            write_marker=False,
        )

        self.assertEqual(smoke["status"], "invalid")
        self.assertEqual(smoke["mode"], "read_only")
        self.assertIn("marker_id is required", smoke["errors"][0])

    def test_stateful_smoke_fails_when_marker_is_not_visible(self) -> None:
        with TestHttpServer(MissingMarkerHandler) as server:
            config = load_railway_config(
                {
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )

            smoke = run_railway_stateful_smoke(
                config,
                marker_id="marker-123",
                run_id="suite-run-123",
                timeout_seconds=1.0,
            )

        self.assertEqual(smoke["status"], "failed")
        self.assertTrue(any("not visible" in error for error in smoke["errors"]), smoke)

    def test_manifest_can_include_stateful_smoke_result(self) -> None:
        stateful_smoke = {
            "status": "passed",
            "marker": {"table": "railway_stateful_markers", "id": "marker-123"},
            "operations": [{"name": "record_get", "ok": True, "status_code": 200}],
        }
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        manifest = build_railway_manifest(
            config,
            suite_id="railway-test",
            stateful_smoke=stateful_smoke,
        )

        self.assertEqual(manifest["stateful_smoke"], stateful_smoke)

    def test_snapshot_restore_check_posts_admin_routes_with_safe_paths(self) -> None:
        with TestHttpServer(SnapshotRestoreHandler) as server:
            config = load_railway_config(
                {
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                    "TRACEDB_RAILWAY_SNAPSHOT_ROOT": "/srv/tracedb-admin",
                }
            )

            result = run_railway_snapshot_restore_check(
                config,
                run_id="suite-run-123",
                marker_id="marker-123",
                timeout_seconds=1.0,
                bearer_token="gateway-secret-token",
                verify_restored_marker=True,
            )

        self.assertEqual(result["status"], "passed", result)
        self.assertEqual(
            result["paths"]["snapshot"],
            "/srv/tracedb-admin/suite-run-123/marker-123/snapshot",
        )
        self.assertEqual(
            result["paths"]["restore"],
            "/srv/tracedb-admin/suite-run-123/marker-123/restore",
        )
        self.assertEqual([op["name"] for op in result["operations"]], ["snapshot", "restore"])
        self.assertTrue(all(op["ok"] for op in result["operations"]))
        self.assertEqual(result["restored_read"]["status"], "passed")
        self.assertTrue(result["restored_read"]["record_visible"])
        self.assertIn("not_managed_backup_dr", result["claim_boundary"])
        self.assertNotIn("gateway-secret-token", repr(result))
        self.assertNotIn("railway-token-secret", repr(result))

    def test_snapshot_restore_check_requires_explicit_absolute_snapshot_root(self) -> None:
        missing_root = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )
        relative_root = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                "TRACEDB_RAILWAY_SNAPSHOT_ROOT": "relative/path",
            }
        )
        same_as_volume = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                "TRACEDB_RAILWAY_SNAPSHOT_ROOT": "/data/tracedb",
            }
        )

        missing = run_railway_snapshot_restore_check(missing_root, run_id="suite-run-123")
        invalid = run_railway_snapshot_restore_check(relative_root, run_id="suite-run-123")
        unsafe = run_railway_snapshot_restore_check(same_as_volume, run_id="suite-run-123")

        self.assertEqual(missing["status"], "not_configured")
        self.assertTrue(any("TRACEDB_RAILWAY_SNAPSHOT_ROOT" in error for error in missing["errors"]))
        self.assertEqual(invalid["status"], "invalid")
        self.assertTrue(any("absolute" in error for error in invalid["errors"]))
        self.assertEqual(unsafe["status"], "invalid")
        self.assertTrue(any("must differ" in error for error in unsafe["errors"]))

    def test_manifest_can_include_snapshot_restore_result(self) -> None:
        snapshot_restore = {
            "status": "passed",
            "paths": {"snapshot": "/srv/tracedb-admin/run/marker/snapshot"},
            "operations": [{"name": "snapshot", "ok": True, "status_code": 200}],
        }
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        manifest = build_railway_manifest(
            config,
            suite_id="railway-test",
            snapshot_restore=snapshot_restore,
        )

        self.assertEqual(manifest["snapshot_restore"], snapshot_restore)

    def test_operation_plan_records_restart_redeploy_readiness_without_leaking_token(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        plan = build_railway_operation_plan(config, suite_id="railway-test")

        self.assertEqual(plan["status"], "plan_only")
        self.assertFalse(plan["execution"]["executed"])
        self.assertEqual(plan["service"]["service_id"], "service_tracedb")
        self.assertIn("restart", plan["operations"])
        self.assertIn("redeploy", plan["operations"])
        self.assertIn("railway status --json", [step["command"] for step in plan["preflight"]])
        self.assertTrue(any("railway service status" in step["command"] for step in plan["preflight"]))
        self.assertTrue(any("railway logs --service service_tracedb" in step["command"] for step in plan["preflight"]))
        self.assertIn("plan_only_not_executed", plan["claim_boundary"])
        self.assertNotIn("railway-token-secret", repr(plan))

    def test_operation_plan_marks_missing_config_without_blocking_on_live_mutation(self) -> None:
        config = load_railway_config({"RAILWAY_API_TOKEN": "railway-token-secret"})

        plan = build_railway_operation_plan(config, suite_id="railway-test")

        self.assertEqual(plan["status"], "missing_config")
        self.assertFalse(plan["execution"]["executed"])
        self.assertIn("RAILWAY_PROJECT_ID", plan["missing"])
        self.assertIn("TRACEDB_RAILWAY_SERVICE_ID", plan["missing"])

    def test_manifest_can_include_operation_plan(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )
        operation_plan = build_railway_operation_plan(config, suite_id="railway-test")

        manifest = build_railway_manifest(
            config,
            suite_id="railway-test",
            operation_plan=operation_plan,
        )

        self.assertEqual(manifest["operation_plan"], operation_plan)

    def test_operator_runbook_records_preflight_backup_and_restart_chain(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        runbook = build_railway_operator_runbook(
            config,
            suite_id="soak-runbook",
            suite_spec_id="soak_railway",
            suite_spec_path="suites/soak_railway.json",
            reports_dir="reports",
            railway={
                "required": True,
                "backup_required": True,
                "restart_required": True,
            },
            runbook_json="reports/soak-runbook/railway-runbook.json",
            runbook_verification_json="reports/soak-runbook/railway-runbook-verification.json",
            suite_baseline_dir="reports/history",
        )
        commands = {command["name"]: command for command in runbook["commands"]}

        self.assertEqual(runbook["kind"], "railway_operator_runbook")
        self.assertEqual(runbook["status"], "ready")
        self.assertTrue(runbook["required_evidence"]["backup_receipt"])
        self.assertTrue(runbook["required_evidence"]["operation_receipt"])
        self.assertTrue(runbook["required_evidence"]["runbook_verification"])
        self.assertIn("--preflight-only", commands["preflight_gate"]["command"])
        self.assertIn(
            "--railway-backup-receipt-json reports/soak-runbook/railway-backup-receipt.json",
            commands["preflight_gate"]["command"],
        )
        self.assertIn("railway-backup-receipt", commands["backup_receipt"]["command"])
        self.assertIn("--railway-stateful-smoke", commands["pre_operation_marker"]["command"])
        self.assertIn("railway restart --service service_tracedb", commands["operator_restart"]["command"])
        self.assertTrue(commands["operator_restart"]["manual"])
        self.assertTrue(commands["operator_restart"]["mutates"])
        self.assertFalse(commands["operator_restart"]["execute_by_default"])
        self.assertIn("railway-receipt", commands["operation_receipt"]["command"])
        self.assertIn("--railway-stateful-read-only", commands["post_operation_marker"]["command"])
        self.assertIn(
            "--railway-persistence-pre-manifest-json reports/soak-runbook-pre/railway-manifest.json",
            commands["post_operation_marker"]["command"],
        )
        self.assertIn(
            "railway-runbook-verify --runbook-json reports/soak-runbook/railway-runbook.json",
            commands["runbook_verification"]["command"],
        )
        self.assertIn(
            "--railway-runbook-verification-json reports/soak-runbook/railway-runbook-verification.json",
            commands["verified_suite_gate"]["command"],
        )
        self.assertIn(
            "--suite-baseline-dir reports/history",
            commands["verified_suite_gate"]["command"],
        )
        self.assertEqual(
            runbook["artifact_paths"]["runbook_verification_json"],
            "reports/soak-runbook/railway-runbook-verification.json",
        )
        self.assertEqual(runbook["artifact_paths"]["final_suite_dir"], "reports/soak-runbook")
        self.assertIn("runbook_only", runbook["claim_boundary"])
        self.assertNotIn("railway-token-secret", repr(runbook))

    def test_runbook_verification_completes_required_artifacts(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            runbook = build_railway_operator_runbook(
                config,
                suite_id="soak-runbook",
                suite_spec_id="soak_railway",
                suite_spec_path="suites/soak_railway.json",
                reports_dir=str(reports),
                railway={
                    "required": True,
                    "backup_required": True,
                    "restart_required": True,
                },
            )
            paths = runbook["artifact_paths"]
            marker = {
                "table": "railway_stateful_markers",
                "tenant_id": "railway-smoke",
                "id": "marker-123",
                "run_id": "soak-runbook-pre",
            }

            preflight_dir = Path(paths["preflight_suite_dir"])
            preflight_dir.mkdir(parents=True)
            (preflight_dir / "suite-gate.json").write_text('{"status":"usable"}\n')

            backup_path = Path(paths["backup_receipt_json"])
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path.write_text(
                json.dumps(
                    build_railway_backup_receipt(
                        config,
                        suite_id="soak-runbook",
                        status="passed",
                        backup_id="backup_123",
                        confirmed=True,
                        backup_created=True,
                        restore_validated=True,
                        restore_validation_method="restored marker smoke",
                    )
                )
                + "\n"
            )

            pre_manifest_path = Path(paths["pre_manifest_json"])
            pre_manifest_path.parent.mkdir(parents=True, exist_ok=True)
            pre_manifest = {
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "stateful_smoke": {
                    "status": "passed",
                    "mode": "write_read",
                    "marker": marker,
                },
            }
            pre_manifest_path.write_text(json.dumps(pre_manifest) + "\n")

            receipt = build_railway_operation_receipt(
                config,
                suite_id="soak-runbook",
                operation="restart",
                status="passed",
                executed=True,
                confirmed=True,
                command="railway restart --service service_tracedb",
            )
            receipt_path = Path(paths["operation_receipt_json"])
            receipt_path.parent.mkdir(parents=True, exist_ok=True)
            receipt_path.write_text(json.dumps(receipt) + "\n")

            post_manifest = {
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "stateful_smoke": {
                    "status": "passed",
                    "mode": "read_only",
                    "marker": dict(marker, run_id="soak-runbook-post"),
                },
                "persistence_verdict": {
                    "kind": "railway_persistence_verdict",
                    "status": "passed",
                },
            }
            post_dir = Path(paths["post_operation_suite_dir"])
            post_dir.mkdir(parents=True, exist_ok=True)
            (post_dir / "railway-manifest.json").write_text(json.dumps(post_manifest) + "\n")

            verification = build_railway_runbook_verification(
                runbook,
                root=Path(temp_dir),
                max_age_seconds=3600.0,
            )
            markdown = railway_runbook_verification_markdown(verification)

        self.assertEqual(verification["kind"], "railway_runbook_verification")
        self.assertEqual(verification["status"], "complete")
        self.assertEqual(verification["missing_steps"], [])
        self.assertEqual(verification["failed_steps"], [])
        self.assertEqual(verification["stale_steps"], [])
        self.assertEqual(
            verification["complete_steps"],
            [
                "preflight_gate",
                "backup_receipt",
                "pre_operation_marker",
                "operation_receipt",
                "post_operation_marker",
            ],
        )
        self.assertIn("preflight_gate", markdown)
        self.assertNotIn("railway-token-secret", repr(verification))

    def test_runbook_verification_reports_missing_and_stale_artifacts(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            runbook = build_railway_operator_runbook(
                config,
                suite_id="soak-runbook",
                suite_spec_id="soak_railway",
                suite_spec_path="suites/soak_railway.json",
                reports_dir=str(reports),
                railway={
                    "required": True,
                    "backup_required": True,
                    "restart_required": True,
                },
            )
            backup_path = Path(runbook["artifact_paths"]["backup_receipt_json"])
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path.write_text(
                json.dumps(
                    build_railway_backup_receipt(
                        config,
                        suite_id="soak-runbook",
                        status="passed",
                        backup_id="backup_123",
                        confirmed=True,
                        backup_created=True,
                        restore_validated=True,
                        restore_validation_method="restored marker smoke",
                    )
                )
                + "\n"
            )
            os.utime(backup_path, (1, 1))

            verification = build_railway_runbook_verification(
                runbook,
                root=Path(temp_dir),
                max_age_seconds=1.0,
            )

        self.assertEqual(verification["status"], "blocked")
        self.assertIn("backup_receipt", verification["stale_steps"])
        self.assertIn("post_operation_marker", verification["missing_steps"])
        self.assertNotIn("railway-token-secret", repr(verification))

    def test_operation_receipt_validation_accepts_confirmed_service_scoped_restart(self) -> None:
        receipt = {
            "kind": "railway_operation_receipt",
            "operation": "restart",
            "status": "passed",
            "executed": True,
            "confirmed": True,
            "service_id": "service_tracedb",
            "RAILWAY_API_TOKEN": "railway-token-secret",
        }

        result = validate_railway_operation_receipt(
            receipt,
            expected_service_id="service_tracedb",
        )

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["status"], "valid")
        self.assertEqual(result["missing"], [])
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["receipt"]["RAILWAY_API_TOKEN"], "<redacted>")
        self.assertNotIn("railway-token-secret", repr(result))

    def test_operation_receipt_validation_rejects_unconfirmed_wrong_service_operation(self) -> None:
        receipt = {
            "kind": "railway_operation_receipt",
            "operation": "delete",
            "status": "passed",
            "executed": True,
            "confirmed": False,
            "service_id": "service_other",
        }

        result = validate_railway_operation_receipt(
            receipt,
            expected_service_id="service_tracedb",
        )

        self.assertFalse(result["ok"], result)
        self.assertEqual(result["status"], "invalid")
        self.assertTrue(any("operation" in error for error in result["errors"]))
        self.assertTrue(any("confirmed" in error for error in result["errors"]))
        self.assertTrue(any("service_id" in error for error in result["errors"]))

    def test_operation_receipt_builder_emits_valid_redacted_operator_receipt(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        receipt = build_railway_operation_receipt(
            config,
            suite_id="railway-test",
            operation="restart",
            status="passed",
            executed=True,
            confirmed=True,
            command="railway restart --service service_tracedb",
            operator="benchmark-operator",
            notes=["manual restart completed"],
            extra={"RAILWAY_API_TOKEN": "railway-token-secret"},
        )
        validation = validate_railway_operation_receipt(
            receipt,
            expected_service_id="service_tracedb",
        )

        self.assertEqual(receipt["kind"], "railway_operation_receipt")
        self.assertEqual(receipt["operation"], "restart")
        self.assertTrue(receipt["executed"])
        self.assertTrue(receipt["confirmed"])
        self.assertEqual(receipt["service_id"], "service_tracedb")
        self.assertEqual(receipt["project_id"], "project_123")
        self.assertEqual(receipt["environment_id"], "env_123")
        self.assertIn("receipt_only", receipt["claim_boundary"])
        self.assertTrue(validation["ok"], validation)
        self.assertNotIn("railway-token-secret", repr(receipt))

    def test_backup_receipt_validation_accepts_confirmed_restore_validation(self) -> None:
        receipt = {
            "kind": "railway_backup_receipt",
            "status": "passed",
            "confirmed": True,
            "backup_created": True,
            "restore_validated": True,
            "service_id": "service_tracedb",
            "backup_id": "backup_123",
            "restore_validation_method": "restored marker smoke",
            "RAILWAY_API_TOKEN": "railway-token-secret",
        }

        result = validate_railway_backup_receipt(
            receipt,
            expected_service_id="service_tracedb",
        )

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["status"], "valid")
        self.assertEqual(result["receipt"]["RAILWAY_API_TOKEN"], "<redacted>")
        self.assertNotIn("railway-token-secret", repr(result))

    def test_backup_receipt_validation_rejects_unconfirmed_unvalidated_backup(self) -> None:
        receipt = {
            "kind": "railway_backup_receipt",
            "status": "passed",
            "confirmed": False,
            "backup_created": True,
            "restore_validated": False,
            "service_id": "service_other",
            "backup_id": "",
            "restore_validation_method": "",
        }

        result = validate_railway_backup_receipt(
            receipt,
            expected_service_id="service_tracedb",
        )

        self.assertFalse(result["ok"], result)
        self.assertEqual(result["status"], "invalid")
        self.assertTrue(any("confirmed" in error for error in result["errors"]))
        self.assertTrue(any("restore_validated" in error for error in result["errors"]))
        self.assertTrue(any("service_id" in error for error in result["errors"]))

    def test_backup_receipt_builder_emits_valid_redacted_receipt(self) -> None:
        config = load_railway_config(
            {
                "RAILWAY_API_TOKEN": "railway-token-secret",
                "RAILWAY_PROJECT_ID": "project_123",
                "RAILWAY_ENVIRONMENT_ID": "env_123",
                "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
            }
        )

        receipt = build_railway_backup_receipt(
            config,
            suite_id="railway-backup-test",
            status="passed",
            backup_id="backup_123",
            confirmed=True,
            backup_created=True,
            restore_validated=True,
            restore_validation_method="restored marker smoke",
            operator="benchmark-operator",
            notes=["backup policy checked"],
            extra={"RAILWAY_API_TOKEN": "railway-token-secret"},
        )
        validation = validate_railway_backup_receipt(
            receipt,
            expected_service_id="service_tracedb",
        )

        self.assertEqual(receipt["kind"], "railway_backup_receipt")
        self.assertEqual(receipt["backup_id"], "backup_123")
        self.assertTrue(receipt["confirmed"])
        self.assertTrue(receipt["backup_created"])
        self.assertTrue(receipt["restore_validated"])
        self.assertIn("receipt_only", receipt["claim_boundary"])
        self.assertTrue(validation["ok"], validation)
        self.assertNotIn("railway-token-secret", repr(receipt))

    def test_backup_verdict_passes_for_confirmed_backup_receipt(self) -> None:
        manifest = {
            "status": "configured",
            "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
        }
        receipt = {
            "kind": "railway_backup_receipt",
            "status": "passed",
            "confirmed": True,
            "backup_created": True,
            "restore_validated": True,
            "service_id": "service_tracedb",
            "backup_id": "backup_123",
            "restore_validation_method": "restored marker smoke",
        }

        verdict = build_railway_backup_verdict(manifest, receipt)

        self.assertEqual(verdict["status"], "passed")
        self.assertEqual(verdict["backup"]["backup_id"], "backup_123")
        self.assertEqual(verdict["backup"]["validation"]["status"], "valid")
        self.assertEqual(verdict["errors"], [])

    def test_backup_verdict_fails_without_restore_validation(self) -> None:
        manifest = {
            "status": "configured",
            "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
        }
        receipt = {
            "kind": "railway_backup_receipt",
            "status": "passed",
            "confirmed": True,
            "backup_created": True,
            "restore_validated": False,
            "service_id": "service_tracedb",
            "backup_id": "backup_123",
            "restore_validation_method": "",
        }

        verdict = build_railway_backup_verdict(manifest, receipt)

        self.assertEqual(verdict["status"], "failed")
        self.assertTrue(any("restore" in error for error in verdict["errors"]))

    def test_artifact_manifest_records_suite_files_without_leaking_contents(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            suite_dir = Path(temp_dir)
            (suite_dir / "suite.json").write_text('{"secret":"railway-token-secret"}\n')
            (suite_dir / "suite.md").write_text("# suite\n")
            (suite_dir / "suite-gate.json").write_text('{"status":"usable"}\n')
            (suite_dir / "railway-manifest.json").write_text('{"status":"configured"}\n')

            manifest = build_railway_artifact_manifest(
                suite_dir,
                suite_id="railway-artifacts-test",
                artifact_paths={
                    "suite_json": "suite.json",
                    "suite_md": "suite.md",
                    "suite_gate_json": "suite-gate.json",
                    "railway_manifest_json": "railway-manifest.json",
                },
                railway_manifest={"status": "configured"},
                suite_gate={"status": "usable"},
            )

        artifacts = {artifact["name"]: artifact for artifact in manifest["artifacts"]}
        self.assertEqual(manifest["kind"], "railway_suite_artifact_manifest")
        self.assertEqual(manifest["suite_id"], "railway-artifacts-test")
        self.assertEqual(artifacts["suite_json"]["path"], "suite.json")
        self.assertTrue(artifacts["suite_json"]["exists"])
        self.assertGreater(artifacts["suite_json"]["size_bytes"], 0)
        self.assertRegex(artifacts["suite_json"]["sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(manifest["railway_claim_status"]["manifest_status"], "configured")
        self.assertEqual(manifest["railway_claim_status"]["gate_status"], "usable")
        self.assertIn("snapshot_restore_not_checked", manifest["open_proof_gaps"])
        self.assertNotIn("railway-token-secret", repr(manifest))

    def test_persistence_verdict_passes_for_matching_marker_and_operator_receipt(self) -> None:
        pre_manifest = {
            "status": "configured",
            "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
            "stateful_smoke": {
                "status": "passed",
                "mode": "write_read",
                "marker": {
                    "table": "railway_stateful_markers",
                    "tenant_id": "railway-smoke",
                    "id": "marker-123",
                    "run_id": "pre-restart-run",
                },
            },
        }
        post_manifest = {
            "status": "configured",
            "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
            "stateful_smoke": {
                "status": "passed",
                "mode": "read_only",
                "marker": {
                    "table": "railway_stateful_markers",
                    "tenant_id": "railway-smoke",
                    "id": "marker-123",
                    "run_id": "post-restart-run",
                },
            },
        }
        receipt = {
            "kind": "railway_operation_receipt",
            "operation": "restart",
            "status": "passed",
            "executed": True,
            "confirmed": True,
            "service_id": "service_tracedb",
            "RAILWAY_API_TOKEN": "railway-token-secret",
        }

        verdict = build_railway_persistence_verdict(pre_manifest, post_manifest, receipt)

        self.assertEqual(verdict["status"], "passed")
        self.assertEqual(verdict["operation"]["operation"], "restart")
        self.assertEqual(verdict["marker"]["id"], "marker-123")
        self.assertTrue(verdict["checks"]["pre_marker_written"])
        self.assertTrue(verdict["checks"]["post_marker_visible"])
        self.assertTrue(verdict["checks"]["operation_executed"])
        self.assertTrue(verdict["checks"]["operation_confirmed"])
        self.assertTrue(verdict["checks"]["receipt_valid"])
        self.assertNotIn("railway-token-secret", repr(verdict))

    def test_persistence_verdict_fails_without_confirmed_operator_receipt(self) -> None:
        pre_manifest = {
            "status": "configured",
            "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
            "stateful_smoke": {
                "status": "passed",
                "mode": "write_read",
                "marker": {
                    "table": "railway_stateful_markers",
                    "tenant_id": "railway-smoke",
                    "id": "marker-123",
                    "run_id": "pre-restart-run",
                },
            },
        }
        post_manifest = {
            "status": "configured",
            "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
            "stateful_smoke": {
                "status": "passed",
                "mode": "read_only",
                "marker": {
                    "table": "railway_stateful_markers",
                    "tenant_id": "railway-smoke",
                    "id": "marker-123",
                    "run_id": "post-restart-run",
                },
            },
        }
        receipt = {
            "kind": "railway_operation_receipt",
            "operation": "restart",
            "status": "passed",
            "executed": True,
            "service_id": "service_tracedb",
        }

        verdict = build_railway_persistence_verdict(pre_manifest, post_manifest, receipt)

        self.assertEqual(verdict["status"], "failed")
        self.assertFalse(verdict["checks"]["operation_confirmed"])
        self.assertFalse(verdict["checks"]["receipt_valid"])
        self.assertTrue(any("confirmed" in error for error in verdict["errors"]))

    def test_persistence_verdict_fails_for_marker_mismatch(self) -> None:
        pre_manifest = {
            "stateful_smoke": {
                "status": "passed",
                "mode": "write_read",
                "marker": {"table": "railway_stateful_markers", "tenant_id": "railway-smoke", "id": "marker-a"},
            },
        }
        post_manifest = {
            "stateful_smoke": {
                "status": "passed",
                "mode": "read_only",
                "marker": {"table": "railway_stateful_markers", "tenant_id": "railway-smoke", "id": "marker-b"},
            },
        }
        receipt = {
            "kind": "railway_operation_receipt",
            "operation": "restart",
            "status": "passed",
            "executed": True,
            "confirmed": True,
        }

        verdict = build_railway_persistence_verdict(pre_manifest, post_manifest, receipt)

        self.assertEqual(verdict["status"], "failed")
        self.assertFalse(verdict["checks"]["marker_match"])
        self.assertTrue(any("marker mismatch" in error for error in verdict["errors"]))


if __name__ == "__main__":
    unittest.main()
