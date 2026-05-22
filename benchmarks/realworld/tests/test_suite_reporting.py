from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread


LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from runner.suite import SCENARIOS, build_suite_report, write_suite_markdown
from railway_bench import validate_railway_operation_receipt


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


class StatefulSmokeHandler(ReadyHandler):
    def do_POST(self) -> None:
        body = self._read_body()
        self.server.requests.append({"path": self.path, "body": body})
        if self.path == "/v1/schema/apply":
            self._send_json(200, {"epoch": 1})
            return
        if self.path == "/v1/records/put":
            record = body.get("record", body)
            self.server.records[(record["table"], record["tenant_id"], record["id"])] = {
                "table": record["table"],
                "tenant_id": record["tenant_id"],
                "id": record["id"],
                "fields": record["fields"],
            }
            self._send_json(200, {"epoch": 2})
            return
        if self.path == "/v1/records/get":
            key = (body["table"], body["tenant_id"], body["id"])
            self._send_json(200, {"record": self.server.records.get(key)})
            return
        if self.path == "/v1/admin/snapshot":
            self._send_json(200, {"snapshot": True, "target": body["target"]})
            return
        if self.path == "/v1/admin/restore":
            self._send_json(
                200,
                {
                    "restored": True,
                    "source": body["source"],
                    "target": body["target"],
                },
            )
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


class TestHttpServer:
    def __init__(self, handler: type[BaseHTTPRequestHandler] = ReadyHandler) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.server.records = {}
        self.server.requests = []
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self) -> "TestHttpServer":
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class SuiteReportingTests(unittest.TestCase):
    def test_suite_command_writes_comprehensive_markdown_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env["BENCH_DISABLE_ENV_FILE"] = "1"
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "suite-test",
                    "--reports-dir",
                    str(reports),
                    "--scenarios",
                    "sdk_cli_surface,search_rag_6",
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            suite_json = reports / "suite-test" / "suite.json"
            suite_md = reports / "suite-test" / "suite.md"
            suite_gate = reports / "suite-test" / "suite-gate.json"
            self.assertTrue(suite_json.exists())
            self.assertTrue(suite_md.exists())
            self.assertTrue(suite_gate.exists())
            payload = json.loads(suite_json.read_text())
            gate = json.loads(suite_gate.read_text())
            markdown = suite_md.read_text()

        self.assertEqual(payload["suite_id"], "suite-test")
        self.assertGreaterEqual(len(payload["scenarios"]), 2)
        self.assertEqual(payload["control_status"], "internal_only_smoke")
        self.assertIn("control_ledger", payload)
        self.assertIn("number_to_beat", payload)
        self.assertTrue(
            all("control_status" in scenario for scenario in payload["scenarios"]),
            payload["scenarios"],
        )
        self.assertIn("## Executive Summary", markdown)
        self.assertIn("Control status: `internal_only_smoke`", markdown)
        self.assertIn("## Control Ledger", markdown)
        self.assertIn("Number to beat", markdown)
        self.assertIn("## How to Read This Report", markdown)
        self.assertIn("## Database Roles Compared", markdown)
        self.assertIn("## What We Simulated", markdown)
        self.assertIn("## Scenario Findings", markdown)
        self.assertIn("## Scenario Comparison Matrix", markdown)
        self.assertIn("## Unavailable Baselines and Caveats", markdown)
        self.assertIn("sdk_cli_surface", markdown)
        self.assertIn("search_rag_6", markdown)
        self.assertIn("TraceDB embedded/SDK/CLI usability", markdown)
        self.assertIn("Side-by-side Search/RAG 6 database comparison", markdown)
        self.assertIn("TraceDB result: available", markdown)
        self.assertNotIn("TraceDB was not requested", markdown)
        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["artifact_paths"]["suite_json"], "suite.json")

    def test_suite_spec_command_writes_gate_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env["BENCH_DISABLE_ENV_FILE"] = "1"
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "suite-spec-test",
                    "--reports-dir",
                    str(reports),
                    "--suite-spec",
                    "suites/platform_pr.json",
                    "--scenarios",
                    "sdk_cli_surface",
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            suite_json = reports / "suite-spec-test" / "suite.json"
            suite_gate = reports / "suite-spec-test" / "suite-gate.json"
            self.assertTrue(suite_json.exists())
            self.assertTrue(suite_gate.exists())
            payload = json.loads(suite_json.read_text())
            gate = json.loads(suite_gate.read_text())

        self.assertEqual(payload["records"], 128)
        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["suite_spec"], "platform_pr")
        self.assertEqual(gate["artifact_paths"]["suite_json"], "suite.json")

    def test_railway_config_from_env_writes_manifest_and_feeds_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_DISABLE_ENV_FILE": "1",
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "railway-suite-test",
                    "--reports-dir",
                    str(reports),
                    "--suite-spec",
                    "suites/railway_stateful.json",
                    "--scenarios",
                    "sdk_cli_surface",
                    "--railway-config-from-env",
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            suite_dir = reports / "railway-suite-test"
            gate = json.loads((suite_dir / "suite-gate.json").read_text())
            manifest = json.loads((suite_dir / "railway-manifest.json").read_text())

        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["railway_services"][0]["service_id"], "service_tracedb")
        self.assertEqual(
            gate["artifact_paths"]["railway_manifest_json"],
            "railway-manifest.json",
        )
        self.assertEqual(manifest["status"], "configured")
        self.assertNotIn("railway-token-secret", repr(manifest))

    def test_railway_health_check_writes_endpoint_result_into_manifest_and_gate(self) -> None:
        with TestHttpServer() as server, tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_DISABLE_ENV_FILE": "1",
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "railway-health-suite-test",
                    "--reports-dir",
                    str(reports),
                    "--suite-spec",
                    "suites/railway_stateful.json",
                    "--scenarios",
                    "sdk_cli_surface",
                    "--railway-config-from-env",
                    "--railway-health-check",
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            suite_dir = reports / "railway-health-suite-test"
            gate = json.loads((suite_dir / "suite-gate.json").read_text())
            manifest = json.loads((suite_dir / "railway-manifest.json").read_text())

        self.assertEqual(manifest["endpoint_health"]["status"], "healthy")
        self.assertEqual(manifest["endpoint_health"]["checks"][0]["status_code"], 200)
        self.assertEqual(gate["claim_status"]["railway_endpoint_health"], "healthy")
        self.assertEqual(gate["status"], "usable")

    def test_railway_stateful_smoke_writes_marker_result_into_manifest_and_gate(self) -> None:
        with TestHttpServer(StatefulSmokeHandler) as server, tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_DISABLE_ENV_FILE": "1",
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "railway-stateful-smoke-suite-test",
                    "--reports-dir",
                    str(reports),
                    "--suite-spec",
                    "suites/railway_stateful.json",
                    "--scenarios",
                    "sdk_cli_surface",
                    "--railway-config-from-env",
                    "--railway-stateful-smoke",
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            suite_dir = reports / "railway-stateful-smoke-suite-test"
            gate = json.loads((suite_dir / "suite-gate.json").read_text())
            manifest = json.loads((suite_dir / "railway-manifest.json").read_text())

        self.assertEqual(manifest["stateful_smoke"]["status"], "passed")
        self.assertEqual(
            manifest["stateful_smoke"]["marker"]["table"],
            "railway_stateful_markers",
        )
        self.assertEqual(gate["claim_status"]["railway_stateful_smoke"], "passed")
        self.assertEqual(gate["status"], "usable")

    def test_railway_snapshot_restore_check_writes_manifest_and_gate(self) -> None:
        with TestHttpServer(StatefulSmokeHandler) as server, tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_DISABLE_ENV_FILE": "1",
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                    "TRACEDB_RAILWAY_SNAPSHOT_ROOT": "/srv/tracedb-admin",
                }
            )
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "railway-snapshot-restore-suite-test",
                    "--reports-dir",
                    str(reports),
                    "--suite-spec",
                    "suites/railway_stateful.json",
                    "--scenarios",
                    "sdk_cli_surface",
                    "--railway-config-from-env",
                    "--railway-stateful-smoke",
                    "--railway-snapshot-restore-check",
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            suite_dir = reports / "railway-snapshot-restore-suite-test"
            gate = json.loads((suite_dir / "suite-gate.json").read_text())
            manifest = json.loads((suite_dir / "railway-manifest.json").read_text())
            artifacts = json.loads((suite_dir / "railway-artifacts.json").read_text())

        self.assertEqual(manifest["snapshot_restore"]["status"], "passed")
        self.assertEqual(
            manifest["snapshot_restore"]["paths"]["snapshot"],
            "/srv/tracedb-admin/railway-snapshot-restore-suite-test/"
            f"{manifest['stateful_smoke']['marker']['id']}/snapshot",
        )
        self.assertEqual(gate["claim_status"]["railway_snapshot_restore"], "passed")
        self.assertEqual(gate["status"], "usable")
        self.assertEqual(
            artifacts["railway_claim_status"]["snapshot_restore"],
            "passed",
        )
        self.assertNotIn("railway-token-secret", repr(manifest))

    def test_railway_stateful_read_only_marker_probe_writes_manifest_without_put(self) -> None:
        with TestHttpServer(StatefulSmokeHandler) as server, tempfile.TemporaryDirectory() as temp_dir:
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
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_DISABLE_ENV_FILE": "1",
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "railway-stateful-readonly-suite-test",
                    "--reports-dir",
                    str(reports),
                    "--suite-spec",
                    "suites/railway_stateful.json",
                    "--scenarios",
                    "sdk_cli_surface",
                    "--railway-config-from-env",
                    "--railway-stateful-smoke",
                    "--railway-stateful-read-only",
                    "--railway-stateful-marker-id",
                    "marker-123",
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            paths = [request["path"] for request in server.server.requests]
            suite_dir = reports / "railway-stateful-readonly-suite-test"
            gate = json.loads((suite_dir / "suite-gate.json").read_text())
            manifest = json.loads((suite_dir / "railway-manifest.json").read_text())

        self.assertEqual(manifest["stateful_smoke"]["status"], "passed")
        self.assertEqual(manifest["stateful_smoke"]["mode"], "read_only")
        self.assertEqual(paths, ["/v1/records/get"])
        self.assertEqual(gate["claim_status"]["railway_stateful_smoke"], "passed")
        self.assertEqual(gate["status"], "usable")

    def test_railway_restart_redeploy_plan_writes_manifest_and_gate_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_DISABLE_ENV_FILE": "1",
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "railway-plan-suite-test",
                    "--reports-dir",
                    str(reports),
                    "--suite-spec",
                    "suites/railway_stateful.json",
                    "--scenarios",
                    "sdk_cli_surface",
                    "--railway-config-from-env",
                    "--railway-restart-redeploy-plan",
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            suite_dir = reports / "railway-plan-suite-test"
            gate = json.loads((suite_dir / "suite-gate.json").read_text())
            manifest = json.loads((suite_dir / "railway-manifest.json").read_text())
            artifact_manifest = json.loads((suite_dir / "railway-artifacts.json").read_text())

        self.assertEqual(manifest["operation_plan"]["status"], "plan_only")
        self.assertFalse(manifest["operation_plan"]["execution"]["executed"])
        self.assertEqual(
            gate["claim_status"]["railway_restart_redeploy"],
            "plan_only",
        )
        self.assertEqual(
            gate["artifact_paths"]["railway_artifacts_json"],
            "railway-artifacts.json",
        )
        self.assertEqual(gate["status"], "usable")
        self.assertEqual(artifact_manifest["kind"], "railway_suite_artifact_manifest")
        self.assertEqual(artifact_manifest["railway_claim_status"]["gate_status"], "usable")
        self.assertTrue(
            any(artifact["name"] == "railway_manifest_json" for artifact in artifact_manifest["artifacts"])
        )
        self.assertNotIn("railway-token-secret", repr(manifest))
        self.assertNotIn("railway-token-secret", repr(artifact_manifest))

    def test_railway_operation_receipt_command_writes_valid_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_path = Path(temp_dir) / "operation-receipt.json"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_DISABLE_ENV_FILE": "1",
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": "http://tracedb.railway.internal:8080",
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )

            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "railway-receipt",
                    "--operation",
                    "restart",
                    "--status",
                    "passed",
                    "--suite-id",
                    "railway-receipt-suite-test",
                    "--confirm-executed",
                    "--operator",
                    "benchmark-operator",
                    "--command",
                    "railway restart --service service_tracedb",
                    "--note",
                    "manual restart completed",
                    "--output-json",
                    str(receipt_path),
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            receipt = json.loads(receipt_path.read_text())
            validation = validate_railway_operation_receipt(
                receipt,
                expected_service_id="service_tracedb",
            )

        self.assertTrue(validation["ok"], validation)
        self.assertEqual(receipt["kind"], "railway_operation_receipt")
        self.assertEqual(receipt["operation"], "restart")
        self.assertTrue(receipt["executed"])
        self.assertTrue(receipt["confirmed"])
        self.assertEqual(receipt["service_id"], "service_tracedb")
        self.assertEqual(receipt["suite_id"], "railway-receipt-suite-test")
        self.assertNotIn("railway-token-secret", repr(receipt))

    def test_railway_persistence_verdict_combines_pre_manifest_receipt_and_postcheck(self) -> None:
        with TestHttpServer(StatefulSmokeHandler) as server, tempfile.TemporaryDirectory() as temp_dir:
            marker = {
                "table": "railway_stateful_markers",
                "tenant_id": "railway-smoke",
                "id": "marker-123",
                "run_id": "pre-restart-run",
            }
            server.server.records[
                (marker["table"], marker["tenant_id"], marker["id"])
            ] = {
                "table": marker["table"],
                "id": marker["id"],
                "tenant_id": marker["tenant_id"],
                "fields": {
                    "id": marker["id"],
                    "tenant": marker["tenant_id"],
                    "kind": "railway_stateful_smoke",
                    "run_id": marker["run_id"],
                    "status": "written",
                    "marker_id": marker["id"],
                    "body": "TraceDB Railway stateful smoke marker marker-123",
                },
            }
            pre_manifest_path = Path(temp_dir) / "pre-railway-manifest.json"
            pre_manifest_path.write_text(
                json.dumps(
                    {
                        "status": "configured",
                        "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                        "stateful_smoke": {
                            "status": "passed",
                            "mode": "write_read",
                            "marker": marker,
                        },
                    }
                )
            )
            receipt_path = Path(temp_dir) / "operation-receipt.json"
            receipt_path.write_text(
                json.dumps(
                    {
                        "kind": "railway_operation_receipt",
                        "operation": "restart",
                        "status": "passed",
                        "executed": True,
                        "confirmed": True,
                        "service_id": "service_tracedb",
                        "RAILWAY_API_TOKEN": "railway-token-secret",
                    }
                )
            )
            reports = Path(temp_dir) / "reports"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_DISABLE_ENV_FILE": "1",
                    "RAILWAY_API_TOKEN": "railway-token-secret",
                    "RAILWAY_PROJECT_ID": "project_123",
                    "RAILWAY_ENVIRONMENT_ID": "env_123",
                    "TRACEDB_RAILWAY_SERVICE_ID": "service_tracedb",
                    "TRACEDB_RAILWAY_PRIVATE_URL": server.base_url,
                    "TRACEDB_RAILWAY_VOLUME_PATH": "/data/tracedb",
                }
            )
            completed = subprocess.run(
                [
                    "python3",
                    "-m",
                    "runner",
                    "suite",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "railway-persistence-suite-test",
                    "--reports-dir",
                    str(reports),
                    "--suite-spec",
                    "suites/railway_stateful.json",
                    "--scenarios",
                    "sdk_cli_surface",
                    "--railway-config-from-env",
                    "--railway-stateful-smoke",
                    "--railway-stateful-read-only",
                    "--railway-stateful-marker-id",
                    "marker-123",
                    "--railway-persistence-pre-manifest-json",
                    str(pre_manifest_path),
                    "--railway-operation-receipt-json",
                    str(receipt_path),
                ],
                cwd=LAB_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
            suite_dir = reports / "railway-persistence-suite-test"
            gate = json.loads((suite_dir / "suite-gate.json").read_text())
            manifest = json.loads((suite_dir / "railway-manifest.json").read_text())

        self.assertEqual(manifest["persistence_verdict"]["status"], "passed")
        self.assertEqual(manifest["persistence_verdict"]["marker"]["id"], "marker-123")
        self.assertEqual(gate["claim_status"]["railway_persistence"], "passed")
        self.assertEqual(gate["status"], "usable")
        self.assertNotIn("railway-token-secret", repr(manifest))

    def test_suite_report_marks_unavailable_external_controls(self) -> None:
        child_report = {
            "summary": {
                "failure_count": 0,
                "control_status": "external_control_unavailable",
            },
            "dataset": {"kind": "generated", "source": "test"},
            "surfaces": ["sdk"],
            "openrouter": {},
            "baselines": [
                {
                    "name": "TraceDB",
                    "available": True,
                    "role": "target under test",
                    "metrics": {
                        "ingest_count": 16,
                        "query_count": 4,
                        "latency_p95_ms": 4.0,
                        "recall_at_5": 1.0,
                        "failure_count": 0,
                    },
                    "notes": [],
                },
                {
                    "name": "PostgreSQL",
                    "available": False,
                    "role": "relational control",
                    "metrics": {
                        "ingest_count": 0,
                        "query_count": 0,
                        "latency_p95_ms": 0.0,
                        "recall_at_5": 0.0,
                        "failure_count": 0,
                    },
                    "notes": ["service not configured"],
                },
            ],
        }
        suite = build_suite_report(
            suite_id="suite-controls",
            profile="smoke",
            dataset="generated",
            records=16,
            reports=[
                {
                    "spec": SCENARIOS["search_rag_6"],
                    "report": child_report,
                    "artifact_dir": "/tmp/search_rag_6",
                }
            ],
        )

        self.assertEqual(suite["control_status"], "external_control_unavailable")
        self.assertEqual(suite["summary"]["control_status"], "external_control_unavailable")
        self.assertEqual(
            suite["control_ledger"]["unavailable_external_controls"][0]["name"],
            "PostgreSQL",
        )
        self.assertIsNone(suite["number_to_beat"]["query_p95_ms"]["value"])

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "suite.md"
            write_suite_markdown(suite, path)
            markdown = path.read_text()

        self.assertIn("Control status: `external_control_unavailable`", markdown)
        self.assertIn("no product-language conclusion is valid", markdown)
        self.assertIn("`search_rag_6` / `PostgreSQL`: service not configured", markdown)

    def test_suite_report_preserves_tracedb_query_storage_attribution(self) -> None:
        child_report = {
            "summary": {
                "failure_count": 0,
                "control_status": "external_control_available",
            },
            "dataset": {"kind": "generated", "source": "test"},
            "surfaces": ["http"],
            "openrouter": {},
            "baselines": [
                {
                    "name": "TraceDB",
                    "available": True,
                    "role": "target under test",
                    "metrics": {
                        "ingest_count": 128,
                        "query_count": 4,
                        "latency_p95_ms": 7.0,
                        "query_latency_p95_ms": 3.5,
                        "query_http_client_latency_p95_ms": 3.5,
                        "query_http_client_socket_connect_latency_p95_ms": 0.2,
                        "query_http_client_request_header_write_latency_p95_ms": 0.1,
                        "query_http_client_request_body_write_latency_p95_ms": 0.05,
                        "query_http_client_request_write_latency_p95_ms": 0.15,
                        "query_http_client_response_header_wait_latency_p95_ms": 2.0,
                        "query_http_client_overhead_latency_p95_ms": 1.0,
                        "query_http_client_unattributed_overhead_latency_p95_ms": 0.6,
                        "query_server_engine_latency_p95_ms": 2.0,
                        "query_server_engine_core_latency_p95_ms": 1.1,
                        "query_server_explain_build_latency_p95_ms": 0.2,
                        "query_server_materialize_latency_p95_ms": 0.3,
                        "query_server_response_shape_latency_p95_ms": 0.4,
                        "query_server_body_encode_latency_p95_ms": 0.1,
                        "query_server_prewrite_total_latency_p95_ms": 2.5,
                        "query_engine_phase_total_latency_p95_ms": 2.25,
                        "query_http_response_body_bytes_p95": 4096,
                        "query_http_response_content_length_bytes_p95": 4096,
                        "query_http_response_processing_latency_p95_ms": 0.4,
                        "query_http_response_content_length_mismatch_count": 0,
                        "query_output_probe_count": 3,
                        "query_output_probe_explain_false_query_latency_p95_ms": 3.0,
                        "query_output_probe_explain_false_body_bytes_p95": 1024,
                        "query_output_probe_explain_true_query_latency_p95_ms": 3.5,
                        "query_output_probe_explain_true_body_bytes_p95": 4096,
                        "query_output_probe_explain_endpoint_query_latency_p95_ms": 2.5,
                        "query_output_probe_explain_endpoint_body_bytes_p95": 2048,
                        "query_output_probe_explain_true_over_false_delta_p95_ms": 0.5,
                        "query_output_probe_result_id_mismatch_count": 0,
                        "query_output_probe_explain_false_explain_returned_count": 0,
                        "query_output_probe_order_mode": (
                            "rotated_explain_false_explain_true_explain_endpoint"
                        ),
                        "query_output_probe_shape_count": 3,
                        "query_output_probe_replication_count": 3,
                        "query_output_probe_randomized_order": 0,
                        "query_output_probe_order_valid_for_latency_comparison": 1,
                        "query_output_probe_order_balance_remainder": 0,
                        "query_phase_probe_sample_count": 3,
                        "query_access_path_probe_sample_count": 3,
                        "query_phase_access_path_build_latency_p95_ms": 2.25,
                        "query_access_path_lexicalpath_build_latency_p95_ms": 1.25,
                        "batch_phase_total_latency_p95_ms": 60.0,
                        "batch_phase_wal_total_latency_p95_ms": 26.0,
                        "batch_phase_manifest_total_latency_p95_ms": 9.0,
                        "batch_phase_store_apply_validate_identity_latency_p95_ms": 2.0,
                        "batch_phase_store_apply_validate_vector_latency_p95_ms": 3.0,
                        "batch_phase_store_apply_key_latency_p95_ms": 1.5,
                        "batch_phase_store_apply_fields_latency_p95_ms": 2.5,
                        "batch_phase_store_apply_finalize_identity_latency_p95_ms": 1.25,
                        "batch_phase_store_apply_features_latency_p95_ms": 4.0,
                        "batch_phase_store_apply_install_latency_p95_ms": 1.0,
                        "batch_size_wal_payload_bytes": 315654,
                        "batch_size_wal_frame_bytes": 315690,
                        "batch_size_manifest_bytes": 2293,
                        "disk_bytes": 4096,
                        "disk_bytes_after_ingest_wal": 1024,
                        "disk_bytes_after_ingest_manifest_tdb": 256,
                        "disk_bytes_after_ingest_segments": 2048,
                        "recall_at_5": 1.0,
                        "failure_count": 0,
                    },
                    "notes": [],
                },
                {
                    "name": "pgvector",
                    "available": True,
                    "role": "external vector control",
                    "metrics": {
                        "ingest_count": 128,
                        "query_count": 4,
                        "latency_p95_ms": 2.0,
                        "ingest_latency_p95_ms": 1.0,
                        "ingest_transaction_total_latency_ms": 80.0,
                        "disk_bytes": 2048,
                        "recall_at_5": 1.0,
                        "failure_count": 0,
                    },
                    "notes": [],
                },
            ],
        }

        suite = build_suite_report(
            suite_id="suite-attribution",
            profile="smoke",
            dataset="generated",
            records=128,
            reports=[
                {
                    "spec": SCENARIOS["search_rag_6"],
                    "report": child_report,
                    "artifact_dir": "/tmp/search_rag_6",
                }
            ],
        )

        query_number = suite["number_to_beat"]["query_p95_ms"]
        self.assertEqual(query_number["baseline"], "pgvector")
        self.assertEqual(query_number["source_metric"], "latency_p95_ms")
        self.assertEqual(query_number["scenario_id"], "search_rag_6")

        attribution = suite["tracedb_attribution"][0]
        self.assertEqual(attribution["scenario_id"], "search_rag_6")
        self.assertNotIn("latency_p95_ms", attribution["query"])
        self.assertEqual(attribution["query"]["query_latency_p95_ms"], 3.5)
        self.assertEqual(
            attribution["query_phases"]["access_path_build_latency_p95_ms"],
            2.25,
        )
        self.assertEqual(attribution["query_phases"]["probe_sample_count"], 3)
        self.assertEqual(
            attribution["access_paths"]["lexicalpath_build_latency_p95_ms"],
            1.25,
        )
        self.assertEqual(attribution["access_paths"]["probe_sample_count"], 3)
        self.assertEqual(attribution["http_client"]["latency_p95_ms"], 3.5)
        self.assertEqual(attribution["http_client"]["overhead_latency_p95_ms"], 1.0)
        self.assertEqual(
            attribution["http_client"]["unattributed_overhead_latency_p95_ms"],
            0.6,
        )
        self.assertEqual(
            attribution["http_client"]["socket_connect_latency_p95_ms"],
            0.2,
        )
        self.assertEqual(
            attribution["http_client"]["request_write_latency_p95_ms"],
            0.15,
        )
        self.assertEqual(
            attribution["http_client"]["response_header_wait_latency_p95_ms"],
            2.0,
        )
        self.assertEqual(attribution["response"]["body_bytes_p95"], 4096)
        self.assertEqual(
            attribution["response"]["content_length_bytes_p95"],
            4096,
        )
        self.assertEqual(attribution["output_shape_probe"]["count"], 3)
        self.assertEqual(
            attribution["output_shape_probe"]["explain_false_body_bytes_p95"],
            1024,
        )
        self.assertEqual(
            attribution["output_shape_probe"]["explain_endpoint_query_latency_p95_ms"],
            2.5,
        )
        self.assertEqual(
            attribution["output_shape_probe"]["explain_false_explain_returned_count"],
            0,
        )
        self.assertEqual(attribution["server"]["engine_latency_p95_ms"], 2.0)
        self.assertEqual(attribution["server"]["engine_core_latency_p95_ms"], 1.1)
        self.assertEqual(
            attribution["output_shape_probe"]["order_valid_for_latency_comparison"],
            1,
        )
        self.assertEqual(
            attribution["output_shape_probe"]["order_balance_remainder"],
            0,
        )
        self.assertEqual(attribution["engine"]["phase_total_latency_p95_ms"], 2.25)
        self.assertEqual(attribution["batch_phases"]["total_latency_p95_ms"], 60.0)
        self.assertEqual(attribution["batch_phases"]["wal_total_latency_p95_ms"], 26.0)
        for name, value in {
            "store_apply_validate_identity": 2.0,
            "store_apply_validate_vector": 3.0,
            "store_apply_key": 1.5,
            "store_apply_fields": 2.5,
            "store_apply_finalize_identity": 1.25,
            "store_apply_features": 4.0,
            "store_apply_install": 1.0,
        }.items():
            self.assertEqual(
                attribution["batch_phases"][f"{name}_latency_p95_ms"],
                value,
            )
        self.assertEqual(attribution["batch_sizes"]["wal_payload_bytes"], 315654)
        self.assertEqual(attribution["batch_sizes"]["manifest_bytes"], 2293)
        self.assertEqual(attribution["storage_after_ingest"]["wal"], 1024)
        self.assertEqual(attribution["storage_after_ingest"]["manifest_tdb"], 256)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "suite.md"
            write_suite_markdown(suite, path)
            markdown = path.read_text()

        self.assertIn("## TraceDB Attribution", markdown)
        self.assertIn("engine_latency_p95_ms=2.0", markdown)
        self.assertIn("engine_core_latency_p95_ms=1.1", markdown)
        self.assertIn("overhead_latency_p95_ms=1.0", markdown)
        self.assertIn("body_bytes_p95=4096", markdown)
        self.assertIn("socket_connect_latency_p95_ms=0.2", markdown)
        self.assertIn("request_write_latency_p95_ms=0.15", markdown)
        self.assertIn("response_header_wait_latency_p95_ms=2.0", markdown)
        self.assertIn("output shape probe", markdown)
        self.assertIn("explain_false_body_bytes_p95=1024", markdown)
        self.assertIn("explain_false_explain_returned_count=0", markdown)
        self.assertIn("order_valid_for_latency_comparison=1", markdown)
        self.assertIn("probe_sample_count=3", markdown)
        self.assertIn("access_path_build_latency_p95_ms", markdown)
        self.assertIn("wal_total_latency_p95_ms=26.0", markdown)
        self.assertIn("wal_payload_bytes=315654", markdown)
        self.assertIn("wal=1024", markdown)

    def test_suite_query_comparison_prefers_query_scoped_latency(self) -> None:
        child_report = {
            "summary": {
                "failure_count": 0,
                "control_status": "external_control_available",
            },
            "dataset": {"kind": "generated", "source": "test"},
            "surfaces": ["http"],
            "openrouter": {},
            "baselines": [
                {
                    "name": "TraceDB",
                    "available": True,
                    "role": "target under test",
                    "metrics": {
                        "ingest_count": 1024,
                        "query_count": 6,
                        "latency_p95_ms": 189.272,
                        "query_latency_p50_ms": 2.4,
                        "query_latency_p95_ms": 3.967,
                        "query_latency_p99_ms": 4.1,
                        "recall_at_5": 0.233,
                        "failure_count": 0,
                    },
                    "notes": [],
                },
                {
                    "name": "pgvector",
                    "available": True,
                    "role": "external vector control",
                    "metrics": {
                        "ingest_count": 1024,
                        "query_count": 6,
                        "latency_p95_ms": 90.0,
                        "query_latency_p50_ms": 0.5,
                        "query_latency_p95_ms": 1.18,
                        "query_latency_p99_ms": 1.3,
                        "recall_at_5": 0.233,
                        "failure_count": 0,
                    },
                    "notes": [],
                },
            ],
        }

        suite = build_suite_report(
            suite_id="suite-query-latency",
            profile="smoke",
            dataset="generated",
            records=1024,
            reports=[
                {
                    "spec": SCENARIOS["search_rag_6"],
                    "report": child_report,
                    "artifact_dir": "/tmp/search_rag_6",
                }
            ],
        )

        query_number = suite["number_to_beat"]["query_p95_ms"]
        self.assertEqual(query_number["baseline"], "pgvector")
        self.assertEqual(query_number["source_metric"], "query_latency_p95_ms")
        self.assertEqual(query_number["value"], 1.18)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "suite.md"
            write_suite_markdown(suite, path)
            markdown = path.read_text()

        self.assertIn("Fastest p95 latency: pgvector (1.180 ms p95)", markdown)
        self.assertIn("TraceDB result: available with 1024 ingested records, 6 queries, p95 3.967 ms", markdown)
        self.assertIn("| search_rag_6 | TraceDB | yes | 1024 | n/a | n/a | 6 | 2.4 | 3.967 | 4.1 |", markdown)


if __name__ == "__main__":
    unittest.main()
