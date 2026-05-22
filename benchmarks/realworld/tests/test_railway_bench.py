from __future__ import annotations

import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import sys


LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from railway_bench import (
    build_railway_manifest,
    load_railway_config,
    redact_env,
    run_railway_endpoint_health,
    validate_railway_config,
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


class TestHttpServer:
    def __init__(self, handler: type[BaseHTTPRequestHandler]) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
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


if __name__ == "__main__":
    unittest.main()
