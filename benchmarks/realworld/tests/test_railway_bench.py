from __future__ import annotations

import unittest
from pathlib import Path

import sys


LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from railway_bench import build_railway_manifest, load_railway_config, redact_env, validate_railway_config


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


if __name__ == "__main__":
    unittest.main()
