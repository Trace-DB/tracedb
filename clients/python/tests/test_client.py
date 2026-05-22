from __future__ import annotations

import io
import sys
import tomllib
import unittest
import urllib.error
from unittest import mock
from pathlib import Path

CLIENT_ROOT = Path(__file__).resolve().parents[1]
if str(CLIENT_ROOT) not in sys.path:
    sys.path.insert(0, str(CLIENT_ROOT))

from tracedb import TraceDB, TraceDBHTTPError, TraceDBRequestError  # noqa: E402


class TraceDBClientTests(unittest.TestCase):
    def test_from_env_builds_connection_and_routing_config(self) -> None:
        db = TraceDB.from_env(
            env={
                "TRACEDB_URL": "http://127.0.0.1:8090/",
                "TRACEDB_TOKEN": "dev-token",
                "TRACEDB_DATABASE_ID": "db_local",
                "TRACEDB_BRANCH_ID": "db_local:main",
                "TRACEDB_TIMEOUT_MS": "2500",
                "TRACEDB_SAFE_RETRIES": "2",
            }
        )

        self.assertEqual(db.url, "http://127.0.0.1:8090")
        self.assertEqual(db.token, "dev-token")
        self.assertEqual(db.database_id, "db_local")
        self.assertEqual(db.branch_id, "db_local:main")
        self.assertEqual(db.timeout, 2.5)
        self.assertEqual(db.safe_retries, 2)

    def test_from_env_allows_explicit_overrides(self) -> None:
        db = TraceDB.from_env(
            url="http://override:8090",
            token="override-token",
            database_id="override-db",
            branch_id="override-branch",
            timeout=1.25,
            env={
                "TRACEDB_URL": "http://env:8090",
                "TRACEDB_TOKEN": "env-token",
                "TRACEDB_DATABASE_ID": "env-db",
                "TRACEDB_BRANCH_ID": "env-branch",
                "TRACEDB_TIMEOUT_MS": "2500",
            },
        )

        self.assertEqual(db.url, "http://override:8090")
        self.assertEqual(db.token, "override-token")
        self.assertEqual(db.database_id, "override-db")
        self.assertEqual(db.branch_id, "override-branch")
        self.assertEqual(db.timeout, 1.25)

    def test_from_env_rejects_missing_url(self) -> None:
        with self.assertRaisesRegex(TraceDBRequestError, "TRACEDB_URL"):
            TraceDB.from_env(env={})

    def test_from_env_rejects_invalid_timeout(self) -> None:
        with self.assertRaisesRegex(TraceDBRequestError, "TRACEDB_TIMEOUT_MS"):
            TraceDB.from_env(
                env={
                    "TRACEDB_URL": "http://127.0.0.1:8090",
                    "TRACEDB_TIMEOUT_MS": "0",
                }
            )

    def test_from_env_rejects_invalid_safe_retries(self) -> None:
        with self.assertRaisesRegex(TraceDBRequestError, "TRACEDB_SAFE_RETRIES"):
            TraceDB.from_env(
                env={
                    "TRACEDB_URL": "http://127.0.0.1:8090",
                    "TRACEDB_SAFE_RETRIES": "-1",
                }
            )

    def test_safe_retries_retry_read_only_5xx_then_return_json(self) -> None:
        db = TraceDB("http://127.0.0.1:8090", safe_retries=1)
        retry_error = urllib.error.HTTPError(
            "http://127.0.0.1:8090/v1/health",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b'{"error":"busy","code":"unavailable"}'),
        )

        with mock.patch("urllib.request.urlopen", side_effect=[retry_error, _FakeResponse('{"ok":true}')]) as urlopen:
            self.assertEqual(db.health(), {"ok": True})

        self.assertEqual(urlopen.call_count, 2)

    def test_safe_retries_do_not_retry_mutation_5xx(self) -> None:
        db = TraceDB("http://127.0.0.1:8090", safe_retries=1)
        retry_error = urllib.error.HTTPError(
            "http://127.0.0.1:8090/v1/schema/apply",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b'{"error":"busy","code":"unavailable"}'),
        )

        with mock.patch("urllib.request.urlopen", side_effect=retry_error) as urlopen:
            with self.assertRaisesRegex(TraceDBHTTPError, "HTTP 503"):
                db.apply_schema({"name": "docs"})

        self.assertEqual(urlopen.call_count, 1)

    def test_pyproject_declares_stdlib_package_boundary(self) -> None:
        pyproject = tomllib.loads((CLIENT_ROOT / "pyproject.toml").read_text())
        project = pyproject["project"]

        self.assertEqual(project["name"], "tracedb")
        self.assertEqual(project["version"], "0.1.0")
        self.assertEqual(project["requires-python"], ">=3.11")
        self.assertEqual(project.get("dependencies", []), [])
        self.assertIn(
            "tracedb",
            pyproject["tool"]["setuptools"]["packages"],
        )

    def test_install_smoke_declares_clean_venv_package_install(self) -> None:
        smoke = (CLIENT_ROOT / "install_smoke.py").read_text()

        self.assertIn("venv.EnvBuilder", smoke)
        self.assertIn("pip", smoke)
        self.assertIn("--no-deps", smoke)
        self.assertIn("--target", smoke)
        self.assertIn("TraceDB.from_env", smoke)
        self.assertIn("python sdk install smoke ok", smoke)


class _FakeResponse:
    status = 200

    def __init__(self, body: str) -> None:
        self.body = body.encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def read(self) -> bytes:
        return self.body


if __name__ == "__main__":
    unittest.main()
