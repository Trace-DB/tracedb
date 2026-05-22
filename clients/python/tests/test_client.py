from __future__ import annotations

import io
import json
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
                "TRACEDB_IDEMPOTENCY_RETRIES": "1",
            }
        )

        self.assertEqual(db.url, "http://127.0.0.1:8090")
        self.assertEqual(db.token, "dev-token")
        self.assertEqual(db.database_id, "db_local")
        self.assertEqual(db.branch_id, "db_local:main")
        self.assertEqual(db.timeout, 2.5)
        self.assertEqual(db.safe_retries, 2)
        self.assertEqual(db.idempotency_retries, 1)

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

    def test_from_env_rejects_invalid_idempotency_retries(self) -> None:
        with self.assertRaisesRegex(TraceDBRequestError, "TRACEDB_IDEMPOTENCY_RETRIES"):
            TraceDB.from_env(
                env={
                    "TRACEDB_URL": "http://127.0.0.1:8090",
                    "TRACEDB_IDEMPOTENCY_RETRIES": "-1",
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

    def test_traceql_posts_query_string_to_canonical_route_with_routing(self) -> None:
        db = TraceDB(
            "http://127.0.0.1:8090",
            database_id="db-local",
            branch_id="db-local:main",
        )
        captured = []

        def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
            captured.append(request)
            return _FakeResponse('{"results":[{"record_id":"intro"}]}')

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            response = db.traceql("FROM docs\nTENANT tenant-a\nLIMIT 1")

        self.assertEqual(response, {"results": [{"record_id": "intro"}]})
        self.assertEqual(len(captured), 1)
        request = captured[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.full_url, "http://127.0.0.1:8090/v1/traceql")
        self.assertEqual(
            json.loads(request.data.decode("utf-8")),
            {
                "branch_id": "db-local:main",
                "database_id": "db-local",
                "query": "FROM docs\nTENANT tenant-a\nLIMIT 1",
            },
        )

    def test_traceql_safe_retries_retry_read_only_5xx_then_return_json(self) -> None:
        db = TraceDB("http://127.0.0.1:8090", safe_retries=1)
        retry_error = urllib.error.HTTPError(
            "http://127.0.0.1:8090/v1/traceql",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b'{"error":"busy","code":"unavailable"}'),
        )

        with mock.patch(
            "urllib.request.urlopen",
            side_effect=[retry_error, _FakeResponse('{"results":[]}')],
        ) as urlopen:
            self.assertEqual(db.traceql("FROM docs\nTENANT tenant-a\nLIMIT 1"), {"results": []})

        self.assertEqual(urlopen.call_count, 2)

    def test_graphql_posts_query_string_to_canonical_route_with_routing(self) -> None:
        db = TraceDB(
            "http://127.0.0.1:8090",
            database_id="db-local",
            branch_id="db-local:main",
        )
        captured = []

        def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
            captured.append(request)
            return _FakeResponse('{"results":[{"record_id":"intro"}]}')

        query = 'query { docs(tenant_id: "tenant-a", limit: 1) { record_id } }'
        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            response = db.graphql(query)

        self.assertEqual(response, {"results": [{"record_id": "intro"}]})
        self.assertEqual(len(captured), 1)
        request = captured[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.full_url, "http://127.0.0.1:8090/v1/graphql")
        self.assertEqual(
            json.loads(request.data.decode("utf-8")),
            {
                "branch_id": "db-local:main",
                "database_id": "db-local",
                "query": query,
            },
        )

    def test_graphql_safe_retries_retry_read_only_5xx_then_return_json(self) -> None:
        db = TraceDB("http://127.0.0.1:8090", safe_retries=1)
        retry_error = urllib.error.HTTPError(
            "http://127.0.0.1:8090/v1/graphql",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b'{"error":"busy","code":"unavailable"}'),
        )

        with mock.patch(
            "urllib.request.urlopen",
            side_effect=[retry_error, _FakeResponse('{"results":[]}')],
        ) as urlopen:
            self.assertEqual(
                db.graphql('query { docs(tenant_id: "tenant-a", limit: 1) { record_id } }'),
                {"results": []},
            )

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

    def test_idempotency_retries_retry_keyed_mutation_5xx_then_return_json(self) -> None:
        db = TraceDB("http://127.0.0.1:8090", idempotency_retries=1)
        retry_error = urllib.error.HTTPError(
            "http://127.0.0.1:8090/v1/records/put",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b'{"error":"busy","code":"unavailable"}'),
        )

        with mock.patch("urllib.request.urlopen", side_effect=[retry_error, _FakeResponse('{"epoch":9}')]) as urlopen:
            response = db.table("docs").tenant("tenant-a").insert(
                "intro",
                {"body": "hello"},
                idempotency_key="python-insert-1",
            )

        self.assertEqual(response, {"epoch": 9})
        self.assertEqual(urlopen.call_count, 2)

    def test_idempotency_retries_skip_unkeyed_mutation_5xx(self) -> None:
        db = TraceDB("http://127.0.0.1:8090", idempotency_retries=1)
        retry_error = urllib.error.HTTPError(
            "http://127.0.0.1:8090/v1/records/put",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b'{"error":"busy","code":"unavailable"}'),
        )

        with mock.patch("urllib.request.urlopen", side_effect=retry_error) as urlopen:
            with self.assertRaisesRegex(TraceDBHTTPError, "HTTP 503"):
                db.table("docs").tenant("tenant-a").insert("intro", {"body": "hello"})

        self.assertEqual(urlopen.call_count, 1)

    def test_idempotency_retries_do_not_retry_conflicts_or_4xx(self) -> None:
        db = TraceDB("http://127.0.0.1:8090", idempotency_retries=1)
        retry_error = urllib.error.HTTPError(
            "http://127.0.0.1:8090/v1/records/put",
            409,
            "Conflict",
            {},
            io.BytesIO(b'{"error":"idempotency conflict","code":"conflict"}'),
        )

        with mock.patch("urllib.request.urlopen", side_effect=retry_error) as urlopen:
            with self.assertRaisesRegex(TraceDBHTTPError, "HTTP 409"):
                db.table("docs").tenant("tenant-a").insert(
                    "intro",
                    {"body": "hello"},
                    idempotency_key="python-insert-1",
                )

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
        self.assertIn("db.graphql", smoke)
        self.assertIn("db.graphql_request", smoke)
        self.assertIn("TRACEDB_IDEMPOTENCY_RETRIES", smoke)
        self.assertIn("idempotency_retries", smoke)
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
