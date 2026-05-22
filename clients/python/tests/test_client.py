from __future__ import annotations

import sys
import tomllib
import unittest
from pathlib import Path

CLIENT_ROOT = Path(__file__).resolve().parents[1]
if str(CLIENT_ROOT) not in sys.path:
    sys.path.insert(0, str(CLIENT_ROOT))

from tracedb import TraceDB, TraceDBRequestError  # noqa: E402


class TraceDBClientTests(unittest.TestCase):
    def test_from_env_builds_connection_and_routing_config(self) -> None:
        db = TraceDB.from_env(
            env={
                "TRACEDB_URL": "http://127.0.0.1:8090/",
                "TRACEDB_TOKEN": "dev-token",
                "TRACEDB_DATABASE_ID": "db_local",
                "TRACEDB_BRANCH_ID": "db_local:main",
                "TRACEDB_TIMEOUT_MS": "2500",
            }
        )

        self.assertEqual(db.url, "http://127.0.0.1:8090")
        self.assertEqual(db.token, "dev-token")
        self.assertEqual(db.database_id, "db_local")
        self.assertEqual(db.branch_id, "db_local:main")
        self.assertEqual(db.timeout, 2.5)

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


if __name__ == "__main__":
    unittest.main()
