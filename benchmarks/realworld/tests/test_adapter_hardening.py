from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from runner.adapters.qdrant import QdrantAdapter
from runner.adapters.pgvector import PgVectorAdapter
from runner.adapters.tracedb import TraceDbAdapter
from runner.datasets import generated_dataset, load_dataset
from runner.http import request_json
from runner.metrics import recall_at_k, same_file_recall_at_k
from runner.report import build_report, write_markdown
from runner.types import RunConfig


class FakeQdrant:
    def __init__(self) -> None:
        self.batch_sizes: list[int] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def start(self) -> "FakeQdrant":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _handler(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def _json(
                self, status: int, payload: dict, headers: dict[str, str] | None = None
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body)

            def do_PUT(self) -> None:
                length = int(self.headers.get("content-length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                if self.path.endswith("/points?wait=true"):
                    owner.batch_sizes.append(len(payload.get("points", [])))
                self._json(200, {"result": True, "status": "ok"})

            def do_POST(self) -> None:
                self._json(200, {"result": []})

        return Handler


class FakeTraceDb:
    def __init__(self) -> None:
        self.records: dict[tuple[str, str, str], dict] = {}
        self.batch_sizes: list[int] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def start(self) -> "FakeTraceDb":
        self.thread.start()
        return self

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _handler(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def _payload(self) -> dict:
                length = int(self.headers.get("content-length", "0"))
                if length == 0:
                    return {}
                return json.loads(self.rfile.read(length).decode("utf-8") or "{}")

            def _json(
                self, status: int, payload: dict, headers: dict[str, str] | None = None
            ) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                if self.path == "/ready":
                    self._json(200, {"ok": True})
                    return
                self._json(404, {"error": "not found"})

            def do_POST(self) -> None:
                payload = self._payload()
                if self.path == "/v1/schema/apply":
                    self._json(200, {"ok": True})
                    return
                if self.path == "/v1/records/put":
                    key = (payload["table"], payload["tenant_id"], payload["id"])
                    owner.records[key] = {
                        "table": payload["table"],
                        "tenant_id": payload["tenant_id"],
                        "record_id": payload["id"],
                        "fields": dict(payload.get("fields", {})),
                        "deleted": False,
                    }
                    self._json(200, {"epoch": len(owner.records)})
                    return
                if self.path == "/v1/records/put-batch":
                    records = payload.get("records", [])
                    owner.batch_sizes.append(len(records))
                    for record in records:
                        key = (record["table"], record["tenant_id"], record["id"])
                        owner.records[key] = {
                            "table": record["table"],
                            "tenant_id": record["tenant_id"],
                            "record_id": record["id"],
                            "fields": dict(record.get("fields", {})),
                            "deleted": False,
                        }
                    self._json(
                        200,
                        {"epoch": len(owner.records), "record_count": len(records)},
                    )
                    return
                if self.path == "/v1/records/get":
                    key = (payload["table"], payload["tenant_id"], payload["id"])
                    record = owner.records.get(key)
                    self._json(
                        200,
                        {"record": None if record is None or record["deleted"] else record},
                    )
                    return
                if self.path == "/v1/records/patch":
                    key = (payload["table"], payload["tenant_id"], payload["id"])
                    owner.records[key]["fields"].update(payload.get("fields", {}))
                    self._json(200, {"epoch": len(owner.records) + 1})
                    return
                if self.path == "/v1/query":
                    results = []
                    for record in owner.records.values():
                        if record["deleted"]:
                            continue
                        fields = record["fields"]
                        if record["table"] != payload["table"]:
                            continue
                        if record["tenant_id"] != payload["tenant_id"]:
                            continue
                        if fields.get("category") != payload.get("scalar_eq", {}).get("category"):
                            continue
                        results.append({"record_id": record["record_id"]})
                    self._json(
                        200,
                        {
                            "results": results[: int(payload.get("top_k", 5))],
                            "explain": {
                                "opened_candidate_streams": ["LexicalPath"],
                                "fusion_method": "fake",
                                "freshness_mode": payload.get("freshness", "AllowDirty"),
                                "scalar_filter_applied": True,
                                "tenant_mask_visible_records": len(results),
                                "scalar_filter_visible_records": len(results),
                                "scalar_filter_removed_records": 0,
                                "candidate_budget": payload.get("top_k", 5),
                                "returned_count": len(results[: int(payload.get("top_k", 5))]),
                                "phase_timings": [
                                    {"phase": "tenant_visibility", "elapsed_ms": 1.25},
                                    {"phase": "access_path_build", "elapsed_ms": 2.5},
                                    {"phase": "fusion", "elapsed_ms": 0.75},
                                    {"phase": "materialization", "elapsed_ms": 0.5},
                                ],
                                "access_path_timings": [
                                    {
                                        "access_path_id": "LexicalPath",
                                        "build_ms": 1.5,
                                        "open_ms": 0.25,
                                    },
                                    {
                                        "access_path_id": "VectorPath",
                                        "build_ms": 3.0,
                                        "open_ms": 0.5,
                                    },
                                ],
                            },
                        },
                        {
                            "Server-Timing": (
                                "read;dur=0.01, parse;dur=0.02, lock_wait;dur=0.03, "
                                "engine;dur=0.04, encode;dur=0.05, prewrite_total;dur=0.15"
                            )
                        },
                    )
                    return
                if self.path in {"/v1/admin/compact", "/v1/admin/snapshot", "/v1/admin/restore"}:
                    self._json(200, {"ok": True})
                    return
                if self.path == "/v1/records/delete":
                    key = (payload["table"], payload["tenant_id"], payload["id"])
                    owner.records[key]["deleted"] = True
                    self._json(200, {"epoch": len(owner.records) + 2})
                    return
                self._json(404, {"error": "not found"})

        return Handler


class FakePsycopg:
    def __init__(self, storage_bytes: int = 49_152) -> None:
        self.storage_bytes = storage_bytes
        self.connection = FakePsycopgConnection(storage_bytes)
        self.connect_calls: list[tuple[str, int]] = []

    def connect(self, dsn: str, connect_timeout: int):
        self.connect_calls.append((dsn, connect_timeout))
        return self.connection


class FakePsycopgConnection:
    def __init__(self, storage_bytes: int) -> None:
        self.storage_bytes = storage_bytes
        self.records: list[tuple[str, str, str, str, str]] = []
        self.commit_count = 0
        self.cursor_instance = FakePsycopgCursor(self)

    def __enter__(self) -> "FakePsycopgConnection":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def cursor(self) -> "FakePsycopgCursor":
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_count += 1


class FakePsycopgCursor:
    def __init__(self, connection: FakePsycopgConnection) -> None:
        self.connection = connection
        self.executed_sql: list[str] = []
        self.last_rows: list[tuple] = []

    def __enter__(self) -> "FakePsycopgCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> "FakePsycopgCursor":
        normalized = " ".join(sql.split())
        self.executed_sql.append(normalized)
        if "pg_total_relation_size" in normalized:
            self.last_rows = [(self.connection.storage_bytes,)]
            return self
        if normalized.startswith("SELECT id FROM bench_vectors"):
            tenant_id, category, _vector, top_k = params or ("", "", "", 0)
            matches = [
                (record_id,)
                for record_id, record_tenant, record_category, _body, _embedding in self.connection.records
                if record_tenant == tenant_id and record_category == category
            ]
            self.last_rows = matches[: int(top_k)]
            return self
        if normalized.startswith("INSERT INTO bench_vectors"):
            if params is not None:
                self.connection.records.append(params)
            self.last_rows = []
            return self
        self.last_rows = []
        return self

    def executemany(self, sql: str, rows: list[tuple[str, str, str, str, str]]) -> None:
        self.executed_sql.append(" ".join(sql.split()))
        self.connection.records.extend(rows)

    def fetchall(self) -> list[tuple]:
        return self.last_rows

    def fetchone(self) -> tuple | None:
        return self.last_rows[0] if self.last_rows else None


class AdapterHardeningTests(unittest.TestCase):
    def test_external_qrels_loaders_use_current_hf_configs(self) -> None:
        class FakeDataset(list):
            def shuffle(self, seed: int) -> "FakeDataset":
                return self

            def select(self, indexes) -> "FakeDataset":
                return FakeDataset([self[index] for index in indexes])

        calls: list[tuple[str, tuple[str, ...], str]] = []

        def fake_load_dataset(name: str, *configs: str, split: str):
            calls.append((name, configs, split))
            fixtures = {
                ("mteb/scifact", ("corpus",), "corpus"): FakeDataset(
                    [{"_id": "sci-1", "title": "SciFact", "text": "claim evidence"}]
                ),
                ("mteb/scifact", ("queries",), "queries"): FakeDataset(
                    [{"_id": "sci-q", "text": "claim"}]
                ),
                ("mteb/scifact", ("default",), "test"): FakeDataset(
                    [{"query-id": "sci-q", "corpus-id": "sci-1", "score": 1.0}]
                ),
                ("mteb/CodeSearchNetRetrieval", ("python-corpus",), "test"): FakeDataset(
                    [{"id": "code-1", "title": "", "text": "def parse_trace(): pass"}]
                ),
                ("mteb/CodeSearchNetRetrieval", ("python-queries",), "test"): FakeDataset(
                    [{"id": "code-q", "text": "parse a trace"}]
                ),
                ("mteb/CodeSearchNetRetrieval", ("python-qrels",), "test"): FakeDataset(
                    [{"query-id": "code-q", "corpus-id": "code-1", "score": 1}]
                ),
            }
            key = (name, configs, split)
            if key not in fixtures:
                raise AssertionError(f"unexpected load_dataset call {key!r}")
            return fixtures[key]

        old_datasets = sys.modules.get("datasets")
        sys.modules["datasets"] = types.SimpleNamespace(load_dataset=fake_load_dataset)
        try:
            scifact = load_dataset("beir_scifact", 1, 42)
            codesearch = load_dataset("codesearchnet", 1, 42)
        finally:
            if old_datasets is None:
                sys.modules.pop("datasets", None)
            else:
                sys.modules["datasets"] = old_datasets

        self.assertEqual(scifact.relevance_label_mode, "external_qrels")
        self.assertEqual(scifact.queries[0].expected_ids, ["sci-1"])
        self.assertEqual(codesearch.relevance_label_mode, "external_qrels")
        self.assertEqual(codesearch.queries[0].expected_ids, ["code-1"])
        self.assertIn(("mteb/scifact", ("default",), "test"), calls)
        self.assertIn(("mteb/CodeSearchNetRetrieval", ("python-qrels",), "test"), calls)

    def test_codesearchnet_codeaware_variant_materializes_path_and_identifier_terms(self) -> None:
        class FakeDataset(list):
            def shuffle(self, seed: int) -> "FakeDataset":
                return self

            def select(self, indexes) -> "FakeDataset":
                return FakeDataset([self[index] for index in indexes])

        def fake_load_dataset(name: str, *configs: str, split: str):
            fixtures = {
                ("mteb/CodeSearchNetRetrieval", ("python-corpus",), "test"): FakeDataset(
                    [
                        {
                            "id": "repo/optimizer/nelder_mead.py#L459-L470",
                            "title": "",
                            "text": "def _accept_reflected_fn(simplex): pass",
                        }
                    ]
                ),
                ("mteb/CodeSearchNetRetrieval", ("python-queries",), "test"): FakeDataset(
                    [
                        {
                            "id": "code-q",
                            "text": "Creates the condition function pair for a reflection to be accepted.",
                        }
                    ]
                ),
                ("mteb/CodeSearchNetRetrieval", ("python-qrels",), "test"): FakeDataset(
                    [
                        {
                            "query-id": "code-q",
                            "corpus-id": "repo/optimizer/nelder_mead.py#L459-L470",
                            "score": 1,
                        }
                    ]
                ),
            }
            key = (name, configs, split)
            if key not in fixtures:
                raise AssertionError(f"unexpected load_dataset call {key!r}")
            return fixtures[key]

        old_datasets = sys.modules.get("datasets")
        sys.modules["datasets"] = types.SimpleNamespace(load_dataset=fake_load_dataset)
        try:
            body = load_dataset("codesearchnet_body", 1, 42)
            codeaware = load_dataset("codesearchnet_codeaware", 1, 42)
        finally:
            if old_datasets is None:
                sys.modules.pop("datasets", None)
            else:
                sys.modules["datasets"] = old_datasets

        self.assertEqual(body.kind, "codesearchnet_body")
        self.assertEqual(codeaware.kind, "codesearchnet_codeaware")
        self.assertEqual(body.queries[0].expected_ids, codeaware.queries[0].expected_ids)
        self.assertNotIn("nelder_mead.py", body.records[0].body)
        self.assertIn("nelder", codeaware.records[0].body)
        self.assertIn("mead", codeaware.records[0].body)
        self.assertIn("accept", codeaware.records[0].body)
        self.assertIn("reflect", codeaware.records[0].body)
        self.assertIn("accept", codeaware.queries[0].text)
        self.assertIn("reflect", codeaware.queries[0].text)
        self.assertTrue(
            any("code-aware lexical" in note for note in codeaware.relevance_label_notes),
            codeaware.relevance_label_notes,
        )

    def test_same_file_recall_distinguishes_span_miss_from_wrong_file(self) -> None:
        expected = [
            "https://github.com/apache/airflow/blob/rev/airflow/models/taskinstance.py#L470-L474"
        ]
        same_file_actual = [
            "https://github.com/apache/airflow/blob/rev/airflow/models/taskinstance.py#L197-L208"
        ]
        wrong_file_actual = [
            "https://github.com/apache/airflow/blob/rev/airflow/models/xcom.py#L188-L218"
        ]

        self.assertEqual(recall_at_k(expected, same_file_actual, 5), 0.0)
        self.assertEqual(same_file_recall_at_k(expected, same_file_actual, 5), 1.0)
        self.assertEqual(same_file_recall_at_k(expected, wrong_file_actual, 5), 0.0)

    def test_qdrant_ingestion_is_batched(self) -> None:
        fake = FakeQdrant().start()
        old_url = os.environ.get("BENCH_QDRANT_URL")
        old_batch = os.environ.get("BENCH_QDRANT_BATCH_SIZE")
        os.environ["BENCH_QDRANT_URL"] = fake.base_url
        os.environ["BENCH_QDRANT_BATCH_SIZE"] = "50"
        try:
            dataset = generated_dataset(130, 42)
            result = QdrantAdapter().run(
                dataset,
                RunConfig(
                    profile="smoke",
                    target=["qdrant"],
                    surfaces=["sdk"],
                    require_services=False,
                    repo_root=".",
                ),
            )
        finally:
            if old_url is None:
                os.environ.pop("BENCH_QDRANT_URL", None)
            else:
                os.environ["BENCH_QDRANT_URL"] = old_url
            if old_batch is None:
                os.environ.pop("BENCH_QDRANT_BATCH_SIZE", None)
            else:
                os.environ["BENCH_QDRANT_BATCH_SIZE"] = old_batch
            fake.stop()

        self.assertTrue(result["available"], result["notes"])
        self.assertEqual(fake.batch_sizes, [50, 50, 30])

    def test_pgvector_reports_ingest_query_and_storage_metrics(self) -> None:
        fake_psycopg = FakePsycopg(storage_bytes=65_536)
        old_psycopg = sys.modules.get("psycopg")
        old_dsn = os.environ.get("BENCH_PGVECTOR_DSN")
        sys.modules["psycopg"] = fake_psycopg
        os.environ["BENCH_PGVECTOR_DSN"] = "postgresql://bench:bench@127.0.0.1:25433/bench"
        try:
            result = PgVectorAdapter().run(
                generated_dataset(16, 42),
                RunConfig(
                    profile="smoke",
                    target=["pgvector"],
                    surfaces=["sdk"],
                    require_services=True,
                    repo_root=".",
                ),
            )
        finally:
            if old_psycopg is None:
                sys.modules.pop("psycopg", None)
            else:
                sys.modules["psycopg"] = old_psycopg
            if old_dsn is None:
                os.environ.pop("BENCH_PGVECTOR_DSN", None)
            else:
                os.environ["BENCH_PGVECTOR_DSN"] = old_dsn

        self.assertTrue(result["available"], result["notes"])
        self.assertEqual(result["metrics"]["ingest_count"], 16)
        self.assertEqual(result["metrics"]["ingest_transaction_count"], 1)
        self.assertIn("ingest_transaction_total_latency_ms", result["metrics"])
        self.assertIn("single_transaction_row_insert_latency_p95_ms", result["metrics"])
        self.assertIn("single_transaction_commit_latency_p95_ms", result["metrics"])
        self.assertEqual(result["metrics"]["disk_bytes"], 65_536)
        self.assertIn("ingest_latency_p95_ms", result["metrics"])
        self.assertIn("ingest_commit_latency_p95_ms", result["metrics"])
        self.assertIn("setup_latency_p95_ms", result["metrics"])
        self.assertIn("query_latency_p95_ms", result["metrics"])
        self.assertEqual(result["metrics"]["latency_p95_ms"], result["metrics"]["query_latency_p95_ms"])
        self.assertEqual(fake_psycopg.connection.commit_count, 1)
        self.assertTrue(
            any("bulk transaction" in note for note in result["notes"]),
            result["notes"],
        )
        self.assertTrue(
            any(
                "pg_total_relation_size" in sql
                for sql in fake_psycopg.connection.cursor_instance.executed_sql
            )
        )
        self.assertEqual(fake_psycopg.connect_calls[0][1], 2)

    def test_request_json_sends_optional_bearer_token(self) -> None:
        seen_headers: list[str] = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def do_POST(self) -> None:
                seen_headers.append(self.headers.get("authorization", ""))
                body = b'{"ok": true}'
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        old_token = os.environ.get("TRACEDB_HTTP_BEARER_TOKEN")
        try:
            os.environ["TRACEDB_HTTP_BEARER_TOKEN"] = "bench-token"
            thread.start()
            host, port = server.server_address
            response = request_json("POST", f"http://{host}:{port}/v1/query", {"q": "x"})
        finally:
            if old_token is None:
                os.environ.pop("TRACEDB_HTTP_BEARER_TOKEN", None)
            else:
                os.environ["TRACEDB_HTTP_BEARER_TOKEN"] = old_token
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertEqual(response, {"ok": True})
        self.assertEqual(seen_headers, ["Bearer bench-token"])

    def test_request_json_retries_transient_connection_close(self) -> None:
        calls = {"count": 0}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def do_POST(self) -> None:
                calls["count"] += 1
                if calls["count"] == 1:
                    self.connection.close()
                    return
                body = b'{"ok": true}'
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        try:
            thread.start()
            host, port = server.server_address
            response = request_json(
                "POST",
                f"http://{host}:{port}/v1/query",
                {"q": "x"},
                timeout=2,
                retries=1,
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertEqual(response, {"ok": True})
        self.assertEqual(calls["count"], 2)

    def test_tracedb_adapter_distinguishes_local_and_remote_snapshot_paths(self) -> None:
        adapter = TraceDbAdapter()

        self.assertTrue(adapter._is_local_http_url("http://127.0.0.1:8080"))
        self.assertTrue(adapter._is_local_http_url("http://localhost:8080"))
        self.assertFalse(adapter._is_local_http_url("https://tracedb-engine-production.up.railway.app"))
        self.assertEqual(adapter._path_token("railway/run:alpha"), "railway_run_alpha")

    def test_tracedb_cli_surface_reports_measured_command_metrics(self) -> None:
        old_cli = os.environ.get("TRACEDB_CLI")
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                cli = Path(temp_dir) / "tracedb"
                cli.write_text("#!/bin/sh\nprintf '{\"ok\":true}\\n'\n", encoding="utf-8")
                cli.chmod(0o755)
                os.environ["TRACEDB_CLI"] = str(cli)

                result = TraceDbAdapter().run(
                    generated_dataset(8, 42),
                    RunConfig(
                        profile="smoke",
                        target=["tracedb"],
                        surfaces=["cli"],
                        require_services=False,
                        repo_root=".",
                    ),
                )
        finally:
            if old_cli is None:
                os.environ.pop("TRACEDB_CLI", None)
            else:
                os.environ["TRACEDB_CLI"] = old_cli

        self.assertTrue(result["available"], result["notes"])
        self.assertEqual(result["metrics"]["cli_command_count"], 4)
        self.assertIn("cli_latency_p50_ms", result["metrics"])
        self.assertIn("cli_latency_p95_ms", result["metrics"])
        self.assertIn("cli_latency_p99_ms", result["metrics"])
        self.assertTrue(
            any("cli_command_count=4" in note for note in result["notes"]),
            result["notes"],
        )

    def test_tracedb_http_surface_reports_split_admin_metrics(self) -> None:
        fake = FakeTraceDb().start()
        old_url = os.environ.get("TRACEDB_HTTP_URL")
        old_data_dir = os.environ.get("TRACEDB_HTTP_DATA_DIR")
        try:
            with tempfile.TemporaryDirectory() as data_dir:
                Path(data_dir, "wal.twal").write_bytes(b"x" * 128)
                os.environ["TRACEDB_HTTP_URL"] = fake.base_url
                os.environ["TRACEDB_HTTP_DATA_DIR"] = data_dir
                result = TraceDbAdapter().run(
                    generated_dataset(12, 42),
                    RunConfig(
                        profile="smoke",
                        target=["tracedb"],
                        surfaces=["http"],
                        require_services=False,
                        repo_root=".",
                        run_id="admin-split",
                    ),
                )
        finally:
            if old_url is None:
                os.environ.pop("TRACEDB_HTTP_URL", None)
            else:
                os.environ["TRACEDB_HTTP_URL"] = old_url
            if old_data_dir is None:
                os.environ.pop("TRACEDB_HTTP_DATA_DIR", None)
            else:
                os.environ["TRACEDB_HTTP_DATA_DIR"] = old_data_dir
            fake.stop()

        self.assertTrue(result["available"], result["notes"])
        self.assertEqual(result["metrics"]["admin_compact_count"], 1)
        self.assertEqual(result["metrics"]["admin_snapshot_count"], 1)
        self.assertEqual(result["metrics"]["admin_restore_count"], 1)
        self.assertIn("admin_latency_p95_ms", result["metrics"])
        self.assertIn("admin_compact_latency_p95_ms", result["metrics"])
        self.assertIn("admin_snapshot_latency_p95_ms", result["metrics"])
        self.assertIn("admin_restore_latency_p95_ms", result["metrics"])
        self.assertEqual(result["metrics"]["freshness_query_count"], 2)
        self.assertEqual(result["metrics"]["ingest_transaction_count"], 12)
        self.assertEqual(result["metrics"]["per_record_durable_transaction_count"], 12)
        self.assertEqual(result["metrics"]["batch_transaction_count"], 0)
        self.assertIn("freshness_query_latency_p95_ms", result["metrics"])
        self.assertGreaterEqual(result["metrics"]["disk_bytes"], 128)
        self.assertEqual(
            result["metrics"]["disk_bytes"],
            result["metrics"]["disk_bytes_after_ingest"],
        )
        self.assertGreaterEqual(result["metrics"]["disk_bytes_after_workload"], 128)
        self.assertTrue(
            any("data directory bytes measured" in note for note in result["notes"]),
            result["notes"],
        )

    def test_tracedb_http_surface_reports_query_phase_and_storage_attribution(self) -> None:
        fake = FakeTraceDb().start()
        old_url = os.environ.get("TRACEDB_HTTP_URL")
        old_data_dir = os.environ.get("TRACEDB_HTTP_DATA_DIR")
        try:
            with tempfile.TemporaryDirectory() as data_dir:
                root = Path(data_dir)
                (root / "manifest.tdb").write_bytes(b"m" * 32)
                (root / "wal").mkdir()
                (root / "wal" / "000001.twal").write_bytes(b"w" * 128)
                (root / "segments").mkdir()
                (root / "segments" / "000001.tseg").write_bytes(b"s" * 256)
                os.environ["TRACEDB_HTTP_URL"] = fake.base_url
                os.environ["TRACEDB_HTTP_DATA_DIR"] = data_dir
                result = TraceDbAdapter().run(
                    generated_dataset(12, 42),
                    RunConfig(
                        profile="smoke",
                        target=["tracedb"],
                        surfaces=["http"],
                        require_services=False,
                        repo_root=".",
                        run_id="phase-storage",
                    ),
                )
        finally:
            if old_url is None:
                os.environ.pop("TRACEDB_HTTP_URL", None)
            else:
                os.environ["TRACEDB_HTTP_URL"] = old_url
            if old_data_dir is None:
                os.environ.pop("TRACEDB_HTTP_DATA_DIR", None)
            else:
                os.environ["TRACEDB_HTTP_DATA_DIR"] = old_data_dir
            fake.stop()

        self.assertTrue(result["available"], result["notes"])
        metrics = result["metrics"]
        self.assertEqual(metrics["query_phase_tenant_visibility_latency_p95_ms"], 1.25)
        self.assertEqual(metrics["query_phase_access_path_build_latency_p95_ms"], 2.5)
        self.assertEqual(metrics["query_phase_fusion_latency_p95_ms"], 0.75)
        self.assertEqual(metrics["query_phase_materialization_latency_p95_ms"], 0.5)
        self.assertEqual(metrics["query_access_path_lexicalpath_build_latency_p95_ms"], 1.5)
        self.assertEqual(metrics["query_access_path_vectorpath_open_latency_p95_ms"], 0.5)
        self.assertEqual(metrics["query_server_engine_latency_p95_ms"], 0.04)
        self.assertEqual(metrics["query_server_prewrite_total_latency_p95_ms"], 0.15)
        self.assertEqual(metrics["query_engine_phase_total_latency_p95_ms"], 5.0)
        self.assertEqual(
            metrics["query_http_client_latency_p95_ms"],
            metrics["query_latency_p95_ms"],
        )
        self.assertIn("query_http_client_overhead_latency_p95_ms", metrics)
        self.assertEqual(metrics["disk_bytes_after_ingest_manifest_tdb"], 32)
        self.assertEqual(metrics["disk_bytes_after_ingest_wal"], 128)
        self.assertEqual(metrics["disk_bytes_after_ingest_segments"], 256)
        self.assertTrue(
            any("query phase attribution" in note for note in result["notes"]),
            result["notes"],
        )
        self.assertTrue(
            any("storage attribution" in note for note in result["notes"]),
            result["notes"],
        )
        self.assertTrue(
            any("server/client query attribution" in note for note in result["notes"]),
            result["notes"],
        )

    def test_tracedb_http_surface_can_use_batch_ingest_mode(self) -> None:
        fake = FakeTraceDb().start()
        old_url = os.environ.get("TRACEDB_HTTP_URL")
        try:
            os.environ["TRACEDB_HTTP_URL"] = fake.base_url
            result = TraceDbAdapter().run(
                generated_dataset(12, 42),
                RunConfig(
                    profile="smoke",
                    target=["tracedb"],
                    surfaces=["http"],
                    require_services=False,
                    repo_root=".",
                    run_id="batch-ingest",
                    tracedb_ingest_mode="batch",
                ),
            )
        finally:
            if old_url is None:
                os.environ.pop("TRACEDB_HTTP_URL", None)
            else:
                os.environ["TRACEDB_HTTP_URL"] = old_url
            fake.stop()

        self.assertTrue(result["available"], result["notes"])
        self.assertEqual(fake.batch_sizes, [12])
        self.assertEqual(result["metrics"]["ingest_count"], 12)
        self.assertEqual(result["metrics"]["ingest_transaction_count"], 1)
        self.assertEqual(result["metrics"]["per_record_durable_transaction_count"], 0)
        self.assertEqual(result["metrics"]["batch_transaction_count"], 1)
        self.assertEqual(result["metrics"]["batch_transaction_record_count"], 12)
        self.assertIn("batch_transaction_total_latency_ms", result["metrics"])
        self.assertTrue(
            any("single-transaction batch" in note for note in result["notes"]),
            result["notes"],
        )

    def test_generated_dataset_labels_are_marked_operational_smoke(self) -> None:
        dataset = generated_dataset(24, 42)
        self.assertEqual(dataset.relevance_label_mode, "synthetic_oracle_rank")
        self.assertEqual(
            dataset.relevance_label_scope,
            "operational_smoke_not_hybrid_quality",
        )
        self.assertTrue(
            any("not aligned to hybrid relevance" in note for note in dataset.notes),
            dataset.notes,
        )

        config = RunConfig(
            profile="smoke",
            target=["tracedb"],
            surfaces=["sdk"],
            require_services=False,
            repo_root=".",
        )
        report = build_report(dataset, config, [])
        self.assertEqual(
            report["dataset"]["relevance_label_scope"],
            "operational_smoke_not_hybrid_quality",
        )
        offline = [
            scenario
            for scenario in report["scenarios"]
            if scenario["name"] == "offline_reproducible_control"
        ][0]
        self.assertIn("dataset.relevance_label_scope", offline["metrics"])

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "report.md"
            report["baselines"] = [
                {
                    "name": "tracedb",
                    "available": True,
                    "role": "transactional hybrid database",
                    "metrics": {
                        "ingest_count": 1,
                        "query_count": 1,
                        "latency_p50_ms": 1.0,
                        "latency_p95_ms": 1.0,
                        "latency_p99_ms": 1.0,
                        "recall_at_5": 1.0,
                        "same_file_recall_at_5": 1.0,
                        "span_gap_count": 0,
                        "ndcg_at_5": 1.0,
                        "mrr_at_5": 1.0,
                    },
                    "notes": [],
                }
            ]
            write_markdown(report, path)
            rendered = path.read_text(encoding="utf-8")

        self.assertIn(
            "Relevance labels: `synthetic_oracle_rank` (`operational_smoke_not_hybrid_quality`)",
            rendered,
        )
        self.assertIn("same-file recall@5", rendered)

    def test_trace_db_only_report_is_marked_internal_smoke(self) -> None:
        dataset = generated_dataset(24, 42)
        config = RunConfig(
            profile="smoke",
            target=["tracedb"],
            surfaces=["sdk"],
            require_services=False,
            repo_root=".",
        )
        report = build_report(
            dataset,
            config,
            [
                {
                    "name": "TraceDB",
                    "available": True,
                    "role": "transactional hybrid database",
                    "metrics": {
                        "ingest_count": 1,
                        "query_count": 1,
                        "latency_p50_ms": 1.0,
                        "latency_p95_ms": 2.0,
                        "latency_p99_ms": 3.0,
                        "recall_at_5": 1.0,
                        "ndcg_at_5": 1.0,
                        "mrr_at_5": 1.0,
                    },
                    "notes": [],
                }
            ],
        )

        self.assertEqual(report["control_status"], "internal_only_smoke")
        self.assertEqual(report["summary"]["control_status"], "internal_only_smoke")
        self.assertIsNone(report["number_to_beat"]["query_p95_ms"]["value"])

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "report.md"
            write_markdown(report, path)
            rendered = path.read_text(encoding="utf-8")

        self.assertIn("Control status: `internal_only_smoke`", rendered)
        self.assertIn("development evidence, not product evidence", rendered)

    def test_report_records_external_number_to_beat(self) -> None:
        dataset = generated_dataset(24, 42)
        config = RunConfig(
            profile="smoke",
            target=["all"],
            surfaces=["sdk"],
            require_services=False,
            repo_root=".",
        )
        report = build_report(
            dataset,
            config,
            [
                {
                    "name": "TraceDB",
                    "available": True,
                    "role": "target under test",
                    "metrics": {
                        "ingest_count": 24,
                        "query_count": 4,
                        "latency_p95_ms": 7.0,
                        "recall_at_5": 0.75,
                        "failure_count": 0,
                    },
                    "notes": [],
                },
                {
                    "name": "PostgreSQL",
                    "available": True,
                    "role": "relational control",
                    "metrics": {
                        "ingest_count": 24,
                        "query_count": 4,
                        "latency_p95_ms": 5.0,
                        "ingest_latency_p95_ms": 1.5,
                        "recall_at_5": 0.5,
                        "disk_bytes": 1024,
                        "failure_count": 0,
                    },
                    "notes": [],
                },
                {
                    "name": "Qdrant",
                    "available": False,
                    "role": "vector control",
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
        )

        self.assertEqual(report["control_status"], "external_control_available")
        self.assertEqual(report["number_to_beat"]["query_p95_ms"]["value"], 5.0)
        self.assertEqual(report["number_to_beat"]["query_p95_ms"]["baseline"], "PostgreSQL")
        self.assertEqual(report["number_to_beat"]["recall_at_5"]["value"], 0.5)
        self.assertEqual(report["control_ledger"]["unavailable_external_controls"][0]["name"], "Qdrant")

    def test_generated_hybrid_dataset_uses_retrieval_quality_labels(self) -> None:
        smoke = load_dataset("generated", 256, 42)
        hybrid = load_dataset("generated_hybrid", 256, 42)

        self.assertEqual(hybrid.kind, "generated_hybrid")
        self.assertEqual(hybrid.relevance_label_mode, "synthetic_text_vector_similarity")
        self.assertEqual(hybrid.relevance_label_scope, "synthetic_retrieval_quality")
        self.assertTrue(
            any("text+vector" in note for note in hybrid.relevance_label_notes),
            hybrid.relevance_label_notes,
        )
        self.assertNotEqual(
            smoke.queries[0].expected_ids,
            hybrid.queries[0].expected_ids,
        )


if __name__ == "__main__":
    unittest.main()
