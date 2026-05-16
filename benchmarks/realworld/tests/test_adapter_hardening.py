from __future__ import annotations

import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from runner.adapters.qdrant import QdrantAdapter
from runner.adapters.tracedb import TraceDbAdapter
from runner.datasets import generated_dataset
from runner.http import request_json
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

            def _json(self, status: int, payload: dict) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
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


class AdapterHardeningTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
