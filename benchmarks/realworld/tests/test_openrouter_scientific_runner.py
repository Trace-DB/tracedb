from __future__ import annotations

import json
import os
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


LAB_ROOT = Path(__file__).resolve().parents[1]


class FakeOpenRouter:
    def __init__(self) -> None:
        self.embedding_requests = 0
        self.chat_requests = 0
        self.rerank_requests = 0
        self.auth_headers: list[str] = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}/api/v1"

    def start(self) -> "FakeOpenRouter":
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

            def do_GET(self) -> None:
                owner.auth_headers.append(self.headers.get("authorization", ""))
                if self.path == "/api/v1/key":
                    self._json(
                        200,
                        {
                            "data": {
                                "label": "fake-key",
                                "limit": 10,
                                "limit_remaining": 9,
                                "usage": 0.01,
                                "usage_daily": 0.01,
                            }
                        },
                    )
                    return
                if self.path == "/api/v1/embeddings/models":
                    self._json(
                        200,
                        {
                            "data": [
                                {
                                    "id": "fake/embed",
                                    "canonical_slug": "fake/embed",
                                    "name": "Fake Embed",
                                    "pricing": {"prompt": "0.000001"},
                                    "top_provider": {"context_length": 8192},
                                }
                            ]
                        },
                    )
                    return
                self._json(404, {"error": "not found"})

            def do_POST(self) -> None:
                owner.auth_headers.append(self.headers.get("authorization", ""))
                length = int(self.headers.get("content-length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                if self.path == "/api/v1/embeddings":
                    owner.embedding_requests += 1
                    inputs = payload.get("input", [])
                    if isinstance(inputs, str):
                        inputs = [inputs]
                    data = []
                    for index, text in enumerate(inputs):
                        basis = float((sum(ord(ch) for ch in str(text)) % 17) + 1)
                        data.append(
                            {
                                "object": "embedding",
                                "index": index,
                                "embedding": [
                                    round(basis / 100.0, 6),
                                    round((basis + 1) / 100.0, 6),
                                    round((basis + 2) / 100.0, 6),
                                    round((basis + 3) / 100.0, 6),
                                    round((basis + 4) / 100.0, 6),
                                ],
                            }
                        )
                    self._json(
                        200,
                        {
                            "object": "list",
                            "model": payload.get("model", "fake/embed"),
                            "data": data,
                            "usage": {
                                "prompt_tokens": sum(len(str(item).split()) for item in inputs),
                                "total_tokens": sum(len(str(item).split()) for item in inputs),
                            },
                        },
                    )
                    return
                if self.path == "/api/v1/chat/completions":
                    owner.chat_requests += 1
                    self._json(
                        200,
                        {
                            "id": "chatcmpl-fake",
                            "object": "chat.completion",
                            "model": payload.get("model", "openrouter/owl-alpha"),
                            "choices": [
                                {
                                    "index": 0,
                                    "finish_reason": "stop",
                                    "message": {
                                        "role": "assistant",
                                        "content": "{\"relevance\": 1, \"reason\": \"fake\"}",
                                    },
                                }
                            ],
                            "usage": {
                                "prompt_tokens": 12,
                                "completion_tokens": 8,
                                "total_tokens": 20,
                            },
                        },
                    )
                    return
                if self.path == "/api/v1/rerank":
                    owner.rerank_requests += 1
                    documents = payload.get("documents", [])
                    results = [
                        {
                            "document": {"text": document},
                            "index": index,
                            "relevance_score": round(1.0 - (index * 0.01), 6),
                        }
                        for index, document in enumerate(documents[: payload.get("top_n", len(documents))])
                    ]
                    self._json(
                        200,
                        {
                            "id": "gen-rerank-fake",
                            "model": payload.get("model", "cohere/rerank-4-fast"),
                            "provider": "fake",
                            "results": results,
                            "usage": {"search_units": 1, "total_tokens": 25},
                        },
                    )
                    return
                self._json(404, {"error": "not found"})

        return Handler


def run_runner(args: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged = os.environ.copy()
    merged.pop("OPENROUTER_API_KEY", None)
    merged.pop("OPENROUTER_BASE_URL", None)
    merged.pop("OPENROUTER_EMBED_MODEL", None)
    merged.pop("OPENROUTER_COMPARE_EMBED_MODELS", None)
    merged.pop("OPENROUTER_JUDGE_MODEL", None)
    merged.pop("OPENROUTER_RERANK_MODEL", None)
    if env:
        merged.update(env)
    return subprocess.run(
        ["python3", "-m", "runner", *args],
        cwd=LAB_ROOT,
        env=merged,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )


class OpenRouterScientificRunnerTests(unittest.TestCase):
    def test_required_mode_without_key_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            completed = run_runner(
                [
                    "run",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "12",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "required",
                    "--output-json",
                    str(Path(temp_dir) / "summary.json"),
                    "--output-md",
                    str(Path(temp_dir) / "report.md"),
                ],
                env={"BENCH_DISABLE_ENV_FILE": "1"},
            )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("OPENROUTER_API_KEY", completed.stderr + completed.stdout)

    def test_doctor_openrouter_off_mode_does_not_require_key(self) -> None:
        completed = run_runner(["doctor", "openrouter", "--openrouter-mode", "off"])
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["openrouter_mode"], "off")
        self.assertEqual(payload["status"], "disabled")

    def test_required_mode_fake_openrouter_writes_artifacts_and_redacts_key(self) -> None:
        fake = FakeOpenRouter().start()
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                reports = Path(temp_dir) / "reports"
                completed = run_runner(
                    [
                        "run",
                        "--profile",
                        "smoke",
                        "--dataset",
                        "generated",
                        "--records",
                        "18",
                        "--target",
                        "tracedb",
                        "--surface",
                        "sdk",
                        "--openrouter-mode",
                        "required",
                        "--openrouter-cap",
                        "conservative",
                        "--embed-model",
                        "fake/embed",
                        "--compare-embed-models",
                        "",
                        "--judge-model",
                        "openrouter/owl-alpha",
                        "--rerank-model",
                        "cohere/rerank-4-fast",
                        "--run-id",
                        "fake-required",
                        "--reports-dir",
                        str(reports),
                    ],
                    env={
                        "OPENROUTER_API_KEY": "sk-test-secret",
                        "OPENROUTER_BASE_URL": fake.base_url,
                    },
                )
                self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
                run_dir = reports / "fake-required"
                manifest = json.loads((run_dir / "manifest.json").read_text())
                summary = json.loads((run_dir / "summary.json").read_text())
                observations = (run_dir / "observations.jsonl").read_text()
                report = (run_dir / "report.md").read_text()
        finally:
            fake.stop()

        self.assertGreater(fake.embedding_requests, 0)
        self.assertGreater(fake.rerank_requests, 0)
        self.assertIn("Bearer sk-test-secret", fake.auth_headers)
        self.assertEqual(manifest["openrouter"]["mode"], "required")
        self.assertEqual(manifest["dataset"]["embedding_dimensions"], 5)
        self.assertEqual(summary["dataset"]["embedding_dimensions"], 5)
        self.assertGreaterEqual(summary["openrouter"]["request_count"], 1)
        self.assertGreaterEqual(summary["openrouter"]["rerank_request_count"], 1)
        self.assertIn("rag_retrieve_then_rerank", json.dumps(summary["scenarios"]))
        self.assertIn("Simulated Scenarios", report)
        self.assertIn("cohere/rerank-4-fast", report)
        self.assertIn("openrouter.embedding_batch", observations)
        self.assertIn("openrouter.rerank", observations)
        self.assertNotIn("sk-test-secret", json.dumps(manifest))
        self.assertNotIn("sk-test-secret", json.dumps(summary))
        self.assertNotIn("sk-test-secret", observations)
        self.assertNotIn("sk-test-secret", report)

    def test_provider_embeddings_are_capped_to_requested_dimensions(self) -> None:
        fake = FakeOpenRouter().start()
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                reports = Path(temp_dir) / "reports"
                completed = run_runner(
                    [
                        "run",
                        "--profile",
                        "smoke",
                        "--dataset",
                        "generated",
                        "--records",
                        "18",
                        "--target",
                        "tracedb",
                        "--surface",
                        "sdk",
                        "--openrouter-mode",
                        "required",
                        "--openrouter-cap",
                        "conservative",
                        "--embed-model",
                        "fake/embed",
                        "--compare-embed-models",
                        "",
                        "--embedding-dimensions",
                        "3",
                        "--run-id",
                        "fake-dims",
                        "--reports-dir",
                        str(reports),
                    ],
                    env={
                        "OPENROUTER_API_KEY": "sk-test-secret",
                        "OPENROUTER_BASE_URL": fake.base_url,
                    },
                )
                self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
                summary = json.loads((reports / "fake-dims" / "summary.json").read_text())
                manifest = json.loads((reports / "fake-dims" / "manifest.json").read_text())
                report = (reports / "fake-dims" / "report.md").read_text()
        finally:
            fake.stop()

        self.assertEqual(summary["dataset"]["embedding_dimensions"], 3)
        self.assertEqual(summary["openrouter"]["provider_native_embedding_dimensions"], 5)
        self.assertEqual(summary["openrouter"]["requested_embedding_dimensions"], 3)
        self.assertEqual(manifest["dataset"]["embedding_dimensions"], 3)
        self.assertIn("Provider-native dimensions: `5`", report)
        self.assertIn("Requested embedding dimensions: `3`", report)

    def test_loop_stops_on_injected_failure_and_writes_minimized_case(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            reports = Path(temp_dir) / "reports"
            completed = run_runner(
                [
                    "loop",
                    "--profile",
                    "smoke",
                    "--dataset",
                    "generated",
                    "--records",
                    "16",
                    "--iterations",
                    "3",
                    "--target",
                    "tracedb",
                    "--surface",
                    "sdk",
                    "--openrouter-mode",
                    "off",
                    "--run-id",
                    "loop-injected",
                    "--reports-dir",
                    str(reports),
                    "--stop-on-failure",
                ],
                env={"BENCH_INJECT_FAILURE_ITERATION": "2"},
            )
            self.assertEqual(completed.returncode, 1, completed.stderr + completed.stdout)
            failure = reports / "loop-injected" / "failure-iteration-2.json"
            self.assertTrue(failure.exists())
            payload = json.loads(failure.read_text())

        self.assertEqual(payload["iteration"], 2)
        self.assertEqual(payload["seed"], 43)
        self.assertIn("injected failure", payload["reason"])


if __name__ == "__main__":
    unittest.main()
