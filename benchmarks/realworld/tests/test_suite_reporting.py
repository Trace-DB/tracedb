from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from runner.suite import SCENARIOS, build_suite_report, write_suite_markdown


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
            self.assertTrue(suite_json.exists())
            self.assertTrue(suite_md.exists())
            payload = json.loads(suite_json.read_text())
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
                        "query_http_client_overhead_latency_p95_ms": 1.0,
                        "query_server_engine_latency_p95_ms": 2.0,
                        "query_server_prewrite_total_latency_p95_ms": 2.5,
                        "query_engine_phase_total_latency_p95_ms": 2.25,
                        "query_phase_access_path_build_latency_p95_ms": 2.25,
                        "query_access_path_lexicalpath_build_latency_p95_ms": 1.25,
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
        self.assertEqual(
            attribution["access_paths"]["lexicalpath_build_latency_p95_ms"],
            1.25,
        )
        self.assertEqual(attribution["http_client"]["latency_p95_ms"], 3.5)
        self.assertEqual(attribution["http_client"]["overhead_latency_p95_ms"], 1.0)
        self.assertEqual(attribution["server"]["engine_latency_p95_ms"], 2.0)
        self.assertEqual(attribution["engine"]["phase_total_latency_p95_ms"], 2.25)
        self.assertEqual(attribution["storage_after_ingest"]["wal"], 1024)
        self.assertEqual(attribution["storage_after_ingest"]["manifest_tdb"], 256)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "suite.md"
            write_suite_markdown(suite, path)
            markdown = path.read_text()

        self.assertIn("## TraceDB Attribution", markdown)
        self.assertIn("engine_latency_p95_ms=2.0", markdown)
        self.assertIn("overhead_latency_p95_ms=1.0", markdown)
        self.assertIn("access_path_build_latency_p95_ms", markdown)
        self.assertIn("wal=1024", markdown)


if __name__ == "__main__":
    unittest.main()
