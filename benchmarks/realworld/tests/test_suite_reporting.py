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
