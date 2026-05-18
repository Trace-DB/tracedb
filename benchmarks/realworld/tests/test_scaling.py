from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from runner.scaling import (
    TraceDbScalingRunner,
    compare_inprocess_scaling_reports,
    parse_record_targets,
    render_inprocess_scaling_comparison_markdown,
    render_scaling_markdown,
)


def command_args(command: list[str]) -> list[str]:
    args = command[1:]
    if len(args) >= 2 and args[0] == "--data":
        args = args[2:]
    return args


class TraceDbScalingTests(unittest.TestCase):
    def test_parse_record_targets_sorts_and_deduplicates(self) -> None:
        self.assertEqual(parse_record_targets("16,4,16, 8"), [4, 8, 16])

    def test_runner_writes_scaling_curve_and_uses_real_cli_commands(self) -> None:
        commands: list[list[str]] = []
        epoch = 0

        def fake_run(command: list[str]) -> tuple[int, str, str]:
            nonlocal epoch
            commands.append(command)
            args = command_args(command)
            if args[:2] == ["inspect", "manifest"]:
                return 0, json.dumps({"latest_epoch": epoch}), ""
            if args and args[0] in {"schema", "put", "init"}:
                if args[0] in {"schema", "put"}:
                    epoch += 1
                return 0, json.dumps({"epoch": epoch}), ""
            if args and args[0] == "query":
                return 0, json.dumps({"results": [{"record_id": "rec-000000"}]}), ""
            return 1, "", f"unexpected command {args!r}"

        with tempfile.TemporaryDirectory(prefix="tracedb-scaling-test-") as temp_dir:
            output_json = Path(temp_dir) / "scaling.json"
            output_md = Path(temp_dir) / "scaling.md"
            runner = TraceDbScalingRunner(
                repo_root=Path(temp_dir),
                tracedb_cli=Path("/bin/tracedb"),
                data_dir=Path(temp_dir) / "db",
                output_json=output_json,
                output_md=output_md,
                record_targets=[2, 4],
                inspect_repetitions=2,
                query_repetitions=1,
                run_command=fake_run,
            )

            report = runner.run()
            self.assertTrue(output_json.exists())
            self.assertTrue(output_md.exists())
            markdown = output_md.read_text(encoding="utf-8")

        self.assertEqual([point["records"] for point in report["points"]], [2, 4])
        normalized = [command_args(command) for command in commands]
        self.assertTrue(any(command[:2] == ["schema", "apply"] for command in normalized))
        self.assertEqual(sum(1 for command in normalized if command[:2] == ["inspect", "manifest"]), 4)
        self.assertEqual(sum(1 for command in normalized if command and command[0] == "query"), 2)
        self.assertIn("TraceDB CLI Open/Recovery Scaling", markdown)
        self.assertIn("reopen p95 ms", markdown)

    def test_runner_can_measure_checkpointed_open_path_at_each_point(self) -> None:
        commands: list[list[str]] = []
        epoch = 0

        def fake_run(command: list[str]) -> tuple[int, str, str]:
            nonlocal epoch
            commands.append(command)
            args = command_args(command)
            if args[:2] == ["inspect", "manifest"]:
                return 0, json.dumps({"latest_epoch": epoch, "checkpoint_epoch": epoch}), ""
            if args and args[0] in {"schema", "put", "init"}:
                if args[0] in {"schema", "put"}:
                    epoch += 1
                return 0, json.dumps({"epoch": epoch}), ""
            if args and args[0] == "checkpoint":
                return 0, json.dumps({"checkpoint_epoch": epoch}), ""
            if args and args[0] == "query":
                return 0, json.dumps({"results": [{"record_id": "rec-000000"}]}), ""
            return 1, "", f"unexpected command {args!r}"

        with tempfile.TemporaryDirectory(prefix="tracedb-scaling-test-") as temp_dir:
            output_json = Path(temp_dir) / "scaling.json"
            output_md = Path(temp_dir) / "scaling.md"
            runner = TraceDbScalingRunner(
                repo_root=Path(temp_dir),
                tracedb_cli=Path("/bin/tracedb"),
                data_dir=Path(temp_dir) / "db",
                output_json=output_json,
                output_md=output_md,
                record_targets=[4],
                inspect_repetitions=2,
                query_repetitions=1,
                checkpoint_at_points=True,
                run_command=fake_run,
            )

            report = runner.run()
            markdown = output_md.read_text(encoding="utf-8")

        point = report["points"][0]
        normalized = [command_args(command) for command in commands]
        self.assertEqual(sum(1 for command in normalized if command and command[0] == "checkpoint"), 1)
        self.assertIn("checkpoint_latency_ms", point)
        self.assertIn("checkpoint_reopen_latency_p95_ms", point)
        self.assertIn("checkpoint_query_latency_p95_ms", point)
        self.assertIn("checkpoint WAL bytes", markdown)

    def test_markdown_renders_scaling_table(self) -> None:
        markdown = render_scaling_markdown(
            {
                "summary": {"max_records": 4},
                "data_dir": "/tmp/tracedb-scaling",
                "points": [
                    {
                        "records": 4,
                        "latest_epoch": 5,
                        "wal_bytes": 1234,
                        "put_latency_p95_ms": 1.2,
                        "recent_put_latency_p95_ms": 1.3,
                        "reopen_latency_p95_ms": 2.4,
                        "query_latency_p95_ms": 3.5,
                        "query_returned_count": 5,
                    }
                ],
            }
        )

        self.assertIn("| 4 | 5 | 1234 | 1.2 | 1.3 | 2.4 | 3.5 | 5 |", markdown)

    def test_inprocess_scaling_comparison_rejects_read_regression_despite_write_gain(self) -> None:
        baseline = {
            "benchmark": "tracedb-inprocess-scaling",
            "points": [
                {
                    "records": 4096,
                    "recent_insert_p95_ms": 40.0,
                    "engine_query_p95_ms": 40.0,
                    "checkpoint_engine_query_p95_ms": 30.0,
                }
            ],
        }
        candidate = {
            "benchmark": "tracedb-inprocess-scaling",
            "points": [
                {
                    "records": 4096,
                    "recent_insert_p95_ms": 28.0,
                    "engine_query_p95_ms": 50.0,
                    "checkpoint_engine_query_p95_ms": 31.0,
                }
            ],
        }

        comparison = compare_inprocess_scaling_reports(
            [baseline],
            [candidate],
            baseline_label="baseline-d995623",
            candidate_label="candidate-experiment",
            min_repeats=1,
            required_write_improvement_pct=25.0,
            allowed_query_regression_pct=10.0,
            allowed_query_regression_ms=5.0,
        )

        point = comparison["points"][0]
        self.assertEqual(comparison["status"], "rejected")
        self.assertEqual(point["status"], "rejected")
        self.assertEqual(point["write_gate"], "passed")
        self.assertEqual(point["hot_query_gate"], "failed")
        self.assertAlmostEqual(point["write_improvement_pct"], 30.0)
        markdown = render_inprocess_scaling_comparison_markdown(comparison)
        self.assertIn("candidate-experiment", markdown)
        self.assertIn("hot query gate", markdown)

    def test_inprocess_scaling_comparison_accepts_write_gain_with_stable_reads(self) -> None:
        baseline = {
            "benchmark": "tracedb-inprocess-scaling",
            "points": [
                {
                    "records": 8192,
                    "recent_insert_p95_ms": 40.0,
                    "engine_query_p95_ms": 60.0,
                    "checkpoint_engine_query_p95_ms": 55.0,
                }
            ],
        }
        candidate = {
            "benchmark": "tracedb-inprocess-scaling",
            "points": [
                {
                    "records": 8192,
                    "recent_insert_p95_ms": 29.0,
                    "engine_query_p95_ms": 64.0,
                    "checkpoint_engine_query_p95_ms": 59.0,
                }
            ],
        }

        comparison = compare_inprocess_scaling_reports(
            [baseline],
            [candidate],
            baseline_label="baseline",
            candidate_label="candidate",
            min_repeats=1,
        )

        point = comparison["points"][0]
        self.assertEqual(comparison["status"], "accepted")
        self.assertEqual(point["write_gate"], "passed")
        self.assertEqual(point["hot_query_gate"], "passed")
        self.assertEqual(point["checkpoint_query_gate"], "passed")

    def test_inprocess_scaling_comparison_rejects_under_replicated_runs(self) -> None:
        report = {
            "benchmark": "tracedb-inprocess-scaling",
            "points": [
                {
                    "records": 4096,
                    "recent_insert_p95_ms": 40.0,
                    "engine_query_p95_ms": 40.0,
                }
            ],
        }

        comparison = compare_inprocess_scaling_reports(
            [report],
            [report],
            baseline_label="baseline",
            candidate_label="candidate",
            min_repeats=2,
        )

        self.assertEqual(comparison["status"], "invalid")
        self.assertIn("min_repeats", comparison["failures"][0])

    def test_inprocess_scaling_comparison_rejects_mismatched_record_targets(self) -> None:
        baseline = {
            "benchmark": "tracedb-inprocess-scaling",
            "points": [
                {
                    "records": 4096,
                    "recent_insert_p95_ms": 40.0,
                    "engine_query_p95_ms": 40.0,
                },
                {
                    "records": 8192,
                    "recent_insert_p95_ms": 45.0,
                    "engine_query_p95_ms": 60.0,
                },
            ],
        }
        candidate = {
            "benchmark": "tracedb-inprocess-scaling",
            "points": [
                {
                    "records": 8192,
                    "recent_insert_p95_ms": 30.0,
                    "engine_query_p95_ms": 62.0,
                }
            ],
        }

        comparison = compare_inprocess_scaling_reports(
            [baseline, baseline],
            [candidate, candidate],
            baseline_label="baseline",
            candidate_label="candidate",
            min_repeats=2,
        )

        self.assertEqual(comparison["status"], "invalid")
        self.assertEqual(comparison["missing_candidate_records"], [4096])


if __name__ == "__main__":
    unittest.main()
