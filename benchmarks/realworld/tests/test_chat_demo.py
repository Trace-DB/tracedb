from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from runner.chat_demo import (
    ChatDemoRunner,
    fixture_records,
    render_markdown_report,
)


def command_args(command: list[str]) -> list[str]:
    args = command[1:]
    if len(args) >= 2 and args[0] == "--data":
        args = args[2:]
    return args


class ChatDemoTest(unittest.TestCase):
    def test_fixture_records_are_deterministic_and_tenant_scoped(self) -> None:
        first = fixture_records()
        second = fixture_records()

        self.assertEqual(first, second)
        self.assertEqual(len(first), 7)
        self.assertEqual(
            sorted({record["tenant_id"] for record in first}),
            ["tenant-alpha", "tenant-beta"],
        )
        self.assertIn(
            "tenant-alpha",
            {record["tenant_id"] for record in first if record["fields"]["user_id"] == "user-erased"},
        )

    def test_runner_writes_report_and_uses_real_cli_commands(self) -> None:
        commands: list[list[str]] = []

        def fake_run(command: list[str]) -> tuple[int, str, str]:
            commands.append(command)
            args = command_args(command)
            joined = " ".join(args)
            if " query " in f" {joined} ":
                payload_arg = command[-1]
                query = json.loads(Path(payload_arg).read_text(encoding="utf-8"))
                if "erasure" in (query.get("text") or ""):
                    rows = (
                        [{"record_id": "alpha-erased-1"}, {"record_id": "alpha-erased-2"}]
                        if len([c for c in commands if "delete" in c]) == 0
                        else []
                    )
                else:
                    rows = [{"record_id": "alpha-memory-1"}]
                return (
                    0,
                    json.dumps(
                        {
                            "results": rows,
                            "explain": {
                                "dirty_feature_count": 1,
                                "pending_feature_count": 1,
                                "failed_feature_count": 1,
                                "freshness_mode": query["freshness"].upper(),
                            },
                        }
                    ),
                    "",
                )
            if " get " in f" {joined} ":
                return 0, json.dumps({"record": {"id": command[-1]}}), ""
            if args[:3] == ["feature", "status", "set"]:
                return 0, json.dumps({"status": args[-1]}), ""
            return 0, json.dumps({"ok": True}), ""

        with tempfile.TemporaryDirectory(prefix="chat-demo-test-") as temp_dir:
            out_json = Path(temp_dir) / "report.json"
            out_md = Path(temp_dir) / "report.md"
            runner = ChatDemoRunner(
                repo_root=Path(temp_dir),
                tracedb_cli=Path("/bin/tracedb"),
                data_dir=Path(temp_dir) / "db",
                output_json=out_json,
                output_md=out_md,
                run_command=fake_run,
            )

            report = runner.run()

            self.assertTrue(out_json.exists())
            self.assertTrue(out_md.exists())
            self.assertEqual(report["summary"]["records_inserted"], 7)
            self.assertEqual(report["summary"]["deleted_subject_records"], ["alpha-erased-1", "alpha-erased-2"])
            self.assertFalse(report["summary"]["erased_subject_visible_after_delete"])
            normalized = [command_args(command) for command in commands]
            self.assertTrue(any(command[:3] == ["feature", "status", "set"] for command in normalized))
            self.assertTrue(any(command[0] == "patch" for command in normalized))
            self.assertTrue(any(command[0] == "delete" for command in normalized))

    def test_runner_raises_when_invariants_fail(self) -> None:
        def fake_run(command: list[str]) -> tuple[int, str, str]:
            args = command_args(command)
            joined = " ".join(args)
            if " query " in f" {joined} ":
                payload_arg = command[-1]
                query = json.loads(Path(payload_arg).read_text(encoding="utf-8"))
                rows = [{"record_id": "beta-memory-1"}]
                if "erasure" in (query.get("text") or ""):
                    rows = [{"record_id": "alpha-erased-1"}]
                return (
                    0,
                    json.dumps(
                        {
                            "results": rows,
                            "explain": {
                                "dirty_feature_count": 0,
                                "pending_feature_count": 0,
                                "failed_feature_count": 0,
                            },
                        }
                    ),
                    "",
                )
            if " get " in f" {joined} ":
                return 0, json.dumps({"record": None}), ""
            if args[:3] == ["feature", "status", "set"]:
                return 0, json.dumps({"status": "Ready"}), ""
            return 0, json.dumps({"ok": True}), ""

        with tempfile.TemporaryDirectory(prefix="chat-demo-test-") as temp_dir:
            out_json = Path(temp_dir) / "report.json"
            out_md = Path(temp_dir) / "report.md"
            runner = ChatDemoRunner(
                repo_root=Path(temp_dir),
                tracedb_cli=Path("/bin/tracedb"),
                data_dir=Path(temp_dir) / "db",
                output_json=out_json,
                output_md=out_md,
                run_command=fake_run,
            )

            with self.assertRaisesRegex(RuntimeError, "chat demo invariant check failed") as raised:
                runner.run()

            self.assertIn("baseline tenant-alpha query returned beta-memory-1", str(raised.exception))
            payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertIn("invariant_failures", payload)
            self.assertTrue(payload["invariant_failures"])

    def test_markdown_report_includes_required_caveats(self) -> None:
        markdown = render_markdown_report(
            {
                "commands": [],
                "summary": {},
                "caveats": [
                    "local logical demo only",
                    "no cloud dependency",
                    "no legal export/purge claim",
                    "no LangSmith canonical storage",
                ],
            }
        )

        self.assertIn("local logical demo only", markdown)
        self.assertIn("no cloud dependency", markdown)
        self.assertIn("no legal export/purge claim", markdown)
        self.assertIn("no LangSmith canonical storage", markdown)


if __name__ == "__main__":
    unittest.main()
