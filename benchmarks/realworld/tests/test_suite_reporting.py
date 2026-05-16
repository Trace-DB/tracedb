from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


LAB_ROOT = Path(__file__).resolve().parents[1]


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
        self.assertIn("## Executive Summary", markdown)
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


if __name__ == "__main__":
    unittest.main()
