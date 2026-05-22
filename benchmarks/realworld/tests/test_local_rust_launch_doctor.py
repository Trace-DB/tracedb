from __future__ import annotations

import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "local_rust_launch_doctor.py"


def load_module():
    spec = importlib.util.spec_from_file_location("local_rust_launch_doctor", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.pop("local_rust_launch_doctor", None)
    spec.loader.exec_module(module)
    return module


class FakeRunner:
    def __init__(self, responses: dict[tuple[str, ...], Any]):
        self.responses = responses

    def __call__(self, argv, **kwargs):
        key = tuple(argv)
        response = self.responses[key]
        if isinstance(response, BaseException):
            raise response
        return response


class LocalRustLaunchDoctorTests(unittest.TestCase):
    def test_missing_binary_is_reported_without_launch(self) -> None:
        module = load_module()

        summary = module.diagnose_launch(
            binary=Path("missing-tracedb"),
            cwd=ROOT,
            runner=FakeRunner({}),
        )

        self.assertEqual(summary["status"], "missing")
        self.assertEqual(summary["classification"], "binary_missing")
        self.assertIn("cargo build -p tracedb-cli", summary["advice"][0])

    def test_pre_main_timeout_is_classified_as_blocked(self) -> None:
        module = load_module()
        binary = ROOT / "README.md"
        timeout = subprocess.TimeoutExpired([str(binary), "--help"], 3.0)
        runner = FakeRunner(
            {
                ("xattr", "-l", str(binary)): subprocess.CompletedProcess(
                    ["xattr"], 0, stdout="com.apple.provenance\n", stderr=""
                ),
                ("spctl", "-a", "-vv", str(binary)): subprocess.CompletedProcess(
                    ["spctl"], 1, stdout="", stderr=f"{binary}: rejected\n"
                ),
                ("codesign", "-dv", str(binary)): subprocess.CompletedProcess(
                    ["codesign"], 0, stdout="", stderr="Signature=adhoc\n"
                ),
                (str(binary), "--help"): timeout,
            }
        )

        summary = module.diagnose_launch(binary=binary, cwd=ROOT, runner=runner)

        self.assertEqual(summary["status"], "blocked")
        self.assertEqual(summary["classification"], "pre_main_launch_timeout")
        self.assertTrue(summary["launch"]["timed_out"])
        self.assertIn("Modal product verification", " ".join(summary["advice"]))

    def test_successful_launch_passes(self) -> None:
        module = load_module()
        binary = ROOT / "README.md"
        runner = FakeRunner(
            {
                ("xattr", "-l", str(binary)): subprocess.CompletedProcess(
                    ["xattr"], 0, stdout="", stderr=""
                ),
                ("spctl", "-a", "-vv", str(binary)): subprocess.CompletedProcess(
                    ["spctl"], 0, stdout="", stderr=f"{binary}: accepted\n"
                ),
                ("codesign", "-dv", str(binary)): subprocess.CompletedProcess(
                    ["codesign"], 0, stdout="", stderr="Signature=adhoc\n"
                ),
                (str(binary), "--help"): subprocess.CompletedProcess(
                    [str(binary), "--help"], 0, stdout="usage\n", stderr=""
                ),
            }
        )

        summary = module.diagnose_launch(binary=binary, cwd=ROOT, runner=runner)

        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["classification"], "launch_passed")


if __name__ == "__main__":
    unittest.main()
