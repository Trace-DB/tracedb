from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "modal_product_verify.py"


def load_module():
    spec = importlib.util.spec_from_file_location("modal_product_verify", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.pop("modal_product_verify", None)
    spec.loader.exec_module(module)
    return module


class ModalProductVerifyTests(unittest.TestCase):
    def test_quickstart_mode_upload_guardrails_and_command_ladder(self) -> None:
        module = load_module()

        self.assertIn("target/**", module.MODAL_IGNORE_PATTERNS)
        self.assertIn(".git/**", module.MODAL_IGNORE_PATTERNS)
        self.assertIn(".env", module.MODAL_IGNORE_PATTERNS)
        self.assertIn(".modal.toml", module.MODAL_IGNORE_PATTERNS)

        commands = module.build_command_plan("quickstart")

        self.assertEqual(
            [command["name"] for command in commands],
            [
                "cargo-fmt",
                "quickstart-receipt-test",
                "quickstart-doc-contract-test",
                "platform-contract-doc-test",
                "product-quickstart-skip-typescript",
            ],
        )
        self.assertEqual(
            commands[-1]["argv"],
            [
                "cargo",
                "run",
                "-q",
                "-p",
                "tracedb-cli",
                "--",
                "product-quickstart",
                "--skip-typescript",
            ],
        )

    def test_workspace_mode_extends_quickstart_ladder(self) -> None:
        module = load_module()

        command_names = [command["name"] for command in module.build_command_plan("workspace")]

        self.assertIn("product-quickstart-skip-typescript", command_names)
        self.assertIn("typescript-npm-ci", command_names)
        self.assertIn("tracedb-cli-demo-tests", command_names)
        self.assertIn("tracedb-testkit-usability-tests", command_names)
        self.assertIn("workspace-all-targets", command_names)

    def test_reduced_quickstart_receipt_contract(self) -> None:
        module = load_module()
        receipt = {
            "ok": True,
            "mode": "local-product-regression",
            "scope": "local_only",
            "report_file": "/workspace/TraceDB/target/tracedb/product-quickstart.json",
            "typescript_enabled": False,
            "claims": {
                "sql_module": "not_implemented",
                "managed_cloud": "not_checked",
                "benchmark": "not_checked",
            },
            "human_summary": {
                "status": "passed",
                "steps_passed": 5,
                "steps_total": 5,
                "failed_step": None,
            },
            "steps": {
                "embedded_demo": {"ok": True},
                "embedded_verify": {"ok": True},
                "http_demo": {"ok": True},
                "local_doctor": {"ok": True},
                "rust_sdk_quickstart": {"ok": True},
            },
        }

        summary = module.validate_reduced_quickstart_receipt(
            json.dumps(receipt),
            receipt,
        )

        self.assertEqual(summary["steps_passed"], 5)
        self.assertEqual(summary["typescript_enabled"], False)
        self.assertEqual(summary["skipped_typescript_steps"], 3)

        bad_receipt = dict(receipt)
        bad_receipt["steps"] = dict(receipt["steps"])
        bad_receipt["steps"]["typescript_check"] = {"ok": True}

        with self.assertRaisesRegex(AssertionError, "typescript_check"):
            module.validate_reduced_quickstart_receipt(
                json.dumps(bad_receipt),
                bad_receipt,
            )


if __name__ == "__main__":
    unittest.main()
