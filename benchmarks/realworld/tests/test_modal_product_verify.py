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
                "platform-conformance-quick",
                "agent-memory-flight-recorder-build",
                "agent-memory-flight-recorder",
                "product-quickstart-skip-typescript",
            ],
        )
        self.assertEqual(
            commands[-2]["argv"],
            [
                "python3",
                "-m",
                "runner",
                "chat-demo",
                "--output-json",
                "/tmp/tracedb-agent-memory-flight-recorder.json",
                "--output-md",
                "/tmp/tracedb-agent-memory-flight-recorder.md",
            ],
        )
        self.assertEqual(commands[-2]["cwd"], "benchmarks/realworld")
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
        self.assertIn("agent-memory-flight-recorder-build", command_names)
        self.assertIn("agent-memory-flight-recorder", command_names)
        self.assertIn("typescript-npm-ci", command_names)
        self.assertIn("tracedb-cli-demo-tests", command_names)
        self.assertIn("tracedb-testkit-usability-tests", command_names)
        self.assertIn("query-field-rust-tests", command_names)
        self.assertIn("typescript-npm-public-gateway-smoke", command_names)
        self.assertIn("python-sdk-unit-tests", command_names)
        self.assertIn("python-sdk-install-smoke", command_names)
        self.assertIn("python-platform-conformance-tests", command_names)
        self.assertIn("python-sdk-conformance", command_names)
        self.assertIn("traceql-sqlish-conformance", command_names)
        self.assertIn("graphql-http-conformance", command_names)
        self.assertIn("workspace-all-targets", command_names)

        install_smoke = next(
            command
            for command in module.build_command_plan("workspace")
            if command["name"] == "python-sdk-install-smoke"
        )
        self.assertEqual(install_smoke["argv"], ["python3", "clients/python/install_smoke.py"])
        sqlish_conformance = next(
            command
            for command in module.build_command_plan("workspace")
            if command["name"] == "traceql-sqlish-conformance"
        )
        self.assertEqual(
            sqlish_conformance["argv"],
            [
                "python3",
                "scripts/platform_conformance.py",
                "--surface",
                "traceql_sqlish",
                "--summary-json",
                "/tmp/tracedb-traceql-sqlish-conformance.json",
            ],
        )
        graphql_conformance = next(
            command
            for command in module.build_command_plan("workspace")
            if command["name"] == "graphql-http-conformance"
        )
        self.assertEqual(
            graphql_conformance["argv"],
            [
                "python3",
                "scripts/platform_conformance.py",
                "--surface",
                "graphql",
                "--summary-json",
                "/tmp/tracedb-graphql-conformance.json",
            ],
        )
        query_field_tests = next(
            command
            for command in module.build_command_plan("workspace")
            if command["name"] == "query-field-rust-tests"
        )
        self.assertEqual(
            query_field_tests["argv"],
            [
                "cargo",
                "test",
                "-p",
                "tracedb-query",
                "hybrid_query_",
                "--",
                "--nocapture",
            ],
        )

    def test_only_typescript_gateway_smoke_runs_install_and_gateway_smoke(self) -> None:
        module = load_module()

        commands = module.build_command_plan("workspace", only="typescript_gateway_smoke")

        self.assertEqual(
            [command["name"] for command in commands],
            [
                "typescript-npm-ci",
                "typescript-npm-public-gateway-smoke",
            ],
        )
        self.assertEqual(commands[0]["argv"], ["npm", "ci"])
        self.assertEqual(commands[0]["cwd"], "clients/typescript")
        self.assertEqual(commands[1]["argv"], ["npm", "run", "gateway-smoke"])
        self.assertEqual(commands[1]["cwd"], "clients/typescript")

    def test_only_agent_memory_flight_recorder_runs_build_and_demo(self) -> None:
        module = load_module()

        commands = module.build_command_plan("quickstart", only="agent_memory_flight_recorder")

        self.assertEqual(
            [command["name"] for command in commands],
            [
                "agent-memory-flight-recorder-build",
                "agent-memory-flight-recorder",
            ],
        )
        self.assertEqual(commands[0]["argv"], ["cargo", "build", "-p", "tracedb-cli"])
        self.assertEqual(commands[1]["cwd"], "benchmarks/realworld")
        self.assertEqual(
            commands[1]["receipt_json"],
            "/tmp/tracedb-agent-memory-flight-recorder.json",
        )

    def test_agent_memory_flight_recorder_receipt_contract(self) -> None:
        module = load_module()
        report = {
            "demo": "local-chat-memory",
            "invariant_failures": [],
            "summary": {},
            "flight_recorder_receipt": {
                "receipt_kind": "agent_memory_flight_recorder",
                "substrate": "TraceDB",
                "scope": "local_product_demo",
                "product_identity": "AI-native transactional candidate-stream database",
                "records": {
                    "table": "chat_memory",
                    "tenant": "tenant-alpha",
                    "record_count": 7,
                    "record_ids": ["alpha-memory-1", "alpha-memory-2"],
                    "tenants": ["tenant-alpha", "tenant-beta"],
                },
                "retrieval": {
                    "query_text": "deterministic local memory hybrid",
                    "freshness": "Strict",
                    "result_ids": ["alpha-memory-1"],
                },
                "provenance": {
                    "deleted_subject_record_ids": ["alpha-erased-1"],
                    "deleted_subject_visible_after_delete": False,
                },
                "replay": {
                    "commands_recorded": 20,
                    "command_exit_failures": [],
                },
                "tracefield_runtime": {"status": "not_implemented"},
                "tensor_artifacts": {"status": "future_module_layer"},
                "non_guarantees": [
                    "no TraceField runtime behavior",
                    "no tensor artifact support",
                ],
            },
        }

        summary = module.validate_agent_memory_flight_recorder_receipt(report)

        self.assertEqual(summary["receipt_kind"], "agent_memory_flight_recorder")
        self.assertEqual(summary["substrate"], "TraceDB")
        self.assertEqual(summary["record_count"], 7)
        self.assertEqual(summary["result_ids"], ["alpha-memory-1"])
        self.assertEqual(summary["commands_recorded"], 20)
        self.assertEqual(summary["tracefield_runtime_status"], "not_implemented")
        self.assertEqual(summary["tensor_artifacts_status"], "future_module_layer")

        bad_report = dict(report)
        bad_report["flight_recorder_receipt"] = dict(report["flight_recorder_receipt"])
        bad_report["flight_recorder_receipt"]["tracefield_runtime"] = {"status": "implemented"}
        with self.assertRaisesRegex(AssertionError, "TraceField runtime"):
            module.validate_agent_memory_flight_recorder_receipt(bad_report)

    def test_only_rejects_unknown_modal_product_command(self) -> None:
        module = load_module()

        with self.assertRaisesRegex(ValueError, "unknown --only command"):
            module.build_command_plan("workspace", only="does_not_exist")

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
                "steps_passed": 6,
                "steps_total": 6,
                "failed_step": None,
            },
            "steps": {
                "embedded_demo": {"ok": True},
                "embedded_verify": {"ok": True},
                "http_demo": {"ok": True},
                "local_doctor": {"ok": True},
                "rust_sdk_quickstart": {"ok": True},
                "python_sdk_smoke": {"ok": True},
            },
        }

        summary = module.validate_reduced_quickstart_receipt(
            json.dumps(receipt),
            receipt,
        )

        self.assertEqual(summary["steps_passed"], 6)
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
