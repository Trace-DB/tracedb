from __future__ import annotations

import importlib
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path


LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))


class ModalBenchTests(unittest.TestCase):
    def test_imports_without_modal_installed(self) -> None:
        sys.modules.pop("modal_bench", None)
        module = importlib.import_module("modal_bench")

        self.assertTrue(hasattr(module, "ModalSmokeConfig"))
        self.assertIsNone(module.modal)

    def test_default_suite_command_is_cost_guarded_cpu_smoke(self) -> None:
        from modal_bench import ModalSmokeConfig, build_suite_command, validate_config

        config = ModalSmokeConfig(run_id="modal-smoke-test")
        validate_config(config)

        self.assertEqual(
            build_suite_command(config),
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
                "128",
                "--target",
                "tracedb",
                "--surface",
                "sdk",
                "--openrouter-mode",
                "off",
                "--openrouter-cap",
                "moderate",
                "--run-id",
                "modal-smoke-test",
                "--reports-dir",
                "/tmp/tracedb-modal-reports",
                "--scenarios",
                "sdk_cli_surface",
            ],
        )
        self.assertFalse(config.gpu_requested)
        self.assertGreaterEqual(config.ephemeral_disk_mb, 524_288)

    def test_resource_guardrails_reject_unsafe_defaults(self) -> None:
        from modal_bench import ModalSmokeConfig, validate_config

        with self.assertRaisesRegex(ValueError, "records"):
            validate_config(ModalSmokeConfig(records=2048))
        with self.assertRaisesRegex(ValueError, "GPU"):
            validate_config(ModalSmokeConfig(gpu_requested=True))
        with self.assertRaisesRegex(ValueError, "OpenRouter"):
            validate_config(ModalSmokeConfig(openrouter_mode="required"))
        with self.assertRaisesRegex(ValueError, "target=all"):
            validate_config(ModalSmokeConfig(target="all"))

    def test_cli_config_can_override_min_free_for_tiny_local_smoke(self) -> None:
        from modal_bench import _parse_args

        config = _parse_args(["--run-id", "tiny", "--records", "16", "--min-free-mb", "512"])

        self.assertEqual(config.run_id, "tiny")
        self.assertEqual(config.records, 16)
        self.assertEqual(config.min_free_mb, 512)

    def test_bundles_report_artifacts_and_extracts_control_summary(self) -> None:
        from modal_bench import (
            ModalSmokeConfig,
            build_manifest,
            bundle_report_artifacts,
            extract_control_summary,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            run_dir = reports / "modal-smoke-test"
            run_dir.mkdir(parents=True)
            suite_json = {
                "suite_id": "modal-smoke-test",
                "control_status": "internal_only_smoke",
                "summary": {"failure_count": 0},
                "control_ledger": {
                    "available_external_controls": [],
                    "unavailable_external_controls": [],
                },
                "number_to_beat": {
                    "query_p95_ms": {"baseline": None, "value": None},
                },
            }
            (run_dir / "suite.json").write_text(json.dumps(suite_json), encoding="utf-8")
            (run_dir / "suite.md").write_text("# suite\n", encoding="utf-8")

            config = ModalSmokeConfig(run_id="modal-smoke-test")
            manifest = build_manifest(config, ["python3", "-m", "runner", "suite"])
            bundle = bundle_report_artifacts(
                run_id=config.run_id,
                reports_dir=reports,
                bundle_dir=root / "bundles",
                manifest=manifest,
            )

            with tarfile.open(bundle, "r:gz") as archive:
                names = set(archive.getnames())

            self.assertIn("modal-smoke-test/suite.json", names)
            self.assertIn("modal-smoke-test/suite.md", names)
            self.assertIn("modal-smoke-test/manifest.json", names)

            summary = extract_control_summary(bundle, "modal-smoke-test")

        self.assertEqual(summary["run_id"], "modal-smoke-test")
        self.assertEqual(summary["control_status"], "internal_only_smoke")
        self.assertEqual(summary["failure_count"], 0)
        self.assertEqual(summary["number_to_beat"]["query_p95_ms"]["value"], None)


if __name__ == "__main__":
    unittest.main()
