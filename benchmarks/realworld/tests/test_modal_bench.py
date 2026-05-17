from __future__ import annotations

import importlib
import json
import os
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


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
        with self.assertRaisesRegex(ValueError, "external controls"):
            validate_config(ModalSmokeConfig(target="tracedb,postgres"))

    def test_postgres_external_control_command_requires_explicit_guardrails(self) -> None:
        from modal_bench import ModalSmokeConfig, build_suite_command, validate_config

        config = ModalSmokeConfig(
            target="postgres",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
        )

        validate_config(config)
        command = build_suite_command(config)

        self.assertIn("--target", command)
        self.assertEqual(command[command.index("--target") + 1], "postgres")
        self.assertIn("--scenarios", command)
        self.assertEqual(command[command.index("--scenarios") + 1], "search_rag_6")
        self.assertIn("--require-services", command)
        self.assertIn("--openrouter-mode", command)
        self.assertEqual(command[command.index("--openrouter-mode") + 1], "off")

        default_command = build_suite_command(ModalSmokeConfig())
        self.assertNotIn("--require-services", default_command)

    def test_postgres_external_control_requires_dsn_when_services_are_required(self) -> None:
        from modal_bench import ModalSmokeConfig, build_runner_env

        config = ModalSmokeConfig(
            target="postgres",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
        )

        with self.assertRaisesRegex(ValueError, "BENCH_POSTGRES_DSN"):
            build_runner_env(config, base_env={})

    def test_run_suite_passes_postgres_dsn_env_without_live_postgres(self) -> None:
        from modal_bench import ModalSmokeConfig, run_suite_and_bundle

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            run_dir = reports / "modal-postgres-smoke"
            run_dir.mkdir(parents=True)
            suite_json = {
                "suite_id": "modal-postgres-smoke",
                "control_status": "external_control_available",
                "summary": {"failure_count": 0},
                "control_ledger": {
                    "available_external_controls": [{"name": "PostgreSQL"}],
                    "unavailable_external_controls": [],
                },
                "number_to_beat": {
                    "query_p95_ms": {"baseline": "PostgreSQL", "value": 5.0},
                },
            }
            (run_dir / "suite.json").write_text(json.dumps(suite_json), encoding="utf-8")
            (run_dir / "suite.md").write_text("# suite\n", encoding="utf-8")

            completed = type(
                "Completed",
                (),
                {"returncode": 0, "stdout": "ok", "stderr": ""},
            )()
            config = ModalSmokeConfig(
                run_id="modal-postgres-smoke",
                target="postgres",
                scenarios="search_rag_6",
                reports_dir=str(reports),
                bundle_dir=str(root / "bundles"),
                min_free_mb=1_000,
                allow_external_controls=True,
                require_services=True,
            )
            base_env = {
                "PATH": os.environ.get("PATH", ""),
                "BENCH_POSTGRES_DSN": "postgresql://user:secret@127.0.0.1:25432/db",
            }
            with patch.dict(os.environ, base_env, clear=True), patch(
                "subprocess.run", return_value=completed
            ) as run:
                summary = run_suite_and_bundle(config, lab_root=LAB_ROOT)

            self.assertEqual(summary["control_status"], "external_control_available")
            self.assertEqual(summary["available_external_controls"], ["PostgreSQL"])
            self.assertEqual(
                summary["number_to_beat"]["query_p95_ms"]["baseline"], "PostgreSQL"
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["BENCH_POSTGRES_DSN"],
                "postgresql://user:secret@127.0.0.1:25432/db",
            )
            self.assertEqual(run.call_args.kwargs["env"]["BENCH_DISABLE_ENV_FILE"], "1")
            self.assertIn("--require-services", run.call_args.args[0])

    def test_manifest_redacts_postgres_dsn(self) -> None:
        from modal_bench import ModalSmokeConfig, build_manifest

        config = ModalSmokeConfig(
            run_id="redaction",
            target="postgres",
            allow_external_controls=True,
            require_services=True,
        )
        manifest = build_manifest(
            config,
            ["python3", "-m", "runner", "suite"],
            runner_env={"BENCH_POSTGRES_DSN": "postgresql://user:secret@127.0.0.1/db"},
        )

        manifest_text = json.dumps(manifest)

        self.assertIn("BENCH_POSTGRES_DSN", manifest_text)
        self.assertNotIn("secret", manifest_text)
        self.assertEqual(manifest["runner_env"]["BENCH_POSTGRES_DSN"], "[redacted]")

    def test_modal_app_identity_can_be_overridden_for_variance_runs(self) -> None:
        from modal_bench import ModalSmokeConfig, _parse_args, build_manifest, modal_app_name

        with patch.dict(os.environ, {"TRACEDB_MODAL_APP_NAME": "tracedb-postgres-a"}, clear=False):
            self.assertEqual(modal_app_name(), "tracedb-postgres-a")
            config = _parse_args(["--run-id", "variance-a"])
            manifest = build_manifest(
                config,
                ["python3", "-m", "runner", "suite"],
            )

        self.assertEqual(config.modal_app_name, "tracedb-postgres-a")
        self.assertEqual(manifest["modal_app_name"], "tracedb-postgres-a")
        self.assertEqual(
            build_manifest(
                ModalSmokeConfig(run_id="explicit", modal_app_name="tracedb-postgres-b"),
                ["python3", "-m", "runner", "suite"],
            )["modal_app_name"],
            "tracedb-postgres-b",
        )

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
                "scenarios": [
                    {
                        "id": "search_rag_6",
                        "dataset": {"digest": "digest-123", "records": 128},
                        "baselines": [
                            {
                                "name": "tracedb",
                                "available": True,
                                "metrics": {"latency_p95_ms": 4.0, "query_count": 6},
                                "notes": ["TraceDB ran"],
                            },
                            {
                                "name": "postgres",
                                "available": True,
                                "metrics": {"latency_p95_ms": 5.0, "query_count": 6},
                                "notes": ["PostgreSQL ran"],
                            },
                        ],
                    }
                ],
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
        self.assertEqual(
            summary["scenario_baselines"]["search_rag_6"]["postgres"]["metrics"][
                "latency_p95_ms"
            ],
            5.0,
        )
        self.assertEqual(
            summary["scenario_baselines"]["search_rag_6"]["tracedb"]["metrics"][
                "latency_p95_ms"
            ],
            4.0,
        )
        self.assertEqual(summary["scenario_datasets"]["search_rag_6"]["digest"], "digest-123")


if __name__ == "__main__":
    unittest.main()
