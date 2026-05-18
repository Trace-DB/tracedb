from __future__ import annotations

import importlib
import io
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
        real_import = __import__

        def import_without_modal(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "modal":
                raise ImportError("modal intentionally hidden for test")
            return real_import(name, globals, locals, fromlist, level)

        sys.modules.pop("modal_bench", None)
        sys.modules.pop("modal", None)
        with patch("builtins.__import__", side_effect=import_without_modal):
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
                "--seed",
                "42",
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

    def test_pgvector_external_control_requires_explicit_guardrails(self) -> None:
        from modal_bench import (
            ModalSmokeConfig,
            build_runner_env,
            build_suite_command,
            validate_config,
        )

        with self.assertRaisesRegex(ValueError, "external controls"):
            validate_config(ModalSmokeConfig(target="pgvector", pgvector_control=True))
        with self.assertRaisesRegex(ValueError, "pgvector_control"):
            validate_config(
                ModalSmokeConfig(
                    target="tracedb",
                    allow_external_controls=True,
                    pgvector_control=True,
                )
            )

        config = ModalSmokeConfig(
            target="pgvector",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
            pgvector_control=True,
        )

        validate_config(config)
        command = build_suite_command(config)
        env = build_runner_env(config, base_env={"PATH": os.environ.get("PATH", "")})

        self.assertEqual(command[command.index("--target") + 1], "pgvector")
        self.assertIn("--require-services", command)
        self.assertEqual(
            env["BENCH_PGVECTOR_DSN"],
            "postgresql://tracedb:tracedb@127.0.0.1:25433/tracedb_bench",
        )
        self.assertNotIn("BENCH_POSTGRES_DSN", env)

    def test_tracedb_engine_control_requires_http_surface_and_tracedb_target(self) -> None:
        from modal_bench import ModalSmokeConfig, build_runner_env, validate_config

        with self.assertRaisesRegex(ValueError, "target including tracedb"):
            validate_config(
                ModalSmokeConfig(
                    target="pgvector",
                    surface="http",
                    allow_external_controls=True,
                    tracedb_engine_control=True,
                )
            )
        with self.assertRaisesRegex(ValueError, "surface including http or curl"):
            validate_config(
                ModalSmokeConfig(
                    target="tracedb",
                    surface="sdk",
                    tracedb_engine_control=True,
                )
            )

        config = ModalSmokeConfig(
            run_id="modal-tracedb-engine-smoke",
            target="tracedb",
            surface="http",
            scenarios="search_rag_6",
            tracedb_engine_control=True,
            tracedb_port=18_081,
        )

        validate_config(config)
        env = build_runner_env(config, base_env={"PATH": os.environ.get("PATH", "")})

        self.assertEqual(env["TRACEDB_HTTP_URL"], "http://127.0.0.1:18081")
        self.assertEqual(
            env["TRACEDB_HTTP_DATA_DIR"],
            "/tmp/tracedb-engine-modal-tracedb-engine-smoke",
        )

    def test_pgvector_external_control_requires_dsn_when_services_are_required(self) -> None:
        from modal_bench import ModalSmokeConfig, build_runner_env

        config = ModalSmokeConfig(
            target="pgvector",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
        )

        with self.assertRaisesRegex(ValueError, "BENCH_PGVECTOR_DSN"):
            build_runner_env(config, base_env={})

    def test_postgres_and_pgvector_controls_use_distinct_ports_and_dsns(self) -> None:
        from modal_bench import ModalSmokeConfig, build_runner_env, validate_config

        config = ModalSmokeConfig(
            target="postgres,pgvector",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
            postgres_control=True,
            pgvector_control=True,
            postgres_port=25_432,
            pgvector_port=25_433,
        )

        validate_config(config)
        env = build_runner_env(config, base_env={"PATH": os.environ.get("PATH", "")})

        self.assertEqual(
            env["BENCH_POSTGRES_DSN"],
            "postgresql://tracedb:tracedb@127.0.0.1:25432/tracedb_bench",
        )
        self.assertEqual(
            env["BENCH_PGVECTOR_DSN"],
            "postgresql://tracedb:tracedb@127.0.0.1:25433/tracedb_bench",
        )
        with self.assertRaisesRegex(ValueError, "distinct ports"):
            validate_config(
                ModalSmokeConfig(
                    target="postgres,pgvector",
                    allow_external_controls=True,
                    postgres_control=True,
                    pgvector_control=True,
                    postgres_port=25_432,
                    pgvector_port=25_432,
                )
            )

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
                "modal_bench.git_identity",
                return_value={"commit": "test", "dirty": False, "status_short": ""},
            ), patch("subprocess.run", return_value=completed) as run:
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

    def test_run_suite_passes_pgvector_dsn_env_without_live_pgvector(self) -> None:
        from modal_bench import ModalSmokeConfig, run_suite_and_bundle

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            run_dir = reports / "modal-pgvector-smoke"
            run_dir.mkdir(parents=True)
            suite_json = {
                "suite_id": "modal-pgvector-smoke",
                "control_status": "external_control_available",
                "summary": {"failure_count": 0},
                "control_ledger": {
                    "available_external_controls": [{"name": "pgvector"}],
                    "unavailable_external_controls": [],
                },
                "number_to_beat": {
                    "query_p95_ms": {"baseline": "pgvector", "value": 4.0},
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
                run_id="modal-pgvector-smoke",
                target="pgvector",
                scenarios="search_rag_6",
                reports_dir=str(reports),
                bundle_dir=str(root / "bundles"),
                min_free_mb=1_000,
                allow_external_controls=True,
                require_services=True,
                pgvector_control=True,
            )
            base_env = {"PATH": os.environ.get("PATH", "")}
            with patch.dict(os.environ, base_env, clear=True), patch(
                "modal_bench.git_identity",
                return_value={"commit": "test", "dirty": False, "status_short": ""},
            ), patch("modal_bench.start_pgvector_control") as start_pgvector, patch(
                "modal_bench.stop_postgres_control"
            ), patch(
                "subprocess.run", return_value=completed
            ) as run:
                start_pgvector.return_value = type(
                    "PostgresControlStub",
                    (),
                    {
                        "data_dir": root / "pgvector",
                        "log_path": root / "pgvector.log",
                        "port": 25433,
                    },
                )()
                summary = run_suite_and_bundle(config, lab_root=LAB_ROOT)

            self.assertEqual(summary["control_status"], "external_control_available")
            self.assertEqual(summary["available_external_controls"], ["pgvector"])
            self.assertEqual(
                summary["number_to_beat"]["query_p95_ms"]["baseline"], "pgvector"
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["BENCH_PGVECTOR_DSN"],
                "postgresql://tracedb:tracedb@127.0.0.1:25433/tracedb_bench",
            )
            self.assertEqual(run.call_args.kwargs["env"]["BENCH_DISABLE_ENV_FILE"], "1")
            self.assertIn("--require-services", run.call_args.args[0])

    def test_run_suite_starts_tracedb_engine_control_and_passes_http_env(self) -> None:
        from modal_bench import ModalSmokeConfig, run_suite_and_bundle

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            run_dir = reports / "modal-tracedb-engine-smoke"
            run_dir.mkdir(parents=True)
            suite_json = {
                "suite_id": "modal-tracedb-engine-smoke",
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

            completed = type(
                "Completed",
                (),
                {"returncode": 0, "stdout": "ok", "stderr": ""},
            )()
            config = ModalSmokeConfig(
                run_id="modal-tracedb-engine-smoke",
                target="tracedb",
                surface="http",
                scenarios="search_rag_6",
                reports_dir=str(reports),
                bundle_dir=str(root / "bundles"),
                min_free_mb=1_000,
                tracedb_engine_control=True,
            )
            service = type(
                "TraceDbEngineControlStub",
                (),
                {
                    "data_dir": root / "tracedb",
                    "log_path": root / "tracedb.log",
                    "port": 18080,
                    "process": None,
                },
            )()
            with patch.dict(os.environ, {"PATH": os.environ.get("PATH", "")}, clear=True), patch(
                "modal_bench.git_identity",
                return_value={"commit": "test", "dirty": False, "status_short": ""},
            ), patch(
                "modal_bench.start_tracedb_engine_control", return_value=service
            ) as start_tracedb, patch(
                "modal_bench.stop_tracedb_engine_control"
            ) as stop_tracedb, patch(
                "subprocess.run", return_value=completed
            ) as run:
                summary = run_suite_and_bundle(config, lab_root=LAB_ROOT)

            start_tracedb.assert_called_once()
            stop_tracedb.assert_called_once_with(service)
            self.assertEqual(summary["control_status"], "internal_only_smoke")
            self.assertEqual(
                run.call_args.kwargs["env"]["TRACEDB_HTTP_URL"],
                "http://127.0.0.1:18080",
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["TRACEDB_HTTP_DATA_DIR"],
                "/tmp/tracedb-engine-modal-tracedb-engine-smoke",
            )
            self.assertEqual(run.call_args.kwargs["env"]["BENCH_DISABLE_ENV_FILE"], "1")

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

    def test_manifest_redacts_pgvector_dsn(self) -> None:
        from modal_bench import ModalSmokeConfig, build_manifest

        config = ModalSmokeConfig(
            run_id="redaction-pgvector",
            target="pgvector",
            allow_external_controls=True,
            require_services=True,
        )
        manifest = build_manifest(
            config,
            ["python3", "-m", "runner", "suite"],
            runner_env={"BENCH_PGVECTOR_DSN": "postgresql://user:secret@127.0.0.1/db"},
        )

        manifest_text = json.dumps(manifest)

        self.assertIn("BENCH_PGVECTOR_DSN", manifest_text)
        self.assertNotIn("secret", manifest_text)
        self.assertEqual(manifest["runner_env"]["BENCH_PGVECTOR_DSN"], "[redacted]")

    def test_manifest_records_tracedb_http_engine_env_without_secrets(self) -> None:
        from modal_bench import ModalSmokeConfig, build_manifest

        manifest = build_manifest(
            ModalSmokeConfig(run_id="redaction-tracedb-engine"),
            ["python3", "-m", "runner", "suite"],
            runner_env={
                "TRACEDB_HTTP_URL": "http://127.0.0.1:18080",
                "TRACEDB_HTTP_DATA_DIR": "/tmp/tracedb-engine-redaction",
                "TRACEDB_HTTP_BEARER_TOKEN": "secret-token",
            },
        )

        manifest_text = json.dumps(manifest)

        self.assertIn("TRACEDB_HTTP_URL", manifest_text)
        self.assertIn("TRACEDB_HTTP_DATA_DIR", manifest_text)
        self.assertNotIn("secret-token", manifest_text)
        self.assertEqual(manifest["runner_env"]["TRACEDB_HTTP_BEARER_TOKEN"], "[redacted]")

    def test_manifest_records_git_identity_for_reproducibility(self) -> None:
        from modal_bench import ModalSmokeConfig, build_manifest

        with patch("modal_bench.git_identity") as identity:
            identity.return_value = {
                "commit": "abc123",
                "dirty": True,
                "status_short": " M benchmarks/realworld/modal_bench.py",
            }
            manifest = build_manifest(
                ModalSmokeConfig(run_id="git-id"),
                ["python3", "-m", "runner", "suite"],
                repo_root=LAB_ROOT.parent.parent,
            )

        identity.assert_called_once_with(LAB_ROOT.parent.parent)
        self.assertEqual(manifest["git"]["commit"], "abc123")
        self.assertTrue(manifest["git"]["dirty"])
        self.assertEqual(
            manifest["git"]["status_short"], " M benchmarks/realworld/modal_bench.py"
        )

    def test_manifest_prefers_source_git_identity_when_remote_mount_has_no_git_dir(self) -> None:
        from modal_bench import ModalSmokeConfig, build_manifest

        config = ModalSmokeConfig(
            run_id="source-git-id",
            source_commit="def456",
            source_dirty=False,
            source_status_short="",
            source_git_error=None,
        )
        with patch("modal_bench.git_identity") as identity:
            manifest = build_manifest(
                config,
                ["python3", "-m", "runner", "suite"],
                repo_root=Path("/workspace/TraceDB"),
            )

        identity.assert_not_called()
        self.assertEqual(manifest["git"]["commit"], "def456")
        self.assertFalse(manifest["git"]["dirty"])
        self.assertEqual(manifest["git"]["status_short"], "")

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

    def test_modal_image_kind_selects_only_requested_control_lane(self) -> None:
        from modal_bench import modal_image_kind_from_args

        self.assertEqual(modal_image_kind_from_args(["modal_bench.py"]), "base")
        self.assertEqual(
            modal_image_kind_from_args(["modal_bench.py", "--postgres-control"]),
            "postgres",
        )
        self.assertEqual(
            modal_image_kind_from_args(["modal_bench.py", "--pgvector-control"]),
            "pgvector",
        )
        self.assertEqual(
            modal_image_kind_from_args(["modal_bench.py", "--tracedb-engine-control"]),
            "tracedb",
        )
        self.assertEqual(
            modal_image_kind_from_args(
                ["modal_bench.py", "--tracedb-engine-control", "--pgvector-control"]
            ),
            "tracedb_pgvector",
        )

    def test_cli_config_can_override_min_free_for_tiny_local_smoke(self) -> None:
        from modal_bench import _parse_args, build_suite_command

        config = _parse_args(
            [
                "--run-id",
                "tiny",
                "--records",
                "16",
                "--min-free-mb",
                "512",
                "--seed",
                "777",
            ]
        )

        self.assertEqual(config.run_id, "tiny")
        self.assertEqual(config.records, 16)
        self.assertEqual(config.min_free_mb, 512)
        self.assertIn("--seed", build_suite_command(config))
        command = build_suite_command(config)
        self.assertEqual(command[command.index("--seed") + 1], "777")

    def test_run_local_writes_clean_summary_json(self) -> None:
        from modal_bench import run_local

        summary = {
            "run_id": "summary-json-test",
            "control_status": "external_control_available",
            "scenario_baselines": {
                "search_rag_6": {
                    "postgres": {"metrics": {"latency_p95_ms": 2.5}},
                    "tracedb": {"metrics": {"latency_p95_ms": 0.02}},
                }
            },
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            summary_path = Path(temp_dir) / "nested" / "summary.json"
            with patch("modal_bench.run_suite_and_bundle", return_value=summary), patch(
                "sys.stdout", new=io.StringIO()
            ):
                exit_code = run_local(
                    [
                        "--run-id",
                        "summary-json-test",
                        "--summary-json",
                        str(summary_path),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(json.loads(summary_path.read_text(encoding="utf-8")), summary)
            self.assertTrue(summary_path.read_text(encoding="utf-8").endswith("\n"))

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
