from __future__ import annotations

import base64
import hashlib
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
                "--tracedb-ingest-mode",
                "per_record",
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
        with self.assertRaisesRegex(ValueError, "tracedb_ingest_mode"):
            validate_config(ModalSmokeConfig(tracedb_ingest_mode="invalid"))
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

    def test_qdrant_external_control_requires_explicit_guardrails(self) -> None:
        from modal_bench import (
            ModalSmokeConfig,
            build_runner_env,
            build_suite_command,
            validate_config,
        )

        with self.assertRaisesRegex(ValueError, "external controls"):
            validate_config(ModalSmokeConfig(target="qdrant", qdrant_control=True))
        with self.assertRaisesRegex(ValueError, "qdrant_control"):
            validate_config(
                ModalSmokeConfig(
                    target="tracedb",
                    allow_external_controls=True,
                    qdrant_control=True,
                )
            )

        config = ModalSmokeConfig(
            run_id="modal-qdrant-smoke",
            target="qdrant",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
            qdrant_control=True,
            qdrant_port=26_333,
        )

        validate_config(config)
        command = build_suite_command(config)
        env = build_runner_env(config, base_env={"PATH": os.environ.get("PATH", "")})

        self.assertEqual(command[command.index("--target") + 1], "qdrant")
        self.assertIn("--require-services", command)
        self.assertEqual(env["BENCH_QDRANT_URL"], "http://127.0.0.1:26333")
        self.assertEqual(env["BENCH_QDRANT_STORAGE_DIR"], "/tmp/tracedb-qdrant-modal-qdrant-smoke")
        self.assertNotIn("BENCH_POSTGRES_DSN", env)
        self.assertNotIn("BENCH_PGVECTOR_DSN", env)

    def test_qdrant_external_control_requires_url_when_services_are_required(self) -> None:
        from modal_bench import ModalSmokeConfig, build_runner_env

        config = ModalSmokeConfig(
            target="qdrant",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
        )

        with self.assertRaisesRegex(ValueError, "BENCH_QDRANT_URL"):
            build_runner_env(config, base_env={})

    def test_opensearch_external_control_requires_explicit_guardrails(self) -> None:
        from modal_bench import (
            ModalSmokeConfig,
            build_runner_env,
            build_suite_command,
            validate_config,
        )

        with self.assertRaisesRegex(ValueError, "external controls"):
            validate_config(ModalSmokeConfig(target="opensearch", opensearch_control=True))
        with self.assertRaisesRegex(ValueError, "opensearch_control"):
            validate_config(
                ModalSmokeConfig(
                    target="tracedb",
                    allow_external_controls=True,
                    opensearch_control=True,
                )
            )

        config = ModalSmokeConfig(
            run_id="modal-opensearch-smoke",
            target="opensearch",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
            opensearch_control=True,
            opensearch_port=29_200,
        )

        validate_config(config)
        command = build_suite_command(config)
        env = build_runner_env(config, base_env={"PATH": os.environ.get("PATH", "")})

        self.assertEqual(command[command.index("--target") + 1], "opensearch")
        self.assertIn("--require-services", command)
        self.assertEqual(env["BENCH_OPENSEARCH_URL"], "http://127.0.0.1:29200")
        self.assertEqual(
            env["BENCH_OPENSEARCH_STORAGE_DIR"],
            "/tmp/tracedb-opensearch-modal-opensearch-smoke",
        )
        self.assertNotIn("BENCH_POSTGRES_DSN", env)
        self.assertNotIn("BENCH_PGVECTOR_DSN", env)
        self.assertNotIn("BENCH_QDRANT_URL", env)

    def test_opensearch_external_control_requires_url_when_services_are_required(self) -> None:
        from modal_bench import ModalSmokeConfig, build_runner_env

        config = ModalSmokeConfig(
            target="opensearch",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
        )

        with self.assertRaisesRegex(ValueError, "BENCH_OPENSEARCH_URL"):
            build_runner_env(config, base_env={})

    def test_mongodb_external_control_requires_explicit_guardrails(self) -> None:
        from modal_bench import (
            ModalSmokeConfig,
            build_runner_env,
            build_suite_command,
            validate_config,
        )

        with self.assertRaisesRegex(ValueError, "external controls"):
            validate_config(ModalSmokeConfig(target="mongodb", mongodb_control=True))
        with self.assertRaisesRegex(ValueError, "mongodb_control"):
            validate_config(
                ModalSmokeConfig(
                    target="tracedb",
                    allow_external_controls=True,
                    mongodb_control=True,
                )
            )

        config = ModalSmokeConfig(
            run_id="modal-mongodb-smoke",
            target="mongodb",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
            mongodb_control=True,
            mongodb_port=27_027,
        )

        validate_config(config)
        command = build_suite_command(config)
        env = build_runner_env(config, base_env={"PATH": os.environ.get("PATH", "")})

        self.assertEqual(command[command.index("--target") + 1], "mongodb")
        self.assertIn("--require-services", command)
        self.assertEqual(env["BENCH_MONGO_URI"], "mongodb://127.0.0.1:27027")
        self.assertEqual(
            env["BENCH_MONGO_STORAGE_DIR"],
            "/tmp/tracedb-mongodb-modal-mongodb-smoke",
        )
        self.assertNotIn("BENCH_POSTGRES_DSN", env)
        self.assertNotIn("BENCH_PGVECTOR_DSN", env)
        self.assertNotIn("BENCH_QDRANT_URL", env)

    def test_mongodb_external_control_requires_uri_when_services_are_required(self) -> None:
        from modal_bench import ModalSmokeConfig, build_runner_env

        config = ModalSmokeConfig(
            target="mongodb",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
        )

        with self.assertRaisesRegex(ValueError, "BENCH_MONGO_URI"):
            build_runner_env(config, base_env={})

    def test_enabled_controls_use_distinct_ports_and_dsns(self) -> None:
        from modal_bench import ModalSmokeConfig, build_runner_env, validate_config

        config = ModalSmokeConfig(
            target="tracedb,postgres,pgvector,qdrant,opensearch,mongodb",
            surface="http",
            scenarios="search_rag_6",
            allow_external_controls=True,
            require_services=True,
            tracedb_engine_control=True,
            postgres_control=True,
            pgvector_control=True,
            qdrant_control=True,
            opensearch_control=True,
            mongodb_control=True,
            tracedb_port=18_080,
            postgres_port=25_432,
            pgvector_port=25_433,
            qdrant_port=26_333,
            opensearch_port=29_200,
            mongodb_port=27_027,
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
        self.assertEqual(env["BENCH_QDRANT_URL"], "http://127.0.0.1:26333")
        self.assertEqual(env["BENCH_OPENSEARCH_URL"], "http://127.0.0.1:29200")
        self.assertEqual(env["BENCH_MONGO_URI"], "mongodb://127.0.0.1:27027")
        self.assertEqual(env["TRACEDB_HTTP_URL"], "http://127.0.0.1:18080")
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
        with self.assertRaisesRegex(ValueError, "distinct ports"):
            validate_config(
                ModalSmokeConfig(
                    target="tracedb,pgvector",
                    surface="http",
                    allow_external_controls=True,
                    tracedb_engine_control=True,
                    pgvector_control=True,
                    tracedb_port=25_433,
                    pgvector_port=25_433,
                )
            )
        with self.assertRaisesRegex(ValueError, "distinct ports"):
            validate_config(
                ModalSmokeConfig(
                    target="pgvector,qdrant",
                    allow_external_controls=True,
                    pgvector_control=True,
                    qdrant_control=True,
                    pgvector_port=25_433,
                    qdrant_port=25_433,
                )
            )
        with self.assertRaisesRegex(ValueError, "distinct ports"):
            validate_config(
                ModalSmokeConfig(
                    target="qdrant,opensearch",
                    allow_external_controls=True,
                    qdrant_control=True,
                    opensearch_control=True,
                    qdrant_port=28_998,
                    opensearch_port=28_999,
                )
            )
        with self.assertRaisesRegex(ValueError, "distinct ports"):
            validate_config(
                ModalSmokeConfig(
                    target="qdrant,mongodb",
                    allow_external_controls=True,
                    qdrant_control=True,
                    mongodb_control=True,
                    qdrant_port=27_026,
                    mongodb_port=27_027,
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

    def test_run_suite_passes_qdrant_env_without_live_qdrant(self) -> None:
        from modal_bench import ModalSmokeConfig, run_suite_and_bundle

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            run_dir = reports / "modal-qdrant-smoke"
            run_dir.mkdir(parents=True)
            suite_json = {
                "suite_id": "modal-qdrant-smoke",
                "control_status": "external_control_available",
                "summary": {"failure_count": 0},
                "control_ledger": {
                    "available_external_controls": [{"name": "qdrant"}],
                    "unavailable_external_controls": [],
                },
                "number_to_beat": {
                    "query_p95_ms": {"baseline": "qdrant", "value": 4.0},
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
                run_id="modal-qdrant-smoke",
                target="qdrant",
                scenarios="search_rag_6",
                reports_dir=str(reports),
                bundle_dir=str(root / "bundles"),
                min_free_mb=1_000,
                allow_external_controls=True,
                require_services=True,
                qdrant_control=True,
            )
            base_env = {"PATH": os.environ.get("PATH", "")}
            qdrant_service = type(
                "QdrantControlStub",
                (),
                {
                    "data_dir": root / "qdrant",
                    "log_path": root / "qdrant.log",
                    "port": 26333,
                    "process": None,
                },
            )()
            with patch.dict(os.environ, base_env, clear=True), patch(
                "modal_bench.git_identity",
                return_value={"commit": "test", "dirty": False, "status_short": ""},
            ), patch(
                "modal_bench.start_qdrant_control", return_value=qdrant_service
            ) as start_qdrant, patch(
                "modal_bench.stop_qdrant_control"
            ) as stop_qdrant, patch(
                "subprocess.run", return_value=completed
            ) as run:
                summary = run_suite_and_bundle(config, lab_root=LAB_ROOT)

            start_qdrant.assert_called_once()
            stop_qdrant.assert_called_once_with(qdrant_service)
            self.assertEqual(summary["control_status"], "external_control_available")
            self.assertEqual(summary["available_external_controls"], ["qdrant"])
            self.assertEqual(
                summary["number_to_beat"]["query_p95_ms"]["baseline"], "qdrant"
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["BENCH_QDRANT_URL"],
                "http://127.0.0.1:26333",
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["BENCH_QDRANT_STORAGE_DIR"],
                "/tmp/tracedb-qdrant-modal-qdrant-smoke",
            )
            self.assertEqual(run.call_args.kwargs["env"]["BENCH_DISABLE_ENV_FILE"], "1")
            self.assertIn("--require-services", run.call_args.args[0])

    def test_run_suite_passes_opensearch_env_without_live_opensearch(self) -> None:
        from modal_bench import ModalSmokeConfig, run_suite_and_bundle

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            run_dir = reports / "modal-opensearch-smoke"
            run_dir.mkdir(parents=True)
            suite_json = {
                "suite_id": "modal-opensearch-smoke",
                "control_status": "external_control_available",
                "summary": {"failure_count": 0},
                "control_ledger": {
                    "available_external_controls": [{"name": "opensearch"}],
                    "unavailable_external_controls": [],
                },
                "number_to_beat": {
                    "query_p95_ms": {"baseline": "opensearch", "value": 4.0},
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
                run_id="modal-opensearch-smoke",
                target="opensearch",
                scenarios="search_rag_6",
                reports_dir=str(reports),
                bundle_dir=str(root / "bundles"),
                min_free_mb=1_000,
                allow_external_controls=True,
                require_services=True,
                opensearch_control=True,
            )
            base_env = {"PATH": os.environ.get("PATH", "")}
            opensearch_service = type(
                "OpenSearchControlStub",
                (),
                {
                    "data_dir": root / "opensearch",
                    "log_path": root / "opensearch.log",
                    "port": 29200,
                    "process": None,
                },
            )()
            with patch.dict(os.environ, base_env, clear=True), patch(
                "modal_bench.git_identity",
                return_value={"commit": "test", "dirty": False, "status_short": ""},
            ), patch(
                "modal_bench.start_opensearch_control", return_value=opensearch_service
            ) as start_opensearch, patch(
                "modal_bench.stop_opensearch_control"
            ) as stop_opensearch, patch(
                "subprocess.run", return_value=completed
            ) as run:
                summary = run_suite_and_bundle(config, lab_root=LAB_ROOT)

            start_opensearch.assert_called_once()
            stop_opensearch.assert_called_once_with(opensearch_service)
            self.assertEqual(summary["control_status"], "external_control_available")
            self.assertEqual(summary["available_external_controls"], ["opensearch"])
            self.assertEqual(
                summary["number_to_beat"]["query_p95_ms"]["baseline"], "opensearch"
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["BENCH_OPENSEARCH_URL"],
                "http://127.0.0.1:29200",
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["BENCH_OPENSEARCH_STORAGE_DIR"],
                "/tmp/tracedb-opensearch-modal-opensearch-smoke",
            )
            self.assertEqual(run.call_args.kwargs["env"]["BENCH_DISABLE_ENV_FILE"], "1")
            self.assertIn("--require-services", run.call_args.args[0])

    def test_run_suite_passes_mongodb_env_without_live_mongodb(self) -> None:
        from modal_bench import ModalSmokeConfig, run_suite_and_bundle

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            run_dir = reports / "modal-mongodb-smoke"
            run_dir.mkdir(parents=True)
            suite_json = {
                "suite_id": "modal-mongodb-smoke",
                "control_status": "external_control_available",
                "summary": {"failure_count": 0},
                "control_ledger": {
                    "available_external_controls": [{"name": "mongodb"}],
                    "unavailable_external_controls": [],
                },
                "number_to_beat": {
                    "query_p95_ms": {"baseline": "mongodb", "value": 4.0},
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
                run_id="modal-mongodb-smoke",
                target="mongodb",
                scenarios="search_rag_6",
                reports_dir=str(reports),
                bundle_dir=str(root / "bundles"),
                min_free_mb=1_000,
                allow_external_controls=True,
                require_services=True,
                mongodb_control=True,
            )
            base_env = {"PATH": os.environ.get("PATH", "")}
            mongodb_service = type(
                "MongoDbControlStub",
                (),
                {
                    "data_dir": root / "mongodb",
                    "log_path": root / "mongodb.log",
                    "port": 27027,
                    "process": None,
                },
            )()
            with patch.dict(os.environ, base_env, clear=True), patch(
                "modal_bench.git_identity",
                return_value={"commit": "test", "dirty": False, "status_short": ""},
            ), patch(
                "modal_bench.start_mongodb_control", return_value=mongodb_service
            ) as start_mongodb, patch(
                "modal_bench.stop_mongodb_control"
            ) as stop_mongodb, patch(
                "subprocess.run", return_value=completed
            ) as run:
                summary = run_suite_and_bundle(config, lab_root=LAB_ROOT)

            start_mongodb.assert_called_once()
            stop_mongodb.assert_called_once_with(mongodb_service)
            self.assertEqual(summary["control_status"], "external_control_available")
            self.assertEqual(summary["available_external_controls"], ["mongodb"])
            self.assertEqual(
                summary["number_to_beat"]["query_p95_ms"]["baseline"], "mongodb"
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["BENCH_MONGO_URI"],
                "mongodb://127.0.0.1:27027",
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["BENCH_MONGO_STORAGE_DIR"],
                "/tmp/tracedb-mongodb-modal-mongodb-smoke",
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

    def test_manifest_redacts_mongodb_uri(self) -> None:
        from modal_bench import ModalSmokeConfig, build_manifest

        config = ModalSmokeConfig(
            run_id="redaction-mongodb",
            target="mongodb",
            allow_external_controls=True,
            require_services=True,
        )
        manifest = build_manifest(
            config,
            ["python3", "-m", "runner", "suite"],
            runner_env={"BENCH_MONGO_URI": "mongodb://user:secret@127.0.0.1/db"},
        )

        manifest_text = json.dumps(manifest)

        self.assertIn("BENCH_MONGO_URI", manifest_text)
        self.assertNotIn("secret", manifest_text)
        self.assertEqual(manifest["runner_env"]["BENCH_MONGO_URI"], "[redacted]")

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

    def test_run_suite_redacts_sensitive_values_from_process_tails(self) -> None:
        from modal_bench import ModalSmokeConfig, run_suite_and_bundle

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports = root / "reports"
            run_dir = reports / "redacted-process-tail"
            run_dir.mkdir(parents=True)
            suite_json = {
                "suite_id": "redacted-process-tail",
                "control_status": "external_control_available",
                "summary": {"failure_count": 0},
                "control_ledger": {
                    "available_external_controls": [{"name": "PostgreSQL"}],
                    "unavailable_external_controls": [],
                },
                "number_to_beat": {},
            }
            (run_dir / "suite.json").write_text(json.dumps(suite_json), encoding="utf-8")
            (run_dir / "suite.md").write_text("# suite\n", encoding="utf-8")

            secret_dsn = "postgresql://user:secret@127.0.0.1:25432/db"
            completed = type(
                "Completed",
                (),
                {
                    "returncode": 0,
                    "stdout": f"connected to {secret_dsn}",
                    "stderr": f"retrying {secret_dsn}",
                },
            )()
            config = ModalSmokeConfig(
                run_id="redacted-process-tail",
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
                "BENCH_POSTGRES_DSN": secret_dsn,
            }
            with patch.dict(os.environ, base_env, clear=True), patch(
                "modal_bench.git_identity",
                return_value={"commit": "test", "dirty": False, "status_short": ""},
            ), patch("subprocess.run", return_value=completed):
                summary = run_suite_and_bundle(config, lab_root=LAB_ROOT)

            process_text = json.dumps(summary["manifest"]["process"])
            self.assertNotIn("secret", process_text)
            self.assertIn("[redacted]", process_text)

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
            modal_image_kind_from_args(["modal_bench.py", "--qdrant-control"]),
            "qdrant",
        )
        self.assertEqual(
            modal_image_kind_from_args(["modal_bench.py", "--opensearch-control"]),
            "opensearch",
        )
        self.assertEqual(
            modal_image_kind_from_args(["modal_bench.py", "--mongodb-control"]),
            "mongodb",
        )
        self.assertEqual(
            modal_image_kind_from_args(
                ["modal_bench.py", "--tracedb-engine-control", "--pgvector-control"]
            ),
            "tracedb_pgvector",
        )
        self.assertEqual(
            modal_image_kind_from_args(
                ["modal_bench.py", "--tracedb-engine-control", "--qdrant-control"]
            ),
            "tracedb_qdrant",
        )
        self.assertEqual(
            modal_image_kind_from_args(
                ["modal_bench.py", "--tracedb-engine-control", "--opensearch-control"]
            ),
            "tracedb_opensearch",
        )
        self.assertEqual(
            modal_image_kind_from_args(
                ["modal_bench.py", "--tracedb-engine-control", "--mongodb-control"]
            ),
            "tracedb_mongodb",
        )
        self.assertEqual(
            modal_image_kind_from_args(
                [
                    "modal_bench.py",
                    "--tracedb-engine-control",
                    "--pgvector-control",
                    "--opensearch-control",
                ]
            ),
            "tracedb_controls",
        )
        self.assertEqual(
            modal_image_kind_from_args(
                ["modal_bench.py", "--pgvector-control", "--opensearch-control"]
            ),
            "external_controls",
        )

    def test_qdrant_modal_release_uses_musl_binary(self) -> None:
        from modal_bench import QDRANT_RELEASE_URL

        self.assertIn("unknown-linux-musl", QDRANT_RELEASE_URL)
        self.assertNotIn("unknown-linux-gnu", QDRANT_RELEASE_URL)

    def test_opensearch_modal_release_uses_official_linux_tarball(self) -> None:
        from modal_bench import OPENSEARCH_RELEASE_URL

        self.assertIn("artifacts.opensearch.org", OPENSEARCH_RELEASE_URL)
        self.assertIn("linux-x64.tar.gz", OPENSEARCH_RELEASE_URL)

    def test_mongodb_modal_release_uses_debian12_tarball(self) -> None:
        from modal_bench import MONGODB_RELEASE_URL, MONGODB_VERSION

        self.assertEqual(MONGODB_VERSION, "8.0.23")
        self.assertIn("fastdl.mongodb.org/linux", MONGODB_RELEASE_URL)
        self.assertIn("debian12", MONGODB_RELEASE_URL)
        self.assertTrue(MONGODB_RELEASE_URL.endswith(".tgz"))

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
                "--tracedb-ingest-mode",
                "batch",
            ]
        )

        self.assertEqual(config.run_id, "tiny")
        self.assertEqual(config.records, 16)
        self.assertEqual(config.min_free_mb, 512)
        self.assertEqual(config.tracedb_ingest_mode, "batch")
        self.assertIn("--seed", build_suite_command(config))
        command = build_suite_command(config)
        self.assertEqual(command[command.index("--seed") + 1], "777")
        self.assertEqual(
            command[command.index("--tracedb-ingest-mode") + 1],
            "batch",
        )

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

    def test_run_local_exports_local_bundle_output_and_records_checksum(self) -> None:
        from modal_bench import run_local

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_bundle = root / "source.tar.gz"
            bundle_bytes = b"bundle-bytes"
            source_bundle.write_bytes(bundle_bytes)
            summary = {
                "run_id": "bundle-output-test",
                "bundle_path": str(source_bundle),
                "control_status": "external_control_available",
            }
            summary_path = root / "summary.json"
            bundle_output = root / "exports" / "bundle.tar.gz"

            with patch("modal_bench.run_suite_and_bundle", return_value=summary), patch(
                "sys.stdout", new=io.StringIO()
            ):
                exit_code = run_local(
                    [
                        "--run-id",
                        "bundle-output-test",
                        "--summary-json",
                        str(summary_path),
                        "--bundle-output",
                        str(bundle_output),
                    ]
                )

            written_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(exit_code, 0)
            self.assertEqual(bundle_output.read_bytes(), bundle_bytes)
            self.assertEqual(str(bundle_output), written_summary["exported_bundle_path"])
            self.assertEqual(
                hashlib.sha256(bundle_bytes).hexdigest(),
                written_summary["exported_bundle_sha256"],
            )
            self.assertEqual(len(bundle_bytes), written_summary["exported_bundle_size_bytes"])
            self.assertEqual(str(source_bundle), written_summary["exported_bundle_source_path"])
            self.assertEqual("local_copy", written_summary["bundle_export_transport"])
            self.assertTrue(written_summary["exported_bundle_checksum_verified"])

    def test_write_bundle_output_decodes_remote_payload_without_leaking_summary_bytes(
        self,
    ) -> None:
        from modal_bench import (
            BUNDLE_BYTES_FIELD,
            BUNDLE_SHA256_FIELD,
            BUNDLE_SIZE_FIELD,
            write_bundle_output,
        )

        bundle_bytes = b"remote-bundle-bytes"
        summary = {
            "run_id": "remote-bundle-output-test",
            "bundle_path": "/tmp/remote-bundle.tar.gz",
            BUNDLE_BYTES_FIELD: base64.b64encode(bundle_bytes).decode("ascii"),
            BUNDLE_SHA256_FIELD: hashlib.sha256(bundle_bytes).hexdigest(),
            BUNDLE_SIZE_FIELD: len(bundle_bytes),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            bundle_output = Path(temp_dir) / "bundle.tar.gz"
            clean_summary = write_bundle_output(summary, str(bundle_output))

            self.assertEqual(bundle_output.read_bytes(), bundle_bytes)
            self.assertNotIn(BUNDLE_BYTES_FIELD, clean_summary)
            self.assertNotIn(BUNDLE_BYTES_FIELD, summary)
            self.assertEqual(str(bundle_output), clean_summary["exported_bundle_path"])
            self.assertEqual(
                hashlib.sha256(bundle_bytes).hexdigest(),
                clean_summary["exported_bundle_sha256"],
            )
            self.assertEqual(len(bundle_bytes), clean_summary["exported_bundle_size_bytes"])
            self.assertEqual(
                "/tmp/remote-bundle.tar.gz",
                clean_summary["exported_bundle_source_path"],
            )
            self.assertEqual("modal_return_bytes", clean_summary["bundle_export_transport"])
            self.assertTrue(clean_summary["exported_bundle_checksum_verified"])

    def test_write_bundle_output_rejects_remote_payload_without_checksum(self) -> None:
        from modal_bench import BUNDLE_BYTES_FIELD, write_bundle_output

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "checksum"):
                write_bundle_output(
                    {
                        "bundle_path": "/tmp/remote-bundle.tar.gz",
                        BUNDLE_BYTES_FIELD: base64.b64encode(b"bundle").decode("ascii"),
                    },
                    str(Path(temp_dir) / "bundle.tar.gz"),
                )

    def test_write_bundle_output_rejects_remote_payload_checksum_mismatch(self) -> None:
        from modal_bench import BUNDLE_BYTES_FIELD, BUNDLE_SHA256_FIELD, write_bundle_output

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                write_bundle_output(
                    {
                        "bundle_path": "/tmp/remote-bundle.tar.gz",
                        BUNDLE_BYTES_FIELD: base64.b64encode(b"bundle").decode("ascii"),
                        BUNDLE_SHA256_FIELD: "not-the-real-sha",
                    },
                    str(Path(temp_dir) / "bundle.tar.gz"),
                )

    def test_attach_bundle_bytes_rejects_oversized_bundle_payload(self) -> None:
        from modal_bench import attach_bundle_bytes

        with tempfile.TemporaryDirectory() as temp_dir:
            source_bundle = Path(temp_dir) / "source.tar.gz"
            source_bundle.write_bytes(b"x")

            with self.assertRaisesRegex(ValueError, "bundle exceeds"):
                attach_bundle_bytes({"bundle_path": str(source_bundle)}, max_mb=0)

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
                                "query_results": [
                                    {
                                        "query_id": "qrel-1",
                                        "expected_ids": ["doc-expected"],
                                        "actual_ids": ["doc-actual"],
                                        "recall_at_k": 0.0,
                                    }
                                ],
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
                "tracedb_attribution": [
                    {
                        "scenario_id": "search_rag_6",
                        "query": {"query_latency_p95_ms": 4.0},
                        "query_phases": {"access_path_build_latency_p95_ms": 1.5},
                        "http_client": {
                            "latency_p95_ms": 4.0,
                            "overhead_latency_p95_ms": 2.5,
                        },
                        "response": {
                            "body_bytes_p95": 2048,
                            "processing_latency_p95_ms": 0.4,
                        },
                        "server": {"engine_latency_p95_ms": 1.0},
                        "engine": {"phase_total_latency_p95_ms": 1.5},
                        "access_paths": {},
                        "storage_after_ingest": {"wal": 512},
                        "storage_after_workload": {"wal": 1024},
                    }
                ],
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
        self.assertEqual(
            summary["scenario_baselines"]["search_rag_6"]["tracedb"]["query_results"][0][
                "query_id"
            ],
            "qrel-1",
        )
        self.assertEqual(
            summary["scenario_baselines"]["search_rag_6"]["tracedb"]["query_results"][0][
                "actual_ids"
            ],
            ["doc-actual"],
        )
        self.assertEqual(summary["scenario_datasets"]["search_rag_6"]["digest"], "digest-123")
        self.assertEqual(
            summary["tracedb_attribution"][0]["query"]["query_latency_p95_ms"],
            4.0,
        )
        self.assertEqual(
            summary["tracedb_attribution"][0]["http_client"]["overhead_latency_p95_ms"],
            2.5,
        )
        self.assertEqual(
            summary["tracedb_attribution"][0]["response"]["body_bytes_p95"],
            2048,
        )


if __name__ == "__main__":
    unittest.main()
