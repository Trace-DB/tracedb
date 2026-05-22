from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "platform_conformance.py"


def load_module():
    spec = importlib.util.spec_from_file_location("platform_conformance", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.pop("platform_conformance", None)
    spec.loader.exec_module(module)
    return module


class PlatformConformanceTests(unittest.TestCase):
    def test_loads_contract_manifest_scenarios_and_surfaces(self) -> None:
        module = load_module()

        manifest = module.load_contract(ROOT / "docs" / "platform-contract-v0.json")

        self.assertEqual(manifest["contract"], "tracedb-platform-contract-v0")
        self.assertEqual(
            module.contract_scenario_ids(manifest),
            [
                "schema_apply",
                "put",
                "batch",
                "patch",
                "get",
                "scan",
                "query",
                "traceql_string_execution",
                "explain",
                "delete",
                "idempotency",
                "errors",
                "snapshot_restore",
            ],
        )
        self.assertIn("http_direct", module.contract_surface_ids(manifest))
        self.assertIn("rust_sdk", module.contract_surface_ids(manifest))
        self.assertIn("typescript_sdk", module.contract_surface_ids(manifest))
        self.assertIn("python_sdk", module.contract_surface_ids(manifest))
        self.assertIn("traceql_sqlish", module.contract_surface_ids(manifest))
        self.assertIn("graphql", module.contract_surface_ids(manifest))

    def test_python_sdk_sync_package_declares_contract_surface(self) -> None:
        package_root = ROOT / "clients" / "python"

        self.assertTrue((package_root / "tracedb" / "__init__.py").exists())
        self.assertTrue((package_root / "tracedb" / "client.py").exists())
        self.assertTrue((package_root / "http_smoke.py").exists())
        self.assertTrue((package_root / "README.md").exists())

        client_source = (package_root / "tracedb" / "client.py").read_text()
        for token in [
            "class TraceDB",
            "class TraceDBTable",
            "class TraceDBQueryBuilder",
            "class TraceDBHTTPError",
            "def insert_batch",
            "def with_options",
            "Idempotency-Key",
            "database_id",
            "branch_id",
            "safe_retries",
            "TRACEDB_SAFE_RETRIES",
        ]:
            self.assertIn(token, client_source)

        smoke_source = (package_root / "http_smoke.py").read_text()
        self.assertIn("TRACEDB_PYTHON_IMPORT_MODE", smoke_source)
        self.assertLess(
            smoke_source.index("TRACEDB_PYTHON_IMPORT_MODE"),
            smoke_source.index("from tracedb import"),
        )

    def test_python_sdk_conformance_installs_package_before_http_smoke(self) -> None:
        script_source = SCRIPT.read_text()

        for token in [
            "install_python_sdk_package_for_conformance",
            "shutil.copytree",
            "--target",
            "PYTHONPATH",
            "TRACEDB_PYTHON_IMPORT_MODE",
            "installed",
            "sys.executable",
        ]:
            self.assertIn(token, script_source)

    def test_typescript_public_http_smoke_declares_contract_evidence_output(self) -> None:
        smoke_source = (ROOT / "clients" / "typescript" / "public-http-smoke.ts").read_text()

        for token in [
            "--summary-json",
            "idempotency",
            "idempotency_conflict_status",
            "error_envelope",
            "TraceDbHttpError",
        ]:
            self.assertIn(token, smoke_source)

    def test_typescript_gateway_smoke_allocates_gateway_port_after_engine_binds(self) -> None:
        smoke_source = (ROOT / "clients" / "typescript" / "gateway-smoke.ts").read_text()

        engine_ready_index = smoke_source.index('await waitForReady("tracedb engine"')
        gateway_port_index = smoke_source.index("const gatewayPort = await freePort();")

        self.assertLess(engine_ready_index, gateway_port_index)

    def test_rust_sdk_product_summary_maps_to_contract_scenarios(self) -> None:
        module = load_module()
        manifest = module.load_contract(ROOT / "docs" / "platform-contract-v0.json")
        product_summary = {
            "ok": True,
            "mode": "local-product-regression",
            "steps": {
                "rust_sdk_quickstart": {
                    "ok": True,
                    "summary": {
                        "ok": True,
                        "mode": "rust-sdk-quickstart",
                        "steps": {
                            "schema_apply": True,
                            "put": True,
                            "batch_ingest": True,
                            "patch": True,
                            "scan": True,
                            "query": True,
                            "traceql_string_execution": True,
                            "explain": True,
                            "delete": True,
                            "error_envelope": True,
                            "snapshot": True,
                            "restore": True,
                        },
                        "patched_status": "reviewed",
                        "deleted_hidden": True,
                        "records_put": 1,
                        "put_epoch": 2,
                        "traceql_result_count": 1,
                        "traceql_explain": True,
                        "error_envelope": {
                            "status": 400,
                            "error": "missing field `table`",
                            "code": "bad_request",
                        },
                        "idempotency_keys": True,
                        "idempotency_retries": 1,
                    },
                }
            },
        }

        surface = module.map_rust_sdk_product_summary(manifest, product_summary)
        scenarios = {scenario["id"]: scenario for scenario in surface["scenarios"]}

        self.assertEqual(surface["surface"], "rust_sdk")
        self.assertEqual(scenarios["schema_apply"]["status"], "passed")
        self.assertEqual(scenarios["batch"]["status"], "passed")
        self.assertEqual(scenarios["patch"]["status"], "passed")
        self.assertEqual(scenarios["get"]["status"], "passed")
        self.assertEqual(scenarios["delete"]["status"], "passed")
        self.assertEqual(scenarios["idempotency"]["status"], "passed")
        self.assertEqual(scenarios["snapshot_restore"]["status"], "passed")
        self.assertEqual(scenarios["put"]["status"], "passed")
        self.assertEqual(scenarios["errors"]["status"], "passed")
        self.assertEqual(scenarios["traceql_string_execution"]["status"], "passed")

    def test_python_sdk_smoke_summary_maps_to_contract_scenarios(self) -> None:
        module = load_module()
        manifest = module.load_contract(ROOT / "docs" / "platform-contract-v0.json")
        smoke_summary = {
            "ok": True,
            "mode": "python-sdk-http-smoke",
            "sdk_surface": "python_sync",
            "steps": {
                "schema_apply": True,
                "put": True,
                "batch_ingest": True,
                "patch": True,
                "get": True,
                "scan": True,
                "query": True,
                "traceql_string_execution": True,
                "explain": True,
                "delete": True,
                "idempotency": True,
                "error_envelope": True,
                "snapshot": True,
                "restore": True,
            },
            "records_put": 1,
            "records_inserted": 3,
            "records_scanned": 3,
            "traceql_result_count": 1,
            "traceql_explain": True,
            "patched_status": "reviewed",
            "deleted_hidden": True,
            "idempotency_replay_epoch": 2,
            "idempotency_conflict_status": 409,
            "error_envelope": {
                "status": 400,
                "error": "missing field `table`",
                "code": "bad_request",
            },
        }

        surface = module.map_python_sdk_smoke_summary(manifest, smoke_summary)
        scenarios = {scenario["id"]: scenario for scenario in surface["scenarios"]}

        self.assertEqual(surface["surface"], "python_sdk")
        self.assertIn("installed package", " ".join(surface["evidence"]))
        self.assertEqual(scenarios["schema_apply"]["status"], "passed")
        self.assertEqual(scenarios["put"]["status"], "passed")
        self.assertEqual(scenarios["batch"]["status"], "passed")
        self.assertEqual(scenarios["patch"]["status"], "passed")
        self.assertEqual(scenarios["get"]["status"], "passed")
        self.assertEqual(scenarios["scan"]["status"], "passed")
        self.assertEqual(scenarios["query"]["status"], "passed")
        self.assertEqual(scenarios["traceql_string_execution"]["status"], "passed")
        self.assertEqual(scenarios["explain"]["status"], "passed")
        self.assertEqual(scenarios["delete"]["status"], "passed")
        self.assertEqual(scenarios["idempotency"]["status"], "passed")
        self.assertEqual(scenarios["errors"]["status"], "passed")
        self.assertEqual(scenarios["snapshot_restore"]["status"], "passed")

    def test_typescript_sdk_smoke_summary_maps_to_contract_scenarios(self) -> None:
        module = load_module()
        manifest = module.load_contract(ROOT / "docs" / "platform-contract-v0.json")
        smoke_summary = {
            "ok": True,
            "mode": "local-http-typescript-public-sdk-smoke",
            "sdk_surface": "public",
            "steps": {
                "schema_apply": True,
                "put": True,
                "batch_ingest": True,
                "patch": True,
                "get": True,
                "scan": True,
                "query": True,
                "traceql_string_execution": True,
                "explain": True,
                "delete": True,
                "idempotency": True,
                "error_envelope": True,
                "snapshot": True,
                "restore": True,
            },
            "records_put": 1,
            "records_inserted": 3,
            "records_scanned": 3,
            "traceql_result_count": 1,
            "traceql_explain": True,
            "patched_status": "reviewed",
            "deleted_hidden": True,
            "idempotency_replay_observed": True,
            "idempotency_conflict_status": 409,
            "error_envelope": {
                "status": 400,
                "error": "missing field `table`",
                "code": "bad_request",
                "method": "POST",
                "path": "/v1/records/get",
            },
        }

        surface = module.map_typescript_sdk_smoke_summary(manifest, smoke_summary)
        scenarios = {scenario["id"]: scenario for scenario in surface["scenarios"]}

        self.assertEqual(surface["surface"], "typescript_sdk")
        self.assertEqual(scenarios["schema_apply"]["status"], "passed")
        self.assertEqual(scenarios["put"]["status"], "passed")
        self.assertEqual(scenarios["batch"]["status"], "passed")
        self.assertEqual(scenarios["patch"]["status"], "passed")
        self.assertEqual(scenarios["get"]["status"], "passed")
        self.assertEqual(scenarios["scan"]["status"], "passed")
        self.assertEqual(scenarios["query"]["status"], "passed")
        self.assertEqual(scenarios["traceql_string_execution"]["status"], "passed")
        self.assertEqual(scenarios["explain"]["status"], "passed")
        self.assertEqual(scenarios["delete"]["status"], "passed")
        self.assertEqual(scenarios["idempotency"]["status"], "passed")
        self.assertEqual(scenarios["errors"]["status"], "passed")
        self.assertEqual(scenarios["snapshot_restore"]["status"], "passed")

    def test_traceql_sqlish_summary_maps_adapter_evidence_without_sql_compatibility(self) -> None:
        module = load_module()
        manifest = module.load_contract(ROOT / "docs" / "platform-contract-v0.json")
        smoke_summary = {
            "ok": True,
            "mode": "traceql-sqlish-conformance",
            "surface": "traceql_sqlish",
            "steps": {
                "sqlish_select": True,
                "sqlish_explain": True,
                "invalid_sqlish": True,
            },
            "sql_compatibility": "not_implemented",
            "postgres_compatibility": "not_compatible",
            "sqlish_result_ids": ["intro"],
            "sqlish_explain": True,
            "invalid_sqlish_status": 400,
            "invalid_sqlish_code": "bad_request",
            "invalid_sqlish_error": "invalid SQL-ish: JOIN is not supported",
        }

        surface = module.map_traceql_sqlish_summary(manifest, smoke_summary)
        scenarios = {scenario["id"]: scenario for scenario in surface["scenarios"]}

        self.assertEqual(surface["surface"], "traceql_sqlish")
        self.assertEqual(surface["status"], "checked")
        self.assertFalse(surface["complete"])
        self.assertEqual(scenarios["query"]["status"], "passed")
        self.assertEqual(scenarios["traceql_string_execution"]["status"], "passed")
        self.assertEqual(scenarios["explain"]["status"], "passed")
        self.assertEqual(scenarios["errors"]["status"], "passed")
        self.assertEqual(scenarios["schema_apply"]["status"], "not_checked")
        self.assertEqual(scenarios["snapshot_restore"]["status"], "not_checked")

    def test_http_direct_runner_executes_traceql_string_scenario(self) -> None:
        source = SCRIPT.read_text()

        for token in [
            "traceql_string_execution_scenario",
            "POST /v1/traceql",
            "traceql_string_execution",
            "invalid TraceQL",
        ]:
            self.assertIn(token, source)

    def test_traceql_sqlish_surface_is_executable_in_runner(self) -> None:
        source = SCRIPT.read_text()

        for token in [
            "run_traceql_sqlish_surface",
            "map_traceql_sqlish_summary",
            "traceql_sqlish_conformance_summary",
            "invalid SQL-ish",
        ]:
            self.assertIn(token, source)

    def test_graphql_surface_reports_compiler_only_not_checked_lane(self) -> None:
        module = load_module()
        manifest = module.load_contract(ROOT / "docs" / "platform-contract-v0.json")
        compiler_summary = {
            "ok": True,
            "mode": "graphql-compiler-conformance",
            "surface": "graphql",
            "compiler": "graphql_query_from_str",
            "status": "compiler_checked",
        }

        surface = module.map_graphql_compiler_summary(manifest, compiler_summary)
        scenarios = {scenario["id"]: scenario for scenario in surface["scenarios"]}

        self.assertEqual(surface["surface"], "graphql")
        self.assertEqual(surface["status"], "compiler_checked")
        self.assertTrue(surface["ok"])
        self.assertFalse(surface["complete"])
        self.assertIn("graphql_query_from_str", " ".join(surface["evidence"]))
        for scenario in module.contract_scenario_ids(manifest):
            self.assertEqual(scenarios[scenario]["status"], "not_checked")
            self.assertIn("compiler primitive", scenarios[scenario]["reason"])

    def test_graphql_surface_is_explicit_in_runner_without_endpoint_claims(self) -> None:
        source = SCRIPT.read_text()

        for token in [
            "run_graphql_surface",
            "map_graphql_compiler_summary",
            "graphql-compiler-conformance",
            "GraphQL compiler primitive",
            "not GraphQL HTTP support",
        ]:
            self.assertIn(token, source)

    def test_writes_report_summary_for_selected_surfaces(self) -> None:
        module = load_module()
        manifest = module.load_contract(ROOT / "docs" / "platform-contract-v0.json")
        surfaces = [
            module.empty_surface_report(manifest, "http_direct", "not_run", "unit test"),
            module.empty_surface_report(manifest, "rust_sdk", "not_run", "unit test"),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "conformance.json"
            report = module.build_report(manifest, surfaces)
            module.write_summary(report, report_path)
            round_trip = json.loads(report_path.read_text())

        self.assertEqual(round_trip["mode"], "platform-conformance")
        self.assertEqual(round_trip["contract"], "tracedb-platform-contract-v0")
        self.assertEqual([surface["surface"] for surface in round_trip["surfaces"]], ["http_direct", "rust_sdk"])
        self.assertEqual(round_trip["ok"], False)


if __name__ == "__main__":
    unittest.main()
