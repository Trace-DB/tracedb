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
                "explain",
                "delete",
                "idempotency",
                "errors",
                "snapshot_restore",
            ],
        )
        self.assertIn("http_direct", module.contract_surface_ids(manifest))
        self.assertIn("rust_sdk", module.contract_surface_ids(manifest))

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
                            "batch_ingest": True,
                            "patch": True,
                            "scan": True,
                            "query": True,
                            "explain": True,
                            "delete": True,
                            "snapshot": True,
                            "restore": True,
                        },
                        "patched_status": "reviewed",
                        "deleted_hidden": True,
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
        self.assertEqual(scenarios["put"]["status"], "not_checked")
        self.assertEqual(scenarios["errors"]["status"], "not_checked")

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
