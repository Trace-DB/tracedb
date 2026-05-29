from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import sys


LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

from runner.suite_spec import build_suite_gate, load_suite_spec, write_suite_gate_json


def minimal_report(*, failure_count: int = 0, control_status: str = "internal_only_smoke") -> dict:
    return {
        "suite_id": "suite-gate-test",
        "control_status": control_status,
        "summary": {
            "failure_count": failure_count,
            "control_status": control_status,
        },
        "number_to_beat": {
            "query_p95_ms": {"baseline": None, "value": None},
            "storage_bytes": {"baseline": None, "value": None},
        },
        "control_ledger": {
            "available_external_controls": (
                [{"name": "PostgreSQL"}] if control_status == "external_control_available" else []
            ),
            "unavailable_external_controls": [],
        },
    }


def report_with_tracedb_metrics(
    *,
    query_latency_p95_ms: float,
    storage_bytes: float = 1000.0,
    control_status: str = "internal_only_smoke",
) -> dict:
    report = minimal_report(control_status=control_status)
    report["scenarios"] = [
        {
            "id": "search_rag_6",
            "baselines": [
                {
                    "name": "TraceDB",
                    "available": True,
                    "metrics": {
                        "query_latency_p95_ms": query_latency_p95_ms,
                        "storage_bytes": storage_bytes,
                        "failure_count": 0,
                    },
                    "notes": [],
                }
            ],
        }
    ]
    return report


class SuiteGateTests(unittest.TestCase):
    def test_platform_pr_suite_spec_defines_contract_and_unsupported_coverage(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "platform_pr.json")

        self.assertEqual(spec.id, "platform_pr")
        self.assertEqual(spec.default_records, 128)
        self.assertEqual(spec.record_counts, [128, 1000])
        self.assertIn("sdk_cli_surface", spec.scenarios)
        self.assertIn("http_falsification", spec.scenarios)
        self.assertIn("python_sdk", spec.surfaces)
        self.assertFalse(spec.requires_external_controls)
        self.assertFalse(spec.railway_required)
        self.assertEqual(spec.unsupported_coverage["sql_compatibility"], "unsupported")
        self.assertEqual(spec.unsupported_coverage["graphql_mutations"], "unsupported")

    def test_gate_marks_trace_only_clean_pr_suite_usable_without_claim_ready(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "platform_pr.json")
        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
        )

        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["blocking_failures"], [])
        self.assertEqual(gate["warnings"], [])
        self.assertEqual(gate["claim_status"]["performance_claim"], "development_only")
        self.assertEqual(gate["artifact_paths"]["suite_gate_json"], "suite-gate.json")
        self.assertIn("sql_compatibility", gate["claim_status"]["unsupported_coverage"])

    def test_gate_blocks_on_suite_failures(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "platform_pr.json")
        gate = build_suite_gate(
            minimal_report(failure_count=2),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertTrue(any("failure_count=2" in item for item in gate["blocking_failures"]))

    def test_gate_blocks_rolling_regression_when_policy_is_blocking(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "platform_push_10k.json")

        gate = build_suite_gate(
            report_with_tracedb_metrics(query_latency_p95_ms=12.5, storage_bytes=1000.0),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
            regression_baseline=report_with_tracedb_metrics(
                query_latency_p95_ms=10.0,
                storage_bytes=1000.0,
            ),
            regression_tolerance_pct=10.0,
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(len(gate["regressions"]), 1)
        self.assertEqual(gate["regressions"][0]["metric"], "query_latency_p95_ms")
        self.assertEqual(gate["regressions"][0]["previous"], 10.0)
        self.assertEqual(gate["regressions"][0]["current"], 12.5)
        self.assertTrue(gate["regressions"][0]["blocking"])
        self.assertTrue(
            any("performance regression" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_gate_warns_regression_when_pr_policy_is_warning_until_baseline(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "platform_pr.json")

        gate = build_suite_gate(
            report_with_tracedb_metrics(query_latency_p95_ms=12.5),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
            regression_baseline=report_with_tracedb_metrics(query_latency_p95_ms=10.0),
            regression_tolerance_pct=10.0,
        )

        self.assertEqual(gate["status"], "degraded")
        self.assertEqual(gate["blocking_failures"], [])
        self.assertEqual(len(gate["regressions"]), 1)
        self.assertFalse(gate["regressions"][0]["blocking"])

    def test_release_gate_requires_external_controls_before_claim_ready(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "release_100k.json")

        self.assertEqual(
            spec.surfaces,
            ["http_direct", "rust_sdk", "typescript_sdk", "python_sdk", "traceql", "graphql"],
        )
        self.assertEqual(spec.unsupported_coverage["graphql_mutations"], "unsupported")

        blocked = build_suite_gate(
            minimal_report(control_status="internal_only_smoke"),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
            railway_manifest={"status": "configured", "services": []},
        )
        self.assertEqual(blocked["status"], "blocked")
        self.assertTrue(
            any("external control" in item for item in blocked["blocking_failures"]),
            blocked["blocking_failures"],
        )

        missing_backup = build_suite_gate(
            minimal_report(control_status="external_control_available"),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
            railway_manifest={"status": "configured", "services": []},
        )
        self.assertEqual(missing_backup["status"], "blocked")
        self.assertEqual(missing_backup["claim_status"]["railway_backup"], "not_checked")
        self.assertTrue(
            any("backup" in item for item in missing_backup["blocking_failures"]),
            missing_backup["blocking_failures"],
        )

        claim_ready = build_suite_gate(
            minimal_report(control_status="external_control_available"),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "backup_verdict": {"status": "passed"},
            },
        )
        self.assertEqual(claim_ready["status"], "claim-ready")
        self.assertEqual(claim_ready["claim_status"]["performance_claim"], "claim_ready")
        self.assertEqual(claim_ready["claim_status"]["railway_backup"], "passed")

    def test_production_1m_suite_spec_defines_release_gate_contract(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "production_1m.json")

        self.assertEqual(spec.id, "production_1m")
        self.assertEqual(spec.default_records, 1_000_000)
        self.assertEqual(spec.record_counts, [1_000_000])
        self.assertEqual(spec.dataset, "generated_hybrid")
        self.assertEqual(
            spec.surfaces,
            ["http_direct", "rust_sdk", "typescript_sdk", "python_sdk", "traceql", "graphql"],
        )
        self.assertEqual(spec.controls, ["tracedb", "pgvector", "qdrant", "opensearch"])
        self.assertTrue(spec.requires_external_controls)
        self.assertTrue(spec.railway_required)
        self.assertTrue(spec.railway["volume_required"])
        self.assertTrue(spec.railway["backup_required"])
        self.assertTrue(spec.railway["restart_required"])
        self.assertTrue(spec.railway["redeploy_required"])
        self.assertTrue(spec.railway["runbook_verification_required"])
        for rule in [
            "correctness",
            "sdk_parity",
            "supported_api_parity",
            "tenant_isolation",
            "tombstones",
            "error_envelopes",
            "snapshot_restore",
            "restart_redeploy",
            "hard_failures",
            "external_controls",
            "rolling_regression_blocking",
        ]:
            self.assertTrue(spec.blocking_rules[rule], rule)
        self.assertEqual(spec.unsupported_coverage["sql_compatibility"], "unsupported")
        self.assertEqual(spec.unsupported_coverage["graphql_subscriptions"], "unsupported")
        self.assertEqual(spec.unsupported_coverage["sql_postgres_wire_protocol"], "unsupported")
        self.assertNotIn("graphql_mutations", spec.unsupported_coverage)

    def test_production_1m_gate_blocks_until_required_railway_evidence_is_complete(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "production_1m.json")

        missing_evidence = build_suite_gate(
            minimal_report(control_status="external_control_available"),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
            },
            railway_runbook_verification_required=spec.railway["runbook_verification_required"],
        )

        self.assertEqual(missing_evidence["status"], "blocked")
        self.assertEqual(missing_evidence["claim_status"]["railway_backup"], "not_checked")
        self.assertEqual(
            missing_evidence["claim_status"]["railway_restart_redeploy"],
            "not_checked",
        )
        self.assertEqual(
            missing_evidence["claim_status"]["railway_runbook_verification"],
            "not_checked",
        )
        self.assertTrue(
            any("backup" in item for item in missing_evidence["blocking_failures"]),
            missing_evidence["blocking_failures"],
        )
        self.assertTrue(
            any("restart/redeploy" in item for item in missing_evidence["blocking_failures"]),
            missing_evidence["blocking_failures"],
        )
        self.assertTrue(
            any("runbook verification" in item for item in missing_evidence["blocking_failures"]),
            missing_evidence["blocking_failures"],
        )

    def test_railway_stateful_gate_requires_configured_manifest(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        blocked = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
        )
        self.assertEqual(blocked["status"], "blocked")
        self.assertTrue(
            any("Railway" in item for item in blocked["blocking_failures"]),
            blocked["blocking_failures"],
        )

        configured = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [
                    {
                        "role": "tracedb",
                        "service_id": "service_tracedb",
                        "configured": True,
                    }
                ],
            },
        )
        self.assertEqual(configured["status"], "usable")
        self.assertEqual(configured["railway_services"][0]["role"], "tracedb")
        self.assertEqual(
            configured["artifact_paths"]["railway_manifest_json"],
            "railway-manifest.json",
        )

    def test_railway_endpoint_health_failure_blocks_when_manifest_includes_probe(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [
                    {
                        "role": "tracedb",
                        "service_id": "service_tracedb",
                        "configured": True,
                    }
                ],
                "endpoint_health": {
                    "status": "unhealthy",
                    "base_url": "http://127.0.0.1:65535",
                    "checks": [{"name": "ready", "ok": False, "status_code": 503}],
                },
            },
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["claim_status"]["railway_endpoint_health"], "unhealthy")
        self.assertTrue(
            any("endpoint health" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_railway_endpoint_health_is_recorded_when_probe_is_healthy(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [
                    {
                        "role": "tracedb",
                        "service_id": "service_tracedb",
                        "configured": True,
                    }
                ],
                "endpoint_health": {
                    "status": "healthy",
                    "base_url": "http://tracedb.railway.internal:8080",
                    "checks": [{"name": "ready", "ok": True, "status_code": 200}],
                },
            },
        )

        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["claim_status"]["railway_endpoint_health"], "healthy")

    def test_railway_stateful_smoke_failure_blocks_when_manifest_includes_probe(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [
                    {
                        "role": "tracedb",
                        "service_id": "service_tracedb",
                        "configured": True,
                    }
                ],
                "stateful_smoke": {
                    "status": "failed",
                    "marker": {"table": "railway_stateful_markers", "id": "marker-123"},
                    "errors": ["marker write was not visible"],
                },
            },
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["claim_status"]["railway_stateful_smoke"], "failed")
        self.assertTrue(
            any("stateful smoke" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_railway_stateful_smoke_is_recorded_when_probe_passes(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [
                    {
                        "role": "tracedb",
                        "service_id": "service_tracedb",
                        "configured": True,
                    }
                ],
                "stateful_smoke": {
                    "status": "passed",
                    "marker": {"table": "railway_stateful_markers", "id": "marker-123"},
                    "errors": [],
                },
            },
        )

        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["claim_status"]["railway_stateful_smoke"], "passed")

    def test_railway_operation_plan_is_recorded_without_claiming_restart_proof(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [
                    {
                        "role": "tracedb",
                        "service_id": "service_tracedb",
                        "configured": True,
                    }
                ],
                "operation_plan": {
                    "status": "plan_only",
                    "execution": {"executed": False},
                    "claim_boundary": "plan_only_not_executed_no_restart_redeploy_or_persistence_proof",
                },
            },
        )

        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["claim_status"]["railway_restart_redeploy"], "plan_only")

    def test_railway_operation_execution_failure_blocks_when_manifest_includes_it(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [
                    {
                        "role": "tracedb",
                        "service_id": "service_tracedb",
                        "configured": True,
                    }
                ],
                "operation_plan": {
                    "status": "failed",
                    "execution": {"executed": True},
                    "errors": ["restart command failed"],
                },
            },
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["claim_status"]["railway_restart_redeploy"], "failed")
        self.assertTrue(
            any("restart/redeploy" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_railway_persistence_verdict_pass_records_claim_status(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "persistence_verdict": {
                    "status": "passed",
                    "marker": {"id": "marker-123"},
                    "operation": {"operation": "restart", "executed": True},
                },
            },
        )

        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["claim_status"]["railway_persistence"], "passed")

    def test_railway_snapshot_restore_pass_records_claim_status(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "snapshot_restore": {
                    "status": "passed",
                    "restored_read": {"status": "passed", "record_visible": True},
                    "paths": {
                        "snapshot": "/srv/tracedb-admin/run/marker/snapshot",
                        "restore": "/srv/tracedb-admin/run/marker/restore",
                    },
                },
            },
        )

        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["claim_status"]["railway_snapshot_restore"], "passed")
        self.assertEqual(gate["claim_status"]["railway_restored_read"], "passed")

    def test_railway_restored_read_failure_blocks(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "snapshot_restore": {
                    "status": "passed",
                    "restored_read": {
                        "status": "failed",
                        "record_visible": False,
                        "errors": ["restored marker was not visible"],
                    },
                },
            },
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["claim_status"]["railway_snapshot_restore"], "passed")
        self.assertEqual(gate["claim_status"]["railway_restored_read"], "failed")
        self.assertTrue(
            any("restored read" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_railway_snapshot_restore_failure_blocks(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "snapshot_restore": {
                    "status": "failed",
                    "errors": ["snapshot route failed"],
                },
            },
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["claim_status"]["railway_snapshot_restore"], "failed")
        self.assertTrue(
            any("snapshot/restore" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_railway_persistence_verdict_failure_blocks(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "railway_stateful.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "persistence_verdict": {
                    "status": "failed",
                    "errors": ["marker mismatch"],
                },
            },
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["claim_status"]["railway_persistence"], "failed")
        self.assertTrue(
            any("persistence" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_railway_backup_verdict_failure_blocks_when_required(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "soak_railway.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "backup_verdict": {
                    "status": "failed",
                    "errors": ["backup restore validation is missing"],
                },
            },
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["claim_status"]["railway_backup"], "failed")
        self.assertTrue(
            any("backup" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_railway_runbook_verification_blocks_when_required_and_missing(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "soak_railway.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "backup_verdict": {"status": "passed"},
            },
            railway_runbook_verification_required=True,
        )

        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["claim_status"]["railway_runbook_verification"], "not_checked")
        self.assertTrue(
            any("runbook verification" in item for item in gate["blocking_failures"]),
            gate["blocking_failures"],
        )

    def test_railway_runbook_verification_complete_records_claim_status(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "soak_railway.json")

        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={
                "suite_json": "suite.json",
                "suite_md": "suite.md",
                "railway_manifest_json": "railway-manifest.json",
                "railway_runbook_verification_json": "railway-runbook-verification.json",
            },
            railway_manifest={
                "status": "configured",
                "services": [{"role": "tracedb", "service_id": "service_tracedb"}],
                "backup_verdict": {"status": "passed"},
            },
            railway_runbook_verification={"status": "complete", "complete_steps": ["preflight_gate"]},
            railway_runbook_verification_required=True,
        )

        self.assertEqual(gate["status"], "usable")
        self.assertEqual(gate["claim_status"]["railway_runbook_verification"], "complete")

    def test_gate_json_writer_persists_stable_artifact(self) -> None:
        spec = load_suite_spec(LAB_ROOT / "suites" / "platform_pr.json")
        gate = build_suite_gate(
            minimal_report(),
            spec,
            artifact_paths={"suite_json": "suite.json", "suite_md": "suite.md"},
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "suite-gate.json"
            write_suite_gate_json(gate, path)
            payload = json.loads(path.read_text())

        self.assertEqual(payload["suite_spec"], "platform_pr")
        self.assertEqual(payload["status"], "usable")
        self.assertIn("number_to_beat", payload)


if __name__ == "__main__":
    unittest.main()
