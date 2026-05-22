from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SuiteSpec:
    id: str
    label: str
    description: str
    default_records: int
    record_counts: list[int]
    dataset: str
    scenarios: list[str]
    surfaces: list[str]
    adapters: list[str]
    controls: list[str]
    blocking_rules: dict[str, Any]
    railway: dict[str, Any]
    unsupported_coverage: dict[str, str]

    @property
    def requires_external_controls(self) -> bool:
        return bool(self.blocking_rules.get("require_external_controls", False))

    @property
    def railway_required(self) -> bool:
        return bool(self.railway.get("required", False))


def load_suite_spec(path: Path) -> SuiteSpec:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return suite_spec_from_mapping(payload, source=str(path))


def default_suite_spec(
    *, scenarios: list[str], surfaces: list[str], controls: list[str], records: int
) -> SuiteSpec:
    return SuiteSpec(
        id="ad_hoc",
        label="Ad Hoc Suite",
        description="Runner-generated suite spec for a command that did not provide --suite-spec.",
        default_records=records,
        record_counts=[records],
        dataset="command",
        scenarios=scenarios,
        surfaces=surfaces,
        adapters=[],
        controls=controls,
        blocking_rules={
            "correctness": True,
            "hard_failures": True,
            "require_external_controls": False,
            "performance_regression_policy": "not_configured",
        },
        railway={"required": False, "services": []},
        unsupported_coverage={},
    )


def suite_spec_from_mapping(payload: dict[str, Any], *, source: str = "<memory>") -> SuiteSpec:
    required = [
        "id",
        "label",
        "description",
        "default_records",
        "record_counts",
        "dataset",
        "scenarios",
        "surfaces",
        "blocking_rules",
        "railway",
        "unsupported_coverage",
    ]
    missing = [key for key in required if key not in payload]
    if missing:
        raise ValueError(f"{source} missing required suite spec keys: {', '.join(missing)}")
    spec = SuiteSpec(
        id=_string(payload, "id", source),
        label=_string(payload, "label", source),
        description=_string(payload, "description", source),
        default_records=_positive_int(payload.get("default_records"), "default_records", source),
        record_counts=[
            _positive_int(value, "record_counts", source)
            for value in _list(payload, "record_counts", source)
        ],
        dataset=_string(payload, "dataset", source),
        scenarios=[_coerce_string(value, "scenarios", source) for value in _list(payload, "scenarios", source)],
        surfaces=[_coerce_string(value, "surfaces", source) for value in _list(payload, "surfaces", source)],
        adapters=[
            _coerce_string(value, "adapters", source)
            for value in payload.get("adapters", [])
        ],
        controls=[
            _coerce_string(value, "controls", source)
            for value in payload.get("controls", [])
        ],
        blocking_rules=_dict(payload, "blocking_rules", source),
        railway=_dict(payload, "railway", source),
        unsupported_coverage={
            _coerce_string(key, "unsupported_coverage", source): _coerce_string(
                value, "unsupported_coverage", source
            )
            for key, value in _dict(payload, "unsupported_coverage", source).items()
        },
    )
    if not spec.record_counts:
        raise ValueError(f"{source} must define at least one record_count")
    if spec.default_records not in spec.record_counts:
        raise ValueError(f"{source} default_records must be present in record_counts")
    if not spec.scenarios:
        raise ValueError(f"{source} must define at least one scenario")
    if not spec.surfaces:
        raise ValueError(f"{source} must define at least one surface")
    return spec


def build_suite_gate(
    report: dict[str, Any],
    spec: SuiteSpec,
    *,
    artifact_paths: dict[str, str],
    railway_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    blocking_failures: list[str] = []
    warnings: list[str] = []
    regressions: list[dict[str, Any]] = []

    failure_count = int(report.get("summary", {}).get("failure_count", 0) or 0)
    if failure_count:
        blocking_failures.append(f"suite summary reported failure_count={failure_count}")

    control_status = report.get("control_status") or report.get("summary", {}).get(
        "control_status", "unknown"
    )
    external_control_available = _external_control_available(report)
    if spec.requires_external_controls and not external_control_available:
        blocking_failures.append(
            "suite requires an external control and number_to_beat before claim-ready status"
        )

    railway_services = _railway_services(spec, railway_manifest)
    if spec.railway_required:
        railway_status = (railway_manifest or {}).get("status")
        if railway_status != "configured":
            blocking_failures.append("suite requires configured Railway services")
    railway_endpoint_health = _railway_endpoint_health_status(railway_manifest)
    if railway_endpoint_health not in {"not_checked", "healthy"}:
        blocking_failures.append(
            f"Railway endpoint health check failed with status={railway_endpoint_health}"
        )

    if control_status == "external_control_unavailable" and not spec.requires_external_controls:
        warnings.append("external controls were requested but unavailable")

    artifact_paths = dict(artifact_paths)
    artifact_paths.setdefault("suite_gate_json", "suite-gate.json")

    if blocking_failures:
        status = "blocked"
        performance_claim = "blocked"
    elif spec.requires_external_controls and external_control_available:
        status = "claim-ready"
        performance_claim = "claim_ready"
    elif warnings or regressions:
        status = "degraded"
        performance_claim = "development_only"
    else:
        status = "usable"
        performance_claim = "development_only"

    return {
        "suite_id": report.get("suite_id", "unknown"),
        "suite_spec": spec.id,
        "status": status,
        "blocking_failures": blocking_failures,
        "warnings": warnings,
        "regressions": regressions,
        "number_to_beat": report.get("number_to_beat", {}),
        "railway_services": railway_services,
        "claim_status": {
            "performance_claim": performance_claim,
            "control_status": control_status,
            "external_control_required": spec.requires_external_controls,
            "external_control_available": external_control_available,
            "railway_endpoint_health": railway_endpoint_health,
            "unsupported_coverage": spec.unsupported_coverage,
        },
        "artifact_paths": artifact_paths,
    }


def write_suite_gate_json(gate: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(gate, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _external_control_available(report: dict[str, Any]) -> bool:
    if report.get("control_status") == "external_control_available":
        return True
    ledger = report.get("control_ledger", {})
    return bool(ledger.get("available_external_controls"))


def _railway_services(
    spec: SuiteSpec, railway_manifest: dict[str, Any] | None
) -> list[dict[str, Any]]:
    if railway_manifest and isinstance(railway_manifest.get("services"), list):
        return railway_manifest["services"]
    services = spec.railway.get("services", [])
    if not isinstance(services, list):
        return []
    normalized = []
    for service in services:
        if isinstance(service, dict):
            normalized.append(service)
        else:
            normalized.append({"role": str(service), "configured": False})
    return normalized


def _railway_endpoint_health_status(railway_manifest: dict[str, Any] | None) -> str:
    if not railway_manifest:
        return "not_checked"
    endpoint_health = railway_manifest.get("endpoint_health")
    if not isinstance(endpoint_health, dict):
        return "not_checked"
    status = endpoint_health.get("status")
    return str(status) if status else "unknown"


def _dict(payload: dict[str, Any], key: str, source: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{source} key {key} must be an object")
    return dict(value)


def _list(payload: dict[str, Any], key: str, source: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"{source} key {key} must be a list")
    return list(value)


def _string(payload: dict[str, Any], key: str, source: str) -> str:
    return _coerce_string(payload.get(key), key, source)


def _coerce_string(value: Any, key: str, source: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{source} key {key} must contain non-empty strings")
    return value.strip()


def _positive_int(value: Any, key: str, source: str) -> int:
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{source} key {key} must contain positive integers")
    return value
