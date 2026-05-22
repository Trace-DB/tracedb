from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urlerror
from urllib import parse, request
from typing import Mapping, Any


SENSITIVE_KEY_PARTS = ("TOKEN", "SECRET", "PASSWORD", "PRIVATE_KEY")

CONTROL_SERVICE_ENV = {
    "postgres": "POSTGRES_RAILWAY_SERVICE_ID",
    "pgvector": "PGVECTOR_RAILWAY_SERVICE_ID",
    "mongodb": "MONGODB_RAILWAY_SERVICE_ID",
    "qdrant": "QDRANT_RAILWAY_SERVICE_ID",
    "opensearch": "OPENSEARCH_RAILWAY_SERVICE_ID",
}
RAILWAY_MUTATING_OPERATIONS = {"restart", "redeploy"}
RAILWAY_OPERATION_SUCCESS_STATUSES = {"passed", "completed", "succeeded", "success", "ok"}
RAILWAY_OPERATION_FAILURE_STATUSES = {
    "blocked",
    "cancelled",
    "canceled",
    "error",
    "failed",
    "failure",
    "timeout",
    "timed_out",
}


def load_railway_config(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    source = os.environ if env is None else env
    token = source.get("RAILWAY_API_TOKEN") or source.get("RAILWAY_TOKEN") or ""
    return {
        "token_configured": bool(token),
        "project_id": source.get("RAILWAY_PROJECT_ID", ""),
        "environment_id": source.get("RAILWAY_ENVIRONMENT_ID", ""),
        "tracedb_service_id": source.get("TRACEDB_RAILWAY_SERVICE_ID", ""),
        "tracedb_private_url": source.get("TRACEDB_RAILWAY_PRIVATE_URL", ""),
        "tracedb_public_url": source.get("TRACEDB_RAILWAY_URL", "")
        or source.get("RAILWAY_TRACEDB_URL", "")
        or source.get("TRACEDB_HTTP_URL", ""),
        "tracedb_volume_mount_path": source.get("TRACEDB_RAILWAY_VOLUME_PATH", ""),
        "tracedb_snapshot_root": source.get("TRACEDB_RAILWAY_SNAPSHOT_ROOT", "")
        or source.get("TRACEDB_REMOTE_SNAPSHOT_ROOT", ""),
        "control_service_ids": {
            role: source.get(env_key, "")
            for role, env_key in CONTROL_SERVICE_ENV.items()
            if source.get(env_key)
        },
        "redacted_env": redact_env(source),
    }


def validate_railway_config(config: Mapping[str, Any]) -> dict[str, Any]:
    required = [
        ("token_configured", "RAILWAY_API_TOKEN or RAILWAY_TOKEN"),
        ("project_id", "RAILWAY_PROJECT_ID"),
        ("environment_id", "RAILWAY_ENVIRONMENT_ID"),
        ("tracedb_service_id", "TRACEDB_RAILWAY_SERVICE_ID"),
        ("tracedb_private_url", "TRACEDB_RAILWAY_PRIVATE_URL"),
        ("tracedb_volume_mount_path", "TRACEDB_RAILWAY_VOLUME_PATH"),
    ]
    missing = [label for key, label in required if not config.get(key)]
    warnings = []
    private_url = str(config.get("tracedb_private_url", ""))
    if private_url and "railway.internal" not in private_url:
        warnings.append("TRACEDB_RAILWAY_PRIVATE_URL does not look like a private Railway URL")
    volume = str(config.get("tracedb_volume_mount_path", ""))
    if volume and not volume.startswith("/"):
        warnings.append("TRACEDB_RAILWAY_VOLUME_PATH should be an absolute mount path")
    snapshot_root = str(config.get("tracedb_snapshot_root", ""))
    if snapshot_root and not snapshot_root.startswith("/"):
        warnings.append("TRACEDB_RAILWAY_SNAPSHOT_ROOT should be an absolute server-side path")
    return {
        "ok": not missing,
        "missing": missing,
        "warnings": warnings,
    }


def build_railway_manifest(
    config: Mapping[str, Any],
    *,
    suite_id: str,
    endpoint_health: Mapping[str, Any] | None = None,
    stateful_smoke: Mapping[str, Any] | None = None,
    snapshot_restore: Mapping[str, Any] | None = None,
    operation_plan: Mapping[str, Any] | None = None,
    persistence_verdict: Mapping[str, Any] | None = None,
    backup_verdict: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    validation = validate_railway_config(config)
    services = []
    if config.get("tracedb_service_id"):
        services.append(
            {
                "role": "tracedb",
                "service_id": config.get("tracedb_service_id"),
                "private_url": config.get("tracedb_private_url"),
                "public_url": config.get("tracedb_public_url"),
                "volume_mount_path": config.get("tracedb_volume_mount_path"),
                "configured": validation["ok"],
            }
        )
    for role, service_id in sorted(dict(config.get("control_service_ids", {})).items()):
        services.append(
            {
                "role": role,
                "service_id": service_id,
                "configured": True,
            }
        )
    manifest = {
        "kind": "railway_benchmark_manifest",
        "suite_id": suite_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "configured" if validation["ok"] else "missing_config",
        "missing": validation["missing"],
        "warnings": validation["warnings"],
        "project_id": config.get("project_id", ""),
        "environment_id": config.get("environment_id", ""),
        "token_configured": bool(config.get("token_configured")),
        "services": services,
        "ssh_hints": _ssh_hints(services),
        "redacted_env": config.get("redacted_env", {}),
    }
    if endpoint_health is not None:
        manifest["endpoint_health"] = dict(endpoint_health)
    if stateful_smoke is not None:
        manifest["stateful_smoke"] = dict(stateful_smoke)
    if snapshot_restore is not None:
        manifest["snapshot_restore"] = dict(snapshot_restore)
    if operation_plan is not None:
        manifest["operation_plan"] = dict(operation_plan)
    if persistence_verdict is not None:
        manifest["persistence_verdict"] = dict(persistence_verdict)
    if backup_verdict is not None:
        manifest["backup_verdict"] = dict(backup_verdict)
    return manifest


def build_railway_artifact_manifest(
    suite_dir: Path,
    *,
    suite_id: str,
    artifact_paths: Mapping[str, str],
    railway_manifest: Mapping[str, Any] | None = None,
    suite_gate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    railway_manifest = _dict_value(railway_manifest)
    suite_gate = _dict_value(suite_gate)
    claim_status = _dict_value(suite_gate.get("claim_status"))
    artifacts = [
        _artifact_entry(suite_dir, name, path)
        for name, path in sorted(dict(artifact_paths).items())
        if name != "railway_artifacts_json"
    ]
    return {
        "kind": "railway_suite_artifact_manifest",
        "suite_id": suite_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "artifacts": artifacts,
        "railway_claim_status": {
            "gate_status": suite_gate.get("status", "unknown"),
            "manifest_status": railway_manifest.get("status", "not_checked"),
            "endpoint_health": claim_status.get("railway_endpoint_health", "not_checked"),
            "stateful_smoke": claim_status.get("railway_stateful_smoke", "not_checked"),
            "snapshot_restore": claim_status.get("railway_snapshot_restore", "not_checked"),
            "restored_read": claim_status.get("railway_restored_read", "not_checked"),
            "restart_redeploy": claim_status.get("railway_restart_redeploy", "not_checked"),
            "persistence": claim_status.get("railway_persistence", "not_checked"),
            "backup": claim_status.get("railway_backup", "not_checked"),
        },
        "open_proof_gaps": _artifact_proof_gaps(claim_status),
        "claim_boundary": "artifact_manifest_indexes_suite_outputs_not_backup_snapshot_restore_proof",
    }


def validate_railway_operation_receipt(
    operation_receipt: Mapping[str, Any],
    *,
    expected_service_id: str = "",
) -> dict[str, Any]:
    receipt = _dict_value(operation_receipt)
    redacted_receipt = _redact_sensitive(receipt)
    required = ["kind", "operation", "status", "executed", "confirmed", "service_id"]
    missing = []
    for key in required:
        value = receipt.get(key)
        if key not in receipt or value is None or value == "":
            missing.append(key)
    errors = []
    warnings = []

    kind = str(receipt.get("kind", ""))
    if kind and kind != "railway_operation_receipt":
        errors.append("kind must be railway_operation_receipt")

    operation = str(receipt.get("operation", "")).lower()
    if operation and operation not in RAILWAY_MUTATING_OPERATIONS:
        errors.append("operation must be restart or redeploy")

    status = str(receipt.get("status", "")).lower()
    if status and status not in RAILWAY_OPERATION_SUCCESS_STATUSES | RAILWAY_OPERATION_FAILURE_STATUSES:
        errors.append("status must be a known operation receipt status")

    if "executed" in receipt and receipt.get("executed") is not True:
        errors.append("executed must be true for persistence evidence")
    if "confirmed" in receipt and receipt.get("confirmed") is not True:
        errors.append("confirmed must be true for operator-approved persistence evidence")

    service_id = str(receipt.get("service_id", ""))
    if expected_service_id and service_id and service_id != expected_service_id:
        errors.append("service_id does not match the TraceDB Railway service")
    if not expected_service_id:
        errors.append("expected TraceDB service_id is required for receipt matching")

    ok = not missing and not errors
    return {
        "ok": ok,
        "status": "valid" if ok else "invalid",
        "missing": missing,
        "errors": errors,
        "warnings": warnings,
        "receipt": redacted_receipt,
    }


def build_railway_operation_receipt(
    config: Mapping[str, Any],
    *,
    suite_id: str,
    operation: str,
    status: str,
    executed: bool,
    confirmed: bool,
    command: str = "",
    operator: str = "",
    deployment_id: str = "",
    notes: list[str] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    validation = validate_railway_config(config)
    receipt = {
        "kind": "railway_operation_receipt",
        "suite_id": suite_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "operation": str(operation).lower(),
        "status": str(status).lower(),
        "executed": bool(executed),
        "confirmed": bool(confirmed),
        "project_id": config.get("project_id", ""),
        "environment_id": config.get("environment_id", ""),
        "service_id": config.get("tracedb_service_id", ""),
        "operator": operator,
        "command": command,
        "deployment_id": deployment_id,
        "notes": list(notes or []),
        "config_validation": {
            "ok": validation["ok"],
            "missing": validation["missing"],
            "warnings": validation["warnings"],
        },
        "claim_boundary": "operator_reported_receipt_only_not_railway_mutation_execution",
    }
    if extra:
        receipt["extra"] = _redact_sensitive(extra)
    return _redact_sensitive(receipt)


def validate_railway_backup_receipt(
    backup_receipt: Mapping[str, Any],
    *,
    expected_service_id: str = "",
) -> dict[str, Any]:
    receipt = _dict_value(backup_receipt)
    redacted_receipt = _redact_sensitive(receipt)
    required = [
        "kind",
        "status",
        "confirmed",
        "backup_created",
        "restore_validated",
        "service_id",
        "backup_id",
        "restore_validation_method",
    ]
    missing = []
    for key in required:
        value = receipt.get(key)
        if key not in receipt or value is None or value == "":
            missing.append(key)

    errors = []
    warnings = []
    kind = str(receipt.get("kind", ""))
    if kind and kind != "railway_backup_receipt":
        errors.append("kind must be railway_backup_receipt")

    status = str(receipt.get("status", "")).lower()
    if status and status not in RAILWAY_OPERATION_SUCCESS_STATUSES | RAILWAY_OPERATION_FAILURE_STATUSES:
        errors.append("status must be a known backup receipt status")

    if "confirmed" in receipt and receipt.get("confirmed") is not True:
        errors.append("confirmed must be true for operator-approved backup evidence")
    if "backup_created" in receipt and receipt.get("backup_created") is not True:
        errors.append("backup_created must be true for backup evidence")
    if "restore_validated" in receipt and receipt.get("restore_validated") is not True:
        errors.append("restore_validated must be true for backup/DR evidence")

    method = str(receipt.get("restore_validation_method", ""))
    if "restore_validated" in receipt and receipt.get("restore_validated") is True and not method:
        errors.append("restore_validation_method is required when restore_validated is true")

    service_id = str(receipt.get("service_id", ""))
    if expected_service_id and service_id and service_id != expected_service_id:
        errors.append("service_id does not match the TraceDB Railway service")
    if not expected_service_id:
        errors.append("expected TraceDB service_id is required for backup receipt matching")

    ok = not missing and not errors
    return {
        "ok": ok,
        "status": "valid" if ok else "invalid",
        "missing": missing,
        "errors": errors,
        "warnings": warnings,
        "receipt": redacted_receipt,
    }


def build_railway_backup_receipt(
    config: Mapping[str, Any],
    *,
    suite_id: str,
    status: str,
    backup_id: str,
    confirmed: bool,
    backup_created: bool,
    restore_validated: bool,
    restore_validation_method: str = "",
    operator: str = "",
    notes: list[str] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    validation = validate_railway_config(config)
    receipt = {
        "kind": "railway_backup_receipt",
        "suite_id": suite_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": str(status).lower(),
        "confirmed": bool(confirmed),
        "backup_created": bool(backup_created),
        "restore_validated": bool(restore_validated),
        "restore_validation_method": restore_validation_method,
        "project_id": config.get("project_id", ""),
        "environment_id": config.get("environment_id", ""),
        "service_id": config.get("tracedb_service_id", ""),
        "volume_mount_path": config.get("tracedb_volume_mount_path", ""),
        "backup_id": backup_id,
        "operator": operator,
        "notes": list(notes or []),
        "config_validation": {
            "ok": validation["ok"],
            "missing": validation["missing"],
            "warnings": validation["warnings"],
        },
        "claim_boundary": "operator_reported_backup_receipt_only_not_railway_backup_execution",
    }
    if extra:
        receipt["extra"] = _redact_sensitive(extra)
    return _redact_sensitive(receipt)


def build_railway_backup_verdict(
    railway_manifest: Mapping[str, Any],
    backup_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    expected_service_id = _tracedb_service_id(railway_manifest)
    receipt = _dict_value(backup_receipt)
    receipt_validation = validate_railway_backup_receipt(
        receipt,
        expected_service_id=expected_service_id,
    )
    redacted_receipt = receipt_validation["receipt"]
    checks = {
        "receipt_valid": receipt_validation["ok"],
        "backup_created": receipt.get("backup_created") is True,
        "backup_confirmed": receipt.get("confirmed") is True,
        "restore_validated": receipt.get("restore_validated") is True,
        "backup_succeeded": str(receipt.get("status", "")).lower()
        in RAILWAY_OPERATION_SUCCESS_STATUSES,
    }
    errors = []
    if not checks["receipt_valid"]:
        errors.extend(receipt_validation["errors"])
        errors.extend(f"missing backup receipt field: {field}" for field in receipt_validation["missing"])
    if not checks["backup_created"]:
        errors.append("backup receipt does not show a created backup")
    if not checks["backup_confirmed"]:
        errors.append("backup receipt does not show explicit operator confirmation")
    if not checks["restore_validated"]:
        errors.append("backup receipt does not show restore validation")
    if not checks["backup_succeeded"]:
        errors.append("backup receipt status is not successful")

    return {
        "kind": "railway_backup_verdict",
        "status": "passed" if not errors else "failed",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "backup": {
            "backup_id": redacted_receipt.get("backup_id", ""),
            "status": redacted_receipt.get("status", ""),
            "service_id": redacted_receipt.get("service_id", ""),
            "confirmed": receipt.get("confirmed") is True,
            "backup_created": receipt.get("backup_created") is True,
            "restore_validated": receipt.get("restore_validated") is True,
            "restore_validation_method": redacted_receipt.get("restore_validation_method", ""),
            "validation": {
                "status": receipt_validation["status"],
                "missing": receipt_validation["missing"],
                "errors": receipt_validation["errors"],
                "warnings": receipt_validation["warnings"],
            },
            "receipt": redacted_receipt,
        },
        "checks": checks,
        "errors": errors,
        "claim_boundary": "backup_verdict_from_operator_receipt_not_raw_performance_claim",
    }


def build_railway_persistence_verdict(
    pre_manifest: Mapping[str, Any],
    post_manifest: Mapping[str, Any],
    operation_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    pre_smoke = _dict_value(pre_manifest.get("stateful_smoke"))
    post_smoke = _dict_value(post_manifest.get("stateful_smoke"))
    pre_marker = _dict_value(pre_smoke.get("marker"))
    post_marker = _dict_value(post_smoke.get("marker"))
    expected_service_id = _tracedb_service_id(post_manifest) or _tracedb_service_id(pre_manifest)
    receipt = _dict_value(operation_receipt)
    receipt_validation = validate_railway_operation_receipt(
        receipt,
        expected_service_id=expected_service_id,
    )
    redacted_receipt = receipt_validation["receipt"]

    checks = {
        "pre_marker_written": pre_smoke.get("status") == "passed"
        and pre_smoke.get("mode", "write_read") == "write_read",
        "post_marker_visible": post_smoke.get("status") == "passed"
        and post_smoke.get("mode") == "read_only",
        "marker_match": _marker_identity(pre_marker) == _marker_identity(post_marker)
        and bool(pre_marker.get("id")),
        "receipt_valid": receipt_validation["ok"],
        "operation_executed": receipt.get("executed") is True,
        "operation_confirmed": receipt.get("confirmed") is True,
        "operation_succeeded": str(receipt.get("status", "")).lower()
        in RAILWAY_OPERATION_SUCCESS_STATUSES,
    }
    errors = []
    if not checks["pre_marker_written"]:
        errors.append("pre-operation marker write/read evidence is missing or failed")
    if not checks["post_marker_visible"]:
        errors.append("post-operation read-only marker evidence is missing or failed")
    if not checks["marker_match"]:
        errors.append("marker mismatch between pre-operation and post-operation evidence")
    if not checks["receipt_valid"]:
        errors.extend(receipt_validation["errors"])
        errors.extend(f"missing receipt field: {field}" for field in receipt_validation["missing"])
    if not checks["operation_executed"]:
        errors.append("operation receipt does not show an executed restart/redeploy")
    if not checks["operation_confirmed"]:
        errors.append("operation receipt does not show explicit operator confirmation")
    if not checks["operation_succeeded"]:
        errors.append("operation receipt status is not successful")

    return {
        "kind": "railway_persistence_verdict",
        "status": "passed" if not errors else "failed",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "marker": {
            "table": pre_marker.get("table", ""),
            "tenant_id": pre_marker.get("tenant_id", ""),
            "id": pre_marker.get("id", ""),
            "pre_run_id": pre_marker.get("run_id", ""),
            "post_run_id": post_marker.get("run_id", ""),
        },
        "operation": {
            "operation": redacted_receipt.get("operation", ""),
            "status": redacted_receipt.get("status", ""),
            "executed": receipt.get("executed") is True,
            "confirmed": receipt.get("confirmed") is True,
            "service_id": redacted_receipt.get("service_id", ""),
            "validation": {
                "status": receipt_validation["status"],
                "missing": receipt_validation["missing"],
                "errors": receipt_validation["errors"],
                "warnings": receipt_validation["warnings"],
            },
            "receipt": redacted_receipt,
        },
        "checks": checks,
        "errors": errors,
        "claim_boundary": "restart_redeploy_persistence_verdict_from_artifacts_not_raw_performance_claim",
    }


def build_railway_operation_plan(
    config: Mapping[str, Any],
    *,
    suite_id: str,
) -> dict[str, Any]:
    validation = validate_railway_config(config)
    service_id = str(config.get("tracedb_service_id", ""))
    operation_status = "manual_required" if validation["ok"] else "blocked_by_missing_config"
    redeploy_command = f'railway up --detach -m "TraceDB benchmark redeploy {_safe_token(suite_id)}"'
    preflight = [
        {
            "name": "cli_context",
            "command": "railway status --json",
            "mutates": False,
            "required": True,
        },
        {
            "name": "service_inventory",
            "command": "railway service status --all --json",
            "mutates": False,
            "required": True,
        },
    ]
    if service_id:
        preflight.append(
            {
                "name": "recent_service_logs",
                "command": f"railway logs --service {service_id} --lines 200 --json",
                "mutates": False,
                "required": False,
            }
        )
    return {
        "kind": "railway_restart_redeploy_plan",
        "suite_id": suite_id,
        "status": "plan_only" if validation["ok"] else "missing_config",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "missing": validation["missing"],
        "warnings": validation["warnings"],
        "service": {
            "project_id": config.get("project_id", ""),
            "environment_id": config.get("environment_id", ""),
            "service_id": service_id,
            "volume_mount_path": config.get("tracedb_volume_mount_path", ""),
        },
        "execution": {
            "executed": False,
            "execute_by_default": False,
            "requires_explicit_operator": True,
        },
        "preflight": preflight,
        "operations": {
            "restart": {
                "status": operation_status,
                "mutates": True,
                "execute_by_default": False,
                "command": "resolve restart command against the installed Railway CLI/API before execution",
                "notes": [
                    "restart command syntax is intentionally not inferred by the benchmark harness",
                    "capture health and marker smoke before and after restart",
                ],
            },
            "redeploy": {
                "status": operation_status,
                "mutates": True,
                "execute_by_default": False,
                "command": redeploy_command,
                "notes": [
                    "requires linked Railway project/environment/service context",
                    "capture health and marker smoke before and after redeploy",
                ],
            },
        },
        "claim_boundary": "plan_only_not_executed_no_restart_redeploy_or_persistence_proof",
    }


def run_railway_endpoint_health(
    config: Mapping[str, Any],
    *,
    timeout_seconds: float = 5.0,
    bearer_token: str | None = None,
) -> dict[str, Any]:
    base_url = _endpoint_base_url(config)
    if not base_url:
        return {
            "status": "not_configured",
            "base_url": "",
            "checks": [],
            "errors": ["no TraceDB Railway URL configured"],
        }

    ready = _probe_http_endpoint(
        base_url,
        "/ready",
        timeout_seconds=timeout_seconds,
        bearer_token=bearer_token,
    )
    if ready["ok"]:
        status = "healthy"
    elif ready.get("status_code") is None:
        status = "unreachable"
    else:
        status = "unhealthy"
    return {
        "status": status,
        "base_url": base_url,
        "checks": [ready],
        "errors": [] if ready["ok"] else [ready["error"]],
    }


def run_railway_stateful_smoke(
    config: Mapping[str, Any],
    *,
    timeout_seconds: float = 5.0,
    bearer_token: str | None = None,
    run_id: str = "",
    marker_id: str | None = None,
    write_marker: bool = True,
) -> dict[str, Any]:
    base_url = _endpoint_base_url(config)
    marker = _stateful_marker(run_id=run_id, marker_id=marker_id)
    mode = "write_read" if write_marker else "read_only"
    if not write_marker and not marker_id:
        return {
            "status": "invalid",
            "mode": mode,
            "base_url": base_url,
            "marker": marker,
            "operations": [],
            "errors": ["marker_id is required for read-only stateful smoke"],
        }
    if not base_url:
        return {
            "status": "not_configured",
            "mode": mode,
            "base_url": "",
            "marker": marker,
            "operations": [],
            "errors": ["no TraceDB Railway URL configured"],
        }

    schema = {
        "name": marker["table"],
        "primary_id_column": "id",
        "tenant_id_column": "tenant",
        "scalar_columns": ["kind", "run_id", "status", "marker_id"],
        "text_indexed_columns": ["body"],
        "vector_columns": [],
    }
    record = {
        "table": marker["table"],
        "id": marker["id"],
        "tenant_id": marker["tenant_id"],
        "fields": {
            "id": marker["id"],
            "tenant": marker["tenant_id"],
            "kind": "railway_stateful_smoke",
            "run_id": marker["run_id"],
            "status": "written",
            "marker_id": marker["id"],
            "body": f"TraceDB Railway stateful smoke marker {marker['id']}",
        },
    }
    get_request = {
        "table": marker["table"],
        "tenant_id": marker["tenant_id"],
        "id": marker["id"],
    }

    operations = []
    if write_marker:
        operations.append(
            _request_json_operation(
                "schema_apply",
                base_url,
                "POST",
                "/v1/schema/apply",
                schema,
                timeout_seconds=timeout_seconds,
                bearer_token=bearer_token,
                idempotency_key=f"railway-smoke:{marker['id']}:schema",
            )
        )
    errors = [operation["error"] for operation in operations if not operation["ok"]]
    if write_marker and not errors:
        operations.append(
            _request_json_operation(
                "record_put",
                base_url,
                "POST",
                "/v1/records/put",
                record,
                timeout_seconds=timeout_seconds,
                bearer_token=bearer_token,
                idempotency_key=f"railway-smoke:{marker['id']}:put",
            )
        )
        if not operations[-1]["ok"]:
            errors.append(operations[-1]["error"])
    if not errors:
        operations.append(
            _request_json_operation(
                "record_get",
                base_url,
                "POST",
                "/v1/records/get",
                get_request,
                timeout_seconds=timeout_seconds,
                bearer_token=bearer_token,
            )
        )
        if not operations[-1]["ok"]:
            errors.append(operations[-1]["error"])
        elif not _marker_visible(
            operations[-1].get("response"),
            marker,
            require_run_id=write_marker,
        ):
            errors.append("marker write was not visible" if write_marker else "marker read was not visible")

    status = "passed" if not errors else "failed"
    if errors and any(operation.get("status_code") is None for operation in operations):
        status = "unreachable"
    return {
        "status": status,
        "mode": mode,
        "base_url": base_url,
        "marker": marker,
        "operations": operations,
        "errors": errors,
    }


def run_railway_snapshot_restore_check(
    config: Mapping[str, Any],
    *,
    timeout_seconds: float = 60.0,
    bearer_token: str | None = None,
    run_id: str = "",
    marker_id: str | None = None,
    snapshot_root: str | None = None,
    verify_restored_marker: bool = False,
) -> dict[str, Any]:
    base_url = _endpoint_base_url(config)
    selected_root = str(snapshot_root or config.get("tracedb_snapshot_root") or "").rstrip("/")
    marker = _stateful_marker(run_id=run_id, marker_id=marker_id)
    restored_read_not_checked = {
        "status": "not_checked",
        "record_visible": False,
        "errors": [],
    }
    if not base_url:
        return {
            "kind": "railway_snapshot_restore_check",
            "status": "not_configured",
            "base_url": "",
            "snapshot_root": selected_root,
            "marker": marker,
            "paths": {},
            "restored_read": restored_read_not_checked,
            "operations": [],
            "errors": ["no TraceDB Railway URL configured"],
            "claim_boundary": "not_checked_no_snapshot_restore_or_backup_dr_proof",
        }
    if not selected_root:
        return {
            "kind": "railway_snapshot_restore_check",
            "status": "not_configured",
            "base_url": base_url,
            "snapshot_root": "",
            "marker": marker,
            "paths": {},
            "restored_read": restored_read_not_checked,
            "operations": [],
            "errors": [
                "TRACEDB_RAILWAY_SNAPSHOT_ROOT or TRACEDB_REMOTE_SNAPSHOT_ROOT is required"
            ],
            "claim_boundary": "not_checked_no_snapshot_restore_or_backup_dr_proof",
        }
    if not selected_root.startswith("/"):
        return {
            "kind": "railway_snapshot_restore_check",
            "status": "invalid",
            "base_url": base_url,
            "snapshot_root": selected_root,
            "marker": marker,
            "paths": {},
            "restored_read": restored_read_not_checked,
            "operations": [],
            "errors": ["snapshot root must be an absolute server-side path"],
            "claim_boundary": "not_checked_no_snapshot_restore_or_backup_dr_proof",
        }
    volume_path = str(config.get("tracedb_volume_mount_path") or "").rstrip("/")
    if volume_path and selected_root == volume_path:
        return {
            "kind": "railway_snapshot_restore_check",
            "status": "invalid",
            "base_url": base_url,
            "snapshot_root": selected_root,
            "marker": marker,
            "paths": {},
            "restored_read": restored_read_not_checked,
            "operations": [],
            "errors": ["snapshot root must differ from the configured Railway volume path"],
            "claim_boundary": "not_checked_no_snapshot_restore_or_backup_dr_proof",
        }

    safe_run_id = _safe_token(marker["run_id"])
    safe_marker_id = _safe_token(marker["id"])
    snapshot_dir = f"{selected_root}/{safe_run_id}/{safe_marker_id}/snapshot"
    restore_dir = f"{selected_root}/{safe_run_id}/{safe_marker_id}/restore"
    operations = []
    errors = []

    operations.append(
        _request_json_operation(
            "snapshot",
            base_url,
            "POST",
            "/v1/admin/snapshot",
            {"target": snapshot_dir},
            timeout_seconds=timeout_seconds,
            bearer_token=bearer_token,
            idempotency_key=f"railway-snapshot:{safe_run_id}:{safe_marker_id}",
        )
    )
    if not operations[-1]["ok"]:
        errors.append(operations[-1]["error"])
    else:
        response = _dict_value(operations[-1].get("response"))
        if response.get("snapshot") is not True or response.get("target") != snapshot_dir:
            errors.append("snapshot response did not confirm the requested target")

    restored_read = dict(restored_read_not_checked)
    if not errors:
        restore_body = {"source": snapshot_dir, "target": restore_dir}
        if verify_restored_marker:
            restore_body["verify_record"] = {
                "table": marker["table"],
                "tenant_id": marker["tenant_id"],
                "id": marker["id"],
            }
        operations.append(
            _request_json_operation(
                "restore",
                base_url,
                "POST",
                "/v1/admin/restore",
                restore_body,
                timeout_seconds=timeout_seconds,
                bearer_token=bearer_token,
                idempotency_key=f"railway-restore:{safe_run_id}:{safe_marker_id}",
            )
        )
        if not operations[-1]["ok"]:
            errors.append(operations[-1]["error"])
        else:
            response = _dict_value(operations[-1].get("response"))
            if (
                response.get("restored") is not True
                or response.get("source") != snapshot_dir
                or response.get("target") != restore_dir
            ):
                errors.append("restore response did not confirm the requested source and target")
            if verify_restored_marker:
                verification = _dict_value(response.get("verification"))
                record = _dict_value(verification.get("record"))
                record_visible = (
                    verification.get("status") == "passed"
                    and verification.get("record_visible") is True
                    and record.get("id") == marker["id"]
                    and record.get("tenant_id") == marker["tenant_id"]
                    and record.get("table") == marker["table"]
                )
                restored_read = {
                    "status": "passed" if record_visible else "failed",
                    "record_visible": record_visible,
                    "request": {
                        "table": marker["table"],
                        "tenant_id": marker["tenant_id"],
                        "id": marker["id"],
                    },
                    "record": record,
                    "errors": []
                    if record_visible
                    else ["restore verification did not return the requested marker"],
                }
                if not record_visible:
                    errors.append("restored marker was not visible in restore verification")

    status = "passed" if not errors else "failed"
    if errors and any(operation.get("status_code") is None for operation in operations):
        status = "unreachable"
    return {
        "kind": "railway_snapshot_restore_check",
        "status": status,
        "base_url": base_url,
        "snapshot_root": selected_root,
        "marker": marker,
        "paths": {
            "snapshot": snapshot_dir,
            "restore": restore_dir,
        },
        "restored_read": restored_read,
        "operations": operations,
        "errors": errors,
        "claim_boundary": "admin_route_snapshot_restore_not_managed_backup_dr"
        if verify_restored_marker
        else "admin_route_snapshot_restore_not_managed_backup_dr_or_restored_service_read_proof",
    }


def redact_env(env: Mapping[str, str]) -> dict[str, str]:
    redacted = {}
    for key, value in env.items():
        redacted[key] = "<redacted>" if _is_sensitive_key(key) and value else value
    return redacted


def _redact_sensitive(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): "<redacted>" if _is_sensitive_key(str(key)) and item else _redact_sensitive(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_sensitive(item) for item in value]
    return value


def _dict_value(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _artifact_entry(suite_dir: Path, name: str, path: str) -> dict[str, Any]:
    artifact_path = Path(path)
    resolved = artifact_path if artifact_path.is_absolute() else suite_dir / artifact_path
    exists = resolved.exists()
    entry = {
        "name": name,
        "path": path,
        "exists": exists,
        "size_bytes": 0,
        "sha256": "",
    }
    if exists and resolved.is_file():
        data = resolved.read_bytes()
        entry["size_bytes"] = len(data)
        entry["sha256"] = hashlib.sha256(data).hexdigest()
    return entry


def _artifact_proof_gaps(claim_status: Mapping[str, Any]) -> list[str]:
    gaps = []
    snapshot_restore_status = claim_status.get("railway_snapshot_restore", "not_checked")
    if snapshot_restore_status == "passed":
        gaps.append("snapshot_restore_admin_route_only_not_managed_backup_dr")
    else:
        gaps.append("snapshot_restore_not_checked")
    restored_read_status = claim_status.get("railway_restored_read", "not_checked")
    if restored_read_status != "passed":
        gaps.append("restored_read_not_checked")
    backup_status = claim_status.get("railway_backup", "not_checked")
    if backup_status != "passed":
        gaps.append("backup_validation_not_checked")
    return gaps


def _marker_identity(marker: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(marker.get("table", "")),
        str(marker.get("tenant_id", "")),
        str(marker.get("id", "")),
    )


def _tracedb_service_id(manifest: Mapping[str, Any]) -> str:
    services = manifest.get("services")
    if not isinstance(services, list):
        return ""
    for service in services:
        if isinstance(service, Mapping) and service.get("role") == "tracedb":
            return str(service.get("service_id", ""))
    return ""


def _ssh_hints(services: list[dict[str, Any]]) -> list[str]:
    hints = []
    for service in services:
        service_id = service.get("service_id")
        if service_id:
            hints.append(f"railway ssh --service {service_id}")
    return hints


def _is_sensitive_key(key: str) -> bool:
    upper = key.upper()
    return any(part in upper for part in SENSITIVE_KEY_PARTS)


def _endpoint_base_url(config: Mapping[str, Any]) -> str:
    return str(config.get("tracedb_private_url") or config.get("tracedb_public_url") or "").rstrip("/")


def _probe_http_endpoint(
    base_url: str,
    path: str,
    *,
    timeout_seconds: float,
    bearer_token: str | None,
) -> dict[str, Any]:
    url = parse.urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/"))
    headers = {"Accept": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    req = request.Request(url, headers=headers)
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read(512).decode("utf-8", errors="replace")
            status_code = int(response.status)
    except urlerror.HTTPError as error:
        try:
            body = error.read(512).decode("utf-8", errors="replace")
        finally:
            error.close()
        status_code = int(error.code)
        return {
            "name": "ready",
            "path": path,
            "url": url,
            "status_code": status_code,
            "ok": False,
            "body_excerpt": body[:200],
            "error": f"HTTP {status_code}",
        }
    except Exception as error:
        return {
            "name": "ready",
            "path": path,
            "url": url,
            "status_code": None,
            "ok": False,
            "body_excerpt": "",
            "error": str(error),
        }
    return {
        "name": "ready",
        "path": path,
        "url": url,
        "status_code": status_code,
        "ok": 200 <= status_code < 300,
        "body_excerpt": body[:200],
        "error": "" if 200 <= status_code < 300 else f"HTTP {status_code}",
    }


def _stateful_marker(*, run_id: str, marker_id: str | None) -> dict[str, str]:
    fallback_id = datetime.now(timezone.utc).strftime("railway-smoke-%Y%m%d%H%M%S%f")
    selected_run_id = run_id or fallback_id
    selected_marker_id = marker_id or f"{_safe_token(selected_run_id)}-{fallback_id}"
    return {
        "table": "railway_stateful_markers",
        "tenant_id": "railway-smoke",
        "id": _safe_token(selected_marker_id),
        "run_id": selected_run_id,
    }


def _safe_token(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in value)[:96]


def _request_json_operation(
    name: str,
    base_url: str,
    method: str,
    path: str,
    body: Mapping[str, Any] | None,
    *,
    timeout_seconds: float,
    bearer_token: str | None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    url = parse.urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/"))
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            raw_body = response.read(4096)
            payload = json.loads(raw_body.decode("utf-8")) if raw_body else {}
            status_code = int(response.status)
    except urlerror.HTTPError as error:
        try:
            raw_body = error.read(4096)
            text = raw_body.decode("utf-8", errors="replace")
        finally:
            error.close()
        return {
            "name": name,
            "method": method,
            "path": path,
            "status_code": int(error.code),
            "ok": False,
            "response": _decode_json_or_excerpt(text),
            "error": f"HTTP {int(error.code)}",
        }
    except Exception as error:
        return {
            "name": name,
            "method": method,
            "path": path,
            "status_code": None,
            "ok": False,
            "response": {},
            "error": str(error),
        }
    return {
        "name": name,
        "method": method,
        "path": path,
        "status_code": status_code,
        "ok": 200 <= status_code < 300,
        "response": payload,
        "error": "" if 200 <= status_code < 300 else f"HTTP {status_code}",
    }


def _decode_json_or_excerpt(text: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {"body_excerpt": text[:200]}
    return payload if isinstance(payload, dict) else {"body": payload}


def _marker_visible(
    response: Any,
    marker: Mapping[str, str],
    *,
    require_run_id: bool = True,
) -> bool:
    if not isinstance(response, dict):
        return False
    record = response.get("record")
    if not isinstance(record, dict):
        return False
    fields = record.get("fields")
    if not isinstance(fields, dict):
        return False
    return (
        record.get("id") == marker["id"]
        and record.get("tenant_id") == marker["tenant_id"]
        and fields.get("marker_id") == marker["id"]
        and (not require_run_id or fields.get("run_id") == marker["run_id"])
    )
