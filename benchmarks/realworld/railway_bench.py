from __future__ import annotations

import json
import os
from datetime import datetime, timezone
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
    operation_plan: Mapping[str, Any] | None = None,
    persistence_verdict: Mapping[str, Any] | None = None,
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
    if operation_plan is not None:
        manifest["operation_plan"] = dict(operation_plan)
    if persistence_verdict is not None:
        manifest["persistence_verdict"] = dict(persistence_verdict)
    return manifest


def build_railway_persistence_verdict(
    pre_manifest: Mapping[str, Any],
    post_manifest: Mapping[str, Any],
    operation_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    pre_smoke = _dict_value(pre_manifest.get("stateful_smoke"))
    post_smoke = _dict_value(post_manifest.get("stateful_smoke"))
    pre_marker = _dict_value(pre_smoke.get("marker"))
    post_marker = _dict_value(post_smoke.get("marker"))
    redacted_receipt = _redact_sensitive(operation_receipt)

    checks = {
        "pre_marker_written": pre_smoke.get("status") == "passed"
        and pre_smoke.get("mode", "write_read") == "write_read",
        "post_marker_visible": post_smoke.get("status") == "passed"
        and post_smoke.get("mode") == "read_only",
        "marker_match": _marker_identity(pre_marker) == _marker_identity(post_marker)
        and bool(pre_marker.get("id")),
        "operation_executed": bool(operation_receipt.get("executed")),
        "operation_succeeded": str(operation_receipt.get("status", "")).lower()
        in {"passed", "completed", "succeeded", "success", "ok"},
    }
    errors = []
    if not checks["pre_marker_written"]:
        errors.append("pre-operation marker write/read evidence is missing or failed")
    if not checks["post_marker_visible"]:
        errors.append("post-operation read-only marker evidence is missing or failed")
    if not checks["marker_match"]:
        errors.append("marker mismatch between pre-operation and post-operation evidence")
    if not checks["operation_executed"]:
        errors.append("operation receipt does not show an executed restart/redeploy")
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
            "executed": bool(operation_receipt.get("executed")),
            "service_id": redacted_receipt.get("service_id", ""),
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


def _marker_identity(marker: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(marker.get("table", "")),
        str(marker.get("tenant_id", "")),
        str(marker.get("id", "")),
    )


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
