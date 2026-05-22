from __future__ import annotations

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
    return manifest


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


def redact_env(env: Mapping[str, str]) -> dict[str, str]:
    redacted = {}
    for key, value in env.items():
        redacted[key] = "<redacted>" if _is_sensitive_key(key) and value else value
    return redacted


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
