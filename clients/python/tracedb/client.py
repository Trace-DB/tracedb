from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


JsonObject = dict[str, Any]


class TraceDBRequestError(ValueError):
    def __init__(self, method: str, path: str, message: str) -> None:
        super().__init__(f"{method} {path}: {message}")
        self.method = method
        self.path = path
        self.message = message


class TraceDBHTTPError(RuntimeError):
    def __init__(self, method: str, path: str, status: int, body: str) -> None:
        self.method = method
        self.path = path
        self.status = status
        self.response_body = body
        self.response_json = _loads_json(body)
        if isinstance(self.response_json, dict):
            self.response_error = self.response_json.get("error")
            self.response_code = self.response_json.get("code")
        else:
            self.response_error = None
            self.response_code = None
        message = self.response_error if isinstance(self.response_error, str) else body
        super().__init__(f"{method} {path} returned HTTP {status}: {message}")


@dataclass(frozen=True)
class TraceDB:
    url: str
    token: str | None = None
    database_id: str | None = None
    branch_id: str | None = None
    timeout: float = 5.0
    safe_retries: int = 0
    idempotency_retries: int = 0

    def __post_init__(self) -> None:
        if not self.url or not self.url.strip():
            raise TraceDBRequestError("CONFIG", "url", "TraceDB requires a non-empty url")
        if self.safe_retries < 0:
            raise TraceDBRequestError("CONFIG", "safe_retries", "safe_retries must be greater than or equal to 0")
        if self.idempotency_retries < 0:
            raise TraceDBRequestError(
                "CONFIG",
                "idempotency_retries",
                "idempotency_retries must be greater than or equal to 0",
            )
        object.__setattr__(self, "url", self.url.rstrip("/"))

    @classmethod
    def from_env(
        cls,
        *,
        url: str | None = None,
        token: str | None = None,
        database_id: str | None = None,
        branch_id: str | None = None,
        timeout: float | None = None,
        safe_retries: int | None = None,
        idempotency_retries: int | None = None,
        env: Mapping[str, str] | None = None,
    ) -> "TraceDB":
        source = os.environ if env is None else env
        resolved_url = url if url is not None else source.get("TRACEDB_URL")
        if resolved_url is None or not resolved_url.strip():
            raise TraceDBRequestError("CONFIG", "TRACEDB_URL", "TraceDB.from_env requires TRACEDB_URL")

        resolved_timeout = timeout
        if resolved_timeout is None:
            timeout_ms = source.get("TRACEDB_TIMEOUT_MS")
            if timeout_ms is not None and timeout_ms.strip():
                try:
                    resolved_timeout = float(timeout_ms) / 1000.0
                except ValueError as error:
                    raise TraceDBRequestError(
                        "CONFIG",
                        "TRACEDB_TIMEOUT_MS",
                        "TRACEDB_TIMEOUT_MS must be a positive number",
                    ) from error

        if resolved_timeout is not None and resolved_timeout <= 0:
            raise TraceDBRequestError(
                "CONFIG",
                "TRACEDB_TIMEOUT_MS",
                "TRACEDB_TIMEOUT_MS must be greater than 0",
            )

        resolved_safe_retries = (
            safe_retries
            if safe_retries is not None
            else _parse_optional_nonnegative_int("TRACEDB_SAFE_RETRIES", source.get("TRACEDB_SAFE_RETRIES"))
        )
        resolved_idempotency_retries = (
            idempotency_retries
            if idempotency_retries is not None
            else _parse_optional_nonnegative_int(
                "TRACEDB_IDEMPOTENCY_RETRIES",
                source.get("TRACEDB_IDEMPOTENCY_RETRIES"),
            )
        )

        kwargs: dict[str, Any] = {
            "url": resolved_url,
            "token": token if token is not None else source.get("TRACEDB_TOKEN"),
            "database_id": database_id if database_id is not None else source.get("TRACEDB_DATABASE_ID"),
            "branch_id": branch_id if branch_id is not None else source.get("TRACEDB_BRANCH_ID"),
        }
        if resolved_timeout is not None:
            kwargs["timeout"] = resolved_timeout
        if resolved_safe_retries is not None:
            kwargs["safe_retries"] = resolved_safe_retries
        if resolved_idempotency_retries is not None:
            kwargs["idempotency_retries"] = resolved_idempotency_retries
        return cls(**kwargs)

    def request_json(
        self,
        method: str,
        path: str,
        body: JsonObject | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        request_body = self._body_with_routing(body)
        headers = {
            "Accept": "application/json",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        data = None
        if request_body is not None:
            data = json.dumps(request_body, sort_keys=True, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if idempotency_key is not None:
            _validate_idempotency_key(method, path, idempotency_key)
            headers["Idempotency-Key"] = idempotency_key
        request = urllib.request.Request(
            f"{self.url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        attempts = _attempt_count(method, path, self.safe_retries, self.idempotency_retries, idempotency_key)
        for attempt in range(attempts):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = response.read().decode("utf-8")
            except urllib.error.HTTPError as error:
                if _should_retry_http_error(method, path, error.code, attempt, attempts):
                    continue
                payload = error.read().decode("utf-8")
                raise TraceDBHTTPError(method, path, error.code, payload) from error
            if not payload:
                return {}
            parsed = _loads_json(payload)
            if not isinstance(parsed, dict):
                raise TraceDBRequestError(method, path, f"expected JSON object response, got {payload!r}")
            return parsed
        raise TraceDBRequestError(method, path, "request retry loop exhausted without a response")

    def ready(self) -> JsonObject:
        return self.request_json("GET", "/v1/ready")

    def health(self) -> JsonObject:
        return self.request_json("GET", "/v1/health")

    def apply_schema(self, schema: JsonObject, *, idempotency_key: str | None = None) -> JsonObject:
        return self.request_json("POST", "/v1/schema/apply", schema, idempotency_key=idempotency_key)

    def list_databases(self) -> JsonObject:
        return self.request_json("GET", "/v1/databases")

    def list_branches(self) -> JsonObject:
        return self.request_json("GET", "/v1/branches")

    def public_safe_metrics(self) -> JsonObject:
        return self.request_json("GET", "/v1/metrics/public-safe")

    def compact(self, *, idempotency_key: str | None = None) -> JsonObject:
        return self.request_json("POST", "/v1/admin/compact", {}, idempotency_key=idempotency_key)

    def snapshot(self, target: str, *, idempotency_key: str | None = None) -> JsonObject:
        return self.request_json(
            "POST",
            "/v1/admin/snapshot",
            {"target": target},
            idempotency_key=idempotency_key,
        )

    def restore(self, source: str, target: str, *, idempotency_key: str | None = None) -> JsonObject:
        return self.request_json(
            "POST",
            "/v1/admin/restore",
            {"source": source, "target": target},
            idempotency_key=idempotency_key,
        )

    def list_admin_jobs(self) -> JsonObject:
        return self.request_json("GET", "/v1/admin/jobs")

    def traceql(self, query: str) -> JsonObject:
        return self.traceql_request({"query": query})

    def traceql_request(self, request: JsonObject) -> JsonObject:
        return self.request_json("POST", "/v1/traceql", dict(request))

    def table(self, name: str) -> "TraceDBTable":
        return TraceDBTable(self, name)

    def _body_with_routing(self, body: JsonObject | None) -> JsonObject | None:
        if body is None:
            return None
        copied = dict(body)
        if self.database_id and "database_id" not in copied:
            copied["database_id"] = self.database_id
        if self.branch_id and "branch_id" not in copied:
            copied["branch_id"] = self.branch_id
        return copied


@dataclass(frozen=True)
class TraceDBTable:
    db: TraceDB
    name: str
    tenant_id: str | None = None
    scan_limit: int = 100

    def tenant(self, tenant_id: str) -> "TraceDBTable":
        return TraceDBTable(self.db, self.name, tenant_id, self.scan_limit)

    def limit(self, limit: int) -> "TraceDBTable":
        return TraceDBTable(self.db, self.name, self.tenant_id, limit)

    def insert(self, record_id: str, fields: JsonObject, *, idempotency_key: str | None = None) -> JsonObject:
        return self.db.request_json(
            "POST",
            "/v1/records/put",
            self._record_input(record_id, fields, "/v1/records/put"),
            idempotency_key=idempotency_key,
        )

    def insert_batch(
        self,
        records: list[dict[str, Any]],
        *,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        tenant_id = self._required_tenant("/v1/records/put-batch")
        return self.db.request_json(
            "POST",
            "/v1/records/put-batch",
            {
                "records": [
                    self._record_input_with_tenant(str(record["id"]), dict(record["fields"]), tenant_id)
                    for record in records
                ]
            },
            idempotency_key=idempotency_key,
        )

    def patch(self, record_id: str, fields: JsonObject, *, idempotency_key: str | None = None) -> JsonObject:
        return self.db.request_json(
            "POST",
            "/v1/records/patch",
            {
                "table": self.name,
                "tenant_id": self._required_tenant("/v1/records/patch"),
                "id": record_id,
                "fields": dict(fields),
            },
            idempotency_key=idempotency_key,
        )

    def get(self, record_id: str) -> JsonObject:
        return self.db.request_json(
            "POST",
            "/v1/records/get",
            {
                "table": self.name,
                "tenant_id": self._required_tenant("/v1/records/get"),
                "id": record_id,
            },
        )

    def scan(self) -> JsonObject:
        return self.db.request_json(
            "POST",
            "/v1/records/scan",
            {
                "table": self.name,
                "tenant_id": self._required_tenant("/v1/records/scan"),
                "limit": self.scan_limit,
            },
        )

    def delete(
        self,
        record_id: str,
        *,
        tombstone: str | None = None,
        idempotency_key: str | None = None,
    ) -> JsonObject:
        body: JsonObject = {
            "table": self.name,
            "tenant_id": self._required_tenant("/v1/records/delete"),
            "id": record_id,
        }
        if tombstone is not None:
            body["tombstone"] = tombstone
        return self.db.request_json("POST", "/v1/records/delete", body, idempotency_key=idempotency_key)

    def query(self) -> "TraceDBQueryBuilder":
        return TraceDBQueryBuilder(self.db, self.name, self.tenant_id)

    def where(self, filters: JsonObject) -> "TraceDBQueryBuilder":
        return self.query().where(filters)

    def where_eq(self, field: str, value: Any) -> "TraceDBQueryBuilder":
        return self.query().where_eq(field, value)

    def match_text(self, field: str, query: str) -> "TraceDBQueryBuilder":
        return self.query().match_text(field, query)

    def near(self, field: str, vector: list[float]) -> "TraceDBQueryBuilder":
        return self.query().near(field, vector)

    def _record_input(self, record_id: str, fields: JsonObject, path: str) -> JsonObject:
        return self._record_input_with_tenant(record_id, dict(fields), self._required_tenant(path))

    def _record_input_with_tenant(self, record_id: str, fields: JsonObject, tenant_id: str) -> JsonObject:
        record_fields = dict(fields)
        record_fields.setdefault("id", record_id)
        record_fields.setdefault("tenant", tenant_id)
        return {
            "table": self.name,
            "tenant_id": tenant_id,
            "id": record_id,
            "fields": record_fields,
        }

    def _required_tenant(self, path: str) -> str:
        if self.tenant_id:
            return self.tenant_id
        raise TraceDBRequestError("POST", path, "table handle execution requires tenant(...)")


@dataclass(frozen=True)
class TraceDBQueryBuilder:
    db: TraceDB
    table_name: str
    tenant_id: str | None = None
    scalar_eq: JsonObject | None = None
    text_query: str | None = None
    vector_query: list[float] | None = None
    top_k: int = 10
    freshness: str = "Strict"
    explain: bool = True

    def where(self, filters: JsonObject) -> "TraceDBQueryBuilder":
        tenant_id = self.tenant_id
        scalar_eq = dict(self.scalar_eq or {})
        for key, value in filters.items():
            if key == "tenant_id" and isinstance(value, str):
                tenant_id = value
            else:
                scalar_eq[key] = value
        return self._copy(tenant_id=tenant_id, scalar_eq=scalar_eq)

    def where_eq(self, field: str, value: Any) -> "TraceDBQueryBuilder":
        scalar_eq = dict(self.scalar_eq or {})
        scalar_eq[field] = value
        return self._copy(scalar_eq=scalar_eq)

    def match_text(self, _field: str, query: str) -> "TraceDBQueryBuilder":
        return self._copy(text_query=query)

    def near(self, _field: str, vector: list[float]) -> "TraceDBQueryBuilder":
        return self._copy(vector_query=list(vector))

    def with_options(
        self,
        *,
        explain: bool | None = None,
        freshness: str | None = None,
    ) -> "TraceDBQueryBuilder":
        return self._copy(
            explain=self.explain if explain is None else explain,
            freshness=self.freshness if freshness is None else _normalize_freshness(freshness),
        )

    def limit(self, limit: int) -> "TraceDBQueryBuilder":
        return self._copy(top_k=limit)

    def all(self) -> JsonObject:
        return self.db.request_json("POST", "/v1/query", self._hybrid_query("/v1/query"))

    def explain_plan(self) -> JsonObject:
        return self.db.request_json("POST", "/v1/explain", self._hybrid_query("/v1/explain"))

    def _hybrid_query(self, path: str) -> JsonObject:
        if not self.tenant_id:
            raise TraceDBRequestError("POST", path, "query execution requires tenant(...) or where({'tenant_id': ...})")
        return {
            "table": self.table_name,
            "tenant_id": self.tenant_id,
            "scalar_eq": dict(self.scalar_eq or {}),
            "text": self.text_query,
            "vector": self.vector_query,
            "top_k": self.top_k,
            "freshness": self.freshness,
            "explain": self.explain,
        }

    def _copy(self, **overrides: Any) -> "TraceDBQueryBuilder":
        values = {
            "db": self.db,
            "table_name": self.table_name,
            "tenant_id": self.tenant_id,
            "scalar_eq": dict(self.scalar_eq or {}),
            "text_query": self.text_query,
            "vector_query": list(self.vector_query) if self.vector_query is not None else None,
            "top_k": self.top_k,
            "freshness": self.freshness,
            "explain": self.explain,
        }
        values.update(overrides)
        return TraceDBQueryBuilder(**values)


def _validate_idempotency_key(method: str, path: str, key: str) -> None:
    if not key:
        raise TraceDBRequestError(method, path, "idempotency key cannot be empty")
    if "\r" in key or "\n" in key:
        raise TraceDBRequestError(method, path, "idempotency key cannot contain CR/LF")


def _parse_optional_nonnegative_int(variable: str, value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise TraceDBRequestError("CONFIG", variable, f"{variable} must be a non-negative integer") from error
    if parsed < 0:
        raise TraceDBRequestError("CONFIG", variable, f"{variable} must be a non-negative integer")
    return parsed


def _attempt_count(
    method: str,
    path: str,
    safe_retries: int,
    idempotency_retries: int,
    idempotency_key: str | None,
) -> int:
    if _is_retry_safe_request(method, path):
        return safe_retries + 1
    if _is_idempotent_retry_request(method, path) and idempotency_key:
        return idempotency_retries + 1
    return 1


def _should_retry_http_error(method: str, path: str, status: int, attempt: int, attempts: int) -> bool:
    return status >= 500 and attempt + 1 < attempts


def _is_retry_safe_request(method: str, path: str) -> bool:
    return (method, path.split("?", 1)[0]) in {
        ("GET", "/v1/health"),
        ("GET", "/v1/ready"),
        ("POST", "/v1/records/get"),
        ("POST", "/v1/records/scan"),
        ("POST", "/v1/query"),
        ("POST", "/v1/traceql"),
        ("POST", "/v1/explain"),
    }


def _is_idempotent_retry_request(method: str, path: str) -> bool:
    return (method, path.split("?", 1)[0]) in {
        ("POST", "/v1/schema/apply"),
        ("POST", "/v1/insert"),
        ("POST", "/v1/records/put"),
        ("POST", "/v1/records/put-batch"),
        ("POST", "/v1/records/patch"),
        ("POST", "/v1/records/delete"),
        ("POST", "/v1/admin/compact"),
        ("POST", "/v1/admin/snapshot"),
        ("POST", "/v1/admin/restore"),
    }


def _normalize_freshness(freshness: str) -> str:
    normalized = freshness.strip().lower()
    if normalized == "strict":
        return "Strict"
    if normalized in {"lazy", "onread", "on_read", "allowstale", "allow_stale"}:
        return "Lazy"
    return freshness


def _loads_json(body: str) -> Any:
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None
