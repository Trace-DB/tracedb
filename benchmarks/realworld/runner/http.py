from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.request
from typing import Any


def request_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    timeout: float | None = None,
    retries: int | None = None,
) -> dict[str, Any]:
    payload, _headers = request_json_with_headers(
        method, url, body, timeout=timeout, retries=retries
    )
    return payload


def request_json_with_headers(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    timeout: float | None = None,
    retries: int | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    payload, headers, _response_meta = request_json_with_response(
        method, url, body, timeout=timeout, retries=retries
    )
    return payload, headers


def request_json_with_response(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
    timeout: float | None = None,
    retries: int | None = None,
) -> tuple[dict[str, Any], dict[str, str], dict[str, float | int]]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"content-type": "application/json"}
    token = os.environ.get("TRACEDB_HTTP_BEARER_TOKEN") or os.environ.get("TRACEDB_API_TOKEN")
    if token:
        headers["authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers=headers,
    )
    timeout = timeout if timeout is not None else _float_env("TRACEDB_HTTP_TIMEOUT_SECONDS", 10.0)
    retries = retries if retries is not None else _int_env("TRACEDB_HTTP_RETRIES", 2)
    attempt = 0
    last_error: BaseException | None = None
    while attempt <= retries:
        try:
            started = time.perf_counter()
            with urllib.request.urlopen(request, timeout=timeout) as response:
                header_wait_ms = (time.perf_counter() - started) * 1000.0
                read_started = time.perf_counter()
                raw_body = response.read()
                body_read_ms = (time.perf_counter() - read_started) * 1000.0
                decode_started = time.perf_counter()
                text = raw_body.decode("utf-8")
                decode_ms = (time.perf_counter() - decode_started) * 1000.0
                response_headers = {
                    name.lower(): value for name, value in response.headers.items()
                }
                parse_started = time.perf_counter()
                payload = json.loads(text) if text else {}
                json_parse_ms = (time.perf_counter() - parse_started) * 1000.0
                body_bytes = len(raw_body)
                content_length = _int_header(response_headers.get("content-length"))
                response_meta: dict[str, float | int] = {
                    "header_wait_ms": header_wait_ms,
                    "body_read_ms": body_read_ms,
                    "decode_ms": decode_ms,
                    "json_parse_ms": json_parse_ms,
                    "processing_ms": body_read_ms + decode_ms + json_parse_ms,
                    "body_bytes": body_bytes,
                    "content_length_missing": 1 if content_length is None else 0,
                    "content_length_mismatch": (
                        1 if content_length is not None and content_length != body_bytes else 0
                    ),
                }
                if content_length is not None:
                    response_meta["content_length_bytes"] = content_length
                return payload, response_headers, response_meta
        except urllib.error.HTTPError as error:
            payload = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {error.code} from {url}: {payload}") from error
        except (urllib.error.URLError, TimeoutError, ConnectionResetError, socket.timeout) as error:
            last_error = error
            if attempt >= retries:
                break
            time.sleep(min(0.25 * (2**attempt), 2.0))
            attempt += 1
            continue
    raise RuntimeError(f"request failed after {retries + 1} attempts to {url}: {last_error}")


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, ""))
    except ValueError:
        return default


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, ""))
    except ValueError:
        return default


def _int_header(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
