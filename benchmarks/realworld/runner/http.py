from __future__ import annotations

import http.client
import json
import os
import socket
import time
from typing import Any
from urllib.parse import urlparse


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
    timeout = timeout if timeout is not None else _float_env("TRACEDB_HTTP_TIMEOUT_SECONDS", 10.0)
    retries = retries if retries is not None else _int_env("TRACEDB_HTTP_RETRIES", 2)
    attempt = 0
    last_error: BaseException | None = None
    while attempt <= retries:
        try:
            return _request_json_with_response_once(method, url, data, headers, timeout)
        except _HttpStatusError as error:
            raise RuntimeError(f"HTTP {error.status} from {url}: {error.payload}") from error
        except (OSError, http.client.HTTPException, TimeoutError, ConnectionResetError, socket.timeout) as error:
            last_error = error
            if attempt >= retries:
                break
            time.sleep(min(0.25 * (2**attempt), 2.0))
            attempt += 1
            continue
    raise RuntimeError(f"request failed after {retries + 1} attempts to {url}: {last_error}")


class _HttpStatusError(Exception):
    def __init__(self, status: int, payload: str) -> None:
        super().__init__(f"HTTP {status}: {payload}")
        self.status = status
        self.payload = payload


def _request_json_with_response_once(
    method: str,
    url: str,
    data: bytes | None,
    headers: dict[str, str],
    timeout: float,
) -> tuple[dict[str, Any], dict[str, str], dict[str, float | int]]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise RuntimeError(f"unsupported URL for benchmark HTTP request: {url}")
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    connection_cls = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    conn = connection_cls(parsed.hostname, parsed.port, timeout=timeout)
    started = time.perf_counter()
    try:
        connect_started = time.perf_counter()
        conn.connect()
        socket_connect_ms = (time.perf_counter() - connect_started) * 1000.0

        request_headers = dict(headers)
        if data is not None:
            request_headers["content-length"] = str(len(data))
        header_write_started = time.perf_counter()
        conn.putrequest(method, path, skip_accept_encoding=True)
        for name, value in request_headers.items():
            conn.putheader(name, value)
        conn.endheaders()
        request_header_write_ms = (time.perf_counter() - header_write_started) * 1000.0

        request_body_write_ms = 0.0
        if data is not None:
            body_write_started = time.perf_counter()
            conn.send(data)
            request_body_write_ms = (time.perf_counter() - body_write_started) * 1000.0
        request_write_ms = request_header_write_ms + request_body_write_ms

        response_header_started = time.perf_counter()
        response = conn.getresponse()
        response_header_wait_ms = (time.perf_counter() - response_header_started) * 1000.0
        header_wait_ms = (time.perf_counter() - started) * 1000.0

        read_started = time.perf_counter()
        raw_body = response.read()
        body_read_ms = (time.perf_counter() - read_started) * 1000.0
        text = raw_body.decode("utf-8", errors="replace")
        if response.status >= 400:
            raise _HttpStatusError(response.status, text)

        decode_started = time.perf_counter()
        text = raw_body.decode("utf-8")
        decode_ms = (time.perf_counter() - decode_started) * 1000.0
        response_headers = {name.lower(): value for name, value in response.headers.items()}
        parse_started = time.perf_counter()
        payload = json.loads(text) if text else {}
        json_parse_ms = (time.perf_counter() - parse_started) * 1000.0
        body_bytes = len(raw_body)
        content_length = _int_header(response_headers.get("content-length"))
        response_meta: dict[str, float | int] = {
            "header_wait_ms": header_wait_ms,
            "socket_connect_ms": socket_connect_ms,
            "request_header_write_ms": request_header_write_ms,
            "request_body_write_ms": request_body_write_ms,
            "request_write_ms": request_write_ms,
            "response_header_wait_ms": response_header_wait_ms,
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
    finally:
        conn.close()


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
