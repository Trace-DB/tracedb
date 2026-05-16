from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SECRET_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9._-]+"),
    re.compile(r"Bearer\s+[A-Za-z0-9._-]+", re.IGNORECASE),
]
SECRET_FIELD_NAMES = {
    "api_key",
    "authorization",
    "creator_user_id",
    "label",
    "token",
}


def new_run_id(prefix: str = "run") -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}-{uuid.uuid4().hex[:8]}"


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if any(name in key_text for name in SECRET_FIELD_NAMES) and isinstance(item, str):
                out[key] = "[redacted]"
            else:
                out[key] = redact(item)
        return out
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    if is_dataclass(value):
        return redact(asdict(value))
    if isinstance(value, str):
        redacted = value
        for pattern in SECRET_PATTERNS:
            redacted = pattern.sub("[redacted]", redacted)
        return redacted
    return value


def json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


class ExperimentRecorder:
    def __init__(self, run_id: str, reports_dir: Path) -> None:
        self.run_id = run_id
        self.run_dir = reports_dir / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.observations_path = self.run_dir / "observations.jsonl"
        self.failures_path = self.run_dir / "failures.md"
        self._events_written = 0

    def write_manifest(self, manifest: dict[str, Any]) -> None:
        path = self.run_dir / "manifest.json"
        path.write_text(
            json.dumps(redact(manifest), indent=2, sort_keys=True, default=json_default) + "\n",
            encoding="utf-8",
        )

    def observe(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        event = {
            "event_type": event_type,
            "run_id": self.run_id,
            "time": datetime.now(timezone.utc).isoformat(),
            "payload": redact(payload or {}),
        }
        with self.observations_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True, default=json_default) + "\n")
        self._events_written += 1

    def write_failures(self, failures: list[str]) -> None:
        lines = ["# Benchmark Failures", ""]
        if not failures:
            lines.append("No benchmark failures recorded.")
        else:
            lines.extend(f"- {redact(failure)}" for failure in failures)
        self.failures_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def write_failure_case(self, iteration: int, seed: int, reason: str) -> Path:
        payload = {
            "iteration": iteration,
            "seed": seed,
            "reason": redact(reason),
            "run_id": self.run_id,
        }
        path = self.run_dir / f"failure-iteration-{iteration}.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.observe("loop.failure", payload)
        self.write_failures([reason])
        return path

    @property
    def event_count(self) -> int:
        if self.observations_path.exists():
            return len(self.observations_path.read_text(encoding="utf-8").splitlines())
        return self._events_written


def service_environment() -> dict[str, str]:
    keys = [
        "TRACEDB_HTTP_URL",
        "BENCH_POSTGRES_DSN",
        "BENCH_PGVECTOR_DSN",
        "BENCH_MONGO_URI",
        "BENCH_QDRANT_URL",
        "BENCH_OPENSEARCH_URL",
        "OPENROUTER_BASE_URL",
        "OPENROUTER_EMBED_MODEL",
        "OPENROUTER_COMPARE_EMBED_MODELS",
        "OPENROUTER_JUDGE_MODEL",
        "OPENROUTER_RERANK_MODEL",
    ]
    return {key: os.environ[key] for key in keys if key in os.environ}
