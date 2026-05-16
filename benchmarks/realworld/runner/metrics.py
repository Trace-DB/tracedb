from __future__ import annotations

import time
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

T = TypeVar("T")


@dataclass
class MetricRecorder:
    latencies_ms: list[float] = field(default_factory=list)

    def timed(self, operation: Callable[[], T]) -> T:
        started = time.perf_counter()
        try:
            return operation()
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self.latencies_ms.append(elapsed_ms)

    def summary(self) -> dict[str, float]:
        return {
            "latency_p50_ms": round(percentile(self.latencies_ms, 50), 3),
            "latency_p95_ms": round(percentile(self.latencies_ms, 95), 3),
            "latency_p99_ms": round(percentile(self.latencies_ms, 99), 3),
        }


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (pct / 100.0)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def recall_at_k(expected_ids: list[str], actual_ids: list[str], k: int) -> float:
    if not expected_ids:
        return 0.0
    expected = set(expected_ids)
    actual = set(actual_ids[:k])
    return len(expected & actual) / len(expected)


def ndcg_at_k(expected_ids: list[str], actual_ids: list[str], k: int) -> float:
    if not expected_ids:
        return 0.0
    relevance = {record_id: 1.0 for record_id in expected_ids}
    dcg = 0.0
    for rank, record_id in enumerate(actual_ids[:k], start=1):
        gain = relevance.get(record_id, 0.0)
        if gain:
            dcg += gain / math.log2(rank + 1)
    ideal_count = min(len(expected_ids), k)
    ideal = sum(1.0 / math.log2(rank + 1) for rank in range(1, ideal_count + 1))
    return dcg / ideal if ideal else 0.0


def mrr_at_k(expected_ids: list[str], actual_ids: list[str], k: int) -> float:
    if not expected_ids:
        return 0.0
    expected = set(expected_ids)
    for rank, record_id in enumerate(actual_ids[:k], start=1):
        if record_id in expected:
            return 1.0 / rank
    return 0.0
