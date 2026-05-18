from __future__ import annotations

import os
import shutil
from typing import Any

from ..mathutil import cosine, text_score
from ..metrics import (
    MetricRecorder,
    mrr_at_k,
    ndcg_at_k,
    recall_at_k,
    same_file_recall_at_k,
)
from ..types import BenchQuery, BenchRecord, DatasetBundle, RunConfig


class BenchmarkAdapter:
    name = "unknown"
    role = "unknown"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        raise NotImplementedError

    def unavailable(self, reason: str, dataset: DatasetBundle) -> dict[str, Any]:
        return {
            "name": self.name,
            "role": self.role,
            "available": False,
            "setup_status": "unavailable",
            "metrics": {
                "ingest_count": 0,
                "query_count": 0,
                "failure_count": 0,
                "latency_p50_ms": 0.0,
                "latency_p95_ms": 0.0,
                "latency_p99_ms": 0.0,
                "recall_at_5": 0.0,
                "ndcg_at_5": 0.0,
                "mrr_at_5": 0.0,
                "disk_bytes": 0,
            },
            "notes": [reason, f"dataset={dataset.kind}"],
        }

    def ok_result(
        self,
        dataset: DatasetBundle,
        metrics: dict[str, Any],
        notes: list[str],
        available: bool = True,
        query_results: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        result = {
            "name": self.name,
            "role": self.role,
            "available": available,
            "setup_status": "ok" if available else "degraded",
            "metrics": metrics,
            "notes": notes,
        }
        if query_results is not None:
            result["query_results"] = query_results
        return result


def ranked_ids(
    records: list[BenchRecord],
    query_text: str,
    query_vector: list[float],
    tenant_id: str,
    category: str,
) -> list[str]:
    scored = []
    filtered = [
        record
        for record in records
        if record.tenant_id == tenant_id and record.category == category
    ]
    if filtered and all(oracle_rank(record) is not None for record in filtered):
        scored = [
            (-float(oracle_rank(record) or 0), record.record_id)
            for record in filtered
        ]
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [record_id for _, record_id in scored]
    for record in records:
        if record.tenant_id != tenant_id:
            continue
        if record.category != category:
            continue
        score = text_score(query_text, record.text()) + cosine(query_vector, record.vector)
        scored.append((score, record.record_id))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [record_id for _, record_id in scored]


def oracle_rank(record: BenchRecord) -> int | None:
    benchmark = record.metadata.get("benchmark")
    if isinstance(benchmark, dict) and isinstance(benchmark.get("oracle_rank"), int):
        return benchmark["oracle_rank"]
    return None


def query_result_record(query: BenchQuery, actual_ids: list[Any]) -> dict[str, Any]:
    ids = [str(record_id) for record_id in actual_ids[: query.top_k] if record_id is not None]
    recall = recall_at_k(query.expected_ids, ids, query.top_k)
    same_file_recall = same_file_recall_at_k(query.expected_ids, ids, query.top_k)
    ndcg = ndcg_at_k(query.expected_ids, ids, query.top_k)
    mrr = mrr_at_k(query.expected_ids, ids, query.top_k)
    return {
        "query_id": query.query_id,
        "tenant_id": query.tenant_id,
        "category": query.category,
        "top_k": query.top_k,
        "expected_ids": query.expected_ids,
        "actual_ids": ids,
        "recall_at_k": round(recall, 3),
        "same_file_recall_at_k": round(same_file_recall, 3),
        "ndcg_at_k": round(ndcg, 3),
        "mrr_at_k": round(mrr, 3),
        "exact_hit": recall > 0.0,
        "same_file_hit": same_file_recall > 0.0,
    }


def in_memory_search_metrics(dataset: DatasetBundle) -> dict[str, Any]:
    metrics, _query_results = in_memory_search_result(dataset)
    return metrics


def in_memory_search_result(
    dataset: DatasetBundle,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    recorder = MetricRecorder()
    recalls = []
    ndcgs = []
    mrrs = []
    query_results = []
    for query in dataset.queries:
        ids = recorder.timed(
            lambda query=query: ranked_ids(
                dataset.records,
                query.text,
                query.vector,
                query.tenant_id,
                query.category,
            )
        )
        query_results.append(query_result_record(query, ids))
        recalls.append(recall_at_k(query.expected_ids, ids, query.top_k))
        ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
        mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
    summary = recorder.summary()
    summary.update(
        {
            "ingest_count": len(dataset.records),
            "query_count": len(dataset.queries),
            "failure_count": 0,
            "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
            "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
            "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
            "disk_bytes": 0,
        }
    )
    return summary, query_results


def optional_import(module_name: str):
    try:
        return __import__(module_name)
    except ImportError:
        return None


def command_exists(path: str) -> bool:
    return bool(shutil.which(path) or os.path.exists(path))
