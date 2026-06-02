from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

from ..http import request_json
from ..metrics import MetricRecorder, mrr_at_k, ndcg_at_k, recall_at_k
from ..types import DatasetBundle, RunConfig
from .base import BenchmarkAdapter

# Number of warmup query iterations before measurement begins.
WARMUP_ITERATIONS = 3


class QdrantAdapter(BenchmarkAdapter):
    name = "qdrant"
    role = "vector-native payload-filtered search"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        base_url = os.environ.get("BENCH_QDRANT_URL", "http://localhost:26333").rstrip(
            "/"
        )
        collection = f"tracedb_bench_{uuid.uuid4().hex[:8]}"
        try:
            setup_recorder = MetricRecorder()
            ingest_recorder = MetricRecorder()
            query_recorder = MetricRecorder()
            setup_recorder.timed(
                lambda: request_json(
                    "PUT",
                    f"{base_url}/collections/{collection}",
                    {
                        "vectors": {
                            "size": dataset.embedding_dimensions,
                            "distance": "Cosine",
                        }
                    },
                )
            )
            if dataset.records:
                batch_size = int(os.environ.get("BENCH_QDRANT_BATCH_SIZE", "64"))
                for start in range(0, len(dataset.records), batch_size):
                    points = [
                        {
                            "id": idx,
                            "vector": record.vector,
                            "payload": {
                                "record_id": record.record_id,
                                "tenant_id": record.tenant_id,
                                "category": record.category,
                                "body": record.body,
                            },
                        }
                        for idx, record in enumerate(
                            dataset.records[start : start + batch_size],
                            start=start,
                        )
                    ]
                    ingest_recorder.timed(
                        lambda points=points: request_json(
                            "PUT",
                            f"{base_url}/collections/{collection}/points?wait=true",
                            {"points": points},
                            timeout=60,
                        )
                    )
            disk_bytes_after_ingest = _directory_bytes(
                os.environ.get("BENCH_QDRANT_STORAGE_DIR")
            )
            # Warmup phase: run a few query iterations (discarded from
            # measurements) to stabilize filesystem cache and connection pooling.
            if dataset.queries:
                warmup_query = dataset.queries[0]
                for _ in range(WARMUP_ITERATIONS):
                    request_json(
                        "POST",
                        f"{base_url}/collections/{collection}/points/search",
                        {
                            "vector": warmup_query.vector,
                            "limit": warmup_query.top_k,
                            "with_payload": True,
                            "filter": {
                                "must": [
                                    {
                                        "key": "tenant_id",
                                        "match": {"value": warmup_query.tenant_id},
                                    },
                                    {
                                        "key": "category",
                                        "match": {"value": warmup_query.category},
                                    },
                                ]
                            },
                        },
                    )
            recalls = []
            ndcgs = []
            mrrs = []
            for query in dataset.queries:
                result = query_recorder.timed(
                    lambda query=query: request_json(
                        "POST",
                        f"{base_url}/collections/{collection}/points/search",
                        {
                            "vector": query.vector,
                            "limit": query.top_k,
                            "with_payload": True,
                            "filter": {
                                "must": [
                                    {
                                        "key": "tenant_id",
                                        "match": {"value": query.tenant_id},
                                    },
                                    {
                                        "key": "category",
                                        "match": {"value": query.category},
                                    },
                                ]
                            },
                        },
                    )
                )
                ids = [
                    point.get("payload", {}).get("record_id")
                    for point in result.get("result", [])
                ]
                recalls.append(recall_at_k(query.expected_ids, ids, query.top_k))
                ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
                mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
            disk_bytes_after_workload = _directory_bytes(
                os.environ.get("BENCH_QDRANT_STORAGE_DIR")
            )
            query_summary = query_recorder.summary()
            ingest_summary = ingest_recorder.summary()
            setup_summary = setup_recorder.summary()
            ingest_transaction_total_ms = round(sum(ingest_recorder.latencies_ms), 3)
            metrics = dict(query_summary)
            metrics.update(
                {f"query_{key}": value for key, value in query_summary.items()}
            )
            metrics.update(
                {f"ingest_{key}": value for key, value in ingest_summary.items()}
            )
            metrics.update(
                {f"setup_{key}": value for key, value in setup_summary.items()}
            )
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "ingest_transaction_count": 1 if dataset.records else 0,
                    "ingest_transaction_total_latency_ms": ingest_transaction_total_ms,
                    "query_count": len(dataset.queries),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3)
                    if recalls
                    else 0.0,
                    "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
                    "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
                    "disk_bytes": disk_bytes_after_ingest,
                    "disk_bytes_after_ingest": disk_bytes_after_ingest,
                    "disk_bytes_after_workload": disk_bytes_after_workload,
                }
            )
            return self.ok_result(
                dataset,
                metrics,
                [
                    "real Qdrant vector payload-filter workload executed through REST API",
                    "Qdrant ingestion used batched point upserts to avoid oversized REST payloads",
                    "Qdrant storage bytes measured from BENCH_QDRANT_STORAGE_DIR when Modal starts the local control service",
                ],
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"qdrant unavailable: {error}", dataset)


def _directory_bytes(path: str | None) -> int:
    if not path:
        return 0
    root = Path(path)
    if not root.exists():
        return 0
    total = 0
    for item in root.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:
            continue
    return total
