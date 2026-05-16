from __future__ import annotations

import os
import uuid
from typing import Any

from .base import BenchmarkAdapter
from ..http import request_json
from ..metrics import MetricRecorder, mrr_at_k, ndcg_at_k, recall_at_k
from ..types import DatasetBundle, RunConfig


class QdrantAdapter(BenchmarkAdapter):
    name = "qdrant"
    role = "vector-native payload-filtered search"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        base_url = os.environ.get("BENCH_QDRANT_URL", "http://localhost:26333").rstrip("/")
        collection = f"tracedb_bench_{uuid.uuid4().hex[:8]}"
        try:
            request_json(
                "PUT",
                f"{base_url}/collections/{collection}",
                {
                    "vectors": {
                        "size": dataset.embedding_dimensions,
                        "distance": "Cosine",
                    }
                },
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
                    request_json(
                        "PUT",
                        f"{base_url}/collections/{collection}/points?wait=true",
                        {"points": points},
                        timeout=60,
                    )
            recorder = MetricRecorder()
            recalls = []
            ndcgs = []
            mrrs = []
            for query in dataset.queries:
                result = recorder.timed(
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
            metrics = recorder.summary()
            metrics.update(
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
            return self.ok_result(
                dataset,
                metrics,
                [
                    "real Qdrant vector payload-filter workload executed through REST API",
                    "Qdrant ingestion used batched point upserts to avoid oversized REST payloads",
                ],
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"qdrant unavailable: {error}", dataset)
