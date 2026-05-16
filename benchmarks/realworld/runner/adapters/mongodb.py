from __future__ import annotations

import os
from typing import Any

from .base import BenchmarkAdapter, optional_import
from ..metrics import MetricRecorder, mrr_at_k, ndcg_at_k, recall_at_k
from ..types import DatasetBundle, RunConfig


class MongoAdapter(BenchmarkAdapter):
    name = "mongodb"
    role = "nested sparse document database"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        pymongo = optional_import("pymongo")
        if pymongo is None:
            return self.unavailable("python dependency missing: pymongo", dataset)
        uri = os.environ.get("BENCH_MONGO_URI", "mongodb://localhost:27027")
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            collection = client.tracedb_bench.records
            collection.delete_many({})
            if dataset.records:
                collection.insert_many([record.to_json() for record in dataset.records])
            collection.create_index([("tenant_id", 1), ("category", 1)])
            recorder = MetricRecorder()
            recalls = []
            ndcgs = []
            mrrs = []
            for query in dataset.queries:
                rows = recorder.timed(
                    lambda query=query: list(
                        collection.find(
                            {
                                "tenant_id": query.tenant_id,
                                "category": query.category,
                                "metadata.nested.priority": {"$exists": True},
                            },
                            {"id": 1},
                        )
                        .sort([("rating", -1), ("id", 1)])
                        .limit(query.top_k)
                    )
                )
                ids = [row["id"] for row in rows]
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
                ["real MongoDB nested sparse document workload executed through pymongo"],
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"mongodb unavailable: {error}", dataset)
