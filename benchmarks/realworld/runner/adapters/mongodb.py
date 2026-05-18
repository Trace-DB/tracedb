from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .base import BenchmarkAdapter, optional_import, query_result_record
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
        client = None
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            database = client.tracedb_bench
            collection = database.records
            setup_recorder = MetricRecorder()
            ingest_recorder = MetricRecorder()
            query_recorder = MetricRecorder()
            setup_recorder.timed(lambda: _setup_collection(collection))
            if dataset.records:
                ingest_recorder.timed(
                    lambda: collection.insert_many([record.to_json() for record in dataset.records])
                )
            disk_bytes_after_ingest = _storage_bytes(database)
            recalls = []
            ndcgs = []
            mrrs = []
            query_results = []
            for query in dataset.queries:
                rows = query_recorder.timed(
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
                query_results.append(query_result_record(query, ids))
                recalls.append(recall_at_k(query.expected_ids, ids, query.top_k))
                ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
                mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
            disk_bytes_after_workload = _storage_bytes(database)
            query_summary = query_recorder.summary()
            ingest_summary = ingest_recorder.summary()
            setup_summary = setup_recorder.summary()
            metrics = dict(query_summary)
            metrics.update({f"query_{key}": value for key, value in query_summary.items()})
            metrics.update({f"ingest_{key}": value for key, value in ingest_summary.items()})
            metrics.update({f"setup_{key}": value for key, value in setup_summary.items()})
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "ingest_transaction_count": 1 if dataset.records else 0,
                    "ingest_transaction_total_latency_ms": round(
                        sum(ingest_recorder.latencies_ms), 3
                    ),
                    "query_count": len(dataset.queries),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
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
                    "real MongoDB nested sparse document workload executed through pymongo",
                    "MongoDB storage bytes measured with dbStats, falling back to BENCH_MONGO_STORAGE_DIR when available",
                ],
                query_results=query_results,
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"mongodb unavailable: {error}", dataset)
        finally:
            if client is not None:
                close = getattr(client, "close", None)
                if close is not None:
                    close()


def _setup_collection(collection: Any) -> None:
    collection.delete_many({})
    collection.create_index([("tenant_id", 1), ("category", 1)])


def _storage_bytes(database: Any) -> int:
    db_stats_bytes = 0
    try:
        stats = database.command("dbStats")
        db_stats_bytes = int(stats.get("storageSize") or stats.get("dataSize") or 0)
    except Exception:
        pass
    directory_bytes = _directory_bytes(os.environ.get("BENCH_MONGO_STORAGE_DIR"))
    return directory_bytes or db_stats_bytes


def _directory_bytes(path_text: str | None) -> int:
    if not path_text:
        return 0
    root = Path(path_text)
    if not root.exists():
        return 0
    total = 0
    for path in root.rglob("*"):
        if path.is_file():
            try:
                total += path.stat().st_size
            except OSError:
                continue
    return total
