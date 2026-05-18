from __future__ import annotations

import json
import os
import uuid
import urllib.request
from pathlib import Path
from typing import Any

from .base import BenchmarkAdapter, query_result_record
from ..http import request_json
from ..metrics import MetricRecorder, mrr_at_k, ndcg_at_k, recall_at_k
from ..types import DatasetBundle, RunConfig


class OpenSearchAdapter(BenchmarkAdapter):
    name = "opensearch"
    role = "lexical full-text search"

    def run(self, dataset: DatasetBundle, config: RunConfig) -> dict[str, Any]:
        base_url = os.environ.get("BENCH_OPENSEARCH_URL", "http://localhost:29200").rstrip("/")
        index = f"tracedb-bench-{uuid.uuid4().hex[:8]}"
        try:
            request_json(
                "PUT",
                f"{base_url}/{index}",
                {
                    "mappings": {
                        "properties": {
                            "record_id": {"type": "keyword"},
                            "tenant_id": {"type": "keyword"},
                            "category": {"type": "keyword"},
                            "body": {"type": "text"},
                        }
                    }
                },
            )
            if dataset.records:
                bulk_lines = []
                for record in dataset.records:
                    bulk_lines.append(json.dumps({"index": {"_index": index, "_id": record.record_id}}))
                    bulk_lines.append(
                        json.dumps(
                            {
                                "record_id": record.record_id,
                                "tenant_id": record.tenant_id,
                                "category": record.category,
                                "body": record.body,
                            }
                        )
                    )
                request = urllib.request.Request(
                    f"{base_url}/_bulk?refresh=true",
                    data=("\n".join(bulk_lines) + "\n").encode("utf-8"),
                    method="POST",
                    headers={"content-type": "application/x-ndjson"},
                )
                with urllib.request.urlopen(request, timeout=10) as response:
                    response.read()
            recorder = MetricRecorder()
            recalls = []
            ndcgs = []
            mrrs = []
            query_results = []
            for query in dataset.queries:
                result = recorder.timed(
                    lambda query=query: request_json(
                        "POST",
                        f"{base_url}/{index}/_search",
                        {
                            "size": query.top_k,
                            "query": {
                                "bool": {
                                    "must": [{"match": {"body": query.text}}],
                                    "filter": [
                                        {"term": {"tenant_id": query.tenant_id}},
                                        {"term": {"category": query.category}},
                                    ],
                                }
                            },
                        },
                    )
                )
                ids = [
                    hit.get("_source", {}).get("record_id")
                    for hit in result.get("hits", {}).get("hits", [])
                ]
                query_results.append(query_result_record(query, ids))
                recalls.append(recall_at_k(query.expected_ids, ids, query.top_k))
                ndcgs.append(ndcg_at_k(query.expected_ids, ids, query.top_k))
                mrrs.append(mrr_at_k(query.expected_ids, ids, query.top_k))
            metrics = recorder.summary()
            disk_bytes = _directory_size(os.environ.get("BENCH_OPENSEARCH_STORAGE_DIR"))
            metrics.update(
                {
                    "ingest_count": len(dataset.records),
                    "query_count": len(dataset.queries),
                    "failure_count": 0,
                    "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
                    "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
                    "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
                    "disk_bytes": disk_bytes,
                    "disk_bytes_after_ingest": disk_bytes,
                    "disk_bytes_after_workload": disk_bytes,
                }
            )
            return self.ok_result(
                dataset,
                metrics,
                ["real OpenSearch BM25 tenant-filtered workload executed through REST API"],
                query_results=query_results,
            )
        except Exception as error:
            if config.require_services:
                raise
            return self.unavailable(f"opensearch unavailable: {error}", dataset)


def _directory_size(path_text: str | None) -> int:
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
