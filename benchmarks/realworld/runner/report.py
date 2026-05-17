from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .types import DatasetBundle, RunConfig


def build_report(
    dataset: DatasetBundle,
    config: RunConfig,
    baselines: list[dict[str, Any]],
    openrouter: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scenarios = simulated_scenarios(dataset, config, openrouter or {})
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": config.profile,
        "run_id": config.run_id,
        "dataset": {
            "kind": dataset.kind,
            "source": dataset.source,
            "records": len(dataset.records),
            "queries": len(dataset.queries),
            "notes": dataset.notes,
            "digest": dataset.digest,
            "embedding_model": dataset.embedding_model,
            "embedding_dimensions": dataset.embedding_dimensions,
            "embedding_source": dataset.embedding_source,
            "relevance_label_mode": dataset.relevance_label_mode,
            "relevance_label_scope": dataset.relevance_label_scope,
            "relevance_label_notes": dataset.relevance_label_notes,
        },
        "surfaces": config.surfaces,
        "openrouter": openrouter or {},
        "scenarios": scenarios,
        "summary": {
            "baseline_count": len(baselines),
            "available_count": sum(1 for baseline in baselines if baseline["available"]),
            "unavailable_count": sum(1 for baseline in baselines if not baseline["available"]),
            "record_count": len(dataset.records),
            "query_count": len(dataset.queries),
            "failure_count": sum(
                int(baseline["metrics"].get("failure_count", 0)) for baseline in baselines
            ),
        },
        "baselines": baselines,
    }


def write_json(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# TraceDB Real-World Benchmark Report",
        "",
        f"- Profile: `{report['profile']}`",
        f"- Run ID: `{report.get('run_id', '')}`",
        f"- Dataset: `{report['dataset']['kind']}` from `{report['dataset']['source']}`",
        f"- Records: `{report['summary']['record_count']}`",
        f"- Queries: `{report['summary']['query_count']}`",
        f"- Embeddings: `{report['dataset'].get('embedding_model', 'unknown')}` ({report['dataset'].get('embedding_dimensions', 0)} benchmark dimensions, {report['dataset'].get('embedding_source', 'unknown')})",
        f"- Relevance labels: `{report['dataset'].get('relevance_label_mode', 'unspecified')}` (`{report['dataset'].get('relevance_label_scope', 'unknown')}`)",
        f"- Provider-native dimensions: `{report.get('openrouter', {}).get('provider_native_embedding_dimensions', report['dataset'].get('embedding_dimensions', 0))}`",
        f"- Requested embedding dimensions: `{report.get('openrouter', {}).get('requested_embedding_dimensions') or 'native'}`",
        f"- Surfaces: {', '.join(report['surfaces'])}",
        f"- OpenRouter requests: `{report.get('openrouter', {}).get('request_count', 0)}`",
        "",
        "## Simulated Scenarios",
        "",
        *[
            "- `{name}`: {description} Metrics: {metrics}.".format(
                name=scenario["name"],
                description=scenario["description"],
                metrics=", ".join(scenario["metrics"]),
            )
            for scenario in report.get("scenarios", [])
        ],
        "",
        "## Provider Metrics",
        "",
        "| provider surface | model | requests | cache hits | cache misses | tokens | search units | recall@5 | nDCG@5 | MRR@5 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        provider_metrics_row(report),
        "",
        "| baseline | available | role | ingest | queries | p50 ms | p95 ms | p99 ms | ingest p95 ms | query p95 ms | admin p95 ms | recall@5 | same-file recall@5 | span gaps | nDCG@5 | MRR@5 | notes |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for baseline in report["baselines"]:
        metrics = baseline["metrics"]
        notes = "; ".join(baseline["notes"]).replace("|", "\\|")
        lines.append(
            "| {name} | {available} | {role} | {ingest} | {queries} | {p50} | {p95} | {p99} | {ingest_p95} | {query_p95} | {admin_p95} | {recall} | {same_file_recall} | {span_gaps} | {ndcg} | {mrr} | {notes} |".format(
                name=baseline["name"],
                available="yes" if baseline["available"] else "no",
                role=baseline["role"],
                ingest=metrics.get("ingest_count", 0),
                queries=metrics.get("query_count", 0),
                p50=metrics.get("latency_p50_ms", 0.0),
                p95=metrics.get("latency_p95_ms", 0.0),
                p99=metrics.get("latency_p99_ms", 0.0),
                ingest_p95=metrics.get("ingest_latency_p95_ms", "n/a"),
                query_p95=metrics.get("query_latency_p95_ms", "n/a"),
                admin_p95=metrics.get("admin_latency_p95_ms", "n/a"),
                recall=metrics.get("recall_at_5", 0.0),
                same_file_recall=metrics.get("same_file_recall_at_5", "n/a"),
                span_gaps=metrics.get("span_gap_count", 0),
                ndcg=metrics.get("ndcg_at_5", 0.0),
                mrr=metrics.get("mrr_at_5", 0.0),
                notes=notes,
            )
        )
    lines.extend(
        [
            "",
            "## Dataset Notes",
            "",
            *[f"- {note}" for note in report["dataset"]["notes"]],
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def provider_metrics_row(report: dict[str, Any]) -> str:
    openrouter = report.get("openrouter", {})
    rerank = openrouter.get("rerank_metrics") if isinstance(openrouter, dict) else {}
    if not isinstance(rerank, dict):
        rerank = {}
    model = rerank.get("model") or openrouter.get("rerank_model") or "n/a"
    return (
        "| openrouter embeddings+rerank | {model} | {requests} | {hits} | {misses} | "
        "{tokens} | {search_units} | {recall} | {ndcg} | {mrr} |"
    ).format(
        model=model,
        requests=openrouter.get("request_count", 0),
        hits=openrouter.get("cache_hits", 0),
        misses=openrouter.get("cache_misses", 0),
        tokens=openrouter.get("total_tokens", 0),
        search_units=openrouter.get("search_units", 0),
        recall=rerank.get("recall_at_5", 0.0),
        ndcg=rerank.get("ndcg_at_5", 0.0),
        mrr=rerank.get("mrr_at_5", 0.0),
    )


def simulated_scenarios(
    dataset: DatasetBundle,
    config: RunConfig,
    openrouter: dict[str, Any],
) -> list[dict[str, Any]]:
    scenarios = [
        {
            "name": "tenant_filtered_semantic_search",
            "description": (
                "Each query is scoped to one tenant and one workload category so a database "
                "must retrieve relevant records without crossing tenant boundaries."
            ),
            "metrics": ["recall_at_5", "ndcg_at_5", "mrr_at_5", "latency_p50_ms", "latency_p95_ms"],
        },
        {
            "name": "document_relational_shape_mix",
            "description": (
                "Records include scalar filters, sparse nested metadata, text bodies, and vectors "
                "to exercise PostgreSQL, MongoDB, vector, and lexical-search database strengths."
            ),
            "metrics": ["ingest_count", "query_count", "failure_count"],
        },
        {
            "name": "api_surface_coverage",
            "description": (
                "TraceDB is exercised through the requested SDK, CLI, HTTP, and curl-equivalent "
                "surfaces instead of only a health endpoint."
            ),
            "metrics": ["failure_count", "latency_p50_ms", "latency_p99_ms"],
        },
    ]
    if openrouter.get("enabled"):
        scenarios.append(
            {
                "name": "openrouter_embedding_model_comparison",
                "description": (
                    "Documents and queries are embedded with OpenRouter, using the primary model "
                    "for benchmark vectors and optional comparison models for provider sanity checks."
                ),
                "metrics": [
                    "openrouter.request_count",
                    "openrouter.embedding_request_count",
                    "openrouter.cache_hits",
                    "openrouter.cache_misses",
                    "embedding_dimensions",
                ],
            }
        )
    rerank_metrics = openrouter.get("rerank_metrics")
    if isinstance(rerank_metrics, dict) and rerank_metrics:
        scenarios.append(
            {
                "name": "rag_retrieve_then_rerank",
                "description": (
                    "A retrieve-and-rerank RAG stage sends the top candidate documents to "
                    f"`{rerank_metrics.get('model', 'configured reranker')}` and measures precision lift "
                    "against the same relevance labels as the database baselines."
                ),
                "metrics": [
                    "openrouter.rerank_request_count",
                    "openrouter.rerank_metrics.recall_at_5",
                    "openrouter.rerank_metrics.ndcg_at_5",
                    "openrouter.rerank_metrics.mrr_at_5",
                ],
            }
        )
    if "http" in config.surfaces or "curl" in config.surfaces:
        scenarios.append(
            {
                "name": "tracedb_http_falsification",
                "description": (
                    "The TraceDB HTTP path verifies fresh writes, patch visibility, tenant isolation, "
                    "strict/lazy/allow-dirty freshness modes, explain fields, compaction, snapshot/restore, "
                    "and tombstone hiding."
                ),
                "metrics": ["failure_count", "tracedb.explain.returned_count", "latency_p95_ms"],
            }
        )
    if dataset.embedding_source == "deterministic":
        scenarios.append(
            {
                "name": "offline_reproducible_control",
                "description": (
                    "The generated dataset uses deterministic labels and fixed vectors "
                    "so CI can reproduce benchmark behavior without provider access. "
                    "For generated oracle-rank labels, read recall as operational-smoke evidence, "
                    "not definitive hybrid retrieval quality."
                ),
                "metrics": [
                    "dataset.digest",
                    "dataset.relevance_label_mode",
                    "dataset.relevance_label_scope",
                    "recall_at_5",
                ],
            }
        )
    return scenarios
