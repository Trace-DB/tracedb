from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .types import DatasetBundle, RunConfig


def query_latency_metric(metrics: dict[str, Any], percentile: int) -> tuple[Any | None, str]:
    return _preferred_metric(
        metrics,
        f"query_latency_p{percentile}_ms",
        f"latency_p{percentile}_ms",
    )


def build_report(
    dataset: DatasetBundle,
    config: RunConfig,
    baselines: list[dict[str, Any]],
    openrouter: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scenarios = simulated_scenarios(dataset, config, openrouter or {})
    control_ledger = build_control_ledger(baselines)
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
        "control": {
            "control_status": control_ledger["control_status"],
            "number_to_beat": control_ledger["number_to_beat"],
        },
        "control_status": control_ledger["control_status"],
        "control_ledger": control_ledger,
        "number_to_beat": control_ledger["number_to_beat"],
        "summary": {
            "baseline_count": len(baselines),
            "available_count": sum(1 for baseline in baselines if baseline["available"]),
            "unavailable_count": sum(1 for baseline in baselines if not baseline["available"]),
            "record_count": len(dataset.records),
            "query_count": len(dataset.queries),
            "control_status": control_ledger["control_status"],
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
        f"- Control status: `{report.get('control_status', 'unknown')}`",
        "",
        "## Control Ledger",
        "",
        _control_status_sentence(report),
        "",
        "### Number to beat",
        "",
        "| metric | baseline | value |",
        "| --- | --- | ---: |",
        *_number_to_beat_rows(report.get("number_to_beat", {})),
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
        "| baseline | available | role | ingest | ingest txns | ingest txn total ms | queries | p50 ms | p95 ms | p99 ms | ingest p95 ms | query p95 ms | admin p95 ms | recall@5 | same-file recall@5 | span gaps | nDCG@5 | MRR@5 | notes |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for baseline in report["baselines"]:
        metrics = baseline["metrics"]
        notes = "; ".join(baseline["notes"]).replace("|", "\\|")
        lines.append(
            "| {name} | {available} | {role} | {ingest} | {ingest_txns} | {ingest_txn_total} | {queries} | {p50} | {p95} | {p99} | {ingest_p95} | {query_p95} | {admin_p95} | {recall} | {same_file_recall} | {span_gaps} | {ndcg} | {mrr} | {notes} |".format(
                name=baseline["name"],
                available="yes" if baseline["available"] else "no",
                role=baseline["role"],
                ingest=metrics.get("ingest_count", 0),
                ingest_txns=metrics.get("ingest_transaction_count", "n/a"),
                ingest_txn_total=metrics.get(
                    "ingest_transaction_total_latency_ms", "n/a"
                ),
                queries=metrics.get("query_count", 0),
                p50=_display_query_latency(metrics, 50),
                p95=_display_query_latency(metrics, 95),
                p99=_display_query_latency(metrics, 99),
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


def build_control_ledger(baselines: list[dict[str, Any]]) -> dict[str, Any]:
    external = [baseline for baseline in baselines if not is_tracedb_baseline(baseline)]
    available_external = [baseline for baseline in external if baseline.get("available")]
    unavailable_external = [baseline for baseline in external if not baseline.get("available")]
    if available_external:
        control_status = "external_control_available"
    elif unavailable_external:
        control_status = "external_control_unavailable"
    else:
        control_status = "internal_only_smoke"
    return {
        "control_status": control_status,
        "external_controls": [_control_entry(baseline) for baseline in external],
        "available_external_controls": [
            _control_entry(baseline) for baseline in available_external
        ],
        "unavailable_external_controls": [
            _control_entry(baseline) for baseline in unavailable_external
        ],
        "number_to_beat": {
            "query_p95_ms": _best_metric(
                available_external,
                ("query_latency_p95_ms", "latency_p95_ms"),
                lower_is_better=True,
                require_queries=True,
            ),
            "recall_at_5": _best_metric(
                available_external,
                "recall_at_5",
                lower_is_better=False,
                require_queries=True,
            ),
            "ingest_p95_ms": _best_metric(
                available_external,
                "ingest_latency_p95_ms",
                lower_is_better=True,
            ),
            "ingest_transaction_total_ms": _best_metric(
                available_external,
                "ingest_transaction_total_latency_ms",
                lower_is_better=True,
                require_positive=True,
            ),
            "storage_bytes": _best_metric(
                available_external,
                "disk_bytes",
                lower_is_better=True,
                require_positive=True,
            ),
        },
    }


def is_tracedb_baseline(baseline: dict[str, Any]) -> bool:
    return str(baseline.get("name", "")).lower() in {"tracedb", "trace_db", "trace-db"}


def _control_entry(baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": baseline.get("name", "unknown"),
        "role": baseline.get("role", "unknown"),
        "available": bool(baseline.get("available")),
        "notes": baseline.get("notes", []),
    }


def _best_metric(
    baselines: list[dict[str, Any]],
    metric: str | tuple[str, ...],
    *,
    lower_is_better: bool,
    require_queries: bool = False,
    require_positive: bool = False,
) -> dict[str, Any]:
    candidates = []
    for baseline in baselines:
        metrics = baseline.get("metrics", {})
        if require_queries and int(metrics.get("query_count", 0)) <= 0:
            continue
        value, source_metric = _preferred_metric(metrics, *_metric_names(metric))
        if value is None:
            continue
        numeric = float(value)
        if require_positive and numeric <= 0:
            continue
        candidates.append(
            (
                numeric,
                baseline.get("name", "unknown"),
                baseline.get("scenario_id"),
                baseline.get("artifact_dir"),
                source_metric,
            )
        )
    if not candidates:
        return {"baseline": None, "value": None, "source_metric": _metric_names(metric)[0]}
    value, name, scenario_id, artifact_dir, source_metric = (min if lower_is_better else max)(
        candidates, key=lambda item: item[0]
    )
    result = {"baseline": name, "value": value, "source_metric": source_metric}
    if scenario_id is not None:
        result["scenario_id"] = scenario_id
    if artifact_dir is not None:
        result["artifact_dir"] = artifact_dir
    return result


def _metric_names(metric: str | tuple[str, ...]) -> tuple[str, ...]:
    return metric if isinstance(metric, tuple) else (metric,)


def _preferred_metric(metrics: dict[str, Any], *names: str) -> tuple[Any | None, str]:
    for name in names:
        if name in metrics:
            return metrics[name], name
    return None, names[0]


def _display_query_latency(metrics: dict[str, Any], percentile: int) -> Any:
    value, _source = query_latency_metric(metrics, percentile)
    return value if value is not None else 0.0


def _control_status_sentence(report: dict[str, Any]) -> str:
    status = report.get("control_status", "unknown")
    if status == "external_control_available":
        return "At least one external control produced metrics; this run has a number to beat."
    if status == "external_control_unavailable":
        return "External controls were requested but unavailable; no product-language conclusion is valid until a control produces metrics."
    if status == "internal_only_smoke":
        return "This TraceDB-only run is development evidence, not product evidence."
    return "Control status is unknown; do not promote this report as product evidence."


def _number_to_beat_rows(number_to_beat: dict[str, Any]) -> list[str]:
    rows = []
    for metric, entry in number_to_beat.items():
        baseline = entry.get("baseline") if isinstance(entry, dict) else None
        value = entry.get("value") if isinstance(entry, dict) else None
        rows.append(f"| {metric} | {baseline or 'n/a'} | {value if value is not None else 'n/a'} |")
    return rows


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
