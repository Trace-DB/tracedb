from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .report import build_control_ledger, is_tracedb_baseline, query_latency_metric


ROLE_DESCRIPTIONS = {
    "TraceDB": (
        "unified record, lexical, vector, freshness, snapshot, and API-surface target under test"
    ),
    "PostgreSQL": "relational baseline for scalar filters, updates, scans, and normalized lookup shape",
    "PostgreSQL+pgvector": "relational plus vector baseline for metadata-filtered semantic retrieval",
    "MongoDB": "document-store baseline for nested JSON, sparse fields, and document-shaped updates",
    "Qdrant": "vector-native baseline for approximate nearest neighbor plus payload filters",
    "OpenSearch": "lexical/search baseline for BM25-style text retrieval and search ranking",
    "Milvus": "vector-native baseline for scalar-filtered dense-vector retrieval",
}


@dataclass(frozen=True)
class ScenarioSpec:
    scenario_id: str
    name: str
    description: str
    hypothesis: str
    target: str
    surface: str
    pass_criteria: list[str]


SCENARIOS = {
    "sdk_cli_surface": ScenarioSpec(
        scenario_id="sdk_cli_surface",
        name="TraceDB embedded/SDK/CLI usability",
        description=(
            "Exercises TraceDB without a network service: request-builder semantics, CLI init, "
            "schema apply, put, and get against a local directory."
        ),
        hypothesis=(
            "TraceDB should behave like a lightweight embeddable database path where application "
            "code and CLI users can create, write, and read records without a service dependency."
        ),
        target="tracedb",
        surface="sdk,cli",
        pass_criteria=[
            "TraceDB baseline is available",
            "failure_count is zero",
            "recall_at_5, ndcg_at_5, and mrr_at_5 are populated",
        ],
    ),
    "http_falsification": ScenarioSpec(
        scenario_id="http_falsification",
        name="TraceDB HTTP durability and correctness falsification",
        description=(
            "Runs TraceDB through HTTP/curl-equivalent paths and verifies fresh writes, patch "
            "visibility, tenant isolation, freshness modes, explain output, compaction, snapshot, "
            "restore, and tombstone hiding."
        ),
        hypothesis=(
            "TraceDB's service API should expose real database behavior rather than only health "
            "checks or in-memory benchmark scoring."
        ),
        target="tracedb",
        surface="http,curl",
        pass_criteria=[
            "HTTP surface is available",
            "TraceDB falsification note is present",
            "failure_count is zero",
        ],
    ),
    "search_rag_6": ScenarioSpec(
        scenario_id="search_rag_6",
        name="Side-by-side Search/RAG 6 database comparison",
        description=(
            "Compares TraceDB, PostgreSQL, pgvector, MongoDB, Qdrant, OpenSearch, and Milvus on the same "
            "tenant-filtered generated RAG corpus with text, vector, scalar, and nested metadata."
        ),
        hypothesis=(
            "TraceDB should be measurable beside the default database stack across relational, "
            "document, lexical, vector, and hybrid retrieval behavior."
        ),
        target="all",
        surface="sdk,cli,http,curl",
        pass_criteria=[
            "All configured services either report metrics or clear unavailable reasons",
            "TraceDB remains available",
            "Comparison table includes all configured baselines",
        ],
    ),
}


def selected_scenarios(value: str) -> list[ScenarioSpec]:
    if value == "all":
        return list(SCENARIOS.values())
    specs = []
    for item in [part.strip() for part in value.split(",") if part.strip()]:
        if item not in SCENARIOS:
            raise SystemExit(
                f"unknown scenario {item}; expected one of {', '.join(sorted(SCENARIOS))}"
            )
        specs.append(SCENARIOS[item])
    return specs


def build_suite_report(
    *,
    suite_id: str,
    profile: str,
    dataset: str,
    records: int,
    reports: list[dict[str, Any]],
) -> dict[str, Any]:
    scenarios = []
    all_baselines = []
    for item in reports:
        spec: ScenarioSpec = item["spec"]
        report = item["report"]
        artifact_dir = item["artifact_dir"]
        all_baselines.extend(
            _baselines_with_context(
                report["baselines"],
                scenario_id=spec.scenario_id,
                artifact_dir=artifact_dir,
            )
        )
        scenarios.append(
            {
                "id": spec.scenario_id,
                "name": spec.name,
                "description": spec.description,
                "hypothesis": spec.hypothesis,
                "pass_criteria": spec.pass_criteria,
                "artifact_dir": artifact_dir,
                "summary": report["summary"],
                "dataset": report["dataset"],
                "surfaces": report["surfaces"],
                "openrouter": report.get("openrouter", {}),
                "control": report.get("control", {}),
                "control_status": report.get("control_status", "unknown"),
                "control_ledger": report.get("control_ledger", {}),
                "number_to_beat": report.get("number_to_beat", {}),
                "baselines": report["baselines"],
            }
        )
    control_ledger = build_control_ledger(all_baselines)
    tracedb_attribution = _tracedb_attribution(scenarios)
    return {
        "suite_id": suite_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "dataset": dataset,
        "records": records,
        "scenarios": scenarios,
        "control": {
            "control_status": control_ledger["control_status"],
            "number_to_beat": control_ledger["number_to_beat"],
        },
        "control_status": control_ledger["control_status"],
        "control_ledger": control_ledger,
        "number_to_beat": control_ledger["number_to_beat"],
        "tracedb_attribution": tracedb_attribution,
        "summary": {
            "scenario_count": len(scenarios),
            "tracedb_attribution_count": len(tracedb_attribution),
            "baseline_observations": sum(len(scenario["baselines"]) for scenario in scenarios),
            "available_observations": sum(
                1
                for scenario in scenarios
                for baseline in scenario["baselines"]
                if baseline.get("available")
            ),
            "unavailable_observations": sum(
                1
                for scenario in scenarios
                for baseline in scenario["baselines"]
                if not baseline.get("available")
            ),
            "failure_count": sum(
                int(scenario["summary"].get("failure_count", 0)) for scenario in scenarios
            ),
            "control_status": control_ledger["control_status"],
        },
    }


def write_suite_json(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_suite_markdown(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# TraceDB Real-World Benchmark Suite Report",
        "",
        "## Executive Summary",
        "",
        f"- Suite ID: `{report['suite_id']}`",
        f"- Profile: `{report['profile']}`",
        f"- Dataset: `{report['dataset']}`",
        f"- Records per scenario: `{report['records']}`",
        f"- Scenarios run: `{report['summary']['scenario_count']}`",
        f"- Baseline observations: `{report['summary']['baseline_observations']}`",
        f"- Available observations: `{report['summary']['available_observations']}`",
        f"- Unavailable observations: `{report['summary']['unavailable_observations']}`",
        f"- Failure count: `{report['summary']['failure_count']}`",
        f"- Control status: `{report.get('control_status', 'unknown')}`",
        "",
        "This report is intended to falsify TraceDB behavior against concrete database workloads, "
        "not to prove success from health checks. Each scenario below states what is simulated, "
        "which surfaces are exercised, and which metrics should be read.",
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
        "## How to Read This Report",
        "",
        "- `available=no` means the adapter or service could not run; its notes should be treated as an experimental caveat, not a passing result.",
        "- `recall@5` measures whether expected relevant records appeared in the first five results.",
        "- `nDCG@5` rewards relevant records that appear earlier in the ranking.",
        "- `MRR@5` measures how quickly the first relevant result appears.",
        "- Dataset `relevance_label_scope` controls how strongly retrieval metrics should be interpreted; generated operational-smoke labels are not definitive hybrid-search quality labels.",
        "- `p50/p95/p99` are query latency percentiles in milliseconds for completed query calls.",
        "- `failures` counts benchmark-observed adapter, API, invariant, or falsification failures.",
        "",
        "## Database Roles Compared",
        "",
        "| database | role in this suite |",
        "| --- | --- |",
        *[
            f"| {database} | {role} |"
            for database, role in ROLE_DESCRIPTIONS.items()
        ],
        "",
        "## What We Simulated",
        "",
    ]
    for scenario in report["scenarios"]:
        lines.extend(
            [
                f"### {scenario['id']} - {scenario['name']}",
                "",
                scenario["description"],
                "",
                f"**Hypothesis:** {scenario['hypothesis']}",
                "",
                "**Pass criteria:**",
                *[f"- {criterion}" for criterion in scenario["pass_criteria"]],
                "",
                f"**Surfaces:** {', '.join(scenario['surfaces'])}",
                f"**Artifact directory:** `{scenario['artifact_dir']}`",
                "",
            ]
        )

    lines.extend(["## Scenario Findings", ""])
    for scenario in report["scenarios"]:
        lines.extend(
            [
                f"### {scenario['id']} - {scenario['name']}",
                "",
                f"- Status: `{_scenario_status(scenario)}`",
                f"- Control status: `{scenario.get('control_status', 'unknown')}`",
                f"- Available baselines: {_baseline_names(scenario, available=True)}",
                f"- Unavailable baselines: {_baseline_names(scenario, available=False)}",
                f"- Fastest p95 latency: {_best_latency(scenario)}",
                f"- Highest recall@5: {_best_score(scenario, 'recall_at_5')}",
                f"- TraceDB result: {_tracedb_result(scenario)}",
                "",
            ]
        )

    lines.extend(
        [
            "## Scenario Comparison Matrix",
            "",
            "| scenario | baseline | available | ingest | ingest txns | ingest txn total ms | queries | p50 ms | p95 ms | p99 ms | recall@5 | nDCG@5 | MRR@5 | failures | notes |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for scenario in report["scenarios"]:
        for baseline in scenario["baselines"]:
            metrics = baseline["metrics"]
            notes = "; ".join(baseline["notes"]).replace("|", "\\|")
            lines.append(
                "| {scenario} | {baseline} | {available} | {ingest} | {ingest_txns} | {ingest_txn_total} | {queries} | {p50} | {p95} | {p99} | {recall} | {ndcg} | {mrr} | {failures} | {notes} |".format(
                    scenario=scenario["id"],
                    baseline=baseline["name"],
                    available="yes" if baseline["available"] else "no",
                    ingest=metrics.get("ingest_count", 0),
                    ingest_txns=metrics.get("ingest_transaction_count", "n/a"),
                    ingest_txn_total=metrics.get(
                        "ingest_transaction_total_latency_ms", "n/a"
                    ),
                    queries=metrics.get("query_count", 0),
                    p50=_display_query_latency(metrics, 50),
                    p95=_display_query_latency(metrics, 95),
                    p99=_display_query_latency(metrics, 99),
                    recall=metrics.get("recall_at_5", 0.0),
                    ndcg=metrics.get("ndcg_at_5", 0.0),
                    mrr=metrics.get("mrr_at_5", 0.0),
                    failures=metrics.get("failure_count", 0),
                    notes=notes,
                )
            )

    lines.extend(
        [
            "",
            "## TraceDB Attribution",
            "",
            "| scenario | query p95 ms | query phases | server timings | http/client | response | output shape probe | access paths | batch phases | batch sizes | storage after ingest | storage after workload |",
            "| --- | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    if report.get("tracedb_attribution"):
        for item in report["tracedb_attribution"]:
            lines.append(
                "| {scenario} | {query_p95} | {query_phases} | {server} | {http_client} | {response} | {output_shape_probe} | {access_paths} | {batch_phases} | {batch_sizes} | {storage_ingest} | {storage_workload} |".format(
                    scenario=item["scenario_id"],
                    query_p95=item.get("query", {}).get("query_latency_p95_ms", "n/a"),
                    query_phases=_metric_map_summary(item.get("query_phases", {})),
                    server=_metric_map_summary(item.get("server", {})),
                    http_client=_metric_map_summary(item.get("http_client", {})),
                    response=_metric_map_summary(item.get("response", {})),
                    output_shape_probe=_metric_map_summary(
                        item.get("output_shape_probe", {})
                    ),
                    access_paths=_metric_map_summary(item.get("access_paths", {})),
                    batch_phases=_metric_map_summary(item.get("batch_phases", {})),
                    batch_sizes=_metric_map_summary(item.get("batch_sizes", {})),
                    storage_ingest=_metric_map_summary(
                        item.get("storage_after_ingest", {})
                    ),
                    storage_workload=_metric_map_summary(
                        item.get("storage_after_workload", {})
                    ),
                )
            )
    else:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |")

    lines.extend(
        [
            "",
            "## Provider and Rerank Evidence",
            "",
            "| scenario | embedding model | used dims | native dims | requested dims | requests | embedding calls | rerank model | rerank calls | cache hits | cache misses | search units | rerank recall@5 | rerank nDCG@5 | rerank MRR@5 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for scenario in report["scenarios"]:
        provider = scenario.get("openrouter", {})
        rerank = provider.get("rerank_metrics") if isinstance(provider, dict) else {}
        if not isinstance(rerank, dict):
            rerank = {}
        dataset = scenario["dataset"]
        lines.append(
            "| {scenario} | {embed_model} | {dims} | {native_dims} | {requested_dims} | {requests} | {embed_calls} | {rerank_model} | {rerank_calls} | {hits} | {misses} | {search_units} | {recall} | {ndcg} | {mrr} |".format(
                scenario=scenario["id"],
                embed_model=dataset.get("embedding_model", "n/a"),
                dims=dataset.get("embedding_dimensions", 0),
                native_dims=provider.get("provider_native_embedding_dimensions", 0),
                requested_dims=provider.get("requested_embedding_dimensions") or "native",
                requests=provider.get("request_count", 0),
                embed_calls=provider.get("embedding_request_count", 0),
                rerank_model=provider.get("rerank_model") or rerank.get("model") or "n/a",
                rerank_calls=provider.get("rerank_request_count", 0),
                hits=provider.get("cache_hits", 0),
                misses=provider.get("cache_misses", 0),
                search_units=provider.get("search_units", 0),
                recall=rerank.get("recall_at_5", 0.0),
                ndcg=rerank.get("ndcg_at_5", 0.0),
                mrr=rerank.get("mrr_at_5", 0.0),
            )
        )

    unavailable = [
        (scenario["id"], baseline["name"], "; ".join(baseline["notes"]))
        for scenario in report["scenarios"]
        for baseline in scenario["baselines"]
        if not baseline.get("available")
    ]
    lines.extend(["", "## Unavailable Baselines and Caveats", ""])
    if unavailable:
        lines.extend(
            f"- `{scenario}` / `{baseline}`: {notes}"
            for scenario, baseline, notes in unavailable
        )
    else:
        lines.append("All requested baselines reported available metrics.")

    lines.extend(["", "## Child Report Artifacts", ""])
    for scenario in report["scenarios"]:
        artifact = scenario["artifact_dir"]
        lines.append(f"- `{scenario['id']}`: `{artifact}`")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _scenario_status(scenario: dict[str, Any]) -> str:
    baselines = scenario.get("baselines", [])
    tracedb = next((baseline for baseline in baselines if _is_tracedb(baseline)), None)
    failures = int(scenario.get("summary", {}).get("failure_count", 0))
    if tracedb and tracedb.get("available") and failures == 0:
        unavailable = [baseline for baseline in baselines if not baseline.get("available")]
        return "passed" if not unavailable else "degraded"
    if tracedb and tracedb.get("available"):
        return "failed-with-tracedb-failures"
    return "blocked-tracedb-unavailable"


def _baseline_names(scenario: dict[str, Any], *, available: bool) -> str:
    names = [
        baseline.get("name", "unknown")
        for baseline in scenario.get("baselines", [])
        if bool(baseline.get("available")) is available
    ]
    return ", ".join(names) if names else "none"


def _best_latency(scenario: dict[str, Any]) -> str:
    candidates = []
    for baseline in scenario.get("baselines", []):
        if not baseline.get("available"):
            continue
        metrics = baseline.get("metrics", {})
        if int(metrics.get("query_count", 0)) <= 0:
            continue
        latency, _source = query_latency_metric(metrics, 95)
        if latency is None:
            continue
        candidates.append((float(latency), baseline.get("name", "unknown")))
    if not candidates:
        return "not measured"
    latency, name = min(candidates)
    return f"{name} ({latency:.3f} ms p95)"


def _best_score(scenario: dict[str, Any], metric: str) -> str:
    candidates = []
    for baseline in scenario.get("baselines", []):
        if not baseline.get("available"):
            continue
        metrics = baseline.get("metrics", {})
        if int(metrics.get("query_count", 0)) <= 0:
            continue
        candidates.append((float(metrics.get(metric, 0.0)), baseline.get("name", "unknown")))
    if not candidates:
        return "not measured"
    score, name = max(candidates)
    return f"{name} ({score:.4f})"


def _tracedb_result(scenario: dict[str, Any]) -> str:
    tracedb = next(
        (baseline for baseline in scenario.get("baselines", []) if _is_tracedb(baseline)),
        None,
    )
    if not tracedb:
        return "TraceDB was not requested in this scenario."
    notes = "; ".join(tracedb.get("notes", [])) or "no notes"
    if not tracedb.get("available"):
        return f"unavailable - {notes}"
    metrics = tracedb.get("metrics", {})
    query_p95, _source = query_latency_metric(metrics, 95)
    return (
        f"available with {metrics.get('ingest_count', 0)} ingested records, "
        f"{metrics.get('query_count', 0)} queries, p95 {float(query_p95 or 0.0):.3f} ms, "
        f"recall@5 {float(metrics.get('recall_at_5', 0.0)):.4f}, "
        f"{metrics.get('failure_count', 0)} failures. Notes: {notes}"
    )


def _is_tracedb(baseline: dict[str, Any]) -> bool:
    return is_tracedb_baseline(baseline)


def _control_status_sentence(report: dict[str, Any]) -> str:
    status = report.get("control_status", "unknown")
    if status == "external_control_available":
        return "At least one external control produced metrics; this suite has a number to beat."
    if status == "external_control_unavailable":
        return "External controls were requested but unavailable; no product-language conclusion is valid until a control produces metrics."
    if status == "internal_only_smoke":
        return "This TraceDB-only suite is development evidence, not product evidence."
    return "Control status is unknown; do not promote this suite as product evidence."


def _number_to_beat_rows(number_to_beat: dict[str, Any]) -> list[str]:
    rows = []
    for metric, entry in number_to_beat.items():
        baseline = entry.get("baseline") if isinstance(entry, dict) else None
        value = entry.get("value") if isinstance(entry, dict) else None
        rows.append(f"| {metric} | {baseline or 'n/a'} | {value if value is not None else 'n/a'} |")
    return rows


def _baselines_with_context(
    baselines: list[dict[str, Any]], *, scenario_id: str, artifact_dir: str
) -> list[dict[str, Any]]:
    contextual = []
    for baseline in baselines:
        copy = dict(baseline)
        copy["scenario_id"] = scenario_id
        copy["artifact_dir"] = artifact_dir
        contextual.append(copy)
    return contextual


def _tracedb_attribution(scenarios: list[dict[str, Any]]) -> list[dict[str, Any]]:
    attribution = []
    for scenario in scenarios:
        tracedb = next(
            (
                baseline
                for baseline in scenario.get("baselines", [])
                if is_tracedb_baseline(baseline) and baseline.get("available")
            ),
            None,
        )
        if tracedb is None:
            continue
        metrics = tracedb.get("metrics", {})
        query_phases = _strip_metric_prefix(metrics, "query_phase_")
        access_paths = _strip_metric_prefix(metrics, "query_access_path_")
        http_client = _strip_metric_prefix(metrics, "query_http_client_")
        response = _strip_metric_prefix(metrics, "query_http_response_")
        output_shape_probe = _strip_metric_prefix(metrics, "query_output_probe_")
        server = _strip_metric_prefix(metrics, "query_server_")
        engine = _strip_metric_prefix(metrics, "query_engine_")
        batch_phases = _strip_metric_prefix(metrics, "batch_phase_")
        batch_sizes = _strip_metric_prefix(metrics, "batch_size_")
        storage_after_ingest = _strip_metric_prefix(metrics, "disk_bytes_after_ingest_")
        storage_after_workload = _strip_metric_prefix(metrics, "disk_bytes_after_workload_")
        if not any(
            [
                query_phases,
                access_paths,
                http_client,
                response,
                output_shape_probe,
                server,
                engine,
                batch_phases,
                batch_sizes,
                storage_after_ingest,
                storage_after_workload,
            ]
        ):
            continue
        attribution.append(
            {
                "scenario_id": scenario["id"],
                "artifact_dir": scenario["artifact_dir"],
                "query": {
                    key: metrics[key]
                    for key in [
                        "query_latency_p50_ms",
                        "query_latency_p95_ms",
                        "query_latency_p99_ms",
                    ]
                    if key in metrics
                },
                "query_phases": query_phases,
                "http_client": http_client,
                "response": response,
                "output_shape_probe": output_shape_probe,
                "server": server,
                "engine": engine,
                "access_paths": access_paths,
                "batch_phases": batch_phases,
                "batch_sizes": batch_sizes,
                "storage_after_ingest": storage_after_ingest,
                "storage_after_workload": storage_after_workload,
            }
        )
    return attribution


def _strip_metric_prefix(metrics: dict[str, Any], prefix: str) -> dict[str, Any]:
    return {
        key[len(prefix) :]: value
        for key, value in sorted(metrics.items())
        if key.startswith(prefix) and key != prefix.rstrip("_")
    }


def _metric_map_summary(values: dict[str, Any]) -> str:
    if not values:
        return "n/a"
    return ", ".join(f"{key}={value}" for key, value in sorted(values.items())).replace("|", "\\|")


def _display_query_latency(metrics: dict[str, Any], percentile: int) -> Any:
    value, _source = query_latency_metric(metrics, percentile)
    return value if value is not None else 0.0
