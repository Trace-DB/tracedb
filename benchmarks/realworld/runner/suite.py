from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROLE_DESCRIPTIONS = {
    "TraceDB": (
        "unified record, lexical, vector, freshness, snapshot, and API-surface target under test"
    ),
    "PostgreSQL": "relational baseline for scalar filters, updates, scans, and normalized lookup shape",
    "PostgreSQL+pgvector": "relational plus vector baseline for metadata-filtered semantic retrieval",
    "MongoDB": "document-store baseline for nested JSON, sparse fields, and document-shaped updates",
    "Qdrant": "vector-native baseline for approximate nearest neighbor plus payload filters",
    "OpenSearch": "lexical/search baseline for BM25-style text retrieval and search ranking",
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
            "Compares TraceDB, PostgreSQL, pgvector, MongoDB, Qdrant, and OpenSearch on the same "
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
            "Comparison table includes all six baselines",
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
    for item in reports:
        spec: ScenarioSpec = item["spec"]
        report = item["report"]
        scenarios.append(
            {
                "id": spec.scenario_id,
                "name": spec.name,
                "description": spec.description,
                "hypothesis": spec.hypothesis,
                "pass_criteria": spec.pass_criteria,
                "artifact_dir": item["artifact_dir"],
                "summary": report["summary"],
                "dataset": report["dataset"],
                "surfaces": report["surfaces"],
                "openrouter": report.get("openrouter", {}),
                "baselines": report["baselines"],
            }
        )
    return {
        "suite_id": suite_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "dataset": dataset,
        "records": records,
        "scenarios": scenarios,
        "summary": {
            "scenario_count": len(scenarios),
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
        "",
        "This report is intended to falsify TraceDB behavior against concrete database workloads, "
        "not to prove success from health checks. Each scenario below states what is simulated, "
        "which surfaces are exercised, and which metrics should be read.",
        "",
        "## How to Read This Report",
        "",
        "- `available=no` means the adapter or service could not run; its notes should be treated as an experimental caveat, not a passing result.",
        "- `recall@5` measures whether expected relevant records appeared in the first five results.",
        "- `nDCG@5` rewards relevant records that appear earlier in the ranking.",
        "- `MRR@5` measures how quickly the first relevant result appears.",
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
            "| scenario | baseline | available | ingest | queries | p50 ms | p95 ms | p99 ms | recall@5 | nDCG@5 | MRR@5 | failures | notes |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for scenario in report["scenarios"]:
        for baseline in scenario["baselines"]:
            metrics = baseline["metrics"]
            notes = "; ".join(baseline["notes"]).replace("|", "\\|")
            lines.append(
                "| {scenario} | {baseline} | {available} | {ingest} | {queries} | {p50} | {p95} | {p99} | {recall} | {ndcg} | {mrr} | {failures} | {notes} |".format(
                    scenario=scenario["id"],
                    baseline=baseline["name"],
                    available="yes" if baseline["available"] else "no",
                    ingest=metrics.get("ingest_count", 0),
                    queries=metrics.get("query_count", 0),
                    p50=metrics.get("latency_p50_ms", 0.0),
                    p95=metrics.get("latency_p95_ms", 0.0),
                    p99=metrics.get("latency_p99_ms", 0.0),
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
        candidates.append((float(metrics.get("latency_p95_ms", 0.0)), baseline.get("name", "unknown")))
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
    return (
        f"available with {metrics.get('ingest_count', 0)} ingested records, "
        f"{metrics.get('query_count', 0)} queries, p95 {float(metrics.get('latency_p95_ms', 0.0)):.3f} ms, "
        f"recall@5 {float(metrics.get('recall_at_5', 0.0)):.4f}, "
        f"{metrics.get('failure_count', 0)} failures. Notes: {notes}"
    )


def _is_tracedb(baseline: dict[str, Any]) -> bool:
    return str(baseline.get("name", "")).lower() in {"tracedb", "trace_db", "trace-db"}
