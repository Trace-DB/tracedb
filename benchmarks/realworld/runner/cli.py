from __future__ import annotations

import argparse
import json
import os
import sys
from copy import copy
from pathlib import Path
from typing import Any

from .adapters import all_adapters
from .chat_demo import run_chat_demo
from .datasets import load_dataset
from .experiment import ExperimentRecorder, new_run_id, redact, service_environment
from .openrouter import (
    OpenRouterClient,
    OpenRouterError,
    config_from_args,
    maybe_apply_openrouter_embeddings,
)
from .report import build_report, write_json, write_markdown
from .suite import build_suite_report, selected_scenarios, write_suite_json, write_suite_markdown
from .types import RunConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m runner")
    subcommands = parser.add_subparsers(dest="command", required=True)

    run = subcommands.add_parser("run", help="run a real-world benchmark profile")
    add_benchmark_args(run)

    loop = subcommands.add_parser("loop", help="loop a benchmark and stop on falsification")
    add_benchmark_args(loop)
    loop.add_argument("--iterations", type=int, default=20)
    loop.add_argument("--stop-on-failure", action="store_true")

    doctor = subcommands.add_parser("doctor", help="inspect benchmark dependencies")
    doctor_subcommands = doctor.add_subparsers(dest="doctor_target", required=True)
    openrouter = doctor_subcommands.add_parser("openrouter", help="inspect OpenRouter config")
    add_openrouter_args(openrouter)

    suite = subcommands.add_parser("suite", help="run a scenario suite and aggregate reports")
    add_benchmark_args(suite)
    suite.add_argument("--scenarios", default="all")

    chat_demo = subcommands.add_parser("chat-demo", help="run the local chat-memory demo")
    chat_demo.add_argument("--data-dir", default="")
    chat_demo.add_argument("--tracedb-cli", default="")
    chat_demo.add_argument("--output-json", default="reports/chat-demo/latest.json")
    chat_demo.add_argument("--output-md", default="reports/chat-demo/latest.md")

    args = parser.parse_args(argv)
    if args.command == "run":
        return run_benchmark(args)
    if args.command == "loop":
        return run_loop(args)
    if args.command == "doctor" and args.doctor_target == "openrouter":
        return doctor_openrouter(args)
    if args.command == "suite":
        return run_suite(args)
    if args.command == "chat-demo":
        return run_chat_demo(args)
    parser.error(f"unknown command {args.command}")
    return 2


def add_benchmark_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", default="smoke", choices=["smoke", "local", "ci"])
    parser.add_argument(
        "--dataset",
        default="generated",
        choices=[
            "generated",
            "generated_hybrid",
            "embedded_movies",
            "beir_scifact",
            "scifact",
            "codesearchnet",
            "code_search_net",
            "codesearchnet_body",
            "codesearchnet_codeaware",
        ],
    )
    parser.add_argument("--records", type=int, default=1000)
    parser.add_argument("--target", default="all")
    parser.add_argument("--surface", default="sdk,cli,http,curl")
    parser.add_argument("--output-json", default="reports/latest.json")
    parser.add_argument("--output-md", default="reports/latest.md")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--require-services", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-id", default="")
    add_openrouter_args(parser)


def add_openrouter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--openrouter-mode", default="auto", choices=["auto", "off", "required"])
    parser.add_argument(
        "--openrouter-cap",
        default="moderate",
        choices=["conservative", "moderate", "aggressive"],
    )
    parser.add_argument("--embed-model", default=None)
    parser.add_argument("--compare-embed-models", default=None)
    parser.add_argument("--judge-model", default=None)
    parser.add_argument("--rerank-model", default=None)
    parser.add_argument(
        "--embedding-dimensions",
        default=None,
        help="Cap provider embeddings to this many dimensions; use 0/native/auto for provider-native size.",
    )


def run_benchmark(args: argparse.Namespace) -> int:
    try:
        report, exit_code = execute_benchmark(args)
    except OpenRouterError as error:
        print(str(error), file=sys.stderr)
        return 1
    if args.require_services and report["summary"]["unavailable_count"]:
        print("one or more required services were unavailable", file=sys.stderr)
        return 1
    return exit_code


def execute_benchmark(
    args: argparse.Namespace,
    *,
    seed: int | None = None,
    run_id: str | None = None,
) -> tuple[dict[str, Any], int]:
    lab_root = Path.cwd()
    repo_root = lab_root.parent.parent
    reports_dir = _resolve_path(lab_root, args.reports_dir)
    selected_run_id = run_id or args.run_id or new_run_id("bench")
    recorder = ExperimentRecorder(selected_run_id, reports_dir)

    targets = parse_csv(args.target)
    surfaces = parse_csv(args.surface)
    selected_seed = args.seed if seed is None else seed
    openrouter_config = config_from_args(args, lab_root)
    dataset = load_dataset(args.dataset, args.records, selected_seed)
    dataset, openrouter_stats = maybe_apply_openrouter_embeddings(
        dataset, openrouter_config, lab_root, recorder
    )
    config = RunConfig(
        profile=args.profile,
        target=targets,
        surfaces=surfaces,
        require_services=args.require_services,
        repo_root=str(repo_root),
        openrouter_mode=openrouter_config.mode,
        openrouter_cap=openrouter_config.cap_name,
        run_id=selected_run_id,
        reports_dir=str(reports_dir),
        observer=recorder,
    )
    manifest = build_manifest(
        args=args,
        config=config,
        dataset=dataset,
        openrouter_config=openrouter_config.public_summary(),
        openrouter_stats=openrouter_stats,
        seed=selected_seed,
        repo_root=repo_root,
    )
    recorder.write_manifest(manifest)
    recorder.observe(
        "benchmark.start",
        {
            "targets": targets,
            "surfaces": surfaces,
            "record_count": len(dataset.records),
            "query_count": len(dataset.queries),
        },
    )

    selected = [adapter for adapter in all_adapters() if "all" in targets or adapter.name in targets]
    if not selected:
        raise SystemExit(f"no adapters selected for target={args.target}")

    baselines = []
    for adapter in selected:
        recorder.observe("adapter.start", {"name": adapter.name})
        try:
            result = adapter.run(dataset, config)
        except Exception as error:
            if args.require_services:
                raise
            result = adapter.unavailable(f"{adapter.name} failed: {error}", dataset)
        baselines.append(result)
        recorder.observe(
            "adapter.complete",
            {
                "name": adapter.name,
                "available": result["available"],
                "metrics": result["metrics"],
                "notes": result["notes"],
            },
        )

    report = build_report(dataset, config, baselines, openrouter_stats)
    failures = collect_failures(report)
    recorder.write_failures(failures)
    write_json(report, recorder.run_dir / "summary.json")
    write_markdown(report, recorder.run_dir / "report.md")
    write_json(report, _resolve_path(lab_root, args.output_json))
    write_markdown(report, _resolve_path(lab_root, args.output_md))
    recorder.observe(
        "benchmark.complete",
        {
            "summary": report["summary"],
            "failure_count": len(failures),
            "artifact_dir": str(recorder.run_dir),
        },
    )
    print(f"wrote {recorder.run_dir / 'summary.json'}")
    print(f"wrote {recorder.run_dir / 'report.md'}")
    print(f"wrote {args.output_json}")
    print(f"wrote {args.output_md}")
    return report, 0


def run_loop(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    reports_dir = _resolve_path(lab_root, args.reports_dir)
    base_run_id = args.run_id or new_run_id("loop")
    recorder = ExperimentRecorder(base_run_id, reports_dir)
    injection = os.environ.get("BENCH_INJECT_FAILURE_ITERATION")
    recorder.write_manifest(
        {
            "run_id": base_run_id,
            "kind": "loop",
            "profile": args.profile,
            "dataset": args.dataset,
            "records": args.records,
            "iterations": args.iterations,
            "seed_start": args.seed,
            "openrouter_mode": args.openrouter_mode,
            "openrouter_cap": args.openrouter_cap,
            "service_environment": service_environment(),
        }
    )
    loop_summary: list[dict[str, Any]] = []
    for iteration in range(1, args.iterations + 1):
        seed = args.seed + iteration - 1
        if injection and int(injection) == iteration:
            reason = f"injected failure at iteration {iteration}"
            recorder.write_failure_case(iteration, seed, reason)
            (recorder.run_dir / "summary.json").write_text(
                json.dumps(
                    {"run_id": base_run_id, "status": "failed", "iterations": loop_summary},
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            if args.stop_on_failure:
                return 1
            continue

        child_args = copy(args)
        child_args.run_id = f"{base_run_id}-iteration-{iteration}"
        try:
            report, exit_code = execute_benchmark(child_args, seed=seed, run_id=child_args.run_id)
        except OpenRouterError as error:
            recorder.write_failure_case(iteration, seed, str(error))
            if args.stop_on_failure:
                return 1
            continue
        summary = {
            "iteration": iteration,
            "seed": seed,
            "run_id": child_args.run_id,
            "exit_code": exit_code,
            "summary": report["summary"],
        }
        loop_summary.append(summary)
        failed = exit_code != 0 or report["summary"]["failure_count"] > 0
        if failed:
            recorder.write_failure_case(iteration, seed, f"benchmark failure in {child_args.run_id}")
            if args.stop_on_failure:
                return 1

    (recorder.run_dir / "summary.json").write_text(
        json.dumps(
            {"run_id": base_run_id, "status": "passed", "iterations": loop_summary},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    recorder.write_failures([])
    print(f"wrote {recorder.run_dir / 'summary.json'}")
    return 0


def run_suite(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    reports_dir = _resolve_path(lab_root, args.reports_dir)
    suite_id = args.run_id or new_run_id("suite")
    suite_dir = reports_dir / suite_id
    suite_dir.mkdir(parents=True, exist_ok=True)
    specs = selected_scenarios(args.scenarios)
    child_reports = []
    exit_code = 0
    for spec in specs:
        child_args = copy(args)
        child_args.run_id = f"{suite_id}-{spec.scenario_id}"
        child_args.target = spec.target if args.target == "all" else args.target
        child_args.surface = spec.surface if args.surface == "sdk,cli,http,curl" else args.surface
        child_args.output_json = str(suite_dir / f"{spec.scenario_id}.json")
        child_args.output_md = str(suite_dir / f"{spec.scenario_id}.md")
        try:
            report, child_exit = execute_benchmark(child_args, run_id=child_args.run_id)
        except OpenRouterError as error:
            print(str(error), file=sys.stderr)
            return 1
        exit_code = max(exit_code, child_exit)
        if args.require_services and report["summary"]["unavailable_count"]:
            exit_code = 1
        child_reports.append(
            {
                "spec": spec,
                "report": report,
                "artifact_dir": str(reports_dir / child_args.run_id),
            }
        )

    suite_report = build_suite_report(
        suite_id=suite_id,
        profile=args.profile,
        dataset=args.dataset,
        records=args.records,
        reports=child_reports,
    )
    write_suite_json(suite_report, suite_dir / "suite.json")
    write_suite_markdown(suite_report, suite_dir / "suite.md")
    print(f"wrote {suite_dir / 'suite.json'}")
    print(f"wrote {suite_dir / 'suite.md'}")
    return exit_code


def doctor_openrouter(args: argparse.Namespace) -> int:
    lab_root = Path.cwd()
    try:
        config = config_from_args(args, lab_root)
        if config.mode == "off":
            print(
                json.dumps(
                    {
                        "status": "disabled",
                        "openrouter_mode": "off",
                        "reason": "disabled by --openrouter-mode off",
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if not config.api_key:
            payload = {
                "status": "missing_key",
                "openrouter_mode": config.mode,
                "reason": "OPENROUTER_API_KEY is not configured",
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            if config.mode == "required":
                print(payload["reason"], file=sys.stderr)
                return 1
            return 0
        client = OpenRouterClient(config, lab_root / ".cache" / "openrouter")
        key_info = client.key_info()
        models = client.list_embedding_models()
        payload = {
            "status": "ok",
            "openrouter_mode": config.mode,
            "base_url": config.base_url,
            "embed_model": config.embed_model,
            "compare_embed_models": config.compare_embed_models,
            "judge_model": config.judge_model,
            "rerank_model": config.rerank_model,
            "embedding_dimensions": config.embedding_dimensions,
            "key": redact(key_info),
            "embedding_model_count": len(models),
            "stats": client.stats,
        }
        print(json.dumps(redact(payload), indent=2, sort_keys=True))
        return 0
    except OpenRouterError as error:
        print(str(error), file=sys.stderr)
        return 1


def build_manifest(
    *,
    args: argparse.Namespace,
    config: RunConfig,
    dataset: Any,
    openrouter_config: dict[str, Any],
    openrouter_stats: dict[str, Any],
    seed: int,
    repo_root: Path,
) -> dict[str, Any]:
    return {
        "run_id": config.run_id,
        "hypothesis": "TraceDB can satisfy real-world database and AI retrieval workloads with observable, falsifiable behavior.",
        "profile": config.profile,
        "seed": seed,
        "dataset": {
            "kind": dataset.kind,
            "source": dataset.source,
            "digest": dataset.digest,
            "record_count": len(dataset.records),
            "query_count": len(dataset.queries),
            "embedding_model": dataset.embedding_model,
            "embedding_dimensions": dataset.embedding_dimensions,
            "embedding_source": dataset.embedding_source,
            "relevance_label_mode": dataset.relevance_label_mode,
            "relevance_label_scope": dataset.relevance_label_scope,
        },
        "targets": config.target,
        "surfaces": config.surfaces,
        "openrouter": {
            **openrouter_config,
            "stats": openrouter_stats,
        },
        "adapter_versions": {
            "runner": "realworld-v1",
            "tracedb": "workspace-debug",
        },
        "service_environment": service_environment(),
        "repo_root": str(repo_root),
        "output_json": args.output_json,
        "output_md": args.output_md,
    }


def collect_failures(report: dict[str, Any]) -> list[str]:
    failures = []
    for baseline in report["baselines"]:
        metrics = baseline.get("metrics", {})
        if not baseline.get("available", False):
            failures.append(f"{baseline['name']} unavailable: {'; '.join(baseline['notes'])}")
        if int(metrics.get("failure_count", 0)) > 0:
            failures.append(f"{baseline['name']} reported failure_count={metrics['failure_count']}")
    return failures


def parse_csv(value: str) -> list[str]:
    out = [item.strip() for item in value.split(",") if item.strip()]
    return out or ["all"]


def _resolve_path(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path
