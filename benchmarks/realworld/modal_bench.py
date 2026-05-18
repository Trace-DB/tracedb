from __future__ import annotations

import argparse
import glob
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

try:
    import modal
except ImportError:  # pragma: no cover - local tests should not require Modal.
    modal = None


LAB_ROOT = Path(__file__).resolve().parent
REPO_ROOT = LAB_ROOT.parent.parent
REMOTE_REPO = "/workspace/TraceDB"
DEFAULT_REPORTS_DIR = "/tmp/tracedb-modal-reports"
DEFAULT_BUNDLE_DIR = "/tmp/tracedb-modal-bundles"
DEFAULT_MAX_RECORDS = 1000
MODAL_MIN_EPHEMERAL_DISK_MB = 524_288
DEFAULT_MODAL_APP_NAME = "tracedb-realworld-smoke"
MODAL_APP_NAME_ENV = "TRACEDB_MODAL_APP_NAME"
MODAL_IMAGE_KIND_ENV = "TRACEDB_MODAL_IMAGE_KIND"
RUST_MODAL_IMAGE = "rust:1.94-bookworm"
PGVECTOR_VERSION = "v0.8.2"
POSTGRES_DSN_ENV = "BENCH_POSTGRES_DSN"
PGVECTOR_DSN_ENV = "BENCH_PGVECTOR_DSN"
TRACEDB_HTTP_URL_ENV = "TRACEDB_HTTP_URL"
TRACEDB_HTTP_DATA_DIR_ENV = "TRACEDB_HTTP_DATA_DIR"
EXTERNAL_CONTROL_TARGETS = {
    "postgres",
    "pgvector",
    "mongodb",
    "qdrant",
    "opensearch",
    "milvus",
    "mysql",
}
SENSITIVE_ENV_KEYS = {
    POSTGRES_DSN_ENV,
    PGVECTOR_DSN_ENV,
    "BENCH_MONGO_URI",
    "OPENROUTER_API_KEY",
    "TRACEDB_HTTP_BEARER_TOKEN",
}
MODAL_IGNORE_PATTERNS = [
    "target/**",
    ".git/**",
    ".env",
    ".env.local",
    "benchmarks/realworld/.env.local",
    "benchmarks/realworld/.cache/**",
    "benchmarks/realworld/.venv/**",
    "benchmarks/realworld/run-data/**",
    "benchmarks/realworld/reports/**",
    "benchmarks/realworld/report-bundles/**",
]


@dataclass(frozen=True)
class ModalSmokeConfig:
    run_id: str = "modal-smoke"
    profile: str = "smoke"
    dataset: str = "generated"
    records: int = 128
    target: str = "tracedb"
    surface: str = "sdk"
    scenarios: str = "sdk_cli_surface"
    openrouter_mode: str = "off"
    openrouter_cap: str = "moderate"
    tracedb_ingest_mode: str = "per_record"
    embedding_dimensions: str | None = None
    seed: int = 42
    reports_dir: str = DEFAULT_REPORTS_DIR
    bundle_dir: str = DEFAULT_BUNDLE_DIR
    min_free_mb: int = 20_000
    cpu: float = 4.0
    memory_mb: int = 16_384
    timeout_seconds: int = 3_600
    ephemeral_disk_mb: int = MODAL_MIN_EPHEMERAL_DISK_MB
    gpu_requested: bool = False
    allow_gpu: bool = False
    allow_large: bool = False
    allow_external_controls: bool = False
    allow_provider: bool = False
    require_services: bool = False
    tracedb_engine_control: bool = False
    postgres_control: bool = False
    pgvector_control: bool = False
    tracedb_port: int = 18_080
    postgres_port: int = 25_432
    pgvector_port: int = 25_433
    modal_app_name: str = DEFAULT_MODAL_APP_NAME
    modal_image_kind: str = "base"
    source_commit: str | None = None
    source_dirty: bool | None = None
    source_status_short: str | None = None
    source_git_error: str | None = None


def validate_config(config: ModalSmokeConfig) -> None:
    if config.tracedb_ingest_mode not in {"per_record", "batch"}:
        raise ValueError("tracedb_ingest_mode must be per_record or batch")
    if config.records > DEFAULT_MAX_RECORDS and not config.allow_large:
        raise ValueError(
            f"records={config.records} exceeds safe default {DEFAULT_MAX_RECORDS}; set allow_large explicitly"
        )
    if config.gpu_requested and not config.allow_gpu:
        raise ValueError("GPU use requires allow_gpu=True")
    if config.openrouter_mode == "required" and not config.allow_provider:
        raise ValueError("OpenRouter required mode needs allow_provider=True")
    targets = requested_targets(config.target)
    if "all" in targets and not config.allow_external_controls:
        raise ValueError("target=all needs allow_external_controls=True")
    if external_targets(targets) and not config.allow_external_controls:
        raise ValueError("external controls need allow_external_controls=True")
    if config.postgres_control and not config.allow_external_controls:
        raise ValueError("postgres_control needs allow_external_controls=True")
    if config.postgres_control and not target_needs_postgres(config):
        raise ValueError("postgres_control needs target including postgres or all")
    if config.tracedb_engine_control and not target_needs_tracedb(config):
        raise ValueError("tracedb_engine_control needs target including tracedb or all")
    if config.tracedb_engine_control and not surface_needs_http(config):
        raise ValueError("tracedb_engine_control needs surface including http or curl")
    if config.pgvector_control and not config.allow_external_controls:
        raise ValueError("pgvector_control needs allow_external_controls=True")
    if config.pgvector_control and not target_needs_pgvector(config):
        raise ValueError("pgvector_control needs target including pgvector or all")
    ports = []
    if config.tracedb_engine_control:
        ports.append(("tracedb_engine_control", config.tracedb_port))
    if config.postgres_control:
        ports.append(("postgres_control", config.postgres_port))
    if config.pgvector_control:
        ports.append(("pgvector_control", config.pgvector_port))
    seen_ports: dict[int, str] = {}
    for name, port in ports:
        previous = seen_ports.get(port)
        if previous is not None:
            raise ValueError(f"{previous} and {name} need distinct ports")
        seen_ports[port] = name
    if config.min_free_mb < 1_000:
        raise ValueError("min_free_mb is too low for reproducible report artifact runs")


def modal_app_name() -> str:
    return os.environ.get(MODAL_APP_NAME_ENV, DEFAULT_MODAL_APP_NAME)


def modal_image_kind_from_flags(
    *,
    tracedb_engine_control: bool,
    pgvector_control: bool,
    postgres_control: bool,
) -> str:
    if tracedb_engine_control and pgvector_control:
        return "tracedb_pgvector"
    if tracedb_engine_control:
        return "tracedb"
    if pgvector_control:
        return "pgvector"
    if postgres_control:
        return "postgres"
    return "base"


def modal_image_kind_from_args(argv: list[str]) -> str:
    env_kind = os.environ.get(MODAL_IMAGE_KIND_ENV)
    if env_kind:
        return validate_modal_image_kind(env_kind)
    return modal_image_kind_from_flags(
        tracedb_engine_control="--tracedb-engine-control" in argv,
        pgvector_control="--pgvector-control" in argv,
        postgres_control="--postgres-control" in argv,
    )


def validate_modal_image_kind(kind: str) -> str:
    valid = {"base", "postgres", "pgvector", "tracedb", "tracedb_pgvector"}
    if kind not in valid:
        raise ValueError(f"{MODAL_IMAGE_KIND_ENV} must be one of {', '.join(sorted(valid))}")
    return kind


def requested_targets(target: str) -> set[str]:
    return {part.strip() for part in target.split(",") if part.strip()}


def external_targets(targets: set[str]) -> set[str]:
    if "all" in targets:
        return set(EXTERNAL_CONTROL_TARGETS)
    return targets & EXTERNAL_CONTROL_TARGETS


def target_needs_postgres(config: ModalSmokeConfig) -> bool:
    targets = requested_targets(config.target)
    return "all" in targets or "postgres" in targets


def target_needs_tracedb(config: ModalSmokeConfig) -> bool:
    targets = requested_targets(config.target)
    return "all" in targets or "tracedb" in targets


def target_needs_pgvector(config: ModalSmokeConfig) -> bool:
    targets = requested_targets(config.target)
    return "all" in targets or "pgvector" in targets


def surface_needs_http(config: ModalSmokeConfig) -> bool:
    surfaces = {part.strip() for part in config.surface.split(",") if part.strip()}
    return bool(surfaces & {"http", "curl"})


def build_suite_command(config: ModalSmokeConfig) -> list[str]:
    command = [
        "python3",
        "-m",
        "runner",
        "suite",
        "--profile",
        config.profile,
        "--dataset",
        config.dataset,
        "--records",
        str(config.records),
        "--target",
        config.target,
        "--surface",
        config.surface,
        "--openrouter-mode",
        config.openrouter_mode,
        "--openrouter-cap",
        config.openrouter_cap,
        "--tracedb-ingest-mode",
        config.tracedb_ingest_mode,
        "--seed",
        str(config.seed),
        "--run-id",
        config.run_id,
        "--reports-dir",
        config.reports_dir,
        "--scenarios",
        config.scenarios,
    ]
    if config.embedding_dimensions is not None:
        command.extend(["--embedding-dimensions", str(config.embedding_dimensions)])
    if config.require_services:
        command.append("--require-services")
    return command


def build_runner_env(
    config: ModalSmokeConfig,
    *,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    env = dict(os.environ if base_env is None else base_env)
    env["BENCH_DISABLE_ENV_FILE"] = "1"
    if config.postgres_control:
        env[POSTGRES_DSN_ENV] = postgres_control_dsn(config)
    elif config.require_services and target_needs_postgres(config) and not env.get(
        POSTGRES_DSN_ENV
    ):
        raise ValueError(f"{POSTGRES_DSN_ENV} is required for required PostgreSQL control runs")
    if config.pgvector_control:
        env[PGVECTOR_DSN_ENV] = pgvector_control_dsn(config)
    elif config.require_services and target_needs_pgvector(config) and not env.get(
        PGVECTOR_DSN_ENV
    ):
        raise ValueError(f"{PGVECTOR_DSN_ENV} is required for required pgvector control runs")
    if config.tracedb_engine_control:
        env[TRACEDB_HTTP_URL_ENV] = tracedb_engine_http_url(config)
        env[TRACEDB_HTTP_DATA_DIR_ENV] = str(tracedb_engine_data_dir(config))
    return env


def postgres_control_dsn(config: ModalSmokeConfig) -> str:
    return f"postgresql://tracedb:tracedb@127.0.0.1:{config.postgres_port}/tracedb_bench"


def pgvector_control_dsn(config: ModalSmokeConfig) -> str:
    return f"postgresql://tracedb:tracedb@127.0.0.1:{config.pgvector_port}/tracedb_bench"


def tracedb_engine_http_url(config: ModalSmokeConfig) -> str:
    return f"http://127.0.0.1:{config.tracedb_port}"


def tracedb_engine_data_dir(config: ModalSmokeConfig) -> Path:
    return Path("/tmp") / f"tracedb-engine-{config.run_id}"


def redacted_env(env: Mapping[str, str]) -> dict[str, str]:
    redacted = {}
    for key, value in env.items():
        if key in SENSITIVE_ENV_KEYS:
            redacted[key] = "[redacted]"
        elif key.startswith("BENCH_") or key.startswith("OPENROUTER_") or key.startswith("TRACEDB_"):
            redacted[key] = value
    return redacted


def git_identity(repo_root: Path) -> dict[str, Any]:
    def git_output(*args: str) -> str:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
        return completed.stdout.strip()

    try:
        commit = git_output("rev-parse", "HEAD")
        status_short = git_output("status", "--short")
    except (OSError, RuntimeError) as error:
        return {
            "commit": None,
            "dirty": None,
            "status_short": None,
            "error": str(error),
        }
    return {
        "commit": commit,
        "dirty": bool(status_short),
        "status_short": status_short,
    }


def config_git_identity(config: ModalSmokeConfig, repo_root: Path) -> dict[str, Any]:
    if (
        config.source_commit is not None
        or config.source_dirty is not None
        or config.source_status_short is not None
        or config.source_git_error is not None
    ):
        identity = {
            "commit": config.source_commit,
            "dirty": config.source_dirty,
            "status_short": config.source_status_short,
        }
        if config.source_git_error is not None:
            identity["error"] = config.source_git_error
        return identity
    return git_identity(repo_root)


def build_manifest(
    config: ModalSmokeConfig,
    suite_command: list[str],
    *,
    repo_root: Path = REPO_ROOT,
    runner_env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "kind": "tracedb_modal_smoke",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_id": config.run_id,
        "modal_app_name": config.modal_app_name,
        "repo_root": str(repo_root),
        "git": config_git_identity(config, repo_root),
        "suite_command": suite_command,
        "modal_resource_class": {
            "cpu": config.cpu,
            "memory_mb": config.memory_mb,
            "timeout_seconds": config.timeout_seconds,
            "ephemeral_disk_mb": config.ephemeral_disk_mb,
            "gpu_requested": config.gpu_requested,
        },
        "config": asdict(config),
        "runner_env": redacted_env(runner_env or {}),
    }


def bundle_report_artifacts(
    *,
    run_id: str,
    reports_dir: Path,
    bundle_dir: Path,
    manifest: dict[str, Any],
) -> Path:
    run_dir = reports_dir / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"report run directory not found: {run_dir}")
    bundle_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle = bundle_dir / f"{run_id}.tar.gz"
    with tarfile.open(bundle, "w:gz") as archive:
        archive.add(run_dir, arcname=run_id)
    return bundle


def extract_control_summary(bundle_path: Path, run_id: str) -> dict[str, Any]:
    suite_member = f"{run_id}/suite.json"
    with tarfile.open(bundle_path, "r:gz") as archive:
        extracted = archive.extractfile(suite_member)
        if extracted is None:
            raise FileNotFoundError(f"{suite_member} not found in {bundle_path}")
        suite = json.loads(extracted.read().decode("utf-8"))
    ledger = suite.get("control_ledger", {})
    scenario_baselines, scenario_datasets = extract_scenario_metrics(suite)
    return {
        "run_id": suite.get("suite_id", run_id),
        "control_status": suite.get("control_status", "unknown"),
        "failure_count": suite.get("summary", {}).get("failure_count", 0),
        "available_external_controls": [
            control.get("name")
            for control in ledger.get("available_external_controls", [])
        ],
        "unavailable_external_controls": [
            control.get("name")
            for control in ledger.get("unavailable_external_controls", [])
        ],
        "number_to_beat": suite.get("number_to_beat", {}),
        "scenario_baselines": scenario_baselines,
        "scenario_datasets": scenario_datasets,
        "suite_json": suite_member,
        "bundle_path": str(bundle_path),
    }


def extract_scenario_metrics(suite: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    baselines: dict[str, Any] = {}
    datasets: dict[str, Any] = {}
    for scenario in suite.get("scenarios", []):
        scenario_id = scenario.get("id", "unknown")
        datasets[scenario_id] = scenario.get("dataset", {})
        baselines[scenario_id] = {}
        for baseline in scenario.get("baselines", []):
            name = baseline.get("name")
            if not name:
                continue
            baselines[scenario_id][name] = {
                "available": baseline.get("available", False),
                "metrics": baseline.get("metrics", {}),
                "notes": baseline.get("notes", []),
            }
    return baselines, datasets


def run_suite_and_bundle(config: ModalSmokeConfig, *, lab_root: Path = LAB_ROOT) -> dict[str, Any]:
    validate_config(config)
    _ensure_free_space(Path(config.bundle_dir), config.min_free_mb)
    reports_dir = Path(config.reports_dir)
    bundle_dir = Path(config.bundle_dir)
    command = build_suite_command(config)
    env = build_runner_env(config)
    tracedb_service: TraceDbEngineControl | None = None
    postgres_service: PostgresControl | None = None
    pgvector_service: PostgresControl | None = None
    try:
        if config.tracedb_engine_control:
            tracedb_service = start_tracedb_engine_control(
                config,
                repo_root=lab_root.parent.parent,
            )
        if config.postgres_control:
            postgres_service = start_postgres_control(config)
        if config.pgvector_control:
            pgvector_service = start_pgvector_control(config)
        completed = subprocess.run(
            command,
            cwd=lab_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    finally:
        if pgvector_service is not None:
            stop_postgres_control(pgvector_service)
        if postgres_service is not None:
            stop_postgres_control(postgres_service)
        if tracedb_service is not None:
            stop_tracedb_engine_control(tracedb_service)
    manifest = build_manifest(config, command, repo_root=lab_root.parent.parent, runner_env=env)
    manifest["process"] = {
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
    }
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr or completed.stdout)
    bundle = bundle_report_artifacts(
        run_id=config.run_id,
        reports_dir=reports_dir,
        bundle_dir=bundle_dir,
        manifest=manifest,
    )
    summary = extract_control_summary(bundle, config.run_id)
    summary["manifest"] = manifest
    return summary


@dataclass(frozen=True)
class PostgresControl:
    data_dir: Path
    log_path: Path
    port: int


@dataclass(frozen=True)
class TraceDbEngineControl:
    data_dir: Path
    log_path: Path
    port: int
    process: subprocess.Popen[str]


def start_tracedb_engine_control(
    config: ModalSmokeConfig,
    *,
    repo_root: Path,
) -> TraceDbEngineControl:
    data_dir = tracedb_engine_data_dir(config)
    log_path = Path("/tmp") / f"tracedb-engine-{config.run_id}.log"
    binary = tracedb_server_binary(repo_root)
    if not binary.exists():
        raise RuntimeError(f"tracedb-server binary not found at {binary}")
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)
    env = os.environ.copy()
    env["TRACEDB_DATA_DIR"] = str(data_dir)
    env["TRACEDB_BIND"] = f"127.0.0.1:{config.tracedb_port}"
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [str(binary)],
            cwd=repo_root,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
    try:
        wait_for_http_ready(tracedb_engine_http_url(config))
    except Exception:
        stop_tracedb_engine_control(
            TraceDbEngineControl(
                data_dir=data_dir,
                log_path=log_path,
                port=config.tracedb_port,
                process=process,
            )
        )
        raise RuntimeError(f"TraceDB engine failed to become ready; log tail: {tail_file(log_path)}")
    return TraceDbEngineControl(
        data_dir=data_dir,
        log_path=log_path,
        port=config.tracedb_port,
        process=process,
    )


def tracedb_server_binary(repo_root: Path) -> Path:
    override = os.environ.get("TRACEDB_SERVER_BIN")
    if override:
        return Path(override)
    release_binary = repo_root / "target" / "release" / "tracedb-server"
    if release_binary.exists():
        return release_binary
    return repo_root / "target" / "debug" / "tracedb-server"


def stop_tracedb_engine_control(service: TraceDbEngineControl) -> None:
    if service.process.poll() is not None:
        return
    service.process.terminate()
    try:
        service.process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        service.process.kill()
        service.process.wait(timeout=10)


def wait_for_http_ready(base_url: str, *, timeout_seconds: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/ready", timeout=1.0) as response:
                if 200 <= response.status < 300:
                    return
        except Exception as error:  # pragma: no cover - exercised in Modal.
            last_error = error
        time.sleep(0.1)
    raise TimeoutError(f"{base_url}/ready did not respond before timeout: {last_error}")


def tail_file(path: Path, *, limit: int = 2000) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[-limit:]
    except OSError as error:
        return str(error)


def start_postgres_control(config: ModalSmokeConfig) -> PostgresControl:
    return start_postgres_service(
        run_id=config.run_id,
        port=config.postgres_port,
        service_name="postgres",
    )


def start_pgvector_control(config: ModalSmokeConfig) -> PostgresControl:
    return start_postgres_service(
        run_id=config.run_id,
        port=config.pgvector_port,
        service_name="pgvector",
    )


def start_postgres_service(*, run_id: str, port: int, service_name: str) -> PostgresControl:
    data_dir = Path("/tmp") / f"tracedb-{service_name}-{run_id}"
    log_path = Path("/tmp") / f"tracedb-{service_name}-{run_id}.log"
    bin_dir = postgres_bin_dir()
    initdb = bin_dir / "initdb"
    pg_ctl = bin_dir / "pg_ctl"
    createdb = shutil.which("createdb")
    if createdb is None:
        raise RuntimeError("createdb not found; install postgresql-client in the Modal image")
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)
    if _running_as_root():
        subprocess.run(
            ["chown", "-R", "postgres:postgres", str(data_dir)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    _run_maybe_as_postgres(
        [
            str(initdb),
            "-D",
            str(data_dir),
            "-U",
            "tracedb",
            "--auth=trust",
            "--no-instructions",
        ]
    )
    _run_maybe_as_postgres(
        [
            str(pg_ctl),
            "-D",
            str(data_dir),
            "-l",
            str(log_path),
            "-o",
            f"-h 127.0.0.1 -p {port}",
            "-w",
            "-t",
            "30",
            "start",
        ]
    )
    subprocess.run(
        [
            createdb,
            "-h",
            "127.0.0.1",
            "-p",
            str(port),
            "-U",
            "tracedb",
            "tracedb_bench",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    return PostgresControl(data_dir=data_dir, log_path=log_path, port=port)


def stop_postgres_control(service: PostgresControl) -> None:
    bin_dir = postgres_bin_dir()
    pg_ctl = bin_dir / "pg_ctl"
    _run_maybe_as_postgres(
        [str(pg_ctl), "-D", str(service.data_dir), "-m", "fast", "-w", "stop"],
        check=False,
    )


def postgres_bin_dir() -> Path:
    candidates = sorted(glob.glob("/usr/lib/postgresql/*/bin/initdb"))
    if candidates:
        return Path(candidates[-1]).parent
    initdb = shutil.which("initdb")
    if initdb is not None:
        return Path(initdb).parent
    raise RuntimeError("initdb not found; install postgresql in the Modal image")


def _run_maybe_as_postgres(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    if _running_as_root():
        shell_command = " ".join(shlex.quote(part) for part in command)
        command = ["su", "postgres", "-c", shell_command]
    return subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=check,
    )


def _running_as_root() -> bool:
    return hasattr(os, "geteuid") and os.geteuid() == 0


def _ensure_free_space(path: Path, min_free_mb: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    usage = os.statvfs(path)
    free_mb = (usage.f_bavail * usage.f_frsize) // (1024 * 1024)
    if free_mb < min_free_mb:
        raise RuntimeError(f"free disk {free_mb} MiB is below required {min_free_mb} MiB")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a cost-guarded TraceDB Modal smoke benchmark")
    parser.add_argument("--run-id", default="modal-smoke")
    parser.add_argument("--records", type=int, default=128)
    parser.add_argument("--dataset", default="generated")
    parser.add_argument("--target", default="tracedb")
    parser.add_argument("--surface", default="sdk")
    parser.add_argument("--scenarios", default="sdk_cli_surface")
    parser.add_argument("--openrouter-mode", default="off", choices=["auto", "off", "required"])
    parser.add_argument("--openrouter-cap", default="moderate")
    parser.add_argument(
        "--tracedb-ingest-mode",
        default="per_record",
        choices=["per_record", "batch"],
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-free-mb", type=int, default=20_000)
    parser.add_argument("--allow-large", action="store_true")
    parser.add_argument("--allow-external-controls", action="store_true")
    parser.add_argument("--allow-provider", action="store_true")
    parser.add_argument("--require-services", action="store_true")
    parser.add_argument("--tracedb-engine-control", action="store_true")
    parser.add_argument("--postgres-control", action="store_true")
    parser.add_argument("--pgvector-control", action="store_true")
    parser.add_argument("--tracedb-port", type=int, default=18_080)
    parser.add_argument("--postgres-port", type=int, default=25_432)
    parser.add_argument("--pgvector-port", type=int, default=25_433)
    parser.add_argument(
        "--summary-json",
        help="Write the returned Modal benchmark summary to a clean local JSON file.",
    )
    return parser


def _config_from_args(args: argparse.Namespace) -> ModalSmokeConfig:
    return ModalSmokeConfig(
        run_id=args.run_id,
        records=args.records,
        dataset=args.dataset,
        target=args.target,
        surface=args.surface,
        scenarios=args.scenarios,
        openrouter_mode=args.openrouter_mode,
        openrouter_cap=args.openrouter_cap,
        tracedb_ingest_mode=args.tracedb_ingest_mode,
        seed=args.seed,
        min_free_mb=args.min_free_mb,
        allow_large=args.allow_large,
        allow_external_controls=args.allow_external_controls,
        allow_provider=args.allow_provider,
        require_services=args.require_services,
        tracedb_engine_control=args.tracedb_engine_control,
        postgres_control=args.postgres_control,
        pgvector_control=args.pgvector_control,
        tracedb_port=args.tracedb_port,
        postgres_port=args.postgres_port,
        pgvector_port=args.pgvector_port,
        modal_app_name=modal_app_name(),
        modal_image_kind=modal_image_kind_from_flags(
            tracedb_engine_control=args.tracedb_engine_control,
            pgvector_control=args.pgvector_control,
            postgres_control=args.postgres_control,
        ),
    )


def _parse_args_with_summary_output(
    argv: list[str] | None = None,
) -> tuple[ModalSmokeConfig, str | None]:
    args = _build_parser().parse_args(argv)
    return _config_from_args(args), args.summary_json


def _parse_args(argv: list[str] | None = None) -> ModalSmokeConfig:
    config, _summary_json = _parse_args_with_summary_output(argv)
    return config


def write_summary_json(summary: dict[str, Any], summary_json: str | None) -> None:
    if not summary_json:
        return
    output_path = Path(summary_json).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def source_git_kwargs(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    identity = git_identity(repo_root)
    return {
        "source_commit": identity.get("commit"),
        "source_dirty": identity.get("dirty"),
        "source_status_short": identity.get("status_short"),
        "source_git_error": identity.get("error"),
    }


def run_local(argv: list[str] | None = None) -> int:
    config, summary_json = _parse_args_with_summary_output(argv)
    summary = run_suite_and_bundle(config)
    write_summary_json(summary, summary_json)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if modal is not None:
    BASE_APT_PACKAGES = (
        "build-essential",
        "ca-certificates",
        "curl",
        "pkg-config",
        "libssl-dev",
    )

    def modal_base_image(*extra_packages: str) -> modal.Image:
        return (
            modal.Image.debian_slim(python_version="3.12")
            .apt_install(*BASE_APT_PACKAGES, *extra_packages)
            .pip_install_from_requirements(str(LAB_ROOT / "requirements.txt"))
        )

    def rust_modal_base_image(*extra_packages: str) -> modal.Image:
        return (
            modal.Image.from_registry(RUST_MODAL_IMAGE, add_python="3.12")
            .apt_install(*BASE_APT_PACKAGES, *extra_packages)
            .pip_install_from_requirements(str(LAB_ROOT / "requirements.txt"))
        )

    def add_repo_source(base_image: modal.Image) -> modal.Image:
        return base_image.add_local_dir(
            str(REPO_ROOT),
            remote_path=REMOTE_REPO,
            ignore=MODAL_IGNORE_PATTERNS,
        )

    def add_repo_source_for_build(base_image: modal.Image) -> modal.Image:
        return base_image.add_local_dir(
            str(REPO_ROOT),
            remote_path=REMOTE_REPO,
            ignore=MODAL_IGNORE_PATTERNS,
            copy=True,
        )

    def modal_image(*extra_packages: str) -> modal.Image:
        return add_repo_source(modal_base_image(*extra_packages))

    def tracedb_engine_image(*extra_packages: str) -> modal.Image:
        return add_repo_source_for_build(
            rust_modal_base_image(*extra_packages)
        ).run_commands(
            f"cd {REMOTE_REPO} && cargo build --release -p tracedb-server"
        )

    def pgvector_control_image() -> modal.Image:
        return add_repo_source(
            modal_base_image(
                "git",
                "postgresql",
                "postgresql-client",
                "postgresql-server-dev-all",
            ).run_commands(
                "cd /tmp && "
                f"git clone --branch {PGVECTOR_VERSION} --depth 1 https://github.com/pgvector/pgvector.git && "
                "cd pgvector && "
                "make && "
                "make install && "
                "rm -rf /tmp/pgvector"
            )
        )

    def tracedb_pgvector_control_image() -> modal.Image:
        return add_repo_source_for_build(
            rust_modal_base_image(
                "git",
                "postgresql",
                "postgresql-client",
                "postgresql-server-dev-all",
            ).run_commands(
                "cd /tmp && "
                f"git clone --branch {PGVECTOR_VERSION} --depth 1 https://github.com/pgvector/pgvector.git && "
                "cd pgvector && "
                "make && "
                "make install && "
                "rm -rf /tmp/pgvector"
            )
        ).run_commands(
            f"cd {REMOTE_REPO} && cargo build --release -p tracedb-server"
        )

    def selected_modal_image(kind: str) -> modal.Image:
        kind = validate_modal_image_kind(kind)
        if kind == "tracedb_pgvector":
            return tracedb_pgvector_control_image()
        if kind == "tracedb":
            return tracedb_engine_image()
        if kind == "pgvector":
            return pgvector_control_image()
        if kind == "postgres":
            return modal_image("postgresql", "postgresql-client")
        return modal_image()

    selected_image_kind = modal_image_kind_from_args(sys.argv)
    image = selected_modal_image(selected_image_kind)
    app = modal.App(modal_app_name())

    @app.function(
        image=image,
        cpu=ModalSmokeConfig.cpu,
        memory=ModalSmokeConfig.memory_mb,
        timeout=ModalSmokeConfig.timeout_seconds,
        ephemeral_disk=ModalSmokeConfig.ephemeral_disk_mb,
    )
    def run_smoke_remote(**kwargs: Any) -> dict[str, Any]:
        config = ModalSmokeConfig(**kwargs)
        return run_suite_and_bundle(config, lab_root=Path(REMOTE_REPO) / "benchmarks" / "realworld")

    @app.local_entrypoint()
    def main(
        run_id: str = "modal-smoke",
        records: int = 128,
        dataset: str = "generated",
        target: str = "tracedb",
        surface: str = "sdk",
        scenarios: str = "sdk_cli_surface",
        openrouter_mode: str = "off",
        tracedb_ingest_mode: str = "per_record",
        seed: int = 42,
        allow_external_controls: bool = False,
        require_services: bool = False,
        tracedb_engine_control: bool = False,
        postgres_control: bool = False,
        pgvector_control: bool = False,
        allow_large: bool = False,
        allow_provider: bool = False,
        summary_json: str = "",
    ) -> None:
        result = run_smoke_remote.remote(
            run_id=run_id,
            records=records,
            dataset=dataset,
            target=target,
            surface=surface,
            scenarios=scenarios,
            openrouter_mode=openrouter_mode,
            tracedb_ingest_mode=tracedb_ingest_mode,
            seed=seed,
            allow_external_controls=allow_external_controls,
            require_services=require_services,
            tracedb_engine_control=tracedb_engine_control,
            postgres_control=postgres_control,
            pgvector_control=pgvector_control,
            allow_large=allow_large,
            allow_provider=allow_provider,
            modal_app_name=modal_app_name(),
            modal_image_kind=selected_image_kind,
            **source_git_kwargs(),
        )
        write_summary_json(result, summary_json)
        print(json.dumps(result, indent=2, sort_keys=True))
else:
    app = None


if __name__ == "__main__":
    raise SystemExit(run_local())
