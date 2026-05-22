from __future__ import annotations

import argparse
import base64
import glob
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

try:
    import modal
except ImportError:  # pragma: no cover - local tests should not require Modal.
    modal = None

from runner.suite_spec import load_suite_spec, select_suite_baseline_json


LAB_ROOT = Path(__file__).resolve().parent
REPO_ROOT = LAB_ROOT.parent.parent
REMOTE_REPO = "/workspace/TraceDB"
DEFAULT_REPORTS_DIR = "/tmp/tracedb-modal-reports"
DEFAULT_BUNDLE_DIR = "/tmp/tracedb-modal-bundles"
MODAL_INPUT_ARTIFACTS_DIR = ".modal-input-artifacts"
DEFAULT_MAX_RECORDS = 1000
MODAL_INPUT_ARTIFACT_FIELDS = (
    (
        "suite_baseline_json",
        "suite_baseline",
        "suite-baseline.json",
    ),
    (
        "railway_persistence_pre_manifest_json",
        "railway_persistence_pre_manifest",
        "railway-persistence-pre-manifest.json",
    ),
    (
        "railway_operation_receipt_json",
        "railway_operation_receipt",
        "railway-operation-receipt.json",
    ),
    (
        "railway_backup_receipt_json",
        "railway_backup_receipt",
        "railway-backup-receipt.json",
    ),
    (
        "railway_runbook_verification_json",
        "railway_runbook_verification",
        "railway-runbook-verification.json",
    ),
)
MODAL_MIN_EPHEMERAL_DISK_MB = 524_288
DEFAULT_MODAL_APP_NAME = "tracedb-realworld-smoke"
MODAL_APP_NAME_ENV = "TRACEDB_MODAL_APP_NAME"
MODAL_IMAGE_KIND_ENV = "TRACEDB_MODAL_IMAGE_KIND"
DEFAULT_BUNDLE_EXPORT_MAX_MB = 64
BUNDLE_BYTES_FIELD = "bundle_bytes_b64"
BUNDLE_SHA256_FIELD = "bundle_bytes_sha256"
BUNDLE_SIZE_FIELD = "bundle_bytes_size"
RUST_MODAL_IMAGE = "rust:1.94-bookworm"
PGVECTOR_VERSION = "v0.8.2"
QDRANT_VERSION = "v1.13.4"
QDRANT_RELEASE_URL = (
    f"https://github.com/qdrant/qdrant/releases/download/{QDRANT_VERSION}/"
    "qdrant-x86_64-unknown-linux-musl.tar.gz"
)
OPENSEARCH_VERSION = "2.18.0"
OPENSEARCH_RELEASE_URL = (
    "https://artifacts.opensearch.org/releases/bundle/opensearch/"
    f"{OPENSEARCH_VERSION}/opensearch-{OPENSEARCH_VERSION}-linux-x64.tar.gz"
)
MONGODB_VERSION = "8.0.23"
MONGODB_RELEASE_URL = (
    "https://fastdl.mongodb.org/linux/"
    f"mongodb-linux-x86_64-debian12-{MONGODB_VERSION}.tgz"
)
POSTGRES_DSN_ENV = "BENCH_POSTGRES_DSN"
PGVECTOR_DSN_ENV = "BENCH_PGVECTOR_DSN"
QDRANT_URL_ENV = "BENCH_QDRANT_URL"
QDRANT_STORAGE_DIR_ENV = "BENCH_QDRANT_STORAGE_DIR"
OPENSEARCH_URL_ENV = "BENCH_OPENSEARCH_URL"
OPENSEARCH_STORAGE_DIR_ENV = "BENCH_OPENSEARCH_STORAGE_DIR"
MONGO_URI_ENV = "BENCH_MONGO_URI"
MONGO_STORAGE_DIR_ENV = "BENCH_MONGO_STORAGE_DIR"
MILVUS_URI_ENV = "BENCH_MILVUS_URI"
MILVUS_TOKEN_ENV = "BENCH_MILVUS_TOKEN"
MILVUS_STORAGE_DIR_ENV = "BENCH_MILVUS_STORAGE_DIR"
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
    MONGO_URI_ENV,
    MILVUS_TOKEN_ENV,
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
SUITE_PRESETS: dict[str, dict[str, Any]] = {
    "platform_pr": {
        "suite_spec": "benchmarks/realworld/suites/platform_pr.json",
        "records": 128,
        "target": "tracedb",
        "surface": "sdk,cli,http,curl",
        "scenarios": "sdk_cli_surface,http_falsification",
        "tracedb_ingest_mode": "batch",
    },
    "platform_push_10k": {
        "suite_spec": "benchmarks/realworld/suites/platform_push_10k.json",
        "records": 10000,
        "target": "tracedb,postgres,pgvector,mongodb,qdrant,opensearch",
        "surface": "sdk,cli,http,curl",
        "scenarios": "sdk_cli_surface,http_falsification,search_rag_6",
        "tracedb_ingest_mode": "batch",
        "allow_large": True,
        "allow_external_controls": True,
    },
    "railway_stateful": {
        "suite_spec": "benchmarks/realworld/suites/railway_stateful.json",
        "records": 1000,
        "target": "tracedb",
        "surface": "http,curl",
        "scenarios": "http_falsification",
        "tracedb_ingest_mode": "batch",
        "railway_config_from_env": True,
        "railway_health_check": True,
        "railway_stateful_smoke": True,
        "railway_snapshot_restore_check": True,
        "railway_verify_restored_marker": True,
        "railway_restart_redeploy_plan": True,
    },
    "release_100k": {
        "suite_spec": "benchmarks/realworld/suites/release_100k.json",
        "records": 100000,
        "target": "tracedb,postgres,pgvector,mongodb,qdrant,opensearch",
        "surface": "sdk,cli,http,curl",
        "scenarios": "sdk_cli_surface,http_falsification,search_rag_6",
        "tracedb_ingest_mode": "batch",
        "allow_large": True,
        "allow_external_controls": True,
        "require_services": True,
        "railway_config_from_env": True,
        "railway_health_check": True,
        "railway_stateful_smoke": True,
        "railway_snapshot_restore_check": True,
        "railway_verify_restored_marker": True,
        "railway_restart_redeploy_plan": True,
        "railway_runbook_verification_required": True,
    },
    "soak_railway": {
        "suite_spec": "benchmarks/realworld/suites/soak_railway.json",
        "records": 10000,
        "target": "tracedb,postgres,pgvector",
        "surface": "http,curl",
        "scenarios": "http_falsification,search_rag_6",
        "tracedb_ingest_mode": "batch",
        "allow_large": True,
        "allow_external_controls": True,
        "railway_config_from_env": True,
        "railway_health_check": True,
        "railway_stateful_smoke": True,
        "railway_snapshot_restore_check": True,
        "railway_verify_restored_marker": True,
        "railway_restart_redeploy_plan": True,
        "railway_runbook_verification_required": True,
    },
    "manual_1m": {
        "suite_spec": "benchmarks/realworld/suites/manual_1m.json",
        "records": 1000000,
        "target": "tracedb,pgvector,qdrant,opensearch",
        "surface": "http,curl",
        "scenarios": "http_falsification,search_rag_6",
        "tracedb_ingest_mode": "batch",
        "allow_large": True,
        "allow_external_controls": True,
        "require_services": True,
    },
}


@dataclass(frozen=True)
class ModalSmokeConfig:
    run_id: str = "modal-smoke"
    profile: str = "smoke"
    dataset: str = "generated"
    records: int = 128
    target: str = "tracedb"
    surface: str = "sdk"
    scenarios: str = "sdk_cli_surface"
    suite_spec: str = ""
    suite_baseline_json: str = ""
    suite_baseline_dir: str = ""
    regression_tolerance_pct: float = 15.0
    regression_tolerance_absolute: float = 0.0
    suite_preflight_only: bool = False
    railway_config_from_env: bool = False
    railway_health_check: bool = False
    railway_health_timeout_seconds: float = 5.0
    railway_stateful_smoke: bool = False
    railway_stateful_smoke_timeout_seconds: float = 5.0
    railway_stateful_marker_id: str = ""
    railway_stateful_read_only: bool = False
    railway_snapshot_restore_check: bool = False
    railway_snapshot_restore_timeout_seconds: float = 60.0
    railway_snapshot_root: str = ""
    railway_verify_restored_marker: bool = False
    railway_restart_redeploy_plan: bool = False
    railway_persistence_pre_manifest_json: str = ""
    railway_operation_receipt_json: str = ""
    railway_backup_receipt_json: str = ""
    railway_runbook_verification_json: str = ""
    railway_runbook_verification_required: bool = False
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
    qdrant_control: bool = False
    opensearch_control: bool = False
    mongodb_control: bool = False
    milvus_control: bool = False
    tracedb_port: int = 18_080
    postgres_port: int = 25_432
    pgvector_port: int = 25_433
    qdrant_port: int = 26_333
    opensearch_port: int = 29_200
    mongodb_port: int = 27_027
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
    if config.qdrant_control and not config.allow_external_controls:
        raise ValueError("qdrant_control needs allow_external_controls=True")
    if config.qdrant_control and not target_needs_qdrant(config):
        raise ValueError("qdrant_control needs target including qdrant or all")
    if config.opensearch_control and not config.allow_external_controls:
        raise ValueError("opensearch_control needs allow_external_controls=True")
    if config.opensearch_control and not target_needs_opensearch(config):
        raise ValueError("opensearch_control needs target including opensearch or all")
    if config.mongodb_control and not config.allow_external_controls:
        raise ValueError("mongodb_control needs allow_external_controls=True")
    if config.mongodb_control and not target_needs_mongodb(config):
        raise ValueError("mongodb_control needs target including mongodb or all")
    if config.milvus_control and not config.allow_external_controls:
        raise ValueError("milvus_control needs allow_external_controls=True")
    if config.milvus_control and not target_needs_milvus(config):
        raise ValueError("milvus_control needs target including milvus or all")
    ports = []
    if config.tracedb_engine_control:
        ports.append(("tracedb_engine_control", config.tracedb_port))
    if config.postgres_control:
        ports.append(("postgres_control", config.postgres_port))
    if config.pgvector_control:
        ports.append(("pgvector_control", config.pgvector_port))
    if config.qdrant_control:
        ports.append(("qdrant_control", config.qdrant_port))
        ports.append(("qdrant_grpc_control", config.qdrant_port + 1))
    if config.opensearch_control:
        ports.append(("opensearch_control", config.opensearch_port))
        ports.append(("opensearch_transport_control", config.opensearch_port + 100))
    if config.mongodb_control:
        ports.append(("mongodb_control", config.mongodb_port))
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
    qdrant_control: bool = False,
    opensearch_control: bool = False,
    mongodb_control: bool = False,
    milvus_control: bool = False,
) -> str:
    external_count = sum(
        int(enabled)
        for enabled in (
            postgres_control,
            pgvector_control,
            qdrant_control,
            opensearch_control,
            mongodb_control,
            milvus_control,
        )
    )
    if tracedb_engine_control and external_count > 1:
        return "tracedb_controls"
    if not tracedb_engine_control and external_count > 1:
        return "external_controls"
    if tracedb_engine_control and pgvector_control:
        return "tracedb_pgvector"
    if tracedb_engine_control and qdrant_control:
        return "tracedb_qdrant"
    if tracedb_engine_control and opensearch_control:
        return "tracedb_opensearch"
    if tracedb_engine_control and mongodb_control:
        return "tracedb_mongodb"
    if tracedb_engine_control and milvus_control:
        return "tracedb_milvus"
    if tracedb_engine_control:
        return "tracedb"
    if pgvector_control:
        return "pgvector"
    if qdrant_control:
        return "qdrant"
    if opensearch_control:
        return "opensearch"
    if mongodb_control:
        return "mongodb"
    if milvus_control:
        return "milvus"
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
        qdrant_control="--qdrant-control" in argv,
        opensearch_control="--opensearch-control" in argv,
        mongodb_control="--mongodb-control" in argv,
        milvus_control="--milvus-control" in argv,
    )


def validate_modal_image_kind(kind: str) -> str:
    valid = {
        "base",
        "postgres",
        "pgvector",
        "qdrant",
        "opensearch",
        "mongodb",
        "milvus",
        "tracedb",
        "tracedb_pgvector",
        "tracedb_qdrant",
        "tracedb_opensearch",
        "tracedb_mongodb",
        "tracedb_milvus",
        "external_controls",
        "tracedb_controls",
    }
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


def target_needs_qdrant(config: ModalSmokeConfig) -> bool:
    targets = requested_targets(config.target)
    return "all" in targets or "qdrant" in targets


def target_needs_opensearch(config: ModalSmokeConfig) -> bool:
    targets = requested_targets(config.target)
    return "all" in targets or "opensearch" in targets


def target_needs_mongodb(config: ModalSmokeConfig) -> bool:
    targets = requested_targets(config.target)
    return "all" in targets or "mongodb" in targets


def target_needs_milvus(config: ModalSmokeConfig) -> bool:
    targets = requested_targets(config.target)
    return "all" in targets or "milvus" in targets


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
    ]
    if config.suite_preflight_only:
        command.append("--preflight-only")
    if config.suite_spec:
        command.extend(["--suite-spec", config.suite_spec])
    if config.suite_baseline_json:
        command.extend(["--suite-baseline-json", config.suite_baseline_json])
        command.extend(["--regression-tolerance-pct", str(config.regression_tolerance_pct)])
        command.extend(
            ["--regression-tolerance-absolute", str(config.regression_tolerance_absolute)]
        )
    elif config.suite_baseline_dir:
        command.extend(["--suite-baseline-dir", config.suite_baseline_dir])
        command.extend(["--regression-tolerance-pct", str(config.regression_tolerance_pct)])
        command.extend(
            ["--regression-tolerance-absolute", str(config.regression_tolerance_absolute)]
        )
    if config.railway_config_from_env:
        command.append("--railway-config-from-env")
    if config.railway_health_check:
        command.append("--railway-health-check")
        command.extend(
            ["--railway-health-timeout-seconds", str(config.railway_health_timeout_seconds)]
        )
    if config.railway_stateful_smoke:
        command.append("--railway-stateful-smoke")
        command.extend(
            [
                "--railway-stateful-smoke-timeout-seconds",
                str(config.railway_stateful_smoke_timeout_seconds),
            ]
        )
        if config.railway_stateful_read_only:
            command.append("--railway-stateful-read-only")
        if config.railway_stateful_marker_id:
            command.extend(["--railway-stateful-marker-id", config.railway_stateful_marker_id])
    if config.railway_snapshot_restore_check:
        command.append("--railway-snapshot-restore-check")
        command.extend(
            [
                "--railway-snapshot-restore-timeout-seconds",
                str(config.railway_snapshot_restore_timeout_seconds),
            ]
        )
        if config.railway_snapshot_root:
            command.extend(["--railway-snapshot-root", config.railway_snapshot_root])
        if config.railway_verify_restored_marker:
            command.append("--railway-verify-restored-marker")
    if config.railway_restart_redeploy_plan:
        command.append("--railway-restart-redeploy-plan")
    if config.railway_persistence_pre_manifest_json:
        command.extend(
            [
                "--railway-persistence-pre-manifest-json",
                config.railway_persistence_pre_manifest_json,
            ]
        )
    if config.railway_operation_receipt_json:
        command.extend(["--railway-operation-receipt-json", config.railway_operation_receipt_json])
    if config.railway_backup_receipt_json:
        command.extend(["--railway-backup-receipt-json", config.railway_backup_receipt_json])
    if config.railway_runbook_verification_json:
        command.extend(
            [
                "--railway-runbook-verification-json",
                config.railway_runbook_verification_json,
            ]
        )
    if config.railway_runbook_verification_required:
        command.append("--railway-require-runbook-verification")
    command.extend(["--scenarios", config.scenarios])
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
    if config.qdrant_control:
        env[QDRANT_URL_ENV] = qdrant_control_url(config)
        env[QDRANT_STORAGE_DIR_ENV] = str(qdrant_control_data_dir(config))
    elif config.require_services and target_needs_qdrant(config) and not env.get(QDRANT_URL_ENV):
        raise ValueError(f"{QDRANT_URL_ENV} is required for required Qdrant control runs")
    if config.opensearch_control:
        env[OPENSEARCH_URL_ENV] = opensearch_control_url(config)
        env[OPENSEARCH_STORAGE_DIR_ENV] = str(opensearch_control_data_dir(config))
    elif config.require_services and target_needs_opensearch(config) and not env.get(
        OPENSEARCH_URL_ENV
    ):
        raise ValueError(f"{OPENSEARCH_URL_ENV} is required for required OpenSearch control runs")
    if config.mongodb_control:
        env[MONGO_URI_ENV] = mongodb_control_uri(config)
        env[MONGO_STORAGE_DIR_ENV] = str(mongodb_control_data_dir(config))
    elif config.require_services and target_needs_mongodb(config) and not env.get(MONGO_URI_ENV):
        raise ValueError(f"{MONGO_URI_ENV} is required for required MongoDB control runs")
    if config.milvus_control:
        env[MILVUS_URI_ENV] = milvus_control_uri(config)
        env[MILVUS_STORAGE_DIR_ENV] = str(milvus_control_data_dir(config))
    elif config.require_services and target_needs_milvus(config) and not env.get(MILVUS_URI_ENV):
        raise ValueError(f"{MILVUS_URI_ENV} is required for required Milvus control runs")
    if config.tracedb_engine_control:
        env[TRACEDB_HTTP_URL_ENV] = tracedb_engine_http_url(config)
        env[TRACEDB_HTTP_DATA_DIR_ENV] = str(tracedb_engine_data_dir(config))
    return env


def postgres_control_dsn(config: ModalSmokeConfig) -> str:
    return f"postgresql://tracedb:tracedb@127.0.0.1:{config.postgres_port}/tracedb_bench"


def pgvector_control_dsn(config: ModalSmokeConfig) -> str:
    return f"postgresql://tracedb:tracedb@127.0.0.1:{config.pgvector_port}/tracedb_bench"


def qdrant_control_url(config: ModalSmokeConfig) -> str:
    return f"http://127.0.0.1:{config.qdrant_port}"


def qdrant_control_data_dir(config: ModalSmokeConfig) -> Path:
    return Path("/tmp") / f"tracedb-qdrant-{config.run_id}"


def opensearch_control_url(config: ModalSmokeConfig) -> str:
    return f"http://127.0.0.1:{config.opensearch_port}"


def opensearch_control_data_dir(config: ModalSmokeConfig) -> Path:
    return Path("/tmp") / f"tracedb-opensearch-{config.run_id}"


def mongodb_control_uri(config: ModalSmokeConfig) -> str:
    return f"mongodb://127.0.0.1:{config.mongodb_port}"


def mongodb_control_data_dir(config: ModalSmokeConfig) -> Path:
    return Path("/tmp") / f"tracedb-mongodb-{config.run_id}"


def milvus_control_data_dir(config: ModalSmokeConfig) -> Path:
    return Path("/tmp") / f"tracedb-milvus-{config.run_id}"


def milvus_control_uri(config: ModalSmokeConfig) -> str:
    return str(milvus_control_data_dir(config) / "milvus_lite.db")


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


def redact_sensitive_text(text: str, env: Mapping[str, str]) -> str:
    redacted = text
    for key in SENSITIVE_ENV_KEYS:
        value = env.get(key)
        if value:
            redacted = redacted.replace(value, "[redacted]")
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


def _safe_modal_artifact_segment(value: str) -> str:
    segment = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in value
    ).strip("._")
    return segment or "run"


def _resolve_local_artifact_path(path_text: str, *, lab_root: Path) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    candidates = [
        Path.cwd() / path,
        REPO_ROOT / path,
        lab_root / path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_modal_suite_baseline(
    config: ModalSmokeConfig,
    *,
    lab_root: Path = LAB_ROOT,
) -> tuple[ModalSmokeConfig, dict[str, Any] | None]:
    if config.suite_baseline_json or not config.suite_baseline_dir:
        return config, None
    baseline_dir = _resolve_local_artifact_path(config.suite_baseline_dir, lab_root=lab_root)
    suite_spec_id = _suite_spec_id_for_modal_config(config, lab_root=lab_root)
    selection = select_suite_baseline_json(
        baseline_dir,
        suite_id=config.run_id,
        suite_spec_id=suite_spec_id,
        dataset=config.dataset,
        records=config.records,
    )
    if selection is None:
        return config, None
    return replace(config, suite_baseline_json=str(selection["path"])), selection


def _suite_spec_id_for_modal_config(
    config: ModalSmokeConfig,
    *,
    lab_root: Path = LAB_ROOT,
) -> str:
    if not config.suite_spec:
        return "ad_hoc"
    suite_spec_path = _resolve_local_artifact_path(config.suite_spec, lab_root=lab_root)
    if suite_spec_path.exists():
        return load_suite_spec(suite_spec_path).id
    return Path(config.suite_spec).stem


def stage_modal_input_artifacts(
    config: ModalSmokeConfig,
    *,
    lab_root: Path = LAB_ROOT,
    remote_lab_root: Path = Path(REMOTE_REPO) / "benchmarks" / "realworld",
) -> tuple[ModalSmokeConfig, list[dict[str, Any]]]:
    """Copy local evidence inputs into a repo path mounted by Modal remote runs."""

    staged_artifacts: list[dict[str, Any]] = []
    replacements: dict[str, str] = {}
    run_segment = _safe_modal_artifact_segment(config.run_id)
    remote_prefix = str(remote_lab_root)
    for field_name, artifact_kind, artifact_filename in MODAL_INPUT_ARTIFACT_FIELDS:
        artifact_path_text = getattr(config, field_name)
        if not artifact_path_text:
            continue
        if artifact_path_text == remote_prefix or artifact_path_text.startswith(f"{remote_prefix}/"):
            continue

        source_path = _resolve_local_artifact_path(artifact_path_text, lab_root=lab_root)
        if not source_path.exists():
            raise FileNotFoundError(f"{artifact_kind} artifact not found: {artifact_path_text}")

        staged_path = (
            lab_root
            / MODAL_INPUT_ARTIFACTS_DIR
            / run_segment
            / artifact_filename
        )
        staged_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.resolve() != staged_path.resolve():
            shutil.copy2(source_path, staged_path)

        remote_path = (
            remote_lab_root
            / MODAL_INPUT_ARTIFACTS_DIR
            / run_segment
            / artifact_filename
        )
        replacements[field_name] = str(remote_path)
        staged_artifacts.append(
            {
                "kind": artifact_kind,
                "source_path": str(source_path),
                "staged_path": str(staged_path),
                "remote_path": str(remote_path),
                "size_bytes": staged_path.stat().st_size,
                "sha256": _file_sha256(staged_path),
            }
        )
    if not replacements:
        return config, staged_artifacts
    return replace(config, **replacements), staged_artifacts


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
    suite_gate_member = f"{run_id}/suite-gate.json"
    with tarfile.open(bundle_path, "r:gz") as archive:
        extracted = archive.extractfile(suite_member)
        if extracted is None:
            raise FileNotFoundError(f"{suite_member} not found in {bundle_path}")
        suite = json.loads(extracted.read().decode("utf-8"))
        try:
            gate_file = archive.extractfile(suite_gate_member)
        except KeyError:
            gate_file = None
        suite_gate = json.loads(gate_file.read().decode("utf-8")) if gate_file else {}
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
        "tracedb_attribution": suite.get("tracedb_attribution", []),
        "scenario_baselines": scenario_baselines,
        "scenario_datasets": scenario_datasets,
        "suite_json": suite_member,
        "suite_gate_json": suite_gate_member if suite_gate else None,
        "suite_gate_status": suite_gate.get("status"),
        "blocking_failures": suite_gate.get("blocking_failures", []),
        "warnings": suite_gate.get("warnings", []),
        "claim_status": suite_gate.get("claim_status", {}),
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
                "query_results": baseline.get("query_results", []),
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
    qdrant_service: QdrantControl | None = None
    opensearch_service: OpenSearchControl | None = None
    mongodb_service: MongoDbControl | None = None
    milvus_service: MilvusLiteControl | None = None
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
        if config.qdrant_control:
            qdrant_service = start_qdrant_control(config)
        if config.opensearch_control:
            opensearch_service = start_opensearch_control(config)
        if config.mongodb_control:
            mongodb_service = start_mongodb_control(config)
        if config.milvus_control:
            milvus_service = start_milvus_control(config)
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
        if milvus_service is not None:
            stop_milvus_control(milvus_service)
        if mongodb_service is not None:
            stop_mongodb_control(mongodb_service)
        if opensearch_service is not None:
            stop_opensearch_control(opensearch_service)
        if qdrant_service is not None:
            stop_qdrant_control(qdrant_service)
        if pgvector_service is not None:
            stop_postgres_control(pgvector_service)
        if postgres_service is not None:
            stop_postgres_control(postgres_service)
        if tracedb_service is not None:
            stop_tracedb_engine_control(tracedb_service)
    manifest = build_manifest(config, command, repo_root=lab_root.parent.parent, runner_env=env)
    manifest["process"] = {
        "returncode": completed.returncode,
        "stdout_tail": redact_sensitive_text(completed.stdout[-4000:], env),
        "stderr_tail": redact_sensitive_text(completed.stderr[-4000:], env),
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


@dataclass(frozen=True)
class QdrantControl:
    data_dir: Path
    log_path: Path
    port: int
    process: subprocess.Popen[str]


@dataclass(frozen=True)
class OpenSearchControl:
    data_dir: Path
    log_path: Path
    port: int
    process: subprocess.Popen[str]


@dataclass(frozen=True)
class MongoDbControl:
    data_dir: Path
    log_path: Path
    port: int
    process: subprocess.Popen[str]


@dataclass(frozen=True)
class MilvusLiteControl:
    data_dir: Path
    uri: str


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


def wait_for_qdrant_ready(base_url: str, *, timeout_seconds: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/readyz", timeout=1.0) as response:
                if 200 <= response.status < 300:
                    return
        except Exception as error:  # pragma: no cover - exercised in Modal.
            last_error = error
        time.sleep(0.1)
    raise TimeoutError(f"{base_url}/readyz did not respond before timeout: {last_error}")


def wait_for_opensearch_ready(base_url: str, *, timeout_seconds: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(base_url, timeout=1.0) as response:
                if 200 <= response.status < 300:
                    return
        except Exception as error:  # pragma: no cover - exercised in Modal.
            last_error = error
        time.sleep(0.25)
    raise TimeoutError(f"{base_url} did not respond before timeout: {last_error}")


def wait_for_mongodb_ready(uri: str, *, timeout_seconds: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        client = None
        try:
            import pymongo

            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=1000)
            client.admin.command("ping")
            return
        except Exception as error:  # pragma: no cover - exercised in Modal.
            last_error = error
        finally:
            if client is not None:
                client.close()
        time.sleep(0.1)
    raise TimeoutError(f"{uri} did not respond before timeout: {last_error}")


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


def start_qdrant_control(config: ModalSmokeConfig) -> QdrantControl:
    data_dir = qdrant_control_data_dir(config)
    snapshots_dir = Path("/tmp") / f"tracedb-qdrant-snapshots-{config.run_id}"
    log_path = Path("/tmp") / f"tracedb-qdrant-{config.run_id}.log"
    config_path = Path("/tmp") / f"tracedb-qdrant-{config.run_id}.yaml"
    binary = qdrant_binary()
    if data_dir.exists():
        shutil.rmtree(data_dir)
    if snapshots_dir.exists():
        shutil.rmtree(snapshots_dir)
    data_dir.mkdir(parents=True)
    snapshots_dir.mkdir(parents=True)
    config_path.write_text(
        "\n".join(
            [
                "log_level: INFO",
                "storage:",
                f"  storage_path: {data_dir}",
                f"  snapshots_path: {snapshots_dir}",
                "service:",
                "  host: 127.0.0.1",
                f"  http_port: {config.qdrant_port}",
                f"  grpc_port: {config.qdrant_port + 1}",
                "cluster:",
                "  enabled: false",
                "telemetry_disabled: true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [str(binary), "--config-path", str(config_path)],
            cwd=Path("/tmp"),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
    service = QdrantControl(
        data_dir=data_dir,
        log_path=log_path,
        port=config.qdrant_port,
        process=process,
    )
    try:
        wait_for_qdrant_ready(qdrant_control_url(config))
    except Exception:
        stop_qdrant_control(service)
        raise RuntimeError(f"Qdrant control failed to become ready; log tail: {tail_file(log_path)}")
    return service


def start_opensearch_control(config: ModalSmokeConfig) -> OpenSearchControl:
    data_dir = opensearch_control_data_dir(config)
    logs_dir = Path("/tmp") / f"tracedb-opensearch-logs-{config.run_id}"
    log_path = Path("/tmp") / f"tracedb-opensearch-{config.run_id}.log"
    config_path = opensearch_config_path()
    install_dir = opensearch_install_dir()
    if data_dir.exists():
        shutil.rmtree(data_dir)
    if logs_dir.exists():
        shutil.rmtree(logs_dir)
    data_dir.mkdir(parents=True)
    logs_dir.mkdir(parents=True)
    opensearch_chown(data_dir)
    opensearch_chown(logs_dir)
    config_path.write_text(
        "\n".join(
            [
                "cluster.name: tracedb-bench-opensearch",
                f"node.name: tracedb-opensearch-{config.run_id}",
                "network.host: 127.0.0.1",
                f"http.port: {config.opensearch_port}",
                f"transport.port: {config.opensearch_port + 100}",
                "discovery.type: single-node",
                "node.store.allow_mmap: false",
                f"path.data: {data_dir}",
                f"path.logs: {logs_dir}",
                "plugins.security.disabled: true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    opensearch_chown(config_path)
    env = os.environ.copy()
    env["OPENSEARCH_JAVA_OPTS"] = "-Xms512m -Xmx512m"
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [str(install_dir / "bin" / "opensearch")],
            cwd=install_dir,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            preexec_fn=opensearch_preexec_fn(),
        )
    service = OpenSearchControl(
        data_dir=data_dir,
        log_path=log_path,
        port=config.opensearch_port,
        process=process,
    )
    try:
        wait_for_opensearch_ready(opensearch_control_url(config))
    except Exception:
        stop_opensearch_control(service)
        raise RuntimeError(
            f"OpenSearch control failed to become ready; log tail: {tail_file(log_path)}"
        )
    return service


def start_mongodb_control(config: ModalSmokeConfig) -> MongoDbControl:
    data_dir = mongodb_control_data_dir(config)
    log_path = Path("/tmp") / f"tracedb-mongodb-{config.run_id}.log"
    binary = mongodb_binary()
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                str(binary),
                "--dbpath",
                str(data_dir),
                "--bind_ip",
                "127.0.0.1",
                "--port",
                str(config.mongodb_port),
                "--nounixsocket",
                "--quiet",
            ],
            cwd=Path("/tmp"),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
    service = MongoDbControl(
        data_dir=data_dir,
        log_path=log_path,
        port=config.mongodb_port,
        process=process,
    )
    try:
        wait_for_mongodb_ready(mongodb_control_uri(config))
    except Exception:
        stop_mongodb_control(service)
        raise RuntimeError(f"MongoDB control failed to become ready; log tail: {tail_file(log_path)}")
    return service


def opensearch_install_dir() -> Path:
    override = os.environ.get("OPENSEARCH_HOME")
    if override:
        return Path(override)
    default_dir = Path(f"/opt/opensearch-{OPENSEARCH_VERSION}")
    if default_dir.exists():
        return default_dir
    raise RuntimeError("OpenSearch install directory not found; install OpenSearch in the Modal image")


def opensearch_config_path() -> Path:
    return opensearch_install_dir() / "config" / "opensearch.yml"


def opensearch_preexec_fn():
    try:
        import grp
        import pwd
    except ImportError:  # pragma: no cover - Unix-only in Modal.
        return None
    try:
        user = pwd.getpwnam("opensearch")
        group = grp.getgrnam("opensearch")
    except KeyError:
        return None

    def drop_privileges() -> None:
        os.setgid(group.gr_gid)
        os.setuid(user.pw_uid)

    return drop_privileges


def opensearch_chown(path: Path) -> None:
    try:
        import grp
        import pwd
    except ImportError:  # pragma: no cover - Unix-only in Modal.
        return
    try:
        user = pwd.getpwnam("opensearch")
        group = grp.getgrnam("opensearch")
    except KeyError:
        return
    shutil.chown(path, user=user.pw_name, group=group.gr_name)


def qdrant_binary() -> Path:
    override = os.environ.get("QDRANT_BIN")
    if override:
        return Path(override)
    binary = shutil.which("qdrant")
    if binary is not None:
        return Path(binary)
    default_binary = Path("/usr/local/bin/qdrant")
    if default_binary.exists():
        return default_binary
    raise RuntimeError("qdrant binary not found; install Qdrant in the Modal image")


def stop_qdrant_control(service: QdrantControl) -> None:
    if service.process.poll() is not None:
        return
    service.process.terminate()
    try:
        service.process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        service.process.kill()
        service.process.wait(timeout=10)


def stop_opensearch_control(service: OpenSearchControl) -> None:
    if service.process.poll() is not None:
        return
    service.process.terminate()
    try:
        service.process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        service.process.kill()
        service.process.wait(timeout=10)


def mongodb_binary() -> Path:
    override = os.environ.get("MONGOD_BIN")
    if override:
        return Path(override)
    binary = shutil.which("mongod")
    if binary is not None:
        return Path(binary)
    for candidate in (Path("/usr/local/bin/mongod"), Path("/usr/bin/mongod")):
        if candidate.exists():
            return candidate
    raise RuntimeError("mongod binary not found; install MongoDB in the Modal image")


def stop_mongodb_control(service: MongoDbControl) -> None:
    if service.process.poll() is not None:
        return
    service.process.terminate()
    try:
        service.process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        service.process.kill()
        service.process.wait(timeout=10)


def start_milvus_control(config: ModalSmokeConfig) -> MilvusLiteControl:
    data_dir = milvus_control_data_dir(config)
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)
    return MilvusLiteControl(data_dir=data_dir, uri=milvus_control_uri(config))


def stop_milvus_control(_service: MilvusLiteControl) -> None:
    return None


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
    parser.add_argument("--suite-spec", default="")
    parser.add_argument("--suite-preset", choices=sorted(SUITE_PRESETS), default="")
    parser.add_argument("--suite-baseline-json", default="")
    parser.add_argument("--suite-baseline-dir", default="")
    parser.add_argument("--regression-tolerance-pct", type=float, default=15.0)
    parser.add_argument("--regression-tolerance-absolute", type=float, default=0.0)
    parser.add_argument("--suite-preflight-only", action="store_true")
    parser.add_argument("--railway-config-from-env", action="store_true")
    parser.add_argument("--railway-health-check", action="store_true")
    parser.add_argument("--railway-health-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--railway-stateful-smoke", action="store_true")
    parser.add_argument("--railway-stateful-smoke-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--railway-stateful-marker-id", default="")
    parser.add_argument("--railway-stateful-read-only", action="store_true")
    parser.add_argument("--railway-snapshot-restore-check", action="store_true")
    parser.add_argument("--railway-snapshot-restore-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--railway-snapshot-root", default="")
    parser.add_argument("--railway-verify-restored-marker", action="store_true")
    parser.add_argument("--railway-restart-redeploy-plan", action="store_true")
    parser.add_argument("--railway-persistence-pre-manifest-json", default="")
    parser.add_argument("--railway-operation-receipt-json", default="")
    parser.add_argument("--railway-backup-receipt-json", default="")
    parser.add_argument("--railway-runbook-verification-json", default="")
    parser.add_argument("--railway-require-runbook-verification", action="store_true")
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
    parser.add_argument("--qdrant-control", action="store_true")
    parser.add_argument("--opensearch-control", action="store_true")
    parser.add_argument("--mongodb-control", action="store_true")
    parser.add_argument("--milvus-control", action="store_true")
    parser.add_argument("--tracedb-port", type=int, default=18_080)
    parser.add_argument("--postgres-port", type=int, default=25_432)
    parser.add_argument("--pgvector-port", type=int, default=25_433)
    parser.add_argument("--qdrant-port", type=int, default=26_333)
    parser.add_argument("--opensearch-port", type=int, default=29_200)
    parser.add_argument("--mongodb-port", type=int, default=27_027)
    parser.add_argument(
        "--summary-json",
        help="Write the returned Modal benchmark summary to a clean local JSON file.",
    )
    parser.add_argument(
        "--bundle-output",
        help="Write the report bundle tarball to this local path when the run completes.",
    )
    parser.add_argument(
        "--bundle-export-max-mb",
        type=int,
        default=DEFAULT_BUNDLE_EXPORT_MAX_MB,
        help="Maximum report bundle size to return or copy through --bundle-output.",
    )
    return parser


def _config_from_args(args: argparse.Namespace) -> ModalSmokeConfig:
    preset = SUITE_PRESETS.get(args.suite_preset, {})
    return ModalSmokeConfig(
        run_id=args.run_id,
        records=int(preset.get("records", args.records)),
        dataset=str(preset.get("dataset", args.dataset)),
        target=str(preset.get("target", args.target)),
        surface=str(preset.get("surface", args.surface)),
        scenarios=str(preset.get("scenarios", args.scenarios)),
        suite_spec=args.suite_spec or str(preset.get("suite_spec", "")),
        suite_baseline_json=args.suite_baseline_json,
        suite_baseline_dir=args.suite_baseline_dir,
        regression_tolerance_pct=args.regression_tolerance_pct,
        regression_tolerance_absolute=args.regression_tolerance_absolute,
        suite_preflight_only=args.suite_preflight_only
        or bool(preset.get("suite_preflight_only", False)),
        railway_config_from_env=args.railway_config_from_env
        or bool(preset.get("railway_config_from_env", False)),
        railway_health_check=args.railway_health_check
        or bool(preset.get("railway_health_check", False)),
        railway_health_timeout_seconds=args.railway_health_timeout_seconds,
        railway_stateful_smoke=args.railway_stateful_smoke
        or bool(preset.get("railway_stateful_smoke", False)),
        railway_stateful_smoke_timeout_seconds=args.railway_stateful_smoke_timeout_seconds,
        railway_stateful_marker_id=args.railway_stateful_marker_id,
        railway_stateful_read_only=args.railway_stateful_read_only,
        railway_snapshot_restore_check=args.railway_snapshot_restore_check
        or bool(preset.get("railway_snapshot_restore_check", False)),
        railway_snapshot_restore_timeout_seconds=args.railway_snapshot_restore_timeout_seconds,
        railway_snapshot_root=args.railway_snapshot_root,
        railway_verify_restored_marker=args.railway_verify_restored_marker
        or bool(preset.get("railway_verify_restored_marker", False)),
        railway_restart_redeploy_plan=args.railway_restart_redeploy_plan
        or bool(preset.get("railway_restart_redeploy_plan", False)),
        railway_persistence_pre_manifest_json=args.railway_persistence_pre_manifest_json,
        railway_operation_receipt_json=args.railway_operation_receipt_json,
        railway_backup_receipt_json=args.railway_backup_receipt_json,
        railway_runbook_verification_json=args.railway_runbook_verification_json,
        railway_runbook_verification_required=args.railway_require_runbook_verification
        or bool(preset.get("railway_runbook_verification_required", False)),
        openrouter_mode=args.openrouter_mode,
        openrouter_cap=args.openrouter_cap,
        tracedb_ingest_mode=str(
            preset.get("tracedb_ingest_mode", args.tracedb_ingest_mode)
        ),
        seed=args.seed,
        min_free_mb=args.min_free_mb,
        allow_large=args.allow_large or bool(preset.get("allow_large", False)),
        allow_external_controls=args.allow_external_controls
        or bool(preset.get("allow_external_controls", False)),
        allow_provider=args.allow_provider,
        require_services=args.require_services or bool(preset.get("require_services", False)),
        tracedb_engine_control=args.tracedb_engine_control,
        postgres_control=args.postgres_control,
        pgvector_control=args.pgvector_control,
        qdrant_control=args.qdrant_control,
        opensearch_control=args.opensearch_control,
        mongodb_control=args.mongodb_control,
        milvus_control=args.milvus_control,
        tracedb_port=args.tracedb_port,
        postgres_port=args.postgres_port,
        pgvector_port=args.pgvector_port,
        qdrant_port=args.qdrant_port,
        opensearch_port=args.opensearch_port,
        mongodb_port=args.mongodb_port,
        modal_app_name=modal_app_name(),
        modal_image_kind=modal_image_kind_from_flags(
            tracedb_engine_control=args.tracedb_engine_control,
            pgvector_control=args.pgvector_control,
            postgres_control=args.postgres_control,
            qdrant_control=args.qdrant_control,
            opensearch_control=args.opensearch_control,
            mongodb_control=args.mongodb_control,
            milvus_control=args.milvus_control,
        ),
    )


def _parse_args_with_summary_output(
    argv: list[str] | None = None,
) -> tuple[ModalSmokeConfig, str | None, str | None, int]:
    args = _build_parser().parse_args(argv)
    return (
        _config_from_args(args),
        args.summary_json,
        args.bundle_output,
        args.bundle_export_max_mb,
    )


def _parse_args(argv: list[str] | None = None) -> ModalSmokeConfig:
    (
        config,
        _summary_json,
        _bundle_output,
        _bundle_export_max_mb,
    ) = _parse_args_with_summary_output(argv)
    return config


def write_summary_json(summary: dict[str, Any], summary_json: str | None) -> None:
    if not summary_json:
        return
    output_path = Path(summary_json).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _ensure_bundle_export_size(size_bytes: int, max_mb: int) -> None:
    max_bytes = max_mb * 1024 * 1024
    if size_bytes > max_bytes:
        raise ValueError(
            f"bundle exceeds --bundle-export-max-mb={max_mb}: "
            f"{size_bytes} bytes > {max_bytes} bytes"
        )


def attach_bundle_bytes(
    summary: dict[str, Any],
    *,
    max_mb: int = DEFAULT_BUNDLE_EXPORT_MAX_MB,
) -> dict[str, Any]:
    bundle_path = summary.get("bundle_path")
    if not bundle_path:
        raise ValueError("summary does not include bundle_path")
    data = Path(bundle_path).read_bytes()
    _ensure_bundle_export_size(len(data), max_mb)
    result = dict(summary)
    result[BUNDLE_BYTES_FIELD] = base64.b64encode(data).decode("ascii")
    result[BUNDLE_SHA256_FIELD] = hashlib.sha256(data).hexdigest()
    result[BUNDLE_SIZE_FIELD] = len(data)
    return result


def write_bundle_output(
    summary: dict[str, Any],
    bundle_output: str | None,
    *,
    max_mb: int = DEFAULT_BUNDLE_EXPORT_MAX_MB,
) -> dict[str, Any]:
    clean_summary = dict(summary)
    payload = clean_summary.pop(BUNDLE_BYTES_FIELD, None)
    expected_sha256 = clean_summary.pop(BUNDLE_SHA256_FIELD, None)
    expected_size = clean_summary.pop(BUNDLE_SIZE_FIELD, None)
    summary.pop(BUNDLE_BYTES_FIELD, None)
    summary.pop(BUNDLE_SHA256_FIELD, None)
    summary.pop(BUNDLE_SIZE_FIELD, None)
    if not bundle_output:
        return clean_summary

    output_path = Path(bundle_output).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if payload is not None:
        if expected_sha256 is None:
            raise ValueError("bundle payload checksum missing")
        data = base64.b64decode(payload.encode("ascii"), validate=True)
    else:
        source_path_text = clean_summary.get("bundle_path")
        if not source_path_text:
            raise ValueError("summary does not include bundle_path")
        data = Path(source_path_text).expanduser().read_bytes()

    _ensure_bundle_export_size(len(data), max_mb)
    if expected_size is not None and expected_size != len(data):
        raise ValueError("bundle payload size mismatch")
    actual_sha256 = hashlib.sha256(data).hexdigest()
    if expected_sha256 is not None and expected_sha256 != actual_sha256:
        raise ValueError("bundle payload checksum mismatch")
    output_path.write_bytes(data)
    clean_summary["exported_bundle_path"] = str(output_path)
    clean_summary["exported_bundle_source_path"] = clean_summary.get("bundle_path")
    clean_summary["exported_bundle_size_bytes"] = len(data)
    clean_summary["exported_bundle_sha256"] = actual_sha256
    clean_summary["exported_bundle_checksum_verified"] = True
    clean_summary["bundle_export_transport"] = (
        "modal_return_bytes" if payload is not None else "local_copy"
    )
    return clean_summary


def source_git_kwargs(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    identity = git_identity(repo_root)
    return {
        "source_commit": identity.get("commit"),
        "source_dirty": identity.get("dirty"),
        "source_status_short": identity.get("status_short"),
        "source_git_error": identity.get("error"),
    }


def run_local(argv: list[str] | None = None) -> int:
    (
        config,
        summary_json,
        bundle_output,
        bundle_export_max_mb,
    ) = _parse_args_with_summary_output(argv)
    summary = run_suite_and_bundle(config)
    summary = write_bundle_output(
        summary,
        bundle_output,
        max_mb=bundle_export_max_mb,
    )
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

    def add_pgvector_extension(base_image: modal.Image) -> modal.Image:
        return base_image.run_commands(
            "cd /tmp && "
            f"git clone --branch {PGVECTOR_VERSION} --depth 1 https://github.com/pgvector/pgvector.git && "
            "cd pgvector && "
            "make && "
            "make install && "
            "rm -rf /tmp/pgvector"
        )

    def pgvector_package_image(base_image: modal.Image) -> modal.Image:
        return add_pgvector_extension(
            base_image.apt_install(
                "git",
                "postgresql",
                "postgresql-client",
                "postgresql-server-dev-all",
            )
        )

    def pgvector_control_image() -> modal.Image:
        return add_repo_source(pgvector_package_image(modal_base_image()))

    def tracedb_pgvector_control_image() -> modal.Image:
        return add_repo_source_for_build(
            pgvector_package_image(rust_modal_base_image())
        ).run_commands(
            f"cd {REMOTE_REPO} && cargo build --release -p tracedb-server"
        )

    def add_qdrant_binary(base_image: modal.Image) -> modal.Image:
        return base_image.run_commands(
            f"curl -L {QDRANT_RELEASE_URL} -o /tmp/qdrant.tar.gz && "
            "tar -xzf /tmp/qdrant.tar.gz -C /usr/local/bin qdrant && "
            "chmod +x /usr/local/bin/qdrant && "
            "rm -f /tmp/qdrant.tar.gz"
        )

    def add_opensearch_binary(base_image: modal.Image) -> modal.Image:
        return base_image.run_commands(
            "useradd -m -r -s /usr/sbin/nologin opensearch || true && "
            f"curl -L {OPENSEARCH_RELEASE_URL} -o /tmp/opensearch.tar.gz && "
            "tar -xzf /tmp/opensearch.tar.gz -C /opt && "
            f"chown -R opensearch:opensearch /opt/opensearch-{OPENSEARCH_VERSION} && "
            "rm -f /tmp/opensearch.tar.gz"
        )

    def add_mongodb_binary(base_image: modal.Image) -> modal.Image:
        return base_image.apt_install(
            "libcurl4",
            "libgssapi-krb5-2",
            "libldap-common",
            "libwrap0",
            "libsasl2-2",
            "libsasl2-modules",
            "libsasl2-modules-gssapi-mit",
            "openssl",
            "liblzma5",
        ).run_commands(
            f"curl --fail --show-error --location {MONGODB_RELEASE_URL} -o /tmp/mongodb.tar.gz && "
            "tar -xzf /tmp/mongodb.tar.gz -C /tmp && "
            "cp /tmp/mongodb-linux-*/bin/mongod /usr/local/bin/mongod && "
            "chmod +x /usr/local/bin/mongod && "
            "rm -rf /tmp/mongodb.tar.gz /tmp/mongodb-linux-*"
        )

    def qdrant_control_image() -> modal.Image:
        return add_repo_source(add_qdrant_binary(modal_base_image()))

    def tracedb_qdrant_control_image() -> modal.Image:
        return add_repo_source_for_build(
            add_qdrant_binary(rust_modal_base_image())
        ).run_commands(
            f"cd {REMOTE_REPO} && cargo build --release -p tracedb-server"
        )

    def opensearch_control_image() -> modal.Image:
        return add_repo_source(add_opensearch_binary(modal_base_image()))

    def tracedb_opensearch_control_image() -> modal.Image:
        return add_repo_source_for_build(
            add_opensearch_binary(rust_modal_base_image())
        ).run_commands(
            f"cd {REMOTE_REPO} && cargo build --release -p tracedb-server"
        )

    def mongodb_control_image() -> modal.Image:
        return add_repo_source(add_mongodb_binary(modal_base_image()))

    def tracedb_mongodb_control_image() -> modal.Image:
        return add_repo_source_for_build(
            add_mongodb_binary(rust_modal_base_image())
        ).run_commands(
            f"cd {REMOTE_REPO} && cargo build --release -p tracedb-server"
        )

    def milvus_control_image() -> modal.Image:
        return modal_image()

    def tracedb_milvus_control_image() -> modal.Image:
        return tracedb_engine_image()

    def external_controls_image() -> modal.Image:
        return add_repo_source(
            add_mongodb_binary(
                add_opensearch_binary(
                    add_qdrant_binary(pgvector_package_image(modal_base_image()))
                )
            )
        )

    def tracedb_controls_image() -> modal.Image:
        return add_repo_source_for_build(
            add_mongodb_binary(
                add_opensearch_binary(
                    add_qdrant_binary(pgvector_package_image(rust_modal_base_image()))
                )
            )
        ).run_commands(
            f"cd {REMOTE_REPO} && cargo build --release -p tracedb-server"
        )

    def selected_modal_image(kind: str) -> modal.Image:
        kind = validate_modal_image_kind(kind)
        if kind == "tracedb_controls":
            return tracedb_controls_image()
        if kind == "external_controls":
            return external_controls_image()
        if kind == "tracedb_pgvector":
            return tracedb_pgvector_control_image()
        if kind == "tracedb_qdrant":
            return tracedb_qdrant_control_image()
        if kind == "tracedb_opensearch":
            return tracedb_opensearch_control_image()
        if kind == "tracedb_mongodb":
            return tracedb_mongodb_control_image()
        if kind == "tracedb_milvus":
            return tracedb_milvus_control_image()
        if kind == "tracedb":
            return tracedb_engine_image()
        if kind == "pgvector":
            return pgvector_control_image()
        if kind == "qdrant":
            return qdrant_control_image()
        if kind == "opensearch":
            return opensearch_control_image()
        if kind == "mongodb":
            return mongodb_control_image()
        if kind == "milvus":
            return milvus_control_image()
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
    def run_smoke_remote(
        export_bundle: bool = False,
        bundle_export_max_mb: int = DEFAULT_BUNDLE_EXPORT_MAX_MB,
        **kwargs: Any,
    ) -> dict[str, Any]:
        config = ModalSmokeConfig(**kwargs)
        summary = run_suite_and_bundle(config, lab_root=Path(REMOTE_REPO) / "benchmarks" / "realworld")
        if export_bundle:
            return attach_bundle_bytes(summary, max_mb=bundle_export_max_mb)
        return summary

    @app.local_entrypoint()
    def main(
        run_id: str = "modal-smoke",
        records: int = 128,
        dataset: str = "generated",
        target: str = "tracedb",
        surface: str = "sdk",
        scenarios: str = "sdk_cli_surface",
        suite_spec: str = "",
        suite_preset: str = "",
        suite_baseline_json: str = "",
        suite_baseline_dir: str = "",
        regression_tolerance_pct: float = 15.0,
        regression_tolerance_absolute: float = 0.0,
        suite_preflight_only: bool = False,
        railway_config_from_env: bool = False,
        railway_health_check: bool = False,
        railway_health_timeout_seconds: float = 5.0,
        railway_stateful_smoke: bool = False,
        railway_stateful_smoke_timeout_seconds: float = 5.0,
        railway_stateful_marker_id: str = "",
        railway_stateful_read_only: bool = False,
        railway_snapshot_restore_check: bool = False,
        railway_snapshot_restore_timeout_seconds: float = 60.0,
        railway_snapshot_root: str = "",
        railway_verify_restored_marker: bool = False,
        railway_restart_redeploy_plan: bool = False,
        railway_persistence_pre_manifest_json: str = "",
        railway_operation_receipt_json: str = "",
        railway_backup_receipt_json: str = "",
        railway_runbook_verification_json: str = "",
        railway_require_runbook_verification: bool = False,
        openrouter_mode: str = "off",
        openrouter_cap: str = "moderate",
        tracedb_ingest_mode: str = "per_record",
        seed: int = 42,
        min_free_mb: int = 20_000,
        allow_external_controls: bool = False,
        require_services: bool = False,
        tracedb_engine_control: bool = False,
        postgres_control: bool = False,
        pgvector_control: bool = False,
        qdrant_control: bool = False,
        opensearch_control: bool = False,
        mongodb_control: bool = False,
        milvus_control: bool = False,
        tracedb_port: int = 18_080,
        postgres_port: int = 25_432,
        pgvector_port: int = 25_433,
        qdrant_port: int = 26_333,
        opensearch_port: int = 29_200,
        mongodb_port: int = 27_027,
        allow_large: bool = False,
        allow_provider: bool = False,
        summary_json: str = "",
        bundle_output: str = "",
        bundle_export_max_mb: int = DEFAULT_BUNDLE_EXPORT_MAX_MB,
    ) -> None:
        if suite_preset and suite_preset not in SUITE_PRESETS:
            raise ValueError(f"unknown suite preset: {suite_preset}")
        args = argparse.Namespace(
            run_id=run_id,
            records=records,
            dataset=dataset,
            target=target,
            surface=surface,
            scenarios=scenarios,
            suite_spec=suite_spec,
            suite_preset=suite_preset,
            suite_baseline_json=suite_baseline_json,
            suite_baseline_dir=suite_baseline_dir,
            regression_tolerance_pct=regression_tolerance_pct,
            regression_tolerance_absolute=regression_tolerance_absolute,
            suite_preflight_only=suite_preflight_only,
            railway_config_from_env=railway_config_from_env,
            railway_health_check=railway_health_check,
            railway_health_timeout_seconds=railway_health_timeout_seconds,
            railway_stateful_smoke=railway_stateful_smoke,
            railway_stateful_smoke_timeout_seconds=railway_stateful_smoke_timeout_seconds,
            railway_stateful_marker_id=railway_stateful_marker_id,
            railway_stateful_read_only=railway_stateful_read_only,
            railway_snapshot_restore_check=railway_snapshot_restore_check,
            railway_snapshot_restore_timeout_seconds=railway_snapshot_restore_timeout_seconds,
            railway_snapshot_root=railway_snapshot_root,
            railway_verify_restored_marker=railway_verify_restored_marker,
            railway_restart_redeploy_plan=railway_restart_redeploy_plan,
            railway_persistence_pre_manifest_json=railway_persistence_pre_manifest_json,
            railway_operation_receipt_json=railway_operation_receipt_json,
            railway_backup_receipt_json=railway_backup_receipt_json,
            railway_runbook_verification_json=railway_runbook_verification_json,
            railway_require_runbook_verification=railway_require_runbook_verification,
            openrouter_mode=openrouter_mode,
            openrouter_cap=openrouter_cap,
            tracedb_ingest_mode=tracedb_ingest_mode,
            seed=seed,
            min_free_mb=min_free_mb,
            allow_external_controls=allow_external_controls,
            require_services=require_services,
            tracedb_engine_control=tracedb_engine_control,
            postgres_control=postgres_control,
            pgvector_control=pgvector_control,
            qdrant_control=qdrant_control,
            opensearch_control=opensearch_control,
            mongodb_control=mongodb_control,
            milvus_control=milvus_control,
            tracedb_port=tracedb_port,
            postgres_port=postgres_port,
            pgvector_port=pgvector_port,
            qdrant_port=qdrant_port,
            opensearch_port=opensearch_port,
            mongodb_port=mongodb_port,
            allow_large=allow_large,
            allow_provider=allow_provider,
        )
        config = replace(
            _config_from_args(args),
            modal_image_kind=selected_image_kind,
            **source_git_kwargs(),
        )
        config, selected_suite_baseline = resolve_modal_suite_baseline(config)
        config, staged_input_artifacts = stage_modal_input_artifacts(config)
        result = run_smoke_remote.remote(
            export_bundle=bool(bundle_output),
            bundle_export_max_mb=bundle_export_max_mb,
            **asdict(config),
        )
        if selected_suite_baseline is not None:
            result["selected_suite_baseline"] = selected_suite_baseline
        if staged_input_artifacts:
            result["staged_input_artifacts"] = staged_input_artifacts
        result = write_bundle_output(result, bundle_output, max_mb=bundle_export_max_mb)
        write_summary_json(result, summary_json)
        print(json.dumps(result, indent=2, sort_keys=True))
else:
    app = None


if __name__ == "__main__":
    raise SystemExit(run_local())
