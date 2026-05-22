from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = Path("target/debug/tracedb")
DEFAULT_TIMEOUT_S = 3.0

Runner = Callable[..., subprocess.CompletedProcess[str]]


def _tail(text: str | None, max_chars: int = 4_000) -> str:
    text = text or ""
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def run_command(
    argv: list[str],
    *,
    cwd: Path,
    timeout_s: float,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        completed = runner(
            argv,
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_s,
        )
        return {
            "argv": argv,
            "ok": completed.returncode == 0,
            "returncode": completed.returncode,
            "timed_out": False,
            "duration_s": round(time.monotonic() - started, 3),
            "stdout_tail": _tail(completed.stdout),
            "stderr_tail": _tail(completed.stderr),
        }
    except subprocess.TimeoutExpired as error:
        return {
            "argv": argv,
            "ok": False,
            "returncode": None,
            "timed_out": True,
            "timeout_s": timeout_s,
            "duration_s": round(time.monotonic() - started, 3),
            "stdout_tail": _tail(error.stdout if isinstance(error.stdout, str) else None),
            "stderr_tail": _tail(error.stderr if isinstance(error.stderr, str) else None),
        }


def inspect_binary(binary: Path, *, cwd: Path, timeout_s: float, runner: Runner) -> dict[str, Any]:
    if not binary.exists():
        return {
            "path": str(binary),
            "exists": False,
            "advice": ["Build the binary first with `cargo build -p tracedb-cli`."],
        }
    return {
        "path": str(binary),
        "exists": True,
        "xattr": run_command(["xattr", "-l", str(binary)], cwd=cwd, timeout_s=timeout_s, runner=runner),
        "spctl": run_command(
            ["spctl", "-a", "-vv", str(binary)],
            cwd=cwd,
            timeout_s=timeout_s,
            runner=runner,
        ),
        "codesign": run_command(
            ["codesign", "-dv", str(binary)],
            cwd=cwd,
            timeout_s=timeout_s,
            runner=runner,
        ),
    }


def diagnose_launch(
    *,
    binary: Path = DEFAULT_BINARY,
    cwd: Path = REPO_ROOT,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    binary = binary if binary.is_absolute() else cwd / binary
    inspection = inspect_binary(binary, cwd=cwd, timeout_s=timeout_s, runner=runner)
    if not inspection["exists"]:
        return {
            "status": "missing",
            "binary": inspection,
            "launch": None,
            "classification": "binary_missing",
            "advice": inspection["advice"],
        }

    launch = run_command([str(binary), "--help"], cwd=cwd, timeout_s=timeout_s, runner=runner)
    blocked_by_pre_main_timeout = launch["timed_out"] and not launch["stdout_tail"] and not launch["stderr_tail"]
    advice = []
    classification = "launch_passed" if launch["ok"] else "launch_failed"
    status = "passed" if launch["ok"] else "failed"
    if blocked_by_pre_main_timeout:
        classification = "pre_main_launch_timeout"
        status = "blocked"
        advice = [
            "Treat local macOS Rust binary execution as blocked for this checkout.",
            "Use the Modal product verification lane for runtime proof.",
            "Run `modal run scripts/modal_product_verify.py --mode quickstart` for remote Linux proof.",
        ]

    return {
        "status": status,
        "classification": classification,
        "binary": inspection,
        "launch": launch,
        "advice": advice,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose local Rust binary launch readiness.")
    parser.add_argument("--binary", default=str(DEFAULT_BINARY))
    parser.add_argument("--timeout-s", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--summary-json", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = diagnose_launch(binary=Path(args.binary), timeout_s=args.timeout_s)
    text = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.summary_json:
        Path(args.summary_json).write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if summary["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
