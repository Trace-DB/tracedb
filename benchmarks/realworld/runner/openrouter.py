from __future__ import annotations

import hashlib
import json
import math
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .datasets import _dataset_digest
from .experiment import ExperimentRecorder, redact
from .metrics import mrr_at_k, ndcg_at_k, recall_at_k
from .types import BenchQuery, BenchRecord, DatasetBundle


DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_EMBED_MODEL = "qwen/qwen3-embedding-8b"
DEFAULT_COMPARE_EMBED_MODELS = ["perplexity/pplx-embed-v1-0.6b"]
DEFAULT_JUDGE_MODEL = "openrouter/owl-alpha"
DEFAULT_RERANK_MODEL = "cohere/rerank-4-fast"
DEFAULT_EMBEDDING_DIMENSIONS = 1536

CAPS = {
    "conservative": {
        "max_embedding_models": 2,
        "max_records": 1_000,
        "max_queries": 24,
        "max_judge_calls": 6,
        "max_rerank_calls": 24,
    },
    "moderate": {
        "max_embedding_models": 2,
        "max_records": 10_000,
        "max_queries": 100,
        "max_judge_calls": 25,
        "max_rerank_calls": 100,
    },
    "aggressive": {
        "max_embedding_models": 2,
        "max_records": 50_000,
        "max_queries": 250,
        "max_judge_calls": 100,
        "max_rerank_calls": 250,
    },
}


class OpenRouterError(RuntimeError):
    pass


@dataclass(frozen=True)
class OpenRouterConfig:
    mode: str
    cap_name: str
    base_url: str
    api_key: str | None
    embed_model: str
    compare_embed_models: list[str]
    judge_model: str
    rerank_model: str
    embedding_dimensions: int | None

    @property
    def caps(self) -> dict[str, int]:
        return CAPS[self.cap_name]

    @property
    def enabled(self) -> bool:
        return self.mode != "off" and bool(self.api_key)

    def selected_embedding_models(self) -> list[str]:
        models = [self.embed_model]
        for model in self.compare_embed_models:
            if model and model not in models:
                models.append(model)
        return models[: self.caps["max_embedding_models"]]

    def public_summary(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "cap": self.cap_name,
            "enabled": self.enabled,
            "base_url": self.base_url,
            "embed_model": self.embed_model,
            "compare_embed_models": self.compare_embed_models,
            "judge_model": self.judge_model,
            "rerank_model": self.rerank_model,
            "embedding_dimensions": self.embedding_dimensions,
            "caps": self.caps,
            "api_key": "[configured]" if self.api_key else "[missing]",
        }


def load_env_file(lab_root: Path) -> None:
    if os.environ.get("BENCH_DISABLE_ENV_FILE") == "1":
        return
    override = os.environ.get("BENCH_ENV_FILE")
    if override is not None:
        env_paths = [Path(override)] if override else []
    else:
        env_paths = [
            lab_root / ".env.local",
            lab_root / ".env",
            lab_root.parent.parent / ".env",
        ]
    for env_path in env_paths:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def parse_csv(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _configured_embedding_dimensions(args: Any) -> int | None:
    raw = (
        getattr(args, "embedding_dimensions", None)
        or os.environ.get("OPENROUTER_EMBED_DIMENSIONS")
        or os.environ.get("OPENROUTER_EMBEDDING_DIMENSIONS")
        or str(DEFAULT_EMBEDDING_DIMENSIONS)
    )
    if raw in {None, "", "0", "native", "auto"}:
        return None
    try:
        dimensions = int(str(raw))
    except ValueError as error:
        raise OpenRouterError(f"invalid embedding dimensions {raw!r}") from error
    if dimensions <= 0:
        return None
    if dimensions > 2048:
        raise OpenRouterError(
            f"embedding dimensions cap must be <= 2048 for this benchmark lab, got {dimensions}"
        )
    return dimensions


def config_from_args(args: Any, lab_root: Path) -> OpenRouterConfig:
    load_env_file(lab_root)
    mode = getattr(args, "openrouter_mode", "auto")
    cap_name = getattr(args, "openrouter_cap", "moderate")
    if cap_name not in CAPS:
        raise OpenRouterError(f"unknown OpenRouter cap {cap_name}")
    embed_model = (
        getattr(args, "embed_model", None)
        or os.environ.get("OPENROUTER_EMBED_MODEL")
        or DEFAULT_EMBED_MODEL
    )
    compare_value = getattr(args, "compare_embed_models", None)
    if compare_value is None:
        compare_models = parse_csv(os.environ.get("OPENROUTER_COMPARE_EMBED_MODELS"))
        if not compare_models:
            compare_models = DEFAULT_COMPARE_EMBED_MODELS.copy()
    else:
        compare_models = parse_csv(compare_value)
    dimensions = _configured_embedding_dimensions(args)
    return OpenRouterConfig(
        mode=mode,
        cap_name=cap_name,
        base_url=(os.environ.get("OPENROUTER_BASE_URL") or DEFAULT_BASE_URL).rstrip("/"),
        api_key=os.environ.get("OPENROUTER_API_KEY"),
        embed_model=embed_model,
        compare_embed_models=compare_models,
        judge_model=(
            getattr(args, "judge_model", None)
            or os.environ.get("OPENROUTER_JUDGE_MODEL")
            or DEFAULT_JUDGE_MODEL
        ),
        rerank_model=(
            getattr(args, "rerank_model", None)
            or os.environ.get("OPENROUTER_RERANK_MODEL")
            or DEFAULT_RERANK_MODEL
        ),
        embedding_dimensions=dimensions,
    )


class OpenRouterClient:
    def __init__(
        self,
        config: OpenRouterConfig,
        cache_dir: Path,
        recorder: ExperimentRecorder | None = None,
    ) -> None:
        if not config.api_key:
            raise OpenRouterError("OPENROUTER_API_KEY is required for OpenRouter calls")
        self.config = config
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.recorder = recorder
        self.model_metadata: dict[str, Any] = {}
        self.stats: dict[str, Any] = {
            "enabled": True,
            "mode": config.mode,
            "request_count": 0,
            "embedding_request_count": 0,
            "chat_request_count": 0,
            "rerank_request_count": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "search_units": 0,
            "estimated_spend": 0.0,
            "cache_hits": 0,
            "cache_misses": 0,
            "rate_limit_events": 0,
            "models": config.selected_embedding_models(),
        }

    def key_info(self) -> dict[str, Any]:
        payload = self._request_json("GET", "/key")
        self._observe("openrouter.key", {"key": redact(payload)})
        return payload

    def list_embedding_models(self) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/embeddings/models")
        data = payload.get("data", []) if isinstance(payload, dict) else []
        models = [item for item in data if isinstance(item, dict)]
        self.model_metadata = {str(item.get("id")): item for item in models if item.get("id")}
        self._observe(
            "openrouter.embedding_models",
            {"model_count": len(models), "selected": self.config.selected_embedding_models()},
        )
        return models

    def ensure_models_available(self, models: list[str]) -> None:
        if not self.model_metadata:
            return
        missing = [model for model in models if model not in self.model_metadata]
        if missing:
            raise OpenRouterError(
                "OpenRouter embedding model(s) unavailable: " + ", ".join(missing)
            )

    def embed_texts(self, model: str, texts: list[str], batch_size: int = 64) -> list[list[float]]:
        embeddings: list[list[float]] = []
        for start in range(0, len(texts), batch_size):
            batch = texts[start : start + batch_size]
            embeddings.extend(self._embed_batch(model, batch))
        return embeddings

    def judge_json(self, prompt: str) -> dict[str, Any]:
        payload = self._request_json(
            "POST",
            "/chat/completions",
            {
                "model": self.config.judge_model,
                "messages": [
                    {
                        "role": "system",
                        "content": "Return only compact JSON for benchmark diagnostics.",
                    },
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0,
            },
        )
        self.stats["chat_request_count"] += 1
        content = (
            payload.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "{}")
        )
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            parsed = {"raw": content}
        self._observe("openrouter.judge", {"model": self.config.judge_model, "response": parsed})
        return parsed

    def rerank(
        self,
        *,
        query: str,
        documents: list[str],
        model: str,
        top_n: int,
    ) -> list[dict[str, Any]]:
        payload = self._request_json(
            "POST",
            "/rerank",
            {
                "model": model,
                "query": query,
                "documents": documents,
                "top_n": top_n,
            },
        )
        self.stats["rerank_request_count"] += 1
        results = payload.get("results", []) if isinstance(payload, dict) else []
        parsed = [
            {
                "index": int(item.get("index", -1)),
                "relevance_score": float(item.get("relevance_score", 0.0)),
            }
            for item in results
            if isinstance(item, dict)
        ]
        self._observe(
            "openrouter.rerank",
            {
                "model": model,
                "document_count": len(documents),
                "top_n": top_n,
                "result_count": len(parsed),
                "usage": payload.get("usage", {}),
            },
        )
        return parsed

    def _embed_batch(self, model: str, texts: list[str]) -> list[list[float]]:
        cached: dict[int, list[float]] = {}
        misses: list[tuple[int, str, Path]] = []
        for index, text in enumerate(texts):
            path = self._cache_path(model, text, dimensions="auto")
            if path.exists():
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    cached[index] = [float(value) for value in payload["embedding"]]
                    self.stats["cache_hits"] += 1
                    continue
                except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                    pass
            misses.append((index, text, path))
            self.stats["cache_misses"] += 1

        if misses:
            payload = self._request_json(
                "POST",
                "/embeddings",
                {"model": model, "input": [text for _, text, _ in misses]},
            )
            self.stats["embedding_request_count"] += 1
            data = payload.get("data", []) if isinstance(payload, dict) else []
            if len(data) != len(misses):
                raise OpenRouterError(
                    f"OpenRouter returned {len(data)} embeddings for {len(misses)} inputs"
                )
            for item, (index, text, path) in zip(data, misses, strict=True):
                embedding = [float(value) for value in item.get("embedding", [])]
                if not embedding:
                    raise OpenRouterError("OpenRouter returned an empty embedding")
                cached[index] = embedding
                path.write_text(
                    json.dumps(
                        {
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "base_url": self.config.base_url,
                            "model": model,
                            "dimensions": len(embedding),
                            "input_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                            "embedding": embedding,
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )
            self._observe(
                "openrouter.embedding_batch",
                {
                    "model": model,
                    "count": len(misses),
                    "dimensions": len(cached[misses[0][0]]),
                    "cache_hits": self.stats["cache_hits"],
                    "cache_misses": self.stats["cache_misses"],
                    "usage": payload.get("usage", {}),
                },
            )

        return [cached[index] for index in range(len(texts))]

    def _cache_path(self, model: str, text: str, dimensions: str) -> Path:
        key = json.dumps(
            {
                "base_url": self.config.base_url,
                "model": model,
                "dimensions": dimensions,
                "input": text,
            },
            sort_keys=True,
        )
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def _request_json(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        encoded = None if body is None else json.dumps(body).encode("utf-8")
        headers = {
            "authorization": f"Bearer {self.config.api_key}",
            "content-type": "application/json",
            "http-referer": "https://tracedb.local/benchmarks",
            "x-title": "TraceDB Real-World Benchmarks",
        }
        last_error: Exception | None = None
        for attempt in range(1, 4):
            self.stats["request_count"] += 1
            request = urllib.request.Request(url, data=encoded, method=method, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8") or "{}")
                    self._account_usage(payload)
                    return payload
            except urllib.error.HTTPError as error:
                payload = error.read().decode("utf-8", errors="replace")
                last_error = OpenRouterError(f"HTTP {error.code} from {path}: {redact(payload)}")
                if error.code == 429:
                    self.stats["rate_limit_events"] += 1
                if error.code not in {429, 500, 502, 503, 504}:
                    break
            except urllib.error.URLError as error:
                last_error = OpenRouterError(f"OpenRouter request failed for {path}: {error}")
            self._observe(
                "openrouter.retry",
                {"path": path, "attempt": attempt, "reason": str(last_error)},
            )
            time.sleep(0.1 * attempt)
        raise OpenRouterError(str(last_error or f"OpenRouter request failed for {path}"))

    def _account_usage(self, payload: dict[str, Any]) -> None:
        usage = payload.get("usage")
        if not isinstance(usage, dict):
            return
        prompt = int(usage.get("prompt_tokens") or 0)
        completion = int(usage.get("completion_tokens") or 0)
        total = int(usage.get("total_tokens") or prompt + completion)
        self.stats["prompt_tokens"] += prompt
        self.stats["completion_tokens"] += completion
        self.stats["total_tokens"] += total
        self.stats["search_units"] += int(usage.get("search_units") or 0)

    def _observe(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.recorder:
            self.recorder.observe(event_type, payload)


def maybe_apply_openrouter_embeddings(
    dataset: DatasetBundle,
    config: OpenRouterConfig,
    lab_root: Path,
    recorder: ExperimentRecorder | None,
) -> tuple[DatasetBundle, dict[str, Any]]:
    if config.mode == "off":
        return dataset, _disabled_stats(config, "disabled by --openrouter-mode off")
    if not config.api_key:
        if config.mode == "required":
            raise OpenRouterError("OPENROUTER_API_KEY is required when --openrouter-mode required")
        return dataset, _disabled_stats(config, "OPENROUTER_API_KEY not configured")

    caps = config.caps
    if len(dataset.records) > caps["max_records"]:
        raise OpenRouterError(
            f"OpenRouter cap {config.cap_name} allows {caps['max_records']} records, got {len(dataset.records)}"
        )
    if len(dataset.queries) > caps["max_queries"]:
        raise OpenRouterError(
            f"OpenRouter cap {config.cap_name} allows {caps['max_queries']} queries, got {len(dataset.queries)}"
        )

    client = OpenRouterClient(config, lab_root / ".cache" / "openrouter", recorder)
    client.key_info()
    client.list_embedding_models()
    selected_models = config.selected_embedding_models()
    client.ensure_models_available(selected_models)
    primary_model = selected_models[0]
    texts = [record.text() for record in dataset.records] + [query.text for query in dataset.queries]
    embeddings = client.embed_texts(primary_model, texts)
    raw_record_embeddings = embeddings[: len(dataset.records)]
    raw_query_embeddings = embeddings[len(dataset.records) :]
    native_dimensions = len(raw_record_embeddings[0]) if raw_record_embeddings else 0
    record_embeddings = [
        _fit_embedding_dimensions(embedding, config.embedding_dimensions)
        for embedding in raw_record_embeddings
    ]
    query_embeddings = [
        _fit_embedding_dimensions(embedding, config.embedding_dimensions)
        for embedding in raw_query_embeddings
    ]
    records = [
        replace(record, vector=embedding)
        for record, embedding in zip(dataset.records, record_embeddings, strict=True)
    ]
    queries = [
        replace(query, vector=embedding)
        for query, embedding in zip(dataset.queries, query_embeddings, strict=True)
    ]
    dimensions = len(record_embeddings[0]) if record_embeddings else 0
    if len(selected_models) > 1 and texts:
        sample = texts[: min(4, len(texts))]
        for model in selected_models[1:]:
            client.embed_texts(model, sample)
            if recorder:
                recorder.observe(
                    "openrouter.embedding_compare",
                    {"primary_model": primary_model, "compare_model": model, "sample_count": len(sample)},
            )

    rerank_metrics = _evaluate_rerank(client, config, records, queries)

    if dataset.kind == "embedded_movies" and dataset.records and dataset.queries:
        prompt = (
            "Score whether this movie document is relevant to the query as JSON with keys "
            "relevance and reason.\n"
            f"Query: {dataset.queries[0].text}\nDocument: {dataset.records[0].title} {dataset.records[0].body[:500]}"
        )
        client.judge_json(prompt)

    digest = _dataset_digest(
        dataset.kind,
        dataset.source,
        records,
        queries,
        primary_model,
        dimensions,
    )
    enriched = DatasetBundle(
        kind=dataset.kind,
        source=dataset.source,
        records=records,
        queries=queries,
        notes=[
            *dataset.notes,
            f"OpenRouter embeddings applied with {primary_model}",
            (
                f"Provider-native embeddings were {native_dimensions} dimensions; "
                f"benchmark vectors use {dimensions} dimensions"
            ),
            "OpenRouter model identity is recorded from API configuration; no upstream model family is assumed.",
        ],
        embedding_model=primary_model,
        embedding_dimensions=dimensions,
        embedding_source="openrouter",
        relevance_label_mode=dataset.relevance_label_mode,
        relevance_label_scope=dataset.relevance_label_scope,
        relevance_label_notes=dataset.relevance_label_notes,
        digest=digest,
    )
    stats = dict(client.stats)
    stats.update(
        {
            "enabled": True,
            "mode": config.mode,
            "cap": config.cap_name,
            "embedding_model": primary_model,
            "embedding_dimensions": dimensions,
            "provider_native_embedding_dimensions": native_dimensions,
            "requested_embedding_dimensions": config.embedding_dimensions,
            "model_metadata": {model: client.model_metadata.get(model, {}) for model in selected_models},
            "rerank_model": config.rerank_model,
            "rerank_metrics": rerank_metrics,
        }
    )
    return enriched, stats


def _fit_embedding_dimensions(values: list[float], dimensions: int | None) -> list[float]:
    if dimensions is None or len(values) <= dimensions:
        return values
    reduced = values[:dimensions]
    norm = math.sqrt(sum(value * value for value in reduced))
    if norm == 0.0:
        return reduced
    return [value / norm for value in reduced]


def _disabled_stats(config: OpenRouterConfig, reason: str) -> dict[str, Any]:
    return {
        "enabled": False,
        "mode": config.mode,
        "cap": config.cap_name,
        "reason": reason,
        "request_count": 0,
        "embedding_request_count": 0,
        "chat_request_count": 0,
        "rerank_request_count": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "search_units": 0,
        "estimated_spend": 0.0,
        "cache_hits": 0,
        "cache_misses": 0,
        "rate_limit_events": 0,
        "models": [],
        "embedding_dimensions": 0,
        "provider_native_embedding_dimensions": 0,
        "requested_embedding_dimensions": config.embedding_dimensions,
        "rerank_model": config.rerank_model,
        "rerank_metrics": {},
    }


def _evaluate_rerank(
    client: OpenRouterClient,
    config: OpenRouterConfig,
    records: list[BenchRecord],
    queries: list[BenchQuery],
) -> dict[str, Any]:
    if not records or not queries:
        return {"query_count": 0, "recall_at_5": 0.0, "ndcg_at_5": 0.0, "mrr_at_5": 0.0}
    from .adapters.base import ranked_ids

    by_id = {record.record_id: record for record in records}
    recalls: list[float] = []
    ndcgs: list[float] = []
    mrrs: list[float] = []
    attempted = 0
    for query in queries[: config.caps["max_rerank_calls"]]:
        candidate_ids = [
            record_id
            for record_id in ranked_ids(
                records,
                query.text,
                query.vector,
                query.tenant_id,
                query.category,
            )
            if record_id in by_id
        ][:20]
        if not candidate_ids:
            continue
        attempted += 1
        documents = [by_id[record_id].text() for record_id in candidate_ids]
        reranked = client.rerank(
            query=query.text,
            documents=documents,
            model=config.rerank_model,
            top_n=min(query.top_k, len(documents)),
        )
        actual_ids = [
            candidate_ids[item["index"]]
            for item in reranked
            if 0 <= item["index"] < len(candidate_ids)
        ]
        recalls.append(recall_at_k(query.expected_ids, actual_ids, query.top_k))
        ndcgs.append(ndcg_at_k(query.expected_ids, actual_ids, query.top_k))
        mrrs.append(mrr_at_k(query.expected_ids, actual_ids, query.top_k))
    return {
        "model": config.rerank_model,
        "query_count": attempted,
        "candidate_count": 20,
        "recall_at_5": round(sum(recalls) / len(recalls), 3) if recalls else 0.0,
        "ndcg_at_5": round(sum(ndcgs) / len(ndcgs), 3) if ndcgs else 0.0,
        "mrr_at_5": round(sum(mrrs) / len(mrrs), 3) if mrrs else 0.0,
    }
