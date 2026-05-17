from __future__ import annotations

import itertools
import hashlib
import json
import random
from typing import Any

from .mathutil import cosine, deterministic_vector, text_score
from .types import DETERMINISTIC_EMBEDDING_MODEL, VECTOR_DIMENSIONS, BenchQuery, BenchRecord, DatasetBundle


TOPICS = [
    ("ai_memory", "agent memory vector retrieval policy freshness"),
    ("code_search", "rust function module query planner index"),
    ("support", "customer support ticket refund billing issue"),
    ("movie", "science fiction movie adventure mystery cast"),
    ("legal", "contract clause obligation party indemnity"),
    ("ops", "incident timeline service recovery latency"),
]


def load_dataset(kind: str, records: int, seed: int = 42) -> DatasetBundle:
    if kind == "generated":
        return generated_dataset(records, seed)
    if kind == "generated_hybrid":
        return generated_hybrid_dataset(records, seed)
    if kind == "embedded_movies":
        return embedded_movies_dataset(records, seed)
    if kind in {"beir_scifact", "scifact"}:
        return beir_scifact_dataset(records, seed)
    if kind in {"codesearchnet", "code_search_net"}:
        return codesearchnet_dataset(records, seed)
    raise ValueError(
        f"unknown dataset {kind}; expected generated, generated_hybrid, embedded_movies, beir_scifact, or codesearchnet"
    )


def generated_dataset(records: int, seed: int = 42) -> DatasetBundle:
    out = _generated_records(records, seed)
    queries = _queries_from_records(out)
    return _bundle(
        kind="generated",
        source="deterministic synthetic real-world-shaped corpus",
        records=out,
        queries=queries,
        notes=[
            "generated dataset is deterministic and safe for CI",
            "generated oracle_rank labels are operational-smoke labels; deterministic vectors are not aligned to hybrid relevance",
        ],
        relevance_label_mode="synthetic_oracle_rank",
        relevance_label_scope="operational_smoke_not_hybrid_quality",
        relevance_label_notes=[
            "expected_ids come from oracle_rank/text-order within tenant and category",
            "deterministic vectors are random-normalized fixtures and should not be used to tune hybrid scoring against oracle_rank recall",
        ],
    )


def generated_hybrid_dataset(records: int, seed: int = 42) -> DatasetBundle:
    out = _generated_records(records, seed)
    queries = _queries_from_records(out, prefer_oracle_rank=False)
    return _bundle(
        kind="generated_hybrid",
        source="deterministic synthetic hybrid-relevance corpus",
        records=out,
        queries=queries,
        notes=[
            "generated_hybrid reuses the deterministic generated corpus but labels expected_ids with local text+vector similarity",
            "generated_hybrid is a no-provider retrieval-quality lane for comparing hybrid scoring behavior",
        ],
        relevance_label_mode="synthetic_text_vector_similarity",
        relevance_label_scope="synthetic_retrieval_quality",
        relevance_label_notes=[
            "expected_ids come from text+vector scoring: text_score(query, record.text) + cosine(query.vector, record.vector) within tenant and category",
            "this lane is suitable for local hybrid scoring diagnosis but is still synthetic, not external qrels ground truth",
        ],
    )


def _generated_records(records: int, seed: int) -> list[BenchRecord]:
    rng = random.Random(seed)
    tenants = ["tenant-a", "tenant-b", "tenant-c", "tenant-rare"]
    statuses = ["draft", "published", "archived"]
    out: list[BenchRecord] = []
    topic_cycle = itertools.cycle(TOPICS)
    for index in range(records):
        category, phrase = next(topic_cycle)
        tenant = tenants[index % len(tenants)]
        title = f"{category.replace('_', ' ').title()} record {index}"
        body = (
            f"{phrase} record {index} tenant {tenant} "
            f"metadata sparse nested update benchmark trace database"
        )
        vector = deterministic_vector(f"{category}:{phrase}:{index}")
        out.append(
            BenchRecord(
                record_id=f"rec-{index:06d}",
                tenant_id=tenant,
                title=title,
                body=body,
                category=category,
                status=statuses[index % len(statuses)],
                rating=round(1.0 + rng.random() * 4.0, 3),
                year=2000 + index % 26,
                vector=vector,
                metadata={
                    "benchmark": {
                        "oracle_rank": index,
                        "oracle_topic": category,
                    },
                    "nested": {
                        "priority": ["low", "medium", "high"][index % 3],
                        "sparse_tag": category if index % 5 == 0 else None,
                    },
                    "array_tags": [category, tenant, statuses[index % len(statuses)]],
                },
            )
        )
    return out


def embedded_movies_dataset(records: int, seed: int) -> DatasetBundle:
    rows = _load_hf_rows("MongoDB/embedded_movies", "train", records, seed)
    out: list[BenchRecord] = []
    for index, item in enumerate(rows):
        plot = str(item.get("plot") or item.get("fullplot") or "")
        if not plot:
            continue
        embedding = item.get("plot_embedding")
        vector = _vector_or_deterministic(embedding, f"movie:{plot}:{index}")
        imdb = item.get("imdb") if isinstance(item.get("imdb"), dict) else {}
        out.append(
            BenchRecord(
                record_id=str(item.get("_id") or item.get("id") or f"movie-{index}"),
                tenant_id=f"tenant-{index % 4}",
                title=str(item.get("title") or f"Movie {index}"),
                body=plot,
                category="movie",
                status="published",
                rating=float(imdb.get("rating") or 0.0),
                year=_year_from_value(item.get("year") or item.get("released")) or 1900,
                vector=vector,
                metadata={
                    "genres": item.get("genres") or [],
                    "cast": item.get("cast") or [],
                    "directors": item.get("directors") or [],
                },
            )
        )
    return _bundle(
        kind="embedded_movies",
        source="MongoDB/embedded_movies",
        records=out[:records],
        queries=_queries_from_records(out[:records]),
        notes=["external dataset loaded from Hugging Face: MongoDB/embedded_movies"],
        relevance_label_mode="synthetic_text_vector_similarity",
        relevance_label_scope="synthetic_retrieval_quality",
    )


def beir_scifact_dataset(records: int, seed: int) -> DatasetBundle:
    rows = _load_hf_rows("irds/beir_scifact", "corpus", records, seed)
    out = [
        BenchRecord(
            record_id=str(item.get("doc_id") or item.get("_id") or f"scifact-{idx}"),
            tenant_id=f"tenant-{idx % 4}",
            title=str(item.get("title") or f"SciFact {idx}"),
            body=str(item.get("text") or item.get("contents") or ""),
            category="scientific_claim",
            status="published",
            rating=0.0,
            year=2020,
            vector=deterministic_vector(str(item.get("text") or item.get("contents") or idx)),
            metadata={"source": "beir_scifact"},
        )
        for idx, item in enumerate(rows)
    ]
    qrel_queries = _queries_from_qrels(
        out,
        dataset_name="irds/beir_scifact",
        query_split="queries",
        qrels_split="qrels",
        limit=records,
        seed=seed,
        category="scientific_claim",
    )
    queries = qrel_queries or _queries_from_records(out)
    notes = ["external retrieval dataset loaded from Hugging Face: irds/beir_scifact"]
    if qrel_queries:
        notes.append("qrels were used when available for relevance labels")
    return _bundle(
        kind="beir_scifact",
        source="irds/beir_scifact",
        records=out,
        queries=queries,
        notes=notes,
        relevance_label_mode="external_qrels" if qrel_queries else "synthetic_text_vector_similarity",
        relevance_label_scope="retrieval_quality" if qrel_queries else "synthetic_retrieval_quality",
    )


def codesearchnet_dataset(records: int, seed: int) -> DatasetBundle:
    rows = _load_hf_rows("mteb/CodeSearchNetRetrieval", "corpus", records, seed)
    out = [
        BenchRecord(
            record_id=str(item.get("_id") or item.get("id") or f"code-{idx}"),
            tenant_id=f"tenant-{idx % 4}",
            title=str(item.get("title") or item.get("metadata", {}).get("language") or f"Code {idx}"),
            body=str(item.get("text") or item.get("contents") or ""),
            category="code_search",
            status="published",
            rating=0.0,
            year=2024,
            vector=deterministic_vector(str(item.get("text") or item.get("contents") or idx)),
            metadata={"source": "mteb/CodeSearchNetRetrieval"},
        )
        for idx, item in enumerate(rows)
    ]
    qrel_queries = _queries_from_qrels(
        out,
        dataset_name="mteb/CodeSearchNetRetrieval",
        query_split="queries",
        qrels_split="qrels",
        limit=records,
        seed=seed,
        category="code_search",
    )
    queries = qrel_queries or _queries_from_records(out)
    notes = ["external code retrieval dataset loaded from Hugging Face"]
    if qrel_queries:
        notes.append("qrels were used when available for relevance labels")
    return _bundle(
        kind="codesearchnet",
        source="mteb/CodeSearchNetRetrieval",
        records=out,
        queries=queries,
        notes=notes,
        relevance_label_mode="external_qrels" if qrel_queries else "synthetic_text_vector_similarity",
        relevance_label_scope="retrieval_quality" if qrel_queries else "synthetic_retrieval_quality",
    )


def _load_hf_rows(dataset_name: str, split: str, records: int, seed: int) -> list[dict[str, Any]]:
    try:
        from datasets import load_dataset
    except ImportError as error:
        raise RuntimeError(
            "external datasets require `pip install -r benchmarks/realworld/requirements.txt`"
        ) from error
    dataset = load_dataset(dataset_name, split=split)
    if hasattr(dataset, "shuffle"):
        dataset = dataset.shuffle(seed=seed)
    return [dict(row) for row in dataset.select(range(min(records, len(dataset))))]


def _vector_or_deterministic(value: Any, seed_text: str) -> list[float]:
    if isinstance(value, list) and value:
        values = [float(item) for item in value[:8]]
        if len(values) < 8:
            values.extend(deterministic_vector(seed_text)[len(values) :])
        return values
    return deterministic_vector(seed_text)


def _year_from_value(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value)
    for token in text.replace("-", " ").replace("/", " ").split():
        if token.isdigit() and len(token) == 4:
            return int(token)
    return None


def _queries_from_records(
    records: list[BenchRecord],
    *,
    prefer_oracle_rank: bool = True,
) -> list[BenchQuery]:
    queries = []
    for category, phrase in TOPICS:
        candidates = [record for record in records if record.category == category]
        if not candidates:
            continue
        first = candidates[0]
        tenant_candidates = [
            record for record in candidates if record.tenant_id == first.tenant_id
        ]
        if prefer_oracle_rank and all(_oracle_rank(record) is not None for record in tenant_candidates):
            scored = [
                (-float(_oracle_rank(record) or 0), record.record_id)
                for record in tenant_candidates
            ]
        else:
            scored = [
                (
                    text_score(phrase, record.text()) + cosine(first.vector, record.vector),
                    record.record_id,
                )
                for record in tenant_candidates
            ]
        scored.sort(key=lambda item: (-item[0], item[1]))
        expected_ids = [record_id for _, record_id in scored[:5]]
        queries.append(
            BenchQuery(
                query_id=f"q-{category}",
                tenant_id=first.tenant_id,
                text=phrase,
                category=category,
                vector=first.vector,
                expected_ids=expected_ids,
            )
        )
    if not queries and records:
        first = records[0]
        queries.append(
            BenchQuery(
                query_id="q-default",
                tenant_id=first.tenant_id,
                text=first.body,
                category=first.category,
                vector=first.vector,
                expected_ids=[first.record_id],
            )
        )
    return queries


def _queries_from_qrels(
    records: list[BenchRecord],
    *,
    dataset_name: str,
    query_split: str,
    qrels_split: str,
    limit: int,
    seed: int,
    category: str,
) -> list[BenchQuery]:
    corpus_ids = {record.record_id for record in records}
    if not corpus_ids:
        return []
    try:
        query_rows = _load_hf_rows(dataset_name, query_split, limit, seed)
        qrel_rows = _load_hf_rows(dataset_name, qrels_split, limit * 10, seed)
    except Exception:
        return []
    query_text = {
        _first_present(row, ["query_id", "query-id", "_id", "id"]): str(
            _first_present(row, ["text", "query", "contents", "title"]) or ""
        )
        for row in query_rows
    }
    relevant: dict[str, list[str]] = {}
    for row in qrel_rows:
        query_id = _first_present(row, ["query_id", "query-id", "qid", "id"])
        doc_id = _first_present(row, ["doc_id", "doc-id", "corpus_id", "document_id"])
        score = _first_present(row, ["relevance", "score", "label"])
        if query_id is None or doc_id is None or str(doc_id) not in corpus_ids:
            continue
        try:
            relevance = float(score if score is not None else 1.0)
        except (TypeError, ValueError):
            relevance = 1.0
        if relevance <= 0:
            continue
        relevant.setdefault(str(query_id), []).append(str(doc_id))
    by_id = {record.record_id: record for record in records}
    queries: list[BenchQuery] = []
    for query_id, expected_ids in relevant.items():
        if not expected_ids or query_id not in query_text:
            continue
        first = by_id[expected_ids[0]]
        queries.append(
            BenchQuery(
                query_id=f"qrel-{query_id}",
                tenant_id=first.tenant_id,
                text=query_text[query_id],
                category=category,
                vector=deterministic_vector(query_text[query_id]),
                expected_ids=expected_ids[:5],
            )
        )
        if len(queries) >= 24:
            break
    return queries


def _first_present(row: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _oracle_rank(record: BenchRecord) -> int | None:
    benchmark = record.metadata.get("benchmark")
    if isinstance(benchmark, dict) and isinstance(benchmark.get("oracle_rank"), int):
        return benchmark["oracle_rank"]
    return None


def _bundle(
    *,
    kind: str,
    source: str,
    records: list[BenchRecord],
    queries: list[BenchQuery],
    notes: list[str],
    embedding_model: str = DETERMINISTIC_EMBEDDING_MODEL,
    embedding_source: str = "deterministic",
    relevance_label_mode: str = "unspecified",
    relevance_label_scope: str = "unknown",
    relevance_label_notes: list[str] | None = None,
) -> DatasetBundle:
    dimensions = len(records[0].vector) if records else VECTOR_DIMENSIONS
    digest = _dataset_digest(kind, source, records, queries, embedding_model, dimensions)
    return DatasetBundle(
        kind=kind,
        source=source,
        records=records,
        queries=queries,
        notes=notes,
        embedding_model=embedding_model,
        embedding_dimensions=dimensions,
        embedding_source=embedding_source,
        relevance_label_mode=relevance_label_mode,
        relevance_label_scope=relevance_label_scope,
        relevance_label_notes=relevance_label_notes or [],
        digest=digest,
    )


def _dataset_digest(
    kind: str,
    source: str,
    records: list[BenchRecord],
    queries: list[BenchQuery],
    embedding_model: str,
    dimensions: int,
) -> str:
    hasher = hashlib.sha256()
    hasher.update(
        json.dumps(
            {
                "kind": kind,
                "source": source,
                "embedding_model": embedding_model,
                "embedding_dimensions": dimensions,
                "record_count": len(records),
                "query_count": len(queries),
            },
            sort_keys=True,
        ).encode("utf-8")
    )
    for record in records:
        hasher.update(
            json.dumps(
                {
                    "id": record.record_id,
                    "tenant_id": record.tenant_id,
                    "category": record.category,
                    "text": record.text(),
                    "vector": [round(value, 8) for value in record.vector],
                },
                sort_keys=True,
            ).encode("utf-8")
        )
    for query in queries:
        hasher.update(
            json.dumps(
                {
                    "id": query.query_id,
                    "tenant_id": query.tenant_id,
                    "category": query.category,
                    "text": query.text,
                    "expected_ids": query.expected_ids,
                    "vector": [round(value, 8) for value in query.vector],
                },
                sort_keys=True,
            ).encode("utf-8")
        )
    return hasher.hexdigest()
