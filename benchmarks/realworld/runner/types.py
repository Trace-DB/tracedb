from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

VECTOR_DIMENSIONS = 8
DETERMINISTIC_EMBEDDING_MODEL = "deterministic-vector-v1"


@dataclass(frozen=True)
class BenchRecord:
    record_id: str
    tenant_id: str
    title: str
    body: str
    category: str
    status: str
    rating: float
    year: int
    vector: list[float]
    metadata: dict[str, Any] = field(default_factory=dict)

    def text(self) -> str:
        return f"{self.title} {self.body}"

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.record_id,
            "tenant_id": self.tenant_id,
            "title": self.title,
            "body": self.body,
            "category": self.category,
            "status": self.status,
            "rating": self.rating,
            "year": self.year,
            "vector": self.vector,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class BenchQuery:
    query_id: str
    tenant_id: str
    text: str
    category: str
    vector: list[float]
    expected_ids: list[str]
    top_k: int = 5


@dataclass(frozen=True)
class DatasetBundle:
    kind: str
    source: str
    records: list[BenchRecord]
    queries: list[BenchQuery]
    notes: list[str]
    embedding_model: str = DETERMINISTIC_EMBEDDING_MODEL
    embedding_dimensions: int = VECTOR_DIMENSIONS
    embedding_source: str = "deterministic"
    relevance_label_mode: str = "unspecified"
    relevance_label_scope: str = "unknown"
    relevance_label_notes: list[str] = field(default_factory=list)
    digest: str = ""


@dataclass(frozen=True)
class RunConfig:
    profile: str
    target: list[str]
    surfaces: list[str]
    require_services: bool
    repo_root: str
    openrouter_mode: str = "auto"
    openrouter_cap: str = "moderate"
    run_id: str = ""
    reports_dir: str = "reports"
    observer: Any | None = None
    tracedb_ingest_mode: str = "batch"
