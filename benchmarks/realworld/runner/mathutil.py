from __future__ import annotations

import math
import random
from collections.abc import Iterable

from .types import VECTOR_DIMENSIONS


def deterministic_vector(seed_text: str, dimensions: int = VECTOR_DIMENSIONS) -> list[float]:
    seed = sum((idx + 1) * ord(char) for idx, char in enumerate(seed_text))
    rng = random.Random(seed)
    values = [rng.uniform(-1.0, 1.0) for _ in range(dimensions)]
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return [round(value / norm, 6) for value in values]


def cosine(left: Iterable[float], right: Iterable[float]) -> float:
    left_values = list(left)
    right_values = list(right)
    dot = sum(a * b for a, b in zip(left_values, right_values))
    left_norm = math.sqrt(sum(value * value for value in left_values)) or 1.0
    right_norm = math.sqrt(sum(value * value for value in right_values)) or 1.0
    return dot / (left_norm * right_norm)


def text_score(query: str, body: str) -> float:
    query_terms = {term for term in query.lower().split() if term}
    body_terms = {term for term in body.lower().split() if term}
    if not query_terms:
        return 0.0
    return len(query_terms & body_terms) / len(query_terms)
