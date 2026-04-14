"""Uncertainty-based active sampling.

Pseudo-labelling alone is wasteful — it adds the *easy* cases to the training
set, which the model already gets right. Uncertainty sampling adds the
*hard* cases (those near the decision boundary) to the human review queue,
where a single label has the most marginal value.

This module is the routing layer — it computes uncertainty, ranks
candidates, and exposes the top-K via a simple service-layer call. The
review_feedback table is populated by the existing review API; we just
flag candidates for that workflow.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass
class UncertaintyCandidate:
    """A single page proposed for human review."""
    crawled_page_id: str
    url: str
    predicted_proba: float
    uncertainty: float          # 0 = certain, 0.5 = maximally uncertain (binary)
    model_version_id: str


def margin_uncertainty(proba: float) -> float:
    """For a binary classifier output in [0, 1], distance from the 0.5 boundary
    is the simplest uncertainty proxy. We invert so 0.5 → 0.5 (max) and 0/1 → 0.
    """
    return 0.5 - abs(proba - 0.5)


def select_uncertain(
    predictions: Sequence[tuple[str, str, float]],
    *,
    model_version_id: str,
    top_k: int = 50,
    min_uncertainty: float = 0.10,
) -> list[UncertaintyCandidate]:
    """Pick the top-K most uncertain predictions for human review.

    `predictions` is an iterable of (crawled_page_id, url, predicted_proba).
    We exclude trivially-confident cases (`uncertainty < min_uncertainty`)
    so the review queue isn't padded with already-decided pages.
    """
    candidates: list[UncertaintyCandidate] = []
    for page_id, url, proba in predictions:
        u = margin_uncertainty(proba)
        if u < min_uncertainty:
            continue
        candidates.append(UncertaintyCandidate(
            crawled_page_id=str(page_id),
            url=url,
            predicted_proba=float(proba),
            uncertainty=float(u),
            model_version_id=str(model_version_id),
        ))
    candidates.sort(key=lambda c: c.uncertainty, reverse=True)
    return candidates[:top_k]


def stratified_uncertain(
    predictions: Sequence[tuple[str, str, float, str]],
    *,
    model_version_id: str,
    per_stratum: int = 10,
    min_uncertainty: float = 0.10,
) -> list[UncertaintyCandidate]:
    """Like `select_uncertain` but enforces per-stratum quotas.

    `predictions` is (page_id, url, proba, stratum_key). Useful for ensuring
    the human review queue isn't dominated by a single ATS or market.
    """
    by_stratum: dict[str, list[UncertaintyCandidate]] = {}
    for page_id, url, proba, stratum in predictions:
        u = margin_uncertainty(proba)
        if u < min_uncertainty:
            continue
        by_stratum.setdefault(stratum, []).append(UncertaintyCandidate(
            crawled_page_id=str(page_id),
            url=url,
            predicted_proba=float(proba),
            uncertainty=float(u),
            model_version_id=str(model_version_id),
        ))

    selected: list[UncertaintyCandidate] = []
    for stratum, items in by_stratum.items():
        items.sort(key=lambda c: c.uncertainty, reverse=True)
        selected.extend(items[:per_stratum])
    selected.sort(key=lambda c: c.uncertainty, reverse=True)
    return selected
