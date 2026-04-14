"""Distribution drift detection for inference inputs.

Uses Population Stability Index (PSI) — the standard credit-risk industry
metric for "is the input distribution today the same as it was when we
trained?". PSI is interpreted as:

  PSI < 0.1   : negligible drift
  0.1–0.25    : moderate drift, watch
  > 0.25      : significant drift, retrain

This module is intentionally feature-agnostic. The same code computes drift
on a numeric feature ("page_word_count"), a categorical one ("ats_platform"),
or a model output ("predicted_proba").
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np


@dataclass
class DriftResult:
    feature_name: str
    psi: float
    severity: str           # 'negligible' | 'moderate' | 'significant'
    bins: list              # bin edges (numeric) or category labels
    baseline_share: list[float]
    current_share: list[float]
    sample_size_baseline: int
    sample_size_current: int


def _safe_share(counts: np.ndarray, smoothing: float = 1e-4) -> np.ndarray:
    """Convert counts to shares with Laplace smoothing to avoid log(0)."""
    smoothed = counts + smoothing
    return smoothed / smoothed.sum()


def psi_numeric(
    baseline: Sequence[float],
    current: Sequence[float],
    *,
    n_bins: int = 10,
    feature_name: str = "feature",
) -> DriftResult:
    """PSI on a continuous feature, using baseline quantiles to define bins.

    Quantile-based bins (rather than fixed-width) keep the test sensitive to
    where the baseline's mass actually sits — fixed-width bins waste
    resolution on tails.
    """
    base = np.asarray(baseline, dtype=float)
    cur = np.asarray(current, dtype=float)
    base = base[~np.isnan(base)]
    cur = cur[~np.isnan(cur)]
    if len(base) < n_bins or len(cur) == 0:
        return DriftResult(
            feature_name=feature_name, psi=0.0, severity="negligible",
            bins=[], baseline_share=[], current_share=[],
            sample_size_baseline=len(base), sample_size_current=len(cur),
        )

    quantiles = np.linspace(0, 1, n_bins + 1)
    edges = np.quantile(base, quantiles)
    edges[0] = -np.inf
    edges[-1] = np.inf
    edges = np.unique(edges)  # collapse duplicates if baseline has spikes

    base_counts = np.histogram(base, bins=edges)[0]
    cur_counts = np.histogram(cur, bins=edges)[0]
    base_share = _safe_share(base_counts)
    cur_share = _safe_share(cur_counts)

    psi = float(np.sum((cur_share - base_share) * np.log(cur_share / base_share)))
    return DriftResult(
        feature_name=feature_name,
        psi=psi,
        severity=_classify(psi),
        bins=edges.tolist(),
        baseline_share=base_share.tolist(),
        current_share=cur_share.tolist(),
        sample_size_baseline=len(base),
        sample_size_current=len(cur),
    )


def psi_categorical(
    baseline: Sequence[str],
    current: Sequence[str],
    *,
    feature_name: str = "feature",
) -> DriftResult:
    """PSI on a categorical feature."""
    categories = sorted(set(baseline) | set(current))
    if not categories:
        return DriftResult(
            feature_name=feature_name, psi=0.0, severity="negligible",
            bins=[], baseline_share=[], current_share=[],
            sample_size_baseline=0, sample_size_current=0,
        )
    base_counts = np.array([sum(1 for v in baseline if v == c) for c in categories], dtype=float)
    cur_counts = np.array([sum(1 for v in current if v == c) for c in categories], dtype=float)
    base_share = _safe_share(base_counts)
    cur_share = _safe_share(cur_counts)
    psi = float(np.sum((cur_share - base_share) * np.log(cur_share / base_share)))
    return DriftResult(
        feature_name=feature_name,
        psi=psi,
        severity=_classify(psi),
        bins=categories,
        baseline_share=base_share.tolist(),
        current_share=cur_share.tolist(),
        sample_size_baseline=len(baseline),
        sample_size_current=len(current),
    )


def _classify(psi: float) -> str:
    if psi < 0.1:
        return "negligible"
    if psi < 0.25:
        return "moderate"
    return "significant"


def baseline_distribution_to_json(result: DriftResult) -> dict:
    """Serialise a baseline DriftResult for storage in drift_baselines.distribution."""
    return {
        "type": "histogram" if all(isinstance(b, (int, float)) for b in result.bins) else "categorical",
        "bins": result.bins,
        "share": result.baseline_share,
        "sample_size": result.sample_size_baseline,
    }
