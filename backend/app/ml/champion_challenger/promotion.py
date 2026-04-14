"""Promotion gates for champion/challenger comparisons.

A "primary score" point comparison is too noisy to drive an autonomous loop —
a 0.5 percentage-point F1 difference on 100 examples is well within sampling
noise. This module wraps:

  - bootstrap_ci    : 95% CIs for any scalar metric (paired or unpaired)
  - mcnemar_test    : the right test for paired binary classifier comparisons
  - evaluate_gates  : multi-metric promotion gate that requires several signals
                      to point the same way before recommending promotion.

The functions deliberately take plain numpy arrays so they can be unit tested
without a model object in scope.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Sequence

import numpy as np


# ─── Bootstrap CIs ─────────────────────────────────────────────────────────

def bootstrap_ci(
    y_true: Sequence[int],
    y_pred: Sequence[float],
    metric_fn: Callable[[np.ndarray, np.ndarray], float],
    *,
    n_iterations: int = 1000,
    confidence: float = 0.95,
    seed: int = 42,
) -> tuple[float, float, float]:
    """Bootstrap a CI for `metric_fn(y_true, y_pred)`.

    Returns (point_estimate, ci_low, ci_high).
    """
    y_true_arr = np.asarray(y_true)
    y_pred_arr = np.asarray(y_pred)
    if len(y_true_arr) != len(y_pred_arr):
        raise ValueError("y_true and y_pred must be the same length")
    if len(y_true_arr) == 0:
        return float("nan"), float("nan"), float("nan")

    rng = np.random.default_rng(seed)
    n = len(y_true_arr)
    samples = np.empty(n_iterations, dtype=float)
    for i in range(n_iterations):
        idx = rng.integers(0, n, size=n)
        try:
            samples[i] = metric_fn(y_true_arr[idx], y_pred_arr[idx])
        except (ValueError, ZeroDivisionError):
            samples[i] = np.nan

    point = float(metric_fn(y_true_arr, y_pred_arr))
    finite = samples[~np.isnan(samples)]
    if len(finite) == 0:
        return point, float("nan"), float("nan")
    alpha = (1 - confidence) / 2
    low = float(np.quantile(finite, alpha))
    high = float(np.quantile(finite, 1 - alpha))
    return point, low, high


# ─── McNemar's test ────────────────────────────────────────────────────────

def mcnemar_test(
    y_true: Sequence[int],
    champion_pred: Sequence[int],
    challenger_pred: Sequence[int],
) -> dict:
    """Paired comparison of two classifiers' binary predictions.

    Returns a dict with keys: b (champion-only-correct), c (challenger-only-correct),
    statistic, p_value (two-sided, exact binomial), and direction ('challenger'|
    'champion'|'tie').

    Uses the exact binomial test on (b, b+c) which is correct for any sample
    size — the chi-squared approximation breaks down when b+c is small, which
    is exactly when we're most tempted to over-interpret a small win.
    """
    yt = np.asarray(y_true)
    cp = np.asarray(champion_pred)
    rp = np.asarray(challenger_pred)
    if not (len(yt) == len(cp) == len(rp)):
        raise ValueError("All three sequences must be the same length")

    champ_correct = cp == yt
    chal_correct = rp == yt
    b = int(np.sum(champ_correct & ~chal_correct))   # champion right, challenger wrong
    c = int(np.sum(~champ_correct & chal_correct))   # champion wrong, challenger right
    n = b + c

    if n == 0:
        return {
            "b": b, "c": c, "n_discordant": 0,
            "statistic": 0.0, "p_value": 1.0, "direction": "tie",
        }

    # Exact two-sided binomial test on (min(b, c), n) under H0: p = 0.5
    k = min(b, c)
    # P(X <= k) under Bin(n, 0.5)
    cum = sum(_binom_pmf(n, i, 0.5) for i in range(k + 1))
    p_two_sided = min(1.0, 2.0 * cum)

    if c > b:
        direction = "challenger"
    elif b > c:
        direction = "champion"
    else:
        direction = "tie"

    statistic = float((abs(b - c) - 1) ** 2 / max(n, 1))  # continuity-corrected χ²

    return {
        "b": b,
        "c": c,
        "n_discordant": n,
        "statistic": statistic,
        "p_value": float(p_two_sided),
        "direction": direction,
    }


def _binom_pmf(n: int, k: int, p: float) -> float:
    from math import comb
    return comb(n, k) * (p ** k) * ((1 - p) ** (n - k))


# ─── Multi-metric promotion gate ───────────────────────────────────────────

@dataclass
class MetricRequirement:
    name: str
    higher_is_better: bool = True
    min_delta: float = 0.0          # challenger must beat champion by at least this much
    weight: float = 1.0             # used only for tiebreaks


@dataclass
class PromotionDecision:
    verdict: str                    # 'promote' | 'reject' | 'inconclusive'
    reasons: list[str] = field(default_factory=list)
    metric_deltas: dict[str, float] = field(default_factory=dict)
    p_value: float | None = None    # from McNemar if available
    n_metrics_won: int = 0
    n_metrics_required: int = 0


def evaluate_gates(
    *,
    champion_metrics: dict[str, float],
    challenger_metrics: dict[str, float],
    requirements: Sequence[MetricRequirement],
    min_metrics_won: int = 2,
    p_value: float | None = None,
    p_value_threshold: float = 0.05,
) -> PromotionDecision:
    """Decide whether to promote the challenger.

    A challenger is promoted only if:
      (a) it beats the champion by at least `min_delta` on at least
          `min_metrics_won` of the listed metrics, AND
      (b) if `p_value` is provided, it must be <= `p_value_threshold`.

    Failing (a) → reject. Failing (b) but passing (a) → inconclusive (run
    again with more data rather than promote on a noisy signal).
    """
    decision = PromotionDecision(verdict="reject")
    decision.n_metrics_required = min_metrics_won
    decision.p_value = p_value

    wins = 0
    for req in requirements:
        champ = champion_metrics.get(req.name)
        chal = challenger_metrics.get(req.name)
        if champ is None or chal is None:
            decision.reasons.append(f"missing metric: {req.name}")
            continue
        delta = (chal - champ) if req.higher_is_better else (champ - chal)
        decision.metric_deltas[req.name] = delta
        if delta >= req.min_delta and delta > 0:
            wins += 1
            decision.reasons.append(
                f"{req.name}: challenger {chal:.4f} vs champion {champ:.4f} "
                f"(Δ={delta:+.4f})"
            )
        else:
            decision.reasons.append(
                f"{req.name}: no meaningful improvement "
                f"(challenger {chal:.4f} vs champion {champ:.4f}, Δ={delta:+.4f})"
            )

    decision.n_metrics_won = wins

    if wins < min_metrics_won:
        decision.verdict = "reject"
        return decision

    if p_value is not None and p_value > p_value_threshold:
        decision.verdict = "inconclusive"
        decision.reasons.append(
            f"p-value {p_value:.4f} > threshold {p_value_threshold} — "
            "improvement may be noise; gather more data before promoting"
        )
        return decision

    decision.verdict = "promote"
    return decision
