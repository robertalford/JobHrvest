"""Unit tests for latency budget tracker pure-logic helpers."""

import math

from app.ml.champion_challenger.latency_budget import (
    percentiles,
    check_budget,
)


class TestPercentiles:
    def test_empty_returns_nans(self):
        out = percentiles([])
        assert all(math.isnan(out[k]) for k in ("p50", "p95", "p99"))

    def test_single_value(self):
        out = percentiles([42.0])
        assert out["p50"] == out["p95"] == out["p99"] == 42.0

    def test_known_values(self):
        out = percentiles(list(range(1, 101)))  # 1..100
        # Linear interpolation: p50 of 1..100 ≈ 50.5
        assert 49.0 < out["p50"] < 52.0
        assert 94.0 < out["p95"] < 96.0
        assert 98.0 < out["p99"] < 100.0


class TestCheckBudget:
    def test_within_budget(self):
        out = check_budget(p95_ms=120.0, budget_ms=200.0, sample_size=500)
        assert out.within_budget is True
        assert "OK" in out.reason

    def test_over_budget(self):
        out = check_budget(p95_ms=350.0, budget_ms=200.0, sample_size=500)
        assert out.within_budget is False
        assert "OVER" in out.reason

    def test_insufficient_samples_passes(self):
        out = check_budget(p95_ms=999.0, budget_ms=10.0, sample_size=10)
        # Defer judgement — not enough data
        assert out.within_budget is True
        assert "insufficient samples" in out.reason
