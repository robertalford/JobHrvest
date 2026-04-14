"""Unit tests for promotion gates — bootstrap CI, McNemar, multi-metric gate."""

import numpy as np
import pytest

from app.ml.champion_challenger.promotion import (
    bootstrap_ci,
    mcnemar_test,
    evaluate_gates,
    MetricRequirement,
)


def f1(y_true, y_pred):
    """Tiny F1 helper that doesn't need sklearn."""
    yt = np.asarray(y_true)
    yp = np.asarray(y_pred)
    tp = float(np.sum((yp == 1) & (yt == 1)))
    fp = float(np.sum((yp == 1) & (yt == 0)))
    fn = float(np.sum((yp == 0) & (yt == 1)))
    if tp == 0:
        return 0.0
    p = tp / (tp + fp)
    r = tp / (tp + fn)
    return 2 * p * r / (p + r)


class TestBootstrapCI:
    def test_returns_three_floats(self):
        rng = np.random.default_rng(0)
        y_true = rng.integers(0, 2, size=100).tolist()
        y_pred = y_true.copy()  # perfect classifier
        point, low, high = bootstrap_ci(y_true, y_pred, f1, n_iterations=100, seed=1)
        assert point == pytest.approx(1.0)
        assert low <= point <= high

    def test_ci_widens_for_small_samples(self):
        rng = np.random.default_rng(0)
        y_true = rng.integers(0, 2, size=20).tolist()
        y_pred = [int(p ^ rng.integers(0, 2)) for p in y_true]  # noisy
        _, low_small, high_small = bootstrap_ci(y_true, y_pred, f1, n_iterations=200, seed=1)
        # Larger sample → tighter CI
        y_true_large = rng.integers(0, 2, size=2000).tolist()
        y_pred_large = [int(p ^ rng.integers(0, 4) // 4) for p in y_true_large]
        _, low_large, high_large = bootstrap_ci(y_true_large, y_pred_large, f1, n_iterations=200, seed=1)
        assert (high_small - low_small) > (high_large - low_large)

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError):
            bootstrap_ci([0, 1], [0], f1)

    def test_empty_returns_nans(self):
        point, low, high = bootstrap_ci([], [], f1)
        assert all(np.isnan(x) for x in (point, low, high))


class TestMcNemar:
    def test_perfect_tie(self):
        y_true = [0, 1, 0, 1]
        cp = [0, 1, 0, 1]
        rp = [0, 1, 0, 1]
        result = mcnemar_test(y_true, cp, rp)
        assert result["b"] == 0
        assert result["c"] == 0
        assert result["direction"] == "tie"
        assert result["p_value"] == 1.0

    def test_challenger_clearly_better(self):
        # Champion gets all wrong, challenger gets all right
        y_true = [1] * 20
        cp = [0] * 20
        rp = [1] * 20
        result = mcnemar_test(y_true, cp, rp)
        assert result["direction"] == "challenger"
        assert result["p_value"] < 0.001

    def test_champion_better(self):
        y_true = [1] * 20
        cp = [1] * 20
        rp = [0] * 20
        result = mcnemar_test(y_true, cp, rp)
        assert result["direction"] == "champion"

    def test_small_sample_not_significant(self):
        # 3-1 split — directionally challenger but not significant
        y_true = [1, 1, 1, 1]
        cp = [0, 0, 0, 1]   # 1 right
        rp = [1, 1, 1, 0]   # 3 right
        result = mcnemar_test(y_true, cp, rp)
        assert result["direction"] == "challenger"
        assert result["p_value"] > 0.05  # not significant on 4 examples

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            mcnemar_test([0, 1], [0, 1, 0], [0, 1])


class TestEvaluateGates:
    REQS = [
        MetricRequirement("f1", min_delta=0.005),
        MetricRequirement("recall", min_delta=0.005),
        MetricRequirement("precision", min_delta=0.005),
        MetricRequirement("false_positive_rate", higher_is_better=False, min_delta=0.005),
    ]

    def test_promote_when_significant_and_multi_metric_win(self):
        decision = evaluate_gates(
            champion_metrics={"f1": 0.80, "recall": 0.75, "precision": 0.85, "false_positive_rate": 0.10},
            challenger_metrics={"f1": 0.85, "recall": 0.82, "precision": 0.88, "false_positive_rate": 0.07},
            requirements=self.REQS,
            min_metrics_won=2,
            p_value=0.01,
        )
        assert decision.verdict == "promote"
        assert decision.n_metrics_won >= 2

    def test_reject_on_single_metric_win(self):
        decision = evaluate_gates(
            champion_metrics={"f1": 0.80, "recall": 0.75, "precision": 0.85, "false_positive_rate": 0.10},
            challenger_metrics={"f1": 0.81, "recall": 0.74, "precision": 0.84, "false_positive_rate": 0.11},
            requirements=self.REQS,
            min_metrics_won=2,
            p_value=0.01,
        )
        assert decision.verdict == "reject"

    def test_inconclusive_when_p_value_too_high(self):
        decision = evaluate_gates(
            champion_metrics={"f1": 0.80, "recall": 0.75, "precision": 0.85, "false_positive_rate": 0.10},
            challenger_metrics={"f1": 0.85, "recall": 0.82, "precision": 0.88, "false_positive_rate": 0.07},
            requirements=self.REQS,
            min_metrics_won=2,
            p_value=0.30,
        )
        assert decision.verdict == "inconclusive"

    def test_promote_without_p_value(self):
        decision = evaluate_gates(
            champion_metrics={"f1": 0.80, "recall": 0.75, "precision": 0.85, "false_positive_rate": 0.10},
            challenger_metrics={"f1": 0.85, "recall": 0.82, "precision": 0.88, "false_positive_rate": 0.07},
            requirements=self.REQS,
            min_metrics_won=2,
        )
        assert decision.verdict == "promote"

    def test_lower_is_better_metric(self):
        # FPR went UP — should not count as a win
        decision = evaluate_gates(
            champion_metrics={"false_positive_rate": 0.10},
            challenger_metrics={"false_positive_rate": 0.20},
            requirements=[MetricRequirement("false_positive_rate", higher_is_better=False, min_delta=0.005)],
            min_metrics_won=1,
        )
        assert decision.verdict == "reject"

    def test_min_delta_filters_noise(self):
        decision = evaluate_gates(
            champion_metrics={"f1": 0.80},
            challenger_metrics={"f1": 0.801},
            requirements=[MetricRequirement("f1", min_delta=0.005)],
            min_metrics_won=1,
        )
        assert decision.verdict == "reject"  # 0.001 < 0.005 min_delta

    def test_missing_metric_logged(self):
        decision = evaluate_gates(
            champion_metrics={"f1": 0.80},
            challenger_metrics={},
            requirements=[MetricRequirement("f1", min_delta=0.005)],
            min_metrics_won=1,
        )
        assert decision.verdict == "reject"
        assert any("missing metric" in r for r in decision.reasons)
