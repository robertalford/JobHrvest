"""Unit tests for PSI-based drift detection."""

import numpy as np

from app.ml.champion_challenger.drift_monitor import (
    psi_numeric,
    psi_categorical,
    baseline_distribution_to_json,
)


class TestPsiNumeric:
    def test_identical_distributions_negligible(self):
        rng = np.random.default_rng(0)
        x = rng.normal(0, 1, size=5000)
        result = psi_numeric(x, x.copy(), n_bins=10)
        assert result.psi < 0.01
        assert result.severity == "negligible"

    def test_shifted_mean_significant(self):
        rng = np.random.default_rng(0)
        baseline = rng.normal(0, 1, size=5000)
        current = rng.normal(2, 1, size=5000)  # 2 sigma shift
        result = psi_numeric(baseline, current, n_bins=10)
        assert result.psi > 0.25
        assert result.severity == "significant"

    def test_moderate_drift_classified_correctly(self):
        rng = np.random.default_rng(0)
        baseline = rng.normal(0, 1, size=5000)
        current = rng.normal(0.4, 1, size=5000)  # mild shift
        result = psi_numeric(baseline, current, n_bins=10)
        assert 0.05 < result.psi < 0.4
        assert result.severity in ("moderate", "significant")

    def test_too_few_baseline_returns_zero(self):
        result = psi_numeric([1.0, 2.0], [1.0, 2.0, 3.0], n_bins=10)
        assert result.psi == 0.0


class TestPsiCategorical:
    def test_identical_distributions_negligible(self):
        baseline = ["greenhouse"] * 100 + ["lever"] * 50 + ["workday"] * 50
        current = baseline.copy()
        result = psi_categorical(baseline, current)
        assert result.psi < 0.001

    def test_new_category_appears(self):
        baseline = ["greenhouse"] * 100 + ["lever"] * 100
        current = ["greenhouse"] * 50 + ["lever"] * 50 + ["workday"] * 100
        result = psi_categorical(baseline, current)
        assert result.psi > 0.1
        assert "workday" in result.bins

    def test_empty_inputs(self):
        result = psi_categorical([], [])
        assert result.psi == 0.0
        assert result.severity == "negligible"


class TestBaselineSerialization:
    def test_numeric_serialises(self):
        rng = np.random.default_rng(0)
        x = rng.normal(0, 1, 1000)
        result = psi_numeric(x, x, n_bins=5, feature_name="word_count")
        payload = baseline_distribution_to_json(result)
        assert payload["type"] == "histogram"
        assert "bins" in payload and "share" in payload
        assert payload["sample_size"] == 1000

    def test_categorical_serialises(self):
        result = psi_categorical(["a", "b", "a"], ["a", "b"], feature_name="ats")
        payload = baseline_distribution_to_json(result)
        assert payload["type"] == "categorical"
        assert sorted(payload["bins"]) == ["a", "b"]
