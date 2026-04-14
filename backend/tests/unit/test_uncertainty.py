"""Unit tests for uncertainty-based active sampling."""

import pytest

from app.ml.champion_challenger.uncertainty import (
    margin_uncertainty,
    select_uncertain,
    stratified_uncertain,
)


class TestMarginUncertainty:
    def test_zero_at_extremes(self):
        assert margin_uncertainty(0.0) == pytest.approx(0.0)
        assert margin_uncertainty(1.0) == pytest.approx(0.0)

    def test_max_at_boundary(self):
        assert margin_uncertainty(0.5) == pytest.approx(0.5)

    def test_symmetric(self):
        assert margin_uncertainty(0.7) == pytest.approx(margin_uncertainty(0.3))


class TestSelectUncertain:
    def test_excludes_confident(self):
        preds = [
            ("p1", "https://a.com/", 0.99),
            ("p2", "https://b.com/", 0.01),
            ("p3", "https://c.com/", 0.50),
        ]
        out = select_uncertain(preds, model_version_id="m1", top_k=10, min_uncertainty=0.10)
        ids = [c.crawled_page_id for c in out]
        assert "p3" in ids
        assert "p1" not in ids and "p2" not in ids

    def test_orders_by_uncertainty_descending(self):
        preds = [
            ("p1", "https://a.com/", 0.55),  # u=0.45
            ("p2", "https://b.com/", 0.50),  # u=0.50
            ("p3", "https://c.com/", 0.65),  # u=0.35
        ]
        out = select_uncertain(preds, model_version_id="m1", top_k=10)
        assert [c.crawled_page_id for c in out] == ["p2", "p1", "p3"]

    def test_top_k_caps(self):
        preds = [(f"p{i}", f"https://x{i}.com/", 0.5) for i in range(20)]
        out = select_uncertain(preds, model_version_id="m1", top_k=5)
        assert len(out) == 5

    def test_attaches_model_version(self):
        preds = [("p1", "https://a.com/", 0.5)]
        out = select_uncertain(preds, model_version_id="abc")
        assert out[0].model_version_id == "abc"


class TestStratifiedUncertain:
    def test_per_stratum_quota_respected(self):
        # 3 strata × 5 uncertain candidates each
        preds = []
        for stratum in ["greenhouse", "lever", "workday"]:
            for i in range(5):
                preds.append((f"{stratum}-{i}", f"https://{stratum}.com/{i}", 0.5, stratum))
        out = stratified_uncertain(preds, model_version_id="m1", per_stratum=2)
        per_stratum_counts = {}
        for c in out:
            stratum = c.url.split("/")[2].split(".")[0]
            per_stratum_counts[stratum] = per_stratum_counts.get(stratum, 0) + 1
        assert all(v <= 2 for v in per_stratum_counts.values())
        assert len(out) == 6

    def test_skips_confident(self):
        preds = [
            ("p1", "https://a.com/", 0.99, "x"),
            ("p2", "https://b.com/", 0.50, "y"),
        ]
        out = stratified_uncertain(preds, model_version_id="m1", per_stratum=5)
        assert [c.crawled_page_id for c in out] == ["p2"]
