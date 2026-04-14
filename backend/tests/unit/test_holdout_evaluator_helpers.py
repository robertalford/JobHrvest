"""Unit tests for the pure helpers in holdout_evaluator.

The full async DB-backed evaluator needs a live Postgres; these tests cover
the pure-logic pieces: metric aggregation and fuzzy title matching.
"""

from app.ml.champion_challenger.holdout_evaluator import (
    _DomainResult,
    _compute_metrics,
    _count_fuzzy_title_matches,
)


class TestComputeMetrics:
    def test_empty_returns_empty(self):
        assert _compute_metrics([]) == {}

    def test_perfect_classifier(self):
        results = [
            _DomainResult(domain=f"d{i}.com", market_id="AU", ats_platform=None,
                          is_career_pred=1, is_career_true=1, classifier_proba=0.9,
                          extracted_count=10, matched_titles=10,
                          job_coverage=1.0, title_accuracy=1.0)
            for i in range(10)
        ]
        m = _compute_metrics(results)
        assert m["precision"] == 1.0
        assert m["recall"] == 1.0
        assert m["f1"] == 1.0
        assert m["job_coverage_rate"] == 1.0
        assert m["title_accuracy"] == 1.0
        assert m["domain_success_rate"] == 1.0
        assert m["error_rate"] == 0.0

    def test_partial_misses(self):
        results = [
            _DomainResult(domain="a.com", market_id="AU", ats_platform=None,
                          is_career_pred=1, is_career_true=1, job_coverage=0.9, title_accuracy=0.85),
            _DomainResult(domain="b.com", market_id="AU", ats_platform=None,
                          is_career_pred=0, is_career_true=1, job_coverage=0.0, title_accuracy=0.0),
        ]
        m = _compute_metrics(results)
        assert m["precision"] == 1.0     # 1 TP, 0 FP
        assert m["recall"] == 0.5        # 1 TP / (1 TP + 1 FN)
        assert m["f1"] > 0.6 and m["f1"] < 0.7
        assert m["domain_success_rate"] == 0.5  # only one domain meets coverage AND title bars

    def test_errors_counted_separately(self):
        results = [
            _DomainResult(domain="a.com", market_id="AU", ats_platform=None,
                          is_career_pred=1, is_career_true=1, job_coverage=1.0, title_accuracy=1.0),
            _DomainResult(domain="b.com", market_id="AU", ats_platform=None,
                          error="snapshot load failed"),
        ]
        m = _compute_metrics(results)
        assert m["error_rate"] == 0.5
        # Valid set only has 1 domain → metrics computed against it
        assert m["f1"] == 1.0


class TestFuzzyTitleMatches:
    def test_exact_match(self):
        assert _count_fuzzy_title_matches(["Senior Engineer"], ["Senior Engineer"]) == 1

    def test_close_match_above_threshold(self):
        # Small wording difference — should match under both fuzz and substring fallback
        assert _count_fuzzy_title_matches(
            ["Senior Software Engineer"],
            ["Senior Software Engineer (Backend)"],
        ) >= 1

    def test_no_match(self):
        assert _count_fuzzy_title_matches(["Senior Engineer"], ["Marketing Manager"]) == 0

    def test_empty_extracted(self):
        assert _count_fuzzy_title_matches(["Senior Engineer"], []) == 0

    def test_empty_gold(self):
        assert _count_fuzzy_title_matches([], ["Senior Engineer"]) == 0
