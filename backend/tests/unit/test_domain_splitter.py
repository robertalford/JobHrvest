"""Unit tests for the domain-aware splitter — guards against per-page leakage."""

import pytest

from app.ml.champion_challenger.domain_splitter import (
    extract_registrable_domain,
    split_by_domain,
    assert_holdout_isolation,
)


class TestExtractRegistrableDomain:
    def test_simple_com(self):
        assert extract_registrable_domain("https://www.atlassian.com/careers") == "atlassian.com"

    def test_compound_au_tld(self):
        assert extract_registrable_domain("https://careers.canva.com.au/jobs") == "canva.com.au"

    def test_compound_uk_tld(self):
        assert extract_registrable_domain("https://www.bbc.co.uk/jobs") == "bbc.co.uk"

    def test_subdomain_stripped(self):
        assert extract_registrable_domain("https://boards.greenhouse.io/canva") == "greenhouse.io"

    def test_no_scheme(self):
        assert extract_registrable_domain("careers.atlassian.com") == "atlassian.com"

    def test_empty_input(self):
        assert extract_registrable_domain("") == ""

    def test_lowercases(self):
        assert extract_registrable_domain("https://CAREERS.ATLASSIAN.COM") == "atlassian.com"


class TestSplitByDomain:
    def test_no_domain_leakage_across_splits(self):
        # 50 distinct domains, 3 pages each
        urls = [f"https://www.example{i}.com/page{j}" for i in range(50) for j in range(3)]
        result = split_by_domain(urls, train_frac=0.7, val_frac=0.15, seed=42)
        # The internal assert_no_leakage already runs, but make it explicit
        result.assert_no_leakage()

    def test_pages_from_same_domain_in_same_split(self):
        urls = [
            "https://atlassian.com/careers",
            "https://atlassian.com/jobs",
            "https://atlassian.com/teams",
        ]
        result = split_by_domain(urls, seed=42)
        # All three indices should be in the same bucket
        in_train = all(i in result.train_indices for i in range(3))
        in_val = all(i in result.val_indices for i in range(3))
        in_test = all(i in result.test_indices for i in range(3))
        assert in_train or in_val or in_test, "atlassian.com pages got split"

    def test_deterministic_given_seed(self):
        urls = [f"https://example{i}.com/" for i in range(200)]
        a = split_by_domain(urls, seed=99)
        b = split_by_domain(urls, seed=99)
        assert a.train_indices == b.train_indices
        assert a.val_indices == b.val_indices

    def test_invalid_fractions_rejected(self):
        with pytest.raises(ValueError):
            split_by_domain(["https://x.com/"], train_frac=0.9, val_frac=0.2)
        with pytest.raises(ValueError):
            split_by_domain(["https://x.com/"], train_frac=1.0, val_frac=0.0)

    def test_empty_url_goes_to_test(self):
        result = split_by_domain(["", "https://x.com/"])
        assert 0 in result.test_indices

    def test_approx_proportions(self):
        urls = [f"https://example{i}.com/" for i in range(2000)]
        result = split_by_domain(urls, train_frac=0.7, val_frac=0.15, seed=1)
        n = len(urls)
        assert 0.65 * n < len(result.train_indices) < 0.75 * n
        assert 0.10 * n < len(result.val_indices) < 0.20 * n


class TestHoldoutIsolation:
    def test_passes_when_disjoint(self):
        assert_holdout_isolation(
            ["atlassian.com", "canva.com.au"],
            ["xero.com", "rea-group.com"],
        )

    def test_raises_on_direct_leak(self):
        with pytest.raises(AssertionError, match="leakage"):
            assert_holdout_isolation(
                ["atlassian.com", "xero.com"],
                ["xero.com", "canva.com"],
            )

    def test_raises_on_subdomain_leak(self):
        # "careers.xero.com" normalises to "xero.com" — must catch this
        with pytest.raises(AssertionError, match="leakage"):
            assert_holdout_isolation(
                ["https://careers.xero.com/"],
                ["https://www.xero.com/jobs"],
            )

    def test_ignores_empty_entries(self):
        assert_holdout_isolation(["", "atlassian.com"], ["", "xero.com"])
