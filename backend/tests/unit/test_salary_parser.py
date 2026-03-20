"""Unit tests for the AU salary parser."""

import pytest
from app.utils.salary_parser import AUSalaryParser, salary_normalizer


@pytest.fixture
def parser():
    return AUSalaryParser()


class TestAUSalaryParser:
    # Annual salary ranges
    def test_dollar_range_commas(self, parser):
        result = parser.parse("$80,000 - $120,000")
        assert result.min_value == 80000
        assert result.max_value == 120000
        assert result.currency == "AUD"
        assert result.period == "annual"

    def test_k_range(self, parser):
        result = parser.parse("$80K - $120K")
        assert result.min_value == 80000
        assert result.max_value == 120000

    def test_aud_explicit(self, parser):
        result = parser.parse("A$90,000 - A$110,000 per annum")
        assert result.min_value == 90000
        assert result.max_value == 110000
        assert result.currency == "AUD"
        assert result.period == "annual"

    def test_super_detected(self, parser):
        result = parser.parse("$120,000 + super")
        assert result.min_value == 120000
        assert result.includes_super is True

    def test_bonus_and_super(self, parser):
        result = parser.parse("$100K-$120K + super + bonus")
        assert result.min_value == 100000
        assert result.max_value == 120000
        assert result.includes_super is True

    # Hourly
    def test_hourly(self, parser):
        result = parser.parse("$45/hr")
        assert result.min_value == 45
        assert result.period == "hourly"
        assert result.currency == "AUD"

    def test_hourly_per_hour(self, parser):
        result = parser.parse("$40 per hour")
        assert result.min_value == 40
        assert result.period == "hourly"

    # Daily
    def test_daily(self, parser):
        result = parser.parse("$800/day")
        assert result.min_value == 800
        assert result.period == "daily"

    # Unparseable
    def test_competitive(self, parser):
        result = parser.parse("Competitive")
        assert result.is_parseable is False
        assert result.min_value is None

    def test_doe(self, parser):
        result = parser.parse("DOE")
        assert result.is_parseable is False

    def test_negotiable(self, parser):
        result = parser.parse("Negotiable")
        assert result.is_parseable is False

    # Other currencies
    def test_gbp(self, parser):
        result = parser.parse("£45,000 - £55,000")
        assert result.currency == "GBP"
        assert result.min_value == 45000

    def test_usd(self, parser):
        result = parser.parse("US$90,000")
        assert result.currency == "USD"
        assert result.min_value == 90000

    # Edge cases
    def test_empty(self, parser):
        result = parser.parse("")
        assert result.is_parseable is False

    def test_single_value(self, parser):
        result = parser.parse("$95,000")
        assert result.min_value == 95000
        assert result.max_value is None

    def test_normalizer_singleton(self):
        result = salary_normalizer.normalize("$80,000 - $100,000 + super")
        assert result.min_value == 80000
        assert result.max_value == 100000
