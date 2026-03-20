"""Unit tests for the AU location parser."""

import pytest
from app.utils.location_parser import AULocationParser, location_normalizer


@pytest.fixture
def parser():
    return AULocationParser()


class TestAULocationParser:
    # Happy path
    def test_city_state_comma(self, parser):
        result = parser.parse("Sydney, NSW")
        assert result.city == "Sydney"
        assert result.state_abbr == "NSW"
        assert result.country == "Australia"

    def test_city_state_space(self, parser):
        result = parser.parse("Melbourne VIC 3000")
        assert result.city == "Melbourne"
        assert result.state_abbr == "VIC"
        assert result.postcode == "3000"

    def test_full_state_name(self, parser):
        result = parser.parse("Brisbane, Queensland")
        assert result.city == "Brisbane"
        assert result.state == "Queensland"
        assert result.state_abbr == "QLD"

    def test_perth_wa(self, parser):
        result = parser.parse("Perth, WA")
        assert result.city == "Perth"
        assert result.state_abbr == "WA"

    # Remote detection
    def test_fully_remote(self, parser):
        result = parser.parse("Remote - Australia")
        assert result.is_remote is True
        assert result.remote_type == "fully_remote"

    def test_wfh(self, parser):
        result = parser.parse("WFH")
        assert result.is_remote is True

    def test_hybrid(self, parser):
        result = parser.parse("Hybrid - Sydney, NSW")
        assert result.is_remote is True
        assert result.remote_type == "hybrid"
        assert result.city == "Sydney"

    def test_onsite(self, parser):
        result = parser.parse("On-site, Melbourne")
        assert result.is_remote is False
        assert result.remote_type == "onsite"

    # Edge cases
    def test_empty_string(self, parser):
        result = parser.parse("")
        assert result.city is None

    def test_just_australia(self, parser):
        result = parser.parse("Australia")
        assert result.country == "Australia"
        assert result.city is None

    def test_all_states(self, parser):
        for abbr in ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]:
            result = parser.parse(f"City, {abbr}")
            assert result.state_abbr == abbr, f"Expected {abbr}, got {result.state_abbr}"

    def test_normalizer_singleton(self):
        result = location_normalizer.normalize("Gold Coast, QLD")
        assert result.city == "Gold Coast"
        assert result.state_abbr == "QLD"
