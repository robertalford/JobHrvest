"""Unit tests for the bulk-domain CSV processor (pure CSV parse/build funcs).

The HTTP endpoint and the Champion-model orchestration are tested separately.
These tests pin the contract for the CSV I/O — the part that a user's
production system imports from — so changes to the output schema fail loudly.
"""

import csv
import io

import pytest

from app.services.bulk_domain_processor import (
    CSV_OUTPUT_FIELDS,
    DomainResult,
    build_output_csv,
    parse_input_csv,
)


class TestParseInputCsv:
    def test_single_column_no_header(self):
        csv_text = "atlassian.com\ncanva.com.au\nxero.com\n"
        assert parse_input_csv(csv_text) == ["atlassian.com", "canva.com.au", "xero.com"]

    def test_header_domain_column(self):
        csv_text = "domain\natlassian.com\ncanva.com.au\n"
        assert parse_input_csv(csv_text) == ["atlassian.com", "canva.com.au"]

    def test_strips_whitespace_and_blank_lines(self):
        csv_text = "  atlassian.com  \n\n canva.com.au \n   \n"
        assert parse_input_csv(csv_text) == ["atlassian.com", "canva.com.au"]

    def test_strips_url_scheme_and_path(self):
        csv_text = "https://www.atlassian.com/careers\nhttp://canva.com.au/\n"
        assert parse_input_csv(csv_text) == ["atlassian.com", "canva.com.au"]

    def test_lowercases_domains(self):
        assert parse_input_csv("ATLASSIAN.COM\n") == ["atlassian.com"]

    def test_deduplicates_preserving_order(self):
        csv_text = "atlassian.com\ncanva.com.au\natlassian.com\n"
        assert parse_input_csv(csv_text) == ["atlassian.com", "canva.com.au"]

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="no domains"):
            parse_input_csv("")

    def test_rejects_header_only(self):
        with pytest.raises(ValueError, match="no domains"):
            parse_input_csv("domain\n")


class TestBuildOutputCsv:
    def _rows(self, csv_text: str) -> list[dict]:
        return list(csv.DictReader(io.StringIO(csv_text)))

    def test_headers_match_schema(self):
        csv_text = build_output_csv([])
        reader = csv.reader(io.StringIO(csv_text))
        headers = next(reader)
        assert headers == CSV_OUTPUT_FIELDS

    def test_high_confidence_result_keeps_selectors(self):
        result = DomainResult(
            domain="atlassian.com",
            careers_url="https://atlassian.com/careers",
            listing_url="https://atlassian.com/careers/all",
            pagination_type="numbered",
            pagination_selector=".pagination a",
            requires_js_rendering=False,
            selectors={
                "title": "h1.job-title",
                "location_raw": ".location",
                "description": ".job-description",
            },
            confidence=0.92,
            status="ok",
            error=None,
        )
        rows = self._rows(build_output_csv([result]))
        assert len(rows) == 1
        row = rows[0]
        assert row["domain"] == "atlassian.com"
        assert row["careers_url"] == "https://atlassian.com/careers"
        assert row["selector_title"] == "h1.job-title"
        assert row["selector_location_raw"] == ".location"
        assert row["selector_description"] == ".job-description"
        assert row["confidence"] == "0.92"
        assert row["status"] == "ok"

    def test_low_confidence_blanks_selectors(self):
        """Selectors must only be emitted when confidence >= threshold."""
        result = DomainResult(
            domain="example.com",
            careers_url="https://example.com/careers",
            listing_url=None,
            pagination_type=None,
            pagination_selector=None,
            requires_js_rendering=False,
            selectors={"title": "h1", "location_raw": ".loc"},
            confidence=0.42,
            status="low_confidence",
            error=None,
        )
        rows = self._rows(build_output_csv([result], confidence_threshold=0.8))
        assert rows[0]["domain"] == "example.com"
        # Domain + status retained; selectors blanked
        assert rows[0]["selector_title"] == ""
        assert rows[0]["selector_location_raw"] == ""
        assert rows[0]["status"] == "low_confidence"

    def test_failed_result_emits_error(self):
        result = DomainResult(
            domain="broken.example",
            careers_url=None,
            listing_url=None,
            pagination_type=None,
            pagination_selector=None,
            requires_js_rendering=False,
            selectors={},
            confidence=0.0,
            status="failed",
            error="no careers page found",
        )
        rows = self._rows(build_output_csv([result]))
        assert rows[0]["status"] == "failed"
        assert rows[0]["error"] == "no careers page found"
        assert rows[0]["selector_title"] == ""

    def test_selector_columns_cover_all_target_fields(self):
        """Every TARGET_FIELDS entry must have a selector_<field> column."""
        from app.extractors.template_learner import TARGET_FIELDS

        for field in TARGET_FIELDS:
            assert f"selector_{field}" in CSV_OUTPUT_FIELDS
