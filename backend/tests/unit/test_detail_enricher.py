"""Unit tests for the DetailEnricher.

These tests validate the two high-risk behaviours:
  1. ATS-specific selectors drive high-confidence extraction.
  2. Enrichment never overwrites non-empty listing fields.

They use small hand-authored fixtures so the tests run offline.
"""

from __future__ import annotations

import asyncio
import pytest

from app.crawlers.detail_enricher import (
    DetailEnricher,
    EnrichmentBudget,
    _extract_detail_fields,
    _clean_text,
)


GREENHOUSE_DETAIL = """
<html><body>
<h1>Senior Platform Engineer</h1>
<div class="job__location">Sydney, NSW · Australia</div>
<article id="content" class="description">
  <p>We're looking for a senior platform engineer to join our SRE team. You'll work
  on the infrastructure that powers our product, from CI/CD pipelines through to
  production observability. This is a hands-on role with a strong emphasis on
  automation, reliability, and security.</p>
  <p>You'll collaborate closely with engineers across the company, own systems
  end-to-end, and mentor junior team members. We're looking for someone who
  thinks in systems and enjoys the mix of architecture, coding, and operational
  work.</p>
  <p>Required experience: 6+ years working with AWS or GCP, deep familiarity with
  Terraform and Kubernetes, hands-on experience running production systems on
  call, and a track record of mentoring other engineers.</p>
</article>
<span class="employment">Full-Time</span>
</body></html>
"""

JSON_LD_DETAIL = """
<html><body>
<script type="application/ld+json">
{"@type": "JobPosting",
 "title": "Data Scientist",
 "description": "Solve hard problems with data at scale.",
 "employmentType": "PART_TIME",
 "jobLocation": {"@type": "Place",
    "address": {"@type": "PostalAddress",
                "addressLocality": "Melbourne",
                "addressRegion": "VIC",
                "addressCountry": "AU"}},
 "baseSalary": {"@type": "MonetaryAmount", "currency": "AUD",
    "value": {"@type": "QuantitativeValue",
              "minValue": 120000, "maxValue": 150000, "unitText": "YEAR"}}}
</script>
</body></html>
"""


def test_clean_text_collapses_whitespace_and_tabs():
    raw = "\t\t\nLine one\n\n\n\nLine   two\t\t\n"
    cleaned = _clean_text(raw)
    assert "\t" not in cleaned
    assert "\n\n\n" not in cleaned
    assert cleaned.startswith("Line one")


def test_extract_uses_ats_selectors():
    fields = _extract_detail_fields(
        GREENHOUSE_DETAIL, "https://boards.greenhouse.io/acme/jobs/123",
        {
            "details_page_description_paths": ["#content.description"],
            "details_page_location_paths": [".job__location"],
            "details_page_job_type_paths": [".employment"],
        },
    )
    assert "platform engineer" in fields.get("description", "").lower()
    assert "Sydney" in fields.get("location_raw", "")
    assert fields.get("employment_type") == "Full-Time"


def test_extract_falls_back_to_json_ld():
    fields = _extract_detail_fields(JSON_LD_DETAIL, "https://example.com/job/1", {})
    assert "Solve hard problems" in fields.get("description", "")
    assert "Melbourne" in fields.get("location_raw", "")
    assert fields.get("employment_type") == "PART_TIME"
    assert "120000" in fields.get("salary_raw", "")


def test_extract_regex_employment_type_fallback():
    html = """<html><body><article>
    <p>This is a Contract role based in Sydney.</p>
    <p>Competitive package offered.</p>
    </article></body></html>"""
    fields = _extract_detail_fields(html, "https://example.com/j/1", {})
    assert fields.get("employment_type", "").lower().startswith("contract")


@pytest.mark.asyncio
async def test_enricher_fills_blank_fields_only():
    """Blank fields get filled; non-empty listing fields survive untouched.

    The fixture combines a rich article body (description picks up via the
    generic fallback) with JSON-LD (location + employment_type via structured
    data). No ATS template is involved here — this exercises the path used
    for unknown ATSes.
    """
    html = GREENHOUSE_DETAIL + JSON_LD_DETAIL

    async def fetch(url: str) -> str:
        return html

    enricher = DetailEnricher(
        http_fetch=fetch,
        budget=EnrichmentBudget(max_pages=5, total_deadline_s=2.0),
    )

    jobs = [
        # Job A: blank description + no location — both should be filled
        {"title": "Senior Platform Engineer",
         "source_url": "https://example.com/a",
         "description": ""},
        # Job B: already has a location from the listing — must not be overwritten
        {"title": "Senior Platform Engineer",
         "source_url": "https://example.com/b",
         "description": "",
         "location_raw": "Remote (AU)"},
    ]
    _, report = await enricher.enrich(jobs, ats=None, page_url="https://example.com")

    assert report.attempted == 2
    assert jobs[0]["description"].strip()                # filled via generic fallback
    assert "Melbourne" in jobs[0].get("location_raw", "")  # filled via JSON-LD
    assert jobs[1]["location_raw"] == "Remote (AU)"       # preserved


@pytest.mark.asyncio
async def test_enricher_skips_when_listing_description_is_long():
    async def fetch(url: str) -> str:  # pragma: no cover — should not be called
        raise AssertionError("enricher should not fetch for long-description jobs")

    enricher = DetailEnricher(
        http_fetch=fetch,
        budget=EnrichmentBudget(max_pages=5, skip_if_description_len_over=200),
    )
    long_desc = "x" * 400
    jobs = [{"title": "t", "source_url": "https://example.com/1", "description": long_desc}]
    _, report = await enricher.enrich(jobs, ats=None, page_url="https://example.com")
    assert report.attempted == 0
