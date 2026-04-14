import json
from pathlib import Path

from app.ml.champion_challenger import failure_analysis


def test_noise_score_penalizes_whitespace_and_entities():
    noisy = "Role&nbsp;&nbsp;\n\n\n\tApply&nbsp;Now"
    clean = "Senior Engineer in Melbourne"

    assert failure_analysis._noise_score(noisy) > failure_analysis._noise_score(clean)


def test_landmark_excerpt_returns_first_matching_text():
    html = "<html><body><main><section><h2>Jobs</h2><p>Meaningful excerpt here.</p></section></main></body></html>"

    excerpt = failure_analysis._landmark_excerpt(html, "//section")

    assert "Meaningful excerpt here." in excerpt


def test_build_site_diff_package_summarizes_field_gaps(tmp_path):
    listing_html = tmp_path / "listing.html"
    detail_html = tmp_path / "detail.html"
    listing_html.write_text("<html><body><main><div class='jobs'>Open Roles</div></main></body></html>")
    detail_html.write_text("<html><body><article><p>Detailed job description.</p></article></body></html>")

    entry = {
        "test_url": "https://example.com/jobs",
        "ats_platform": "workday",
        "model_tier": "tier_2",
        "baseline_titles": ["Engineer"],
        "model_titles": ["Engineer"],
        "baseline_jobs": 2,
        "model_jobs": 1,
        "baseline_extracted_jobs": [
            {
                "title": "Engineer",
                "source_url": "https://example.com/jobs/1",
                "location_raw": "Melbourne",
                "salary_raw": "$100000",
                "employment_type": "Full-Time",
                "description": "A" * 600,
            }
        ],
        "model_extracted_jobs": [
            {
                "title": "Engineer",
                "source_url": "https://example.com/jobs/1",
                "location_raw": "",
                "salary_raw": "",
                "employment_type": "",
                "description": "short",
            }
        ],
        "baseline_full_wrapper": {
            "boundary": "//main",
            "title": ".job-title",
            "details_page_description_paths": ["//article"],
        },
    }

    pkg = failure_analysis.build_site_diff_package(
        entry,
        html_file=str(listing_html),
        detail_html_file=str(detail_html),
    )

    assert pkg["url"] == "https://example.com/jobs"
    assert pkg["ats"] == "workday"
    assert pkg["diff"]["field_gap_per_job"]["location_raw"] == 1
    assert pkg["diff"]["description_noise_delta"] >= 0
    assert pkg["html_landmark_excerpts"]["baseline_wrapper_title"]


def test_write_postmortem_drops_hallucinated_references(tmp_path, monkeypatch):
    crawlers = tmp_path / "backend" / "app" / "crawlers"
    crawlers.mkdir(parents=True)
    parent = crawlers / "tiered_extractor_v610.py"
    child = crawlers / "tiered_extractor_v611.py"
    parent.write_text(
        "from app.crawlers.tiered_extractor_v16 import TieredExtractorV16\n\n"
        "class TieredExtractorV610(TieredExtractorV16):\n"
        "    async def extract(self, career_page, company, html):\n"
        "        return await super().extract(career_page, company, html)\n"
    )
    child.write_text(
        "from app.crawlers.tiered_extractor_v16 import TieredExtractorV16\n\n"
        "class TieredExtractorV611(TieredExtractorV16):\n"
        "    async def extract(self, career_page, company, html):\n"
        "        return []\n"
    )

    memory_path = tmp_path / "memory.json"
    memory_path.write_text(json.dumps({"schema": "v2", "recent_rejections": [], "banned_approaches": [], "recent_promotions": [], "baseline": {"version": "v6.9", "composite": 85.4, "axes": {}}}))

    monkeypatch.setattr(failure_analysis, "_repo_root", lambda: tmp_path)
    monkeypatch.setattr(
        failure_analysis,
        "_ollama_postmortem",
        lambda diff_text, outcome: {
            "symptom": "fixture regression",
            "likely_cause": "missing function does_not_exist()",
            "rule_for_future": "call super().extract",
        },
    )

    record = failure_analysis.write_postmortem(
        "v6.11",
        {"gate_failures": ["fixture_harness"], "memory_path": str(memory_path)},
    )

    assert record is None
