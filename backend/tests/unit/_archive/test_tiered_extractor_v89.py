from pathlib import Path

import pytest

from app.crawlers.tiered_extractor_v89 import TieredExtractorV89


_ROOT = Path(__file__).resolve().parents[3]
_CTX = _ROOT / "storage" / "auto_improve_context" / "v8_9"


def _read(name: str) -> str:
    path = _CTX / name
    if not path.exists():
        pytest.skip(f"Missing context file: {path}")
    return path.read_text(encoding="utf-8", errors="replace")


def test_extract_queryid_card_rows_handles_careers_page_cards():
    extractor = TieredExtractorV89()
    html = _read("failure_1_microresumes_careers_page_com.html")

    jobs = extractor._extract_queryid_card_rows_v89(html, "https://microresumes.careers-page.com")

    assert len(jobs) >= 20
    assert any(j["title"] == "Senior Associate, CLCM" for j in jobs)


def test_extract_queryid_card_rows_handles_dense_bootstrap_cards():
    extractor = TieredExtractorV89()
    html = _read("failure_2_career_astra_otoparts_com.html")

    jobs = extractor._extract_queryid_card_rows_v89(html, "https://career.astra-otoparts.com")

    assert len(jobs) >= 50
    assert any(j["title"] == "Operation Control Staff HO" for j in jobs)
    assert any("DetailPekerjaan.aspx?id=" in j["source_url"] for j in jobs)


def test_extract_query_table_rows_keeps_clean_table_titles():
    extractor = TieredExtractorV89()
    html = _read("failure_3_jobs_bmwgroup_com.html")

    jobs = extractor._extract_query_table_jobs_v80(html, "https://jobs.bmwgroup.com/search")

    assert len(jobs) >= 20
    assert jobs[0]["title"] == "Metrologist"
    assert "Apr" not in jobs[0]["title"]


def test_extract_queryid_card_rows_handles_jobcard_theme():
    extractor = TieredExtractorV89()
    html = _read("failure_4_pasonahr_my.html")

    jobs = extractor._extract_queryid_card_rows_v89(html, "https://pasonahr.my/en/job_search/")

    assert len(jobs) >= 18
    assert any(j["title"] == "Credit Analyst-Bank Officer" for j in jobs)
    assert all("resume-entry" not in j["source_url"] for j in jobs)
