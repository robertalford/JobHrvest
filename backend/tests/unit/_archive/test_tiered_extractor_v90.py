from pathlib import Path

import pytest

from app.crawlers.tiered_extractor_v90 import TieredExtractorV90


_ROOT = Path(__file__).resolve().parents[3]
_CTX = _ROOT / "storage" / "auto_improve_context" / "v9_0"


def _read(name: str) -> str:
    path = _CTX / name
    if not path.exists():
        pytest.skip(f"Missing context file: {path}")
    return path.read_text(encoding="utf-8", errors="replace")


def test_pagination_urls_fill_missing_pp_sequence():
    extractor = TieredExtractorV90()
    html = _read("gap_2_pasonahr_my.html")

    urls = extractor._collect_listing_pagination_urls_v89(
        html,
        "https://pasonahr.my/en/job_search/",
        max_pages=4,
    )

    assert any("pp=6" in u for u in urls)
    assert urls[0].endswith("pp=2")


def test_pagination_urls_expand_single_page_path_link():
    extractor = TieredExtractorV90()
    html = """
    <html><body>
      <nav class="pagination">
        <a href="/jobs/page/2/">2</a>
      </nav>
    </body></html>
    """

    urls = extractor._collect_listing_pagination_urls_v89(
        html,
        "https://example.org/jobs/",
        max_pages=3,
    )

    assert "https://example.org/jobs/page/2/" in urls
    assert "https://example.org/jobs/page/3/" in urls


def test_awsm_rows_accept_multilingual_titles_with_strong_urls():
    extractor = TieredExtractorV90()
    html = _read("gap_6_karir_szetoaccurate_com.html")

    jobs = extractor._extract_wp_job_openings_v66(html, "https://karir.szetoaccurate.com")
    titles = {j.get("title") for j in jobs}

    assert "Konsultan Accurate Jakarta & Bandung" in titles
    assert "Konsultan Accurate" in titles
    assert len(jobs) >= 2


def test_linked_card_title_validation_rejects_editorial_labels():
    extractor = TieredExtractorV90()

    assert extractor._is_valid_card_title_v67("Powerline Workers", has_strong_job_path=True)
    assert not extractor._is_valid_card_title_v67("Career Guide", has_strong_job_path=True)


def test_linked_cards_drop_career_guide_nav_label():
    extractor = TieredExtractorV90()
    html = _read("spotcheck_3_justdigitalpeople_com_au.html")

    jobs = extractor._extract_linked_job_cards_v67(html, "https://www.justdigitalpeople.com.au/jobs")
    titles = {j.get("title") for j in jobs}

    assert "Career Guide" not in titles
    assert "Senior Application Developer" in titles


def test_linked_cards_keep_compact_structured_roles():
    extractor = TieredExtractorV90()
    html = _read("gap_5_careers_sapowernetworks_com_au.html")

    jobs = extractor._extract_linked_job_cards_v67(
        html,
        "https://careers.sapowernetworks.com.au/jobs/search",
    )
    titles = {j.get("title") for j in jobs}

    assert "Powerline Workers" in titles
    assert "Storeperson" in titles
