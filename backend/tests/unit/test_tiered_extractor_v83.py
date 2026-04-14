import asyncio

from app.crawlers.tiered_extractor_v81 import TieredExtractorV81
from app.crawlers.tiered_extractor_v83 import TieredExtractorV83


def test_extract_prefers_parent_jobs_over_heading_fallback(monkeypatch):
    extractor = TieredExtractorV83()

    async def fake_parent_extract(self, career_page, company, html):
        return [
            {
                "title": "Software Engineer",
                "source_url": "https://example.com/jobs/1",
                "location_raw": "Sydney",
                "description": "Build backend systems",
                "salary_raw": None,
                "employment_type": None,
                "extraction_method": "tier2_heuristic_v16",
                "extraction_confidence": 0.9,
            },
            {
                "title": "Data Engineer",
                "source_url": "https://example.com/jobs/2",
                "location_raw": "Sydney",
                "description": "Build data pipelines",
                "salary_raw": None,
                "employment_type": None,
                "extraction_method": "tier2_heuristic_v16",
                "extraction_confidence": 0.9,
            },
            {
                "title": "QA Engineer",
                "source_url": "https://example.com/jobs/3",
                "location_raw": "Melbourne",
                "description": "Ensure release quality",
                "salary_raw": None,
                "employment_type": None,
                "extraction_method": "tier2_heuristic_v16",
                "extraction_confidence": 0.9,
            },
            {
                "title": "DevOps Engineer",
                "source_url": "https://example.com/jobs/4",
                "location_raw": "Melbourne",
                "description": "Maintain infrastructure",
                "salary_raw": None,
                "employment_type": None,
                "extraction_method": "tier2_heuristic_v16",
                "extraction_confidence": 0.9,
            },
        ]

    monkeypatch.setattr(TieredExtractorV81, "extract", fake_parent_extract)
    monkeypatch.setattr(
        extractor,
        "_extract_heading_action_rows_v82",
        lambda *_: [
            {"title": "New Opportunities", "source_url": "https://example.com/careers"},
            {"title": "Browse Jobs", "source_url": "https://example.com/careers"},
            {"title": "Search Jobs", "source_url": "https://example.com/careers"},
        ],
    )

    class Page:
        url = "https://example.com/careers"
        requires_js_rendering = False

    class Company:
        name = "Example"
        ats_platform = None

    jobs = asyncio.run(extractor.extract(Page(), Company(), "<html><body></body></html>"))

    assert len(jobs) == 4
    assert jobs[0]["title"] == "Software Engineer"


def test_reject_generic_heading_titles_v83():
    extractor = TieredExtractorV83()

    assert not extractor._is_valid_title_v60("New Opportunities")
    assert not extractor._is_valid_title_v60("Explore Current Job Openings")
    assert not extractor._is_valid_title_v60("Don't see your dream role?")
    assert extractor._is_valid_title_v60("Customer Success Manager")


def test_extract_greenhouse_embed_rows_v83():
    extractor = TieredExtractorV83()
    html = """
    <main>
      <div class="job-posts--table">
        <table><tbody>
          <tr class="job-post"><td><a href="https://company.example/jobs/1"><p class="body body--medium">Customer Success Manager</p><p class="body body__secondary body--metadata">Sydney</p></a></td></tr>
          <tr class="job-post"><td><a href="https://company.example/jobs/2"><p class="body body--medium">Customer Success Manager (French)</p><p class="body body__secondary body--metadata">Sofia</p></a></td></tr>
          <tr class="job-post"><td><a href="https://company.example/jobs/3"><p class="body body--medium">Upmarket Customer Success Manager</p><p class="body body__secondary body--metadata">New York</p></a></td></tr>
        </tbody></table>
      </div>
    </main>
    """

    jobs = extractor._extract_greenhouse_embed_rows_v83(
        html,
        "https://job-boards.greenhouse.io/embed/job_board?for=example",
    )

    assert len(jobs) == 3
    assert jobs[0]["title"] == "Customer Success Manager"
    assert jobs[0]["location_raw"] == "Sydney"


def test_extract_elementor_accordion_rows_v83():
    extractor = TieredExtractorV83()
    html = """
    <div class="elementor-accordion-item">
      <h4 id="elementor-tab-title-1" class="elementor-tab-title"><a class="elementor-accordion-title" href="">Year 2 Teacher (Maternity Cover)</a></h4>
      <div id="elementor-tab-content-1" class="elementor-tab-content">Full-time contract role.</div>
    </div>
    <div class="elementor-accordion-item">
      <h4 id="elementor-tab-title-2" class="elementor-tab-title"><a class="elementor-accordion-title" href="">Year 5 Teacher (Maternity Cover)</a></h4>
      <div id="elementor-tab-content-2" class="elementor-tab-content">Primary classroom role.</div>
    </div>
    <div class="elementor-accordion-item">
      <h4 id="elementor-tab-title-3" class="elementor-tab-title"><a class="elementor-accordion-title" href="">Primary Inclusive Education Coordinator</a></h4>
      <div id="elementor-tab-content-3" class="elementor-tab-content">Coordinator role.</div>
    </div>
    <div class="elementor-accordion-item">
      <h4 id="elementor-tab-title-4" class="elementor-tab-title"><a class="elementor-accordion-title" href="">Relief Teacher</a></h4>
      <div id="elementor-tab-content-4" class="elementor-tab-content">Relief opportunities.</div>
    </div>
    """

    jobs = extractor._extract_elementor_accordion_rows_v83(html, "https://school.example/jobs")

    assert len(jobs) == 4
    assert extractor._passes_accordion_jobset_v83(jobs)
    assert all("#elementor-tab-title-" in j["source_url"] for j in jobs)


def test_collect_pagination_urls_v83_filters_locale_noise():
    extractor = TieredExtractorV83()
    html = """
    <ul class="pagination">
      <li><a href="?q=&startrow=25">2</a></li>
      <li><a href="?q=&startrow=50">3</a></li>
      <li><a href="?q=&locale=fr_FR">French</a></li>
    </ul>
    """

    urls = extractor._collect_pagination_urls_v83(
        html,
        "https://jobs.example/search/?q=&locationsearch=",
    )

    assert len(urls) == 2
    assert all("startrow=" in u for u in urls)


def test_jobs2web_endpoint_candidates_include_search_result_view_variants():
    extractor = TieredExtractorV83()
    cfg = {
        "company_id": "ACME",
        "locale": "en_US",
        "api_url": "https://api2.successfactors.eu",
        "csrf": None,
        "referrer": "rmk-map-2.jobs2web.com",
    }

    endpoints = extractor._jobs2web_endpoint_candidates_v66(
        "https://careers.example.com/search?searchResultView=LIST&pageNumber=0&facetFilters=%7B%7D&sortBy=date",
        cfg,
    )

    assert endpoints
    assert "/search/" in endpoints[0]
    assert any("searchresultview=list" in e.lower() for e in endpoints)
