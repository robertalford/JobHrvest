import asyncio

from app.crawlers.tiered_extractor_v84 import TieredExtractorV84
from app.crawlers.tiered_extractor_v85 import TieredExtractorV85


def test_linked_role_rows_v85_keep_multi_location_duplicates():
    extractor = TieredExtractorV85()
    html = """
    <div class="careers-listing">
      <div class="role"><a href="/career/barista">Barista - Full Time</a><div class="city">Quezon City, Metro Manila</div></div>
      <div class="role"><a href="/career/barista">Barista - Full Time</a><div class="city">Mandaluyong City, Metro Manila</div></div>
      <div class="role"><a href="/career/assistant-cafe-leader">Assistant Cafe Lead</a><div class="city">Quezon City, Metro Manila</div></div>
      <div class="role"><a href="/career/pastry-chef">Pastry Chef</a><div class="city">Metro Manila</div></div>
    </div>
    """

    jobs = extractor._extract_linked_job_cards_v67(html, "https://www.everydaycoffee.ph/careers")

    barista_jobs = [j for j in jobs if j["title"] == "Barista - Full Time"]
    locations = {j.get("location_raw") for j in barista_jobs}

    assert len(barista_jobs) >= 2
    assert "Quezon City, Metro Manila" in locations
    assert "Mandaluyong City, Metro Manila" in locations


def test_extract_open_state_rows_v85():
    extractor = TieredExtractorV85()
    html = """
    <section id="current-positions">
      <div data-state="open">
        <h4><button>Marketing Campaign Executive</button></h4>
        <p>Location: Collingwood, Victoria</p>
        <a href="/about-us/careers/marketing-campaign-executive-2025/">Find out more</a>
      </div>
    </section>
    """

    jobs = extractor._extract_open_state_rows_v85(html, "https://www.id.com.au/careers/")

    assert len(jobs) == 1
    assert jobs[0]["title"] == "Marketing Campaign Executive"
    assert jobs[0]["location_raw"] == "Collingwood, Victoria"
    assert jobs[0]["source_url"].endswith("/about-us/careers/marketing-campaign-executive-2025/")


def test_jobs2web_candidates_v85_include_sso_host():
    extractor = TieredExtractorV85()
    cfg = {
        "company_id": "Bausch",
        "locale": "en_US",
        "api_url": "https://api2.successfactors.eu",
        "csrf": "token",
        "referrer": "rmk-map-2.jobs2web.com",
        "sso_url": "https://career2.successfactors.eu",
    }
    page_url = (
        "https://careers.bauschhealth.com/search?searchResultView=LIST&pageNumber=0"
        "&markerViewed=&carouselIndex=&facetFilters=%7B%7D&sortBy=date"
    )

    endpoints = extractor._jobs2web_endpoint_candidates_v66(page_url, cfg)

    assert endpoints
    assert "careers.bauschhealth.com/search" in endpoints[0].lower()
    assert any("career2.successfactors.eu/career" in e.lower() for e in endpoints)


def test_extract_dense_role_rows_v85():
    extractor = TieredExtractorV85()
    html = """
    <div class="careers-listing">
      <div class="role"><a href="/career/barista">Barista - Full Time</a><div class="city">Quezon City, Metro Manila</div></div>
      <div class="role"><a href="/career/barista">Barista - Full Time</a><div class="city">Mandaluyong City, Metro Manila</div></div>
      <div class="role"><a href="/career/barista">Barista - Full Time</a><div class="city">Santa Rosa, Laguna</div></div>
      <div class="role"><a href="/career/assistant-cafe-leader">Assistant Cafe Lead</a><div class="city">Quezon City, Metro Manila</div></div>
      <div class="role"><a href="/career/pastry-chef">Pastry Chef</a><div class="city">Metro Manila / Laguna / Batangas</div></div>
      <div class="role"><a href="/career/line-cook">Line Cook</a><div class="city">Batangas</div></div>
    </div>
    """

    class Page:
        url = "https://www.everydaycoffee.ph/careers"
        requires_js_rendering = False

    class Company:
        name = "Everyday Coffee"
        ats_platform = None

    jobs = asyncio.run(extractor.extract(Page(), Company(), html))
    assert len(jobs) >= 6
    assert len({j.get("location_raw") for j in jobs if j.get("location_raw")}) >= 4


def test_extract_drops_single_generic_listing_title_v85(monkeypatch):
    extractor = TieredExtractorV85()

    async def fake_parent_extract(self, career_page, company, html):
        return [
            {
                "title": "CareersThe Latest Job Opportunities",
                "source_url": "https://example.com/jobs",
                "location_raw": "Careers",
                "description": "Careers page",
                "salary_raw": None,
                "employment_type": None,
                "extraction_method": "tier2_links",
                "extraction_confidence": 0.7,
            }
        ]

    monkeypatch.setattr(TieredExtractorV84, "extract", fake_parent_extract)

    class Page:
        url = "https://example.com/jobs"
        requires_js_rendering = False

    class Company:
        name = "Example"
        ats_platform = None

    jobs = asyncio.run(extractor.extract(Page(), Company(), "<html></html>"))
    assert jobs == []
