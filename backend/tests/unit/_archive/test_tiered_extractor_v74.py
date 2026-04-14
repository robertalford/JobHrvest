from app.crawlers.tiered_extractor_v74 import TieredExtractorV74


def test_extract_recruitee_cards_supports_o_path_and_dotnet_titles():
    html = """
    <section>
      <h2>Open positions</h2>
      <div data-testid="offer-list-cards-desktop-display">
        <div class="sc-uzptka-1">
          <a href="/o/net-developer" class="sc-6exb5d-1">.NET Developer</a>
          <li data-testid="styled-location-list-item">Culemborg, Netherlands</li>
        </div>
        <div class="sc-uzptka-1">
          <a href="/o/internship-implementation-of-the-assai-intranet-microsoft-sharepoint">
            Internship: Implementation of the Assai Intranet (Microsoft SharePoint)
          </a>
        </div>
      </div>
    </section>
    """
    extractor = TieredExtractorV74()
    jobs = extractor._extract_recruitee_jobs_v74(html, "https://assaisoftware.recruitee.com/")

    assert len(jobs) == 2
    assert jobs[0]["title"] == ".NET Developer"
    assert jobs[0]["source_url"].endswith("/o/net-developer")
    assert jobs[0]["location_raw"] == "Culemborg, Netherlands"


def test_extract_pageup_listing_rows_and_pagination_links():
    html = """
    <section id="recent-jobs-content">
      <div class="list-item">
        <a href="/915/cw/en/job/496266/project-manager">
          <h3 class="list-title">Project Manager</h3>
        </a>
        <span class="location">Brisbane</span>
      </div>
      <div class="list-item">
        <a href="/915/cw/en/job/496267/site-engineer">
          <h3 class="list-title">Site Engineer</h3>
        </a>
        <span class="location">Townsville</span>
      </div>
      <a class="more-link" href="/915/cw/en/listing/?page=2&page-items=20">More jobs 29</a>
    </section>
    """
    extractor = TieredExtractorV74()
    jobs = extractor._extract_pageup_listing_jobs_v74(
        html,
        "https://careers.pageuppeople.com/915/cw/en/listing",
    )
    pages = extractor._pageup_pagination_urls_v74(
        html,
        "https://careers.pageuppeople.com/915/cw/en/listing",
    )

    assert len(jobs) == 2
    assert [j["title"] for j in jobs] == ["Project Manager", "Site Engineer"]
    assert jobs[0]["location_raw"] == "Brisbane"
    assert pages == ["https://careers.pageuppeople.com/915/cw/en/listing/?page=2&page-items=20"]


def test_extract_jobs_json_items_creates_unique_job_urls():
    extractor = TieredExtractorV74()
    items = [
        {
            "title": "3D Animator",
            "location": "Singapore",
            "description": "<p>Create character animation.</p>",
        },
        {
            "title": "Concept Artist",
            "location": "Kuala Lumpur, Malaysia",
            "description": "Design and paint concepts.",
        },
    ]

    jobs = extractor._extract_jobs_json_items_v74(items, "https://www.omens-studios.com/careers/")
    assert len(jobs) == 2
    assert jobs[0]["title"] == "3D Animator"
    assert jobs[0]["source_url"].startswith("https://www.omens-studios.com/careers/?job=")
    assert jobs[0]["location_raw"] == "Singapore"
