from app.crawlers.tiered_extractor_v75 import TieredExtractorV75


def test_linked_card_preserves_specialized_hyphen_titles():
    html = """
    <section>
      <a href="/jobs/legal-secretary-property-infrastructure/">
        <h3>Legal Secretary - Property &amp; Infrastructure</h3>
      </a>
    </section>
    """
    extractor = TieredExtractorV75()
    jobs = extractor._extract_linked_job_cards_v67(html, "https://egconsulting.com.au/jobs/")

    assert len(jobs) == 1
    assert jobs[0]["title"] == "Legal Secretary - Property & Infrastructure"


def test_pageup_accepts_lead_role_titles():
    html = """
    <section id="recent-jobs-content">
      <div class="list-item row">
        <a href="/915/cw/en/job/496472/proposals-lead">
          <h3 class="list-title">Proposals Lead</h3>
        </a>
        <span class="location">Brisbane</span>
      </div>
      <div class="list-item row">
        <a href="/915/cw/en/job/496451/leading-hand">
          <h3 class="list-title">Leading Hand</h3>
        </a>
        <span class="location">Townsville</span>
      </div>
    </section>
    """
    extractor = TieredExtractorV75()
    jobs = extractor._extract_pageup_listing_jobs_v74(
        html,
        "https://careers.pageuppeople.com/915/cw/en/listing",
    )

    assert [j["title"] for j in jobs] == ["Proposals Lead", "Leading Hand"]


def test_connx_grid_extractor_keeps_unique_detail_urls():
    html = """
    <div class="GridTable GridTable--rows">
      <div class="GridTable__row">
        <div class="name">Communications and Engagement Advisor</div>
        <a href="/job/details/communications-and-engagement-advisor">View</a>
        <div class="location">Traralgon</div>
        <div class="employmentType">Permanent</div>
      </div>
      <div class="GridTable__row">
        <div class="name">Functional Leads (Oracle)</div>
        <a href="/job/details/functional-leads-oracle">View</a>
        <div class="location">Melbourne</div>
        <div class="employmentType">Contract</div>
      </div>
    </div>
    """
    extractor = TieredExtractorV75()
    jobs = extractor._extract_connx_grid_jobs_v75(html, "https://gippswater.connxcareers.com/")

    assert len(jobs) == 2
    assert jobs[0]["source_url"].endswith("/job/details/communications-and-engagement-advisor")
    assert jobs[0]["location_raw"] == "Traralgon"
    assert jobs[0]["employment_type"] == "Permanent"


def test_large_pageup_set_with_noisy_descriptions_still_enriches():
    extractor = TieredExtractorV75()
    jobs = [
        {
            "title": f"Role {i}",
            "source_url": f"https://careers.pageuppeople.com/915/cw/en/job/{1000+i}/role-{i}",
            "location_raw": None,
            "description": "Sort By Department Location DepartmentsAll (13) LocationsAll (13)",
            "salary_raw": None,
            "employment_type": None,
            "extraction_method": "ats_pageup_listing_v74",
        }
        for i in range(30)
    ]

    assert extractor._should_enrich_fast_path_v73(
        jobs,
        "https://careers.pageuppeople.com/915/cw/en/listing",
    )


def test_clean_description_drops_filter_noise():
    extractor = TieredExtractorV75()
    dirty = "Sort By Department Location DepartmentsAll (13) LocationsAll (13)"

    assert extractor._clean_description_v73(dirty) is None
