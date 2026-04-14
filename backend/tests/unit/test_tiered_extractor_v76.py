from app.crawlers.tiered_extractor_v76 import TieredExtractorV76


def test_pageup_split_row_link_association_recovers_detail_urls():
    html = """
    <section id="recent-jobs-content">
      <div class="list-item row">
        <div class="column-3-4 list-item-left">
          <h3 class="list-title">Project Manager</h3>
          <p class="list-content">Drive project outcomes.</p>
        </div>
        <div class="column-1-4 list-item-right">
          <a href="/915/cw/en/job/496266/project-manager">View role</a>
          <span class="location">Brisbane</span>
        </div>
      </div>
      <div class="list-item row">
        <div class="column-3-4 list-item-left">
          <h3 class="list-title">Project Engineer - Mechanical</h3>
          <p class="list-content">Deliver plant upgrades.</p>
        </div>
        <div class="column-1-4 list-item-right">
          <a href="/915/cw/en/job/496267/project-engineer-mechanical">View role</a>
          <span class="location">Townsville</span>
        </div>
      </div>
    </section>
    """
    extractor = TieredExtractorV76()
    jobs = extractor._extract_pageup_listing_jobs_v74(
        html,
        "https://careers.pageuppeople.com/915/cw/en/listing",
    )

    assert len(jobs) == 2
    assert jobs[0]["source_url"].endswith("/915/cw/en/job/496266/project-manager")
    assert jobs[1]["source_url"].endswith("/915/cw/en/job/496267/project-engineer-mechanical")
    assert jobs[0]["location_raw"] == "Brisbane"


def test_teamtailor_rows_accept_multilingual_short_titles_on_strong_urls():
    html = """
    <div class="jobs-list-container">
      <ul id="jobs_list_container">
        <li><a href="/jobs/7492197-volaagent">Võlaagent</a></li>
        <li><a href="/jobs/7346956-compliance-leader">Compliance Leader</a></li>
        <li><a href="/jobs/7300451-klienditeeninduse-konsultant">Klienditeeninduse konsultant</a></li>
        <li><a href="/jobs/6967136-crm-expert">CRM Expert</a></li>
      </ul>
    </div>
    """
    extractor = TieredExtractorV76()
    jobs = extractor._extract_teamtailor_rows_v76(html, "https://ipfdigital.teamtailor.com/jobs")

    assert len(jobs) == 4
    assert [j["title"] for j in jobs] == [
        "Võlaagent",
        "Compliance Leader",
        "Klienditeeninduse konsultant",
        "CRM Expert",
    ]


def test_query_id_cards_extract_bootstrap_listing_rows():
    html = """
    <div class="row">
      <div class="col-lg-4 mb-4">
        <a href="/career?id=282"><h3>Staf Pelayanan</h3></a>
      </div>
      <div class="col-lg-4 mb-4">
        <a href="/career?id=276"><h3>Apoteker Pendamping</h3></a>
      </div>
      <div class="col-lg-4 mb-4">
        <a href="/career?id=283"><h3>Staf Accounting</h3></a>
      </div>
    </div>
    """
    extractor = TieredExtractorV76()
    jobs = extractor._extract_query_id_cards_v76(html, "https://simap.afgindo.com/career")

    assert len(jobs) == 3
    assert jobs[0]["source_url"].endswith("/career?id=282")
    assert jobs[1]["source_url"].endswith("/career?id=276")
    assert jobs[2]["source_url"].endswith("/career?id=283")


def test_connx_rows_support_anchor_row_markup():
    html = """
    <div class="GridTable GridTable--rows">
      <a class="GridTable__row" href="/job/details/communications-and-engagement-advisor">
        <div class="name">Communications and Engagement Advisor</div>
        <div class="location">Traralgon</div>
      </a>
    </div>
    """
    extractor = TieredExtractorV76()
    jobs = extractor._extract_connx_grid_jobs_v75(html, "https://gippswater.connxcareers.com/")

    assert len(jobs) == 1
    assert jobs[0]["title"] == "Communications and Engagement Advisor"
    assert jobs[0]["source_url"].endswith("/job/details/communications-and-engagement-advisor")
    assert jobs[0]["location_raw"] == "Traralgon"


def test_description_cleaner_removes_skip_link_boilerplate():
    extractor = TieredExtractorV76()
    dirty = (
        "Skip to primary navigation Skip to main content "
        "Back to all positions Senior Data Engineer role details"
    )

    cleaned = extractor._clean_description_v73(dirty)
    assert cleaned is not None
    assert "Skip to primary navigation" not in cleaned
    assert "Skip to main content" not in cleaned
    assert "Back to all positions" not in cleaned
    assert "Senior Data Engineer role details" in cleaned


def test_connx_shell_detector_matches_empty_app_shell():
    html = """
    <html><body>
      <div id="app"></div>
      <script src="/assets/js/index-ecfb757d.js"></script>
    </body></html>
    """
    assert TieredExtractorV76._is_connx_app_shell_v76(
        "https://gippswater.connxcareers.com/",
        html,
    )

