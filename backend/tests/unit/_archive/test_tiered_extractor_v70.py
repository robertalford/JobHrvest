from app.crawlers.tiered_extractor_v70 import TieredExtractorV70


def test_greenhouse_card_title_uses_primary_text_not_location_tail():
    html = """
    <div class="job-posts--table">
      <table><tbody>
        <tr class="job-post"><td class="cell">
          <a href="https://job-boards.greenhouse.io/juvare/jobs/4668287005" target="_top">
            <p class="body body--medium">DevOps Engineer</p>
            <p class="body body__secondary body--metadata">Vijayawada</p>
          </a>
        </td></tr>
        <tr class="job-post"><td class="cell">
          <a href="https://job-boards.greenhouse.io/juvare/jobs/4668293005" target="_top">
            <p class="body body--medium">Pre-Sales Engineer</p>
            <p class="body body__secondary body--metadata">Bangalore</p>
          </a>
        </td></tr>
        <tr class="job-post"><td class="cell">
          <a href="https://job-boards.greenhouse.io/juvare/jobs/4671844005" target="_top">
            <p class="body body--medium">Product Owner</p>
            <p class="body body__secondary body--metadata">Vijayawada</p>
          </a>
        </td></tr>
      </tbody></table>
    </div>
    """

    extractor = TieredExtractorV70()
    jobs = extractor._extract_linked_job_cards_v67(
        html,
        "https://job-boards.greenhouse.io/embed/job_board?for=juvare",
    )

    assert len(jobs) == 3
    titles = {j["title"] for j in jobs}
    assert "DevOps Engineer" in titles
    assert "Pre-Sales Engineer" in titles
    assert "Product Owner" in titles
    assert all("Vijayawada" not in j["title"] for j in jobs)


def test_linked_card_accepts_short_role_title_with_strong_detail_slug():
    html = """
    <div class="pt-cv-content-item">
      <h4 class="pt-cv-title">
        <a href="https://salvationarmy.org.hk/career/%e5%94%ae%e8%b2%a8%e5%93%a1-2/?lang=en">Sales</a>
      </h4>
    </div>
    """

    extractor = TieredExtractorV70()
    jobs = extractor._extract_linked_job_cards_v67(
        html,
        "https://salvationarmy.org.hk/join-us/job-vacancies/?lang=en",
    )

    assert len(jobs) == 1
    assert jobs[0]["title"] == "Sales"


def test_linked_card_accepts_uppercase_skill_title_on_job_detail_paths():
    html = """
    <div class="col-sm-4 career-list">
      <a href="https://digimonk.in/job-detail/mernstack/" id="career-search-form-1">
        <span class="sub-title">MERNSTACK </span>
        <span class="job-location">Gwalior/Noida</span>
      </a>
    </div>
    """

    extractor = TieredExtractorV70()
    jobs = extractor._extract_linked_job_cards_v67(
        html,
        "https://digimonk.in/career/",
    )

    assert len(jobs) == 1
    assert jobs[0]["title"] == "MERNSTACK"


def test_linked_card_rejects_show_more_pagination_as_job():
    html = """
    <ul>
      <li class="w-full">
        <a href="https://hohepacanterbury.teamtailor.com/jobs/7518784-leap-support-coordinator">LEAP Support Coordinator</a>
      </li>
    </ul>
    <div id="show_more_button">
      <a href="/jobs/show_more?page=2">Show 5 more</a>
    </div>
    """

    extractor = TieredExtractorV70()
    jobs = extractor._extract_linked_job_cards_v67(
        html,
        "https://hohepacanterbury.teamtailor.com/jobs",
    )

    assert len(jobs) == 1
    assert jobs[0]["title"] == "LEAP Support Coordinator"


def test_pagination_includes_show_more_links():
    html = """
    <div id="show_more_button" class="flex justify-center mx-auto mt-12">
      <a href="/jobs/show_more?page=2" data-turbo-stream="true">Show 5 more</a>
    </div>
    """

    extractor = TieredExtractorV70()
    urls = extractor._pagination_urls_v67(html, "https://hohepacanterbury.teamtailor.com/jobs")

    assert urls == ["https://hohepacanterbury.teamtailor.com/jobs/show_more?page=2"]


def test_title_validator_rejects_non_job_headings_and_pagination_labels():
    extractor = TieredExtractorV70()

    assert extractor._is_valid_title_v60("Working with us") is False
    assert extractor._is_valid_title_v60("Show 5 more") is False
    assert extractor._is_valid_title_v60("DevOps Engineer") is True
