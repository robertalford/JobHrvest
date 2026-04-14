from app.crawlers.tiered_extractor_v79 import TieredExtractorV79


def test_title_validator_rejects_date_and_job_index_labels():
    extractor = TieredExtractorV79()

    assert extractor._is_valid_title_v60("Apr 7, 2026") is False
    assert extractor._is_valid_title_v60("03 April 2026") is False
    assert extractor._is_valid_title_v60("Job Index") is False
    assert extractor._is_valid_title_v60("Project Control Engineer") is True


def test_non_job_url_rejects_listing_filter_queries_but_keeps_detail_ids():
    extractor = TieredExtractorV79()

    assert extractor._is_non_job_url("https://www.jobthai.com/en/jobs?jobtype=11") is True
    assert extractor._is_non_job_url("https://www.jobthai.com/en/jobs?district=4901") is True
    assert extractor._is_non_job_url("https://example.com/jobs?jobid=123") is False
    assert extractor._is_non_job_url("https://example.com/jobs?ajid=ABC123") is False


def test_linked_cards_skip_date_title_and_pick_real_role_for_same_url():
    extractor = TieredExtractorV79()
    html = """
    <div>
      <a href="https://www.jobthai.com/en/job/1825979"><span class="title">Apr 7, 2026</span></a>
      <a href="https://www.jobthai.com/en/job/1825979"><h3 class="title">Project Control Engineer</h3></a>
      <a href="https://www.jobthai.com/en/jobs?jobtype=11"><h3 class="title">Technician</h3></a>
    </div>
    """

    jobs = extractor._extract_linked_job_cards_v67(html, "https://www.jobthai.com/en/company/98976")

    assert len(jobs) == 1
    assert jobs[0]["title"] == "Project Control Engineer"
    assert jobs[0]["source_url"].endswith("/en/job/1825979")


def test_numeric_job_path_fallback_recovers_titles_with_weak_role_vocab():
    extractor = TieredExtractorV79()
    html = """
    <table>
      <tr><td><a href="https://gunamandiri.com/jobs/777/ink-mixer.html">Ink Mixer</a></td></tr>
      <tr><td><a href="https://gunamandiri.com/jobs/1367/third-enginner.html">Third Enginner</a></td></tr>
      <tr><td><a href="https://gunamandiri.com/jobs/429/mechanical-supervisors.html">Mechanical Supervisors</a></td></tr>
    </table>
    """

    jobs = extractor._extract_linked_job_cards_v67(html, "https://gunamandiri.com/vacancies/")
    titles = {j["title"] for j in jobs}

    assert "Ink Mixer" in titles
    assert "Third Enginner" in titles
    assert "Mechanical Supervisors" in titles
