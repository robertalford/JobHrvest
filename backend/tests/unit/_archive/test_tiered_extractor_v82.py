from app.crawlers.tiered_extractor_v82 import TieredExtractorV82


def test_heading_action_rows_extract_mav3rik_style_cards():
    extractor = TieredExtractorV82()
    html = """
    <div class="careers-common-tab-wrapper">
      <h4 class="job-position">Senior Salesforce Developer</h4>
      <a href="/mav3rik-career/senior-salesforce-developer/">View Job</a>
    </div>
    <div class="careers-common-tab-wrapper">
      <h4 class="job-position">Salesforce Technical Architect</h4>
      <a href="/mav3rik-career/salesforce-technical-architect/">View Job</a>
    </div>
    <div class="careers-common-tab-wrapper">
      <h4 class="job-position">Senior Business Development Manager</h4>
      <a href="/mav3rik-career/senior-business-development-manager/">View Job</a>
    </div>
    """

    page_url = "https://mav3rik.com/careers/"
    jobs = extractor._extract_heading_action_rows_v82(html, page_url)

    assert len(jobs) == 3
    assert all("/mav3rik-career/" in j["source_url"] for j in jobs)
    assert extractor._passes_jobset_validation(jobs, page_url)


def test_heading_action_rows_allow_mailto_application_cards():
    extractor = TieredExtractorV82()
    html = """
    <div class="elementor-element">
      <h4>Front-end developer, Hong Kong</h4>
      <a href="mailto:career@example.com?subject=Application%20for%20front-end%20developer"></a>
    </div>
    <div class="elementor-element">
      <h4>Back-end developer, Hong Kong</h4>
      <a href="mailto:career@example.com?subject=Application%20for%20back-end%20developer"></a>
    </div>
    <div class="elementor-element">
      <h4>Marketing Assistant, Hong Kong</h4>
      <a href="mailto:career@example.com?subject=Application%20for%20marketing%20assistant"></a>
    </div>
    """

    jobs = extractor._extract_heading_action_rows_v82(html, "https://example.com/about/career/")

    assert len(jobs) == 3
    assert all(j["source_url"].startswith("mailto:") for j in jobs)
    assert extractor._passes_mailto_heading_jobset_v82(jobs)


def test_reject_region_openings_title():
    extractor = TieredExtractorV82()

    assert not extractor._is_valid_title_v60("Job Openings in North America")
    assert not extractor._is_valid_title_v60("All Job Openings")
    assert extractor._is_valid_title_v60("Territory Manager")


def test_parent_superset_recovery_prefers_larger_valid_set(monkeypatch):
    extractor = TieredExtractorV82()
    page_url = "https://jobs.example.com/careers"
    html = "<html><body>stub</body></html>"

    parent_jobs = [
        {"title": "Software Engineer", "source_url": "https://jobs.example.com/jobs/1"},
        {"title": "Data Engineer", "source_url": "https://jobs.example.com/jobs/2"},
        {"title": "QA Engineer", "source_url": "https://jobs.example.com/jobs/3"},
        {"title": "Product Manager", "source_url": "https://jobs.example.com/jobs/4"},
        {"title": "DevOps Engineer", "source_url": "https://jobs.example.com/jobs/5"},
    ]
    current_jobs = parent_jobs[:2]

    monkeypatch.setattr(extractor, "_extract_tier2_v16", lambda *_: parent_jobs)
    monkeypatch.setattr(extractor, "_passes_jobset_validation", lambda *_: True)

    recovered = extractor._recover_parent_superset_v82(html, page_url, current_jobs)
    assert recovered is not None
    assert len(recovered) == 5
