from app.crawlers.tiered_extractor_v69 import TieredExtractorV69


def test_jobs2web_endpoint_order_prioritizes_same_host_search_urls():
    html = """
    <script>
      var resultStyles = {
        companyId: 'ptsayapmas',
        apiURL: 'https://api44.sapsf.com',
        currentLocale: 'en_GB'
      };
      var appParams = { referrer: "rmk-map-44.jobs2web.com" };
      j2w.init({
        "ssoCompanyId": "ptsayapmas",
        "ssoUrl": "https://career44.sapsf.com"
      });
    </script>
    """

    extractor = TieredExtractorV69()
    cfg = extractor._extract_jobs2web_config_v66(html)
    endpoints = extractor._jobs2web_endpoint_candidates_v66(
        "https://www.wingscareer.com/search/?q=&skillsSearch=false",
        cfg,
    )

    assert len(endpoints) >= 6
    top6 = endpoints[:6]
    assert any("www.wingscareer.com/search/?q=&skillsSearch=false" in ep for ep in top6)
    assert any("api44.sapsf.com/career/jobsearch" in ep for ep in top6)


def test_title_validator_rejects_generic_job_vacancies_heading():
    extractor = TieredExtractorV69()

    assert extractor._is_valid_title_v60("Job Vacancies") is False
    assert extractor._is_valid_title_v60("Current Vacancies") is False
    assert extractor._is_valid_title_v60("Sales Supervisor") is True
