from app.crawlers.tiered_extractor_v68 import TieredExtractorV68


def test_jobvite_table_extraction_skips_seeker_tools_links():
    html = """
    <html><body>
      <nav class="jv-seeker-tools">
        <a href="/acme/jobAlerts">Sign up for Job Alerts.</a>
        <a href="/acme/apply">Submit a general application</a>
      </nav>
      <table class="jv-job-list">
        <tbody>
          <tr>
            <td class="jv-job-list-name"><a href="/acme/job/abc123" title="Accountant">Accountant</a></td>
            <td class="jv-job-list-location">Sydney, Australia</td>
          </tr>
          <tr>
            <td class="jv-job-list-name"><a href="/acme/job/def456" title="Acheteur">Acheteur</a></td>
            <td class="jv-job-list-location">Paris, France</td>
          </tr>
        </tbody>
      </table>
    </body></html>
    """

    extractor = TieredExtractorV68()
    jobs = extractor._extract_jobvite_jobs_v68(html, "https://jobs.jobvite.com/acme/jobs")

    assert len(jobs) == 2
    titles = {j["title"] for j in jobs}
    assert "Accountant" in titles
    assert "Acheteur" in titles
    assert all("jobalerts" not in j["source_url"].lower() for j in jobs)
    assert all("general application" not in j["title"].lower() for j in jobs)


def test_pagination_detection_includes_div_pagination_links():
    html = """
    <html><body>
      <div class="pagination clearfix">
        <a href="/hgv-driver-jobs/page/2/?et_blog">Older Entries</a>
      </div>
    </body></html>
    """

    extractor = TieredExtractorV68()
    urls = extractor._pagination_urls_v67(html, "https://tomorange.co.uk/hgv-driver-jobs/")

    assert urls == ["https://tomorange.co.uk/hgv-driver-jobs/page/2/?et_blog"]


def test_jobs2web_config_parses_sso_fields_and_candidate_endpoints():
    html = """
    <script>
      var appParams = { locale: 'en_GB' };
      var resultStyles = { companyId: 'ptsayapmas', apiURL: 'https://api44.sapsf.com', currentLocale: 'en_GB' };
      j2w.init({
        "ssoCompanyId"   : 'ptsayapmas',
        "ssoUrl"         : 'https://career44.sapsf.com'
      });
    </script>
    """

    extractor = TieredExtractorV68()
    cfg = extractor._extract_jobs2web_config_v66(html)
    endpoints = extractor._jobs2web_endpoint_candidates_v66(
        "https://www.wingscareer.com/search/?q=&skillsSearch=false",
        cfg,
    )

    assert cfg["company_id"] == "ptsayapmas"
    assert cfg["sso_company_id"] == "ptsayapmas"
    assert cfg["sso_url"] == "https://career44.sapsf.com"
    assert any("career44.sapsf.com/career?company=ptsayapmas" in ep for ep in endpoints)
    assert any("/search/?q=&skillsSearch=false" in ep for ep in endpoints)


def test_title_validator_rejects_known_non_job_cta_titles():
    extractor = TieredExtractorV68()

    assert extractor._is_valid_title_v60("Sign up for Job Alerts.") is False
    assert extractor._is_valid_title_v60("Submit a general application") is False
