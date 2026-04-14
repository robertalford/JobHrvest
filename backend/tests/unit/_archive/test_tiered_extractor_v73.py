from app.crawlers.tiered_extractor_v73 import TieredExtractorV73
from app.crawlers.tiered_extractor import _parse_html


def test_linked_cards_filter_nav_and_company_root_links():
    html = """
    <div>
      <a href="/en/wishlist"><span>Saved jobs (0)</span></a>
      <a href="/en/jobs"><span>Find Your Dream Job</span></a>
      <a href="https://careers.middleton.edu.sg"><span>Middleton International School</span></a>
      <a href="/en/jobs/senior-data-engineer"><h3>Senior Data Engineer</h3></a>
    </div>
    """
    extractor = TieredExtractorV73()
    jobs = extractor._extract_linked_job_cards_v67(html, "https://careers.quadient.com/en/jobs")

    assert [j["title"] for j in jobs] == ["Senior Data Engineer"]
    assert jobs[0]["source_url"].endswith("/en/jobs/senior-data-engineer")


def test_extract_nuxt_job_rows_reads_titles_urls_and_locations():
    html = """
    <ul class="job-postings-list">
      <li>
        <div class="job-row">
          <a href="/en/jobs/senior-data-engineer" class="text-left">
            <div class="heading">
              <div class="created"><span>2026-04-03 - </span><span>Remote, </span></div>
              <h2>Senior Data Engineer</h2>
            </div>
          </a>
        </div>
      </li>
      <li>
        <div class="job-row">
          <a href="/en/jobs/qa-analyst" class="text-left">
            <div class="heading">
              <div class="created"><span>2026-04-03 - </span><span>Singapore, </span></div>
              <h2>QA Analyst</h2>
            </div>
          </a>
        </div>
      </li>
    </ul>
    """
    extractor = TieredExtractorV73()
    jobs = extractor._extract_nuxt_job_rows_v73(html, "https://careers.quadient.com/en/jobs")

    assert len(jobs) == 2
    assert jobs[0]["title"] == "Senior Data Engineer"
    assert jobs[0]["source_url"].endswith("/en/jobs/senior-data-engineer")
    assert jobs[0]["location_raw"] == "Remote"
    assert jobs[0]["extraction_method"] == "ats_nuxt_job_rows_v73"


def test_job_link_fallback_requires_job_url_or_apply_context():
    html = """
    <div>
      <footer>
        <a href="https://www.techjobasia.com/hk-events/ysip">HYAB Youth Start-up Internship Programme 2025</a>
      </footer>
      <div class="job-card">
        <a href="/jobs/ml-engineer">
          <h3>ML Engineer</h3>
        </a>
      </div>
    </div>
    """
    extractor = TieredExtractorV73()
    root = _parse_html(html)
    jobs = extractor._extract_from_job_links(root, "https://www.techjobasia.com/jobs")

    assert len(jobs) == 1
    assert jobs[0]["title"] == "ML Engineer"
    assert jobs[0]["source_url"].endswith("/jobs/ml-engineer")


def test_drop_obvious_non_jobs_removes_saved_jobs_and_nav_pages():
    extractor = TieredExtractorV73()
    jobs = [
        {"title": "Saved jobs (0)", "source_url": "https://careers.quadient.com/en/wishlist"},
        {"title": "Learning & Growth", "source_url": "https://careers.quadient.com/en/learning-and-growth"},
        {"title": "Senior Product Manager", "source_url": "https://careers.quadient.com/en/jobs/senior-product-manager"},
    ]

    kept = extractor._drop_obvious_non_jobs_v73(jobs)
    assert len(kept) == 1
    assert kept[0]["title"] == "Senior Product Manager"
