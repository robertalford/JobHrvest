from app.crawlers.tiered_extractor import _parse_html
from app.crawlers.tiered_extractor_v77 import TieredExtractorV77


def test_backfill_uses_metadata_ancestor_for_location():
    html = """
    <div class="jobs_main_info">
      <div class="info-project-inner">
        <div>
          <a href="/jobs/legal-secretary-disputes/"><h3>Legal Secretary - Disputes</h3></a>
          <p>Top Tier Global Firm | Competitive Salary package | WFH Flexibility.</p>
        </div>
        <div class="info-icon">
          <div class="location"><span>Brisbane</span></div>
          <div class="time"><span>Full Time</span></div>
          <div class="money"><span>Up to $95,000 package</span></div>
        </div>
      </div>
    </div>
    """
    extractor = TieredExtractorV77()
    page_url = "https://egconsulting.com.au/jobs/"

    raw_jobs = extractor._extract_linked_job_cards_v67(html, page_url)
    assert len(raw_jobs) == 1
    assert raw_jobs[0]["location_raw"] is None

    jobs = extractor._postprocess_jobs_v73(raw_jobs, html, page_url)
    assert len(jobs) == 1
    assert jobs[0]["location_raw"] == "Brisbane"


def test_row_description_prefers_summary_text_over_meta_noise():
    html = """
    <div class="info-project-inner">
      <div>
        <a href="/jobs/legal-secretary-technology/"><h3>Legal Secretary - Technology</h3></a>
        <p>Top Tier Global Firm | Competitive Salary package | WFH Flexibility.</p>
      </div>
      <div class="info-icon">
        <div class="location"><span>Melbourne</span></div>
        <div class="time"><span>Full Time</span></div>
        <div class="money"><span>$110,000 package</span></div>
        <a href="/jobs/legal-secretary-technology/">Apply</a>
      </div>
    </div>
    """

    root = _parse_html(html)
    assert root is not None
    anchor = root.xpath("//a[@href][1]")[0]

    extractor = TieredExtractorV77()
    row = extractor._find_row_container_v73(anchor)
    assert row is not None

    desc = extractor._extract_row_description_v73(row, "Legal Secretary - Technology")
    assert desc is not None
    assert "Top Tier Global Firm" in desc
    assert "Apply" not in desc
    assert "Full Time" not in desc


def test_description_cleaner_removes_glued_prefix_and_cta_tail():
    extractor = TieredExtractorV77()
    dirty = (
        "Automation Test EngineerVijayawada, IndiaApply"
        "Juvare is a SaaS software company focused on resilience solutions. Read More"
    )

    cleaned = extractor._clean_description_v73(dirty)
    assert cleaned is not None
    assert cleaned.startswith("Juvare is a SaaS software company")
    assert "Read More" not in cleaned
