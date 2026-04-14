from app.crawlers.tiered_extractor_v78 import TieredExtractorV78


def test_careers_page_api_base_parsing_uses_base_slug_template():
    html = """
    <script>
      const baseUrl = "https://www.careers-page.com";
      const clientSlug = "the-cavalry-global";
      const vm = new Vue({
        apiBaseURL: `${baseUrl}/api/v1.0/c/${clientSlug}/`,
      });
    </script>
    """
    extractor = TieredExtractorV78()
    bases = extractor._careers_page_api_bases_v78(
        "https://www.careers-page.com/the-cavalry-global",
        html,
    )

    assert len(bases) >= 1
    assert bases[0][0] == "https://www.careers-page.com/api/v1.0/c/the-cavalry-global/"
    assert bases[0][1] == "https://www.careers-page.com"
    assert bases[0][2] == "the-cavalry-global"


def test_careers_page_item_to_job_builds_detail_url_from_hash():
    extractor = TieredExtractorV78()
    item = {
        "position_name": "General Manager",
        "hash": "L9YWVRV6",
        "city": "Amsterdam",
        "country": "Netherlands",
        "description": "Lead day-to-day venue operations.",
        "employment_type": "Full Time",
    }
    job = extractor._careers_page_item_to_job_v78(
        item,
        "https://www.careers-page.com/the-cavalry-global",
        "https://www.careers-page.com",
        "the-cavalry-global",
    )

    assert job is not None
    assert job["title"] == "General Manager"
    assert job["source_url"] == "https://www.careers-page.com/the-cavalry-global/job/L9YWVRV6"
    assert job["location_raw"] == "Amsterdam, Netherlands"
    assert job["extraction_method"] == "ats_careers_page_api_v78"


def test_careers_page_item_rejects_template_title():
    extractor = TieredExtractorV78()
    item = {"position_name": "[[ job.position_name ]]", "hash": "AAA111"}
    job = extractor._careers_page_item_to_job_v78(
        item,
        "https://www.careers-page.com/the-cavalry-global",
        "https://www.careers-page.com",
        "the-cavalry-global",
    )
    assert job is None


def test_connx_same_page_urls_are_repaired_from_detail_paths():
    extractor = TieredExtractorV78()
    page_url = "https://gippswater.connxcareers.com/"
    html = """
    <div class="GridTable GridTable--rows">
      <div onclick="window.location='/job/details/communications-and-engagement-advisor'">
        <div class="name">Communications and Engagement Advisor</div>
      </div>
      <div onclick="window.location='/job/details/expression-of-interest-aboriginal-employment'">
        <div class="name">Expression of Interest - Aboriginal Employment</div>
      </div>
      <div onclick="window.location='/job/details/functional-leads-oracle'">
        <div class="name">Functional Leads (Oracle)</div>
      </div>
    </div>
    """
    jobs = [
        {"title": "Communications and Engagement Advisor", "source_url": page_url},
        {"title": "Expression of Interest - Aboriginal Employment", "source_url": page_url},
        {"title": "Functional Leads (Oracle)", "source_url": page_url},
    ]

    repaired = extractor._repair_connx_same_page_urls_v78(jobs, html, page_url)

    assert repaired is not jobs
    assert repaired[0]["source_url"].endswith("/job/details/communications-and-engagement-advisor")
    assert repaired[1]["source_url"].endswith("/job/details/expression-of-interest-aboriginal-employment")
    assert repaired[2]["source_url"].endswith("/job/details/functional-leads-oracle")
