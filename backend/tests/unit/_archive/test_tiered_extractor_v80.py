from app.crawlers.tiered_extractor_v80 import TieredExtractorV80


def test_query_table_extractor_recovers_slug_rows_and_short_acronyms():
    extractor = TieredExtractorV80()
    html = """
    <table id="jobs-list-table">
      <tbody>
        <tr>
          <td><a href="./?slug=fom-141&v=0">FOM</a></td>
          <td>Wyndham Tamansari Jivva Resort Bali</td>
          <td>indonesian</td>
          <td><a href="./?slug=fom-141&v=0">View</a></td>
        </tr>
        <tr>
          <td><a href="./?slug=sales-manager-142&v=0">SALES MANAGER</a></td>
          <td>Wyndham Tamansari Jivva Resort Bali</td>
          <td>indonesian</td>
          <td><a href="./?slug=sales-manager-142&v=0">View</a></td>
        </tr>
        <tr>
          <td><a href="./?slug=pastry-chef-88&v=0">Pastry Chef</a></td>
          <td>Wyndham Tamansari Jivva Resort Bali</td>
          <td>indonesian</td>
          <td><a href="./?slug=pastry-chef-88&v=0">View</a></td>
        </tr>
      </tbody>
    </table>
    """

    jobs = extractor._extract_query_table_jobs_v80(
        html,
        "https://www.balihotelsassociation.com/career-opportunities/",
    )
    titles = {j["title"] for j in jobs}

    assert len(jobs) == 3
    assert "FOM" in titles
    assert "SALES MANAGER" in titles
    assert "Pastry Chef" in titles


def test_normalize_title_restores_hyphen_specialization_suffix():
    extractor = TieredExtractorV80()

    title = extractor._normalize_title("Technical Account Manager - Service & Consulting")

    assert title == "Technical Account Manager - Service & Consulting"


def test_clean_description_removes_css_noise_and_mojibake():
    extractor = TieredExtractorV80()

    css_noise = """
    < Back Buying Executive (Buyer Commodity)
    Job Type Full Time Workspace On-Site
    Job Description
    .comp-m2wubzjl { --wix-color-1: 255,255,255; --wix-color-2: 202,221,183; --wix-color-3: 150,188,111; }
    """
    assert extractor._clean_description_v73(css_noise) is None

    mojibake = "Our Story We are owned by the worldâ\x80\x99s largest producer."
    cleaned = extractor._clean_description_v73(mojibake)
    assert cleaned is not None
    assert "world's" in cleaned
