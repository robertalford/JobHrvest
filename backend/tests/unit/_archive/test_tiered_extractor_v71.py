from app.crawlers.tiered_extractor_v71 import TieredExtractorV71


def test_title_validator_rejects_generic_career_and_location_headings():
    extractor = TieredExtractorV71()

    assert extractor._is_valid_title_v60("Peluang Karir") is False
    assert extractor._is_valid_title_v60("Get In Touch!") is False
    assert extractor._is_valid_title_v60("USA, New York") is False
    assert extractor._is_valid_title_v60("CareerLink Recruitment (Thailand) Co., Ltd.") is False
    assert extractor._is_valid_title_v60("Senior Interior Designer") is True


def test_linked_cards_skip_location_titles_and_keep_real_roles():
    html = """
    <div class="job-listing">
      <a href="https://www.porticorecruitment.com/job/senior-interior-designer-hotels-1234">
        <h3 class="title">Senior Interior Designer (Hotels)</h3>
      </a>
      <a href="https://www.porticorecruitment.com/job/senior-interior-designer-hotels-1234#location">
        <span class="title">USA, New York</span>
      </a>
    </div>
    """

    extractor = TieredExtractorV71()
    jobs = extractor._extract_linked_job_cards_v67(
        html,
        "https://www.porticorecruitment.com/search",
    )

    assert len(jobs) == 1
    assert jobs[0]["title"] == "Senior Interior Designer (Hotels)"


def test_successfactors_table_extraction_and_pagination_urls():
    html = """
    <div class="paginationShell">
      <ul class="pagination">
        <li><a href="?q=&sortColumn=referencedate&sortDirection=desc&startrow=275">12</a></li>
        <li><a href="?q=&sortColumn=referencedate&sortDirection=desc&startrow=300">13</a></li>
      </ul>
    </div>
    <table>
      <tbody>
        <tr class="data-row">
          <td class="colTitle">
            <a href="/job/Paris-Accounting-Executive-HF-Melia-France/1368060233/" class="jobTitle-link">
              Accounting Executive H/F - Melia France
            </a>
          </td>
          <td class="colLocation"><span class="jobLocation">Paris, FR</span></td>
        </tr>
        <tr class="data-row">
          <td class="colTitle">
            <a href="/job/Berlin-PRUEBA-DE-SISTEMA-%28NO-APLICAR%29/1006512801/" class="jobTitle-link">
              PRUEBA DE SISTEMA (NO APLICAR)
            </a>
          </td>
          <td class="colLocation"><span class="jobLocation">Berlin, DE</span></td>
        </tr>
      </tbody>
    </table>
    """

    extractor = TieredExtractorV71()
    page_url = "https://careers.melia.com/search/?q=&sortColumn=referencedate&sortDirection=desc&startrow=250"

    jobs = extractor._extract_successfactors_table_v71(html, page_url)
    assert len(jobs) == 1
    assert jobs[0]["title"] == "Accounting Executive H/F - Melia France"
    assert jobs[0]["location_raw"] == "Paris, FR"

    pages = extractor._successfactors_pagination_urls_v71(html, page_url)
    assert pages == [
        "https://careers.melia.com/search/?q=&sortColumn=referencedate&sortDirection=desc&startrow=275",
        "https://careers.melia.com/search/?q=&sortColumn=referencedate&sortDirection=desc&startrow=300",
    ]


def test_homerun_state_payload_extraction():
    html = r"""
    <section id="job-list">
      <job-list v-bind="{&quot;content&quot;:{&quot;vacancies&quot;:[{&quot;title&quot;:&quot;Creative Director&quot;,&quot;url&quot;:&quot;https:\/\/careers.resn.co.nz\/creative-director\/en&quot;,&quot;location_id&quot;:11934,&quot;job_type_id&quot;:0},{&quot;title&quot;:&quot;Expressions of Interest - Amsterdam or Wellington&quot;,&quot;url&quot;:&quot;https:\/\/careers.resn.co.nz\/expressions-of-interest\/en&quot;,&quot;location_id&quot;:18245,&quot;job_type_id&quot;:0}],&quot;locations&quot;:[{&quot;id&quot;:11934,&quot;name&quot;:&quot;Wellington, NZ&quot;},{&quot;id&quot;:18245,&quot;name&quot;:&quot;Wellington WGN or Amsterdam NL&quot;}],&quot;job_types&quot;:[{&quot;id&quot;:0,&quot;name&quot;:&quot;Full-time&quot;}]}}"></job-list>
    </section>
    """

    extractor = TieredExtractorV71()
    jobs = extractor._extract_homerun_jobs_v71(html, "https://careers.resn.co.nz/")

    assert len(jobs) == 2
    titles = {j["title"] for j in jobs}
    assert "Creative Director" in titles
    assert "Expressions of Interest - Amsterdam or Wellington" in titles
    first = next(j for j in jobs if j["title"] == "Creative Director")
    assert first["location_raw"] == "Wellington, NZ"
    assert first["employment_type"] == "Full-time"
    assert first["extraction_method"] == "ats_homerun_state_v71"
