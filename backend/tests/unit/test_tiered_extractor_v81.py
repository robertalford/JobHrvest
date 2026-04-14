from app.crawlers.tiered_extractor_v81 import TieredExtractorV81


def test_query_table_extracts_title_text_when_detail_link_is_in_action_cell():
    extractor = TieredExtractorV81()
    html = """
    <table>
      <tbody id="jobResults">
        <tr>
          <td>Kasir &amp; Baker</td>
          <td>Jakarta</td>
          <td><a href="/Career/detail/22">Detail</a></td>
        </tr>
        <tr>
          <td>Produksi Buns &amp; Pastry</td>
          <td>Bandung</td>
          <td><a href="/Career/detail/23">Lihat</a></td>
        </tr>
        <tr>
          <td>Driver B1/BII (BALIKPAPAN)</td>
          <td>Balikpapan</td>
          <td><a href="/Career/detail/31">Apply</a></td>
        </tr>
      </tbody>
    </table>
    """

    page_url = "https://rotio.id/Career"
    jobs = extractor._extract_query_table_jobs_v80(html, page_url)

    titles = {j["title"] for j in jobs}
    assert len(jobs) == 3
    assert "Kasir & Baker" in titles
    assert "Produksi Buns & Pastry" in titles
    assert extractor._passes_structured_row_jobset_v81(jobs, page_url)


def test_gupy_row_extractor_keeps_clean_title_and_metadata():
    extractor = TieredExtractorV81()
    html = """
    <ul data-testid="job-list__list">
      <li data-testid="job-list__listitem">
        <a href="/jobs/11006388?jobBoardSource=gupy_public_page">
          <div><div>ADVOGADO CONTRATUAL PLENO</div><div>Belo Horizonte - MG</div><div>Efetivo</div></div>
        </a>
      </li>
      <li data-testid="job-list__listitem">
        <a href="/jobs/10815507?jobBoardSource=gupy_public_page">
          <div><div>ANALISTA ADMINISTRATIVO</div><div>Sao Paulo - SP</div><div>Efetivo</div></div>
        </a>
      </li>
      <li data-testid="job-list__listitem">
        <a href="/jobs/10956962?jobBoardSource=gupy_public_page">
          <div><div>ANALISTA ADMINISTRATIVO DE OBRAS</div><div>Belo Horizonte - MG</div><div>Temporario</div></div>
        </a>
      </li>
    </ul>
    """

    page_url = "https://direcionalengenharia.gupy.io/"
    jobs = extractor._extract_gupy_jobs_v81(html, page_url)

    assert len(jobs) == 3
    assert jobs[0]["title"] == "ADVOGADO CONTRATUAL PLENO"
    assert jobs[0]["location_raw"] == "Belo Horizonte - MG"
    assert jobs[0]["employment_type"] == "Efetivo"
    assert extractor._passes_structured_row_jobset_v81(jobs, page_url)


def test_jobmonster_rows_preferred_over_menu_service_links():
    extractor = TieredExtractorV81()
    html = """
    <nav><a href="https://www.frenz.co.nz/jobcheck/">JOB CHECKS</a></nav>
    <section>
      <article class="loadmore-item noo_job" data-url="https://www.frenz.co.nz/jobs/dairy-farm-assistant-kokatahi/">
        <h2><a href="https://www.frenz.co.nz/jobs/dairy-farm-assistant-kokatahi/">Experienced Farm Assistant Needed in Kokatahi</a></h2>
        <span class="job-location"><a><em>West Coast</em></a></span>
      </article>
      <article class="loadmore-item noo_job" data-url="https://www.frenz.co.nz/jobs/lead-refrigeration-service-technician/">
        <h2><a href="https://www.frenz.co.nz/jobs/lead-refrigeration-service-technician/">Lead Refrigeration Service Technician</a></h2>
        <span class="job-location"><a><em>Auckland</em></a></span>
      </article>
      <article class="loadmore-item noo_job" data-url="https://www.frenz.co.nz/jobs/stockperson-canterbury/">
        <h2><a href="https://www.frenz.co.nz/jobs/stockperson-canterbury/">Stockperson Needed in Christchurch</a></h2>
        <span class="job-location"><a><em>Canterbury</em></a></span>
      </article>
    </section>
    """

    page_url = "https://www.frenz.co.nz/?post_type=noo_job&s=&_noo_job_field_job_position=&type="
    jobs = extractor._extract_jobmonster_jobs_v81(html, page_url)

    assert len(jobs) == 3
    assert all("jobcheck" not in j["source_url"] for j in jobs)
    assert all("/jobs/" in j["source_url"] for j in jobs)
    assert extractor._passes_structured_row_jobset_v81(jobs, page_url)


def test_title_validation_rejects_generic_job_detail_labels():
    extractor = TieredExtractorV81()

    assert extractor._is_valid_title_v60("Project Manager")
    assert not extractor._is_valid_title_v60("Job Details")
    assert not extractor._is_valid_title_v60("JOB CHECKS")
