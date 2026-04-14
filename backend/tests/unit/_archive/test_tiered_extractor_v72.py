from app.crawlers.tiered_extractor_v72 import TieredExtractorV72


def test_rekrutmen_titles_count_as_job_signal_for_jobset_validation():
    extractor = TieredExtractorV72()
    page_url = "https://rekrutmen.pln.co.id/vacancy/site/index"
    jobs = [
        {
            "title": "REKRUTMEN PLN GROUP TINGKAT S1/D4 BIDANG MATEMATIKA TAHUN 2025",
            "source_url": "https://rekrutmen.pln.co.id/vacancy/site/view/id/1",
            "description": None,
            "location_raw": None,
        },
        {
            "title": "REKRUTMEN PUTRA-PUTRI ASLI PAPUA PLN GROUP TAHUN 2025 LOKASI MERAUKE",
            "source_url": "https://rekrutmen.pln.co.id/vacancy/site/view/id/2",
            "description": None,
            "location_raw": None,
        },
        {
            "title": "REKRUTMEN PUTRA-PUTRI ASLI PAPUA PLN GROUP TAHUN 2025 LOKASI WAMENA",
            "source_url": "https://rekrutmen.pln.co.id/vacancy/site/view/id/3",
            "description": None,
            "location_raw": None,
        },
        {
            "title": "REKRUTMEN PUTRA-PUTRI ASLI PAPUA PLN GROUP TAHUN 2025 LOKASI TIMIKA",
            "source_url": "https://rekrutmen.pln.co.id/vacancy/site/view/id/4",
            "description": None,
            "location_raw": None,
        },
    ]

    assert extractor._title_has_job_signal(jobs[0]["title"]) is True
    assert extractor._passes_jobset_validation(jobs, page_url) is True


def test_drop_obvious_non_jobs_removes_sitemap_and_nav_titles():
    extractor = TieredExtractorV72()
    jobs = [
        {"title": "Job Campaigns", "source_url": "https://www.techjobasia.com/sitemap/job-campaigns"},
        {"title": "All Jobs", "source_url": "https://www.techjobasia.com/sitemap/jobs"},
        {"title": "Google Career Certificates", "source_url": "https://gcc.talentlabs.org"},
        {"title": "Senior Data Engineer", "source_url": "https://example.org/jobs/senior-data-engineer"},
    ]

    kept = extractor._drop_obvious_non_jobs_v72(jobs)

    assert len(kept) == 1
    assert kept[0]["title"] == "Senior Data Engineer"


def test_backfill_from_row_context_populates_location_and_description():
    extractor = TieredExtractorV72()
    html = """
    <ul id=\"jobs_list_container\">
      <li>
        <a href=\"/jobs/7259071-hse-advisor\"><h3>HSE Advisor</h3></a>
        <span class=\"mt-1 text-md\">Hawthorn</span>
        <p>Lead safety programs across major infrastructure projects.</p>
      </li>
    </ul>
    """
    jobs = [
        {
            "title": "HSE Advisor",
            "source_url": "https://mazzeigroup-1750399679.teamtailor.com/jobs/7259071-hse-advisor",
            "location_raw": None,
            "description": None,
            "extraction_method": "tier2_linked_cards_v67",
        }
    ]

    filled = extractor._backfill_from_row_context_v72(
        jobs,
        html,
        "https://mazzeigroup-1750399679.teamtailor.com/jobs",
    )

    assert filled[0]["location_raw"] == "Hawthorn"
    assert "Lead safety programs" in (filled[0]["description"] or "")


def test_fast_path_enrichment_skips_example_host():
    extractor = TieredExtractorV72()
    jobs = [
        {
            "title": "Senior Data Engineer",
            "source_url": "https://example.com/jobs/senior-data-engineer",
            "location_raw": None,
            "description": None,
            "extraction_method": "tier2_linked_cards_v67",
        },
        {
            "title": "Data Analyst",
            "source_url": "https://example.com/jobs/data-analyst",
            "location_raw": None,
            "description": None,
            "extraction_method": "tier2_linked_cards_v67",
        },
    ]

    assert extractor._should_enrich_fast_path_v72(jobs, "https://example.com/jobs") is False
