import asyncio
from pathlib import Path

import pytest

from app.crawlers.tiered_extractor_v100 import TieredExtractorV100, _truncate_html


_ROOT = Path(__file__).resolve().parents[3]
_CTX = _ROOT / "storage" / "auto_improve_context" / "v10_latest"


def _read(name: str) -> str:
    path = _CTX / name
    if not path.exists():
        pytest.skip(f"Missing context file: {path}")
    return path.read_text(encoding="utf-8", errors="replace")


def test_local_extraction_recovers_breezy_jobs():
    extractor = TieredExtractorV100()
    html = _read("failure_1_cloudcommerce_breezy_hr.html")

    jobs = extractor._extract_local_jobs(html, "https://cloudcommerce.breezy.hr/")  # noqa: SLF001
    titles = {str(j.get("title") or "").strip() for j in jobs}

    assert "Business Growth Manager" in titles
    assert "Area Manager" in titles
    assert len(jobs) >= 25


def test_local_extraction_recovers_generic_job_grid():
    extractor = TieredExtractorV100()
    html = _read("failure_2_odfjelltechnology_com.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html, "https://www.odfjelltechnology.com/career/job-openings/"
    )
    titles = {str(j.get("title") or "").strip() for j in jobs}

    assert "eLearning Content Designer" in titles
    assert "GBS Finance Intern MNL" in titles
    assert len(jobs) >= 18


def test_local_extraction_recovers_teamtailor_rows():
    extractor = TieredExtractorV100()
    html = _read("failure_5_hohepacanterbury_teamtailor_co.html")

    jobs = extractor._extract_local_jobs(html, "https://hohepacanterbury.teamtailor.com/jobs")  # noqa: SLF001
    titles = {str(j.get("title") or "").strip() for j in jobs}

    assert "LEAP Support Coordinator" in titles
    assert "Support Worker - Pamu Service" in titles
    assert len(jobs) >= 18


def test_local_extraction_recovers_wordpress_career_cards():
    extractor = TieredExtractorV100()
    html = _read("failure_8_salvationarmy_org_hk.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html, "https://salvationarmy.org.hk/join-us/job-vacancies/?lang=en"
    )
    titles = {str(j.get("title") or "").strip() for j in jobs}

    assert "Part-time Assistant Centre Supervisor" in titles
    assert "Ministry Assistant III & Administrative Assistant – Wan Chai Corps" in titles
    assert len(jobs) >= 30


def test_local_extraction_recovers_greenhouse_titles_without_location_glue():
    extractor = TieredExtractorV100()
    html = _read("failure_1_job_boards_greenhouse_io.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html,
        "https://job-boards.greenhouse.io/embed/job_board?for=juvare",
    )
    titles = {str(j.get("title") or "").strip() for j in jobs}

    assert "Automation Test Engineer" in titles
    assert "Lead Fullstack Developer" in titles
    assert len(jobs) >= 9


def test_local_extraction_recovers_generic_card_title_with_query_detail_link():
    extractor = TieredExtractorV100()
    html = _read("failure_6_simap_afgindo_com.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html,
        "https://simap.afgindo.com/career",
    )
    titles = {str(j.get("title") or "").strip() for j in jobs}
    urls = {str(j.get("source_url") or "").strip() for j in jobs}

    assert "Staf Pelayanan" in titles
    assert "Apoteker Pendamping" in titles
    assert "https://simap.afgindo.com/career?id=282" in urls
    assert len(jobs) >= 8


def test_local_extraction_keeps_multilingual_job_titles():
    extractor = TieredExtractorV100()
    html = _read("failure_8_careerlink_co_th.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html,
        "https://www.careerlink.co.th/job/list?keyword_use=A",
    )
    titles = {str(j.get("title") or "").strip() for j in jobs}

    assert "วิศวกรวิจัยและพัฒนา (การฉีดขึ้นรูปพลาสติก)" in titles
    assert "หัวหน้างานซ่อมบำรุง (ชิ้นส่วนโลหะแม่นยำ)" in titles
    assert len(jobs) >= 50


def test_local_extraction_recovers_elementor_heading_cards_with_shared_apply_link():
    extractor = TieredExtractorV100()
    html = _read("failure_6_itconnexion_com.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html,
        "https://www.itconnexion.com/career-opportunities/",
    )
    titles = {str(j.get("title") or "").strip() for j in jobs}
    urls = {str(j.get("source_url") or "").strip() for j in jobs}

    assert "IT Support Engineer (Field)" in titles
    assert "Technical Account Manager" in titles
    assert "IT System Engineer(Senior / L3)" in titles
    assert "https://www.itconnexion.com/career-job/" in urls
    assert len(jobs) >= 3


def test_local_extraction_recovers_metadata_from_bootstrap_list_rows():
    extractor = TieredExtractorV100()
    html = _read("failure_7_careerlink_co_th.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html,
        "https://www.careerlink.co.th/job/list?keyword_use=A",
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Japanese speaking Production Officer (Manufacturing Aluminum)" in by_title
    assert by_title["Japanese speaking Production Officer (Manufacturing Aluminum)"]["location_raw"]
    assert len(jobs) >= 50


def test_local_extraction_recovers_metadata_from_span_card_layout():
    extractor = TieredExtractorV100()
    html = _read("failure_8_digimonk_in.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html,
        "https://digimonk.in/career/",
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Python Developer (Django)" in by_title
    assert by_title["Python Developer (Django)"]["location_raw"] == "Gwalior/Noida"
    assert by_title["Python Developer (Django)"]["description"]
    assert len(jobs) >= 5


def test_truncate_html_preserves_application_json_state_scripts():
    html = """
    <html><body>
    <script id="__NEXT_DATA__" type="application/json">
    {"props":{"pageProps":{"jobs":[{"title":"Site Engineer","url":"/job/site-engineer"}]}}}
    </script>
    <script>window.analytics={"event":"page_view"}</script>
    </body></html>
    """

    truncated = _truncate_html(html, max_chars=5000)

    assert "__NEXT_DATA__" in truncated
    assert '"Site Engineer"' in truncated
    assert "window.analytics" not in truncated


def test_local_extraction_greenhouse_dedupes_anchor_duplicates_and_keeps_location():
    extractor = TieredExtractorV100()
    html = _read("failure_8_job_boards_greenhouse_io.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html,
        "https://job-boards.greenhouse.io/pixocial?gh_src=f3bdf5d78us",
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert len(jobs) == 3
    assert "Product Designer" in by_title
    assert by_title["Product Designer"]["location_raw"]


def test_local_anchor_extraction_recovers_location_from_row_context():
    extractor = TieredExtractorV100()
    html = _read("failure_6_hays_com_my.html")

    jobs = extractor._extract_local_jobs(  # noqa: SLF001
        html,
        "https://www.hays.com.my/job-search",
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Executive, Prophet Modelling" in by_title
    assert by_title["Executive, Prophet Modelling"]["location_raw"] == "Malaysia"


def test_embedded_state_jobs_accept_absolute_url_and_structured_location():
    extractor = TieredExtractorV100()
    html = """
    <html><body>
    <script type="application/json">
    {"jobs":[{"title":"Platform Engineer","absolute_url":"/jobs/123","location":{"name":"Sydney, NSW"}}]}
    </script>
    </body></html>
    """

    jobs = extractor._extract_local_jobs(html, "https://example.com/careers")  # noqa: SLF001
    assert jobs
    assert jobs[0]["title"] == "Platform Engineer"
    assert jobs[0]["source_url"] == "https://example.com/jobs/123"
    assert jobs[0]["location_raw"] == "Sydney, NSW"


def test_local_extraction_recovers_same_page_sections_without_detail_links():
    extractor = TieredExtractorV100()
    html = _read("failure_6_prudenceinv_com.html")

    jobs = extractor._extract_local_jobs(html, "https://prudenceinv.com/career/")  # noqa: SLF001
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Marketing Assistant" in by_title
    assert "Research Intern" in by_title
    assert "Summer Internship Programme" in by_title
    assert by_title["Marketing Assistant"]["location_raw"] == "Hong Kong"
    assert by_title["Marketing Assistant"]["source_url"].startswith("https://prudenceinv.com/career/#job-")
    assert len(jobs) >= 3


def test_local_extraction_recovers_wordpress_entry_title_job_posts():
    extractor = TieredExtractorV100()
    html = _read("failure_7_tomorange_co_uk.html")

    jobs = extractor._extract_local_jobs(html, "https://tomorange.co.uk/hgv-driver-jobs/")  # noqa: SLF001
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "HGV Class 2 Driver – Kettering" in by_title
    assert "HGV Class 1 Driver New Contract – Scunthorpe" in by_title
    assert by_title["HGV Class 2 Driver – Kettering"]["source_url"].startswith("https://tomorange.co.uk/hgv-class-2-driver-kettering/")
    assert len(jobs) >= 9


def test_local_extraction_recovers_tailwind_group_card_jobs():
    extractor = TieredExtractorV100()
    html = _read("failure_5_fredrecruitment_co_nz.html")

    jobs = extractor._extract_local_jobs(html, "https://fredrecruitment.co.nz/job-board/")  # noqa: SLF001
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Risk and Compliance Specialist" in by_title
    assert "People & Culture Coordinator" in by_title
    assert by_title["Risk and Compliance Specialist"]["source_url"].startswith(
        "https://fredrecruitment.co.nz/job/risk-and-compliance-specialist/"
    )
    assert by_title["Risk and Compliance Specialist"]["location_raw"] == "Auckland City Fringe"
    assert len(jobs) >= 9


def test_local_extraction_recovers_gupy_rows_with_clean_titles():
    extractor = TieredExtractorV100()
    html = _read("failure_7_direcionalengenharia_gupy_io.html")

    jobs = extractor._extract_local_jobs(html, "https://direcionalengenharia.gupy.io/")  # noqa: SLF001
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "ADVOGADO CONTRATUAL PLENO" in by_title
    assert "ANALISTA ADMINISTRATIVO DE OBRAS" in by_title
    assert by_title["ADVOGADO CONTRATUAL PLENO"]["location_raw"] == "Belo Horizonte - MG"
    assert by_title["ADVOGADO CONTRATUAL PLENO"]["employment_type"] == "Efetivo"
    assert len(jobs) >= 10


def test_local_anchor_extraction_recovers_jobthai_location_metadata():
    extractor = TieredExtractorV100()
    html = _read("failure_8_jobthai_com.html")

    jobs = extractor._extract_local_jobs(html, "https://www.jobthai.com/en/company/98976")  # noqa: SLF001
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Project Control Engineer" in by_title
    assert by_title["Project Control Engineer"]["location_raw"] == "Mueang Rayong, Rayong"
    assert by_title["Accounting Manager"]["location_raw"] == "Si Racha, Chon Buri"


def test_shell_endpoint_recovery_extracts_jobs_from_fetch_json(monkeypatch: pytest.MonkeyPatch):
    extractor = TieredExtractorV100()
    html = _read("failure_5_omens_studios_com.html")

    async def fake_fetch_json(url: str, method: str = "GET", payload: dict | None = None):  # noqa: ARG001
        if url == "https://www.omens-studios.com/jobs.json":
            return [
                {
                    "title": "3D Animator",
                    "location": "sg",
                    "description": "<p>Character animation role</p>",
                },
                {
                    "title": "Business Executive",
                    "location": "kl",
                    "description": "<p>Business development support</p>",
                },
            ]
        return None

    monkeypatch.setattr(extractor, "_fetch_json_endpoint_payload", fake_fetch_json)  # noqa: SLF001

    jobs = asyncio.run(  # noqa: PLW1510
        extractor._extract_shell_endpoint_jobs(  # noqa: SLF001
            html,
            "https://www.omens-studios.com/careers/",
        )
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "3D Animator" in by_title
    assert by_title["3D Animator"]["location_raw"] == "Singapore"
    assert by_title["3D Animator"]["source_url"].startswith("https://www.omens-studios.com/careers/#job-")
    assert len(jobs) >= 2


def test_shell_endpoint_recovery_uses_martian_theme_probe(monkeypatch: pytest.MonkeyPatch):
    extractor = TieredExtractorV100()
    html = _read("failure_2_careers_wem_com_au.html")

    async def fake_fetch_json(url: str, method: str = "GET", payload: dict | None = None):  # noqa: ARG001
        if "jobBoardThemeId=689" in url and "clientCode=wem-civil" in url:
            return [
                {
                    "title": "Site Engineer - Civil",
                    "location": "North & South West Sydney",
                    "url": "/wem-civil/site-engineer-civil",
                }
            ]
        return None

    monkeypatch.setattr(extractor, "_fetch_json_endpoint_payload", fake_fetch_json)  # noqa: SLF001

    jobs = asyncio.run(  # noqa: PLW1510
        extractor._extract_shell_endpoint_jobs(  # noqa: SLF001
            html,
            "https://careers.wem.com.au/wem-civil/",
        )
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Site Engineer - Civil" in by_title
    assert by_title["Site Engineer - Civil"]["source_url"].startswith("https://careers.wem.com.au/wem-civil/")


def test_shell_endpoint_recovery_uses_martian_next_data_probe(monkeypatch: pytest.MonkeyPatch):
    extractor = TieredExtractorV100()
    html = _read("failure_2_careers_wem_com_au.html")

    async def fake_fetch_json(url: str, method: str = "GET", payload: dict | None = None):  # noqa: ARG001
        if "/_next/data/" in url and "wem-civil" in url:
            return {
                "pageProps": {
                    "jobs": [
                        {
                            "title": "Project Surveyor - Civil",
                            "jobUrl": "/jobs/project-surveyor-civil",
                            "location": "North Sydney",
                        }
                    ]
                }
            }
        return None

    monkeypatch.setattr(extractor, "_fetch_json_endpoint_payload", fake_fetch_json)  # noqa: SLF001

    jobs = asyncio.run(  # noqa: PLW1510
        extractor._extract_shell_endpoint_jobs(  # noqa: SLF001
            html,
            "https://careers.wem.com.au/wem-civil/",
        )
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Project Surveyor - Civil" in by_title
    assert by_title["Project Surveyor - Civil"]["source_url"].startswith("https://careers.wem.com.au/jobs/")


def test_shell_endpoint_recovery_uses_successfactors_portalcareer_probe(monkeypatch: pytest.MonkeyPatch):
    extractor = TieredExtractorV100()
    html = _read("failure_4_career10_successfactors_com.html")

    async def fake_fetch_json(url: str, method: str = "GET", payload: dict | None = None):  # noqa: ARG001
        if "portalcareer" in url and "navBarLevel=JOB_SEARCH" in url:
            return [
                {
                    "title": "Nutrient Specialist Service Centre - Western Lower North Island",
                    "url": "/career?career_ns=job_listing&career_job_req_id=123456",
                    "location": "New Zealand",
                }
            ]
        return None

    monkeypatch.setattr(extractor, "_fetch_json_endpoint_payload", fake_fetch_json)  # noqa: SLF001

    jobs = asyncio.run(  # noqa: PLW1510
        extractor._extract_shell_endpoint_jobs(  # noqa: SLF001
            html,
            "https://career10.successfactors.com/career?company=Ballance&career%5fns=job%5flisting%5fsummary&navBarLevel=JOB%5fSEARCH",
        )
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Nutrient Specialist Service Centre - Western Lower North Island" in by_title
    assert "career_job_req_id=123456" in by_title["Nutrient Specialist Service Centre - Western Lower North Island"][
        "source_url"
    ]


def test_shell_endpoint_recovery_uses_icims_search_probe(monkeypatch: pytest.MonkeyPatch):
    extractor = TieredExtractorV100()
    html = """
    <html><body>
    <p>Already applied? <a href="https://sapient-publicisgroupe.icims.com/">View application status</a></p>
    </body></html>
    """

    async def fake_fetch_json(url: str, method: str = "GET", payload: dict | None = None):  # noqa: ARG001
        if "sapient-publicisgroupe.icims.com/jobs/search" in url:
            return [
                {
                    "title": "Senior Strategy Consultant",
                    "url": "https://sapient-publicisgroupe.icims.com/jobs/9999/senior-strategy-consultant/job",
                    "location": "Sydney, Australia",
                }
            ]
        return None

    monkeypatch.setattr(extractor, "_fetch_json_endpoint_payload", fake_fetch_json)  # noqa: SLF001

    jobs = asyncio.run(  # noqa: PLW1510
        extractor._extract_shell_endpoint_jobs(  # noqa: SLF001
            html,
            "https://careers.publicissapient.com/job-search?country=Australia",
        )
    )
    by_title = {str(j.get("title") or "").strip(): j for j in jobs}

    assert "Senior Strategy Consultant" in by_title
