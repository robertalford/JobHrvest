import asyncio
import glob
import importlib
import os
import sys
import types


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

CRAWLERS_DIR = os.path.join(os.path.dirname(__file__), "..", "app", "crawlers")
CONTEXT_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "storage", "auto_improve_context", "v9_1"
)


def _latest_version() -> int:
    files = glob.glob(os.path.join(CRAWLERS_DIR, "tiered_extractor_v*.py"))
    versions = []
    for path in files:
        raw = os.path.basename(path).replace("tiered_extractor_v", "").replace(".py", "")
        try:
            versions.append(int(raw))
        except ValueError:
            continue
    return max(versions) if versions else 16


def _load_latest_extractor():
    version = _latest_version()
    mod = importlib.import_module(f"app.crawlers.tiered_extractor_v{version}")
    cls = getattr(mod, f"TieredExtractorV{version}")
    return cls()


def _read_context(name: str) -> str:
    path = os.path.join(CONTEXT_DIR, name)
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def test_tbrhsc_linked_cards_rejects_job_basket_nav_label():
    ext = _load_latest_extractor()
    html = _read_context("failure_1_jobs_tbrhsc_net.html")

    jobs = ext._extract_linked_job_cards_v67(  # noqa: SLF001 - regression guard
        html, "https://jobs.tbrhsc.net/RecentPostings.aspx"
    )
    titles = [str(j.get("title") or "").strip() for j in jobs]

    assert "My Job Basket (0)" not in titles
    assert len(jobs) >= 5
    assert all("VacancyDetail.aspx?VacancyUID=" in str(j.get("source_url") or "") for j in jobs)


def test_bajau_single_easyjobs_card_is_extracted():
    ext = _load_latest_extractor()
    html = _read_context("failure_2_jobs_bajau_com.html")

    # Keep this test deterministic and offline-only.
    ext._should_enrich_fast_path_v73 = types.MethodType(  # noqa: SLF001
        lambda self, jobs, page_url: False,  # noqa: ARG005
        ext,
    )
    ext._collect_listing_pagination_urls_v89 = types.MethodType(  # noqa: SLF001
        lambda self, doc, page_url, max_pages=4: [],  # noqa: ARG005
        ext,
    )

    async def _return_seed(self, doc, page_url, seed_jobs):  # noqa: ARG001
        return list(seed_jobs)

    async def _return_jobs(self, doc, page_url, jobs):  # noqa: ARG001
        return list(jobs)

    async def _return_empty(self, page_url, doc):  # noqa: ARG001
        return []

    async def _return_empty_nuxt(self, page_url, doc):  # noqa: ARG001
        return []

    ext._expand_queryid_card_rows_v89 = types.MethodType(_return_seed, ext)  # noqa: SLF001
    ext._expand_paginated_heuristic_jobs_v88 = types.MethodType(_return_jobs, ext)  # noqa: SLF001
    ext._extract_jobs_json_feed_v74 = types.MethodType(_return_empty, ext)  # noqa: SLF001
    ext._probe_localized_nuxt_jobs_v73 = types.MethodType(_return_empty_nuxt, ext)  # noqa: SLF001

    class MockPage:
        url = "https://jobs.bajau.com/"
        requires_js_rendering = False

    class MockCompany:
        name = "Pt Bajau Escorindo"
        ats_platform = None

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    jobs = loop.run_until_complete(ext.extract(MockPage(), MockCompany(), html))
    titles = [str(j.get("title") or "").strip() for j in jobs]

    assert "Finance, Accounting & Tax Senior Staff" in titles
    assert "PT Bajau Escorindo" not in titles
    assert "Terapkan Sekarang" not in titles

    finance = next(j for j in jobs if str(j.get("title") or "").strip() == "Finance, Accounting & Tax Senior Staff")
    assert "finance-accounting-tax-senior-staff" in str(finance.get("source_url") or "")
    assert "jakarta" in str(finance.get("location_raw") or "").lower()
