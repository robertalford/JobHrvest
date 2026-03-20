"""
Aggregator Link Harvester — Link-discovery engine.

Visits permitted aggregator sites to extract outbound links to company
career pages. NEVER extracts or stores job content from aggregators.

Hard rules enforced on every request:
  - Jora, SEEK, Jobstreet, JobsDB are completely off-limits.
  - We only FOLLOW links out to company domains — we never store aggregator content.
  - Companies are unique by domain; career pages are unique by URL.
    ON CONFLICT DO NOTHING is used for all inserts.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse, quote_plus

from bs4 import BeautifulSoup
from sqlalchemy import text

from app.crawlers.domain_blocklist import assert_not_blocked, is_blocked

logger = logging.getLogger(__name__)

# ── Aggregator domain blocklist (never add these as companies) ─────────────
AGGREGATOR_DOMAINS = frozenset([
    "indeed.com", "au.indeed.com", "uk.indeed.com",
    "linkedin.com", "glassdoor.com", "glassdoor.com.au", "glassdoor.co.uk",
    "careerone.com.au", "adzuna.com.au", "adzuna.co.uk", "adzuna.com",
    "monster.com", "monster.co.uk", "ziprecruiter.com", "careerbuilder.com",
    "reed.co.uk", "totaljobs.com", "angel.co", "wellfound.com",
    "careerjet.com.au", "careerjet.co.uk", "careerjet.com",
    "talent.com", "au.talent.com", "uk.talent.com",
    "jooble.org", "au.jooble.org",
    "whatjobs.com", "au.whatjobs.com",
    "simplyhired.com", "simplyhired.co.uk",
    "jobrapido.com", "recruit.net", "juju.com",
    "getwork.com", "seek.com.au", "jora.com",
    "jobstreet.com", "jobsdb.com",
    "ethicaljobs.com.au", "gumtree.com.au",
    "apsjobs.gov.au", "workforceaustralia.gov.au",
    "gradconnection.com", "au.gradconnection.com",
])


def _is_aggregator_domain(domain: str) -> bool:
    d = domain.lower().lstrip("www.")
    return any(d == agg or d.endswith(f".{agg}") for agg in AGGREGATOR_DOMAINS)


def _normalise_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def _is_company_url(url: str) -> bool:
    if not url:
        return False
    domain = _normalise_domain(url)
    if not domain:
        return False
    if is_blocked(url):
        return False
    if _is_aggregator_domain(domain):
        return False
    return True


# ── Shared HTTP helpers ────────────────────────────────────────────────────

async def _fetch_static(url: str, session=None) -> Optional[str]:
    from curl_cffi.requests import AsyncSession
    assert_not_blocked(url)
    try:
        if session:
            r = await session.get(url, timeout=20, impersonate="chrome124")
        else:
            async with AsyncSession() as s:
                r = await s.get(url, timeout=20, impersonate="chrome124")
        if r.status_code == 200:
            return r.text
        logger.debug(f"HTTP {r.status_code} for {url}")
        return None
    except Exception as e:
        logger.debug(f"fetch_static failed for {url}: {e}")
        return None


async def _fetch_playwright(url: str, wait_selector: str = None, timeout: int = 15000) -> Optional[str]:
    from playwright.async_api import async_playwright
    assert_not_blocked(url)
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                locale="en-AU",
            )
            page = await ctx.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=5000)
                except Exception:
                    pass
            else:
                await asyncio.sleep(2)
            html = await page.content()
            await browser.close()
            return html
    except Exception as e:
        logger.debug(f"fetch_playwright failed for {url}: {e}")
        return None


# ── DB helpers ─────────────────────────────────────────────────────────────

async def upsert_company(db, name: str, domain: str, market: str = "AU", source: str = "discovery") -> Optional[str]:
    """
    Insert company if domain not already present (unique by domain).
    Returns new company_id string, or None if already existed.
    Also auto-enqueues new companies into company_config queue.
    """
    if not domain or _is_aggregator_domain(domain):
        return None
    domain = domain.lower().lstrip("www.")
    try:
        result = await db.execute(text("""
            INSERT INTO companies (name, domain, root_url, market_code, discovered_via, crawl_priority, company_status)
            VALUES (:name, :domain, :root_url, :market, :source, 5, 'no_sites_new')
            ON CONFLICT (domain) DO NOTHING
            RETURNING id
        """), {
            "name": (name or domain)[:255],
            "domain": domain,
            "root_url": f"https://{domain}",
            "market": market,
            "source": source,
        })
        row = result.fetchone()
        if row:
            company_id = str(row[0])
            from app.services import queue_manager
            import uuid
            await queue_manager.enqueue(db, "company_config", uuid.UUID(company_id), added_by="discovery")
            await db.commit()
            logger.info(f"New company discovered: {domain} ({market}) via {source}")
            return company_id
        await db.commit()
        return None
    except Exception as e:
        logger.warning(f"upsert_company failed for {domain}: {e}")
        await db.rollback()
        return None


async def _update_harvest_time(db, source_name: str) -> None:
    await db.execute(text(
        "UPDATE aggregator_sources SET last_link_harvest_at = NOW() WHERE name = :name"
    ), {"name": source_name})
    await db.commit()


# ── Base harvester ─────────────────────────────────────────────────────────

class BaseHarvester:
    SOURCE_NAME: str = ""
    MARKET: str = "AU"
    MAX_PAGES: int = 50
    RESULTS_PER_PAGE: int = 10
    USE_PLAYWRIGHT: bool = False
    WAIT_SELECTOR: Optional[str] = None

    def _search_url(self, page: int) -> str:
        raise NotImplementedError

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        raise NotImplementedError

    def _has_results(self, html: str) -> bool:
        if not html:
            return False
        lower = html.lower()
        return not any(p in lower for p in [
            "no jobs found", "no results found", "0 jobs", "0 results",
            "no matching jobs", "no positions found", "couldn't find any jobs",
        ])

    async def harvest(self, db) -> list[dict]:
        discovered = []
        seen_domains: set = set()
        empty_pages = 0

        from curl_cffi.requests import AsyncSession
        async with AsyncSession() as session:
            for page_num in range(self.MAX_PAGES):
                url = self._search_url(page_num + 1)
                try:
                    assert_not_blocked(url)
                except Exception:
                    break

                if self.USE_PLAYWRIGHT:
                    html = await _fetch_playwright(url, wait_selector=self.WAIT_SELECTOR)
                else:
                    html = await _fetch_static(url, session=session)

                if not html or not self._has_results(html):
                    empty_pages += 1
                    if empty_pages >= 2:
                        break
                    continue

                empty_pages = 0
                links = self._extract_links(html, url)

                if not links:
                    empty_pages += 1
                    if empty_pages >= 2:
                        break
                    continue

                new_this_page = 0
                for link in links:
                    domain = link.get("domain", "")
                    if not domain or domain in seen_domains:
                        continue
                    career_url = link.get("career_url", f"https://{domain}")
                    if not _is_company_url(career_url):
                        continue
                    seen_domains.add(domain)
                    company_id = await upsert_company(
                        db,
                        name=link.get("company_name", domain),
                        domain=domain,
                        market=self.MARKET,
                        source=f"discovery_{self.SOURCE_NAME.lower().replace(' ', '_').replace('.', '')}",
                    )
                    if company_id:
                        discovered.append(link)
                        new_this_page += 1

                logger.info(f"{self.SOURCE_NAME} p{page_num + 1}: {len(links)} links, {new_this_page} new")

                # Stop when no new companies found for several consecutive pages
                if page_num >= 5 and new_this_page == 0:
                    logger.info(f"{self.SOURCE_NAME}: no new companies, stopping at page {page_num + 1}")
                    break

                await asyncio.sleep(1.5)

        await _update_harvest_time(db, self.SOURCE_NAME)
        logger.info(f"{self.SOURCE_NAME} done: {len(discovered)} new companies")
        return discovered

    def _soup_outbound_links(self, html: str, base_url: str, self_domain: str) -> list[dict]:
        """Generic: extract all outbound non-aggregator links."""
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href or href.startswith("#") or href.startswith("javascript"):
                continue
            abs_url = urljoin(base_url, href)
            domain = _normalise_domain(abs_url)
            if not domain or domain == self_domain or domain in seen:
                continue
            if not _is_company_url(abs_url):
                continue
            seen.add(domain)
            results.append({
                "company_name": a.get_text(strip=True)[:80] or domain,
                "domain": domain,
                "career_url": abs_url,
            })
        return results


# ── Indeed ─────────────────────────────────────────────────────────────────

class IndeedHarvester(BaseHarvester):
    RESULTS_PER_PAGE = 10
    MAX_PAGES = 100

    def __init__(self, subdomain="au.indeed.com", location="Australia", market="AU", source_name="Indeed AU"):
        self.subdomain = subdomain
        self.location = location
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        start = (page - 1) * self.RESULTS_PER_PAGE
        return f"https://{self.subdomain}/jobs?q=&l={quote_plus(self.location)}&start={start}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()
        apply_kws = ("apply on company site", "apply on employer site", "company website", "visit company")

        for a in soup.find_all("a", href=True):
            label = a.get_text(strip=True).lower()
            if not any(kw in label for kw in apply_kws):
                continue
            abs_url = urljoin(base_url, a["href"])
            if "indeed.com" in abs_url or not _is_company_url(abs_url):
                continue
            domain = _normalise_domain(abs_url)
            if domain and domain not in seen:
                seen.add(domain)
                results.append({"company_name": domain, "domain": domain, "career_url": abs_url})

        for card in soup.select("[data-jk], .job_seen_beacon, .jobsearch-SerpJobCard, [data-testid='job-card']"):
            name_el = card.select_one("[data-testid='company-name'], .companyName, .company")
            company_name = name_el.get_text(strip=True) if name_el else ""
            for a in card.find_all("a", href=True):
                abs_url = urljoin(base_url, a["href"])
                if "indeed.com" in abs_url or not _is_company_url(abs_url):
                    continue
                domain = _normalise_domain(abs_url)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append({"company_name": company_name or domain, "domain": domain, "career_url": abs_url})
                break

        return results

    def _has_results(self, html: str) -> bool:
        return bool(html) and ("data-jk" in html or "job_seen_beacon" in html)


# ── Careerjet ──────────────────────────────────────────────────────────────

class CareerjetHarvester(BaseHarvester):
    RESULTS_PER_PAGE = 20
    MAX_PAGES = 100

    def __init__(self, tld="com.au", location="Australia", market="AU", source_name="Careerjet AU"):
        self.tld = tld
        self.location = location
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        return f"https://www.careerjet.{self.tld}/jobs.html?s=&l={quote_plus(self.location)}&p={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()
        self_domain = f"careerjet.{self.tld}"
        for article in soup.select("article.job, .jobs article, li.job"):
            company_el = article.select_one(".company, [itemprop='hiringOrganization']")
            company_name = company_el.get_text(strip=True) if company_el else ""
            for a in article.find_all("a", href=True):
                abs_url = urljoin(base_url, a["href"])
                if self_domain in abs_url or not _is_company_url(abs_url):
                    continue
                domain = _normalise_domain(abs_url)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append({"company_name": company_name or domain, "domain": domain, "career_url": abs_url})
                break
        return results

    def _has_results(self, html: str) -> bool:
        return bool(html) and "article" in html and "job" in html.lower()


# ── Adzuna ─────────────────────────────────────────────────────────────────

class AdzunaHarvester(BaseHarvester):
    RESULTS_PER_PAGE = 50
    MAX_PAGES = 50

    def __init__(self, country_code="au", tld="com.au", location="Australia", market="AU", source_name="Adzuna AU"):
        self.country_code = country_code
        self.tld = tld
        self.location = location
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        return f"https://www.adzuna.{self.tld}/search?q=&loc=1&p={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()
        self_domain = f"adzuna.{self.tld}"
        for article in soup.select("article, .result, [data-aid]"):
            company_el = article.select_one("[class*='company'], [class*='employer'], [itemprop='hiringOrganization']")
            company_name = company_el.get_text(strip=True) if company_el else ""
            for a in article.find_all("a", href=True):
                abs_url = urljoin(base_url, a["href"])
                if self_domain in abs_url or not _is_company_url(abs_url):
                    continue
                domain = _normalise_domain(abs_url)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append({"company_name": company_name or domain, "domain": domain, "career_url": abs_url})
                break
        return results

    def _has_results(self, html: str) -> bool:
        return bool(html) and ("<article" in html or "data-aid" in html)


# ── Talent.com ─────────────────────────────────────────────────────────────

class TalentHarvester(BaseHarvester):
    RESULTS_PER_PAGE = 20
    MAX_PAGES = 50

    def __init__(self, subdomain="au", location="Australia", market="AU", source_name="Talent.com AU"):
        self.subdomain = subdomain
        self.location = location
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        return f"https://{self.subdomain}.talent.com/jobs?k=&l={quote_plus(self.location)}&p={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()
        for card in soup.select(".card__job, [class*='job-card'], .result"):
            company_el = card.select_one("[class*='company'], [class*='employer']")
            company_name = company_el.get_text(strip=True) if company_el else ""
            for a in card.find_all("a", href=True):
                abs_url = urljoin(base_url, a["href"])
                if "talent.com" in abs_url or not _is_company_url(abs_url):
                    continue
                domain = _normalise_domain(abs_url)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append({"company_name": company_name or domain, "domain": domain, "career_url": abs_url})
                break
        return results

    def _has_results(self, html: str) -> bool:
        return bool(html) and ("card__job" in html or "job-card" in html)


# ── Jooble ─────────────────────────────────────────────────────────────────

class JoobleHarvester(BaseHarvester):
    RESULTS_PER_PAGE = 20
    MAX_PAGES = 30

    def __init__(self, subdomain="au", location="Australia", market="AU", source_name="Jooble AU"):
        self.subdomain = subdomain
        self.location = location
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        loc = quote_plus(self.location)
        base = f"https://{self.subdomain}.jooble.org/jobs/{loc}"
        return base if page == 1 else f"{base}?p={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        return self._soup_outbound_links(html, base_url, "jooble.org")

    def _has_results(self, html: str) -> bool:
        return bool(html) and ("job" in html.lower())


# ── LinkedIn (Playwright) ──────────────────────────────────────────────────

class LinkedInHarvester(BaseHarvester):
    USE_PLAYWRIGHT = True
    WAIT_SELECTOR = ".job-search-card"
    RESULTS_PER_PAGE = 25
    MAX_PAGES = 40

    def __init__(self, location="Australia", geo_id="101452733", market="AU", source_name="LinkedIn AU"):
        self.location = location
        self.geo_id = geo_id
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        start = (page - 1) * self.RESULTS_PER_PAGE
        return (f"https://www.linkedin.com/jobs/search/?keywords=&location={quote_plus(self.location)}"
                f"&geoId={self.geo_id}&start={start}")

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()
        apply_kws = ("apply on company", "apply on employer", "visit company", "company website")

        for a in soup.find_all("a", href=True):
            abs_url = urljoin(base_url, a["href"])
            if "linkedin.com" in abs_url or not _is_company_url(abs_url):
                continue
            label = a.get_text(strip=True).lower()
            if any(kw in label for kw in apply_kws):
                domain = _normalise_domain(abs_url)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append({"company_name": domain, "domain": domain, "career_url": abs_url})

        for card in soup.select(".job-search-card, .base-card, [data-entity-urn]"):
            company_el = card.select_one(".job-search-card__subtitle-link, .base-search-card__subtitle")
            company_name = company_el.get_text(strip=True) if company_el else ""
            for a in card.find_all("a", href=True):
                abs_url = urljoin(base_url, a["href"])
                if "linkedin.com" in abs_url or not _is_company_url(abs_url):
                    continue
                domain = _normalise_domain(abs_url)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append({"company_name": company_name or domain, "domain": domain, "career_url": abs_url})
                break

        return results

    def _has_results(self, html: str) -> bool:
        return bool(html) and ("job-search-card" in html or "base-card" in html)


# ── Glassdoor (Playwright) ─────────────────────────────────────────────────

class GlassdoorHarvester(BaseHarvester):
    USE_PLAYWRIGHT = True
    MAX_PAGES = 30

    def __init__(self, tld="com.au", location_slug="australia-jobs-SRCH_IL.0,9_IN16", market="AU", source_name="Glassdoor AU"):
        self.tld = tld
        self.location_slug = location_slug
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        base = f"https://www.glassdoor.{self.tld}/Job/{self.location_slug}"
        return f"{base}.htm" if page == 1 else f"{base}_IP{page}.htm"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen = set()
        apply_kws = ("apply on", "company site", "company website", "apply now")
        for a in soup.find_all("a", href=True):
            abs_url = urljoin(base_url, a["href"])
            if "glassdoor" in abs_url or not _is_company_url(abs_url):
                continue
            label = a.get_text(strip=True).lower()
            if any(kw in label for kw in apply_kws):
                domain = _normalise_domain(abs_url)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append({"company_name": domain, "domain": domain, "career_url": abs_url})
        return results

    def _has_results(self, html: str) -> bool:
        return bool(html) and ("JobCard" in html or "job-listing" in html or "data-test=" in html)


# ── WhatJobs ───────────────────────────────────────────────────────────────

class WhatJobsHarvester(BaseHarvester):
    MAX_PAGES = 30
    RESULTS_PER_PAGE = 20

    def __init__(self, subdomain="au", location="Australia", market="AU", source_name="WhatJobs AU"):
        self.subdomain = subdomain
        self.location = location
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        loc = quote_plus(self.location)
        prefix = f"{self.subdomain}." if self.subdomain else ""
        return f"https://{prefix}whatjobs.com/jobs?searchWhat=&searchWhere={loc}&p={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        self_domain = f"{self.subdomain}.whatjobs.com" if self.subdomain else "whatjobs.com"
        return self._soup_outbound_links(html, base_url, self_domain)

    def _has_results(self, html: str) -> bool:
        return bool(html) and "job" in html.lower()


# ── SimplyHired ────────────────────────────────────────────────────────────

class SimplyHiredHarvester(BaseHarvester):
    MAX_PAGES = 50

    def __init__(self, tld="com", location="", market="US", source_name="SimplyHired US"):
        self.tld = tld
        self.location = location
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        loc = quote_plus(self.location)
        return f"https://www.simplyhired.{self.tld}/search?q=&l={loc}&pn={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        return self._soup_outbound_links(html, base_url, f"simplyhired.{self.tld}")

    def _has_results(self, html: str) -> bool:
        return bool(html) and ("SerpJob" in html or "job-card" in html or "viewjob" in html)


# ── Glints (SG, MY, ID) ────────────────────────────────────────────────────

class GlintsHarvester(BaseHarvester):
    """Glints — leading SEA job platform (SG, MY, ID, TH, VN, TW)."""
    MAX_PAGES = 30
    USE_PLAYWRIGHT = True

    def __init__(self, country_slug="sg", location="Singapore", market="SG", source_name="Glints SG"):
        self.country_slug = country_slug
        self.location = location
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        offset = (page - 1) * 30
        return f"https://glints.com/{self.country_slug}/opportunities/jobs/explore?offset={offset}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        return self._soup_outbound_links(html, base_url, "glints.com")

    def _has_results(self, html: str) -> bool:
        return bool(html) and ("job" in html.lower() and "company" in html.lower())


# ── Hiredly (MY) ───────────────────────────────────────────────────────────

class HiredlyHarvester(BaseHarvester):
    """Hiredly — Malaysia's leading job platform."""
    MAX_PAGES = 20
    USE_PLAYWRIGHT = True

    def __init__(self, market="MY", source_name="Hiredly MY"):
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        return f"https://hiredly.com/jobs?page={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        return self._soup_outbound_links(html, base_url, "hiredly.com")

    def _has_results(self, html: str) -> bool:
        return bool(html) and "job" in html.lower()


# ── Kalibrr (PH) ──────────────────────────────────────────────────────────

class KalibrrHarvester(BaseHarvester):
    """Kalibrr — Philippine job platform with company career pages."""
    MAX_PAGES = 20
    USE_PLAYWRIGHT = True

    def __init__(self, market="PH", source_name="Kalibrr PH"):
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        return f"https://www.kalibrr.com/job-board?page={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        return self._soup_outbound_links(html, base_url, "kalibrr.com")

    def _has_results(self, html: str) -> bool:
        return bool(html) and "job" in html.lower()


# ── Karir.com (ID) ────────────────────────────────────────────────────────

class KarirHarvester(BaseHarvester):
    """Karir.com — Indonesia's largest job portal."""
    MAX_PAGES = 20

    def __init__(self, market="ID", source_name="Karir.com"):
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        return f"https://karir.com/search-lowongan?page={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        return self._soup_outbound_links(html, base_url, "karir.com")

    def _has_results(self, html: str) -> bool:
        return bool(html) and "lowongan" in html.lower()


# ── JobThai (TH) ──────────────────────────────────────────────────────────

class JobThaiHarvester(BaseHarvester):
    """JobThai — Thailand's most popular job board."""
    MAX_PAGES = 20

    def __init__(self, market="TH", source_name="JobThai"):
        self.MARKET = market
        self.SOURCE_NAME = source_name

    def _search_url(self, page: int) -> str:
        return f"https://www.jobthai.com/en/jobs?page={page}"

    def _extract_links(self, html: str, base_url: str) -> list[dict]:
        return self._soup_outbound_links(html, base_url, "jobthai.com")

    def _has_results(self, html: str) -> bool:
        return bool(html) and "job" in html.lower()


# ── Source-name → harvester factory ───────────────────────────────────────

def get_harvester_for_source(source_name: str) -> Optional[BaseHarvester]:
    """Map aggregator_sources.name → harvester instance. Case-insensitive.
    Supported markets: AU, NZ, SG, MY, HK, PH, ID, TH."""
    name = source_name.strip().lower()
    MAP = {
        # AU — Australia
        "indeed au":         IndeedHarvester("au.indeed.com", "Australia", "AU", source_name),
        "careerjet au":      CareerjetHarvester("com.au", "Australia", "AU", source_name),
        "adzuna au":         AdzunaHarvester("au", "com.au", "Australia", "AU", source_name),
        "talent.com au":     TalentHarvester("au", "Australia", "AU", source_name),
        "jooble au":         JoobleHarvester("au", "Australia", "AU", source_name),
        "linkedin au":       LinkedInHarvester("Australia", "101452733", "AU", source_name),
        "glassdoor au":      GlassdoorHarvester("com.au", "australia-jobs-SRCH_IL.0,9_IN16", "AU", source_name),
        "whatjobs au":       WhatJobsHarvester("au", "Australia", "AU", source_name),
        # NZ — New Zealand
        "indeed nz":         IndeedHarvester("nz.indeed.com", "New Zealand", "NZ", source_name),
        "linkedin nz":       LinkedInHarvester("New Zealand", "105490917", "NZ", source_name),
        "glassdoor nz":      GlassdoorHarvester("co.nz", "new-zealand-jobs-SRCH_IL.0,11_IN185", "NZ", source_name),
        "adzuna nz":         AdzunaHarvester("nz", "co.nz", "New Zealand", "NZ", source_name),
        # SG — Singapore
        "indeed sg":         IndeedHarvester("sg.indeed.com", "Singapore", "SG", source_name),
        "linkedin sg":       LinkedInHarvester("Singapore", "102454443", "SG", source_name),
        "glassdoor sg":      GlassdoorHarvester("sg", "singapore-jobs-SRCH_IL.0,9_IN217", "SG", source_name),
        "glints sg":         GlintsHarvester("sg", "Singapore", "SG", source_name),
        # MY — Malaysia
        "indeed my":         IndeedHarvester("my.indeed.com", "Malaysia", "MY", source_name),
        "linkedin my":       LinkedInHarvester("Malaysia", "100583900", "MY", source_name),
        "glassdoor my":      GlassdoorHarvester("com", "malaysia-jobs-SRCH_IL.0,8_IN170", "MY", source_name),
        "glints my":         GlintsHarvester("id", "Malaysia", "MY", source_name),
        "hiredly my":        HiredlyHarvester("MY", source_name),
        # HK — Hong Kong
        "indeed hk":         IndeedHarvester("hk.indeed.com", "Hong Kong", "HK", source_name),
        "linkedin hk":       LinkedInHarvester("Hong Kong SAR", "103291313", "HK", source_name),
        "glassdoor hk":      GlassdoorHarvester("com", "hong-kong-jobs-SRCH_IL.0,9_IN123", "HK", source_name),
        # PH — Philippines
        "indeed ph":         IndeedHarvester("ph.indeed.com", "Philippines", "PH", source_name),
        "linkedin ph":       LinkedInHarvester("Philippines", "103121230", "PH", source_name),
        "kalibrr ph":        KalibrrHarvester("PH", source_name),
        # ID — Indonesia
        "indeed id":         IndeedHarvester("id.indeed.com", "Indonesia", "ID", source_name),
        "linkedin id":       LinkedInHarvester("Indonesia", "102478259", "ID", source_name),
        "glassdoor id":      GlassdoorHarvester("com", "indonesia-jobs-SRCH_IL.0,9_IN121", "ID", source_name),
        "glints id":         GlintsHarvester("id", "Indonesia", "ID", source_name),
        "karir.com":         KarirHarvester("ID", source_name),
        # TH — Thailand
        "indeed th":         IndeedHarvester("th.indeed.com", "Thailand", "TH", source_name),
        "linkedin th":       LinkedInHarvester("Thailand", "100560411", "TH", source_name),
        "glassdoor th":      GlassdoorHarvester("com", "thailand-jobs-SRCH_IL.0,8_IN115", "TH", source_name),
        "jobthai":           JobThaiHarvester("TH", source_name),
    }
    return MAP.get(name)
