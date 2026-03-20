"""
Domain list import tasks — bulk company seeding from publicly available domain lists.

Sources:
  - Tranco Top 1M: https://tranco-list.eu/top-1m.csv.zip  (daily, free)
  - Majestic Million: http://downloads.majestic.com/majestic_million.csv (daily, free)
  - Wikidata SPARQL: https://query.wikidata.org/sparql  (free, no auth)
    Queries for organisations/companies per country that have an official website (P856).
    This catches established companies that don't use country-code TLDs (e.g. qantas.com).

Strategy:
  1. Download / query the source
  2. Filter by country TLD (Tranco/Majestic) or by country property (Wikidata)
  3. Insert each domain as a company using ON CONFLICT DO NOTHING (dedup by domain)
  4. Auto-enqueue new companies into company_config queue

Supported markets and their TLDs:
  AU: .com.au  .net.au  .org.au
  NZ: .co.nz   .net.nz  .org.nz
  SG: .com.sg  .sg
  MY: .com.my  .my
  HK: .com.hk  .hk
  PH: .com.ph  .ph
  ID: .co.id   .id  (excluding generic .id infra domains)
  TH: .co.th   .th
"""

import asyncio
import csv
import io
import logging
import zipfile
from datetime import datetime, timezone

from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

# TLD → market code mapping
MARKET_TLDS: dict[str, str] = {
    # AU
    ".com.au": "AU", ".net.au": "AU", ".org.au": "AU", ".asn.au": "AU",
    # NZ
    ".co.nz": "NZ", ".net.nz": "NZ", ".org.nz": "NZ",
    # SG
    ".com.sg": "SG", ".sg": "SG",
    # MY
    ".com.my": "MY", ".my": "MY",
    # HK
    ".com.hk": "HK", ".hk": "HK",
    # PH
    ".com.ph": "PH", ".ph": "PH",
    # ID
    ".co.id": "ID", ".id": "ID",
    # TH
    ".co.th": "TH", ".th": "TH",
}

# Domains to skip (infra, CDN, not companies)
SKIP_DOMAINS = frozenset([
    "google.com", "facebook.com", "youtube.com", "amazon.com", "cloudflare.com",
    "amazonaws.com", "fastly.net", "akamai.net", "akamaiedge.net",
    "wordpress.com", "blogspot.com", "tumblr.com", "github.io",
    "googleapis.com", "gstatic.com", "googlevideo.com",
    "gov.au", "edu.au", "gov.nz", "ac.nz",
])

# Skip TLDs within .id that are clearly not company sites
ID_SKIP_PATTERNS = ("gov.id", "ac.id", "mil.id", "go.id", "sch.id", "web.id")
TH_SKIP_PATTERNS = ("go.th", "ac.th", "mi.th", "police.th")


def _get_market(domain: str) -> str | None:
    """Return market code if domain has a target-market TLD, else None."""
    d = domain.lower()
    for tld, market in MARKET_TLDS.items():
        if d.endswith(tld):
            if market == "ID" and any(d.endswith(p) for p in ID_SKIP_PATTERNS):
                return None
            if market == "TH" and any(d.endswith(p) for p in TH_SKIP_PATTERNS):
                return None
            return market
    return None


def _domain_to_name(domain: str) -> str:
    """Guess a company name from a domain (strip TLD, replace hyphens)."""
    parts = domain.split(".")
    if len(parts) >= 3 and parts[-2] in ("com", "co", "net", "org", "asn"):
        name = parts[0]
    elif len(parts) >= 2:
        name = parts[0]
    else:
        name = domain
    return name.replace("-", " ").replace("_", " ").title()


async def _import_domains_from_csv(rows: list[tuple[str, str]], source: str) -> dict:
    """Import domain rows into companies table. Returns stats dict."""
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text
    import uuid as uuid_lib
    from app.services import queue_manager

    stats = {"total": len(rows), "new": 0, "skipped": 0}
    BATCH = 500

    async with AsyncSessionLocal() as db:
        for i in range(0, len(rows), BATCH):
            batch = rows[i:i + BATCH]
            new_ids = []

            for domain, market in batch:
                try:
                    result = await db.execute(text("""
                        INSERT INTO companies (name, domain, root_url, market_code, discovered_via, crawl_priority, company_status)
                        VALUES (:name, :domain, :root_url, :market, :source, 4, 'no_sites_new')
                        ON CONFLICT (domain) DO NOTHING
                        RETURNING id
                    """), {
                        "name": _domain_to_name(domain),
                        "domain": domain,
                        "root_url": f"https://{domain}",
                        "market": market,
                        "source": source,
                    })
                    row = result.fetchone()
                    if row:
                        new_ids.append(row[0])
                        stats["new"] += 1
                    else:
                        stats["skipped"] += 1
                except Exception as e:
                    logger.debug(f"Skip {domain}: {e}")
                    stats["skipped"] += 1

            # Enqueue all new companies in this batch
            for company_id in new_ids:
                await queue_manager.enqueue(db, "company_config", company_id, added_by=source)

            await db.commit()
            logger.info(f"Domain import batch {i // BATCH + 1}: {len(new_ids)} new companies")

    return stats


def _run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


@celery_app.task(name="domain_import.tranco", bind=True, max_retries=2)
def import_tranco_domains(self, max_domains: int = 1_000_000):
    """
    Download Tranco Top 1M list and import all domains with target-market TLDs.
    Deduplicates by domain (ON CONFLICT DO NOTHING).
    """
    import urllib.request

    logger.info("Downloading Tranco top-1M list...")
    try:
        url = "https://tranco-list.eu/top-1m.csv.zip"
        with urllib.request.urlopen(url, timeout=60) as resp:
            data = resp.read()
    except Exception as e:
        logger.error(f"Tranco download failed: {e}")
        raise self.retry(exc=e)

    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                for rank_str, domain in reader:
                    if len(rows) >= max_domains:
                        break
                    domain = domain.strip().lower()
                    if domain in SKIP_DOMAINS:
                        continue
                    market = _get_market(domain)
                    if market:
                        rows.append((domain, market))
    except Exception as e:
        logger.error(f"Tranco parse failed: {e}")
        raise self.retry(exc=e)

    logger.info(f"Tranco: {len(rows)} target-market domains to import")
    stats = _run_async(_import_domains_from_csv(rows, "tranco_domain_list"))
    logger.info(f"Tranco import complete: {stats}")
    return stats


@celery_app.task(name="domain_import.majestic", bind=True, max_retries=2)
def import_majestic_domains(self):
    """
    Download Majestic Million and import all domains with target-market TLDs.
    Majestic ranks by referring subnets (backlink authority) — excellent for established companies.
    """
    import urllib.request

    logger.info("Downloading Majestic Million list...")
    try:
        url = "http://downloads.majestic.com/majestic_million.csv"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read().decode("utf-8")
    except Exception as e:
        logger.error(f"Majestic download failed: {e}")
        raise self.retry(exc=e)

    rows = []
    try:
        reader = csv.DictReader(io.StringIO(data))
        for row in reader:
            domain = row.get("Domain", "").strip().lower()
            if not domain or domain in SKIP_DOMAINS:
                continue
            market = _get_market(domain)
            if market:
                rows.append((domain, market))
    except Exception as e:
        logger.error(f"Majestic parse failed: {e}")
        raise self.retry(exc=e)

    logger.info(f"Majestic: {len(rows)} target-market domains to import")
    stats = _run_async(_import_domains_from_csv(rows, "majestic_domain_list"))
    logger.info(f"Majestic import complete: {stats}")
    return stats


@celery_app.task(name="domain_import.asic", bind=True, max_retries=2)
def import_asic_companies(self):
    """
    Download ASIC company dataset (Australian registered companies).
    ~389MB CSV with all ACN-registered companies (Pty Ltd, Ltd, etc.).
    Filters to active/registered companies only.
    """
    import urllib.request
    import re
    from datetime import date

    # Build URL for current month's file
    today = date.today()
    filename = f"company_{today.strftime('%Y%m')}.zip"
    url = f"https://data.gov.au/data/dataset/7b8656f9-606d-4337-af29-66b89b2eeefb/resource/d6d03876-71a4-4e82-8c77-2e4df5da4236/download/{filename}"

    logger.info(f"Downloading ASIC dataset: {url}")
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            data = resp.read()
    except Exception as e:
        logger.error(f"ASIC download failed: {e}")
        raise self.retry(exc=e)

    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    status = row.get("Status", "").strip().upper()
                    if status not in ("REGD", "REGISTERED"):
                        continue
                    name = row.get("Company Name", "").strip()
                    if not name:
                        continue
                    # Derive domain from company name (best effort)
                    slug = re.sub(r"[^a-z0-9]", "", name.lower().split("pty")[0].split("ltd")[0].strip())
                    if len(slug) < 3:
                        continue
                    domain = f"{slug}.com.au"
                    rows.append((domain, "AU", name))
    except Exception as e:
        logger.error(f"ASIC parse failed: {e}")
        raise self.retry(exc=e)

    # For ASIC, we import with the actual company name
    logger.info(f"ASIC: {len(rows)} active AU companies to import")
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text
    from app.services import queue_manager

    async def _import():
        stats = {"total": len(rows), "new": 0, "skipped": 0}
        BATCH = 500
        async with AsyncSessionLocal() as db:
            for i in range(0, len(rows), BATCH):
                batch = rows[i:i + BATCH]
                new_ids = []
                for domain, market, company_name in batch:
                    try:
                        result = await db.execute(text("""
                            INSERT INTO companies (name, domain, root_url, market_code, discovered_via, crawl_priority, company_status)
                            VALUES (:name, :domain, :root_url, :market, 'asic_registry', 3, 'no_sites_new')
                            ON CONFLICT (domain) DO NOTHING
                            RETURNING id
                        """), {"name": company_name, "domain": domain, "root_url": f"https://{domain}", "market": market})
                        row = result.fetchone()
                        if row:
                            new_ids.append(row[0])
                            stats["new"] += 1
                        else:
                            stats["skipped"] += 1
                    except Exception:
                        stats["skipped"] += 1
                for company_id in new_ids:
                    await queue_manager.enqueue(db, "company_config", company_id, added_by="asic_registry")
                await db.commit()
        return stats

    stats = _run_async(_import())
    logger.info(f"ASIC import complete: {stats}")
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Wikidata SPARQL importer
# Finds organisations with an official website (P856) registered in each market.
# Covers companies that use .com or other non-country-TLD domains — the gap that
# Tranco/Majestic miss.
# ─────────────────────────────────────────────────────────────────────────────

# Wikidata QIDs for each market country
WIKIDATA_COUNTRY_QIDS: dict[str, str] = {
    "AU": "Q408",   # Australia
    "NZ": "Q664",   # New Zealand
    "SG": "Q334",   # Singapore
    "MY": "Q833",   # Malaysia
    "HK": "Q8646",  # Hong Kong
    "PH": "Q928",   # Philippines
    "ID": "Q252",   # Indonesia
    "TH": "Q869",   # Thailand
}

# Wikidata SPARQL endpoint
_WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

# Instance-of types that represent companies / organisations
_ORG_TYPES = " ".join(f"wd:{q}" for q in [
    "Q4830453",   # business
    "Q783794",    # company
    "Q891723",    # public company
    "Q6881511",   # enterprise
    "Q17222056",  # limited liability company
    "Q43229",     # organization (catches NGOs, educational orgs etc.)
])

_SPARQL_TEMPLATE = """
SELECT DISTINCT ?label ?website WHERE {{
  ?org wdt:P17 wd:{country_qid} ;
       wdt:P856 ?website .
  ?org rdfs:label ?label FILTER(lang(?label) = "en") .
}}
LIMIT {limit}
OFFSET {offset}
"""


def _extract_domain(url: str) -> str | None:
    """Extract bare domain from a URL string."""
    import re
    url = url.strip().rstrip("/")
    m = re.match(r"https?://(?:www\.)?([^/?#]+)", url, re.IGNORECASE)
    if not m:
        return None
    domain = m.group(1).lower()
    # Skip social media, aggregators, file hosts
    _SKIP = ("facebook.com", "twitter.com", "linkedin.com", "youtube.com",
             "instagram.com", "wikipedia.org", "wikimedia.org", "github.com",
             "google.com", "apple.com", "bloomberg.com", "crunchbase.com")
    if any(domain.endswith(s) for s in _SKIP):
        return None
    return domain


@celery_app.task(name="domain_import.wikidata", bind=True, max_retries=2, time_limit=3600)
def import_wikidata_companies(self, markets: list | None = None, limit_per_market: int = 10_000):
    """
    Query Wikidata for companies/organisations with official websites in each target market.
    Pages through results 1,000 at a time; polite 1s delay between requests.
    """
    import time
    import urllib.request
    import urllib.parse
    import json

    if markets is None:
        markets = list(WIKIDATA_COUNTRY_QIDS.keys())

    total_stats: dict[str, int] = {"new": 0, "skipped": 0, "total": 0}

    for market in markets:
        qid = WIKIDATA_COUNTRY_QIDS.get(market)
        if not qid:
            logger.warning("Wikidata: unknown market %s, skipping", market)
            continue

        logger.info("Wikidata: querying %s (QID=%s)", market, qid)
        rows: list[tuple[str, str]] = []  # (domain, company_name)
        page_size = 1000
        offset = 0

        while offset < limit_per_market:
            sparql = _SPARQL_TEMPLATE.format(
                country_qid=qid,
                limit=page_size,
                offset=offset,
            )
            url = _WIKIDATA_SPARQL + "?" + urllib.parse.urlencode({
                "query": sparql,
                "format": "json",
            })
            req = urllib.request.Request(url, headers={
                "User-Agent": "JobHarvest/1.0 (company-seed; contact: admin@localhost)",
                "Accept": "application/sparql-results+json",
            })
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            except Exception as exc:
                logger.warning("Wikidata %s offset=%d error: %s", market, offset, exc)
                break

            bindings = payload.get("results", {}).get("bindings", [])
            if not bindings:
                break  # no more results

            for b in bindings:
                website_val = b.get("website", {}).get("value", "")
                label_val = b.get("label", {}).get("value", "")
                domain = _extract_domain(website_val)
                if domain and label_val:
                    rows.append((domain, label_val.strip()))

            logger.info("Wikidata %s: fetched %d results at offset %d", market, len(bindings), offset)
            offset += page_size

            if len(bindings) < page_size:
                break  # last page

            time.sleep(1)  # polite delay between SPARQL requests

        if not rows:
            logger.info("Wikidata %s: no results", market)
            continue

        logger.info("Wikidata %s: %d candidate domains to import", market, len(rows))
        stats = _run_async(_import_wikidata_rows(rows, market))
        for k in ("new", "skipped", "total"):
            total_stats[k] = total_stats.get(k, 0) + stats.get(k, 0)
        logger.info("Wikidata %s complete: %s", market, stats)

    logger.info("Wikidata import finished: %s", total_stats)
    return total_stats


async def _import_wikidata_rows(rows: list[tuple[str, str]], market: str) -> dict:
    """Insert Wikidata-sourced companies into the DB and enqueue for config."""
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text
    from app.services import queue_manager

    # Deduplicate by domain (keep first occurrence = most specific label)
    seen: dict[str, str] = {}
    for domain, name in rows:
        if domain not in seen:
            seen[domain] = name
    deduped = list(seen.items())

    stats = {"total": len(deduped), "new": 0, "skipped": 0}
    BATCH = 500

    async with AsyncSessionLocal() as db:
        for i in range(0, len(deduped), BATCH):
            batch = deduped[i:i + BATCH]
            new_ids = []
            for domain, name in batch:
                try:
                    result = await db.execute(text("""
                        INSERT INTO companies
                            (name, domain, root_url, market_code, discovered_via,
                             crawl_priority, company_status)
                        VALUES
                            (:name, :domain, :root_url, :market, 'wikidata',
                             4, 'no_sites_new')
                        ON CONFLICT (domain) DO NOTHING
                        RETURNING id
                    """), {
                        "name": name,
                        "domain": domain,
                        "root_url": f"https://{domain}",
                        "market": market,
                    })
                    row = result.fetchone()
                    if row:
                        new_ids.append(row[0])
                        stats["new"] += 1
                    else:
                        stats["skipped"] += 1
                except Exception as exc:
                    logger.debug("Wikidata skip %s: %s", domain, exc)
                    stats["skipped"] += 1

            for company_id in new_ids:
                await queue_manager.enqueue(db, "company_config", company_id, added_by="wikidata")
            await db.commit()
            logger.info("Wikidata %s batch %d/%d: %d new", market, i // BATCH + 1,
                        (len(deduped) - 1) // BATCH + 1, len(new_ids))

    return stats
