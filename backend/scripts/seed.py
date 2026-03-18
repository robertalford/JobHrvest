#!/usr/bin/env python3
"""
Seed script — populates the database with initial data:
  - AU market (active) + inactive stubs for US, UK, NZ, SG
  - 50+ Australian seed companies across industries and ATS platforms
  - Aggregator sources (link-discovery only)
  - Blocked domains (SEEK, Jora, Jobstreet, JobsDB — pre-populated)
"""

import asyncio
import uuid
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


AU_SALARY_CONFIG = {
    "default_currency": "AUD",
    "currency_symbol": "$",
    "annual_keywords": ["per annum", "p.a.", "pa", "per year", "annual"],
    "hourly_keywords": ["per hour", "p/h", "ph", "/hr", "/hour", "hourly"],
    "daily_keywords": ["per day", "/day", "daily"],
    "super_keywords": ["super", "superannuation", "incl. super", "+ super"],
    "patterns": [
        r"A?\$\s*(\d[\d,]*)\s*[-–]\s*A?\$\s*(\d[\d,]*)",  # $80,000 - $120,000
        r"A?\$\s*(\d+(?:\.\d+)?)[Kk]\s*[-–]\s*A?\$\s*(\d+(?:\.\d+)?)[Kk]",  # $80K - $120K
        r"A?\$\s*(\d[\d,]*)\s*\+?\s*super",  # $90,000 + super
        r"A?\$\s*(\d+(?:\.\d+)?)/hr",  # $45/hr
    ],
    "state_abbreviations": ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"],
}

AU_LOCATION_CONFIG = {
    "country": "Australia",
    "country_code": "AU",
    "state_abbreviations": {
        "NSW": "New South Wales",
        "VIC": "Victoria",
        "QLD": "Queensland",
        "WA": "Western Australia",
        "SA": "South Australia",
        "TAS": "Tasmania",
        "ACT": "Australian Capital Territory",
        "NT": "Northern Territory",
    },
    "major_cities": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra", "Hobart", "Darwin"],
    "remote_keywords": ["Remote", "WFH", "Work from Home", "Remote - Australia", "Anywhere in Australia"],
}

MARKETS = [
    {
        "code": "AU", "name": "Australia", "is_active": True,
        "default_currency": "AUD", "locale": "en-AU",
        "salary_parsing_config": AU_SALARY_CONFIG,
        "location_parsing_config": AU_LOCATION_CONFIG,
        "aggregator_search_queries": {
            "indeed": ["jobs in Australia", "software engineer Australia", "data analyst Australia"],
            "linkedin": ["Australia jobs", "remote jobs Australia"],
        },
    },
    {"code": "US", "name": "United States", "is_active": False, "default_currency": "USD", "locale": "en-US", "salary_parsing_config": {}, "location_parsing_config": {}, "aggregator_search_queries": {}},
    {"code": "UK", "name": "United Kingdom", "is_active": False, "default_currency": "GBP", "locale": "en-GB", "salary_parsing_config": {}, "location_parsing_config": {}, "aggregator_search_queries": {}},
    {"code": "NZ", "name": "New Zealand", "is_active": False, "default_currency": "NZD", "locale": "en-NZ", "salary_parsing_config": {}, "location_parsing_config": {}, "aggregator_search_queries": {}},
    {"code": "SG", "name": "Singapore", "is_active": False, "default_currency": "SGD", "locale": "en-SG", "salary_parsing_config": {}, "location_parsing_config": {}, "aggregator_search_queries": {}},
]

BLOCKED_DOMAINS = [
    # SEEK
    ("seek.com.au", "Hard-blocked per project rules — SEEK is off-limits"),
    ("www.seek.com.au", "Hard-blocked per project rules — SEEK is off-limits"),
    ("talent.seek.com.au", "Hard-blocked per project rules — SEEK is off-limits"),
    ("employer.seek.com.au", "Hard-blocked per project rules — SEEK is off-limits"),
    # Jora
    ("jora.com", "Hard-blocked per project rules — Jora is off-limits"),
    ("au.jora.com", "Hard-blocked per project rules — Jora is off-limits"),
    ("www.jora.com", "Hard-blocked per project rules — Jora is off-limits"),
    # Jobstreet
    ("jobstreet.com", "Hard-blocked per project rules — Jobstreet is off-limits"),
    ("jobstreet.com.au", "Hard-blocked per project rules — Jobstreet is off-limits"),
    ("www.jobstreet.com", "Hard-blocked per project rules — Jobstreet is off-limits"),
    ("jobstreet.com.my", "Hard-blocked per project rules — Jobstreet is off-limits"),
    ("jobstreet.co.id", "Hard-blocked per project rules — Jobstreet is off-limits"),
    ("jobstreet.com.ph", "Hard-blocked per project rules — Jobstreet is off-limits"),
    ("jobstreet.com.sg", "Hard-blocked per project rules — Jobstreet is off-limits"),
    # JobsDB
    ("jobsdb.com", "Hard-blocked per project rules — JobsDB is off-limits"),
    ("www.jobsdb.com", "Hard-blocked per project rules — JobsDB is off-limits"),
    ("hk.jobsdb.com", "Hard-blocked per project rules — JobsDB is off-limits"),
    ("th.jobsdb.com", "Hard-blocked per project rules — JobsDB is off-limits"),
]

AGGREGATOR_SOURCES = [
    {"name": "Indeed AU", "base_url": "https://au.indeed.com", "market": "AU", "is_active": True, "purpose": "link_discovery_only"},
    {"name": "LinkedIn Jobs", "base_url": "https://www.linkedin.com/jobs", "market": "AU", "is_active": True, "purpose": "link_discovery_only"},
    {"name": "Glassdoor AU", "base_url": "https://www.glassdoor.com.au", "market": "AU", "is_active": True, "purpose": "link_discovery_only"},
    {"name": "CareerOne", "base_url": "https://www.careerone.com.au", "market": "AU", "is_active": True, "purpose": "link_discovery_only"},
    {"name": "Adzuna AU", "base_url": "https://www.adzuna.com.au", "market": "AU", "is_active": True, "purpose": "link_discovery_only"},
]

# 50+ Australian seed companies across industries and ATS platforms
AU_SEED_COMPANIES = [
    # ─── ASX-Listed Enterprises ────────────────────────────────────────────
    {"name": "BHP", "root_url": "https://www.bhp.com", "ats_platform": "workday", "industry": "mining"},
    {"name": "Rio Tinto", "root_url": "https://www.riotinto.com", "ats_platform": "workday", "industry": "mining"},
    {"name": "Telstra", "root_url": "https://www.telstra.com.au", "ats_platform": "workday", "industry": "telecommunications"},
    {"name": "Commonwealth Bank", "root_url": "https://www.commbank.com.au", "ats_platform": "workday", "industry": "banking"},
    {"name": "ANZ Bank", "root_url": "https://www.anz.com.au", "ats_platform": "taleo", "industry": "banking"},
    {"name": "Westpac", "root_url": "https://www.westpac.com.au", "ats_platform": "taleo", "industry": "banking"},
    {"name": "NAB", "root_url": "https://www.nab.com.au", "ats_platform": "workday", "industry": "banking"},
    {"name": "Woolworths Group", "root_url": "https://www.woolworthsgroup.com.au", "ats_platform": "workday", "industry": "retail"},
    {"name": "Coles Group", "root_url": "https://www.colesgroup.com.au", "ats_platform": "workday", "industry": "retail"},
    {"name": "Wesfarmers", "root_url": "https://www.wesfarmers.com.au", "ats_platform": "workday", "industry": "conglomerate"},
    {"name": "Macquarie Group", "root_url": "https://www.macquarie.com", "ats_platform": "workday", "industry": "finance"},
    {"name": "AMP", "root_url": "https://www.amp.com.au", "ats_platform": "taleo", "industry": "finance"},
    {"name": "Qantas", "root_url": "https://www.qantas.com", "ats_platform": "taleo", "industry": "aviation"},
    {"name": "Virgin Australia", "root_url": "https://www.virginaustralia.com", "ats_platform": "icims", "industry": "aviation"},
    {"name": "Medibank", "root_url": "https://www.medibank.com.au", "ats_platform": "workday", "industry": "health_insurance"},
    {"name": "Woodside Energy", "root_url": "https://www.woodside.com", "ats_platform": "taleo", "industry": "energy"},
    {"name": "Santos", "root_url": "https://www.santos.com", "ats_platform": "taleo", "industry": "energy"},
    {"name": "CSL Limited", "root_url": "https://www.csl.com", "ats_platform": "workday", "industry": "biotech"},
    {"name": "REA Group", "root_url": "https://www.rea-group.com", "ats_platform": "greenhouse", "industry": "proptech"},
    {"name": "Afterpay (Block)", "root_url": "https://www.afterpay.com", "ats_platform": "greenhouse", "industry": "fintech"},

    # ─── Tech Companies (Greenhouse / Lever / Ashby) ───────────────────────
    {"name": "Canva", "root_url": "https://www.canva.com", "ats_platform": "greenhouse", "industry": "tech"},
    {"name": "Atlassian", "root_url": "https://www.atlassian.com", "ats_platform": "greenhouse", "industry": "tech"},
    {"name": "Xero", "root_url": "https://www.xero.com", "ats_platform": "greenhouse", "industry": "tech"},
    {"name": "Safety Culture", "root_url": "https://www.safetyculture.com", "ats_platform": "greenhouse", "industry": "tech"},
    {"name": "Culture Amp", "root_url": "https://www.cultureamp.com", "ats_platform": "greenhouse", "industry": "tech"},
    {"name": "Buildkite", "root_url": "https://buildkite.com", "ats_platform": "ashby", "industry": "tech"},
    {"name": "MYOB", "root_url": "https://www.myob.com", "ats_platform": "lever", "industry": "tech"},
    {"name": "Finder", "root_url": "https://www.finder.com.au", "ats_platform": "greenhouse", "industry": "fintech"},
    {"name": "Zip Co", "root_url": "https://www.zip.co", "ats_platform": "greenhouse", "industry": "fintech"},
    {"name": "Prospa", "root_url": "https://www.prospa.com", "ats_platform": "lever", "industry": "fintech"},
    {"name": "Deputy", "root_url": "https://www.deputy.com", "ats_platform": "greenhouse", "industry": "tech"},
    {"name": "Employment Hero", "root_url": "https://www.employmenthero.com", "ats_platform": "greenhouse", "industry": "hrtech"},
    {"name": "Envato", "root_url": "https://www.envato.com", "ats_platform": "greenhouse", "industry": "tech"},
    {"name": "Car Next Door (Uber Carshare)", "root_url": "https://www.carnextdoor.com.au", "ats_platform": "lever", "industry": "mobility"},
    {"name": "Rokt", "root_url": "https://www.rokt.com", "ats_platform": "greenhouse", "industry": "tech"},

    # ─── Healthcare ────────────────────────────────────────────────────────
    {"name": "Ramsay Health Care", "root_url": "https://www.ramsayhealth.com", "ats_platform": "icims", "industry": "healthcare"},
    {"name": "Healthscope", "root_url": "https://www.healthscope.com.au", "ats_platform": "taleo", "industry": "healthcare"},
    {"name": "Australian Clinical Labs", "root_url": "https://www.clinicallabs.com.au", "ats_platform": "bamboohr", "industry": "healthcare"},
    {"name": "Primary Health Care", "root_url": "https://www.primaryhealthcare.com.au", "ats_platform": "taleo", "industry": "healthcare"},

    # ─── Government & Education ────────────────────────────────────────────
    {"name": "University of Sydney", "root_url": "https://www.sydney.edu.au", "ats_platform": "taleo", "industry": "education"},
    {"name": "University of Melbourne", "root_url": "https://www.unimelb.edu.au", "ats_platform": "pageup", "industry": "education"},
    {"name": "University of Queensland", "root_url": "https://www.uq.edu.au", "ats_platform": "pageup", "industry": "education"},
    {"name": "Australian Public Service Commission", "root_url": "https://www.apsc.gov.au", "ats_platform": "custom", "industry": "government"},

    # ─── Professional Services ─────────────────────────────────────────────
    {"name": "Deloitte Australia", "root_url": "https://www2.deloitte.com/au", "ats_platform": "workday", "industry": "professional_services"},
    {"name": "PwC Australia", "root_url": "https://www.pwc.com.au", "ats_platform": "workday", "industry": "professional_services"},
    {"name": "KPMG Australia", "root_url": "https://www.kpmg.com.au", "ats_platform": "taleo", "industry": "professional_services"},
    {"name": "EY Australia", "root_url": "https://www.ey.com/en_au", "ats_platform": "taleo", "industry": "professional_services"},

    # ─── Retail & Consumer ─────────────────────────────────────────────────
    {"name": "JB Hi-Fi", "root_url": "https://www.jbhifi.com.au", "ats_platform": "custom", "industry": "retail"},
    {"name": "Harvey Norman", "root_url": "https://www.harveynorman.com.au", "ats_platform": "custom", "industry": "retail"},
    {"name": "Bunnings Warehouse", "root_url": "https://www.bunnings.com.au", "ats_platform": "workday", "industry": "retail"},
    {"name": "Dan Murphy's", "root_url": "https://www.danmurphys.com.au", "ats_platform": "workday", "industry": "retail"},

    # ─── Brisbane-based ────────────────────────────────────────────────────
    {"name": "Suncorp Group", "root_url": "https://www.suncorp.com.au", "ats_platform": "taleo", "industry": "insurance"},
    {"name": "Flight Centre", "root_url": "https://www.flightcentre.com.au", "ats_platform": "smartrecruiters", "industry": "travel"},

    # ─── Perth-based ───────────────────────────────────────────────────────
    {"name": "Fortescue", "root_url": "https://www.fortescue.com", "ats_platform": "taleo", "industry": "mining"},
    {"name": "Mineral Resources", "root_url": "https://www.mineralresources.com.au", "ats_platform": "smartrecruiters", "industry": "mining"},
]


async def seed():
    async with AsyncSessionLocal() as db:
        print("🌱 Seeding database...")

        # Markets
        print("  Creating markets...")
        for m in MARKETS:
            await db.execute(text("""
                INSERT INTO markets (id, code, name, is_active, default_currency, locale,
                    salary_parsing_config, location_parsing_config, aggregator_search_queries)
                VALUES (:id, :code, :name, :is_active, :default_currency, :locale,
                    :salary::jsonb, :location::jsonb, :queries::jsonb)
                ON CONFLICT (code) DO NOTHING
            """), {
                "id": str(uuid.uuid4()), "code": m["code"], "name": m["name"],
                "is_active": m["is_active"], "default_currency": m["default_currency"],
                "locale": m["locale"],
                "salary": __import__("json").dumps(m["salary_parsing_config"]),
                "location": __import__("json").dumps(m["location_parsing_config"]),
                "queries": __import__("json").dumps(m["aggregator_search_queries"]),
            })

        # Blocked domains
        print("  Creating blocked domains...")
        for domain, reason in BLOCKED_DOMAINS:
            await db.execute(text("""
                INSERT INTO blocked_domains (id, domain, reason)
                VALUES (:id, :domain, :reason)
                ON CONFLICT (domain) DO NOTHING
            """), {"id": str(uuid.uuid4()), "domain": domain, "reason": reason})

        # Aggregator sources
        print("  Creating aggregator sources...")
        for src in AGGREGATOR_SOURCES:
            await db.execute(text("""
                INSERT INTO aggregator_sources (id, name, base_url, market, is_active, purpose)
                VALUES (:id, :name, :base_url, :market, :is_active, :purpose)
                ON CONFLICT DO NOTHING
            """), {"id": str(uuid.uuid4()), **src})

        # Seed companies
        print(f"  Creating {len(AU_SEED_COMPANIES)} seed companies...")
        from urllib.parse import urlparse
        for c in AU_SEED_COMPANIES:
            domain = urlparse(c["root_url"]).netloc.lstrip("www.")
            await db.execute(text("""
                INSERT INTO companies (id, name, domain, root_url, market_code, ats_platform,
                    crawl_priority, discovered_via, is_active)
                VALUES (:id, :name, :domain, :root_url, 'AU', :ats_platform, 5, 'seed', true)
                ON CONFLICT (domain) DO NOTHING
            """), {
                "id": str(uuid.uuid4()), "name": c["name"], "domain": domain,
                "root_url": c["root_url"], "ats_platform": c.get("ats_platform"),
            })

        await db.commit()
        print("✅ Seed complete!")


if __name__ == "__main__":
    asyncio.run(seed())
