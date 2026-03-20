"""
Market configuration — defines per-market settings for crawling, salary parsing,
location parsing, and aggregator queries.

Add a new market by appending a MarketConfig entry to MARKETS. The system will
automatically pick up new markets for scheduling, harvesting, and parsing.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AggregatorConfig:
    """Per-market aggregator search configuration."""
    name: str                          # "Indeed AU"
    base_url: str                      # "https://au.indeed.com"
    search_queries: list[str]          # queries to run
    location_param: str = ""          # location filter value
    max_pages_per_query: int = 5       # pages to harvest per query
    enabled: bool = True


@dataclass
class MarketConfig:
    code: str                          # "AU", "US", "UK", "SG", etc.
    name: str                          # "Australia"
    locale: str                        # "en-AU"
    default_currency: str              # "AUD"
    timezone: str                      # "Australia/Sydney"
    is_active: bool = True

    # Aggregator sources for link discovery
    aggregators: list[AggregatorConfig] = field(default_factory=list)

    # Salary parsing config
    salary_currency_symbols: list[str] = field(default_factory=lambda: ["$"])
    salary_period_default: str = "annual"   # annual, monthly, hourly

    # Location parsing: major cities for heuristic scanning
    major_cities: list[str] = field(default_factory=list)
    state_abbreviations: dict[str, str] = field(default_factory=dict)  # abbr → full name

    # Seed company domains to start crawling
    seed_domains: list[str] = field(default_factory=list)


# ── Market definitions ────────────────────────────────────────────────────────

MARKETS: dict[str, MarketConfig] = {

    "AU": MarketConfig(
        code="AU",
        name="Australia",
        locale="en-AU",
        default_currency="AUD",
        timezone="Australia/Sydney",
        aggregators=[
            AggregatorConfig(
                name="Indeed AU",
                base_url="https://au.indeed.com",
                location_param="Australia",
                search_queries=[
                    "software engineer", "data analyst", "product manager",
                    "marketing manager", "finance manager", "operations manager",
                    "sales manager", "engineer", "developer", "designer",
                    "nurse", "teacher", "accountant", "project manager",
                    "business analyst", "UX designer", "DevOps",
                ],
            ),
            AggregatorConfig(
                name="LinkedIn Jobs",
                base_url="https://www.linkedin.com/jobs",
                location_param="Australia",
                search_queries=[
                    "software engineer Australia", "product manager Australia",
                    "marketing Australia", "finance Australia",
                ],
                max_pages_per_query=3,
            ),
        ],
        salary_currency_symbols=["$", "A$", "AUD"],
        salary_period_default="annual",
        major_cities=[
            "Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
            "Canberra", "Darwin", "Hobart", "Gold Coast", "Newcastle",
            "Wollongong", "Geelong", "Townsville", "Cairns", "Toowoomba",
            "Ballarat", "Bendigo", "Albury", "Launceston", "Mackay",
        ],
        state_abbreviations={
            "NSW": "New South Wales",
            "VIC": "Victoria",
            "QLD": "Queensland",
            "WA": "Western Australia",
            "SA": "South Australia",
            "TAS": "Tasmania",
            "ACT": "Australian Capital Territory",
            "NT": "Northern Territory",
        },
        seed_domains=[
            "atlassian.com", "canva.com", "afterpay.com", "xero.com",
            "seek.com.au", "carsales.com.au", "realestate.com.au",
            "telstra.com.au", "commbank.com.au", "anz.com.au",
            "westpac.com.au", "nab.com.au", "macquarie.com",
            "bhp.com", "rio.com", "wesfarmers.com.au", "woolworths.com.au",
            "coles.com.au", "qantas.com", "virginaustralia.com",
            "deloitte.com.au", "pwc.com.au", "kpmg.com.au", "ey.com/en_au",
        ],
    ),

    "NZ": MarketConfig(
        code="NZ",
        name="New Zealand",
        locale="en-NZ",
        default_currency="NZD",
        timezone="Pacific/Auckland",
        is_active=False,  # Enable when ready
        aggregators=[
            AggregatorConfig(
                name="Indeed NZ",
                base_url="https://nz.indeed.com",
                location_param="New Zealand",
                search_queries=[
                    "software engineer", "developer", "project manager",
                    "marketing", "finance", "operations",
                ],
            ),
        ],
        salary_currency_symbols=["$", "NZ$", "NZD"],
        major_cities=[
            "Auckland", "Wellington", "Christchurch", "Hamilton",
            "Tauranga", "Dunedin", "Palmerston North", "Napier", "Nelson",
        ],
        state_abbreviations={},
    ),

    "SG": MarketConfig(
        code="SG",
        name="Singapore",
        locale="en-SG",
        default_currency="SGD",
        timezone="Asia/Singapore",
        is_active=False,
        aggregators=[
            AggregatorConfig(
                name="Indeed SG",
                base_url="https://sg.indeed.com",
                location_param="Singapore",
                search_queries=[
                    "software engineer", "data analyst", "product manager",
                    "finance", "marketing", "operations",
                ],
            ),
        ],
        salary_currency_symbols=["S$", "SGD", "$"],
        salary_period_default="monthly",
        major_cities=["Singapore"],
        state_abbreviations={},
    ),

    "MY": MarketConfig(
        code="MY",
        name="Malaysia",
        locale="en-MY",
        default_currency="MYR",
        timezone="Asia/Kuala_Lumpur",
        is_active=False,
        aggregators=[
            AggregatorConfig(
                name="Indeed MY",
                base_url="https://my.indeed.com",
                location_param="Malaysia",
                search_queries=[
                    "software engineer", "developer", "project manager",
                    "finance", "marketing",
                ],
            ),
        ],
        salary_currency_symbols=["RM", "MYR"],
        salary_period_default="monthly",
        major_cities=["Kuala Lumpur", "Petaling Jaya", "Johor Bahru", "Penang", "Kota Kinabalu"],
        state_abbreviations={},
    ),

    "HK": MarketConfig(
        code="HK",
        name="Hong Kong",
        locale="en-HK",
        default_currency="HKD",
        timezone="Asia/Hong_Kong",
        is_active=False,
        aggregators=[
            AggregatorConfig(
                name="Indeed HK",
                base_url="https://hk.indeed.com",
                location_param="Hong Kong",
                search_queries=[
                    "software engineer", "finance", "marketing", "operations",
                ],
            ),
        ],
        salary_currency_symbols=["HK$", "HKD"],
        salary_period_default="monthly",
        major_cities=["Hong Kong", "Kowloon", "New Territories", "Central", "Wan Chai"],
        state_abbreviations={},
    ),

    "PH": MarketConfig(
        code="PH",
        name="Philippines",
        locale="en-PH",
        default_currency="PHP",
        timezone="Asia/Manila",
        is_active=False,
        aggregators=[
            AggregatorConfig(
                name="Indeed PH",
                base_url="https://ph.indeed.com",
                location_param="Philippines",
                search_queries=[
                    "software engineer", "BPO", "call center", "IT support",
                    "accounting", "marketing",
                ],
            ),
        ],
        salary_currency_symbols=["₱", "PHP"],
        salary_period_default="monthly",
        major_cities=["Manila", "Makati", "Quezon City", "Cebu City", "Davao", "Taguig", "BGC"],
        state_abbreviations={},
    ),

    "ID": MarketConfig(
        code="ID",
        name="Indonesia",
        locale="id-ID",
        default_currency="IDR",
        timezone="Asia/Jakarta",
        is_active=False,
        aggregators=[
            AggregatorConfig(
                name="Indeed ID",
                base_url="https://id.indeed.com",
                location_param="Indonesia",
                search_queries=[
                    "software engineer", "IT", "marketing", "finance",
                    "operations", "HRD",
                ],
            ),
        ],
        salary_currency_symbols=["Rp", "IDR"],
        salary_period_default="monthly",
        major_cities=["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Bali", "Denpasar"],
        state_abbreviations={},
    ),

    "TH": MarketConfig(
        code="TH",
        name="Thailand",
        locale="th-TH",
        default_currency="THB",
        timezone="Asia/Bangkok",
        is_active=False,
        aggregators=[
            AggregatorConfig(
                name="Indeed TH",
                base_url="https://th.indeed.com",
                location_param="Thailand",
                search_queries=[
                    "software engineer", "IT", "marketing", "finance",
                    "operations",
                ],
            ),
        ],
        salary_currency_symbols=["฿", "THB"],
        salary_period_default="monthly",
        major_cities=["Bangkok", "Chiang Mai", "Pattaya", "Phuket", "Khon Kaen"],
        state_abbreviations={},
    ),

}


def get_market(code: str) -> Optional[MarketConfig]:
    """Get market config by code (case-insensitive)."""
    return MARKETS.get(code.upper())


def get_active_markets() -> list[MarketConfig]:
    """Return all active market configs."""
    return [m for m in MARKETS.values() if m.is_active]


def get_all_markets() -> list[MarketConfig]:
    return list(MARKETS.values())
