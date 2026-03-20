"""Seed market configuration rows from markets.py.

Revision ID: 0010
Revises: 0009
Create Date: 2026-03-19
"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime, timezone

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None

MARKETS = [
    {
        "code": "AU", "name": "Australia", "locale": "en-AU",
        "default_currency": "AUD", "is_active": True,
        "aggregator_search_queries": {
            "indeed": [
                "software engineer", "data analyst", "product manager",
                "marketing manager", "finance manager", "operations manager",
                "sales manager", "engineer", "developer", "designer",
                "nurse", "teacher", "accountant", "project manager",
                "business analyst", "UX designer", "DevOps",
            ]
        },
        "salary_parsing_config": {"currency": "AUD", "period_default": "annual"},
        "location_parsing_config": {
            "major_cities": [
                "Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide",
                "Canberra", "Darwin", "Hobart", "Gold Coast", "Newcastle",
            ],
            "state_abbreviations": {
                "NSW": "New South Wales", "VIC": "Victoria", "QLD": "Queensland",
                "WA": "Western Australia", "SA": "South Australia",
                "TAS": "Tasmania", "ACT": "Australian Capital Territory",
                "NT": "Northern Territory",
            },
        },
    },
    {
        "code": "NZ", "name": "New Zealand", "locale": "en-NZ",
        "default_currency": "NZD", "is_active": False,
        "aggregator_search_queries": {"indeed": ["software engineer", "developer", "project manager"]},
        "salary_parsing_config": {"currency": "NZD", "period_default": "annual"},
        "location_parsing_config": {"major_cities": ["Auckland", "Wellington", "Christchurch"]},
    },
    {
        "code": "SG", "name": "Singapore", "locale": "en-SG",
        "default_currency": "SGD", "is_active": False,
        "aggregator_search_queries": {"indeed": ["software engineer", "finance", "marketing"]},
        "salary_parsing_config": {"currency": "SGD", "period_default": "monthly"},
        "location_parsing_config": {"major_cities": ["Singapore"]},
    },
    {
        "code": "MY", "name": "Malaysia", "locale": "en-MY",
        "default_currency": "MYR", "is_active": False,
        "aggregator_search_queries": {"indeed": ["software engineer", "finance", "marketing"]},
        "salary_parsing_config": {"currency": "MYR", "period_default": "monthly"},
        "location_parsing_config": {"major_cities": ["Kuala Lumpur", "Petaling Jaya", "Penang"]},
    },
    {
        "code": "HK", "name": "Hong Kong", "locale": "en-HK",
        "default_currency": "HKD", "is_active": False,
        "aggregator_search_queries": {"indeed": ["software engineer", "finance", "marketing"]},
        "salary_parsing_config": {"currency": "HKD", "period_default": "monthly"},
        "location_parsing_config": {"major_cities": ["Hong Kong", "Kowloon", "Central"]},
    },
    {
        "code": "PH", "name": "Philippines", "locale": "en-PH",
        "default_currency": "PHP", "is_active": False,
        "aggregator_search_queries": {"indeed": ["software engineer", "BPO", "call center"]},
        "salary_parsing_config": {"currency": "PHP", "period_default": "monthly"},
        "location_parsing_config": {"major_cities": ["Manila", "Makati", "Cebu City"]},
    },
    {
        "code": "ID", "name": "Indonesia", "locale": "id-ID",
        "default_currency": "IDR", "is_active": False,
        "aggregator_search_queries": {"indeed": ["software engineer", "IT", "marketing"]},
        "salary_parsing_config": {"currency": "IDR", "period_default": "monthly"},
        "location_parsing_config": {"major_cities": ["Jakarta", "Surabaya", "Bandung"]},
    },
    {
        "code": "TH", "name": "Thailand", "locale": "th-TH",
        "default_currency": "THB", "is_active": False,
        "aggregator_search_queries": {"indeed": ["software engineer", "IT", "marketing"]},
        "salary_parsing_config": {"currency": "THB", "period_default": "monthly"},
        "location_parsing_config": {"major_cities": ["Bangkok", "Chiang Mai", "Phuket"]},
    },
]


def upgrade():
    now = datetime.now(timezone.utc).isoformat()
    conn = op.get_bind()
    for m in MARKETS:
        existing = conn.execute(
            sa.text("SELECT code FROM markets WHERE code = :code"),
            {"code": m["code"]},
        ).fetchone()
        if existing:
            continue
        import json
        conn.execute(
            sa.text("""
                INSERT INTO markets (id, code, name, locale, default_currency, is_active,
                    aggregator_search_queries, salary_parsing_config, location_parsing_config,
                    created_at, updated_at)
                VALUES (gen_random_uuid(), :code, :name, :locale, :currency, :active,
                    :agg::jsonb, :sal::jsonb, :loc::jsonb, :now, :now)
            """),
            {
                "code": m["code"],
                "name": m["name"],
                "locale": m["locale"],
                "currency": m["default_currency"],
                "active": m["is_active"],
                "agg": json.dumps(m["aggregator_search_queries"]),
                "sal": json.dumps(m["salary_parsing_config"]),
                "loc": json.dumps(m["location_parsing_config"]),
                "now": now,
            },
        )


def downgrade():
    codes = [m["code"] for m in MARKETS]
    op.execute(sa.text(f"DELETE FROM markets WHERE code = ANY(ARRAY{codes})"))
