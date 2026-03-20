"""Seed comprehensive aggregator sources list and add career_pages URL unique constraint.

Revision ID: 0020
Revises: 0019
Create Date: 2026-03-19
"""
from alembic import op

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None

SOURCES = [
    # AU market
    ("Indeed AU",       "https://au.indeed.com",          "AU", "link_discovery_only"),
    ("Careerjet AU",    "https://www.careerjet.com.au",   "AU", "link_discovery_only"),
    ("Adzuna AU",       "https://www.adzuna.com.au",      "AU", "link_discovery_only"),
    ("Talent.com AU",   "https://au.talent.com",          "AU", "link_discovery_only"),
    ("Jooble AU",       "https://au.jooble.org",          "AU", "link_discovery_only"),
    ("LinkedIn AU",     "https://www.linkedin.com/jobs",  "AU", "link_discovery_only"),
    ("Glassdoor AU",    "https://www.glassdoor.com.au",   "AU", "link_discovery_only"),
    ("WhatJobs AU",     "https://au.whatjobs.com",        "AU", "link_discovery_only"),
    # US market
    ("Indeed US",       "https://www.indeed.com",         "US", "link_discovery_only"),
    ("Careerjet US",    "https://www.careerjet.com",      "US", "link_discovery_only"),
    ("Adzuna US",       "https://www.adzuna.com",         "US", "link_discovery_only"),
    ("SimplyHired US",  "https://www.simplyhired.com",   "US", "link_discovery_only"),
    ("LinkedIn US",     "https://www.linkedin.com/jobs",  "US", "link_discovery_only"),
    ("Glassdoor US",    "https://www.glassdoor.com",      "US", "link_discovery_only"),
    ("WhatJobs US",     "https://us.whatjobs.com",        "US", "link_discovery_only"),
    # UK market
    ("Indeed UK",       "https://uk.indeed.com",          "UK", "link_discovery_only"),
    ("Careerjet UK",    "https://www.careerjet.co.uk",   "UK", "link_discovery_only"),
    ("Adzuna UK",       "https://www.adzuna.co.uk",       "UK", "link_discovery_only"),
    ("SimplyHired UK",  "https://www.simplyhired.co.uk", "UK", "link_discovery_only"),
    ("LinkedIn UK",     "https://www.linkedin.com/jobs",  "UK", "link_discovery_only"),
    ("Glassdoor UK",    "https://www.glassdoor.co.uk",   "UK", "link_discovery_only"),
    ("WhatJobs UK",     "https://www.whatjobs.com",       "UK", "link_discovery_only"),
]


def upgrade() -> None:
    # Add unique constraint on name so ON CONFLICT works
    op.execute("ALTER TABLE aggregator_sources ADD CONSTRAINT uq_aggregator_sources_name UNIQUE (name)")

    for name, url, market, purpose in SOURCES:
        op.execute(f"""
            INSERT INTO aggregator_sources (id, name, base_url, market, purpose, is_active)
            VALUES (gen_random_uuid(), '{name}', '{url}', '{market}', '{purpose}', true)
            ON CONFLICT (name) DO NOTHING
        """)

    # Seed new sources into discovery queue
    op.execute("""
        INSERT INTO run_queue (queue_type, item_id, item_type, added_by)
        SELECT 'discovery', id, 'aggregator_source', 'migration_0020'
        FROM aggregator_sources
        WHERE is_active = true
        ON CONFLICT DO NOTHING
    """)


def downgrade() -> None:
    pass
