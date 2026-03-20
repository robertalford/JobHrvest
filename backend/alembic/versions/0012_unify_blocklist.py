"""Unify blocked-domains and excluded-sites into a single excluded_sites table.

Revision ID: 0012
Revises: 0011
Create Date: 2026-03-19

Changes:
- Inserts the four hardcoded off-limits domains (SEEK, Jora, JobsDB, JobStreet) into excluded_sites
- Migrates any rows from blocked_domains_config into excluded_sites
- Drops the now-redundant blocked_domains_config and blocked_domains tables
"""
from alembic import op
import sqlalchemy as sa

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None

# Hard-blocked brands that must never be crawled — seeded once into excluded_sites
_HARDCODED = [
    ("seek.com.au",    "SEEK Australia", "https://www.seek.com.au", "job_board", "AU"),
    ("jora.com",       "Jora",           "https://www.jora.com",    "job_board", None),
    ("jobsdb.com",     "JobsDB",         "https://www.jobsdb.com",  "job_board", None),
    ("jobstreet.com",  "JobStreet",      "https://www.jobstreet.com","job_board", None),
]


def upgrade():
    conn = op.get_bind()

    # 1. Seed the hardcoded off-limits domains
    for domain, company_name, site_url, site_type, country_code in _HARDCODED:
        conn.execute(sa.text("""
            INSERT INTO excluded_sites
                (id, domain, company_name, site_url, site_type, country_code, reason)
            VALUES
                (gen_random_uuid(), :domain, :company_name, :site_url,
                 :site_type, :country_code, 'hardcoded_block')
            ON CONFLICT (domain) DO NOTHING
        """), {
            "domain": domain,
            "company_name": company_name,
            "site_url": site_url,
            "site_type": site_type,
            "country_code": country_code,
        })

    # 2. Migrate blocked_domains_config → excluded_sites (ignore if table absent)
    try:
        rows = conn.execute(sa.text(
            "SELECT domain, reason FROM blocked_domains_config WHERE is_active = true"
        )).fetchall()
        for row in rows:
            conn.execute(sa.text("""
                INSERT INTO excluded_sites (id, domain, reason)
                VALUES (gen_random_uuid(), :domain, :reason)
                ON CONFLICT (domain) DO NOTHING
            """), {"domain": row[0], "reason": row[1] or "manual"})
    except Exception:
        pass  # Table may not exist in some environments

    # 3. Drop obsolete tables
    for tbl in ("blocked_domains_config", "blocked_domains"):
        try:
            op.drop_table(tbl)
        except Exception:
            pass  # Already absent — nothing to do


def downgrade():
    # Recreate blocked_domains_config so the migration is reversible
    op.create_table(
        "blocked_domains_config",
        sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("domain", sa.Text(), nullable=False, unique=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
