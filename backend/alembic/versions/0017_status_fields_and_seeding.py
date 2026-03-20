"""Add company_status and site_status fields; seed excluded domains and discovery sources.

Companies get 4 statuses:
  1. ok              — Sites identified, working as expected
  2. at_risk         — Reduced sites/jobs, needs attention
  3. no_sites_new    — No sites discovered yet (new company)
  4. no_sites_broken — Had sites before, now broken

Sites (career_pages) get 4 statuses:
  1. ok                  — Job listing structure mapped
  2. at_risk             — Reduced job information
  3. no_structure_new    — New, no structure mapped yet
  4. no_structure_broken — Had structure before, now broken

Also seeds:
  - Excluded domains: ricebowl.my, seek.com.au, jobsdb.com, jobstreet.com, jora.com
  - Discovery sources: indeed.com.au, linkedin.com (if not present)

Revision ID: 0017
"""

from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade():
    # ── Company status ────────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE companies
          ADD COLUMN IF NOT EXISTS company_status TEXT NOT NULL DEFAULT 'no_sites_new'
    """)

    # Derive initial status from existing data
    op.execute("""
        UPDATE companies c SET company_status = CASE
            -- Has active sites AND crawled within 7 days → OK
            WHEN EXISTS (
                SELECT 1 FROM career_pages cp
                WHERE cp.company_id = c.id AND cp.is_active = true
            ) AND c.last_crawl_at >= NOW() - INTERVAL '7 days'
            THEN 'ok'

            -- Has active sites but not crawled recently → at_risk
            WHEN EXISTS (
                SELECT 1 FROM career_pages cp
                WHERE cp.company_id = c.id AND cp.is_active = true
            )
            THEN 'at_risk'

            -- No sites AND has been crawled before → broken
            WHEN c.last_crawl_at IS NOT NULL
            THEN 'no_sites_broken'

            -- No sites, never crawled → new
            ELSE 'no_sites_new'
        END
    """)

    # ── Site (career_page) status ─────────────────────────────────────────────
    op.execute("""
        ALTER TABLE career_pages
          ADD COLUMN IF NOT EXISTS site_status TEXT NOT NULL DEFAULT 'no_structure_new'
    """)

    # Derive initial status from existing template + extraction data
    op.execute("""
        UPDATE career_pages cp SET site_status = CASE
            -- Has active template AND extracted jobs within 7 days → OK
            WHEN EXISTS (
                SELECT 1 FROM site_templates st
                WHERE st.career_page_id = cp.id AND st.is_active = true
            ) AND cp.last_extraction_at >= NOW() - INTERVAL '7 days'
            THEN 'ok'

            -- Has active template but stale extraction → at_risk
            WHEN EXISTS (
                SELECT 1 FROM site_templates st
                WHERE st.career_page_id = cp.id AND st.is_active = true
            )
            THEN 'at_risk'

            -- No template AND has been extracted before → broken
            WHEN cp.last_extraction_at IS NOT NULL
            THEN 'no_structure_broken'

            -- No template, never extracted → new
            ELSE 'no_structure_new'
        END
    """)

    # ── Seed excluded domains (hard blocklist required by spec) ───────────────
    op.execute("""
        INSERT INTO excluded_sites (id, domain, company_name, site_type, reason, source_file)
        VALUES
          (gen_random_uuid(), 'ricebowl.my',      'RiceBowl',       'job_board', 'Off-limits aggregator per spec', 'system'),
          (gen_random_uuid(), 'seek.com.au',       'SEEK',            'job_board', 'Off-limits aggregator per spec', 'system'),
          (gen_random_uuid(), 'jobsdb.com',        'JobsDB',          'job_board', 'Off-limits aggregator per spec', 'system'),
          (gen_random_uuid(), 'jobstreet.com',     'JobStreet',       'job_board', 'Off-limits aggregator per spec', 'system'),
          (gen_random_uuid(), 'jora.com',          'Jora',            'job_board', 'Off-limits aggregator per spec', 'system')
        ON CONFLICT (domain) DO NOTHING
    """)

    # ── Seed aggregator discovery sources ─────────────────────────────────────
    op.execute("""
        INSERT INTO aggregator_sources (id, name, base_url, market, is_active, purpose)
        VALUES
          (gen_random_uuid(), 'Indeed AU', 'https://au.indeed.com', 'AU', true, 'link_discovery_only'),
          (gen_random_uuid(), 'LinkedIn',  'https://www.linkedin.com', 'AU', true, 'link_discovery_only')
        ON CONFLICT DO NOTHING
    """)

    # ── Index for status-based filtering ─────────────────────────────────────
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_companies_status
          ON companies (company_status)
          WHERE is_active = true
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_career_pages_status
          ON career_pages (site_status)
          WHERE is_active = true
    """)

    op.execute("ANALYZE companies")
    op.execute("ANALYZE career_pages")


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_companies_status")
    op.execute("DROP INDEX IF EXISTS ix_career_pages_status")
    op.execute("ALTER TABLE companies DROP COLUMN IF EXISTS company_status")
    op.execute("ALTER TABLE career_pages DROP COLUMN IF EXISTS site_status")
