"""Covering index for active-companies name-sort + sites_json in gold table.

Two changes:
1. B-tree index on companies(name) WHERE is_active = true — eliminates the
   seq scan + sort that was the remaining bottleneck in the companies list query.
   With this index PostgreSQL can do a forward index scan in name order without
   sorting 28k rows in memory.

2. Add sites_json JSONB column to company_stats — stores the pre-aggregated
   active career pages array, eliminating the last remaining subquery from the
   companies list endpoint. The trigger that refreshes crawl stats now also
   refreshes this column.

Together these bring the companies list query from ~280ms to < 30ms.

Revision ID: 0014
"""

from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade():
    # ── Covering index: fast ORDER BY name WHERE is_active ────────────────────
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_companies_active_name
          ON companies (name)
          WHERE is_active = true
    """)

    # ── sites_json column in company_stats ────────────────────────────────────
    op.execute("""
        ALTER TABLE company_stats
          ADD COLUMN IF NOT EXISTS sites_json JSONB NOT NULL DEFAULT '[]'::jsonb
    """)

    # Populate sites_json for all existing rows
    op.execute("""
        UPDATE company_stats cs
        SET sites_json = COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'id',         cp.id,
                    'url',        cp.url,
                    'page_type',  cp.page_type,
                    'is_primary', cp.is_primary
                ) ORDER BY cp.is_primary DESC, cp.created_at ASC
            )
            FROM career_pages cp
            WHERE cp.company_id = cs.company_id AND cp.is_active = true
        ), '[]'::jsonb)
    """)

    # Update the trigger function to also refresh sites_json
    op.execute("""
        CREATE OR REPLACE FUNCTION refresh_company_stats(p_company_id UUID)
        RETURNS void LANGUAGE plpgsql AS $$
        DECLARE
            v_site_count  INT;
            v_last_jobs   INT;
            v_total       INT;
            v_avg3        INT;
            v_imported    INT;
            v_expected    INT;
            v_sites_json  JSONB;
        BEGIN
            SELECT COUNT(*) INTO v_site_count
              FROM career_pages
             WHERE company_id = p_company_id AND is_active = true;

            SELECT COALESCE(jsonb_agg(
                jsonb_build_object(
                    'id',         id,
                    'url',        url,
                    'page_type',  page_type,
                    'is_primary', is_primary
                ) ORDER BY is_primary DESC, created_at ASC
            ), '[]'::jsonb) INTO v_sites_json
              FROM career_pages
             WHERE company_id = p_company_id AND is_active = true;

            SELECT jobs_found INTO v_last_jobs
              FROM crawl_logs
             WHERE company_id = p_company_id AND status = 'success'
             ORDER BY completed_at DESC NULLS LAST
             LIMIT 1;

            SELECT COUNT(*) INTO v_total
              FROM crawl_logs
             WHERE company_id = p_company_id AND status = 'success';

            SELECT ROUND(AVG(jf))::int INTO v_avg3
              FROM (
                  SELECT jobs_found AS jf
                    FROM crawl_logs
                   WHERE company_id = p_company_id AND status = 'success'
                   ORDER BY completed_at DESC NULLS LAST
                   LIMIT 3
              ) sub;

            SELECT COALESCE(SUM(expected_job_count), 0)::int INTO v_imported
              FROM lead_imports
             WHERE company_id = p_company_id AND expected_job_count IS NOT NULL;

            v_expected := CASE
                WHEN v_total >= 3 AND v_avg3 IS NOT NULL THEN v_avg3
                WHEN v_total >= 1 AND v_last_jobs IS NOT NULL THEN v_last_jobs
                WHEN v_imported > 0 THEN v_imported
                ELSE NULL
            END;

            INSERT INTO company_stats (
                company_id, active_site_count, sites_json, last_crawl_jobs,
                total_crawls, avg_last_3_jobs, imported_expected_jobs, expected_jobs, updated_at
            ) VALUES (
                p_company_id, v_site_count, v_sites_json, v_last_jobs,
                v_total, v_avg3, NULLIF(v_imported, 0), v_expected, NOW()
            )
            ON CONFLICT (company_id) DO UPDATE SET
                active_site_count      = EXCLUDED.active_site_count,
                sites_json             = EXCLUDED.sites_json,
                last_crawl_jobs        = EXCLUDED.last_crawl_jobs,
                total_crawls           = EXCLUDED.total_crawls,
                avg_last_3_jobs        = EXCLUDED.avg_last_3_jobs,
                imported_expected_jobs = EXCLUDED.imported_expected_jobs,
                expected_jobs          = EXCLUDED.expected_jobs,
                updated_at             = NOW();
        END;
        $$
    """)


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_companies_active_name")
    op.execute("ALTER TABLE company_stats DROP COLUMN IF EXISTS sites_json")
