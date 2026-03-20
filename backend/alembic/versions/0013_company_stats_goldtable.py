"""Pre-aggregated company_stats gold table for fast list queries.

Eliminates 6 correlated subqueries per row on the Companies and Career Pages
list endpoints by maintaining a single row per company with pre-computed counts.

Performance impact:
  Before: ~6N subquery executions per page load (300+ for page_size=50)
  After:  One LEFT JOIN, O(1) per row

Updated by a PostgreSQL trigger on:
  - crawl_logs INSERT/UPDATE (recalculates crawl stats)
  - career_pages INSERT/UPDATE/DELETE (recalculates site_count)
  - lead_imports INSERT/UPDATE/DELETE (recalculates imported_expected_jobs)

Also adds:
  - pg_trgm extension + trigram index on companies.name for fast ILIKE search
  - Index on crawl_logs.company_id for the trigger refresh queries
  - Trigram index on jobs.title for fast ILIKE job search

Revision ID: 0013
"""

from alembic import op
import sqlalchemy as sa

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade():
    # ── pg_trgm for fast ILIKE / trigram search ───────────────────────────────
    # Guard: pg_trgm may already be installed by a previous migration
    op.execute("""
        DO $$ BEGIN
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)

    # ── Trigram indexes for ILIKE search ──────────────────────────────────────
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_companies_name_trgm
          ON companies USING GIN (name gin_trgm_ops)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_companies_domain_trgm
          ON companies USING GIN (domain gin_trgm_ops)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_jobs_title_trgm
          ON jobs USING GIN (title gin_trgm_ops)
          WHERE is_active = true
    """)

    # ── Index on crawl_logs.company_id (used by the trigger refresh) ──────────
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_crawl_logs_company_id
          ON crawl_logs (company_id)
    """)

    # ── company_stats gold table ───────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS company_stats (
            company_id            UUID        PRIMARY KEY
                                              REFERENCES companies(id) ON DELETE CASCADE,
            active_site_count     INT         NOT NULL DEFAULT 0,
            last_crawl_jobs       INT,
            total_crawls          INT         NOT NULL DEFAULT 0,
            avg_last_3_jobs       INT,
            imported_expected_jobs INT,
            expected_jobs         INT,
            updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # ── Populate initial data ──────────────────────────────────────────────────
    op.execute("""
        INSERT INTO company_stats (
            company_id,
            active_site_count,
            last_crawl_jobs,
            total_crawls,
            avg_last_3_jobs,
            imported_expected_jobs,
            expected_jobs,
            updated_at
        )
        SELECT
            c.id,

            COALESCE((
                SELECT COUNT(*) FROM career_pages cp
                 WHERE cp.company_id = c.id AND cp.is_active = true
            ), 0)::int,

            (SELECT cl.jobs_found FROM crawl_logs cl
              WHERE cl.company_id = c.id AND cl.status = 'success'
              ORDER BY cl.completed_at DESC NULLS LAST LIMIT 1),

            COALESCE((
                SELECT COUNT(*) FROM crawl_logs cl
                 WHERE cl.company_id = c.id AND cl.status = 'success'
            ), 0)::int,

            (SELECT ROUND(AVG(jf))::int FROM (
                SELECT cl.jobs_found AS jf FROM crawl_logs cl
                 WHERE cl.company_id = c.id AND cl.status = 'success'
                 ORDER BY cl.completed_at DESC NULLS LAST LIMIT 3
            ) sub),

            (SELECT COALESCE(SUM(li.expected_job_count), 0)::int
               FROM lead_imports li
              WHERE li.company_id = c.id AND li.expected_job_count IS NOT NULL),

            NULL,  -- expected_jobs computed below

            NOW()
        FROM companies c
        ON CONFLICT (company_id) DO NOTHING
    """)

    # Compute expected_jobs from the populated data
    op.execute("""
        UPDATE company_stats SET
            expected_jobs = CASE
                WHEN total_crawls >= 3 AND avg_last_3_jobs IS NOT NULL THEN avg_last_3_jobs
                WHEN total_crawls >= 1 AND last_crawl_jobs IS NOT NULL THEN last_crawl_jobs
                WHEN imported_expected_jobs IS NOT NULL AND imported_expected_jobs > 0
                    THEN imported_expected_jobs
                ELSE NULL
            END
    """)

    # ── Trigger function ───────────────────────────────────────────────────────
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
        BEGIN
            SELECT COUNT(*) INTO v_site_count
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
                company_id, active_site_count, last_crawl_jobs, total_crawls,
                avg_last_3_jobs, imported_expected_jobs, expected_jobs, updated_at
            ) VALUES (
                p_company_id, v_site_count, v_last_jobs, v_total,
                v_avg3, NULLIF(v_imported, 0), v_expected, NOW()
            )
            ON CONFLICT (company_id) DO UPDATE SET
                active_site_count     = EXCLUDED.active_site_count,
                last_crawl_jobs       = EXCLUDED.last_crawl_jobs,
                total_crawls          = EXCLUDED.total_crawls,
                avg_last_3_jobs       = EXCLUDED.avg_last_3_jobs,
                imported_expected_jobs = EXCLUDED.imported_expected_jobs,
                expected_jobs         = EXCLUDED.expected_jobs,
                updated_at            = NOW();
        END;
        $$
    """)

    # Trigger on crawl_logs
    op.execute("""
        CREATE OR REPLACE FUNCTION trg_crawl_logs_stats()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                PERFORM refresh_company_stats(OLD.company_id);
            ELSE
                PERFORM refresh_company_stats(NEW.company_id);
            END IF;
            RETURN NULL;
        END;
        $$
    """)
    op.execute("""
        DROP TRIGGER IF EXISTS trg_crawl_logs_stats ON crawl_logs
    """)
    op.execute("""
        CREATE TRIGGER trg_crawl_logs_stats
        AFTER INSERT OR UPDATE OR DELETE ON crawl_logs
        FOR EACH ROW EXECUTE FUNCTION trg_crawl_logs_stats()
    """)

    # Trigger on career_pages (site_count)
    op.execute("""
        CREATE OR REPLACE FUNCTION trg_career_pages_stats()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                PERFORM refresh_company_stats(OLD.company_id);
            ELSE
                PERFORM refresh_company_stats(NEW.company_id);
            END IF;
            RETURN NULL;
        END;
        $$
    """)
    op.execute("""
        DROP TRIGGER IF EXISTS trg_career_pages_stats ON career_pages
    """)
    op.execute("""
        CREATE TRIGGER trg_career_pages_stats
        AFTER INSERT OR UPDATE OR DELETE ON career_pages
        FOR EACH ROW EXECUTE FUNCTION trg_career_pages_stats()
    """)

    # Trigger on lead_imports (imported_expected_jobs)
    op.execute("""
        CREATE OR REPLACE FUNCTION trg_lead_imports_stats()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                PERFORM refresh_company_stats(OLD.company_id);
            ELSIF NEW.company_id IS NOT NULL THEN
                PERFORM refresh_company_stats(NEW.company_id);
            END IF;
            RETURN NULL;
        END;
        $$
    """)
    op.execute("""
        DROP TRIGGER IF EXISTS trg_lead_imports_stats ON lead_imports
    """)
    op.execute("""
        CREATE TRIGGER trg_lead_imports_stats
        AFTER INSERT OR UPDATE OR DELETE ON lead_imports
        FOR EACH ROW EXECUTE FUNCTION trg_lead_imports_stats()
    """)


def downgrade():
    op.execute("DROP TRIGGER IF EXISTS trg_lead_imports_stats ON lead_imports")
    op.execute("DROP TRIGGER IF EXISTS trg_career_pages_stats ON career_pages")
    op.execute("DROP TRIGGER IF EXISTS trg_crawl_logs_stats ON crawl_logs")
    op.execute("DROP FUNCTION IF EXISTS trg_lead_imports_stats()")
    op.execute("DROP FUNCTION IF EXISTS trg_career_pages_stats()")
    op.execute("DROP FUNCTION IF EXISTS trg_crawl_logs_stats()")
    op.execute("DROP FUNCTION IF EXISTS refresh_company_stats(UUID)")
    op.execute("DROP TABLE IF EXISTS company_stats")
    op.execute("DROP INDEX IF EXISTS ix_crawl_logs_company_id")
    op.execute("DROP INDEX IF EXISTS ix_jobs_title_trgm")
    op.execute("DROP INDEX IF EXISTS ix_companies_domain_trgm")
    op.execute("DROP INDEX IF EXISTS ix_companies_name_trgm")
