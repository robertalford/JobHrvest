"""Universality gate: ever-passed set + per-site result history.

Adds two tables that back the redesigned auto-improve promotion gate:

  - ever_passed_sites     : monotonic set of sites any version has ever passed.
                            Closes the ratcheting-loss gap where site S passes
                            in v6.9, is silently lost by transient v7.0
                            champion, and then v7.1's regression gate (which
                            only sees the current champion) doesn't notice.

  - site_result_history   : append-only per-site/per-run verdict log, capped
                            to the last N observations per site. Backs the
                            oscillation detector in stability.py — a site that
                            has flipped pass/fail twice in the last 5 runs is
                            'unstable' and blocks narrow fixes on its cluster.

Revision ID: 0028
Revises: 0027
"""

from alembic import op


revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None


def upgrade():
    # ── ever_passed_sites ───────────────────────────────────────────────────
    # url is the natural key. We dedupe via (TRIM(LOWER(url))) at the app layer
    # so tracking-query variants collapse.
    op.execute("""
        CREATE TABLE IF NOT EXISTS ever_passed_sites (
            url                 TEXT PRIMARY KEY,
            company             TEXT,
            ats_platform        TEXT,
            best_composite      DOUBLE PRECISION NOT NULL,
            best_version_name   TEXT NOT NULL,
            best_run_id         UUID,
            jobs_quality        INTEGER NOT NULL DEFAULT 0,
            baseline_jobs       INTEGER NOT NULL DEFAULT 0,
            first_passed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_ever_passed_sites_ats
            ON ever_passed_sites (ats_platform)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_ever_passed_sites_last_seen
            ON ever_passed_sites (last_seen_at DESC)
    """)

    # ── site_result_history ─────────────────────────────────────────────────
    # Append-only per-run verdict. Older rows are trimmed by stability.py after
    # each insert so storage stays bounded — we only need the last N per site.
    op.execute("""
        CREATE TABLE IF NOT EXISTS site_result_history (
            id              BIGSERIAL PRIMARY KEY,
            url             TEXT NOT NULL,
            run_id          UUID NOT NULL,
            model_id        UUID,
            model_name      TEXT,
            ats_platform    TEXT,
            match           TEXT NOT NULL,
            passed          BOOLEAN NOT NULL,
            baseline_jobs   INTEGER NOT NULL DEFAULT 0,
            model_jobs      INTEGER NOT NULL DEFAULT 0,
            jobs_quality    INTEGER NOT NULL DEFAULT 0,
            composite_pts   DOUBLE PRECISION,
            observed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_site_result_history_url_time
            ON site_result_history (url, observed_at DESC)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_site_result_history_run
            ON site_result_history (run_id)
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS site_result_history")
    op.execute("DROP TABLE IF EXISTS ever_passed_sites")
