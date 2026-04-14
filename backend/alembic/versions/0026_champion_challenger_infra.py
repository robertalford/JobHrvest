"""Champion/challenger ML infrastructure — registry, GOLD holdout, ATS quarantine, drift, latency.

Adds the persistence layer required to run a hard-gated champion/challenger loop
without falling into the closed-loop "model learns the rules" trap.

Tables added:
  - model_versions          : registered model artifacts + lineage
  - experiments             : champion-vs-challenger comparisons
  - metric_snapshots        : stratified metrics for a model_version on a holdout
  - gold_holdout_sets       : frozen evaluation sets (point-in-time)
  - gold_holdout_domains    : per-domain rows in a holdout set
  - gold_holdout_snapshots  : raw HTML snapshots for reproducibility
  - gold_holdout_jobs       : verified job records the model is measured against
  - ats_pattern_proposals   : LLM-suggested ATS selectors awaiting validation
  - drift_baselines         : reference feature distributions for drift detection
  - inference_metrics_hourly: aggregated latency/escalation per (model_version, hour)

The split between gold_holdout_domains and gold_holdout_jobs is intentional:
domains are the holdout unit (split-by-domain), jobs are the per-domain ground
truth that coverage/title-accuracy metrics are scored against.

Revision ID: 0023
Revises: 0022
"""

from alembic import op


revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


def upgrade():
    # ── model_versions ────────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS model_versions (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name            TEXT NOT NULL,
            version         INTEGER NOT NULL,
            algorithm       TEXT NOT NULL,
            artifact_path   TEXT,
            config          JSONB NOT NULL DEFAULT '{}'::jsonb,
            feature_set     JSONB NOT NULL DEFAULT '[]'::jsonb,
            training_corpus_hash TEXT,
            parent_version_id UUID REFERENCES model_versions(id) ON DELETE SET NULL,
            status          TEXT NOT NULL DEFAULT 'candidate',
            -- candidate | challenger | champion | retired | rejected
            trained_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            promoted_at     TIMESTAMPTZ,
            retired_at      TIMESTAMPTZ,
            notes           TEXT,
            CONSTRAINT model_versions_name_version_uk UNIQUE (name, version)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_model_versions_name_status
            ON model_versions (name, status)
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_model_versions_one_champion_per_name
            ON model_versions (name) WHERE status = 'champion'
    """)

    # ── gold_holdout_sets ─────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS gold_holdout_sets (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name            TEXT NOT NULL UNIQUE,
            description     TEXT,
            source          TEXT NOT NULL DEFAULT 'lead_imports',
            -- lead_imports | manual | mixed
            market_id       TEXT,
            is_frozen       BOOLEAN NOT NULL DEFAULT false,
            frozen_at       TIMESTAMPTZ,
            is_active       BOOLEAN NOT NULL DEFAULT true,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # ── gold_holdout_domains ──────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS gold_holdout_domains (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            holdout_set_id  UUID NOT NULL REFERENCES gold_holdout_sets(id) ON DELETE CASCADE,
            domain          TEXT NOT NULL,
            advertiser_name TEXT,
            expected_job_count INTEGER,
            market_id       TEXT,
            ats_platform    TEXT,
            -- Filled by detect_ats() once HTML is fetched
            source_lead_import_id UUID REFERENCES lead_imports(id) ON DELETE SET NULL,
            verification_status TEXT NOT NULL DEFAULT 'unverified',
            -- unverified | auto | human_verified | rejected
            verified_at     TIMESTAMPTZ,
            verified_by     TEXT,
            CONSTRAINT gold_holdout_domains_uk UNIQUE (holdout_set_id, domain)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_gold_holdout_domains_set
            ON gold_holdout_domains (holdout_set_id)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_gold_holdout_domains_ats_market
            ON gold_holdout_domains (ats_platform, market_id)
    """)

    # ── gold_holdout_snapshots ────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS gold_holdout_snapshots (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            holdout_domain_id UUID NOT NULL REFERENCES gold_holdout_domains(id) ON DELETE CASCADE,
            url             TEXT NOT NULL,
            snapshot_path   TEXT NOT NULL,
            content_hash    TEXT NOT NULL,
            content_type    TEXT,
            http_status     INTEGER,
            byte_size       INTEGER,
            snapshotted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT gold_holdout_snapshots_uk UNIQUE (holdout_domain_id, content_hash)
        )
    """)

    # ── gold_holdout_jobs ─────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS gold_holdout_jobs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            holdout_domain_id UUID NOT NULL REFERENCES gold_holdout_domains(id) ON DELETE CASCADE,
            title           TEXT NOT NULL,
            location        TEXT,
            employment_type TEXT,
            apply_url       TEXT,
            verified_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            verified_by     TEXT,
            notes           TEXT
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_gold_holdout_jobs_domain
            ON gold_holdout_jobs (holdout_domain_id)
    """)

    # ── experiments ───────────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS experiments (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name            TEXT NOT NULL,
            model_name      TEXT NOT NULL,
            champion_version_id  UUID REFERENCES model_versions(id) ON DELETE SET NULL,
            challenger_version_id UUID REFERENCES model_versions(id) ON DELETE SET NULL,
            holdout_set_id  UUID NOT NULL REFERENCES gold_holdout_sets(id) ON DELETE RESTRICT,
            strategy        TEXT NOT NULL,
            -- hyperparam_search | feature_ablation | feature_addition | algorithm_swap
            -- pseudo_label_threshold | embedding_swap | ensemble | manual
            status          TEXT NOT NULL DEFAULT 'pending',
            -- pending | running | promoted | rejected | aborted
            promotion_decision JSONB,
            -- {"verdict": "promote"|"reject", "reasons": [...], "p_value": ..., "delta": {...}}
            started_at      TIMESTAMPTZ,
            completed_at    TIMESTAMPTZ,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT experiments_name_uk UNIQUE (name)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_experiments_model_status
            ON experiments (model_name, status)
    """)

    # ── metric_snapshots ──────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS metric_snapshots (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            model_version_id UUID NOT NULL REFERENCES model_versions(id) ON DELETE CASCADE,
            holdout_set_id  UUID NOT NULL REFERENCES gold_holdout_sets(id) ON DELETE CASCADE,
            experiment_id   UUID REFERENCES experiments(id) ON DELETE SET NULL,
            stratum_key     TEXT NOT NULL DEFAULT 'all',
            -- 'all' | 'ats=greenhouse' | 'market=AU' | etc.
            metric_name     TEXT NOT NULL,
            metric_value    DOUBLE PRECISION NOT NULL,
            sample_size     INTEGER NOT NULL,
            ci_low          DOUBLE PRECISION,
            ci_high         DOUBLE PRECISION,
            computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT metric_snapshots_uk UNIQUE
                (model_version_id, holdout_set_id, stratum_key, metric_name, computed_at)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_metric_snapshots_model
            ON metric_snapshots (model_version_id, computed_at DESC)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_metric_snapshots_experiment
            ON metric_snapshots (experiment_id)
    """)

    # ── ats_pattern_proposals ─────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS ats_pattern_proposals (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            ats_name        TEXT NOT NULL,
            source          TEXT NOT NULL DEFAULT 'llm',
            -- llm | manual | discovered
            sample_url      TEXT,
            url_patterns    JSONB NOT NULL DEFAULT '[]'::jsonb,
            html_patterns   JSONB NOT NULL DEFAULT '[]'::jsonb,
            selectors       JSONB NOT NULL,
            pagination      JSONB,
            confidence      DOUBLE PRECISION,
            status          TEXT NOT NULL DEFAULT 'proposed',
            -- proposed | shadow | active | rejected | superseded
            shadow_match_count   INTEGER NOT NULL DEFAULT 0,
            shadow_failure_count INTEGER NOT NULL DEFAULT 0,
            shadow_first_seen    TIMESTAMPTZ,
            shadow_last_seen     TIMESTAMPTZ,
            promoted_at     TIMESTAMPTZ,
            rejected_at     TIMESTAMPTZ,
            rejection_reason TEXT,
            notes           TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_ats_pattern_proposals_status
            ON ats_pattern_proposals (status, ats_name)
    """)

    # ── drift_baselines ───────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS drift_baselines (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            model_name      TEXT NOT NULL,
            feature_name    TEXT NOT NULL,
            distribution    JSONB NOT NULL,
            -- {"type": "histogram", "bins": [...], "counts": [...]}
            -- or {"type": "categorical", "values": {"A": 0.4, "B": 0.6}}
            window_start    TIMESTAMPTZ NOT NULL,
            window_end      TIMESTAMPTZ NOT NULL,
            sample_size     INTEGER NOT NULL,
            computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_active       BOOLEAN NOT NULL DEFAULT true,
            CONSTRAINT drift_baselines_uk UNIQUE (model_name, feature_name, window_end)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_drift_baselines_active
            ON drift_baselines (model_name, feature_name) WHERE is_active = true
    """)

    # ── inference_metrics_hourly ──────────────────────────────────────────────
    # Aggregated by hour to keep this table bounded — raw per-page metrics live
    # in Redis (see app.ml.latency_budget) and roll up via a periodic Celery task.
    op.execute("""
        CREATE TABLE IF NOT EXISTS inference_metrics_hourly (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            model_version_id UUID NOT NULL REFERENCES model_versions(id) ON DELETE CASCADE,
            hour_bucket     TIMESTAMPTZ NOT NULL,
            sample_count    INTEGER NOT NULL,
            latency_p50_ms  DOUBLE PRECISION,
            latency_p95_ms  DOUBLE PRECISION,
            latency_p99_ms  DOUBLE PRECISION,
            llm_escalation_count INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            CONSTRAINT inference_metrics_hourly_uk UNIQUE (model_version_id, hour_bucket)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_inference_metrics_hourly_bucket
            ON inference_metrics_hourly (model_version_id, hour_bucket DESC)
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS inference_metrics_hourly CASCADE")
    op.execute("DROP TABLE IF EXISTS drift_baselines CASCADE")
    op.execute("DROP TABLE IF EXISTS ats_pattern_proposals CASCADE")
    op.execute("DROP TABLE IF EXISTS metric_snapshots CASCADE")
    op.execute("DROP TABLE IF EXISTS experiments CASCADE")
    op.execute("DROP TABLE IF EXISTS gold_holdout_jobs CASCADE")
    op.execute("DROP TABLE IF EXISTS gold_holdout_snapshots CASCADE")
    op.execute("DROP TABLE IF EXISTS gold_holdout_domains CASCADE")
    op.execute("DROP TABLE IF EXISTS gold_holdout_sets CASCADE")
    op.execute("DROP TABLE IF EXISTS model_versions CASCADE")
