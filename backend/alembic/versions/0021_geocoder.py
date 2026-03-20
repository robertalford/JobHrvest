"""Geocoder — geo_locations table, geocode_cache, and geo fields on jobs.

Revision ID: 0021
Revises: 0020
Create Date: 2026-03-19
"""
from alembic import op

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # ── geo_locations ────────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE geo_locations (
            id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            level         INTEGER NOT NULL,
            name          VARCHAR(255) NOT NULL,
            ascii_name    VARCHAR(255),
            alt_names     TEXT[],
            parent_id     UUID REFERENCES geo_locations(id),
            market_code   VARCHAR(10) REFERENCES markets(code),
            country_code  CHAR(2),
            geonames_id   INTEGER,
            lat           NUMERIC(10, 7),
            lng           NUMERIC(10, 7),
            population    INTEGER,
            timezone      VARCHAR(100),
            admin1_code   VARCHAR(20),
            feature_code  VARCHAR(20),
            is_active     BOOLEAN DEFAULT TRUE,
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            updated_at    TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    op.execute("CREATE INDEX idx_geo_loc_level_market ON geo_locations(level, market_code)")
    op.execute("CREATE INDEX idx_geo_loc_parent       ON geo_locations(parent_id)")
    op.execute("CREATE INDEX idx_geo_loc_country      ON geo_locations(country_code, level)")
    op.execute("CREATE INDEX idx_geo_loc_admin1       ON geo_locations(country_code, admin1_code)")
    op.execute("""
        CREATE UNIQUE INDEX idx_geo_loc_geonames
        ON geo_locations(geonames_id) WHERE geonames_id IS NOT NULL
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_geo_loc_country_l1
        ON geo_locations(country_code) WHERE level = 1
    """)
    op.execute("CREATE INDEX idx_geo_loc_ascii_trgm ON geo_locations USING gin(lower(ascii_name) gin_trgm_ops)")
    op.execute("CREATE INDEX idx_geo_loc_name_trgm  ON geo_locations USING gin(lower(name) gin_trgm_ops)")

    # ── geocode_cache ────────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE geocode_cache (
            id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            raw_text          TEXT NOT NULL,
            market_code       VARCHAR(10),
            geo_location_id   UUID REFERENCES geo_locations(id),
            confidence        FLOAT,
            resolution_method VARCHAR(50),
            created_at        TIMESTAMPTZ DEFAULT NOW(),
            last_used_at      TIMESTAMPTZ DEFAULT NOW(),
            use_count         INTEGER DEFAULT 1
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_geocache_text_market
        ON geocode_cache(lower(raw_text), COALESCE(market_code, ''))
    """)
    op.execute("CREATE INDEX idx_geocache_method ON geocode_cache(resolution_method)")
    op.execute("CREATE INDEX idx_geocache_location ON geocode_cache(geo_location_id) WHERE geo_location_id IS NOT NULL")

    # ── geo fields on jobs ───────────────────────────────────────────────────
    op.execute("ALTER TABLE jobs ADD COLUMN geo_location_id      UUID REFERENCES geo_locations(id)")
    op.execute("ALTER TABLE jobs ADD COLUMN geo_level            INTEGER")
    op.execute("ALTER TABLE jobs ADD COLUMN geo_confidence       FLOAT")
    op.execute("ALTER TABLE jobs ADD COLUMN geo_resolution_method VARCHAR(50)")
    # NULL=not attempted  TRUE=resolved  FALSE=failed
    op.execute("ALTER TABLE jobs ADD COLUMN geo_resolved         BOOLEAN")

    op.execute("CREATE INDEX idx_jobs_geo_location ON jobs(geo_location_id) WHERE geo_location_id IS NOT NULL")
    op.execute("CREATE INDEX idx_jobs_geo_resolved ON jobs(geo_resolved)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_jobs_geo_resolved")
    op.execute("DROP INDEX IF EXISTS idx_jobs_geo_location")
    op.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS geo_resolved")
    op.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS geo_resolution_method")
    op.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS geo_confidence")
    op.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS geo_level")
    op.execute("ALTER TABLE jobs DROP COLUMN IF EXISTS geo_location_id")
    op.execute("DROP TABLE IF EXISTS geocode_cache")
    op.execute("DROP TABLE IF EXISTS geo_locations")
