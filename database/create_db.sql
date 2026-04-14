--
-- PostgreSQL database dump
--

\restrict iu68Ss0cB6rrzG37wyQSbaZqc58Abdxpo9B8DCTEwhBsJuJKc0fxtnzGvh9SuXd

-- Dumped from database version 16.13
-- Dumped by pg_dump version 16.13

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: refresh_company_stats(uuid); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_company_stats(p_company_id uuid) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
        $$;


--
-- Name: trg_career_pages_stats(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_career_pages_stats() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                PERFORM refresh_company_stats(OLD.company_id);
            ELSE
                PERFORM refresh_company_stats(NEW.company_id);
            END IF;
            RETURN NULL;
        END;
        $$;


--
-- Name: trg_crawl_logs_stats(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_crawl_logs_stats() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                PERFORM refresh_company_stats(OLD.company_id);
            ELSE
                PERFORM refresh_company_stats(NEW.company_id);
            END IF;
            RETURN NULL;
        END;
        $$;


--
-- Name: trg_lead_imports_stats(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.trg_lead_imports_stats() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                PERFORM refresh_company_stats(OLD.company_id);
            ELSIF NEW.company_id IS NOT NULL THEN
                PERFORM refresh_company_stats(NEW.company_id);
            END IF;
            RETURN NULL;
        END;
        $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _tmp_status; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._tmp_status (
    crawler_id text,
    status text
);


--
-- Name: aggregator_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.aggregator_sources (
    id uuid NOT NULL,
    name text NOT NULL,
    base_url text NOT NULL,
    market character varying(10) NOT NULL,
    is_active boolean,
    purpose text,
    last_link_harvest_at timestamp with time zone
);


--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


--
-- Name: app_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.app_users (
    id uuid NOT NULL,
    username character varying(255) NOT NULL,
    password_hash character varying(255) NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: ats_pattern_proposals; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ats_pattern_proposals (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    ats_name text NOT NULL,
    source text DEFAULT 'llm'::text NOT NULL,
    sample_url text,
    url_patterns jsonb DEFAULT '[]'::jsonb NOT NULL,
    html_patterns jsonb DEFAULT '[]'::jsonb NOT NULL,
    selectors jsonb NOT NULL,
    pagination jsonb,
    confidence double precision,
    status text DEFAULT 'proposed'::text NOT NULL,
    shadow_match_count integer DEFAULT 0 NOT NULL,
    shadow_failure_count integer DEFAULT 0 NOT NULL,
    shadow_first_seen timestamp with time zone,
    shadow_last_seen timestamp with time zone,
    promoted_at timestamp with time zone,
    rejected_at timestamp with time zone,
    rejection_reason text,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: career_pages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.career_pages (
    id uuid NOT NULL,
    company_id uuid NOT NULL,
    url text NOT NULL,
    page_type text,
    discovery_method text,
    discovery_confidence double precision,
    is_primary boolean,
    is_paginated boolean,
    pagination_type text,
    pagination_selector text,
    requires_js_rendering boolean,
    last_content_hash text,
    last_crawled_at timestamp with time zone,
    last_extraction_at timestamp with time zone,
    is_active boolean,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    site_status text DEFAULT 'no_structure_new'::text NOT NULL
);


--
-- Name: codex_improvement_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.codex_improvement_runs (
    id uuid NOT NULL,
    source_model_id uuid,
    test_run_id uuid,
    output_model_id uuid,
    status text DEFAULT 'analysing'::text NOT NULL,
    description text,
    source_model_name text,
    output_model_name text,
    test_winner text,
    error_message text,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: companies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.companies (
    id uuid NOT NULL,
    name text NOT NULL,
    domain text NOT NULL,
    root_url text NOT NULL,
    market_code character varying(10),
    discovered_via text,
    ats_platform text,
    ats_confidence double precision,
    crawl_priority integer,
    crawl_frequency_hours integer,
    last_crawl_at timestamp with time zone,
    next_crawl_at timestamp with time zone,
    is_active boolean,
    requires_js_rendering boolean,
    anti_bot_level text,
    notes text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    quality_score double precision,
    quality_scored_at timestamp with time zone,
    company_status text DEFAULT 'no_sites_new'::text NOT NULL
);


--
-- Name: company_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_stats (
    company_id uuid NOT NULL,
    active_site_count integer DEFAULT 0 NOT NULL,
    last_crawl_jobs integer,
    total_crawls integer DEFAULT 0 NOT NULL,
    avg_last_3_jobs integer,
    imported_expected_jobs integer,
    expected_jobs integer,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    sites_json jsonb DEFAULT '[]'::jsonb NOT NULL
);


--
-- Name: crawl_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.crawl_logs (
    id uuid NOT NULL,
    company_id uuid,
    career_page_id uuid,
    crawl_type text NOT NULL,
    status text NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    pages_crawled integer,
    jobs_found integer,
    jobs_new integer,
    jobs_updated integer,
    jobs_removed integer,
    error_message text,
    error_details jsonb,
    method_used text,
    duration_seconds double precision
);


--
-- Name: crawl_steps_test_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.crawl_steps_test_data (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    external_id text,
    crawler_id text NOT NULL,
    step_name text NOT NULL,
    step_index integer NOT NULL,
    options jsonb,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: crawler_test_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.crawler_test_data (
    id uuid NOT NULL,
    external_id text NOT NULL,
    job_site_id text NOT NULL,
    name text NOT NULL,
    crawler_type text NOT NULL,
    country text,
    country_code text,
    frequency integer,
    status text,
    current_status text,
    disabled boolean DEFAULT false,
    statistics_data jsonb,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: drift_baselines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.drift_baselines (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    model_name text NOT NULL,
    feature_name text NOT NULL,
    distribution jsonb NOT NULL,
    window_start timestamp with time zone NOT NULL,
    window_end timestamp with time zone NOT NULL,
    sample_size integer NOT NULL,
    computed_at timestamp with time zone DEFAULT now() NOT NULL,
    is_active boolean DEFAULT true NOT NULL
);


--
-- Name: ever_passed_sites; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ever_passed_sites (
    url text NOT NULL,
    company text,
    ats_platform text,
    best_composite double precision NOT NULL,
    best_version_name text NOT NULL,
    best_run_id uuid,
    jobs_quality integer DEFAULT 0 NOT NULL,
    baseline_jobs integer DEFAULT 0 NOT NULL,
    first_passed_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: excluded_sites; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.excluded_sites (
    id uuid NOT NULL,
    domain text NOT NULL,
    company_name text,
    site_url text,
    site_type character varying(50),
    country_code character varying(10),
    expected_job_count integer,
    reason text,
    source_file text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: experiments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.experiments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    model_name text NOT NULL,
    champion_version_id uuid,
    challenger_version_id uuid,
    holdout_set_id uuid NOT NULL,
    strategy text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    promotion_decision jsonb,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: extraction_comparisons; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.extraction_comparisons (
    id uuid NOT NULL,
    job_id uuid NOT NULL,
    career_page_id uuid NOT NULL,
    method_a text NOT NULL,
    method_b text NOT NULL,
    method_a_result jsonb,
    method_b_result jsonb,
    agreement_score double precision,
    resolved_result jsonb,
    resolution_method text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: fixed_test_sites; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fixed_test_sites (
    url text,
    company_name text,
    known_selectors jsonb
);


--
-- Name: geo_locations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geo_locations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    level integer NOT NULL,
    name character varying(255) NOT NULL,
    ascii_name character varying(255),
    alt_names text[],
    parent_id uuid,
    market_code character varying(10),
    country_code character(2),
    geonames_id integer,
    lat numeric(10,7),
    lng numeric(10,7),
    population integer,
    timezone character varying(100),
    admin1_code character varying(20),
    feature_code character varying(20),
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: geocode_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.geocode_cache (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    raw_text text NOT NULL,
    market_code character varying(10),
    geo_location_id uuid,
    confidence double precision,
    resolution_method character varying(50),
    created_at timestamp with time zone DEFAULT now(),
    last_used_at timestamp with time zone DEFAULT now(),
    use_count integer DEFAULT 1
);


--
-- Name: gold_holdout_domains; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gold_holdout_domains (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    holdout_set_id uuid NOT NULL,
    domain text NOT NULL,
    advertiser_name text,
    expected_job_count integer,
    market_id text,
    ats_platform text,
    source_lead_import_id uuid,
    verification_status text DEFAULT 'unverified'::text NOT NULL,
    verified_at timestamp with time zone,
    verified_by text
);


--
-- Name: gold_holdout_jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gold_holdout_jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    holdout_domain_id uuid NOT NULL,
    title text NOT NULL,
    location text,
    employment_type text,
    apply_url text,
    verified_at timestamp with time zone DEFAULT now() NOT NULL,
    verified_by text,
    notes text,
    verification_status text DEFAULT 'unverified'::text NOT NULL,
    source text DEFAULT 'manual'::text NOT NULL,
    source_url text,
    description_length integer
);


--
-- Name: gold_holdout_sets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gold_holdout_sets (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    description text,
    source text DEFAULT 'lead_imports'::text NOT NULL,
    market_id text,
    is_frozen boolean DEFAULT false NOT NULL,
    frozen_at timestamp with time zone,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: gold_holdout_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gold_holdout_snapshots (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    holdout_domain_id uuid NOT NULL,
    url text NOT NULL,
    snapshot_path text NOT NULL,
    content_hash text NOT NULL,
    content_type text,
    http_status integer,
    byte_size integer,
    snapshotted_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: inference_metrics_hourly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.inference_metrics_hourly (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    model_version_id uuid NOT NULL,
    hour_bucket timestamp with time zone NOT NULL,
    sample_count integer NOT NULL,
    latency_p50_ms double precision,
    latency_p95_ms double precision,
    latency_p99_ms double precision,
    llm_escalation_count integer DEFAULT 0 NOT NULL,
    error_count integer DEFAULT 0 NOT NULL
);


--
-- Name: job_site_test_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_site_test_data (
    id uuid NOT NULL,
    external_id text NOT NULL,
    name text NOT NULL,
    site_type text NOT NULL,
    num_of_jobs integer,
    expected_job_count integer,
    disabled boolean DEFAULT false,
    uncrawlable_reason text,
    tags jsonb,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: job_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_tags (
    id uuid NOT NULL,
    job_id uuid NOT NULL,
    tag_type text NOT NULL,
    tag_value text NOT NULL,
    confidence double precision
);


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.jobs (
    id uuid NOT NULL,
    company_id uuid NOT NULL,
    career_page_id uuid,
    source_url text NOT NULL,
    external_id text,
    title text NOT NULL,
    description text,
    description_html text,
    location_raw text,
    location_city text,
    location_state text,
    location_country text,
    is_remote boolean,
    remote_type text,
    employment_type text,
    seniority_level text,
    department text,
    team text,
    salary_raw text,
    salary_min numeric(12,2),
    salary_max numeric(12,2),
    salary_currency text,
    salary_period text,
    requirements text,
    benefits text,
    application_url text,
    date_posted date,
    date_expires date,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    is_active boolean,
    extraction_method text,
    extraction_confidence double precision,
    raw_data jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    quality_score double precision,
    quality_completeness double precision,
    quality_description double precision,
    quality_issues jsonb,
    quality_flags jsonb,
    quality_scored_at timestamp with time zone,
    canonical_job_id uuid,
    is_canonical boolean DEFAULT true NOT NULL,
    duplicate_count integer DEFAULT 0 NOT NULL,
    dedup_score double precision,
    quality_override boolean,
    description_enriched_at timestamp with time zone,
    geo_location_id uuid,
    geo_level integer,
    geo_confidence double precision,
    geo_resolution_method character varying(50),
    geo_resolved boolean
);


--
-- Name: COLUMN jobs.canonical_job_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.jobs.canonical_job_id IS 'Points to the canonical (best) version of this job. NULL = this job IS canonical.';


--
-- Name: COLUMN jobs.is_canonical; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.jobs.is_canonical IS 'True if this is the best/canonical version. False = a duplicate exists.';


--
-- Name: COLUMN jobs.duplicate_count; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.jobs.duplicate_count IS 'On canonical jobs: how many lower-quality duplicates exist.';


--
-- Name: COLUMN jobs.dedup_score; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.jobs.dedup_score IS 'Similarity score to canonical job (0.0-1.0). NULL if no dedup run.';


--
-- Name: lead_import_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lead_import_batches (
    id uuid NOT NULL,
    filename text NOT NULL,
    original_filename text NOT NULL,
    file_size_bytes integer,
    total_rows integer,
    validation_status character varying(20) DEFAULT 'pending'::character varying,
    validation_errors jsonb,
    import_status character varying(20) DEFAULT 'pending'::character varying,
    imported_leads integer DEFAULT 0,
    failed_leads integer DEFAULT 0,
    blocked_leads integer DEFAULT 0,
    skipped_leads integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    import_started_at timestamp with time zone,
    import_completed_at timestamp with time zone,
    error_message text
);


--
-- Name: lead_imports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lead_imports (
    id uuid NOT NULL,
    country_id character varying(10) NOT NULL,
    advertiser_name text NOT NULL,
    origin_domain text NOT NULL,
    sample_linkout_url text,
    ad_origin_category text,
    expected_job_count integer,
    origin_rank integer,
    status text NOT NULL,
    company_id uuid,
    career_pages_found integer,
    jobs_extracted integer,
    error_message text,
    error_details jsonb,
    skip_reason text,
    imported_at timestamp with time zone DEFAULT now(),
    processed_at timestamp with time zone,
    batch_id uuid
);


--
-- Name: markets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.markets (
    id uuid NOT NULL,
    code character varying(10) NOT NULL,
    name text NOT NULL,
    is_active boolean,
    default_currency character varying(10),
    locale character varying(20),
    salary_parsing_config jsonb,
    location_parsing_config jsonb,
    aggregator_search_queries jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: metric_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metric_snapshots (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    model_version_id uuid NOT NULL,
    holdout_set_id uuid NOT NULL,
    experiment_id uuid,
    stratum_key text DEFAULT 'all'::text NOT NULL,
    metric_name text NOT NULL,
    metric_value double precision NOT NULL,
    sample_size integer NOT NULL,
    ci_low double precision,
    ci_high double precision,
    computed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: ml_model_test_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ml_model_test_runs (
    id uuid NOT NULL,
    model_id uuid NOT NULL,
    test_name text,
    total_tests integer DEFAULT 0,
    tests_passed integer DEFAULT 0,
    tests_failed integer DEFAULT 0,
    accuracy double precision,
    precision_score double precision,
    recall double precision,
    f1_score double precision,
    test_config jsonb,
    results_detail jsonb,
    status text DEFAULT 'pending'::text NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    error_message text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: ml_models; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ml_models (
    id uuid NOT NULL,
    name text NOT NULL,
    model_type text NOT NULL,
    description text,
    config jsonb,
    status text DEFAULT 'new'::text NOT NULL,
    version integer DEFAULT 1,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: ml_test_feedback; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ml_test_feedback (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    test_run_id uuid NOT NULL,
    site_url text NOT NULL,
    comment text NOT NULL,
    screenshot_path text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: model_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.model_versions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    version integer NOT NULL,
    algorithm text NOT NULL,
    artifact_path text,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    feature_set jsonb DEFAULT '[]'::jsonb NOT NULL,
    training_corpus_hash text,
    parent_version_id uuid,
    status text DEFAULT 'candidate'::text NOT NULL,
    trained_at timestamp with time zone DEFAULT now() NOT NULL,
    promoted_at timestamp with time zone,
    retired_at timestamp with time zone,
    notes text
);


--
-- Name: review_feedback; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.review_feedback (
    id uuid NOT NULL,
    job_id uuid NOT NULL,
    review_type character varying(20) NOT NULL,
    decision character varying(20) NOT NULL,
    canonical_job_id uuid,
    features_snapshot jsonb,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: run_queue; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.run_queue (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    queue_type text NOT NULL,
    item_id uuid,
    item_type text,
    status text DEFAULT 'pending'::text NOT NULL,
    priority integer DEFAULT 5 NOT NULL,
    added_at timestamp with time zone DEFAULT now() NOT NULL,
    processing_started_at timestamp with time zone,
    processing_completed_at timestamp with time zone,
    error_message text,
    added_by text DEFAULT 'system'::text
);


--
-- Name: site_result_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.site_result_history (
    id bigint NOT NULL,
    url text NOT NULL,
    run_id uuid NOT NULL,
    model_id uuid,
    model_name text,
    ats_platform text,
    match text NOT NULL,
    passed boolean NOT NULL,
    baseline_jobs integer DEFAULT 0 NOT NULL,
    model_jobs integer DEFAULT 0 NOT NULL,
    jobs_quality integer DEFAULT 0 NOT NULL,
    composite_pts double precision,
    observed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: site_result_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.site_result_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: site_result_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.site_result_history_id_seq OWNED BY public.site_result_history.id;


--
-- Name: site_templates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.site_templates (
    id uuid NOT NULL,
    company_id uuid NOT NULL,
    career_page_id uuid NOT NULL,
    template_type text NOT NULL,
    selectors jsonb,
    learned_via text,
    accuracy_score double precision,
    last_validated_at timestamp with time zone,
    is_active boolean,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: site_url_test_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.site_url_test_data (
    id uuid NOT NULL,
    site_id text NOT NULL,
    url text NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: site_wrapper_test_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.site_wrapper_test_data (
    id uuid NOT NULL,
    external_id text NOT NULL,
    crawler_id text NOT NULL,
    selectors jsonb NOT NULL,
    paths_config jsonb,
    has_detail_page boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone
);


--
-- Name: system_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.system_settings (
    key text NOT NULL,
    value jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: word_filters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.word_filters (
    id uuid NOT NULL,
    word text NOT NULL,
    filter_type text NOT NULL,
    markets jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: site_result_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.site_result_history ALTER COLUMN id SET DEFAULT nextval('public.site_result_history_id_seq'::regclass);


--
-- Name: aggregator_sources aggregator_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.aggregator_sources
    ADD CONSTRAINT aggregator_sources_pkey PRIMARY KEY (id);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: app_users app_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_users
    ADD CONSTRAINT app_users_pkey PRIMARY KEY (id);


--
-- Name: app_users app_users_username_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_users
    ADD CONSTRAINT app_users_username_key UNIQUE (username);


--
-- Name: ats_pattern_proposals ats_pattern_proposals_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ats_pattern_proposals
    ADD CONSTRAINT ats_pattern_proposals_pkey PRIMARY KEY (id);


--
-- Name: career_pages career_pages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.career_pages
    ADD CONSTRAINT career_pages_pkey PRIMARY KEY (id);


--
-- Name: codex_improvement_runs codex_improvement_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codex_improvement_runs
    ADD CONSTRAINT codex_improvement_runs_pkey PRIMARY KEY (id);


--
-- Name: companies companies_domain_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_domain_key UNIQUE (domain);


--
-- Name: companies companies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (id);


--
-- Name: company_stats company_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_stats
    ADD CONSTRAINT company_stats_pkey PRIMARY KEY (company_id);


--
-- Name: crawl_logs crawl_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crawl_logs
    ADD CONSTRAINT crawl_logs_pkey PRIMARY KEY (id);


--
-- Name: crawl_steps_test_data crawl_steps_test_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crawl_steps_test_data
    ADD CONSTRAINT crawl_steps_test_data_pkey PRIMARY KEY (id);


--
-- Name: crawler_test_data crawler_test_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crawler_test_data
    ADD CONSTRAINT crawler_test_data_pkey PRIMARY KEY (id);


--
-- Name: drift_baselines drift_baselines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.drift_baselines
    ADD CONSTRAINT drift_baselines_pkey PRIMARY KEY (id);


--
-- Name: drift_baselines drift_baselines_uk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.drift_baselines
    ADD CONSTRAINT drift_baselines_uk UNIQUE (model_name, feature_name, window_end);


--
-- Name: ever_passed_sites ever_passed_sites_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ever_passed_sites
    ADD CONSTRAINT ever_passed_sites_pkey PRIMARY KEY (url);


--
-- Name: excluded_sites excluded_sites_domain_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.excluded_sites
    ADD CONSTRAINT excluded_sites_domain_key UNIQUE (domain);


--
-- Name: excluded_sites excluded_sites_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.excluded_sites
    ADD CONSTRAINT excluded_sites_pkey PRIMARY KEY (id);


--
-- Name: experiments experiments_name_uk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_name_uk UNIQUE (name);


--
-- Name: experiments experiments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_pkey PRIMARY KEY (id);


--
-- Name: extraction_comparisons extraction_comparisons_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.extraction_comparisons
    ADD CONSTRAINT extraction_comparisons_pkey PRIMARY KEY (id);


--
-- Name: geo_locations geo_locations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geo_locations
    ADD CONSTRAINT geo_locations_pkey PRIMARY KEY (id);


--
-- Name: geocode_cache geocode_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geocode_cache
    ADD CONSTRAINT geocode_cache_pkey PRIMARY KEY (id);


--
-- Name: gold_holdout_domains gold_holdout_domains_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_domains
    ADD CONSTRAINT gold_holdout_domains_pkey PRIMARY KEY (id);


--
-- Name: gold_holdout_domains gold_holdout_domains_uk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_domains
    ADD CONSTRAINT gold_holdout_domains_uk UNIQUE (holdout_set_id, domain);


--
-- Name: gold_holdout_jobs gold_holdout_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_jobs
    ADD CONSTRAINT gold_holdout_jobs_pkey PRIMARY KEY (id);


--
-- Name: gold_holdout_sets gold_holdout_sets_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_sets
    ADD CONSTRAINT gold_holdout_sets_name_key UNIQUE (name);


--
-- Name: gold_holdout_sets gold_holdout_sets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_sets
    ADD CONSTRAINT gold_holdout_sets_pkey PRIMARY KEY (id);


--
-- Name: gold_holdout_snapshots gold_holdout_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_snapshots
    ADD CONSTRAINT gold_holdout_snapshots_pkey PRIMARY KEY (id);


--
-- Name: gold_holdout_snapshots gold_holdout_snapshots_uk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_snapshots
    ADD CONSTRAINT gold_holdout_snapshots_uk UNIQUE (holdout_domain_id, content_hash);


--
-- Name: inference_metrics_hourly inference_metrics_hourly_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inference_metrics_hourly
    ADD CONSTRAINT inference_metrics_hourly_pkey PRIMARY KEY (id);


--
-- Name: inference_metrics_hourly inference_metrics_hourly_uk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inference_metrics_hourly
    ADD CONSTRAINT inference_metrics_hourly_uk UNIQUE (model_version_id, hour_bucket);


--
-- Name: job_site_test_data job_site_test_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_site_test_data
    ADD CONSTRAINT job_site_test_data_pkey PRIMARY KEY (id);


--
-- Name: job_tags job_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_tags
    ADD CONSTRAINT job_tags_pkey PRIMARY KEY (id);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: lead_import_batches lead_import_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lead_import_batches
    ADD CONSTRAINT lead_import_batches_pkey PRIMARY KEY (id);


--
-- Name: lead_imports lead_imports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lead_imports
    ADD CONSTRAINT lead_imports_pkey PRIMARY KEY (id);


--
-- Name: markets markets_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.markets
    ADD CONSTRAINT markets_code_key UNIQUE (code);


--
-- Name: markets markets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.markets
    ADD CONSTRAINT markets_pkey PRIMARY KEY (id);


--
-- Name: metric_snapshots metric_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric_snapshots
    ADD CONSTRAINT metric_snapshots_pkey PRIMARY KEY (id);


--
-- Name: metric_snapshots metric_snapshots_uk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric_snapshots
    ADD CONSTRAINT metric_snapshots_uk UNIQUE (model_version_id, holdout_set_id, stratum_key, metric_name, computed_at);


--
-- Name: ml_model_test_runs ml_model_test_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ml_model_test_runs
    ADD CONSTRAINT ml_model_test_runs_pkey PRIMARY KEY (id);


--
-- Name: ml_models ml_models_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ml_models
    ADD CONSTRAINT ml_models_pkey PRIMARY KEY (id);


--
-- Name: ml_test_feedback ml_test_feedback_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ml_test_feedback
    ADD CONSTRAINT ml_test_feedback_pkey PRIMARY KEY (id);


--
-- Name: model_versions model_versions_name_version_uk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_versions
    ADD CONSTRAINT model_versions_name_version_uk UNIQUE (name, version);


--
-- Name: model_versions model_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_versions
    ADD CONSTRAINT model_versions_pkey PRIMARY KEY (id);


--
-- Name: review_feedback review_feedback_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.review_feedback
    ADD CONSTRAINT review_feedback_pkey PRIMARY KEY (id);


--
-- Name: run_queue run_queue_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.run_queue
    ADD CONSTRAINT run_queue_pkey PRIMARY KEY (id);


--
-- Name: site_result_history site_result_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.site_result_history
    ADD CONSTRAINT site_result_history_pkey PRIMARY KEY (id);


--
-- Name: site_templates site_templates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.site_templates
    ADD CONSTRAINT site_templates_pkey PRIMARY KEY (id);


--
-- Name: site_url_test_data site_url_test_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.site_url_test_data
    ADD CONSTRAINT site_url_test_data_pkey PRIMARY KEY (id);


--
-- Name: site_wrapper_test_data site_wrapper_test_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.site_wrapper_test_data
    ADD CONSTRAINT site_wrapper_test_data_pkey PRIMARY KEY (id);


--
-- Name: system_settings system_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_settings
    ADD CONSTRAINT system_settings_pkey PRIMARY KEY (key);


--
-- Name: aggregator_sources uq_aggregator_sources_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.aggregator_sources
    ADD CONSTRAINT uq_aggregator_sources_name UNIQUE (name);


--
-- Name: word_filters word_filters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.word_filters
    ADD CONSTRAINT word_filters_pkey PRIMARY KEY (id);


--
-- Name: crawl_steps_test_data_crawler_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX crawl_steps_test_data_crawler_id_idx ON public.crawl_steps_test_data USING btree (crawler_id);


--
-- Name: crawl_steps_test_data_crawler_id_step_index_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX crawl_steps_test_data_crawler_id_step_index_idx ON public.crawl_steps_test_data USING btree (crawler_id, step_index);


--
-- Name: idx_geo_loc_ascii_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_geo_loc_ascii_trgm ON public.geo_locations USING gin (ascii_name public.gin_trgm_ops);


--
-- Name: idx_geo_loc_country; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_geo_loc_country ON public.geo_locations USING btree (country_code, level);


--
-- Name: idx_geo_loc_country_l1; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_geo_loc_country_l1 ON public.geo_locations USING btree (country_code) WHERE (level = 1);


--
-- Name: idx_geo_loc_geonames; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_geo_loc_geonames ON public.geo_locations USING btree (geonames_id) WHERE (geonames_id IS NOT NULL);


--
-- Name: idx_geo_loc_level_market; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_geo_loc_level_market ON public.geo_locations USING btree (level, market_code);


--
-- Name: idx_geo_loc_name_lower_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_geo_loc_name_lower_trgm ON public.geo_locations USING gin (lower((name)::text) public.gin_trgm_ops);


--
-- Name: idx_geo_loc_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_geo_loc_parent ON public.geo_locations USING btree (parent_id);


--
-- Name: idx_geocache_location; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_geocache_location ON public.geocode_cache USING btree (geo_location_id) WHERE (geo_location_id IS NOT NULL);


--
-- Name: idx_geocache_method; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_geocache_method ON public.geocode_cache USING btree (resolution_method);


--
-- Name: idx_geocache_text_market; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_geocache_text_market ON public.geocode_cache USING btree (lower(raw_text), COALESCE(market_code, ''::character varying));


--
-- Name: idx_jobs_geo_location; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_geo_location ON public.jobs USING btree (geo_location_id) WHERE (geo_location_id IS NOT NULL);


--
-- Name: idx_jobs_geo_resolved; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_geo_resolved ON public.jobs USING btree (geo_resolved);


--
-- Name: idx_ml_test_feedback_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ml_test_feedback_run ON public.ml_test_feedback USING btree (test_run_id);


--
-- Name: idx_ml_test_feedback_site; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ml_test_feedback_site ON public.ml_test_feedback USING btree (test_run_id, site_url);


--
-- Name: ix_ats_pattern_proposals_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ats_pattern_proposals_status ON public.ats_pattern_proposals USING btree (status, ats_name);


--
-- Name: ix_career_pages_active_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_career_pages_active_created ON public.career_pages USING btree (created_at DESC) WHERE (is_active = true);


--
-- Name: ix_career_pages_company_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_career_pages_company_id ON public.career_pages USING btree (company_id);


--
-- Name: ix_career_pages_created_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_career_pages_created_at_desc ON public.career_pages USING btree (created_at DESC);


--
-- Name: ix_career_pages_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_career_pages_status ON public.career_pages USING btree (site_status) WHERE (is_active = true);


--
-- Name: ix_career_pages_url_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_career_pages_url_unique ON public.career_pages USING btree (url);


--
-- Name: ix_codex_improvement_runs_source_model_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_codex_improvement_runs_source_model_id ON public.codex_improvement_runs USING btree (source_model_id);


--
-- Name: ix_codex_improvement_runs_started_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_codex_improvement_runs_started_at ON public.codex_improvement_runs USING btree (started_at);


--
-- Name: ix_companies_active_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_active_name ON public.companies USING btree (name) WHERE (is_active = true);


--
-- Name: ix_companies_domain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_domain ON public.companies USING btree (domain);


--
-- Name: ix_companies_domain_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_domain_trgm ON public.companies USING gin (domain public.gin_trgm_ops);


--
-- Name: ix_companies_market_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_market_code ON public.companies USING btree (market_code) WHERE (is_active = true);


--
-- Name: ix_companies_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_name ON public.companies USING btree (name);


--
-- Name: ix_companies_name_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_name_trgm ON public.companies USING gin (name public.gin_trgm_ops);


--
-- Name: ix_companies_next_crawl_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_next_crawl_active ON public.companies USING btree (next_crawl_at) WHERE (is_active = true);


--
-- Name: ix_companies_quality_score; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_quality_score ON public.companies USING btree (quality_score) WHERE (quality_score IS NOT NULL);


--
-- Name: ix_companies_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_status ON public.companies USING btree (company_status) WHERE (is_active = true);


--
-- Name: ix_crawl_logs_company_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_crawl_logs_company_id ON public.crawl_logs USING btree (company_id);


--
-- Name: ix_crawl_logs_company_started; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_crawl_logs_company_started ON public.crawl_logs USING btree (company_id, started_at);


--
-- Name: ix_crawl_logs_started_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_crawl_logs_started_at_desc ON public.crawl_logs USING btree (started_at DESC);


--
-- Name: ix_crawl_logs_status_started; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_crawl_logs_status_started ON public.crawl_logs USING btree (status, started_at DESC);


--
-- Name: ix_crawler_test_data_external_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_crawler_test_data_external_id ON public.crawler_test_data USING btree (external_id);


--
-- Name: ix_drift_baselines_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_drift_baselines_active ON public.drift_baselines USING btree (model_name, feature_name) WHERE (is_active = true);


--
-- Name: ix_ever_passed_sites_ats; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ever_passed_sites_ats ON public.ever_passed_sites USING btree (ats_platform);


--
-- Name: ix_ever_passed_sites_last_seen; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ever_passed_sites_last_seen ON public.ever_passed_sites USING btree (last_seen_at DESC);


--
-- Name: ix_excluded_sites_domain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_excluded_sites_domain ON public.excluded_sites USING btree (domain);


--
-- Name: ix_experiments_model_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_experiments_model_status ON public.experiments USING btree (model_name, status);


--
-- Name: ix_gold_holdout_domains_ats_market; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gold_holdout_domains_ats_market ON public.gold_holdout_domains USING btree (ats_platform, market_id);


--
-- Name: ix_gold_holdout_domains_set; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gold_holdout_domains_set ON public.gold_holdout_domains USING btree (holdout_set_id);


--
-- Name: ix_gold_holdout_jobs_domain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gold_holdout_jobs_domain ON public.gold_holdout_jobs USING btree (holdout_domain_id);


--
-- Name: ix_gold_holdout_jobs_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gold_holdout_jobs_source ON public.gold_holdout_jobs USING btree (source);


--
-- Name: ix_gold_holdout_jobs_verification_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gold_holdout_jobs_verification_status ON public.gold_holdout_jobs USING btree (verification_status);


--
-- Name: ix_inference_metrics_hourly_bucket; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_inference_metrics_hourly_bucket ON public.inference_metrics_hourly USING btree (model_version_id, hour_bucket DESC);


--
-- Name: ix_job_site_test_data_external_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_site_test_data_external_id ON public.job_site_test_data USING btree (external_id);


--
-- Name: ix_job_tags_job_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_job_tags_job_id ON public.job_tags USING btree (job_id);


--
-- Name: ix_jobs_active_canonical_company; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_active_canonical_company ON public.jobs USING btree (company_id) WHERE ((is_active = true) AND (is_canonical = true));


--
-- Name: ix_jobs_active_canonical_seen; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_active_canonical_seen ON public.jobs USING btree (first_seen_at DESC) WHERE ((is_active = true) AND (is_canonical = true));


--
-- Name: ix_jobs_canonical_job_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_canonical_job_id ON public.jobs USING btree (canonical_job_id);


--
-- Name: ix_jobs_company_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_company_active ON public.jobs USING btree (company_id) WHERE (is_active = true);


--
-- Name: ix_jobs_company_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_company_id ON public.jobs USING btree (company_id);


--
-- Name: ix_jobs_description_enrichment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_description_enrichment ON public.jobs USING btree (description_enriched_at, is_active);


--
-- Name: ix_jobs_first_seen_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_first_seen_at ON public.jobs USING btree (first_seen_at);


--
-- Name: ix_jobs_fts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_fts ON public.jobs USING gin (to_tsvector('english'::regconfig, ((COALESCE(title, ''::text) || ' '::text) || COALESCE(description, ''::text))));


--
-- Name: ix_jobs_is_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_is_active ON public.jobs USING btree (is_active);


--
-- Name: ix_jobs_is_active_canonical; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_is_active_canonical ON public.jobs USING btree (is_canonical) WHERE (is_active = true);


--
-- Name: ix_jobs_is_active_first_seen; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_is_active_first_seen ON public.jobs USING btree (first_seen_at DESC) WHERE (is_active = true);


--
-- Name: ix_jobs_is_active_quality_score; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_is_active_quality_score ON public.jobs USING btree (quality_score) WHERE (is_active = true);


--
-- Name: ix_jobs_is_canonical; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_is_canonical ON public.jobs USING btree (is_canonical);


--
-- Name: ix_jobs_last_seen_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_last_seen_at ON public.jobs USING btree (last_seen_at);


--
-- Name: ix_jobs_location; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_location ON public.jobs USING btree (location_country, location_city);


--
-- Name: ix_jobs_location_country_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_location_country_active ON public.jobs USING btree (location_country) WHERE (is_active = true);


--
-- Name: ix_jobs_quality_score; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_quality_score ON public.jobs USING btree (quality_score);


--
-- Name: ix_jobs_quality_score_not_null; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_quality_score_not_null ON public.jobs USING btree (quality_score) WHERE ((quality_score IS NOT NULL) AND (is_active = true));


--
-- Name: ix_jobs_title_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_title_trgm ON public.jobs USING gin (title public.gin_trgm_ops) WHERE (is_active = true);


--
-- Name: ix_lead_imports_batch_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_lead_imports_batch_id ON public.lead_imports USING btree (batch_id);


--
-- Name: ix_lead_imports_category; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_lead_imports_category ON public.lead_imports USING btree (ad_origin_category);


--
-- Name: ix_lead_imports_country_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_lead_imports_country_id ON public.lead_imports USING btree (country_id);


--
-- Name: ix_lead_imports_origin_domain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_lead_imports_origin_domain ON public.lead_imports USING btree (origin_domain);


--
-- Name: ix_lead_imports_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_lead_imports_status ON public.lead_imports USING btree (status);


--
-- Name: ix_metric_snapshots_experiment; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_metric_snapshots_experiment ON public.metric_snapshots USING btree (experiment_id);


--
-- Name: ix_metric_snapshots_model; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_metric_snapshots_model ON public.metric_snapshots USING btree (model_version_id, computed_at DESC);


--
-- Name: ix_ml_model_test_runs_model_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ml_model_test_runs_model_id ON public.ml_model_test_runs USING btree (model_id);


--
-- Name: ix_model_versions_name_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_model_versions_name_status ON public.model_versions USING btree (name, status);


--
-- Name: ix_model_versions_one_champion_per_name; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_model_versions_one_champion_per_name ON public.model_versions USING btree (name) WHERE (status = 'champion'::text);


--
-- Name: ix_review_feedback_job_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_review_feedback_job_id ON public.review_feedback USING btree (job_id);


--
-- Name: ix_review_feedback_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_review_feedback_type ON public.review_feedback USING btree (review_type);


--
-- Name: ix_run_queue_drain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_run_queue_drain ON public.run_queue USING btree (queue_type, priority DESC, added_at) WHERE (status = 'pending'::text);


--
-- Name: ix_run_queue_unique_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_run_queue_unique_pending ON public.run_queue USING btree (queue_type, item_id) WHERE (status = 'pending'::text);


--
-- Name: ix_site_result_history_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_site_result_history_run ON public.site_result_history USING btree (run_id);


--
-- Name: ix_site_result_history_url_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_site_result_history_url_time ON public.site_result_history USING btree (url, observed_at DESC);


--
-- Name: ix_site_url_test_data_site_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_site_url_test_data_site_id ON public.site_url_test_data USING btree (site_id);


--
-- Name: ix_site_wrapper_test_data_crawler_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_site_wrapper_test_data_crawler_id ON public.site_wrapper_test_data USING btree (crawler_id);


--
-- Name: career_pages trg_career_pages_stats; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_career_pages_stats AFTER INSERT OR DELETE OR UPDATE ON public.career_pages FOR EACH ROW EXECUTE FUNCTION public.trg_career_pages_stats();


--
-- Name: crawl_logs trg_crawl_logs_stats; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_crawl_logs_stats AFTER INSERT OR DELETE OR UPDATE ON public.crawl_logs FOR EACH ROW EXECUTE FUNCTION public.trg_crawl_logs_stats();


--
-- Name: lead_imports trg_lead_imports_stats; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_lead_imports_stats AFTER INSERT OR DELETE OR UPDATE ON public.lead_imports FOR EACH ROW EXECUTE FUNCTION public.trg_lead_imports_stats();


--
-- Name: career_pages career_pages_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.career_pages
    ADD CONSTRAINT career_pages_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: codex_improvement_runs codex_improvement_runs_output_model_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codex_improvement_runs
    ADD CONSTRAINT codex_improvement_runs_output_model_id_fkey FOREIGN KEY (output_model_id) REFERENCES public.ml_models(id) ON DELETE SET NULL;


--
-- Name: codex_improvement_runs codex_improvement_runs_source_model_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codex_improvement_runs
    ADD CONSTRAINT codex_improvement_runs_source_model_id_fkey FOREIGN KEY (source_model_id) REFERENCES public.ml_models(id) ON DELETE SET NULL;


--
-- Name: codex_improvement_runs codex_improvement_runs_test_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codex_improvement_runs
    ADD CONSTRAINT codex_improvement_runs_test_run_id_fkey FOREIGN KEY (test_run_id) REFERENCES public.ml_model_test_runs(id) ON DELETE SET NULL;


--
-- Name: companies companies_market_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_market_code_fkey FOREIGN KEY (market_code) REFERENCES public.markets(code);


--
-- Name: company_stats company_stats_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_stats
    ADD CONSTRAINT company_stats_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: crawl_logs crawl_logs_career_page_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crawl_logs
    ADD CONSTRAINT crawl_logs_career_page_id_fkey FOREIGN KEY (career_page_id) REFERENCES public.career_pages(id) ON DELETE SET NULL;


--
-- Name: crawl_logs crawl_logs_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crawl_logs
    ADD CONSTRAINT crawl_logs_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE SET NULL;


--
-- Name: experiments experiments_challenger_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_challenger_version_id_fkey FOREIGN KEY (challenger_version_id) REFERENCES public.model_versions(id) ON DELETE SET NULL;


--
-- Name: experiments experiments_champion_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_champion_version_id_fkey FOREIGN KEY (champion_version_id) REFERENCES public.model_versions(id) ON DELETE SET NULL;


--
-- Name: experiments experiments_holdout_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.experiments
    ADD CONSTRAINT experiments_holdout_set_id_fkey FOREIGN KEY (holdout_set_id) REFERENCES public.gold_holdout_sets(id) ON DELETE RESTRICT;


--
-- Name: extraction_comparisons extraction_comparisons_career_page_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.extraction_comparisons
    ADD CONSTRAINT extraction_comparisons_career_page_id_fkey FOREIGN KEY (career_page_id) REFERENCES public.career_pages(id) ON DELETE CASCADE;


--
-- Name: extraction_comparisons extraction_comparisons_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.extraction_comparisons
    ADD CONSTRAINT extraction_comparisons_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id) ON DELETE CASCADE;


--
-- Name: geo_locations geo_locations_market_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geo_locations
    ADD CONSTRAINT geo_locations_market_code_fkey FOREIGN KEY (market_code) REFERENCES public.markets(code);


--
-- Name: geo_locations geo_locations_parent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geo_locations
    ADD CONSTRAINT geo_locations_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.geo_locations(id);


--
-- Name: geocode_cache geocode_cache_geo_location_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.geocode_cache
    ADD CONSTRAINT geocode_cache_geo_location_id_fkey FOREIGN KEY (geo_location_id) REFERENCES public.geo_locations(id);


--
-- Name: gold_holdout_domains gold_holdout_domains_holdout_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_domains
    ADD CONSTRAINT gold_holdout_domains_holdout_set_id_fkey FOREIGN KEY (holdout_set_id) REFERENCES public.gold_holdout_sets(id) ON DELETE CASCADE;


--
-- Name: gold_holdout_domains gold_holdout_domains_source_lead_import_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_domains
    ADD CONSTRAINT gold_holdout_domains_source_lead_import_id_fkey FOREIGN KEY (source_lead_import_id) REFERENCES public.lead_imports(id) ON DELETE SET NULL;


--
-- Name: gold_holdout_jobs gold_holdout_jobs_holdout_domain_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_jobs
    ADD CONSTRAINT gold_holdout_jobs_holdout_domain_id_fkey FOREIGN KEY (holdout_domain_id) REFERENCES public.gold_holdout_domains(id) ON DELETE CASCADE;


--
-- Name: gold_holdout_snapshots gold_holdout_snapshots_holdout_domain_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gold_holdout_snapshots
    ADD CONSTRAINT gold_holdout_snapshots_holdout_domain_id_fkey FOREIGN KEY (holdout_domain_id) REFERENCES public.gold_holdout_domains(id) ON DELETE CASCADE;


--
-- Name: inference_metrics_hourly inference_metrics_hourly_model_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inference_metrics_hourly
    ADD CONSTRAINT inference_metrics_hourly_model_version_id_fkey FOREIGN KEY (model_version_id) REFERENCES public.model_versions(id) ON DELETE CASCADE;


--
-- Name: job_tags job_tags_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_tags
    ADD CONSTRAINT job_tags_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id) ON DELETE CASCADE;


--
-- Name: jobs jobs_canonical_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_canonical_job_id_fkey FOREIGN KEY (canonical_job_id) REFERENCES public.jobs(id) ON DELETE SET NULL;


--
-- Name: jobs jobs_career_page_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_career_page_id_fkey FOREIGN KEY (career_page_id) REFERENCES public.career_pages(id) ON DELETE SET NULL;


--
-- Name: jobs jobs_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: jobs jobs_geo_location_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_geo_location_id_fkey FOREIGN KEY (geo_location_id) REFERENCES public.geo_locations(id);


--
-- Name: lead_imports lead_imports_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lead_imports
    ADD CONSTRAINT lead_imports_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.lead_import_batches(id) ON DELETE SET NULL;


--
-- Name: lead_imports lead_imports_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lead_imports
    ADD CONSTRAINT lead_imports_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE SET NULL;


--
-- Name: metric_snapshots metric_snapshots_experiment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric_snapshots
    ADD CONSTRAINT metric_snapshots_experiment_id_fkey FOREIGN KEY (experiment_id) REFERENCES public.experiments(id) ON DELETE SET NULL;


--
-- Name: metric_snapshots metric_snapshots_holdout_set_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric_snapshots
    ADD CONSTRAINT metric_snapshots_holdout_set_id_fkey FOREIGN KEY (holdout_set_id) REFERENCES public.gold_holdout_sets(id) ON DELETE CASCADE;


--
-- Name: metric_snapshots metric_snapshots_model_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric_snapshots
    ADD CONSTRAINT metric_snapshots_model_version_id_fkey FOREIGN KEY (model_version_id) REFERENCES public.model_versions(id) ON DELETE CASCADE;


--
-- Name: ml_model_test_runs ml_model_test_runs_model_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ml_model_test_runs
    ADD CONSTRAINT ml_model_test_runs_model_id_fkey FOREIGN KEY (model_id) REFERENCES public.ml_models(id) ON DELETE CASCADE;


--
-- Name: ml_test_feedback ml_test_feedback_test_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ml_test_feedback
    ADD CONSTRAINT ml_test_feedback_test_run_id_fkey FOREIGN KEY (test_run_id) REFERENCES public.ml_model_test_runs(id) ON DELETE CASCADE;


--
-- Name: model_versions model_versions_parent_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_versions
    ADD CONSTRAINT model_versions_parent_version_id_fkey FOREIGN KEY (parent_version_id) REFERENCES public.model_versions(id) ON DELETE SET NULL;


--
-- Name: review_feedback review_feedback_canonical_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.review_feedback
    ADD CONSTRAINT review_feedback_canonical_job_id_fkey FOREIGN KEY (canonical_job_id) REFERENCES public.jobs(id) ON DELETE SET NULL;


--
-- Name: review_feedback review_feedback_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.review_feedback
    ADD CONSTRAINT review_feedback_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id) ON DELETE CASCADE;


--
-- Name: site_templates site_templates_career_page_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.site_templates
    ADD CONSTRAINT site_templates_career_page_id_fkey FOREIGN KEY (career_page_id) REFERENCES public.career_pages(id) ON DELETE CASCADE;


--
-- Name: site_templates site_templates_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.site_templates
    ADD CONSTRAINT site_templates_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict iu68Ss0cB6rrzG37wyQSbaZqc58Abdxpo9B8DCTEwhBsJuJKc0fxtnzGvh9SuXd

