# JobHarvest — Comprehensive Build Prompt

## Project Overview

Build a locally-hosted, full-stack application called **JobHarvest** — an intelligent job listing crawler, extractor, and aggregation engine. The system crawls company websites, discovers careers/jobs pages, identifies individual job listings, extracts structured job data with high accuracy, and stores everything in a structured database. It includes a web-based dashboard for monitoring, management, and analytics.

**Primary Goal:** Build the most exhaustive, accurate, and up-to-date database of job listings by crawling company career pages directly.

**Critical Rule — Off-Limits Sites:** The following brands/sites must NEVER be crawled, scraped, or visited in any capacity (not even for link discovery): **Jora, SEEK, Jobstreet, JobsDB**. These are completely off-limits — do not send any requests to their domains whatsoever. Implement a hard domain blocklist enforced at the crawler level that prevents any request to these domains or their subdomains.

**Critical Rule — Aggregator Link Discovery:** Other aggregator sites (Indeed, Glassdoor, LinkedIn Jobs, ZipRecruiter, Monster, CareerBuilder, Reed, Totaljobs, etc.) must never have job data scraped FROM them. However, the system SHOULD follow outbound links from these sites to discover destination company career pages, then crawl those career pages directly. **Indeed is particularly encouraged as a link discovery source** — it has strong AU market coverage and should be a primary source for discovering Australian company career pages.

**Target Market:** The initial focus is the **Australian (AU) market**. Seed data, default search queries, aggregator link harvesting, location parsing, and salary parsing should all be AU-first. However, the system must be designed with multi-market extensibility from day one — the data model, configuration, and pipeline should support adding new markets (US, UK, EU, etc.) later via configuration rather than code changes. Implement a `markets` configuration system where each market defines: its aggregator sources and search queries, locale-specific salary/location parsing rules, seed company lists, and crawl priorities. When a new market is added, the system should automatically configure and begin crawling based on that market's configuration.

**Build Philosophy:** This project takes a "defense in depth" approach — for each major challenge (careers page discovery, job identification, field extraction), implement MULTIPLE complementary methods (heuristic, ML/AI, structural) that cross-validate each other. Accuracy and completeness are paramount.

---

## Tech Stack

### Backend
- **Language:** Python 3.12+
- **Framework:** FastAPI (async, high-performance API layer)
- **Task Queue:** Celery with Redis as broker and result backend
- **Crawling:** Scrapy + scrapy-playwright for JS-rendered pages
- **Secondary Crawler:** crawl4ai as an alternative extraction-oriented crawler
- **Browser Automation:** Playwright (for JS-heavy sites, infinite scroll, "Load More" buttons)
- **HTML Parsing:** BeautifulSoup4, lxml, html2text, markdownify
- **Structured Data Extraction:** extruct (JSON-LD, Microdata, RDFa, OpenGraph)
- **LLM Integration:** Ollama (local) running Llama 3.1 8B (or whatever the latest small-but-capable model is at build time — research this)
- **Structured LLM Output:** instructor library (Pydantic-based structured extraction from LLMs)
- **ML/NLP:** scikit-learn, sentence-transformers (for embeddings/classification)
- **HTTP Fingerprinting:** curl_cffi (for TLS fingerprint mimicry on protected sites)
- **Scheduling:** Celery Beat for periodic tasks
- **Pipeline Orchestration:** Prefect (for monitoring complex workflows)

### Frontend
- **Framework:** React 18+ with TypeScript
- **Build Tool:** Vite
- **UI Library:** shadcn/ui + Tailwind CSS
- **Charting:** Recharts
- **State Management:** TanStack Query (React Query) for server state
- **Routing:** React Router v6

### Database
- **Primary Database:** PostgreSQL 16+ (structured job data, site configurations, crawl metadata)
- **Cache/Queue:** Redis (task queue, crawl rate limiting, page content hashing, caching)
- **Object Storage:** Local filesystem with structured directories (raw HTML snapshots, screenshots) — design the storage interface so it could be swapped to MinIO/S3 later

### Infrastructure (All Local/Docker via Colima)
- **Container Runtime:** Colima (NOT Docker Desktop) — use `colima start --cpu 4 --memory 8 --disk 60` or similar as the container runtime. All Docker and Docker Compose commands run against the Colima daemon. Include setup instructions for Colima in the project README.
- **Containerization:** Docker Compose for the full stack
- **Reverse Proxy:** Caddy or Traefik (for local HTTPS and routing)
- **Ollama:** Running as a Docker service for local LLM inference

---

## Database Schema Design

Design and implement the following PostgreSQL schema. Use SQLAlchemy as the ORM with Alembic for migrations.

### Core Tables

**companies**
- id (UUID, PK)
- name (text, not null)
- domain (text, unique, not null) — e.g. "example.com"
- root_url (text, not null)
- market_code (text, FK → markets.code, default "AU") — which market this company belongs to
- discovered_via (text) — how we found this company (seed, aggregator_link, search, crawl_expansion)
- ats_platform (text, nullable) — detected ATS: greenhouse, lever, workday, bamboohr, icims, taleo, smartrecruiters, ashby, custom, unknown
- ats_confidence (float) — confidence score of ATS detection
- crawl_priority (int, default 5) — 1=highest, 10=lowest
- crawl_frequency_hours (int, default 24)
- last_crawl_at (timestamptz)
- next_crawl_at (timestamptz)
- is_active (bool, default true)
- requires_js_rendering (bool, default false)
- anti_bot_level (text, default 'none') — none, basic, moderate, aggressive
- notes (text)
- created_at, updated_at (timestamptz)

**career_pages**
- id (UUID, PK)
- company_id (FK → companies)
- url (text, not null)
- page_type (text) — listing_page, department_page, location_page, single_job_page, ats_embed
- discovery_method (text) — heuristic, llm_classification, ats_fingerprint, manual, link_following
- discovery_confidence (float)
- is_primary (bool) — is this the main careers landing page
- is_paginated (bool)
- pagination_type (text) — url_param, load_more_button, infinite_scroll, next_link, none
- pagination_selector (text, nullable) — CSS selector for pagination element
- requires_js_rendering (bool, default false)
- last_content_hash (text) — to detect changes
- last_crawled_at (timestamptz)
- last_extraction_at (timestamptz)
- is_active (bool, default true)
- created_at, updated_at (timestamptz)

**jobs**
- id (UUID, PK)
- company_id (FK → companies)
- career_page_id (FK → career_pages, nullable)
- source_url (text, not null) — the specific page this job was extracted from
- external_id (text, nullable) — the job ID from the source site if available
- title (text, not null)
- description (text) — full job description, stored as cleaned text
- description_html (text) — original HTML of the description
- location_raw (text) — raw location string as found on page
- location_city (text, nullable)
- location_state (text, nullable)
- location_country (text, nullable)
- is_remote (bool, nullable)
- remote_type (text, nullable) — fully_remote, hybrid, onsite, flexible
- employment_type (text, nullable) — full_time, part_time, contract, internship, temporary, volunteer
- seniority_level (text, nullable) — entry, mid, senior, lead, director, executive
- department (text, nullable)
- team (text, nullable)
- salary_raw (text, nullable) — raw salary string
- salary_min (numeric, nullable)
- salary_max (numeric, nullable)
- salary_currency (text, nullable)
- salary_period (text, nullable) — hourly, daily, weekly, monthly, annual
- requirements (text, nullable) — as extracted text
- benefits (text, nullable) — as extracted text
- application_url (text, nullable)
- date_posted (date, nullable)
- date_expires (date, nullable)
- first_seen_at (timestamptz, not null)
- last_seen_at (timestamptz, not null)
- is_active (bool, default true) — set to false when no longer found on site
- extraction_method (text) — structural, llm, schema_org, ats_api, hybrid
- extraction_confidence (float) — overall confidence score
- raw_data (jsonb) — the complete raw extracted data before normalization
- created_at, updated_at (timestamptz)

**job_tags**
- id (UUID, PK)
- job_id (FK → jobs)
- tag_type (text) — skill, technology, qualification, industry, category
- tag_value (text)
- confidence (float)

**crawl_logs**
- id (UUID, PK)
- company_id (FK → companies, nullable)
- career_page_id (FK → career_pages, nullable)
- crawl_type (text) — discovery, extraction, verification, full_crawl
- status (text) — pending, running, success, partial_success, failed, blocked
- started_at (timestamptz)
- completed_at (timestamptz)
- pages_crawled (int)
- jobs_found (int)
- jobs_new (int)
- jobs_updated (int)
- jobs_removed (int)
- error_message (text, nullable)
- error_details (jsonb, nullable)
- method_used (text) — which extraction pipeline was used
- duration_seconds (float)

**site_templates**
- id (UUID, PK)
- company_id (FK → companies)
- career_page_id (FK → career_pages)
- template_type (text) — listing_page, detail_page
- selectors (jsonb) — mapped CSS/XPath selectors for each schema field
- learned_via (text) — manual, llm_bootstrapped, auto_learned
- accuracy_score (float) — tracked accuracy over time
- last_validated_at (timestamptz)
- is_active (bool, default true)
- created_at, updated_at (timestamptz)

**extraction_comparisons**
- id (UUID, PK)
- job_id (FK → jobs)
- career_page_id (FK → career_pages)
- method_a (text) — extraction method name
- method_b (text) — extraction method name
- method_a_result (jsonb)
- method_b_result (jsonb)
- agreement_score (float) — how much the two methods agree
- resolved_result (jsonb, nullable) — final merged result after resolution
- resolution_method (text) — auto, llm_tiebreak, manual
- created_at (timestamptz)

**aggregator_sources**
- id (UUID, PK)
- name (text) — e.g. "Indeed", "LinkedIn", "Glassdoor"
- base_url (text)
- market (text) — which market this source serves, e.g. "AU", "US", "UK", "global"
- is_active (bool, default true)
- purpose (text) — "link_discovery_only" (never scrape content, only follow links to company sites)
- last_link_harvest_at (timestamptz)
- NOTE: Jora, SEEK, Jobstreet, and JobsDB must NEVER appear in this table. They are hard-blocked at the crawler level.

**blocked_domains**
- id (UUID, PK)
- domain (text, unique, not null) — e.g. "seek.com.au", "jora.com", "jobstreet.com", "jobsdb.com"
- reason (text) — why this domain is blocked
- created_at (timestamptz)
- NOTE: Pre-populate with all known domains/subdomains for Jora, SEEK, Jobstreet, JobsDB. The crawler must check every outbound request against this blocklist before making the request.

**markets**
- id (UUID, PK)
- code (text, unique, not null) — e.g. "AU", "US", "UK"
- name (text) — e.g. "Australia", "United States", "United Kingdom"
- is_active (bool, default true)
- default_currency (text) — e.g. "AUD", "USD", "GBP"
- locale (text) — e.g. "en-AU", "en-US", "en-GB"
- salary_parsing_config (jsonb) — market-specific salary patterns, ranges, and norms
- location_parsing_config (jsonb) — market-specific location formats (e.g. AU uses "Sydney, NSW" vs US "San Francisco, CA")
- aggregator_search_queries (jsonb) — default search queries for this market's aggregators
- created_at, updated_at (timestamptz)
- NOTE: Seed with "AU" as the only active market. Other markets (US, UK, etc.) can be added as inactive and switched on later.

### Indexes
Create appropriate indexes on: jobs(company_id), jobs(is_active), jobs(title), jobs(location_country, location_city), jobs(first_seen_at), jobs(last_seen_at), career_pages(company_id), career_pages(url), companies(domain), crawl_logs(company_id, started_at), and full-text search indexes on jobs(title, description).

---

## Architecture: Multi-Method Pipeline

Each stage of the pipeline uses multiple approaches that cross-validate each other. Implement ALL of the following.

### Stage 1: Site Ingestion & ATS Fingerprinting

**1a. Seed URL Ingestion**
- Accept a list of company URLs via the dashboard or API (CSV upload, manual entry, or bulk paste)
- Normalize URLs (strip tracking params, ensure scheme, resolve redirects)
- Deduplicate against existing companies by domain

**1b. ATS Fingerprinting Engine**
Build a fingerprinting system that detects which Applicant Tracking System a company uses. This is HIGH PRIORITY because known ATS platforms have predictable structures.

Detection signals to check:
- **Greenhouse:** URLs containing `boards.greenhouse.io` or `job-boards.greenhouse.io`, iframes pointing to greenhouse, `<meta name="greenhouse">` tags, API endpoints at `/gh_jboard`
- **Lever:** URLs containing `jobs.lever.co`, Lever-specific CSS classes, `lever-jobs-container` divs
- **Workday:** URLs containing `myworkdayjobs.com` or `wd5.myworkdayjobs.com`, Workday-specific script references
- **BambooHR:** URLs containing `bamboohr.com/careers` or embedded BambooHR widgets
- **iCIMS:** URLs containing `careers-*.icims.com` or iCIMS-specific page structures
- **Taleo:** URLs containing `taleo.net` or Oracle-specific career page structures
- **SmartRecruiters:** URLs containing `careers.smartrecruiters.com` or SmartRecruiters embeds
- **Ashby:** URLs containing `jobs.ashbyhq.com` or Ashby-specific elements
- **Jobvite:** URLs containing `jobs.jobvite.com`
- **JazzHR:** URLs containing `jazzhr.com` or embedded JazzHR widgets

For each known ATS, implement a dedicated extractor class that knows the exact page structure and can reliably extract all job data. These are your highest-accuracy extractors.

Research the current state of each ATS's public page structure at build time. Check their documentation, look at live examples, and build robust selectors.

**1c. Aggregator Link Harvesting**
For PERMITTED aggregator sites only, implement a link-discovery-only crawler that:
- Searches the aggregator for job listings (initially focused on AU market queries)
- Extracts the outbound link to the company's actual career page
- Adds the company domain and career page URL to the database
- NEVER extracts job content from the aggregator page itself
- Marks the company's `discovered_via` as "aggregator_link"
- Checks every URL against the blocked_domains table before making any request

**Permitted aggregator sources for AU market:** Indeed (au.indeed.com — primary source, encouraged), LinkedIn Jobs, Glassdoor, CareerOne, and other non-blocked aggregators.

**HARD BLOCKED — never crawl under any circumstances:** Jora (jora.com, au.jora.com, and all subdomains), SEEK (seek.com.au and all subdomains), Jobstreet (jobstreet.com and all regional variants), JobsDB (jobsdb.com and all regional variants). Implement this as a domain blocklist checked at the lowest level of the crawl stack — before any HTTP request is made. Log and discard any attempted requests to blocked domains.

### Stage 2: Careers Page Discovery

Given a company's root URL, find their careers/jobs pages. Use ALL of the following methods and merge results.

**2a. Heuristic URL & Link Analysis**
- Crawl the site to depth 2-3 (respect robots.txt, rate limit to 1 request per 2 seconds)
- Score every discovered URL using weighted signals:
  - URL path patterns: `/careers`, `/jobs`, `/opportunities`, `/vacancies`, `/work-with-us`, `/join`, `/openings`, `/hiring`, `/employment`, `/talent`, `/people`, `/team/join` (weight: 0.3)
  - Anchor text of links pointing to the URL: "careers", "jobs", "join us", "we're hiring", "work with us", "open positions", "view openings" (weight: 0.3)
  - Page title and meta description containing job-related keywords (weight: 0.2)
  - Navigation placement: links in main nav or footer get higher scores (weight: 0.1)
  - Presence of structured data (JobPosting schema) (weight: 0.1)
- Return all URLs scoring above a configurable threshold

**2b. LLM Page Classification**
- For top candidate pages (and any pages scoring in the "uncertain" range from 2a), fetch the page content
- Convert to clean markdown using markdownify
- Truncate to ~2000 tokens
- Send to Ollama with this classification prompt:

```
You are a classifier that determines if a web page is a careers/jobs page.

Page URL: {url}
Page Title: {title}
Page Content (truncated):
{content}

Classify this page into exactly one category:
- CAREERS_LISTING: A page that lists multiple job openings/positions
- CAREERS_LANDING: A careers landing page that links to job listings but doesn't list jobs itself
- SINGLE_JOB: A page for a single specific job posting
- CAREERS_RELATED: Related to careers (about the team, culture, benefits) but no job listings
- NOT_CAREERS: Not related to careers or jobs

Respond with JSON only:
{"classification": "...", "confidence": 0.0-1.0, "reasoning": "one sentence"}
```

**2c. Trained Classifier (Bootstrap from LLM labels)**
- After the system has classified ~500+ pages via the LLM method, train a lightweight classifier:
  - Extract features: URL tokens, page title tokens, heading text, meta description, body text TF-IDF
  - Train a scikit-learn pipeline: TfidfVectorizer → LogisticRegression (or GradientBoosting)
  - Store the model and use it as the fast primary classifier, with LLM as fallback for low-confidence predictions
  - Retrain periodically as more labeled data accumulates
- This should run automatically — collect LLM classification results, and when sufficient data exists, trigger model training

**2d. ATS Detection Shortcut**
- If the ATS fingerprinting (Stage 1b) identified the ATS, the careers page URL is often already known or follows a predictable pattern
- Skip discovery for these sites and go directly to extraction

**Merge Strategy:**
- If ATS is detected → use ATS-known URL (highest confidence)
- Otherwise, combine scores from heuristic + LLM + trained classifier
- A page confirmed by 2+ methods gets highest confidence
- Store all discovered career pages with their discovery method and confidence

### Stage 3: Job Listing Identification

Given a careers page, find all individual job listings. Use ALL methods.

**3a. Structured Data Extraction (Highest Priority)**
- Use `extruct` to check for JobPosting schema.org markup (JSON-LD, Microdata, RDFa)
- If present, this is the gold standard — extract directly
- Many sites include this for Google for Jobs compliance

**3b. ATS-Specific Extraction**
- If the site uses a known ATS, use the ATS-specific extractor
- These know the exact DOM structure and can extract with near-100% accuracy

**3c. Repeating Block Detection (Structural Analysis)**
- Parse the DOM and identify repeating structural patterns
- Algorithm:
  1. Find all elements that are siblings with the same tag and similar class names
  2. Group elements that share the same parent and have similar child structures
  3. Score groups by: number of repetitions (more = more likely job listings), presence of links (job listings almost always link to detail pages), presence of text that looks like job titles (short text, title case), presence of location-like text
  4. The highest-scoring group is your job listing container
- Extract individual listing elements from the identified container
- For each listing element, extract: title (the most prominent text/link), URL (the href of the primary link), and any visible metadata (location, department, date)

**3d. LLM Listing Identification**
- Convert the page to clean markdown
- Send to Ollama with a structured extraction prompt:

```
You are analyzing a careers page to identify all job listings.

Page URL: {url}
Page Content:
{markdown_content}

Identify every job listing visible on this page. For each job, extract:
- title: The job title
- url: The link to the full job posting (absolute URL)
- location: If visible
- department: If visible
- any other visible metadata

Respond with JSON only:
{"jobs": [{"title": "...", "url": "...", "location": "...", "department": "...", "metadata": {...}}, ...]}

IMPORTANT: Include ALL jobs. Do not skip any. If you are unsure whether something is a job listing, include it with a note.
```

**3e. Pagination Handling**
Before declaring extraction complete for a careers page, detect and handle pagination:
- Check for URL-based pagination: `?page=2`, `?offset=20`, `?start=10`, `/page/2/`
- Check for "Load More" buttons: look for buttons/links with text like "Load More", "Show More", "View All", "See More Jobs"
- Check for infinite scroll: use Playwright to scroll to bottom, wait for new content, repeat until no new content loads
- Check for "Next" links: pagination navs with next/previous links
- Extract from ALL pages, not just the first

**3f. Cross-Validation**
- Compare results from methods 3a-3d
- If structured data (3a) is present, use it as ground truth and validate others against it
- If multiple methods agree on a listing, high confidence
- If a listing appears in one method but not others, flag for review but include it (err on the side of completeness)
- Log disagreements in extraction_comparisons table for analysis

### Stage 4: Job Detail Extraction & Schema Mapping

For each identified job listing, extract full structured data. Use ALL methods.

**4a. Schema.org / Structured Data**
- On the job detail page, check for JobPosting structured data first
- Parse all available structured data using extruct
- Map to our schema

**4b. ATS-Specific Detail Extraction**
- If using a known ATS, apply the ATS-specific detail extractor
- These know exactly where title, description, location, requirements, etc. live in the DOM

**4c. LLM Structured Extraction (Primary for unknown sites)**
- Use the `instructor` library with Ollama to extract structured data directly into Pydantic models
- Define the Pydantic model matching our job schema
- Send the cleaned page content and get back a validated, typed response
- This is your most flexible extractor — it handles arbitrary page structures

```python
# Example structure for the instructor-based extractor
class ExtractedJob(BaseModel):
    title: str
    description: str
    location_raw: Optional[str]
    is_remote: Optional[bool]
    remote_type: Optional[Literal["fully_remote", "hybrid", "onsite", "flexible"]]
    employment_type: Optional[Literal["full_time", "part_time", "contract", "internship", "temporary"]]
    seniority_level: Optional[str]
    department: Optional[str]
    team: Optional[str]
    salary_raw: Optional[str]
    requirements: Optional[str]
    benefits: Optional[str]
    date_posted: Optional[str]
    date_expires: Optional[str]
    skills_mentioned: List[str]
    qualifications: List[str]
```

**4d. Template Learning (Auto-Generate CSS Selectors)**
This is a key innovation. The process:
1. For a new site, run LLM extraction (4c) on 3-5 different job detail pages
2. For each page, also store the full DOM
3. Compare the DOMs: find which CSS selectors/XPath expressions consistently contain the same type of data that the LLM extracted
4. Generate a selector map: `{"title": "h1.job-title", "location": "span.location", "description": "div.job-description", ...}`
5. Store this in the site_templates table
6. For subsequent crawls of this site, use fast selector-based extraction instead of slow LLM extraction
7. Periodically validate templates by running LLM extraction on a sample and comparing results

**4e. Location Parsing & Normalization**
- Parse raw location strings into structured components (city, state, country)
- Detect remote/hybrid/onsite from both the location field and the description text
- AU-first defaults: assume AUD currency and Australian locations when ambiguous. Australian state abbreviations (NSW, VIC, QLD, WA, SA, TAS, ACT, NT) should be recognized natively.
- Use a combination of:
  - Rule-based parsing for common AU formats: "Sydney, NSW", "Melbourne VIC 3000", "Brisbane, Queensland", "CBD", "Remote - Australia", "WFH", etc.
  - A geocoding library (geopy with Nominatim) for ambiguous locations
  - LLM parsing as fallback for unusual formats
- The location parser should be configurable per market (loaded from the markets table) so that when US/UK markets are added, their locale-specific patterns are handled without code changes

**4f. Salary Parsing & Normalization**
- Extract salary from raw strings into structured min/max/currency/period
- AU-first: prioritize Australian salary formats and default to AUD when currency symbol is ambiguous (bare "$" should default to AUD for AU-market jobs)
- Handle AU-specific formats: "$80,000 - $120,000", "$80K - $120K", "$40/hr", "$800/day", "A$90,000 - A$110,000 per annum", "$120,000 + super", "$100K-$120K + super + bonus", "Competitive", "DOE"
- Note: Australian salaries often reference "super" (superannuation) — extract this as a separate note/benefit when mentioned
- Also handle formats from other markets for future extensibility: "£45-55k", "€50,000", "US$90,000"
- Rule-based parser with regex patterns for common formats, configurable per market
- Currency detection (handle AUD as default, plus USD, GBP, EUR, NZD, SGD, etc.)
- Normalize all to annual equivalent where possible

**4g. Cross-Validation & Confidence Scoring**
- When multiple extraction methods are used, compare results field by field
- Calculate an agreement score
- For disagreements, use a resolution strategy:
  1. Structured data (schema.org) wins for fields it covers
  2. ATS-specific extractor wins for known ATS sites
  3. If LLM and structural methods disagree, use a second LLM call as tiebreaker
  4. Log all disagreements for analysis
- Assign per-field confidence scores and an overall extraction confidence score

### Stage 5: Change Detection & Continuous Crawling

**5a. Content Hashing**
- Store a structural hash of each career page (hash the text content, ignoring dynamic elements like timestamps, session tokens, CSRF tokens)
- On re-crawl, compare hash — only trigger full extraction if content changed
- Also hash individual job listings to detect modifications

**5b. Job Lifecycle Tracking**
- Track each job through states: first_seen → active → last_seen → removed
- A job is "removed" when it's no longer found on the career page after 2-3 consecutive crawls (to handle temporary page errors)
- Track how long jobs stay active (useful analytics)

**5c. Smart Scheduling**
- Sites with more frequent changes get crawled more often
- Sites that rarely change get crawled less often
- Adapt crawl frequency based on observed change rates
- Implement with Celery Beat + dynamic schedule stored in the database
- Default: daily for high-priority sites, every 3 days for medium, weekly for low

**5d. Failure Handling & Alerting**
- Track consecutive failures per site
- If a site fails 3+ times in a row, reduce priority and flag for review
- Detect site redesigns (when the template breaks — extraction results suddenly differ dramatically from history)
- Detect anti-bot escalation (when requests start getting blocked that previously worked)

---

## Frontend Dashboard

Build a React + TypeScript SPA with the following pages/features. Use shadcn/ui components and Tailwind CSS. The design should be professional, clean, and data-dense — think operations dashboard, not marketing site.

### Dashboard Home
- Summary statistics: total companies, total active jobs, jobs added today/this week, crawl success rate, extraction confidence distribution
- Crawl activity timeline (last 24h, 7d, 30d)
- Recent crawl results feed (live-updating)
- Health indicators: queue depth, failed crawls, sites needing attention
- Charts: jobs by country, jobs by employment type, top hiring companies, new jobs trend over time

### Companies Management
- Searchable, sortable, filterable table of all companies
- Columns: name, domain, ATS platform, active jobs count, last crawl, crawl status, extraction confidence avg
- Filters: ATS type, crawl status, confidence range, active/inactive
- Bulk actions: activate/deactivate, change crawl priority, trigger re-crawl
- Add company form: URL input with auto-detection of domain, ATS, and career pages
- CSV bulk import
- Company detail page: all career pages, all jobs, crawl history, template configurations

### Career Pages
- List of all discovered career pages with their classification, discovery method, and confidence
- Status indicators: active, inactive, broken, changed
- For each page: view current template mappings, trigger re-discovery, view raw HTML snapshot

### Jobs Browser
- Searchable, filterable table/grid of all extracted jobs
- Full-text search across title and description
- Filters: company, location, remote type, employment type, seniority, department, date range, salary range, extraction confidence
- Job detail view: all extracted fields, extraction method, confidence scores, raw data, source URL
- Export: CSV, JSON

### Crawl Monitor
- Real-time view of active crawl tasks
- Crawl history with detailed logs
- Per-site crawl performance metrics
- Error analysis: most common failure reasons, sites with degrading performance
- Queue management: view pending tasks, cancel tasks, reprioritize

### Extraction Analytics
- Method accuracy comparison: how often do different methods agree?
- Confidence score distributions
- Field coverage analysis: what percentage of jobs have salary? location? requirements?
- Template health: which templates are degrading?
- Extraction comparison viewer: side-by-side view of different methods' results for the same job

### Settings
- Global crawl settings: default rate limits, default crawl frequency, concurrent crawler count
- Ollama model configuration
- Aggregator source management
- System health: Redis, Postgres, Ollama, Celery worker status
- Manual controls: trigger full re-crawl, retrain classifier, rebuild templates

---

## API Design

FastAPI backend with the following endpoint groups. Use proper REST conventions, pagination, filtering.

### /api/companies
- GET / — list companies (with filters, search, pagination)
- POST / — add company (by URL, auto-triggers ATS detection and career page discovery)
- POST /bulk — bulk import from CSV
- GET /{id} — company detail
- PATCH /{id} — update company settings
- POST /{id}/crawl — trigger immediate crawl
- GET /{id}/jobs — list jobs for this company
- GET /{id}/crawl-history — crawl logs for this company

### /api/career-pages
- GET / — list career pages
- GET /{id} — career page detail with template info
- POST /{id}/recrawl — trigger re-crawl of this page
- GET /{id}/template — current extraction template
- POST /{id}/validate-template — run template validation against LLM extraction

### /api/jobs
- GET / — list jobs (with full-text search, filters, pagination, sorting)
- GET /{id} — job detail with full extraction data
- GET /stats — aggregate statistics
- GET /export — export as CSV or JSON

### /api/crawl
- GET /active — currently running crawl tasks
- GET /queue — pending tasks in queue
- GET /history — crawl logs with filters
- POST /trigger-full — trigger a full crawl cycle for all active companies
- DELETE /cancel/{task_id} — cancel a queued or running task

### /api/analytics
- GET /extraction-accuracy — method comparison stats
- GET /field-coverage — what % of jobs have each field populated
- GET /discovery-stats — career page discovery success rates
- GET /trends — jobs over time, by location, by type

### /api/system
- GET /health — health check for all services (Postgres, Redis, Ollama, Celery)
- GET /config — current system configuration
- PATCH /config — update configuration
- POST /retrain-classifier — trigger classifier retraining
- POST /rebuild-templates — trigger template validation and rebuilding

---

## Docker Compose Setup

Create a `docker-compose.yml` with the following services:

1. **postgres** — PostgreSQL 16, with a volume for data persistence, initialized with the schema
2. **redis** — Redis 7, used for Celery broker, caching, rate limiting
3. **ollama** — Ollama service with a volume for model storage. On first start, pull the appropriate model automatically.
4. **api** — FastAPI application (the main backend)
5. **celery-worker** — Celery worker(s) for crawl tasks (configure concurrency appropriately)
6. **celery-beat** — Celery Beat scheduler for periodic tasks
7. **frontend** — React app served via nginx (or Vite dev server for development)

Include:
- A `.env.example` file with all configuration variables
- Health checks for all services
- Proper dependency ordering (wait for postgres, redis, ollama to be ready)
- Volume mounts for persistent data
- A `Makefile` or shell scripts for common operations (start, stop, reset, seed, etc.)

---

## Implementation Order

Build in this sequence. Each phase should result in a working, testable system.

### Phase 1: Foundation
1. Set up the project structure (monorepo with `/backend`, `/frontend`, `/docker` directories)
2. Docker Compose with Postgres, Redis, Ollama
3. Database schema with SQLAlchemy models and Alembic migrations
4. Basic FastAPI app with health check endpoint
5. Basic React app with routing and layout shell

### Phase 2: Core Crawling
1. ATS fingerprinting engine (all major ATS platforms)
2. Basic Scrapy crawler with Playwright integration
3. Careers page discovery (heuristic method first)
4. Celery task setup for async crawling

### Phase 3: Extraction Pipeline
1. Schema.org / structured data extraction
2. ATS-specific extractors (start with Greenhouse, Lever, Workday — the most common)
3. LLM-based extraction with instructor
4. Repeating block detection (structural analysis)
5. Cross-validation and confidence scoring

### Phase 4: Intelligence Layer
1. LLM-based page classification for career page discovery
2. Template learning system (auto-generate selectors from LLM results)
3. Location and salary parsing/normalization
4. Job deduplication (same job found via different paths)
5. Tag extraction (skills, technologies, qualifications)

### Phase 5: Frontend
1. Dashboard home with summary stats
2. Companies management (CRUD, bulk import)
3. Jobs browser with search and filters
4. Crawl monitor
5. Extraction analytics
6. Settings page

### Phase 6: Scheduling & Operations
1. Celery Beat scheduled crawling
2. Change detection and smart re-crawling
3. Job lifecycle tracking
4. Failure handling and alerting
5. Aggregator link harvesting (for discovering new companies)

### Phase 7: Advanced
1. Train sklearn classifier from accumulated LLM labels
2. Self-discovery: search-based company discovery
3. Crawl expansion: follow links from known sites to discover new companies
4. Anti-bot handling improvements (curl_cffi, browser profile rotation)
5. Performance optimization (batch LLM calls, parallel extraction)

---

## Key Implementation Notes

### Rate Limiting & Politeness
- Always respect robots.txt
- Default rate limit: 1 request per 2 seconds per domain
- Use a per-domain rate limiter in Redis
- Set a descriptive User-Agent: `JobHarvest/1.0 (job-listing-research; contact@yourdomain.com)`
- Implement exponential backoff on failures

### Error Handling
- Every crawl task must be wrapped in proper error handling
- Distinguish between: network errors (retry), HTTP errors (log and handle), parsing errors (log and flag), anti-bot blocks (log, backoff, flag)
- Never let a single site's failure affect other sites' crawling

### LLM Usage Optimization
- LLM calls are the bottleneck — use them judiciously
- Always try faster methods first (structured data, ATS extractors, templates)
- Only fall back to LLM when other methods fail or for cross-validation
- Cache LLM results aggressively
- Batch similar requests when possible

### Testing
- Write tests for each extraction method using saved HTML fixtures
- Create a test suite of ~20 diverse company career pages (different structures, ATS platforms, complexities)
- Test ATS-specific extractors against live sites
- Integration tests for the full pipeline: URL → discovered careers page → extracted jobs

### Code Quality
- Type hints throughout the Python codebase
- Pydantic models for all data transfer objects
- Proper logging (structured JSON logging)
- Configuration via environment variables with sensible defaults

---

## Seed Data

Include a seed script that populates the database with:

**Initial AU market configuration:**
- Create the "AU" market record as the only active market, with AUD currency, en-AU locale, and AU-specific salary/location parsing patterns

**50+ Australian company URLs across:**
- Companies using each major ATS platform (at least 3 per ATS) — research which Australian companies use Greenhouse, Lever, Workday, etc. and include real examples
- Companies with custom-built career pages
- Companies of various sizes: ASX-listed enterprises (BHP, Telstra, CBA, Woolworths, etc.), mid-market companies, and startups
- A mix of industries: tech, finance/banking, mining/resources, healthcare, retail, government, education, professional services
- Include companies headquartered in various Australian cities (Sydney, Melbourne, Brisbane, Perth, Adelaide, etc.)

**Aggregator sources (link discovery only):**
- Indeed AU (au.indeed.com) — active, primary link discovery source
- LinkedIn Jobs — active
- Glassdoor AU — active
- CareerOne — active
- Other non-blocked AU aggregators as discovered during research

**Blocked domains (pre-populated, enforced at crawler level):**
- seek.com.au (and all subdomains like www.seek.com.au, talent.seek.com.au)
- jora.com, au.jora.com (and all subdomains/regional variants)
- jobstreet.com (and all regional variants like jobstreet.com.au, jobstreet.com.my, etc.)
- jobsdb.com (and all regional variants)

**Inactive market stubs (for future expansion):**
- Create inactive market records for US, UK, NZ, SG so the structure is in place. These can be activated later and will auto-configure their own aggregator sources and seed lists.

---

## Success Metrics

The system should be evaluated against:
- **Discovery Rate:** What percentage of seeded companies have their career pages successfully found?
- **Extraction Completeness:** What percentage of visible jobs on a career page are successfully extracted?
- **Field Accuracy:** When fields are extracted, are they correct? (Compare against manual verification)
- **Field Coverage:** What percentage of jobs have title (should be 100%), location (>90%), description (>95%), salary (as available on site), employment type (>80%)?
- **Freshness:** Are jobs detected within one crawl cycle of appearing on the source site?
- **False Positive Rate:** How often does the system extract non-job content as a job?

Build a simple evaluation script that can be run against a manually-verified test set of 20 companies to measure these metrics.

---

## Final Notes

- This is a complex system. Build iteratively — get each phase working before moving to the next.
- Prioritize the ATS-specific extractors — they give you the highest accuracy for the most sites with the least effort.
- The template learning system (auto-generating selectors from LLM results) is the key scaling innovation — invest time in getting it right.
- When in doubt, err on the side of extracting more data (false positives are easier to clean than missed listings).
- Keep the raw data — always store raw HTML and raw extraction results so you can reprocess later.
- If you hit a decision point where you need guidance, document the options and tradeoffs clearly and ask — but do not delegate manual tasks like researching ATS page structures, writing seed data, or training models. Handle all of this autonomously.