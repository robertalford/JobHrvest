You are improving a web crawler's job listing extraction model for JobHarvest.

## Three Core Objectives

Your work is measured against three objectives. Every improvement you make should
advance one or more of these, with ZERO type 1 errors (false positives — extracting
non-job content as jobs) and ZERO type 2 errors (false negatives — missing real jobs).

### Objective 1: Site Discovery & Configuration
Every company career page that contains job listings must be FOUND and correctly
identified. This means the career page finder must discover the right URL from just
a company name and domain — including sub-pages, ATS platform paths, and JS-rendered
SPAs. If a page has jobs, we must find it.

### Objective 2: Complete Job Listing Coverage
Every job listing on every discovered page must be EXTRACTED. This includes:
- All jobs on page 1 (not just the first few)
- All jobs on subsequent pages (follow "Next", "Load More", pagination links)
- All jobs across multiple listing pages if the site has them
If a site shows 50 jobs, we must extract 50 jobs — not 5, not 25.

### Objective 3: Maximum Job Data Quality & Depth
Every piece of information about each job must be CAPTURED. The 4 core fields are
mandatory (title, URL, location, description), but ALL available data is required:
- Salary/compensation, employment type (full-time/part-time/contract/casual)
- Department, team, closing date, listed date
- Requirements, benefits, qualifications
This means identifying and navigating to "View Job" / "Read More" / "Job Details"
links to access the full detail page where richer information lives.

## Convergence & Stopping Criteria

This is an INFINITE continuous improvement loop. Keep iterating until:
- The model OUTPERFORMS the baseline across all 3 objectives
- AND the model has shown NO meaningful improvement over the last 5 iterations
  (i.e. accuracy, job count, and quality score are stable within ±2%)
If both conditions are met, stop the loop — the task is complete.
Otherwise, KEEP GOING. There is always something to improve.

## Current State

Model: 3-Tier Hybrid Extractor v2.4
Description: v2.4: Tier-1 quality gating for nav/language pseudo-jobs; Greenhouse jobPosts payload parsing; finder fast paths for applytojob/elmotalent; listing-link and vacancies-vacatures route repair.
Test Results: 12% success rate (6/50 sites)
Match breakdown: {"model_only": 1, "both_failed": 22, "model_failed": 22, "model_equal_or_better": 5}
Tier breakdown: {"tier2_heuristic_v17": 2, "tier2_structured_v18_nga_table": 1, "tier2_structured_v17_list_group": 1, "tier2_structured_v21_article_cards": 1, "tier2_structured_v18_elementor_loop": 1}

## Failures to Analyse

There are 22 sites where the model performed worse than the baseline
or failed entirely. Below are the details with HTML snippets for analysis.


--- Failure 1: model_failed ---
Company: Sampoerna Kayoe
Domain: sampoernakayoe.co.id
Test URL (known from test data): https://www.sampoernakayoe.co.id/vacancies/
Baseline: 6 jobs | Titles: ['Management Trainee', 'MAINTENANCE HEAD', 'SR. MECHANICAL ENGINEER']
  Wrapper selectors: boundary=.//table[@class="table vac-table"]/tbody/tr[@data-toggle="co | title=.//td[2]
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Opportunities → https://www.sampoernakayoe.co.id/opportunities/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_1_sampoernakayoe_co_id.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_1_sampoernakayoe_co_id_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_1_sampoernakayoe_co_id_wrapper.json

--- Failure 2: model_failed ---
Company: Great Wall TCM
Domain: greatwalltcm.com.sg
Test URL (known from test data): https://www.greatwalltcm.com.sg/career/
Baseline: 3 jobs | Titles: ['TCM Physician (Tiong Bahru)', 'Clinic Customer Care Executive (Tiong Bahru)', 'Tuina Massage Therapist']
  Wrapper selectors: boundary=article.elementor-post | title=.//div[@class='elementor-post__text']/h3[@class='elementor-p
Model: 0 jobs | Tier: None | Titles: []
  Discovery: None → None
  Error: Phase timeout (60s)
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_2_greatwalltcm_com_sg.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_2_greatwalltcm_com_sg_wrapper.json

--- Failure 3: model_failed ---
Company: Victoria CAREERS.VIC
Domain: careers.vic.gov.au
Test URL (known from test data): https://www.careers.vic.gov.au/jobs?keywords=
Baseline: 15 jobs | Titles: ['Integration Aide', 'Payroll & HR Administration Officer', 'Teacher Assistant']
  Wrapper selectors: boundary=div.views-row | title=h3
Model: 0 jobs | Tier: None | Titles: []
  Discovery: careers_subdomain:careers → https://www.careers.vic.gov.au/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_3_careers_vic_gov_au.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_3_careers_vic_gov_au_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_3_careers_vic_gov_au_wrapper.json

--- Failure 4: model_failed ---
Company: Flynn Group of Companies
Domain: jobs.lever.co
Test URL (known from test data): https://jobs.lever.co/flynncompanies
Baseline: 2 jobs | Titles: ['Construction Administrator', 'Service Administrator - Commercial Construction']
  Wrapper selectors: boundary=div.posting | title=.//a[@class='posting-title']/h5[1]
Model: 0 jobs | Tier: None | Titles: []
  Discovery: ats_domain:lever.co → https://www.lever.co/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_4_jobs_lever_co.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_4_jobs_lever_co_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_4_jobs_lever_co_wrapper.json

--- Failure 5: model_failed ---
Company: Ausenco
Domain: careers.ausenco.com
Test URL (known from test data): https://careers.ausenco.com/jobs/search?page=1&location_group_uids%5B%5D=3bb716e29f5118d9475f1f20fc886ac1&query=
Baseline: 30 jobs | Titles: ['Finance Analyst', 'Ausenco Engineering Scholarship', 'Project Administrator']
  Wrapper selectors: boundary=article.col-12 | title=.//div[contains(concat(' ',normalize-space(@class),' '),' ca
Model: 0 jobs | Tier: None | Titles: []
  Discovery: careers_subdomain:careers → https://careers.ausenco.com
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_5_careers_ausenco_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_5_careers_ausenco_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_5_careers_ausenco_com_wrapper.json

--- Failure 6: model_failed ---
Company: GP Outsourcing Asia
Domain: gpoasia.com
Test URL (known from test data): https://www.gpoasia.com/career-opportunities
Baseline: 9 jobs | Titles: ['Join our\xa0Talent Community', 'WFH Language Interpreter\xa0(中英口译员）', 'WFH Language Interpreter\xa0(中英口译员）']
  Wrapper selectors: boundary=.wixui-column-strip__column | title=h2,h5
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Careers+subpage:href_match → https://www.gpoasia.com/career-opportunities/language-interpreter
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_6_gpoasia_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_6_gpoasia_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_6_gpoasia_com_wrapper.json

--- Failure 7: model_failed ---
Company: BayanTech
Domain: careers-page.com
Test URL (known from test data): https://www.careers-page.com/bayantech-2?page=1
Baseline: 21 jobs | Titles: ['[[ job.position_name ]]', 'Content Writer (Hybrid)', 'Senior Business Development']
  Wrapper selectors: boundary=li.media | title=.//div[@class='media-body']/a[@class='text-secondary']/h5[co
Model: 0 jobs | Tier: None | Titles: []
  Discovery: None → None
  Error: Phase timeout (60s)
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_7_careers_page_com.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_7_careers_page_com_wrapper.json

--- Failure 8: model_failed ---
Company: SSP
Domain: careers.foodtravelexperts.com
Test URL (known from test data): https://careers.foodtravelexperts.com/search/?searchby=location&createNewAlert=false&q=&locationsearch=&geolocation=&optionsFacetsDD_department=&optionsFacetsDD_shifttype=&optionsFacetsDD_country=
Baseline: 25 jobs | Titles: ['Verkäufer Tankstelle (m/w/d) Raststätte Fuchsberg (23561)', 'Barista Starbucks [Rotterdam CS Hal]', 'Barista Starbucks [Utrecht CS]']
  Wrapper selectors: boundary=li.job-tile | title=.//div[@class='job-tile-cell']/div[contains(concat(' ',norma
Model: 0 jobs | Tier: None | Titles: []
  Discovery: v10_fast_subdomain_root → https://careers.foodtravelexperts.com
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_8_careers_foodtravelexperts_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_8_careers_foodtravelexperts_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/failure_8_careers_foodtravelexperts_com_wrapper.json


## Spot-Check Successes

These are sites where the model appeared to succeed. VERIFY them by visiting the actual
sites to check: (a) did we find ALL the jobs? (b) did we get the right job titles?
(c) are we extracting all available data (description, salary, location, job type)?


--- Spot-check 1: model_only ---
Company: Suzanne Grae
Domain: careers.suzannegrae.com.au
Test URL: http://careers.suzannegrae.com.au/sgrae/en/listing/
Baseline: 0 jobs | Titles: []
Model: 23 jobs | Tier: tier2_structured_v21_article_cards | Titles: ['Group Casual Sales Team Member - Wendouree, VIC', 'Suzanne Grae NEW STORE OPENING Sales Team Members - Port Macquarie, NSW', 'Group Casual Sales Team Member - Coffs Harbour, NSW']
  Discovery: v10_fast_subdomain_root → https://careers.suzannegrae.com.au/jobs/search
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/spotcheck_1_careers_suzannegrae_com_au.html

--- Spot-check 2: model_equal_or_better ---
Company: Singapore Corporate Services
Domain: careers.oa-goc.com
Test URL: https://careers.oa-goc.com/vacancy-singapore
Baseline: 5 jobs | Titles: ['HR EXECUTIVE (TALENT ACQUISITION AND MANAGEMENT)(SG)', 'SENIOR ASSOCIATE, ACCOUNTS AND OUTSOURCING (SG)', 'ASSOCIATE, AUDIT AND ASSURANCE (SG)']
Model: 17 jobs | Tier: tier2_heuristic_v17 | Titles: ['ACCOUNTS INTERN (SG)', 'AUDIT INTERN (SG)', 'CUSTOMER SERVICE INTERN (MY)']
  Discovery: v10_fast_subdomain_root → https://careers.oa-goc.com
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/spotcheck_2_careers_oa_goc_com.html

--- Spot-check 3: model_equal_or_better ---
Company: Adilstone Group
Domain: adilstonegroup.applytojob.com
Test URL: https://adilstonegroup.applytojob.com/
Baseline: 39 jobs | Titles: ['Business Development and Sales Manager (Ref#089)', 'Business Director (Ref#067)', 'Chief Operating Officer (Ref#091)']
Model: 39 jobs | Tier: tier2_structured_v17_list_group | Titles: ['Business Development and Sales Manager (Ref#089)', 'Business Director (Ref#067)', 'Chief Operating Officer (Ref#091)']
  Discovery: applytojob_apply_path_v11 → https://adilstonegroup.applytojob.com/apply/
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5/spotcheck_3_adilstonegroup_applytojob_com.html


## Memory (READ FIRST, WRITE AFTER)

A memory file tracks all previous iterations — what issues were found, what fixes were
tried, and what worked vs didn't. This prevents going in circles.

**BEFORE designing any fixes, read this file:**
  `storage/auto_improve_memory.json`

It contains:
- Every past version's accuracy, issues, and fixes
- Known hard patterns that are difficult to solve
- What approaches work well vs don't work
- Your task is to AVOID repeating fixes that didn't work before

**AFTER implementing your fixes and BEFORE triggering the test, UPDATE the memory file:**
Add a new entry to the `iterations` array with:
```json
{
  "version": "v2.5",
  "accuracy": null,
  "key_issues": ["list the specific issues you identified in THIS iteration"],
  "fixes_applied": ["list the specific fixes you implemented"],
  "result": "pending — will be updated after test"
}
```
Also update `known_hard_patterns`, `what_works_well`, and `what_doesnt_work` if you
learned anything new from your analysis.

After the test completes (in the NEXT iteration), the memory will be updated with results.

## Context Files (MUST READ)

Full HTML and wrapper configs for each failure are saved as files — NOT inlined in this
prompt. You MUST read these files to understand what's on each page:

Context directory: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_5

For each failure, there are up to 3 files:
- `failure_N_domain.html` — the full HTML of the page the model discovered
- `failure_N_domain_baseline.html` — the full HTML of the page the baseline used (if different URL)
- `failure_N_domain_wrapper.json` — the full wrapper/selector config that the baseline used

**Read the HTML files** to see the actual page structure. Look for:
- Job listings (repeated elements with titles, locations, Apply buttons)
- CSS classes and IDs that could be used as selectors
- Whether content is server-rendered or requires JavaScript

**Read the wrapper JSON files** to see exactly what selectors the baseline uses and why it succeeds.
Compare the wrapper's boundary/title selectors with the actual HTML to understand the pattern.

For spot-check successes, the HTML file lets you verify if we captured all jobs.

## CRITICAL: Self-Evaluate by Visiting Sites

You have Playwright and headless Chrome available. You MUST use them to visually inspect
sites before implementing changes. This is essential for understanding what's really on
the page vs what the server-rendered HTML shows.

**For EVERY failure site (or at least the top 10):**
```python
# Use this pattern to visit a site and see what's actually there:
import asyncio
from playwright.async_api import async_playwright

async def inspect_site(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)  # Let JS finish rendering

        # Get the RENDERED HTML (after JavaScript)
        html = await page.content()
        print(f"Rendered HTML length: {len(html)}")

        # Screenshot for visual inspection
        await page.screenshot(path='/tmp/site_inspect.png', full_page=True)

        # Count job-like elements in rendered DOM
        job_elements = await page.query_selector_all('[class*="job"], [class*="position"], [class*="vacancy"], [class*="opening"]')
        print(f"Job-related elements: {len(job_elements)}")

        # Count "Apply" buttons
        apply_buttons = await page.query_selector_all('text=Apply, text=Apply Now')
        print(f"Apply buttons: {len(apply_buttons)}")

        # Get all visible text
        body_text = await page.inner_text('body')
        print(f"Visible text length: {len(body_text)}")
        print(f"First 500 chars: {body_text[:500]}")

        await browser.close()
        return html

asyncio.run(inspect_site("https://example.com/careers"))
```

**For spot-check successes (at least 3):**
- Visit the site and count the ACTUAL number of jobs visible
- Compare with what our model extracted — did we miss any?
- Click on a job listing — does the detail page have description, salary, location?
- If the detail page has data we didn't extract, that's a quality improvement opportunity

**Use screenshots** to capture what the page looks like. Compare the rendered page with
the server-rendered HTML to understand JS rendering gaps.

## Your Task

1. **Visit and inspect** the failure sites using Playwright (rendered HTML + screenshots)
2. **Visit and spot-check** 3-5 success sites to verify:
   - Objective 1: Did we find the RIGHT page? Are there other career pages we missed?
   - Objective 2: Did we find ALL jobs? Count visible jobs vs extracted count. Check pagination.
   - Objective 3: Click into a job detail page — is there description, salary, location, type
     that we didn't capture? If so, our detail page enrichment needs improvement.
3. **Analyse root causes** of each failure against the 3 objectives:
   - Obj 1 failures: wrong URL, discovery missed the page, ATS slug wrong
   - Obj 2 failures: found the page but missed jobs (pagination not followed, wrong container)
   - Obj 3 failures: found jobs but missing fields (didn't visit detail page, missed salary/type)
4. **Design high-impact fixes** that address MULTIPLE failures across objectives
5. **Implement** by creating new extractor/finder files (subclass previous version)
6. **Wire in** to `backend/app/api/v1/endpoints/ml_models.py` (_pick_extractor and _pick_finder)
7. **Create the model** via API:
   ```bash
   TOKEN=$(curl -s http://localhost:8001/api/v1/auth/login -d 'username=r.m.l.alford@gmail.com&password=Uu00dyandben!' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
   curl -X POST http://localhost:8001/api/v1/ml-models/ -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"name": "3-Tier Hybrid Extractor v2.5", "model_type": "site_job_listings", "description": "YOUR DESCRIPTION OF CHANGES"}'
   ```
8. **Rebuild and restart**:
   ```bash
   docker compose -f docker-compose.server.yml up -d --build api
   docker restart jobharvest-api
   ```
9. **Trigger a 50-site test WITH auto_improve enabled** (so the loop continues after this test):
   ```bash
   MODEL_ID=$(curl -s "http://localhost:8001/api/v1/ml-models/?page=1&page_size=1" -H "Authorization: Bearer $TOKEN" | python3 -c 'import sys,json;print(json.load(sys.stdin)["items"][0]["id"])')
   curl -X POST "http://localhost:8001/api/v1/ml-models/$MODEL_ID/test-runs/execute" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"sample_size": 50, "auto_improve": true}'
   ```

## Important Guidelines

- **TIME LIMIT: You have 25 minutes max.** Spend at most 5 min reading/inspecting, then implement.
  Don't read every file exhaustively — skim the latest version's class and key methods only.
  Inspect at most 3-4 failure sites with Playwright, not all of them.
- **Read AGENTS.md first** — it has environment setup, common pitfalls, and step-by-step instructions
- **Use `python3`** (not `python`) for all commands
- **Playwright is installed on the host** — run `python3 backend/scripts/inspect_site.py <url>` directly, no need for docker exec
- Each new extractor version MUST inherit from the LATEST version (check what's newest in backend/app/crawlers/)
- Focus on high-impact changes that fix the MOST failures
- Read the existing code FIRST before making changes
- Don't break existing functionality — only ADD improvements
- ALWAYS visit sites with Playwright before making assumptions about what's on the page
- Pay special attention to JS-rendered content — many career pages are SPAs
- Check if detail pages exist (click on job listings) and whether we're extracting from them

## Helper Tool: Site Inspector

A helper script is available at `backend/scripts/inspect_site.py` that visits a URL with
headless Chrome and reports what's visible. Use it to quickly inspect sites:

```bash
# Basic inspection — shows job signals, links, title nouns, visible text
python backend/scripts/inspect_site.py https://example.com/careers

# With screenshot
python backend/scripts/inspect_site.py https://example.com/careers -s /tmp/site.png

# Click first job link to inspect detail page
python backend/scripts/inspect_site.py https://example.com/careers --click-first-job

# Full structural analysis + click first job
python backend/scripts/inspect_site.py https://example.com/careers -f -c -s /tmp/site.png
```

**You are STRONGLY ENCOURAGED to:**
1. Run `inspect_site.py` on ALL failure sites to see the rendered page
2. Run `inspect_site.py -c` on 3+ success sites to verify job count and check detail pages
3. Compare the rendered HTML (from Playwright) with the server HTML (in the snippets above)
4. Use screenshots to understand the visual layout
5. Check if "Apply" buttons, "Read more" links, or job title nouns are visible but not in server HTML

This is how you discover JS-rendering issues, missing selectors, and extraction gaps.
