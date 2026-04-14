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

Model: 3-Tier Hybrid Extractor v2.5
Description: v2.5: vacancy article-card coverage (button-linked detail cards), stricter article pseudo-job filtering, and v12 discovery recovery for Teamtailor/NGA/Rymnet detail-document-root misroutes.
Test Results: 8% success rate (3/38 sites)
Match breakdown: {"model_only": 1, "both_failed": 18, "model_failed": 17, "model_equal_or_better": 2}
Tier breakdown: {"tier2_heuristic_v17": 3}

## Failures to Analyse

There are 17 sites where the model performed worse than the baseline
or failed entirely. Below are the details with HTML snippets for analysis.


--- Failure 1: model_failed ---
Company: Percept Brand Design
Domain: percept.com.au
Test URL (known from test data): https://percept.com.au/contact/careers/
Baseline: 1 jobs | Titles: ['Mid-weight Creative']
  Wrapper selectors: boundary=.blockContent_textFeature-links--item | title=h3 a
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Careers+repair_v8:href_listing_v8 → https://percept.com.au/contact/careers/mid-weight-designer/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_1_percept_com_au.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_1_percept_com_au_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_1_percept_com_au_wrapper.json

--- Failure 2: model_failed ---
Company: Stopgap Limited
Domain: stopgap.co.uk
Test URL (known from test data): https://www.stopgap.co.uk/roles?perpage=30
Baseline: 12 jobs | Titles: ['Marketing Manager', 'Digital Marketing Officer', 'Head of Client & Marketing Delivery']
  Wrapper selectors: boundary=div.jobs-avlab | title=.//div[contains(concat(' ',normalize-space(@class),' '),' d-
Model: 0 jobs | Tier: None | Titles: []
  Discovery: None → None
  Error: Phase timeout (60s)
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_2_stopgap_co_uk.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_2_stopgap_co_uk_wrapper.json

--- Failure 3: model_failed ---
Company: Finviz Financial Visualizations
Domain: finviz.com
Test URL (known from test data): https://finviz.com/careers
Baseline: 2 jobs | Titles: ['AI Engineer (LLM/Agents)', 'Senior Backend & DevOps Engineer']
  Wrapper selectors: boundary=div.bg-primary | title=.//div[contains(concat(' ',normalize-space(@class),' '),' fl
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Careers+repair_v8:href_listing_v8 → https://finviz.com/careers/ai-engineer
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_3_finviz_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_3_finviz_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_3_finviz_com_wrapper.json

--- Failure 4: model_failed ---
Company: Toshiba Tec Malaysia Sdn Bhd
Domain: toshibatec.com.my
Test URL (known from test data): https://www.toshibatec.com.my/career/
Baseline: 12 jobs | Titles: ['Despatch', 'Credit Control Executive', 'Warehouse Assistant']
  Wrapper selectors: boundary=div.uc_ac_box | title=div.uc-heading div.uc_ac_box_title
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Career → https://www.toshibatec.com.my/career/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_4_toshibatec_com_my.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_4_toshibatec_com_my_wrapper.json

--- Failure 5: model_failed ---
Company: SAMSUNG GSG - Global Strategy Group
Domain: sgsg.samsung.com
Test URL (known from test data): https://sgsg.samsung.com/newpage/newpage.php?f_id=experienced_hires
Baseline: 3 jobs | Titles: ['Engagement Manager', 'Associate Principal', 'Principal']
  Wrapper selectors: boundary=//div/div[2]/div[2]/div[1]/ul/li | title=.//p[@class="ehires_tit"]
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:CAREER GROWTH → https://sgsg.samsung.com/newpage/newpage.php?f_id=gsg_careerGrowth
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_5_sgsg_samsung_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_5_sgsg_samsung_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_5_sgsg_samsung_com_wrapper.json

--- Failure 6: model_failed ---
Company: Anglicare NSW South NSW West ACT
Domain: anglicare.recruitmenthub.com.au
Test URL (known from test data): https://anglicare.recruitmenthub.com.au/Current-vacancies/
Baseline: 4 jobs | Titles: ['Case Manager- Housing & Homelessness Services', 'Disability Support Worker', 'Disability Support Worker']
  Wrapper selectors: boundary=div.list | title=.//a
Model: 0 jobs | Tier: None | Titles: []
  Discovery: ats_slug:recruitmenthub.com+repair_v7:href_listing_v7 → https://anglicare.recruitmenthub.com.au/Current-vacancies/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_6_anglicare_recruitmenthub_com_a.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_6_anglicare_recruitmenthub_com_a_wrapper.json

--- Failure 7: model_failed ---
Company: Mitra Keluarga Karyasehat Tbk.
Domain: karier.mitrakeluarga.com
Test URL (known from test data): https://karier.mitrakeluarga.com/vacancy?location=undefined
Baseline: 10 jobs | Titles: ['Staf Customer Service', 'Staf Fisioterapi', 'Staf Bidan']
  Wrapper selectors: boundary=.branch-mika-vacancy | title=.//p[@class="title-vacancy-detail"]
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Cari Lowongan → https://karier.mitrakeluarga.com/vacancy
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_7_karier_mitrakeluarga_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_7_karier_mitrakeluarga_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_7_karier_mitrakeluarga_com_wrapper.json

--- Failure 8: model_failed ---
Company: Global Fishing Watch
Domain: boards.greenhouse.io
Test URL (known from test data): https://boards.greenhouse.io/globalfishingwatch
Baseline: 1 jobs | Titles: ['API and Front-End QA Automation EngineerNew']
  Wrapper selectors: boundary=tr.job-post | title=p.body.body--medium
Model: 0 jobs | Tier: None | Titles: []
  Discovery: None → None
  Error: Phase timeout (60s)
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_8_boards_greenhouse_io.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/failure_8_boards_greenhouse_io_wrapper.json


## Spot-Check Successes

These are sites where the model appeared to succeed. VERIFY them by visiting the actual
sites to check: (a) did we find ALL the jobs? (b) did we get the right job titles?
(c) are we extracting all available data (description, salary, location, job type)?


--- Spot-check 1: model_equal_or_better ---
Company: Fortis Energy
Domain: careers.fortisbc.com
Test URL: https://careers.fortisbc.com/search/?createNewAlert=false&q=&locationsearch=
Baseline: 25 jobs | Titles: ['Communications Coordinator', 'Communications Insights Advisor', 'Conservation & Energy Management Program Manager, Commercial & Industrial']
Model: 25 jobs | Tier: tier2_heuristic_v17 | Titles: ['Communications Coordinator', 'Communications Insights Advisor', 'Conservation & Energy Management Program Manager, Commercial & Industrial']
  Discovery: careers_subdomain:careers+v5:careers_subdomain_probe:/jobs/search+repair_v8:href_listing_v8+repair_v12:detail_to_jobs_search_v12 → https://careers.fortisbc.com/jobs/search
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/spotcheck_1_careers_fortisbc_com.html

--- Spot-check 2: model_equal_or_better ---
Company: Dentons US
Domain: careers.dentons.com
Test URL: https://careers.dentons.com/search/?createNewAlert=false&q=&optionsFacetsDD_location=&optionsFacetsDD_department=&optionsFacetsDD_customfield1=
Baseline: 10 jobs | Titles: ['Bilingual Manager, Proposals and Recognition', 'Manager, Portfolio Management and Strategic Initiatives', 'Rates Specialist']
Model: 10 jobs | Tier: tier2_heuristic_v17 | Titles: ['Bilingual Manager, Proposals and Recognition', 'Manager, Portfolio Management and Strategic Initiatives', 'Rates Specialist']
  Discovery: careers_subdomain:careers+v5:careers_subdomain_probe:/jobs/search+promote_v12:href_force_listing_v12+repair_v12:detail_to_jobs_search_v12 → https://careers.dentons.com/jobs/search
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/spotcheck_2_careers_dentons_com.html

--- Spot-check 3: model_only ---
Company: Kaidee
Domain: jobs.kaidee.com
Test URL: https://jobs.kaidee.com/c345-jobs
Baseline: 0 jobs | Titles: []
Model: 9 jobs | Tier: tier2_heuristic_v17 | Titles: ['TG Human Resource Services (Thailand)Executive Coordinator (Marketing and Sales ', 'WE InteractiveJunior Art Director฿ 30 - 50Kรายเดือนกรุงเทพมหานคร', 'AP (Thailand)Project Marketing (บ้าน-คอนโด)กรุงเทพมหานคร']
  Discovery: v10_fast_subdomain_root → https://www.okaijobs.com/
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6/spotcheck_3_jobs_kaidee_com.html


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
  "version": "v2.6",
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

Context directory: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_6

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
   curl -X POST http://localhost:8001/api/v1/ml-models/ -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"name": "3-Tier Hybrid Extractor v2.6", "model_type": "site_job_listings", "description": "YOUR DESCRIPTION OF CHANGES"}'
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
