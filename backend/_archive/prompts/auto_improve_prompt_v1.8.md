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

Model: 3-Tier Hybrid Extractor v1.7
Description: v1.7: ATS path normalization (applytojob /apply, elmotalent iframe paths, careers-page slug variants), high-intent subpage promotion, targeted structural extraction for vacancy/smoothie/list-group layouts, Greenhouse title parsing fix, and tier2 link-quality gating with expanded nav-title rejection.
Test Results: 48% success rate (24/50 sites)
Match breakdown: {"partial": 3, "model_only": 5, "both_failed": 17, "model_worse": 6, "model_failed": 9, "model_equal_or_better": 10}
Tier breakdown: {"tier2_heuristic": 22, "tier1_ats_livehire": 1, "tier1_ats_teamtailor": 1}

## Failures to Analyse

There are 15 sites where the model performed worse than the baseline
or failed entirely. Below are the details with HTML snippets for analysis.


--- Failure 1: model_failed ---
Company: Montgomery Group - Global Events
Domain: montgomerygroup.com
Test URL (known from test data): https://www.montgomerygroup.com/careers-1
Baseline: 2 jobs | Titles: ['Portfolio Marketing Manager', 'Senior Commercial Executive']
  Wrapper selectors: boundary=//li[contains(@class, 'js-library-item')] | title=.//h2
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Careers → https://www.montgomerygroup.com/careers
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_1_montgomerygroup_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_1_montgomerygroup_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_1_montgomerygroup_com_wrapper.json

--- Failure 2: model_failed ---
Company: Civil Aviation Safety Authority
Domain: casajobs.nga.net.au
Test URL (known from test data): https://casajobs.nga.net.au/cp/?audiencetypecode=EXT
Baseline: 2 jobs | Titles: ['CASA Inspector (Flight Operations)/ Senior CASA Inspector (Flight Operations)', 'Senior Medical Officer']
  Wrapper selectors: boundary=tr.cp_row | title=a.cp_jobListJobTitle
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:How to apply for a job at CASA → https://casajobs.nga.net.au/publicfiles/casajobs/jobs/24D648E3-D877-F347-6A6E-ECBFF00A64A2/How%20to%20apply%20for%20a%20job%20at%20CASA_1_1_1.pdf
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_2_casajobs_nga_net_au.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_2_casajobs_nga_net_au_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_2_casajobs_nga_net_au_wrapper.json

--- Failure 3: model_worse ---
Company: Nedap N.V. 
Domain: nedap.com
Test URL (known from test data): https://nedap.com/careers/vacancies/
Baseline: 41 jobs | Titles: ['Technical Operations Specialist', 'Full-stack developer', 'Kotlin Backend Developer']
  Wrapper selectors: boundary=.//article | title=.//h3[1]
Model: 3 jobs | Tier: tier2_heuristic | Titles: ['Working at Nedap', 'Students at Nedap', 'Personal Stories']
  Discovery: homepage_link:Vacancies → https://www.nedap.com/en/careers/vacancies
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_3_nedap_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_3_nedap_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_3_nedap_com_wrapper.json

--- Failure 4: model_failed ---
Company: Old Town
Domain: oldtown.rymnet.com
Test URL (known from test data): https://oldtown.rymnet.com/rcpjobdetail
Baseline: 3 jobs | Titles: ['Senior Officer - Warehouse (Subang Jaya, Selangor)', 'Technician (Subang Jaya, Selangor)', 'Executive - Menu Development (Subang Jaya, Selangor)']
  Wrapper selectors: boundary=.friend-widget | title=h4
Model: 0 jobs | Tier: None | Titles: []
  Discovery: domain_root_fallback → https://oldtown.rymnet.com
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_4_oldtown_rymnet_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_4_oldtown_rymnet_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_4_oldtown_rymnet_com_wrapper.json

--- Failure 5: model_worse ---
Company: Linnovate Partners
Domain: linnovatepartners.com
Test URL (known from test data): https://www.linnovatepartners.com/join-us/current-vacancies/
Baseline: 9 jobs | Titles: ['軟件开发工程師 (C#)', '高级软件工程师 (C#)', 'Associate Director, Clients Success (HK)']
  Wrapper selectors: boundary=.e-loop-item | title=.elementor-image-box-title a
Model: 4 jobs | Tier: tier2_heuristic | Titles: ['who we Serve', 'what we Do', 'Company']
  Discovery: homepage_link:Careers → https://linnovatepartners.com/careers/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_5_linnovatepartners_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_5_linnovatepartners_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_5_linnovatepartners_com_wrapper.json

--- Failure 6: model_failed ---
Company: Azendian Solutions
Domain: azendian.com
Test URL (known from test data): https://azendian.com/people/careers/
Baseline: 10 jobs | Titles: ['Senior Technical Architect', 'Channel Sales Manager (Malaysia)', 'Senior Data Engineer']
  Wrapper selectors: boundary=//div[1]/div[2]/section[1]/div/div/div[2]/div[2]/div/div[2]/ | title=h2
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Careers → https://azendian.com/people/careers/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_6_azendian_com.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_6_azendian_com_wrapper.json

--- Failure 7: model_worse ---
Company: Mazzei Group Corporate
Domain: mazzeigroup-1750399679.teamtailor.com
Test URL (known from test data): https://mazzeigroup-1750399679.teamtailor.com/jobs
Baseline: 5 jobs | Titles: ['HSE Advisor', 'HSE Advisor', 'Learning and Development Advisor']
  Wrapper selectors: boundary=ul#jobs_list_container > li | title=a
Model: 2 jobs | Tier: tier1_ats_teamtailor | Titles: ['Start', 'Start']
  Discovery: ats_domain:teamtailor.com+subpage:href_match → https://mazzeigroup-1750399679.teamtailor.com/jobs/7208533-accounts-payable-officer
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_7_mazzeigroup_1750399679_teamtai.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_7_mazzeigroup_1750399679_teamtai_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_7_mazzeigroup_1750399679_teamtai_wrapper.json

--- Failure 8: model_failed ---
Company: Digital India
Domain: dic.gov.in
Test URL (known from test data): https://dic.gov.in/careers/?search_keywords=&selected_category=-1&selected_jobtype=-1&selected_location=-1
Baseline: 9 jobs | Titles: ['Product Cum Project Manager', 'Executive Assistant', 'Technical Architect – AI']
  Wrapper selectors: boundary=div.col-md-4 | title=.//div[@class='list-data']/div[1]/header[1]/div[@class='row'
Model: 0 jobs | Tier: None | Titles: []
  Discovery: homepage_link:Careers+subpage:href_match → https://dic.gov.in/jobs/product-cum-project-manager/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_8_dic_gov_in.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_8_dic_gov_in_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/failure_8_dic_gov_in_wrapper.json


## Spot-Check Successes

These are sites where the model appeared to succeed. VERIFY them by visiting the actual
sites to check: (a) did we find ALL the jobs? (b) did we get the right job titles?
(c) are we extracting all available data (description, salary, location, job type)?


--- Spot-check 1: model_equal_or_better ---
Company: Bendigo Regional Institute of TAFE
Domain: careers.bki.edu.au
Test URL: https://careers.bki.edu.au/search/?searchby=location&createNewAlert=false&q=&locationsearch=&geolocation=
Baseline: 18 jobs | Titles: ['Teacher Visual Arts', 'Technical Simulation Officer - Hair Beauty & Barbering', 'People & Culture Business Partner']
Model: 18 jobs | Tier: tier2_heuristic | Titles: ['Teacher Visual Arts', 'Technical Simulation Officer - Hair Beauty & Barbering', 'People & Culture Business Partner']
  Discovery: careers_subdomain:careers+subpage:href_match → https://careers.bki.edu.au/search/
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/spotcheck_1_careers_bki_edu_au.html

--- Spot-check 2: model_only ---
Company: Blumenthal Richter Sumet & Schuler Ltd.
Domain: brsslawyers.com
Test URL: https://brsslawyers.com/career-en/
Baseline: 0 jobs | Titles: []
Model: 2 jobs | Tier: tier2_heuristic | Titles: ['PRACTICE GROUPS', 'CORPORATE AND COMMERCIAL / M&A']
  Discovery: homepage_link:CAREER → https://brsslawyers.com/career-en/
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/spotcheck_2_brsslawyers_com.html

--- Spot-check 3: model_equal_or_better ---
Company: Centre for Evidence and Implementation
Domain: ceiglobal.org
Test URL: https://www.ceiglobal.org/about/careers/vacancies
Baseline: 1 jobs | Titles: ['Business Operations Lead - Singapore']
Model: 3 jobs | Tier: tier2_heuristic | Titles: ['Vacancies', 'Business Operations Lead - Singapore', 'Stay in the loop']
  Discovery: homepage_link:Careers+subpage_v5:href_match_v5 → https://www.ceiglobal.org/about/careers/vacancies
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8/spotcheck_3_ceiglobal_org.html


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
  "version": "v1.8",
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

Context directory: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v1_8

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
   curl -X POST http://localhost:8001/api/v1/ml-models/ -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"name": "3-Tier Hybrid Extractor v1.8", "model_type": "site_job_listings", "description": "YOUR DESCRIPTION OF CHANGES"}'
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
