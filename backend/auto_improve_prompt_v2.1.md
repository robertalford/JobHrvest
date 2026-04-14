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

Model: 3-Tier Hybrid Extractor v1.8
Description: v1.8 (crawl intelligence): Tier 0 JSON-LD/structured data extraction. Cookie banner dismissal. Longer Playwright waits (5-8s). Iframe detection for ATS widgets. Search form submission. Accordion expansion. Pagination following. Detail page enrichment. Inherits from v1.6.
Test Results: 62% success rate (34/55 sites)
Match breakdown: {"partial": 2, "model_only": 15, "both_failed": 12, "model_worse": 5, "model_failed": 9, "model_equal_or_better": 12}
Tier breakdown: {"tier0_rss_feed": 1, "tier1_ats_bamboohr": 1, "tier2_heuristic_v16": 31, "tier1_ats_careers_page": 1}

## Failures to Analyse

There are 14 sites where the model performed worse than the baseline
or failed entirely. Below are the details with HTML snippets for analysis.


--- Failure 1: model_worse ---
Company: Virtue
Domain: virtue.love
Test URL (known from test data): https://virtue.love/pages/careers
Baseline: 7 jobs | Titles: ['Careers', 'Open Roles', 'Marketing & Communications Internship']
  Wrapper selectors: boundary=h2.font-heading | title=.
Model: 2 jobs | Tier: tier2_heuristic_v16 | Titles: ['Size & Fit', 'Shipping & Returns']
  Discovery: homepage_link:Careers → https://virtue.love/pages/careers
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_1_virtue_love.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_1_virtue_love_wrapper.json

--- Failure 2: model_worse ---
Company: CloudCommerce
Domain: cloudcommerce.breezy.hr
Test URL (known from test data): https://cloudcommerce.breezy.hr/
Baseline: 28 jobs | Titles: ['Business Growth Manager', 'Customer Experience Executive (Cohort)', 'Customer Support Representative']
  Wrapper selectors: boundary=li.position | title=.//a[1]/h2[1]
Model: 13 jobs | Tier: tier2_heuristic_v16 | Titles: ['Accounting Intern', 'Content Creator Intern', 'Customer Service Intern']
  Discovery: path_probe_best_guess+playwright → https://cloudcommerce.breezy.hr/
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_2_cloudcommerce_breezy_hr.html
  HTML (baseline's test URL):     same as above
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_2_cloudcommerce_breezy_hr_wrapper.json

--- Failure 3: model_failed ---
Company: Green Tomato Limited
Domain: gtomato.com
Test URL (known from test data): https://www.gtomato.com/careers/join-our-team
Baseline: 7 jobs | Titles: ['Agentic AI Engineer', 'AI Adoption Specialist', 'Senior Business Analyst']
  Wrapper selectors: boundary=ul li | title=.//div[contains(concat(' ',normalize-space(@class),' '),' Fa
Model: 0 jobs | Tier: None | Titles: []
  Discovery: path_probe_best_guess → https://gtomato.com/careers
  Error: Page too short (0 bytes), even after Playwright
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_3_gtomato_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_3_gtomato_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_3_gtomato_com_wrapper.json

--- Failure 4: model_failed ---
Company: Forest and Bird
Domain: forestandbird.org.nz
Test URL (known from test data): https://www.forestandbird.org.nz/about-us/our-people/current-vacancies
Baseline: 1 jobs | Titles: ["Sorry, we couldn't find what you were looking for."]
  Wrapper selectors: boundary=.prose h3 | title=.
Model: 0 jobs | Tier: None | Titles: []
  Discovery: path_probe_best_guess+playwright → https://www.forestandbird.org.nz/support-us/become-member-forest-bird
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_4_forestandbird_org_nz.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_4_forestandbird_org_nz_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_4_forestandbird_org_nz_wrapper.json

--- Failure 5: model_worse ---
Company: OVEA Digital Creator
Domain: oveadc.com
Test URL (known from test data): https://oveadc.com/lowongan/
Baseline: 10 jobs | Titles: ['Influencer', 'Social Media Specialist', 'Akuntan']
  Wrapper selectors: boundary=.elementor-column.elementor-inner-column | title=h2.elementor-heading-title
Model: 3 jobs | Tier: tier2_heuristic_v16 | Titles: ['Lowongan Kerja', 'Alamat Kantor', 'Model Incubator']
  Discovery: homepage_fallback → https://oveadc.com
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_5_oveadc_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_5_oveadc_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_5_oveadc_com_wrapper.json

--- Failure 6: model_worse ---
Company: Juvare
Domain: job-boards.greenhouse.io
Test URL (known from test data): https://job-boards.greenhouse.io/embed/job_board?for=juvare&validityToken=Hl9jSEoYw1bv0p3xIPVgWgMod3sf_qo7o-4h6P0Abavm55qEnO9KBYcabBcCwsaxpyYBmhH4i2tJAWf6iIlMqyFobBRYnbhLZaOKEPE4wxJVT21RroVht-lNa65QlsHImffuOcUKvBZkQmzlfQNnZ9lXujoTdbJmsMKL2P1c3UNHcL7htz1o4FrmOUYjES2OT-HmSZtzjcn4MQZ7z1Zf_7k6FXJtzeUGFgReudRGPfT6nLsoyiMhYmwPAhC4WTwMg-XegBHxUW3dFzY308sDrjXCqWSgU6LRR2GdwyZCrnnzer5UGt2F0XbqYe7fhDyIZvQLi93a859NUt3B7lpEBA%3D%3D
Baseline: 10 jobs | Titles: ['Automation Test Engineer', 'DevOps Engineer', 'Lead Automation Test Engineer']
  Wrapper selectors: boundary=.job-post | title=.body--medium
Model: 2 jobs | Tier: tier2_heuristic_v16 | Titles: ['General Opportunities', 'NMC Opportunities New York, NY']
  Discovery: path_probe_best_guess+playwright → https://job-boards.greenhouse.io/opportunities
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_6_job_boards_greenhouse_io.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_6_job_boards_greenhouse_io_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_6_job_boards_greenhouse_io_wrapper.json

--- Failure 7: model_failed ---
Company: Cliq HR
Domain: cliqhr.zohorecruit.in
Test URL (known from test data): https://cliqhr.zohorecruit.in/recruit/Portal.na
Baseline: 25 jobs | Titles: ['IT System Engineer', 'Power Builder Developer', 'Project Coordinator']
  Wrapper selectors: boundary=tr.jobDetailRow | title=.//td[1]/a[@class='jobdetail']
Model: 0 jobs | Tier: None | Titles: []
  Discovery: path_probe:/jobs → https://cliqhr.zohorecruit.in/jobs
  Error: None
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_7_cliqhr_zohorecruit_in.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_7_cliqhr_zohorecruit_in_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_7_cliqhr_zohorecruit_in_wrapper.json

--- Failure 8: model_failed ---
Company: Odfjell Technology Corp.
Domain: odfjelltechnology.com
Test URL (known from test data): https://www.odfjelltechnology.com/career/job-openings/
Baseline: 17 jobs | Titles: ['eLearning Content Designer', 'GBS Finance Intern MNL', 'Service Technician NL']
  Wrapper selectors: boundary=.job | title=div.job__name
Model: 0 jobs | Tier: None | Titles: []
  Discovery: path_probe_best_guess → https://www.odfjelltechnology.com/career/
  Error: Page too short (0 bytes), even after Playwright
Context files (READ THESE for full analysis):
  HTML (model's discovered page): /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_8_odfjelltechnology_com.html
  HTML (baseline's test URL):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_8_odfjelltechnology_com_baseline.html
  Full wrapper config (JSON):     /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/failure_8_odfjelltechnology_com_wrapper.json


## Spot-Check Successes

These are sites where the model appeared to succeed. VERIFY them by visiting the actual
sites to check: (a) did we find ALL the jobs? (b) did we get the right job titles?
(c) are we extracting all available data (description, salary, location, job type)?


--- Spot-check 1: model_only ---
Company: Heritage Bank
Domain: hcyt.fa.ap1.oraclecloud.com
Test URL: https://hcyt.fa.ap1.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions
Baseline: 0 jobs | Titles: []
Model: 3 jobs | Tier: tier2_heuristic_v16 | Titles: ["People's Choice website", 'Heritage Bank website', 'Careers info']
  Discovery: path_probe_best_guess+playwright → https://hcyt.fa.ap1.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/spotcheck_1_hcyt_fa_ap1_oraclecloud_com.html

--- Spot-check 2: partial ---
Company: UKinbound
Domain: ukinbound.org
Test URL: https://www.ukinbound.org/tourism-jobs/
Baseline: 3 jobs | Titles: ['Admission and Marketing Manager', 'Senior Trade Coordinator', 'Executive Director']
Model: 2 jobs | Tier: tier2_heuristic_v16 | Titles: ['Jobs in tourism', 'Office address']
  Discovery: homepage_link:Jobs in Tourism → https://www.ukinbound.org/tourism-jobs/
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/spotcheck_2_ukinbound_org.html

--- Spot-check 3: model_equal_or_better ---
Company: FitFlop Limited
Domain: careers.fitflop.com
Test URL: https://careers.fitflop.com/jobs
Baseline: 10 jobs | Titles: ['Data Engineer', 'Finance Data Analyst (FTC)', 'Materials Specialist']
Model: 10 jobs | Tier: tier2_heuristic_v16 | Titles: ['Data Engineer', 'Finance Data Analyst (FTC)', 'Materials Specialist']
  Discovery: path_probe_best_guess+playwright → https://careers.fitflop.com/jobs
  HTML file: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1/spotcheck_3_careers_fitflop_com.html


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
  "version": "v2.1",
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

Context directory: /home/moonlight-admin/Documents/Server-Apps/moonlight/Services/jobharvest/storage/auto_improve_context/v2_1

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
6. **DO NOT run Docker, curl, or API commands.** You don't have network/Docker access from the sandbox.
   The deployment pipeline (rebuild, model creation, test trigger) runs AUTOMATICALLY after you finish.
   Just write the code files and update memory.
7. **Update memory**: add your iteration to `storage/auto_improve_memory.json`

## Important Guidelines

## RULES (read AGENTS.md for full details)

1. **TIME: 25 min max.** 2 min read, 3 min inspect, 15 min implement, 5 min deploy.
2. **Inherit from TieredExtractorV16 DIRECTLY.** Not from any deeper chain. Same for finder: inherit from CareerPageFinderV4.
3. **Be AMBITIOUS.** Add 3-6 high-impact general-pattern improvements. Target 5-15% accuracy gain.
4. **NEVER add single-site fixes.** Every change must help 3+ sites.
5. **Fixed test set.** Same 50 sites every run. Pass `use_fixed_set: true`.
6. **Best accuracy: 67% (v1.6).** You MUST beat or match this.
7. **Use `python3`.** Playwright is on the host: `python3 backend/scripts/inspect_site.py <url> -f`

## ⚠️ REGRESSION ALERT
Previous version scored 62%, down from best of 67%.
The approach FAILED. Read memory to see why. Try a COMPLETELY DIFFERENT strategy.
DO NOT add more complexity. Simplify. The v1.6 base (66%) works — build on it carefully.

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
