# Let's define a modified process / workflow to implement. 

## Entities, Relationships & Statuses

- Companies
     - Top level entity
     - Parent entity of Sites
     - The purpose of companies is to identify all possible sites (career sites, job pages etc) that are relevant - which could be a single page of the companies website, or a complex series of mini-sites across multiple sub-domains, each with multiple pages of job listings
     - Companies can have 1 of 4 statuses;
        1. OK - Sites identified
        2. At risk - Reeduced sites/jobs
        3. No sites - New
        4. No sites - Broken
    - Companies with a status of 1 need no action, and are working as expected
    - Companies that have a status of 2, 3 or 4 need to be re-defined. Every 2 hours a cron job should be run with the objective of changing as many companies that do not have a status of 1, to status 1.
    - The process required to fix a company, and return it to 'OK - Sites identified', is to analyse the company domain to identify the site pages that relate to jobs or careers, and contain job listings that can be crawled and extracted.
    - A "Heuristic Extracter" service should exist to accurately identify & define all of the site(s) per company. This service will employ a range of tools to achieve this, escalating to a more powerful tool as necessary until the objective is complete. These include;
        - Fingerprinting system that detects which ATS a site uses, to select a standard URL template map
        - Multi-signal scorer that combines URL pattern matching (paths containing /careers, /jobs, /opportunities, /vacancies, /work-with-us, /join, /openings and their variations)
        - TF-IDF + LR classifier
        - DistilBERT classifier
        - 3b paramter LLM
        - 8b parameter LLM

- Sites
    - Site are a child entity of a company
    - Sites must have 1 company (parent)
    - Sites are the parent entity of jobs
    - Sites can any number of jobs
    - The purpose of a sites is as a defind URL that belongs to a company, and hosts job listing conteont/information that can be scraped.
    - Sites can have one of the following statuses;
        1. OK - Job listing structure mapped
        2. At Risk - Reduced job information
        3. No job listing structure - New
        4. No job listing structure - Broken
    - Sites with a status of 1 need no action, and are working as expected
    - Sites with a status of 2, 3, or 4 need ot be re-mapped. Every 2 hours a cron job should be run with the objective of changing as many sites currently not in status 1, to status 1.
    - The process required to fix a site, and return it to 'OK - Job listing structure mapped', is to analyse the site (url) to identify the job listings & how the information is structured on the sites page(s) - so it can be crawled and extracted.
    - Navigation tools
        - A headless browser pool managed by something like Browserless (open-source, self-hostable)
        - Playwright to navigate sites (e.g. pagination).
    - Schema
        - Use the following schema to map site data to: Job title, Company name, Location (city/state/country/remote), Employment type (full-time/part-time/contract), Salary/compensation range, Description, Application URL, Date posted, Date closing
    - A "Heuristic Extracter" service should exist to accurately identify & define all of the job listings on a sites page(s), and all of the html/css selectors (using xpath or regex etc) for each schema field we need to extract. This service will employ a range of tools to achieve this, escalating to a more powerful tool as necessary until the objective is complete. These include;
        - Libraries (e.g. lxml.html.diff) to help compare page structures
        - A "repeating block detector" that finds groups of structurally similar elements
        - LangChain (has extraction chains)
        - Instructor (a Python library that forces LLM output into Pydantic models) - Define your job schema as a Pydantic model, and instructor guarantees you get valid structured output.
        - Extruct library to extract structured data (JSON-LD, Microdata, RDFa)
        - LLaVA (or a similar open-source multimodal mode) via Ollama, to look at a screenshot of the page and identify the job listing region
        - 3b paramter LLM
        - 8b parameter LLM
    - Extraction
        - Scrapy (Pyhton)

- Jobs
    - All sites should be crawled regularly (cron job every 2 hours) to extract all job listings, and their job listing information.
    - All jobs that are extracted should have; role-title, company name, location, description at a minimum.
    - They should also ideally have; job type (full-time, part-time, permanent, contract, casual/temp), salary information, date job posted, job expiry date
    - Jobs are scored based on quality - including depth of information. Jobs with a low enough score will be excluded  (e.g. flagged as inactive)
    - Jobs that when processed are found to contain bad-words, or scam-words will be excluded (e.g. flagged as inactive)
    - Jobs that are duplicates (using a reasonable definition of duplicate - e.g. represent the the job opportunity) of another included job, will be excluded (e.g. flagged as inactive)
    - Jobs that passed their "job expiry date" should be exlucded (e.g. flagged as inactive)
    - Jobs that are more than 90 days older than their ":job posted date" should be exlcuded (e.g. flagged as inactive)
    - All other jobs that pass these checks are included in the production database (e.g. flagged as active)
    

    - Processes - these three pages should be moved to a separate menu section, with a heading of "Monitor Runs" below the "Prod Database" section.
        - In the 'crawl monitor' page (renaemd to 'Crawling Runs'), the top section should list all "crawl" runs - including scheduled (2-hourly cron) and ad-hoc runs. These should be listed in a paginated tabke/list containing columns/values for crawl type, status, company, site, number of jobs, date/time of run. When the user clicks on a row, the section underneath should update to show the full details of the crawl (e.g. All of the content currently displayed in the 'crawl monitor' page - and only relevant to that run)
        - Separately, we should have equiavlent pages for running the 'site' processes called 'Site Config Runs'. It should have a similar design with a table/list at the top showing all scheduled and adhoc runs (e.g. to re-define a site, and move sites not in status 1, to status 1) - the paginated list/table should contain relevant information about the run, and clicking on a table row should details of the run underneath (similar to the 'crawl monitor' page, but obviously relevant to the 'site run')
        - And again separately, we should have equiavlent pages for running the 'company' processes called 'Company Config Runs'. It should have a similar design with a table/list at the top showing all scheduled and adhoc runs (e.g. to re-define a company's sites, and move companies not in status 1, to status 1) - the paginated list/table should contain relevant information about the run, and clicking on a table row should details of the run underneath (similar to the 'crawl monitor' page, but obviously relevant to the 'company run')
        - And again separately, we should have equiavlent pages for running the 'Discovery Runs' processes called 'company run monitor'. It should have a similar design with a table/list at the top showing all scheduled and adhoc runs (e.g. to re-define a company's sites, and move companies not in status 1, to status 1) - the paginated list/table should contain relevant information about the run, and clicking on a table row should details of the run underneath (similar to the 'crawl monitor' page, but obviously relevant to the 'company run')

 - Rename the "Crawling" section to "Run Settings".

 - the "excluded sites" page should include ricebowl.my, seek.com.au, jobsdb.com, jobstreet.com, jora.com and all sites/domains stored with a "site_disabled_status" column with a true value.

 - The link discovery sources' page should have indeed.com and linkedin.com listed.

- The "Crawl Schedule" menu-link/page should be renamed to "Scheduled Runs". It should have 4 sections (white cards), one each for Discovery Runs, Company Config Runs, Site Config Runs, and Job Crawling Runs. Each section should have a "scheduler enabled" check box, and a text box to add a value for "Hourly interval between runs". There should be a view state (not editable text boxed, greyed out) with a light grey edit button. Clicking "edit" should enable the textbox so the value can be edited (change greyed out background to white), and the "edit" button should change to a primary-green "Save" button. When clicked, the value should be saved (text field becomes disabled/greyed out again, and edit button becomes a light grey "edit" button again). Also on the card should be some text explaining when the next run is scheduled (unless the checkbox is unticked, and there is no next scheduled run).

- Update the bad-words and scam-words pages to display the list of markets that the word applies to in columns, that are neatly visually/vertically aligned.

- Update the "Duplicates" and "Job quality" pages... so that the "Confirm" and "Overrule" buttons actually work (e.g. when clicked they change the status of the job, and disappear so the next job can be actioned.)

- Finally... once these changes have been made, set a 2-hour interval schedule for 4 run types. Trigger a run for all 4 runs immediately.

- Re-process all current duplicates/poor quality using the new appraoch and escalation process.
