import type { ReactNode } from 'react';
import {
  Globe, Briefcase, BarChart3, Upload, Building2,
  PlayCircle, Clock, Trash2, PlusCircle, Filter,
  ChevronRight, AlertTriangle, CheckCircle, Zap, Database,
  Search, Settings, ShieldCheck,
} from 'lucide-react';

function Section({ id, title, icon: Icon, children }: {
  id: string;
  title: string;
  icon: React.ElementType;
  children: ReactNode;
}) {
  return (
    <section id={id} className="card p-6 space-y-4 scroll-mt-6">
      <div className="flex items-center gap-3 pb-3 border-b border-gray-100">
        <div className="w-8 h-8 rounded-lg bg-brand/10 flex items-center justify-center flex-shrink-0">
          <Icon className="w-4 h-4 text-brand" />
        </div>
        <h2 className="text-lg font-semibold text-gray-900">{title}</h2>
      </div>
      {children}
    </section>
  );
}

function Step({ n, title, children }: { n: number; title: string; children: ReactNode }) {
  return (
    <div className="flex gap-4">
      <div className="flex-shrink-0 w-7 h-7 rounded-full bg-brand text-white text-xs font-bold flex items-center justify-center mt-0.5">
        {n}
      </div>
      <div>
        <div className="font-medium text-gray-800 text-sm">{title}</div>
        <div className="text-sm text-gray-500 mt-0.5">{children}</div>
      </div>
    </div>
  );
}

function Note({ type = 'info', children }: { type?: 'info' | 'warn' | 'ok'; children: ReactNode }) {
  const styles = {
    info: 'bg-blue-50 border-blue-200 text-blue-800',
    warn: 'bg-amber-50 border-amber-200 text-amber-800',
    ok:   'bg-green-50 border-green-200 text-green-800',
  };
  const icons = { info: AlertTriangle, warn: AlertTriangle, ok: CheckCircle };
  const Icon = icons[type];
  return (
    <div className={`flex gap-2 p-3 rounded-lg border text-sm ${styles[type]}`}>
      <Icon className="w-4 h-4 flex-shrink-0 mt-0.5" />
      <div>{children}</div>
    </div>
  );
}

function Code({ children }: { children: string }) {
  return (
    <code className="block bg-gray-900 text-green-400 text-xs rounded-md px-4 py-3 font-mono whitespace-pre overflow-x-auto">
      {children}
    </code>
  );
}

function Pill({ children }: { children: ReactNode }) {
  return (
    <span className="inline-block px-2 py-0.5 bg-gray-100 text-gray-700 rounded text-xs font-medium">
      {children}
    </span>
  );
}

const NAV = [
  { id: 'overview', label: 'System Overview' },
  { id: 'how-it-works', label: 'How It Works' },
  { id: 'run-crawl', label: 'Running Crawlers' },
  { id: 'scheduled', label: 'Scheduled Crawls' },
  { id: 'add-sites', label: 'Adding Companies' },
  { id: 'lead-imports', label: 'Lead Imports' },
  { id: 'review-results', label: 'Reviewing Results' },
  { id: 'quality', label: 'Quality Scoring' },
  { id: 'delete', label: 'Deleting Data' },
  { id: 'markets', label: 'Markets & Config' },
];

export function HowTo() {
  return (
    <div className="flex gap-8 p-6">
      {/* Sticky sidebar nav */}
      <aside className="hidden lg:block w-44 flex-shrink-0">
        <div className="sticky top-6 space-y-0.5">
          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">Contents</div>
          {NAV.map(({ id, label }) => (
            <a
              key={id}
              href={`#${id}`}
              className="flex items-center gap-1.5 text-sm text-gray-500 hover:text-brand py-1 group"
            >
              <ChevronRight className="w-3 h-3 flex-shrink-0 opacity-0 group-hover:opacity-100 text-brand" />
              {label}
            </a>
          ))}
        </div>
      </aside>

      {/* Main content */}
      <div className="flex-1 space-y-6 max-w-3xl">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">How To Use JobHarvest</h1>
          <p className="text-sm text-gray-500 mt-1">
            A guide to the system's functionality and how to get the most out of it.
          </p>
        </div>

        {/* ── System Overview ── */}
        <Section id="overview" title="System Overview" icon={Database}>
          <p className="text-sm text-gray-600">
            JobHarvest is an intelligent job listing crawler, extractor, and aggregation engine. It crawls
            company career pages directly — not aggregator sites — to build the most accurate and
            up-to-date database of job listings possible.
          </p>

          <div className="grid grid-cols-2 md:grid-cols-3 gap-3 pt-1">
            {[
              { icon: Building2, label: 'Companies', desc: '41k+ employer sites tracked across 8 markets' },
              { icon: Globe, label: 'Career Pages', desc: 'Discovered automatically via heuristics & ATS detection' },
              { icon: Briefcase, label: 'Jobs', desc: 'Extracted with multi-method pipeline for maximum accuracy' },
              { icon: ShieldCheck, label: 'Quality Scoring', desc: 'Every job scored 0–100 for completeness, scam, discrimination' },
              { icon: BarChart3, label: 'Analytics', desc: 'Field coverage, extraction accuracy, quality distribution' },
              { icon: Upload, label: 'Lead Imports', desc: 'Bulk company ingestion from CSV with per-market tracking' },
            ].map(({ icon: Icon, label, desc }) => (
              <div key={label} className="bg-gray-50 rounded-lg p-3 space-y-1">
                <div className="flex items-center gap-2">
                  <Icon className="w-4 h-4 text-brand" />
                  <span className="text-sm font-medium text-gray-800">{label}</span>
                </div>
                <p className="text-xs text-gray-500">{desc}</p>
              </div>
            ))}
          </div>

          <Note type="warn">
            <strong>Off-limits sites:</strong> SEEK, Jora, Jobstreet, and JobsDB are hard-blocked at the
            network layer. The system will never send requests to these domains — not even for link
            discovery. This is enforced in code and cannot be overridden via the UI.
          </Note>
        </Section>

        {/* ── How It Works ── */}
        <Section id="how-it-works" title="How It Works" icon={Zap}>
          <p className="text-sm text-gray-600">
            JobHarvest uses a multi-stage pipeline. Each stage applies multiple complementary methods
            that cross-validate each other for maximum accuracy.
          </p>

          <div className="space-y-3">
            {[
              {
                step: '1. ATS Fingerprinting',
                desc: 'Detects which Applicant Tracking System a company uses (Greenhouse, Lever, Workday, BambooHR, iCIMS, Taleo, SmartRecruiters, Ashby, Jobvite, JazzHR). Known ATS platforms have predictable structures, enabling highly accurate extraction.',
              },
              {
                step: '2. Career Page Discovery',
                desc: 'Finds the careers page via URL heuristics (scoring /careers, /jobs paths), link text analysis, and ATS shortcuts. ATS shortcut discovery can jump directly to the job listings page.',
              },
              {
                step: '3. Job Listing Extraction',
                desc: 'Applies five methods in parallel: structured data (Schema.org JSON-LD), ATS-specific HTML extractors, learned CSS selectors (templates), LLM extraction via Ollama (llama3.1:8b), and structural block detection. Results are cross-validated and merged.',
              },
              {
                step: '4. Field Normalisation',
                desc: 'Location parser extracts city/state/country and remote type. Salary parser handles ranges, K-suffixes, hourly/daily/super, and multi-currency. Tag extractor identifies skills, technologies, qualifications, and industry.',
              },
              {
                step: '5. Quality Scoring',
                desc: 'Each job is scored 0–100 across field completeness, description quality, scam detection, discrimination language, and inappropriate content. Companies get an aggregate site score that weights outliers heavily — one scam job caps a site at 20/100.',
              },
              {
                step: '6. Aggregator Link Discovery',
                desc: 'Indeed AU is crawled to harvest outbound links to company career pages. Job content is never extracted from aggregators — only the destination URLs are used to discover new companies.',
              },
            ].map(({ step, desc }, i) => (
              <div key={i} className="flex gap-3">
                <div className="flex-shrink-0 mt-0.5">
                  <div className="w-6 h-6 rounded-full bg-brand/10 text-brand text-xs font-bold flex items-center justify-center">
                    {i + 1}
                  </div>
                </div>
                <div>
                  <div className="text-sm font-semibold text-gray-800">{step}</div>
                  <p className="text-sm text-gray-500 mt-0.5">{desc}</p>
                </div>
              </div>
            ))}
          </div>
        </Section>

        {/* ── Running Crawlers ── */}
        <Section id="run-crawl" title="Running Crawlers" icon={PlayCircle}>
          <p className="text-sm text-gray-600">
            You can trigger crawls manually from the UI or via the API.
          </p>

          <div className="space-y-5">
            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">From the UI</div>
              <div className="space-y-3">
                <Step n={1} title="Trigger a full crawl cycle">
                  Go to <strong>Crawl Monitor</strong> and click <Pill>Trigger Full Crawl</Pill>. This queues
                  crawl tasks for every company whose <code className="text-xs bg-gray-100 px-1 rounded">next_crawl_at</code> is
                  due. On first run, all companies qualify.
                </Step>
                <Step n={2} title="Crawl a single company">
                  Go to <strong>Companies</strong>, find the company, and click its <Pill>Crawl Now</Pill> button.
                  This triggers an immediate crawl regardless of schedule.
                </Step>
                <Step n={3} title="Harvest aggregator links">
                  In <strong>Crawl Monitor</strong>, click <Pill>Run Aggregator Harvest</Pill>. This searches
                  Indeed AU across 10 default queries and discovers new company career pages to add to the queue.
                </Step>
              </div>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Via the API</div>
              <Code>{`# Trigger full crawl cycle
curl -X POST http://localhost:8001/api/v1/crawl/trigger-full

# Crawl a specific company (replace with actual UUID)
curl -X POST http://localhost:8001/api/v1/companies/{id}/crawl

# Run aggregator harvester (Indeed AU link discovery)
curl -X POST http://localhost:8001/api/v1/crawl/harvest-aggregators`}</Code>
            </div>

            <Note type="info">
              Crawls run as Celery tasks — they are queued and processed asynchronously by the worker
              containers. Check <strong>Crawl Monitor</strong> for live status and history.
            </Note>
          </div>
        </Section>

        {/* ── Scheduled Crawls ── */}
        <Section id="scheduled" title="Scheduled Crawls" icon={Clock}>
          <p className="text-sm text-gray-600">
            Celery Beat runs automatically and manages the crawl schedule. No action is needed — it starts
            with the stack.
          </p>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="text-left px-3 py-2 text-gray-500 font-medium text-xs">Task</th>
                  <th className="text-left px-3 py-2 text-gray-500 font-medium text-xs">Schedule</th>
                  <th className="text-left px-3 py-2 text-gray-500 font-medium text-xs">What it does</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {[
                  ['Full Crawl Cycle', 'Every 1 hour', 'Crawls all companies with a past-due next_crawl_at'],
                  ['Aggregator Harvest', 'Every 6 hours', 'Discovers new companies via Indeed AU link harvesting'],
                  ['Mark Inactive Jobs', 'Every 24 hours', 'Sets jobs not seen in 7+ days to inactive'],
                  ['Quality Scoring', 'On-demand (manual trigger)', 'Scores unscored jobs in batches of 1,000'],
                ].map(([task, schedule, desc]) => (
                  <tr key={task} className="hover:bg-gray-50">
                    <td className="px-3 py-2.5 font-medium text-gray-800">{task}</td>
                    <td className="px-3 py-2.5"><Pill>{schedule}</Pill></td>
                    <td className="px-3 py-2.5 text-gray-500">{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div>
            <div className="text-sm font-semibold text-gray-800 mb-2">Each company crawls on its own schedule</div>
            <p className="text-sm text-gray-500">
              Companies have a <code className="text-xs bg-gray-100 px-1 rounded">crawl_frequency_hours</code> setting
              (default: 24h). After each crawl, <code className="text-xs bg-gray-100 px-1 rounded">next_crawl_at</code> is
              set to <code className="text-xs bg-gray-100 px-1 rounded">now + crawl_frequency_hours</code>. High-priority
              companies (lower <code className="text-xs bg-gray-100 px-1 rounded">crawl_priority</code> number) are crawled
              first.
            </p>
          </div>

          <Note type="ok">
            To check that Celery Beat is running: go to <strong>Dashboard</strong> — the system health card
            shows Redis status (Beat requires Redis). You can also check container logs:
            <Code>{`docker compose logs celery-beat --tail=20`}</Code>
          </Note>
        </Section>

        {/* ── Adding Companies ── */}
        <Section id="add-sites" title="Adding Companies & Sites" icon={PlusCircle}>
          <div className="space-y-5">
            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Manually via the UI</div>
              <div className="space-y-3">
                <Step n={1} title="Go to Companies">
                  Click <strong>Companies</strong> in the sidebar.
                </Step>
                <Step n={2} title="Click Add Company">
                  Enter the company name and root URL (e.g. <code className="text-xs bg-gray-100 px-1 rounded">https://www.atlassian.com</code>).
                  Market, ATS platform, and crawl priority can be set optionally.
                </Step>
                <Step n={3} title="Trigger a crawl">
                  Click <Pill>Crawl Now</Pill> on the company row, or wait for the next scheduled cycle.
                </Step>
              </div>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Via the API</div>
              <Code>{`curl -X POST http://localhost:8001/api/v1/companies \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "Atlassian",
    "root_url": "https://www.atlassian.com",
    "market_code": "AU",
    "crawl_priority": 3
  }'`}</Code>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Via seed script (bulk)</div>
              <p className="text-sm text-gray-500 mb-2">
                Add entries to <code className="text-xs bg-gray-100 px-1 rounded">backend/scripts/seed.py</code> and
                re-run inside the API container:
              </p>
              <Code>{`docker compose exec api python scripts/seed.py`}</Code>
            </div>

            <Note type="info">
              The system automatically deduplicates companies by domain. Adding a company whose domain
              already exists will update its name and leave the existing crawl data intact.
            </Note>
          </div>
        </Section>

        {/* ── Lead Imports ── */}
        <Section id="lead-imports" title="Lead Imports (Bulk CSV)" icon={Upload}>
          <p className="text-sm text-gray-600">
            The Lead Imports pipeline ingests bulk company lists from CSV and tracks the outcome of each
            lead. The 41,824-company dataset from <code className="text-xs bg-gray-100 px-1 rounded">ad_gap_data_all_markets.csv</code> has
            already been imported.
          </p>

          <div className="space-y-5">
            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">CSV format</div>
              <p className="text-sm text-gray-500 mb-2">
                The importer expects these columns (UTF-8 BOM encoded):
              </p>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead className="bg-gray-50">
                    <tr>
                      {['Column', 'Description'].map(h => (
                        <th key={h} className="text-left px-3 py-2 text-gray-500 font-medium">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {[
                      ['country_id', 'Market code: AU, SG, PH, NZ, MY, ID, TH, HK'],
                      ['advertiser_name', 'Company name'],
                      ['origin', 'Career site domain (fallback URL)'],
                      ['sample_linkout_url', 'Direct URL to career page (preferred over origin)'],
                      ['ad_origin_category', 'Category label for grouping'],
                      ['cnt_ads_202504_202509', 'Expected job count (for validation)'],
                      ['origin_rank_by_ads_count', 'Rank by ad volume'],
                    ].map(([col, desc]) => (
                      <tr key={col} className="hover:bg-gray-50">
                        <td className="px-3 py-2 font-mono text-brand">{col}</td>
                        <td className="px-3 py-2 text-gray-500">{desc}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Run an import</div>
              <Code>{`# Import all leads from default CSV path
docker compose exec api python scripts/import_leads.py

# Import a custom CSV
docker compose exec api python scripts/import_leads.py /storage/my_leads.csv

# Import only Australian leads (for testing)
docker compose exec api python scripts/import_leads.py --country AU --limit 100`}</Code>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Trigger via the UI</div>
              <p className="text-sm text-gray-500">
                Go to <strong>Lead Imports</strong> and click <Pill>Run Import</Pill>. This triggers the
                import in the background and the page will auto-refresh stats every 10 seconds.
              </p>
            </div>

            <Note type="info">
              Import results are tracked per lead in the <code className="text-xs bg-gray-100 px-1 rounded">lead_imports</code> table.
              Status values: <Pill>success</Pill> <Pill>failed</Pill> <Pill>blocked</Pill> <Pill>skipped</Pill>.
              Blocked leads are domains that matched the hard-block list (SEEK, Jora, etc.).
            </Note>
          </div>
        </Section>

        {/* ── Reviewing Results ── */}
        <Section id="review-results" title="Reviewing Results" icon={Search}>
          <div className="space-y-5">
            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Jobs</div>
              <p className="text-sm text-gray-500 mb-3">
                Go to <strong>Jobs</strong> to browse extracted job listings. Use the filters to narrow results:
              </p>
              <div className="grid grid-cols-2 gap-2">
                {[
                  ['Search', 'Full-text search across title and description'],
                  ['Remote type', 'Filter by Remote / Hybrid / On-site'],
                  ['Quality band', 'Show only Excellent, Good, Fair, Poor, or Disqualified jobs'],
                  ['Export', 'Download all jobs as CSV or JSON via the Export button'],
                ].map(([label, desc]) => (
                  <div key={label} className="flex gap-2 text-sm">
                    <Filter className="w-4 h-4 text-brand flex-shrink-0 mt-0.5" />
                    <div>
                      <span className="font-medium text-gray-800">{label}:</span>{' '}
                      <span className="text-gray-500">{desc}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Career Pages</div>
              <p className="text-sm text-gray-500">
                Go to <strong>Career Pages</strong> to see which pages have been discovered for each company,
                their discovery method, last crawl time, and whether they're marked as the primary listing page.
              </p>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Crawl Monitor</div>
              <p className="text-sm text-gray-500">
                Go to <strong>Crawl Monitor</strong> to see the history of every crawl run — how many
                pages were visited, jobs found/new/updated/removed, errors, duration, and extraction method used.
              </p>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Analytics</div>
              <p className="text-sm text-gray-500">
                Go to <strong>Analytics</strong> for aggregate views: jobs over time, field coverage rates,
                quality distribution by band, flag breakdown (scam/discrimination/bad words), and top/bottom
                quality sites.
              </p>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Via the API</div>
              <Code>{`# List jobs with filters
curl "http://localhost:8001/api/v1/jobs/?quality_min=60&remote_type=fully_remote"

# Export all active jobs as JSON
curl "http://localhost:8001/api/v1/jobs/export?format=json"

# Get extraction analytics
curl "http://localhost:8001/api/v1/analytics/field-coverage"
curl "http://localhost:8001/api/v1/analytics/quality-distribution"`}</Code>
            </div>
          </div>
        </Section>

        {/* ── Quality Scoring ── */}
        <Section id="quality" title="Quality Scoring" icon={ShieldCheck}>
          <p className="text-sm text-gray-600">
            Every job is scored 0–100. Scores above 60 are considered good quality for use.
          </p>

          <div className="grid grid-cols-5 gap-2">
            {[
              { band: 'Excellent', range: '80–100', color: '#0e8136' },
              { band: 'Good', range: '60–79', color: '#22c55e' },
              { band: 'Fair', range: '40–59', color: '#eab308' },
              { band: 'Poor', range: '20–39', color: '#f97316' },
              { band: 'Disqualified', range: '0–19', color: '#ef4444' },
            ].map(({ band, range, color }) => (
              <div key={band} className="text-center p-2 rounded-lg border border-gray-100">
                <div className="w-8 h-8 rounded-full mx-auto mb-1 flex items-center justify-center text-white text-xs font-bold" style={{ backgroundColor: color }}>
                  {range.split('–')[0]}
                </div>
                <div className="text-xs font-medium text-gray-700">{band}</div>
                <div className="text-xs text-gray-400">{range}</div>
              </div>
            ))}
          </div>

          <div className="space-y-2 text-sm">
            <div className="font-semibold text-gray-800">Score components</div>
            {[
              ['Field completeness', '0–50 pts', 'Title quality, location, employment type, description length, date posted, salary, requirements'],
              ['Description quality', '0–20 pts', 'Substantive (≥500 chars) = +20, reasonable (150–499) = +10, very short = 0, near-empty = −5'],
              ['Scam detection', 'Caps at 10', 'Wire transfers, MLM/pyramid, payment requests, guaranteed income scams'],
              ['Discrimination language', 'Caps at 10', 'Age, gender, ethnicity, nationality, religion preferences in job text'],
              ['Inappropriate content', 'Caps at 15', 'Profanity or obscene language in title or description'],
            ].map(([name, pts, desc]) => (
              <div key={name} className="flex gap-3 py-2 border-b border-gray-50 last:border-0">
                <div className="w-40 flex-shrink-0 font-medium text-gray-700">{name}</div>
                <div className="w-24 flex-shrink-0"><Pill>{pts}</Pill></div>
                <div className="text-gray-500">{desc}</div>
              </div>
            ))}
          </div>

          <div>
            <div className="text-sm font-semibold text-gray-800 mb-2">Trigger quality scoring</div>
            <p className="text-sm text-gray-500 mb-2">
              Go to <strong>Analytics</strong> and click <Pill>Score Jobs</Pill>, or via API:
            </p>
            <Code>{`curl -X POST http://localhost:8001/api/v1/analytics/trigger-quality-scoring`}</Code>
          </div>

          <Note type="warn">
            Site quality scores are NOT a simple average. A single scam job caps the entire site at 20/100.
            A discrimination violation caps it at 25/100. This intentionally makes harmful content
            disproportionately damaging to a site's overall score.
          </Note>
        </Section>

        {/* ── Deleting Data ── */}
        <Section id="delete" title="Deleting Data" icon={Trash2}>
          <Note type="warn">
            Deletions are permanent. There is no recycle bin. Always verify before deleting.
          </Note>

          <div className="space-y-4">
            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Deactivate vs Delete</div>
              <p className="text-sm text-gray-500">
                Most records have an <code className="text-xs bg-gray-100 px-1 rounded">is_active</code> flag.
                Prefer deactivation over deletion — it preserves history and can be reversed:
              </p>
              <Code>{`# Deactivate a company (stops future crawls, hides from active views)
curl -X PATCH http://localhost:8001/api/v1/companies/{id} \\
  -H "Content-Type: application/json" \\
  -d '{"is_active": false}'

# Reactivate
curl -X PATCH http://localhost:8001/api/v1/companies/{id} \\
  -d '{"is_active": true}'`}</Code>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Delete a company and all its data</div>
              <p className="text-sm text-gray-500 mb-2">
                Deleting a company cascades to its career pages and jobs (via <code className="text-xs bg-gray-100 px-1 rounded">ON DELETE CASCADE</code>):
              </p>
              <Code>{`curl -X DELETE http://localhost:8001/api/v1/companies/{id}`}</Code>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Clear all jobs for a company</div>
              <Code>{`# Via psql inside the postgres container
docker compose exec postgres psql -U jobharvest -d jobharvest -c \\
  "DELETE FROM jobs WHERE company_id = '{uuid}'"`}</Code>
            </div>

            <div>
              <div className="text-sm font-semibold text-gray-800 mb-2">Mark stale jobs as inactive</div>
              <p className="text-sm text-gray-500 mb-2">
                The scheduled task does this automatically (jobs not seen in 7+ days). To run on-demand:
              </p>
              <Code>{`curl -X POST http://localhost:8001/api/v1/crawl/mark-inactive`}</Code>
            </div>
          </div>
        </Section>

        {/* ── Markets & Config ── */}
        <Section id="markets" title="Markets & Configuration" icon={Settings}>
          <p className="text-sm text-gray-600">
            JobHarvest supports 8 markets. Each market has its own salary/location parsing rules,
            aggregator search queries, and currency.
          </p>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  {['Code', 'Market', 'Currency', 'Companies'].map(h => (
                    <th key={h} className="text-left px-3 py-2 text-gray-500 font-medium text-xs">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {[
                  ['AU', '🇦🇺 Australia', 'AUD', '15,325'],
                  ['SG', '🇸🇬 Singapore', 'SGD', '4,804'],
                  ['PH', '🇵🇭 Philippines', 'PHP', '4,398'],
                  ['NZ', '🇳🇿 New Zealand', 'NZD', '3,940'],
                  ['MY', '🇲🇾 Malaysia', 'MYR', '3,837'],
                  ['ID', '🇮🇩 Indonesia', 'IDR', '3,634'],
                  ['TH', '🇹🇭 Thailand', 'THB', '3,286'],
                  ['HK', '🇭🇰 Hong Kong', 'HKD', '2,600'],
                ].map(([code, name, currency, count]) => (
                  <tr key={code} className="hover:bg-gray-50">
                    <td className="px-3 py-2 font-mono text-brand font-semibold">{code}</td>
                    <td className="px-3 py-2 text-gray-800">{name}</td>
                    <td className="px-3 py-2"><Pill>{currency}</Pill></td>
                    <td className="px-3 py-2 text-gray-500">{count} leads</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div>
            <div className="text-sm font-semibold text-gray-800 mb-2">Adding a new market</div>
            <div className="space-y-2">
              <Step n={1} title="Add market config to seed.py">
                Add an entry to the <code className="text-xs bg-gray-100 px-1 rounded">MARKETS</code> list
                in <code className="text-xs bg-gray-100 px-1 rounded">backend/scripts/seed.py</code> with
                the market code, currency, locale, and parsing configs.
              </Step>
              <Step n={2} title="Re-run the seed script">
                <Code>{`docker compose exec api python scripts/seed.py`}</Code>
              </Step>
              <Step n={3} title="Import or add companies for the new market">
                Use the CSV importer or add companies manually via the API with the new market code.
              </Step>
            </div>
          </div>

          <div>
            <div className="text-sm font-semibold text-gray-800 mb-2">API access to market data</div>
            <Code>{`# List all markets
curl http://localhost:8001/api/v1/system/markets

# Health check (includes all service statuses)
curl http://localhost:8001/api/v1/health`}</Code>
          </div>
        </Section>

        <div className="text-xs text-gray-400 text-center pb-4">
          JobHarvest v0.2.0 — Multi-Market Edition · API at{' '}
          <a href="http://localhost:8001/docs" target="_blank" rel="noreferrer" className="text-brand hover:underline">
            localhost:8001/docs
          </a>
          {' '}· Built with FastAPI + React
        </div>
      </div>
    </div>
  );
}
