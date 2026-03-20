import { useState, useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Globe, Search, ChevronLeft, ChevronRight, ExternalLink,
  Eye, CheckCircle2, XCircle, Cpu, Layers, Clock,
  ChevronDown, ChevronUp, Download,
} from 'lucide-react';
import { getCareerPages, getCareerPageDetail, exportCareerPages } from '../../lib/api';

// ── Types ─────────────────────────────────────────────────────────────────────

type SiteSummary = {
  id: string;
  company_id: string;
  company_name: string;
  company_domain: string;
  url: string;
  page_type: string | null;
  discovery_method: string | null;
  discovery_confidence: number | null;
  is_primary: boolean;
  is_paginated: boolean;
  requires_js_rendering: boolean;
  last_crawled_at: string | null;
  is_active: boolean;
  created_at: string;
  last_crawl_jobs: number | null;
  expected_jobs: number | null;
  has_template: boolean;
  template_accuracy: number | null;
  template_learned_via: string | null;
};

type CrawlLog = {
  status: string;
  started_at: string | null;
  completed_at: string | null;
  jobs_found: number;
  jobs_new: number;
  duration_seconds: number | null;
  error_message: string | null;
};

type Template = {
  id: string;
  template_type: string;
  selectors: Record<string, unknown>;
  learned_via: string;
  accuracy_score: number | null;
  last_validated_at: string | null;
  created_at: string;
};

type SiteDetail = {
  id: string;
  company_name: string | null;
  company_domain: string | null;
  url: string;
  page_type: string | null;
  discovery_method: string | null;
  discovery_confidence: number | null;
  is_primary: boolean;
  is_paginated: boolean;
  pagination_type: string | null;
  pagination_selector: string | null;
  requires_js_rendering: boolean;
  last_crawled_at: string | null;
  last_extraction_at: string | null;
  is_active: boolean;
  created_at: string;
  template: Template | null;
  crawl_history: CrawlLog[];
};

// ── Selector field definitions ────────────────────────────────────────────────

const KNOWN_FIELDS: { key: string; label: string; description: string }[] = [
  { key: 'job_container',   label: 'Job Container',    description: 'Repeating element wrapping each job listing' },
  { key: 'job_title',       label: 'Job Title',        description: 'Selector for the job title text' },
  { key: 'location',        label: 'Location',         description: 'Job location (city, state, remote)' },
  { key: 'job_type',        label: 'Job Type',         description: 'Employment type (full-time, part-time, contract)' },
  { key: 'description',     label: 'Description',      description: 'Full job description body' },
  { key: 'salary',          label: 'Salary',           description: 'Salary or compensation text' },
  { key: 'date_posted',     label: 'Date Posted',      description: 'Date the job was posted' },
  { key: 'apply_url',       label: 'Apply URL',        description: 'Link to the job detail or application page' },
  { key: 'department',      label: 'Department',       description: 'Team or department name' },
  { key: 'remote_flag',     label: 'Remote Flag',      description: 'Indicator that the role is remote/hybrid' },
  { key: 'next_page',       label: 'Next Page',        description: 'Pagination — selector for the "next page" control' },
  { key: 'load_more',       label: 'Load More',        description: 'Pagination — selector for a "load more" button' },
];

// ── Modal ─────────────────────────────────────────────────────────────────────

function SiteDetailModal({
  siteId,
  onClose,
}: {
  siteId: string;
  onClose: () => void;
}) {
  const overlayRef = useRef<HTMLDivElement>(null);
  const [showRawSelectors, setShowRawSelectors] = useState(false);

  const { data: detail, isLoading } = useQuery<SiteDetail>({
    queryKey: ['career-page-detail', siteId],
    queryFn: () => getCareerPageDetail(siteId),
    staleTime: 60_000,
  });

  // Close on Escape key
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  function overlayClick(e: React.MouseEvent) {
    if (e.target === overlayRef.current) onClose();
  }

  const selectors = detail?.template?.selectors ?? {};

  // Partition selectors into known + extra
  const knownKeys = new Set(KNOWN_FIELDS.map(f => f.key));
  const extraKeys = Object.keys(selectors).filter(k => !knownKeys.has(k));

  return (
    <div
      ref={overlayRef}
      onClick={overlayClick}
      className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4"
    >
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-3xl max-h-[90vh] flex flex-col">

        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-100 flex items-start justify-between gap-4">
          <div className="min-w-0">
            {isLoading ? (
              <div className="h-5 w-64 bg-gray-100 animate-pulse rounded" />
            ) : (
              <>
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-xs font-semibold text-gray-400 uppercase tracking-wider">{detail?.company_name}</span>
                  {detail?.is_primary && (
                    <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold bg-brand/10 text-brand uppercase tracking-wider">Primary</span>
                  )}
                  {detail?.requires_js_rendering && (
                    <span className="flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-semibold bg-amber-50 text-amber-600">
                      <Cpu className="w-3 h-3" /> JS
                    </span>
                  )}
                </div>
                <a
                  href={detail?.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm font-medium text-brand hover:underline flex items-center gap-1 truncate"
                >
                  {detail?.url}
                  <ExternalLink className="w-3.5 h-3.5 flex-shrink-0" />
                </a>
              </>
            )}
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 flex-shrink-0 mt-0.5">
            <XCircle className="w-5 h-5" />
          </button>
        </div>

        {/* Body — scrollable */}
        <div className="overflow-y-auto flex-1 px-6 py-5 space-y-6">
          {isLoading ? (
            <div className="space-y-3">
              {[1, 2, 3].map(i => (
                <div key={i} className="h-8 bg-gray-100 animate-pulse rounded" />
              ))}
            </div>
          ) : !detail ? (
            <p className="text-sm text-gray-400 text-center py-8">Failed to load site details.</p>
          ) : (
            <>
              {/* Meta info */}
              <section>
                <h3 className="text-xs font-bold uppercase tracking-widest text-gray-400 mb-3">Page Info</h3>
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                  {[
                    { label: 'Type',         value: detail.page_type?.replace('_', ' ') ?? '—' },
                    { label: 'Discovered via', value: detail.discovery_method?.replace('_', ' ') ?? '—' },
                    { label: 'Confidence',   value: detail.discovery_confidence != null ? `${(detail.discovery_confidence * 100).toFixed(0)}%` : '—' },
                    { label: 'Paginated',    value: detail.is_paginated ? 'Yes' : 'No' },
                    { label: 'Pagination type', value: detail.pagination_type ?? '—' },
                    { label: 'Last crawled', value: detail.last_crawled_at ? new Date(detail.last_crawled_at).toLocaleString() : 'Never' },
                  ].map(({ label, value }) => (
                    <div key={label} className="bg-gray-50 rounded-lg p-3">
                      <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-0.5">{label}</div>
                      <div className="text-sm text-gray-800 capitalize">{value}</div>
                    </div>
                  ))}
                </div>
                {detail.pagination_selector && (
                  <div className="mt-2 bg-gray-50 rounded-lg p-3">
                    <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1">Pagination Selector</div>
                    <code className="text-xs text-gray-700 font-mono">{detail.pagination_selector}</code>
                  </div>
                )}
              </section>

              {/* Template / Selectors */}
              <section>
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-xs font-bold uppercase tracking-widest text-gray-400">Extraction Selectors</h3>
                  {detail.template && (
                    <div className="flex items-center gap-3 text-xs text-gray-400">
                      <span className="flex items-center gap-1">
                        <Layers className="w-3.5 h-3.5" />
                        {detail.template.template_type}
                      </span>
                      {detail.template.accuracy_score != null && (
                        <span className={`font-semibold ${detail.template.accuracy_score >= 0.8 ? 'text-green-600' : detail.template.accuracy_score >= 0.5 ? 'text-amber-600' : 'text-red-500'}`}>
                          {(detail.template.accuracy_score * 100).toFixed(0)}% accuracy
                        </span>
                      )}
                    </div>
                  )}
                </div>

                {!detail.template ? (
                  <div className="rounded-xl border border-dashed border-gray-200 py-8 text-center text-gray-400 text-sm">
                    No extraction template learned yet for this page.
                    <br />
                    <span className="text-xs">Selectors are generated automatically after the first successful crawl.</span>
                  </div>
                ) : (
                  <>
                    <div className="rounded-xl overflow-hidden border border-gray-100">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="bg-gray-50 border-b border-gray-100">
                            <th className="text-left px-4 py-2.5 text-xs font-semibold text-gray-500 uppercase tracking-wide w-36">Field</th>
                            <th className="text-left px-4 py-2.5 text-xs font-semibold text-gray-500 uppercase tracking-wide">Selector / Value</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-50">
                          {KNOWN_FIELDS.map(({ key, label, description }) => {
                            const val = selectors[key];
                            const hasVal = val !== undefined && val !== null && val !== '';
                            return (
                              <tr key={key} className={hasVal ? '' : 'opacity-40'}>
                                <td className="px-4 py-2.5">
                                  <div className="font-medium text-gray-700 text-xs">{label}</div>
                                  <div className="text-[10px] text-gray-400 mt-0.5">{description}</div>
                                </td>
                                <td className="px-4 py-2.5">
                                  {hasVal ? (
                                    typeof val === 'object' ? (
                                      <code className="text-xs font-mono text-gray-700 bg-gray-50 px-2 py-0.5 rounded block">
                                        {JSON.stringify(val)}
                                      </code>
                                    ) : (
                                      <code className="text-xs font-mono text-gray-700 bg-gray-50 px-2 py-0.5 rounded">
                                        {String(val)}
                                      </code>
                                    )
                                  ) : (
                                    <span className="text-gray-300 text-xs">not configured</span>
                                  )}
                                </td>
                              </tr>
                            );
                          })}

                          {/* Extra / unknown keys */}
                          {extraKeys.map(key => (
                            <tr key={key}>
                              <td className="px-4 py-2.5">
                                <div className="font-medium text-gray-700 text-xs">{key}</div>
                                <div className="text-[10px] text-purple-400 mt-0.5">custom field</div>
                              </td>
                              <td className="px-4 py-2.5">
                                <code className="text-xs font-mono text-gray-700 bg-gray-50 px-2 py-0.5 rounded">
                                  {typeof selectors[key] === 'object'
                                    ? JSON.stringify(selectors[key])
                                    : String(selectors[key])}
                                </code>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    {/* Raw JSON toggle */}
                    <button
                      className="mt-2 text-xs text-gray-400 hover:text-gray-600 flex items-center gap-1"
                      onClick={() => setShowRawSelectors(v => !v)}
                    >
                      {showRawSelectors ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
                      {showRawSelectors ? 'Hide' : 'Show'} raw JSON
                    </button>
                    {showRawSelectors && (
                      <pre className="mt-2 bg-gray-900 text-green-400 text-xs rounded-xl p-4 overflow-x-auto max-h-48">
                        {JSON.stringify(selectors, null, 2)}
                      </pre>
                    )}
                  </>
                )}
              </section>

              {/* Crawl history */}
              {detail.crawl_history.length > 0 && (
                <section>
                  <h3 className="text-xs font-bold uppercase tracking-widest text-gray-400 mb-3">Recent Crawls</h3>
                  <div className="space-y-2">
                    {detail.crawl_history.map((log, i) => (
                      <div key={i} className="flex items-center gap-3 bg-gray-50 rounded-lg px-4 py-2.5 text-xs">
                        <span className={`w-2 h-2 rounded-full flex-shrink-0 ${log.status === 'success' ? 'bg-green-400' : log.status === 'running' ? 'bg-blue-400 animate-pulse' : 'bg-red-400'}`} />
                        <span className="text-gray-500 w-32 flex-shrink-0">
                          {log.started_at ? new Date(log.started_at).toLocaleString() : '—'}
                        </span>
                        <span className={`font-semibold ${log.status === 'success' ? 'text-green-600' : log.status === 'running' ? 'text-blue-600' : 'text-red-500'}`}>
                          {log.status}
                        </span>
                        {log.status === 'success' && (
                          <>
                            <span className="text-gray-600">{log.jobs_found ?? 0} jobs</span>
                            <span className="text-gray-400">({log.jobs_new ?? 0} new)</span>
                          </>
                        )}
                        {log.duration_seconds && (
                          <span className="text-gray-400 ml-auto flex items-center gap-1">
                            <Clock className="w-3 h-3" />{log.duration_seconds.toFixed(1)}s
                          </span>
                        )}
                        {log.error_message && (
                          <span className="text-red-400 truncate max-w-xs ml-auto" title={log.error_message}>
                            {log.error_message}
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                </section>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-3 border-t border-gray-100 flex justify-end">
          <button onClick={onClose} className="btn-secondary text-sm">Close</button>
        </div>
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export function CareerPages() {
  const [page, setPage] = useState(1);
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const [activeOnly, setActiveOnly] = useState(false);
  const [pageType, setPageType] = useState('');
  const [discoveryMethod, setDiscoveryMethod] = useState('');
  const [isPrimary, setIsPrimary] = useState('');
  const [hasTemplate, setHasTemplate] = useState('');
  const [requiresJs, setRequiresJs] = useState('');
  const [detailId, setDetailId] = useState<string | null>(null);
  const pageSize = 50;

  const filterParams = {
    page, page_size: pageSize,
    search: search || undefined,
    active_only: activeOnly || undefined,
    page_type: pageType || undefined,
    discovery_method: discoveryMethod || undefined,
    is_primary: isPrimary !== '' ? isPrimary === 'true' : undefined,
    has_template: hasTemplate !== '' ? hasTemplate === 'true' : undefined,
    requires_js: requiresJs !== '' ? requiresJs === 'true' : undefined,
  };

  const { data, isLoading } = useQuery({
    queryKey: ['career-pages', filterParams],
    queryFn: () => getCareerPages(filterParams),
    placeholderData: (prev) => prev,
  });

  const totalPages = data ? Math.ceil((data.total as number) / pageSize) : 1;

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    setSearch(searchInput);
    setPage(1);
  }

  function handleFilterChange(setter: (v: string) => void) {
    return (e: React.ChangeEvent<HTMLSelectElement>) => {
      setter(e.target.value);
      setPage(1);
    };
  }

  function clearAll() {
    setSearch(''); setSearchInput(''); setActiveOnly(false);
    setPageType(''); setDiscoveryMethod(''); setIsPrimary('');
    setHasTemplate(''); setRequiresJs(''); setPage(1);
  }

  const hasFilters = search || activeOnly || pageType || discoveryMethod || isPrimary || hasTemplate || requiresJs;

  function doExport() {
    exportCareerPages({
      search: search || undefined,
      active_only: activeOnly || undefined,
      page_type: pageType || undefined,
      discovery_method: discoveryMethod || undefined,
      is_primary: isPrimary || undefined,
      has_template: hasTemplate || undefined,
      requires_js: requiresJs || undefined,
    });
  }

  const items: SiteSummary[] = data?.items ?? [];

  return (
    <div className="p-6 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-blue-50 flex items-center justify-center">
            <Globe className="w-5 h-5 text-blue-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Sites</h1>
            <p className="text-sm text-gray-500">
              {data ? `${(data.total as number).toLocaleString()} career pages discovered` : null}
            </p>
          </div>
        </div>
        <button
          onClick={doExport}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
        >
          <Download className="w-4 h-4" /> Export CSV
        </button>
      </div>

      {/* Search bar */}
      <form onSubmit={handleSearch} className="flex gap-2">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            value={searchInput}
            onChange={e => setSearchInput(e.target.value)}
            placeholder="Search company or URL…"
            className="w-full pl-9 pr-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brand/30"
          />
        </div>
        <button type="submit" className="px-4 py-2 bg-[#0e8136] text-white text-sm font-medium rounded-lg hover:bg-[#0a6b2c] transition-colors">
          Search
        </button>
        {hasFilters && (
          <button type="button" onClick={clearAll}
            className="px-4 py-2 text-sm text-gray-500 border border-gray-200 rounded-lg hover:bg-gray-50">
            Clear
          </button>
        )}
      </form>

      {/* Filter row */}
      <div className="card p-3 flex flex-wrap gap-3 items-center">
        <select value={pageType} onChange={handleFilterChange(setPageType)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">All page types</option>
          <option value="careers_main">Careers main</option>
          <option value="ats_hosted">ATS hosted</option>
          <option value="jobs_listing">Jobs listing</option>
          <option value="department">Department</option>
        </select>
        <select value={discoveryMethod} onChange={handleFilterChange(setDiscoveryMethod)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">All discovery methods</option>
          <option value="ats_shortcut">ATS shortcut</option>
          <option value="heuristic">Heuristic</option>
          <option value="manual">Manual</option>
          <option value="aggregator">Aggregator</option>
        </select>
        <select value={isPrimary} onChange={handleFilterChange(setIsPrimary)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">Primary / All</option>
          <option value="true">Primary only</option>
          <option value="false">Non-primary</option>
        </select>
        <select value={hasTemplate} onChange={handleFilterChange(setHasTemplate)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">Template / All</option>
          <option value="true">Has template</option>
          <option value="false">No template</option>
        </select>
        <select value={requiresJs} onChange={handleFilterChange(setRequiresJs)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">JS rendering / All</option>
          <option value="true">Requires JS</option>
          <option value="false">No JS needed</option>
        </select>
        <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
          <input type="checkbox" checked={activeOnly}
            onChange={e => { setActiveOnly(e.target.checked); setPage(1); }}
            className="rounded" />
          Active only
        </label>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-100 flex items-center justify-between">
          <span className="text-sm font-medium text-gray-700">
            {data ? `${(data.total as number).toLocaleString()} sites` : null}
          </span>
          {data && totalPages > 1 && (
            <span className="text-xs text-gray-400">Page {page} of {totalPages}</span>
          )}
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-100">
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Company</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">URL</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Type</th>
                <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Exp. Jobs</th>
                <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Last Crawl</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Template</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Status</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Crawled</th>
                <th className="px-4 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {!isLoading && !items.length ? (
                <tr><td colSpan={9} className="px-4 py-10 text-center text-gray-400 text-sm">No sites discovered yet.</td></tr>
              ) : (
                items.map((site) => (
                  <tr key={site.id} className="hover:bg-gray-50 transition-colors">
                    {/* Company */}
                    <td className="px-4 py-3">
                      <div className="font-medium text-gray-900 text-xs">{site.company_name}</div>
                      <div className="text-[10px] text-gray-400 font-mono mt-0.5">{site.company_domain}</div>
                    </td>

                    {/* URL */}
                    <td className="px-4 py-3 max-w-xs">
                      <a
                        href={site.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-brand hover:underline text-xs truncate flex items-center gap-1 max-w-[260px]"
                        title={site.url}
                      >
                        <span className="truncate">{site.url.replace(/^https?:\/\//,'')}</span>
                        <ExternalLink className="w-3 h-3 flex-shrink-0" />
                      </a>
                      <div className="flex items-center gap-1.5 mt-1">
                        {site.is_primary && (
                          <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold bg-brand/10 text-brand">Primary</span>
                        )}
                        {site.requires_js_rendering && (
                          <span className="flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] font-semibold bg-amber-50 text-amber-600">
                            <Cpu className="w-3 h-3" />JS
                          </span>
                        )}
                        {site.is_paginated && (
                          <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold bg-blue-50 text-blue-600">Paginated</span>
                        )}
                      </div>
                    </td>

                    {/* Page type */}
                    <td className="px-4 py-3 text-xs text-gray-500 capitalize">
                      {site.page_type?.replace('_', ' ') ?? <span className="text-gray-300">—</span>}
                    </td>

                    {/* Expected jobs */}
                    <td className="px-4 py-3 text-right text-xs font-medium text-gray-700">
                      {site.expected_jobs != null
                        ? site.expected_jobs.toLocaleString()
                        : <span className="text-gray-300">—</span>}
                    </td>

                    {/* Last crawl jobs */}
                    <td className="px-4 py-3 text-right text-xs font-medium text-gray-700">
                      {site.last_crawl_jobs != null
                        ? site.last_crawl_jobs.toLocaleString()
                        : <span className="text-gray-300">—</span>}
                    </td>

                    {/* Template */}
                    <td className="px-4 py-3">
                      {site.has_template ? (
                        <div className="flex flex-col gap-0.5">
                          <div className="flex items-center gap-1 text-green-600 text-xs">
                            <CheckCircle2 className="w-3.5 h-3.5" /> Learned
                          </div>
                          {site.template_accuracy != null && (
                            <span className={`text-[10px] font-medium ${site.template_accuracy >= 0.8 ? 'text-green-600' : site.template_accuracy >= 0.5 ? 'text-amber-600' : 'text-red-500'}`}>
                              {(site.template_accuracy * 100).toFixed(0)}% accuracy
                            </span>
                          )}
                        </div>
                      ) : (
                        <span className="text-gray-300 text-xs">None</span>
                      )}
                    </td>

                    {/* Status */}
                    <td className="px-4 py-3">
                      <span className={site.is_active ? 'badge-green' : 'badge-gray'}>
                        {site.is_active ? 'Active' : 'Inactive'}
                      </span>
                    </td>

                    {/* Last crawled */}
                    <td className="px-4 py-3 text-xs text-gray-400">
                      {site.last_crawled_at
                        ? new Date(site.last_crawled_at).toLocaleDateString()
                        : 'Never'}
                    </td>

                    {/* Actions */}
                    <td className="px-4 py-3">
                      <button
                        onClick={() => setDetailId(site.id)}
                        className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50 hover:border-gray-300 transition-colors"
                        title="View details"
                      >
                        <Eye className="w-3.5 h-3.5" />
                        Details
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {data && totalPages > 1 && (
          <div className="px-4 py-3 border-t border-gray-100 flex items-center justify-between">
            <button
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
              className="flex items-center gap-1 px-3 py-1.5 text-sm border border-gray-200 rounded-lg disabled:opacity-40 hover:bg-gray-50"
            >
              <ChevronLeft className="w-4 h-4" /> Prev
            </button>
            <span className="text-xs text-gray-500">{page} / {totalPages}</span>
            <button
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="flex items-center gap-1 px-3 py-1.5 text-sm border border-gray-200 rounded-lg disabled:opacity-40 hover:bg-gray-50"
            >
              Next <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        )}
      </div>

      {/* Detail modal */}
      {detailId && (
        <SiteDetailModal siteId={detailId} onClose={() => setDetailId(null)} />
      )}
    </div>
  );
}
