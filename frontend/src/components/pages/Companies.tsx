import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getCompanies, triggerCompanyCrawl, exportCompanies } from '../../lib/api';
import { Search, PlayCircle, ExternalLink, Globe, ChevronDown, ChevronUp, Download, Building2 } from 'lucide-react';

type Site = {
  id: string;
  url: string;
  page_type: string | null;
  is_primary: boolean;
};

type CompanyRow = {
  id: string;
  name: string;
  domain: string;
  ats_platform: string | null;
  crawl_priority: number;
  last_crawl_at: string | null;
  is_active: boolean;
  sites: Site[];
  site_count: number;
  last_crawl_jobs: number | null;
  expected_jobs: number | null;
};

// Truncated site URL list with expand/collapse
function SiteList({ sites }: { sites: Site[] }) {
  const [expanded, setExpanded] = useState(false);
  if (!sites.length) return <span className="text-gray-300 text-xs">—</span>;

  const visible = expanded ? sites : sites.slice(0, 2);
  const remainder = sites.length - 2;

  return (
    <div className="space-y-1">
      {visible.map(s => (
        <a
          key={s.id}
          href={s.url}
          target="_blank"
          rel="noopener noreferrer"
          title={s.url}
          className="flex items-center gap-1 text-brand hover:underline text-xs max-w-[240px]"
        >
          {s.is_primary && (
            <span className="w-1.5 h-1.5 rounded-full bg-brand flex-shrink-0" title="Primary" />
          )}
          <span className="truncate">{s.url.replace(/^https?:\/\/(www\.)?/, '')}</span>
          <ExternalLink className="w-3 h-3 flex-shrink-0 text-gray-400" />
        </a>
      ))}
      {sites.length > 2 && (
        <button
          onClick={e => { e.stopPropagation(); setExpanded(v => !v); }}
          className="flex items-center gap-0.5 text-[10px] text-gray-400 hover:text-gray-600"
        >
          {expanded ? (
            <><ChevronUp className="w-3 h-3" /> less</>
          ) : (
            <><ChevronDown className="w-3 h-3" /> {remainder} more</>
          )}
        </button>
      )}
    </div>
  );
}

export function Companies() {
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const [atsPlatform, setAtsPlatform] = useState('');
  const [isActive, setIsActive] = useState('');
  const [page, setPage] = useState(1);
  const pageSize = 50;

  const filterParams = {
    search: search || undefined,
    ats_platform: atsPlatform || undefined,
    is_active: isActive !== '' ? isActive === 'true' : undefined,
    page, page_size: pageSize,
  };

  const { data, isLoading } = useQuery({
    queryKey: ['companies', filterParams],
    queryFn: () => getCompanies(filterParams),
    staleTime: 15000,
    placeholderData: (prev) => prev,
  });

  const handleCrawl = async (id: string) => {
    await triggerCompanyCrawl(id);
    alert('Crawl queued!');
  };

  const items: CompanyRow[] = data?.items ?? [];
  const total: number = data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    setSearch(searchInput);
    setPage(1);
  }

  function handleFilterChange(setter: (v: string) => void) {
    return (e: React.ChangeEvent<HTMLSelectElement>) => { setter(e.target.value); setPage(1); };
  }

  function clearAll() {
    setSearch(''); setSearchInput(''); setAtsPlatform(''); setIsActive(''); setPage(1);
  }

  const hasFilters = search || atsPlatform || isActive;

  function doExport() {
    exportCompanies({
      search: search || undefined,
      ats_platform: atsPlatform || undefined,
      is_active: isActive || undefined,
    });
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-green-50 flex items-center justify-center">
            <Building2 className="w-5 h-5 text-green-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Companies</h1>
            <p className="text-sm text-gray-500 mt-0.5">{total.toLocaleString()} companies</p>
          </div>
        </div>
        <button
          onClick={doExport}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
        >
          <Download className="w-4 h-4" /> Export CSV
        </button>
      </div>

      {/* Search */}
      <form onSubmit={handleSearch} className="flex gap-2">
        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search companies…"
            value={searchInput}
            onChange={e => setSearchInput(e.target.value)}
            className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brand/30"
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
        <select value={atsPlatform} onChange={handleFilterChange(setAtsPlatform)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">All ATS platforms</option>
          <option value="greenhouse">Greenhouse</option>
          <option value="lever">Lever</option>
          <option value="workday">Workday</option>
          <option value="bamboohr">BambooHR</option>
          <option value="icims">iCIMS</option>
          <option value="taleo">Taleo</option>
          <option value="smartrecruiters">SmartRecruiters</option>
          <option value="ashby">Ashby</option>
          <option value="jobvite">Jobvite</option>
          <option value="jazzhr">JazzHR</option>
        </select>
        <select value={isActive} onChange={handleFilterChange(setIsActive)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">Active / All</option>
          <option value="true">Active only</option>
          <option value="false">Inactive only</option>
        </select>
      </div>

      <div className="card overflow-hidden">
        {/* Table header */}
        <div className="px-5 py-3 border-b border-gray-100 flex items-center justify-between">
          <span className="text-sm font-medium text-gray-700">{total.toLocaleString()} companies</span>
          {totalPages > 1 && <span className="text-xs text-gray-400">Page {page} of {totalPages}</span>}
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Company</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">ATS</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">
                  <span className="flex items-center gap-1"><Globe className="w-3.5 h-3.5" />Sites</span>
                </th>
                <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Exp. Jobs</th>
                <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Last Crawl</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Status</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Crawled</th>
                <th className="px-4 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {!isLoading && !items.length ? (
                <tr><td colSpan={8} className="px-4 py-10 text-center text-gray-400">No companies yet.</td></tr>
              ) : (
                items.map(c => (
                  <tr key={c.id} className="hover:bg-gray-50 align-top">
                    {/* Company */}
                    <td className="px-4 py-3">
                      <div className="font-medium text-gray-900">{c.name}</div>
                      <div className="text-xs text-gray-400 font-mono mt-0.5">{c.domain}</div>
                    </td>

                    {/* ATS */}
                    <td className="px-4 py-3">
                      {c.ats_platform
                        ? <span className="badge-blue">{c.ats_platform}</span>
                        : <span className="text-gray-300">—</span>}
                    </td>

                    {/* Sites */}
                    <td className="px-4 py-3 min-w-[220px] max-w-[280px]">
                      {c.site_count > 0 ? (
                        <div>
                          <div className="text-[10px] text-gray-400 mb-1 font-medium">
                            {c.site_count} site{c.site_count !== 1 ? 's' : ''}
                          </div>
                          <SiteList sites={c.sites} />
                        </div>
                      ) : (
                        <span className="text-gray-300 text-xs">No sites yet</span>
                      )}
                    </td>

                    {/* Expected jobs */}
                    <td className="px-4 py-3 text-right">
                      {c.expected_jobs != null
                        ? <span className="text-sm font-medium text-gray-700">{c.expected_jobs.toLocaleString()}</span>
                        : <span className="text-gray-300 text-xs">—</span>}
                    </td>

                    {/* Last crawl jobs */}
                    <td className="px-4 py-3 text-right">
                      {c.last_crawl_jobs != null
                        ? <span className="text-sm font-medium text-gray-700">{c.last_crawl_jobs.toLocaleString()}</span>
                        : <span className="text-gray-300 text-xs">—</span>}
                    </td>

                    {/* Status */}
                    <td className="px-4 py-3">
                      <span className={c.is_active ? 'badge-green' : 'badge-gray'}>
                        {c.is_active ? 'Active' : 'Inactive'}
                      </span>
                    </td>

                    {/* Last crawled */}
                    <td className="px-4 py-3 text-xs text-gray-400">
                      {c.last_crawl_at ? new Date(c.last_crawl_at).toLocaleDateString() : 'Never'}
                    </td>

                    {/* Crawl action */}
                    <td className="px-4 py-3">
                      <button
                        onClick={() => handleCrawl(c.id)}
                        className="text-gray-300 hover:text-brand transition-colors"
                        title="Trigger crawl"
                      >
                        <PlayCircle className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="px-4 py-3 border-t border-gray-100 flex items-center justify-between">
            <button
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
              className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg disabled:opacity-40 hover:bg-gray-50"
            >
              Prev
            </button>
            <span className="text-xs text-gray-500">{page} / {totalPages}</span>
            <button
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg disabled:opacity-40 hover:bg-gray-50"
            >
              Next
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
