import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getJobs, exportJobs } from '../../lib/api';
import { Search, Download, Briefcase, ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react';

const BAND_COLORS: Record<string, string> = {
  excellent: '#0e8136',
  good: '#22c55e',
  fair: '#eab308',
  poor: '#f97316',
  disqualified: '#ef4444',
};

function qualityBand(score: number | null): string {
  if (score === null || score === undefined) return 'unscored';
  if (score >= 80) return 'excellent';
  if (score >= 60) return 'good';
  if (score >= 40) return 'fair';
  if (score >= 20) return 'poor';
  return 'disqualified';
}

function QualityBadge({ score }: { score: number | null }) {
  const band = qualityBand(score);
  if (band === 'unscored') return <span className="text-gray-300 text-xs">—</span>;
  return (
    <span
      className="inline-block px-1.5 py-0.5 rounded text-xs font-semibold text-white"
      style={{ backgroundColor: BAND_COLORS[band] ?? '#6b7280' }}
      title={band}
    >
      {score !== null ? Math.round(score) : '?'}
    </span>
  );
}

export function Jobs() {
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(1);
  const [remoteType, setRemoteType] = useState('');
  const [employmentType, setEmploymentType] = useState('');
  const [seniorityLevel, setSeniorityLevel] = useState('');
  const [qualityBandFilter, setQualityBandFilter] = useState('');
  const pageSize = 50;

  const filterParams = {
    search: search || undefined,
    page,
    page_size: pageSize,
    remote_type: remoteType || undefined,
    employment_type: employmentType || undefined,
    seniority_level: seniorityLevel || undefined,
    quality_band: qualityBandFilter || undefined,
  };

  const { data, isLoading } = useQuery({
    queryKey: ['jobs', filterParams],
    queryFn: () => getJobs(filterParams),
    staleTime: 10000,
    placeholderData: (prev) => prev,
  });

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
    setSearch(''); setSearchInput(''); setRemoteType('');
    setEmploymentType(''); setSeniorityLevel(''); setQualityBandFilter(''); setPage(1);
  }

  const hasFilters = search || remoteType || employmentType || seniorityLevel || qualityBandFilter;

  function doExport() {
    exportJobs({
      search: search || undefined,
      remote_type: remoteType || undefined,
      employment_type: employmentType || undefined,
      seniority_level: seniorityLevel || undefined,
      quality_band: qualityBandFilter || undefined,
    });
  }

  return (
    <div className="p-6 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-purple-50 flex items-center justify-center">
            <Briefcase className="w-5 h-5 text-purple-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Jobs</h1>
            <p className="text-sm text-gray-500 mt-0.5">{total.toLocaleString()} active jobs</p>
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
            placeholder="Search job titles and descriptions…"
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
        <select value={qualityBandFilter} onChange={handleFilterChange(setQualityBandFilter)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">All quality bands</option>
          <option value="excellent">Excellent (80+)</option>
          <option value="good">Good (60–79)</option>
          <option value="fair">Fair (40–59)</option>
          <option value="poor">Poor (20–39)</option>
          <option value="disqualified">Disqualified (0–19)</option>
        </select>
        <select value={employmentType} onChange={handleFilterChange(setEmploymentType)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">All employment types</option>
          <option value="full_time">Full-time</option>
          <option value="part_time">Part-time</option>
          <option value="contract">Contract</option>
          <option value="casual">Casual</option>
          <option value="internship">Internship</option>
        </select>
        <select value={remoteType} onChange={handleFilterChange(setRemoteType)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">All remote types</option>
          <option value="fully_remote">Remote</option>
          <option value="hybrid">Hybrid</option>
          <option value="onsite">On-site</option>
        </select>
        <select value={seniorityLevel} onChange={handleFilterChange(setSeniorityLevel)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-brand/30 bg-white">
          <option value="">All seniority levels</option>
          <option value="entry">Entry</option>
          <option value="mid">Mid</option>
          <option value="senior">Senior</option>
          <option value="lead">Lead</option>
          <option value="executive">Executive</option>
        </select>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-100 flex items-center justify-between">
          <span className="text-sm font-medium text-gray-700">{total.toLocaleString()} jobs</span>
          {totalPages > 1 && <span className="text-xs text-gray-400">Page {page} of {totalPages}</span>}
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Title</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Company</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Location</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Type</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Salary</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Quality</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Method</th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">First Seen</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {!isLoading && !data?.items?.length ? (
                <tr><td colSpan={8} className="px-4 py-10 text-center text-gray-400">No jobs found. Run a crawl to extract job listings.</td></tr>
              ) : (
                (data?.items ?? []).map((j: Record<string, unknown>) => (
                  <tr key={String(j.id)} className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-3 max-w-xs">
                      {j.source_url ? (
                        <a href={String(j.source_url)} target="_blank" rel="noopener noreferrer"
                          className="font-medium text-gray-900 hover:text-brand flex items-center gap-1 truncate">
                          <span className="truncate">{String(j.title)}</span>
                          <ExternalLink className="w-3 h-3 flex-shrink-0 text-gray-400" />
                        </a>
                      ) : (
                        <span className="font-medium text-gray-900">{String(j.title)}</span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <div className="text-xs font-medium text-gray-700">
                        {j.company_name ? String(j.company_name) : <span className="text-gray-300">—</span>}
                      </div>
                      {j.company_domain ? (
                        <div className="text-[10px] text-gray-400 font-mono mt-0.5">{String(j.company_domain)}</div>
                      ) : null}
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">
                      {j.location_raw ? String(j.location_raw) : <span className="text-gray-300">—</span>}
                    </td>
                    <td className="px-4 py-3">
                      {j.employment_type
                        ? <span className="badge-gray text-xs">{String(j.employment_type).replace('_', ' ')}</span>
                        : <span className="text-gray-300">—</span>}
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">
                      {j.salary_raw ? String(j.salary_raw).slice(0, 30) : <span className="text-gray-300">—</span>}
                    </td>
                    <td className="px-4 py-3">
                      <QualityBadge score={j.quality_score as number | null} />
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-xs ${j.extraction_method === 'schema_org' ? 'badge-green' : j.extraction_method === 'ats_api' ? 'badge-blue' : 'badge-gray'}`}>
                        {j.extraction_method ? String(j.extraction_method).replace('_', ' ') : '—'}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-400">
                      {j.first_seen_at ? new Date(String(j.first_seen_at)).toLocaleDateString() : '—'}
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
    </div>
  );
}
