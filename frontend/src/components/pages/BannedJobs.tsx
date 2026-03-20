import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '../../lib/api';
import { Search, AlertTriangle } from 'lucide-react';

function getBannedJobs(params: Record<string, unknown>) {
  return api.get('/jobs/banned', { params }).then(r => r.data);
}

export function BannedJobs() {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState('');
  const [country, setCountry] = useState('');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');

  const { data, isLoading } = useQuery({
    queryKey: ['banned-jobs', page, search, country, dateFrom, dateTo],
    queryFn: () => getBannedJobs({
      page,
      page_size: 50,
      search: search || undefined,
      location_country: country || undefined,
      date_from: dateFrom || undefined,
      date_to: dateTo || undefined,
    }),
    placeholderData: (prev) => prev,
    refetchInterval: 30000,
  });

  const total = data?.total ?? 0;
  const totalPages = Math.ceil(total / 50) || 1;

  const scoreColor = (score: number) => {
    if (score < 20) return 'text-red-600 font-bold';
    if (score < 40) return 'text-orange-500 font-semibold';
    return 'text-yellow-600';
  };

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center gap-3">
        <AlertTriangle className="w-6 h-6 text-red-500" />
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Banned Jobs</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {total.toLocaleString()} jobs excluded from results due to low quality score or policy violations
          </p>
        </div>
      </div>

      {/* Filters */}
      <div className="card p-4 flex flex-wrap gap-3 items-end">
        <div className="relative flex-1 min-w-48">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search jobs, companies, reasons..."
            value={search}
            onChange={e => { setSearch(e.target.value); setPage(1); }}
            className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-brand/30"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Country</label>
          <select
            value={country}
            onChange={e => { setCountry(e.target.value); setPage(1); }}
            className="px-3 py-2 border border-gray-200 rounded-md text-sm text-gray-700 focus:outline-none"
          >
            <option value="">All countries</option>
            <option value="Australia">Australia</option>
            <option value="New Zealand">New Zealand</option>
            <option value="Malaysia">Malaysia</option>
            <option value="Singapore">Singapore</option>
          </select>
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Crawled from</label>
          <input
            type="date"
            value={dateFrom}
            onChange={e => { setDateFrom(e.target.value); setPage(1); }}
            className="px-3 py-2 border border-gray-200 rounded-md text-sm text-gray-700 focus:outline-none"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Crawled to</label>
          <input
            type="date"
            value={dateTo}
            onChange={e => { setDateTo(e.target.value); setPage(1); }}
            className="px-3 py-2 border border-gray-200 rounded-md text-sm text-gray-700 focus:outline-none"
          />
        </div>
        {(search || country || dateFrom || dateTo) && (
          <button
            onClick={() => { setSearch(''); setCountry(''); setDateFrom(''); setDateTo(''); setPage(1); }}
            className="btn-secondary text-sm px-3 py-2"
          >
            Clear
          </button>
        )}
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              {['Job Title', 'Company / Site', 'Country', 'Score', 'Primary Reason', 'Offending Snippet', 'Crawled'].map(h => (
                <th key={h} className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {isLoading ? (
              <tr><td colSpan={7} className="px-4 py-10 text-center text-gray-400">Loading...</td></tr>
            ) : !data?.items?.length ? (
              <tr>
                <td colSpan={7} className="px-4 py-12 text-center">
                  <AlertTriangle className="w-8 h-8 text-gray-300 mx-auto mb-2" />
                  <div className="text-gray-400 text-sm">No banned jobs found.</div>
                  <div className="text-gray-300 text-xs mt-1">Run quality scoring from the Analytics page to populate this list.</div>
                </td>
              </tr>
            ) : (
              data.items.map((j: Record<string, unknown>) => (
                <tr key={String(j.id)} className="hover:bg-red-50/30">
                  <td className="px-4 py-3 max-w-[200px]">
                    <a
                      href={String(j.source_url)}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="font-medium text-gray-800 hover:text-brand text-xs truncate block"
                    >
                      {String(j.title)}
                    </a>
                  </td>
                  <td className="px-4 py-3 text-xs">
                    <div className="font-medium text-gray-700">{j.company_name ? String(j.company_name) : '—'}</div>
                    {j.company_domain ? <div className="text-gray-400">{String(j.company_domain)}</div> : null}
                  </td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{j.location_country ? String(j.location_country) : '—'}</td>
                  <td className="px-4 py-3">
                    <span className={`text-sm ${scoreColor(Number(j.quality_score))}`}>
                      {Math.round(Number(j.quality_score))}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-600 max-w-[180px]">
                    <span className="line-clamp-2">{j.primary_reason ? String(j.primary_reason) : '—'}</span>
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-400 max-w-[200px]">
                    {j.snippet ? (
                      <span className="italic line-clamp-2" title={String(j.snippet)}>
                        "{String(j.snippet).slice(0, 100)}{String(j.snippet).length > 100 ? '…' : ''}"
                      </span>
                    ) : <span>—</span>}
                  </td>
                  <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">
                    {j.first_seen_at ? new Date(String(j.first_seen_at)).toLocaleDateString() : '—'}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>

        {totalPages > 1 && (
          <div className="p-3 border-t border-gray-100 flex items-center justify-between">
            <span className="text-xs text-gray-500">Page {page} of {totalPages} · {total.toLocaleString()} total</span>
            <div className="flex gap-1">
              <button onClick={() => setPage(1)} disabled={page === 1} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">«</button>
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="btn-secondary text-xs px-3 py-1 disabled:opacity-40">Prev</button>
              <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn-secondary text-xs px-3 py-1 disabled:opacity-40">Next</button>
              <button onClick={() => setPage(totalPages)} disabled={page >= totalPages} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">»</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
