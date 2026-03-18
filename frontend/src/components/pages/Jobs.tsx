import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getJobs } from '../../lib/api';
import { Search, Download } from 'lucide-react';

export function Jobs() {
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(1);
  const [remoteType, setRemoteType] = useState('');

  const { data, isLoading } = useQuery({
    queryKey: ['jobs', search, page, remoteType],
    queryFn: () => getJobs({ search, page, page_size: 50, remote_type: remoteType || undefined }),
    staleTime: 30000,
  });

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Jobs</h1>
          <p className="text-sm text-gray-500 mt-1">{data?.total ?? '—'} active jobs</p>
        </div>
        <a href="/api/v1/jobs/export?format=csv" className="btn-secondary flex items-center gap-2">
          <Download className="w-4 h-4" /> Export CSV
        </a>
      </div>

      <div className="flex gap-3">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search job titles and descriptions..."
            value={search}
            onChange={e => { setSearch(e.target.value); setPage(1); }}
            className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-brand/30"
          />
        </div>
        <select
          value={remoteType}
          onChange={e => setRemoteType(e.target.value)}
          className="px-3 py-2 border border-gray-200 rounded-md text-sm text-gray-700 focus:outline-none"
        >
          <option value="">All types</option>
          <option value="fully_remote">Remote</option>
          <option value="hybrid">Hybrid</option>
          <option value="onsite">On-site</option>
        </select>
      </div>

      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              {['Title', 'Company', 'Location', 'Type', 'Salary', 'Method', 'First Seen'].map(h => (
                <th key={h} className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {isLoading ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">Loading...</td></tr>
            ) : !data?.items?.length ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No jobs found. Run a crawl to extract job listings.</td></tr>
            ) : (
              data.items.map((j: Record<string, unknown>) => (
                <tr key={String(j.id)} className="hover:bg-gray-50 cursor-pointer">
                  <td className="px-4 py-3">
                    <a href={String(j.source_url)} target="_blank" rel="noopener noreferrer"
                      className="font-medium text-gray-900 hover:text-brand">
                      {String(j.title)}
                    </a>
                  </td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{String(j.company_id).slice(0, 8)}</td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{j.location_raw ? String(j.location_raw) : '—'}</td>
                  <td className="px-4 py-3">
                    {j.employment_type ? <span className="badge-gray">{String(j.employment_type).replace('_', ' ')}</span> : '—'}
                  </td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{j.salary_raw ? String(j.salary_raw).slice(0, 30) : '—'}</td>
                  <td className="px-4 py-3">
                    <span className={`badge ${j.extraction_method === 'schema_org' ? 'badge-green' : j.extraction_method === 'ats_api' ? 'badge-blue' : 'badge-gray'}`}>
                      {j.extraction_method ? String(j.extraction_method) : '—'}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-gray-400 text-xs">
                    {j.first_seen_at ? new Date(String(j.first_seen_at)).toLocaleDateString() : '—'}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
        {data && data.total > 50 && (
          <div className="p-4 border-t border-gray-100 flex items-center justify-between">
            <span className="text-sm text-gray-500">Page {page} of {Math.ceil(data.total / 50)}</span>
            <div className="flex gap-2">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="btn-secondary text-xs px-3 py-1 disabled:opacity-50">Previous</button>
              <button onClick={() => setPage(p => p + 1)} disabled={page >= Math.ceil(data.total / 50)} className="btn-secondary text-xs px-3 py-1 disabled:opacity-50">Next</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
