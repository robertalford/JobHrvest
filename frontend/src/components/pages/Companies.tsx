import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getCompanies, triggerCompanyCrawl } from '../../lib/api';
import { Search, Plus, PlayCircle } from 'lucide-react';

export function Companies() {
  const [search, setSearch] = useState('');
  const { data, isLoading } = useQuery({
    queryKey: ['companies', search],
    queryFn: () => getCompanies({ search, page_size: 100 }),
    staleTime: 30000,
  });

  const handleCrawl = async (id: string) => {
    await triggerCompanyCrawl(id);
    alert('Crawl queued!');
  };

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Companies</h1>
          <p className="text-sm text-gray-500 mt-1">{data?.total ?? '—'} companies</p>
        </div>
        <button className="btn-primary flex items-center gap-2">
          <Plus className="w-4 h-4" /> Add Company
        </button>
      </div>

      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
        <input
          type="text"
          placeholder="Search companies..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-brand/30"
        />
      </div>

      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              {['Company', 'Domain', 'ATS', 'Priority', 'Last Crawl', 'Status', ''].map(h => (
                <th key={h} className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {isLoading ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">Loading...</td></tr>
            ) : !data?.items?.length ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No companies yet. Add companies to get started.</td></tr>
            ) : (
              data.items.map((c: Record<string, unknown>) => (
                <tr key={String(c.id)} className="hover:bg-gray-50">
                  <td className="px-4 py-3 font-medium text-gray-900">{String(c.name)}</td>
                  <td className="px-4 py-3 text-gray-500">{String(c.domain)}</td>
                  <td className="px-4 py-3">
                    {c.ats_platform ? (
                      <span className="badge-blue">{String(c.ats_platform)}</span>
                    ) : <span className="text-gray-400">—</span>}
                  </td>
                  <td className="px-4 py-3 text-gray-500">{String(c.crawl_priority)}</td>
                  <td className="px-4 py-3 text-gray-400 text-xs">
                    {c.last_crawl_at ? new Date(String(c.last_crawl_at)).toLocaleDateString() : 'Never'}
                  </td>
                  <td className="px-4 py-3">
                    <span className={c.is_active ? 'badge-green' : 'badge-gray'}>
                      {c.is_active ? 'Active' : 'Inactive'}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => handleCrawl(String(c.id))}
                      className="text-gray-400 hover:text-brand transition-colors"
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
    </div>
  );
}
