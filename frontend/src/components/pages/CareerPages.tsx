import { useQuery } from '@tanstack/react-query';
import { api } from '../../lib/api';

export function CareerPages() {
  const { data, isLoading } = useQuery({
    queryKey: ['career-pages'],
    queryFn: () => api.get('/career-pages').then(r => r.data),
  });

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-2xl font-bold text-gray-900">Career Pages</h1>
      <p className="text-sm text-gray-500">{Array.isArray(data) ? data.length : '—'} discovered career pages</p>

      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              {['URL', 'Type', 'Discovery Method', 'Confidence', 'Primary', 'Last Crawled', 'Status'].map(h => (
                <th key={h} className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {isLoading ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">Loading...</td></tr>
            ) : !data?.length ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No career pages discovered yet.</td></tr>
            ) : (
              data.map((p: Record<string, unknown>) => (
                <tr key={String(p.id)} className="hover:bg-gray-50">
                  <td className="px-4 py-3 max-w-xs truncate">
                    <a href={String(p.url)} target="_blank" rel="noopener noreferrer" className="text-brand hover:underline text-xs">
                      {String(p.url)}
                    </a>
                  </td>
                  <td className="px-4 py-3 text-gray-500 text-xs">{p.page_type ? String(p.page_type) : '—'}</td>
                  <td className="px-4 py-3 text-xs">
                    {p.discovery_method ? <span className="badge-gray">{String(p.discovery_method)}</span> : '—'}
                  </td>
                  <td className="px-4 py-3 text-gray-500 text-xs">
                    {p.discovery_confidence ? `${(Number(p.discovery_confidence) * 100).toFixed(0)}%` : '—'}
                  </td>
                  <td className="px-4 py-3 text-xs">{p.is_primary ? <span className="badge-green">Primary</span> : '—'}</td>
                  <td className="px-4 py-3 text-gray-400 text-xs">
                    {p.last_crawled_at ? new Date(String(p.last_crawled_at)).toLocaleDateString() : 'Never'}
                  </td>
                  <td className="px-4 py-3">
                    <span className={p.is_active ? 'badge-green' : 'badge-gray'}>{p.is_active ? 'Active' : 'Inactive'}</span>
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
