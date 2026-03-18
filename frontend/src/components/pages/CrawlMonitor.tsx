import { useQuery } from '@tanstack/react-query';
import { getCrawlHistory, getActiveCrawls, triggerFullCrawl } from '../../lib/api';
import { Play, RefreshCw } from 'lucide-react';

export function CrawlMonitor() {
  const { data: active, refetch: refetchActive } = useQuery({
    queryKey: ['crawl-active'],
    queryFn: getActiveCrawls,
    refetchInterval: 5000,
  });
  const { data: history } = useQuery({
    queryKey: ['crawl-history-full'],
    queryFn: () => getCrawlHistory(50),
    refetchInterval: 10000,
  });

  const handleFullCrawl = async () => {
    await triggerFullCrawl();
    refetchActive();
    alert('Full crawl cycle queued!');
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Crawl Monitor</h1>
        <button onClick={handleFullCrawl} className="btn-primary flex items-center gap-2">
          <Play className="w-4 h-4" /> Trigger Full Crawl
        </button>
      </div>

      {/* Active */}
      <div className="card p-5">
        <div className="flex items-center justify-between mb-3">
          <h2 className="font-semibold text-gray-900">Active Crawls</h2>
          <button onClick={() => refetchActive()} className="text-gray-400 hover:text-gray-600">
            <RefreshCw className="w-4 h-4" />
          </button>
        </div>
        {!active?.length ? (
          <p className="text-sm text-gray-400">No crawls currently running.</p>
        ) : (
          <div className="space-y-2">
            {active.map((log: Record<string, unknown>) => (
              <div key={String(log.id)} className="flex items-center justify-between bg-blue-50 rounded-md p-3">
                <div>
                  <span className="text-sm font-medium text-blue-800">{String(log.crawl_type)}</span>
                  <span className="text-xs text-blue-600 ml-2">{String(log.company_id ?? '').slice(0, 8)}</span>
                </div>
                <span className="badge-blue">running</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* History */}
      <div className="card">
        <div className="p-4 border-b border-gray-100">
          <h2 className="font-semibold text-gray-900">Crawl History</h2>
        </div>
        <div className="overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                {['Type', 'Status', 'Started', 'Duration', 'Jobs Found', 'New', 'Error'].map(h => (
                  <th key={h} className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {!history?.length ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No crawl history yet.</td></tr>
              ) : (
                history.map((log: Record<string, unknown>) => (
                  <tr key={String(log.id)} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-gray-700">{String(log.crawl_type)}</td>
                    <td className="px-4 py-3">
                      <span className={`badge ${
                        log.status === 'success' ? 'badge-green' :
                        log.status === 'running' ? 'badge-blue' :
                        log.status === 'failed' ? 'badge-red' : 'badge-gray'
                      }`}>{String(log.status)}</span>
                    </td>
                    <td className="px-4 py-3 text-gray-400 text-xs">
                      {log.started_at ? new Date(String(log.started_at)).toLocaleString() : '—'}
                    </td>
                    <td className="px-4 py-3 text-gray-500 text-xs">
                      {log.duration_seconds ? `${Number(log.duration_seconds).toFixed(1)}s` : '—'}
                    </td>
                    <td className="px-4 py-3 text-gray-700">{String(log.jobs_found ?? 0)}</td>
                    <td className="px-4 py-3 text-green-600">+{String(log.jobs_new ?? 0)}</td>
                    <td className="px-4 py-3 text-red-500 text-xs truncate max-w-xs">
                      {log.error_message ? String(log.error_message).slice(0, 60) : '—'}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
