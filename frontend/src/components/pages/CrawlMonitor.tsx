import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getCrawlHistory, triggerFullCrawl, getQueueStats } from '../../lib/api';
import { Activity, Play, Loader2, CheckCircle, XCircle, Clock } from 'lucide-react';

const STATUS_COLORS: Record<string, string> = {
  success: 'badge-green',
  running: 'badge-blue',
  failed: 'badge-red',
  pending: 'badge-gray',
};

export function CrawlMonitor() {
  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState('');
  const [selectedLog, setSelectedLog] = useState<Record<string, unknown> | null>(null);
  const qc = useQueryClient();

  const { data: queueStats } = useQuery<Record<string, Record<string, number>>>({
    queryKey: ['queue-stats'],
    queryFn: getQueueStats,
    refetchInterval: 5000,
  });

  const { data: history, isLoading } = useQuery<{ items: Record<string, unknown>[]; total: number }>({
    queryKey: ['crawl-history', page, statusFilter, 'full_crawl'],
    queryFn: () => getCrawlHistory(page, 50, statusFilter || undefined, 'full_crawl'),
    refetchInterval: 5000,
    placeholderData: (prev) => prev,
  });

  const crawlMutation = useMutation({
    mutationFn: triggerFullCrawl,
    onSuccess: () => {
      setTimeout(() => qc.invalidateQueries({ queryKey: ['crawl-history'] }), 1500);
    },
  });

  const qs = queueStats?.['job_crawling'] ?? {};
  const queueDepth  = qs.pending    ?? null;
  const processing  = qs.processing ?? 0;
  const done        = qs.done       ?? 0;
  const failed      = qs.failed     ?? 0;

  const items = history?.items ?? [];
  const total = history?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / 50));

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl flex items-center justify-center" style={{ backgroundColor: '#0284c718' }}>
            <Activity className="w-5 h-5" style={{ color: '#0284c7' }} />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Site Crawling Runs</h1>
            <p className="text-sm text-gray-500">Extract live job listings from mapped career pages</p>
          </div>
        </div>
        <button
          onClick={() => crawlMutation.mutate()}
          disabled={crawlMutation.isPending}
          className="btn-primary flex items-center gap-2"
        >
          {crawlMutation.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
          Run Now
        </button>
      </div>

      {crawlMutation.isSuccess && (
        <div className="rounded-lg bg-green-50 border border-green-200 px-4 py-3 text-sm text-green-700">
          Crawl triggered successfully — runs will appear in the log below shortly.
        </div>
      )}

      {/* Stats row */}
      <div className="grid grid-cols-4 gap-4">
        <div className="card p-4 flex items-center gap-3">
          <Clock className="w-5 h-5 text-amber-500 flex-shrink-0" />
          <div>
            <div className="text-xl font-bold text-gray-900">{queueDepth !== null ? queueDepth.toLocaleString() : '—'}</div>
            <div className="text-xs text-gray-500">Queue depth</div>
          </div>
        </div>
        <div className="card p-4 flex items-center gap-3">
          <Loader2 className="w-5 h-5 text-blue-500 flex-shrink-0" />
          <div>
            <div className="text-xl font-bold text-gray-900">{processing.toLocaleString()}</div>
            <div className="text-xs text-gray-500">Running now</div>
          </div>
        </div>
        <div className="card p-4 flex items-center gap-3">
          <CheckCircle className="w-5 h-5 text-green-500 flex-shrink-0" />
          <div>
            <div className="text-xl font-bold text-gray-900">{done.toLocaleString()}</div>
            <div className="text-xs text-gray-500">Completed</div>
          </div>
        </div>
        <div className="card p-4 flex items-center gap-3">
          <XCircle className="w-5 h-5 text-red-500 flex-shrink-0" />
          <div>
            <div className="text-xl font-bold text-gray-900">{failed.toLocaleString()}</div>
            <div className="text-xs text-gray-500">Failed</div>
          </div>
        </div>
      </div>

      {/* Table + detail panel */}
      <div className="flex gap-4">
        <div className="card flex-1 min-w-0">
          <div className="p-4 border-b border-gray-100 flex items-center justify-between gap-3">
            <h2 className="font-semibold text-gray-900">
              Crawl History
              {total > 0 && <span className="ml-2 text-sm font-normal text-gray-400">{total.toLocaleString()} total</span>}
            </h2>
            <div className="flex items-center gap-2">
              <select
                value={statusFilter}
                onChange={e => { setStatusFilter(e.target.value); setPage(1); }}
                className="px-2 py-1 border border-gray-200 rounded text-xs text-gray-600 focus:outline-none"
              >
                <option value="">All statuses</option>
                <option value="success">Success</option>
                <option value="running">Running</option>
                <option value="failed">Failed</option>
              </select>
              <span className="text-xs text-gray-400">auto-updates 5s</span>
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  {['Company', 'Status', 'Started', 'Duration', 'Pages', 'Jobs', 'New', 'Error'].map(h => (
                    <th key={h} className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {isLoading ? (
                  <tr><td colSpan={8} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
                ) : items.length === 0 ? (
                  <tr><td colSpan={8} className="px-4 py-8 text-center text-gray-400">No crawl history yet.</td></tr>
                ) : items.map((log) => (
                  <tr
                    key={String(log.id)}
                    className={`hover:bg-gray-50 cursor-pointer ${selectedLog?.id === log.id ? 'bg-blue-50' : ''}`}
                    onClick={() => setSelectedLog(log === selectedLog ? null : log)}
                  >
                    <td className="px-4 py-3 max-w-[160px]">
                      <div className="font-medium text-gray-800 truncate">{log.company_name ? String(log.company_name) : '—'}</div>
                      {!!log.company_domain && <div className="text-xs text-gray-400 truncate">{String(log.company_domain)}</div>}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`badge ${STATUS_COLORS[String(log.status)] ?? 'badge-gray'}`}>{String(log.status)}</span>
                    </td>
                    <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">
                      {log.started_at ? new Date(String(log.started_at)).toLocaleString() : '—'}
                    </td>
                    <td className="px-4 py-3 text-gray-500 text-xs">
                      {log.duration_seconds ? `${Number(log.duration_seconds).toFixed(1)}s` : '—'}
                    </td>
                    <td className="px-4 py-3 text-gray-600 text-center text-xs">{String(log.pages_crawled ?? 0)}</td>
                    <td className="px-4 py-3 text-gray-700 text-center font-medium text-xs">{String(log.jobs_found ?? 0)}</td>
                    <td className="px-4 py-3 text-center text-xs">
                      {Number(log.jobs_new ?? 0) > 0
                        ? <span className="text-green-600 font-medium">+{String(log.jobs_new)}</span>
                        : <span className="text-gray-300">—</span>}
                    </td>
                    <td className="px-4 py-3 text-red-400 text-xs max-w-[160px] truncate" title={log.error_message ? String(log.error_message) : ''}>
                      {log.error_message ? String(log.error_message).slice(0, 50) : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {totalPages > 1 && (
            <div className="p-3 border-t border-gray-100 flex items-center justify-between">
              <span className="text-xs text-gray-500">Page {page} of {totalPages} · {total.toLocaleString()} records</span>
              <div className="flex gap-1">
                <button onClick={() => setPage(1)} disabled={page === 1} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">«</button>
                <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="btn-secondary text-xs px-3 py-1 disabled:opacity-40">Prev</button>
                <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn-secondary text-xs px-3 py-1 disabled:opacity-40">Next</button>
                <button onClick={() => setPage(totalPages)} disabled={page >= totalPages} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">»</button>
              </div>
            </div>
          )}
        </div>

        {/* Detail panel */}
        {selectedLog && (
          <div className="card w-72 flex-shrink-0 self-start p-4 space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-gray-800 text-sm">Run Detail</h3>
              <button className="text-gray-400 hover:text-gray-600 text-xs" onClick={() => setSelectedLog(null)}>✕</button>
            </div>
            <div className="space-y-2 text-xs">
              <Row label="ID" value={String(selectedLog.id).slice(0, 8) + '…'} />
              <Row label="Company" value={selectedLog.company_name ? String(selectedLog.company_name) : '—'} />
              <Row label="Domain" value={selectedLog.company_domain ? String(selectedLog.company_domain) : '—'} />
              <Row label="Status" value={String(selectedLog.status)} />
              <Row label="Started" value={selectedLog.started_at ? new Date(String(selectedLog.started_at)).toLocaleString() : '—'} />
              <Row label="Completed" value={selectedLog.completed_at ? new Date(String(selectedLog.completed_at)).toLocaleString() : '—'} />
              <Row label="Duration" value={selectedLog.duration_seconds ? `${Number(selectedLog.duration_seconds).toFixed(1)}s` : '—'} />
              <Row label="Pages crawled" value={String(selectedLog.pages_crawled ?? 0)} />
              <Row label="Jobs found" value={String(selectedLog.jobs_found ?? 0)} />
              <Row label="New jobs" value={String(selectedLog.jobs_new ?? 0)} />
              {!!selectedLog.error_message && (
                <div>
                  <div className="text-gray-400 mb-1">Error</div>
                  <div className="text-red-500 bg-red-50 rounded p-2 break-words">{String(selectedLog.error_message)}</div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-2">
      <span className="text-gray-400 flex-shrink-0">{label}</span>
      <span className="text-gray-800 text-right truncate">{value}</span>
    </div>
  );
}
