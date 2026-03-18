import { useQuery } from '@tanstack/react-query';
import { getJobStats, getSystemHealth, getCrawlHistory } from '../../lib/api';
import { StatCard } from '../ui/StatCard';
import { Activity, CheckCircle, XCircle } from 'lucide-react';

export function Dashboard() {
  const { data: stats } = useQuery({ queryKey: ['job-stats'], queryFn: getJobStats, refetchInterval: 30000 });
  const { data: health } = useQuery({ queryKey: ['health'], queryFn: getSystemHealth, refetchInterval: 15000 });
  const { data: crawlHistory } = useQuery({ queryKey: ['crawl-history'], queryFn: () => getCrawlHistory(10) });

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <p className="text-sm text-gray-500 mt-1">JobHarvest — AU market overview</p>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Total Active Jobs" value={stats?.active ?? '—'} />
        <StatCard label="New Today" value={stats?.new_today ?? '—'} />
        <StatCard label="New This Week" value={stats?.new_this_week ?? '—'} />
        <StatCard label="Total Jobs (all time)" value={stats?.total ?? '—'} />
      </div>

      {/* System Health */}
      <div className="card p-5">
        <h2 className="text-base font-semibold text-gray-900 mb-4">System Health</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {health?.services && Object.entries(health.services).map(([svc, status]) => (
            <div key={svc} className="flex items-center gap-2">
              {status === 'ok'
                ? <CheckCircle className="w-4 h-4 text-green-500" />
                : <XCircle className="w-4 h-4 text-red-500" />
              }
              <div>
                <div className="text-xs font-medium text-gray-700 capitalize">{svc}</div>
                <div className={`text-xs ${status === 'ok' ? 'text-green-600' : 'text-red-600'}`}>
                  {String(status)}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Recent Crawls */}
      <div className="card">
        <div className="p-4 border-b border-gray-100">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-gray-500" />
            <h2 className="text-base font-semibold text-gray-900">Recent Crawls</h2>
          </div>
        </div>
        <div className="divide-y divide-gray-100">
          {!crawlHistory || crawlHistory.length === 0 ? (
            <div className="p-6 text-center text-sm text-gray-400">No crawls yet — add companies and trigger a crawl to get started.</div>
          ) : (
            crawlHistory.map((log: Record<string, unknown>) => (
              <div key={String(log.id)} className="p-4 flex items-center justify-between">
                <div>
                  <div className="text-sm font-medium text-gray-800">{String(log.crawl_type)}</div>
                  <div className="text-xs text-gray-400">{log.started_at ? new Date(String(log.started_at)).toLocaleString() : '—'}</div>
                </div>
                <div className="flex items-center gap-4">
                  <span className="text-xs text-gray-500">{String(log.jobs_found ?? 0)} jobs found</span>
                  <span className={`badge ${log.status === 'success' ? 'badge-green' : log.status === 'running' ? 'badge-blue' : 'badge-red'}`}>
                    {String(log.status)}
                  </span>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
