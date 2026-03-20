/**
 * Monitor Runs Overview — one-page summary of all 4 run-type queues.
 * Shows pending/processing/done/failed counts with quick-trigger buttons.
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getQueueStats, getJobStats, triggerRun, resetStaleQueueItems } from '../../lib/api';
import { Link } from 'react-router-dom';
import {
  Globe, Building2, Search, Activity, Briefcase,
  Clock, Loader2, CheckCircle, XCircle, Play, ArrowRight, RefreshCw,
} from 'lucide-react';

interface QueueTypeDef {
  key: string;       // queue_type key in queue-stats response
  runType: string;   // value for POST /crawl/trigger/{runType}
  label: string;
  description: string;
  icon: React.ElementType;
  iconColor: string;
  href: string;
}

const QUEUE_TYPES: QueueTypeDef[] = [
  {
    key: 'discovery',
    runType: 'discovery',
    label: 'Discovery',
    description: 'Harvest aggregators to discover new company career pages',
    icon: Search,
    iconColor: '#f59e0b',
    href: '/discovery-runs',
  },
  {
    key: 'company_config',
    runType: 'company_config',
    label: 'Company Config',
    description: 'Find & map career pages for each company',
    icon: Building2,
    iconColor: '#6366f1',
    href: '/company-config-runs',
  },
  {
    key: 'site_config',
    runType: 'site_config',
    label: 'Site Config',
    description: 'Map job listing structure for each career page',
    icon: Globe,
    iconColor: '#0e8136',
    href: '/site-config-runs',
  },
  {
    key: 'job_crawling',
    runType: 'job_crawling',
    label: 'Site Crawling',
    description: 'Extract live job listings from mapped sites',
    icon: Activity,
    iconColor: '#0284c7',
    href: '/crawl',
  },
];

function StatPill({ value, label, colorClass }: { value: number; label: string; colorClass: string }) {
  return (
    <div className="flex flex-col items-center">
      <span className={`text-lg font-bold ${colorClass}`}>{value.toLocaleString()}</span>
      <span className="text-xs text-gray-400">{label}</span>
    </div>
  );
}

function QueueCard({ qt, stats, onTrigger, isTriggering }: {
  qt: QueueTypeDef;
  stats: Record<string, number> | undefined;
  onTrigger: () => void;
  isTriggering: boolean;
}) {
  const Icon = qt.icon;
  const pending    = stats?.pending    ?? 0;
  const processing = stats?.processing ?? 0;
  const done       = stats?.done       ?? 0;
  const failed     = stats?.failed     ?? 0;
  const total      = pending + processing + done + failed;

  // Progress bar: percentage of done+failed out of total
  const completedPct = total > 0 ? Math.round(((done + failed) / total) * 100) : 0;
  const failPct      = total > 0 ? Math.round((failed / total) * 100) : 0;

  return (
    <div className="card p-5 flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0"
            style={{ backgroundColor: qt.iconColor + '18' }}>
            <Icon className="w-4 h-4" style={{ color: qt.iconColor }} />
          </div>
          <div>
            <div className="font-semibold text-gray-900">{qt.label}</div>
            <div className="text-xs text-gray-400">{qt.description}</div>
          </div>
        </div>
        <button
          onClick={onTrigger}
          disabled={isTriggering}
          className="btn-secondary text-xs flex items-center gap-1 px-2 py-1"
          title="Trigger run now"
        >
          {isTriggering ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
          Run
        </button>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-4 gap-2 py-3 border-y border-gray-100">
        <StatPill value={pending}    label="Pending"    colorClass="text-amber-600" />
        <StatPill value={processing} label="Processing" colorClass="text-blue-600" />
        <StatPill value={done}       label="Done"       colorClass="text-green-600" />
        <StatPill value={failed}     label="Failed"     colorClass="text-red-500" />
      </div>

      {/* Progress bar */}
      <div>
        <div className="flex justify-between text-xs text-gray-400 mb-1">
          <span>{completedPct}% processed</span>
          <span>{total.toLocaleString()} total</span>
        </div>
        <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
          {total > 0 && (
            <div className="h-full flex">
              <div
                className="h-full bg-green-400 transition-all"
                style={{ width: `${completedPct - failPct}%` }}
              />
              {failPct > 0 && (
                <div
                  className="h-full bg-red-400 transition-all"
                  style={{ width: `${failPct}%` }}
                />
              )}
            </div>
          )}
        </div>
      </div>

      {/* Detail link */}
      <Link to={qt.href} className="flex items-center gap-1 text-xs text-blue-600 hover:underline self-end">
        View logs <ArrowRight className="w-3 h-3" />
      </Link>
    </div>
  );
}

export function MonitorRunsOverview() {
  const qc = useQueryClient();

  const { data: queueStats, isLoading } = useQuery<Record<string, Record<string, number>>>({
    queryKey: ['queue-stats'],
    queryFn: getQueueStats,
    refetchInterval: 5000,
  });

  const { data: jobStats } = useQuery<Record<string, number>>({
    queryKey: ['job-stats'],
    queryFn: getJobStats,
    refetchInterval: 30000,
  });

  const resetStaleMut = useMutation({
    mutationFn: () => resetStaleQueueItems(120),
    onSuccess: (data) => {
      qc.invalidateQueries({ queryKey: ['queue-stats'] });
      alert(`Reset ${data.reset} stuck item(s) back to pending.`);
    },
  });

  const triggerMuts = Object.fromEntries(
    QUEUE_TYPES.map(qt => [
      qt.runType,
      // eslint-disable-next-line react-hooks/rules-of-hooks
      useMutation({
        mutationFn: () => triggerRun(qt.runType),
        onSuccess: () => {
          setTimeout(() => qc.invalidateQueries({ queryKey: ['queue-stats'] }), 1500);
        },
      }),
    ])
  );

  const totals = QUEUE_TYPES.reduce(
    (acc, qt) => {
      const s = queueStats?.[qt.key] ?? {};
      acc.pending    += s.pending    ?? 0;
      acc.processing += s.processing ?? 0;
      acc.done       += s.done       ?? 0;
      acc.failed     += s.failed     ?? 0;
      return acc;
    },
    { pending: 0, processing: 0, done: 0, failed: 0 }
  );

  return (
    <div className="p-6 space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-gray-100 flex items-center justify-center">
            <Activity className="w-5 h-5 text-gray-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Monitor Runs</h1>
            <p className="text-sm text-gray-500">Live overview of all pipeline queues · auto-updates every 5s</p>
          </div>
        </div>
        <button
          onClick={() => resetStaleMut.mutate()}
          disabled={resetStaleMut.isPending}
          className="btn-secondary flex items-center gap-2 text-xs"
          title="Reset items stuck in 'processing' for >2h back to pending"
        >
          {resetStaleMut.isPending ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
          Reset Stuck Items
        </button>
      </div>

      {/* Overall summary row */}
      <div className="grid grid-cols-5 gap-4">
        {[
          { label: 'Total Pending',    value: totals.pending,           icon: Clock,       color: 'text-amber-600',  loading: isLoading },
          { label: 'Processing Now',   value: totals.processing,        icon: Loader2,     color: 'text-blue-600',   loading: isLoading },
          { label: 'Completed',        value: totals.done,              icon: CheckCircle, color: 'text-green-600',  loading: isLoading },
          { label: 'Failed',           value: totals.failed,            icon: XCircle,     color: 'text-red-500',    loading: isLoading },
          { label: 'Live Jobs',        value: jobStats?.live_jobs ?? 0, icon: Briefcase,   color: 'text-[#0e8136]',  loading: !jobStats },
        ].map(({ label, value, icon: Icon, color, loading }) => (
          <div key={label} className="card p-4 flex items-center gap-3">
            <Icon className={`w-5 h-5 flex-shrink-0 ${color}`} />
            <div>
              <div className={`text-xl font-bold ${color}`}>
                {loading ? '…' : value.toLocaleString()}
              </div>
              <div className="text-xs text-gray-500">{label}</div>
            </div>
          </div>
        ))}
      </div>

      {/* Queue cards */}
      <div className="grid grid-cols-2 gap-4">
        {QUEUE_TYPES.map(qt => (
          <QueueCard
            key={qt.key}
            qt={qt}
            stats={queueStats?.[qt.key]}
            onTrigger={() => triggerMuts[qt.runType].mutate()}
            isTriggering={triggerMuts[qt.runType].isPending}
          />
        ))}
      </div>
    </div>
  );
}
