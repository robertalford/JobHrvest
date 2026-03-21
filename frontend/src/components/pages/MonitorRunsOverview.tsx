/**
 * Monitor Runs Overview — one-page summary of all 4 run-type queues
 * with date range filtering and jobs crawled breakdown.
 */
import { useState, useMemo, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getQueueStats, getJobCrawlBreakdown, triggerRun, resetStaleQueueItems } from '../../lib/api';
import { Link } from 'react-router-dom';
import {
  Globe, Building2, Search, Activity, Briefcase,
  Clock, Loader2, CheckCircle, XCircle, Play, ArrowRight, RefreshCw,
  FileX, Ban, Copy, CalendarOff, ShieldAlert, Zap,
} from 'lucide-react';

/* ── Date range helpers ──────────────────────────────────────────── */

function hoursAgo(h: number): string {
  return new Date(Date.now() - h * 3600_000).toISOString();
}

function todayStart(): string {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  return d.toISOString();
}

const QUICK_RANGES: { label: string; from: () => string }[] = [
  { label: '1h',    from: () => hoursAgo(1) },
  { label: '2h',    from: () => hoursAgo(2) },
  { label: '6h',    from: () => hoursAgo(6) },
  { label: '12h',   from: () => hoursAgo(12) },
  { label: '24h',   from: () => hoursAgo(24) },
  { label: '3d',    from: () => hoursAgo(72) },
  { label: '7d',    from: () => hoursAgo(168) },
  { label: 'Today', from: () => todayStart() },
];

function toLocalInput(iso: string): string {
  const d = new Date(iso);
  const off = d.getTimezoneOffset();
  const local = new Date(d.getTime() - off * 60000);
  return local.toISOString().slice(0, 16);
}

function fromLocalInput(val: string): string {
  return new Date(val).toISOString();
}

/* ── Queue type definitions ──────────────────────────────────────── */

interface QueueTypeDef {
  key: string;
  runType: string;
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

/* ── Sub-components ──────────────────────────────────────────────── */

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
  const completedPct = total > 0 ? Math.round(((done + failed) / total) * 100) : 0;
  const failPct      = total > 0 ? Math.round((failed / total) * 100) : 0;

  return (
    <div className="card p-5 flex flex-col gap-4">
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

      <div className="grid grid-cols-4 gap-2 py-3 border-y border-gray-100">
        <StatPill value={pending}    label="Pending"    colorClass="text-amber-600" />
        <StatPill value={processing} label="Processing" colorClass="text-blue-600" />
        <StatPill value={done}       label="Done"       colorClass="text-green-600" />
        <StatPill value={failed}     label="Failed"     colorClass="text-red-500" />
      </div>

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

      <Link to={qt.href} className="flex items-center gap-1 text-xs text-blue-600 hover:underline self-end">
        View logs <ArrowRight className="w-3 h-3" />
      </Link>
    </div>
  );
}

interface QualityBreakdownEntry {
  count: number;
  pct: number;
}

interface CrawlBreakdownData {
  total_extracted: number;
  failed_core_fields: number;
  failed_bad_words: number;
  failed_duplicates: number;
  failed_expired: number;
  failed_scam: number;
  live_jobs: number;
  quality_breakdown: {
    A_complete: QualityBreakdownEntry;
    B_missing_location: QualityBreakdownEntry;
    C_fair: QualityBreakdownEntry;
    D_poor: QualityBreakdownEntry;
    total: number;
  };
}

function QualityBar({ breakdown }: { breakdown: CrawlBreakdownData['quality_breakdown'] }) {
  const bands = [
    { key: 'A', label: 'A - Complete', data: breakdown.A_complete, color: 'bg-green-500' },
    { key: 'B', label: 'B - Missing Loc', data: breakdown.B_missing_location, color: 'bg-blue-500' },
    { key: 'C', label: 'C - Fair', data: breakdown.C_fair, color: 'bg-yellow-500' },
    { key: 'D', label: 'D - Poor', data: breakdown.D_poor, color: 'bg-red-500' },
  ];

  return (
    <div className="space-y-2">
      <div className="h-3 rounded-full overflow-hidden flex bg-gray-100">
        {bands.map(b => (
          b.data.pct > 0 ? (
            <div
              key={b.key}
              className={`h-full ${b.color} transition-all`}
              style={{ width: `${b.data.pct}%` }}
              title={`${b.label}: ${b.data.count.toLocaleString()} (${b.data.pct}%)`}
            />
          ) : null
        ))}
      </div>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1">
        {bands.map(b => (
          <div key={b.key} className="flex items-center gap-2 text-xs">
            <span className={`w-2 h-2 rounded-full ${b.color} flex-shrink-0`} />
            <span className="text-gray-600">{b.label}</span>
            <span className="ml-auto font-medium text-gray-900">{b.data.count.toLocaleString()}</span>
            <span className="text-gray-400">({b.data.pct}%)</span>
          </div>
        ))}
      </div>
      <div className="text-xs text-gray-400 text-right">
        {breakdown.total.toLocaleString()} total live
      </div>
    </div>
  );
}

/* ── Main component ──────────────────────────────────────────────── */

export function MonitorRunsOverview() {
  const qc = useQueryClient();

  // Date range state — default 24h
  const [range, setRange] = useState<{ from: string; to: string }>(() => {
    const now = new Date();
    const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    return { from: yesterday.toISOString(), to: now.toISOString() };
  });
  const [activeQuick, setActiveQuick] = useState<string>('24h');

  const rangeParams = useMemo(() => ({
    from_dt: range.from,
    to_dt: range.to,
  }), [range]);

  // Queue stats (filtered by date range)
  const { data: queueStats, isLoading, dataUpdatedAt } = useQuery<Record<string, Record<string, number>>>({
    queryKey: ['queue-stats', rangeParams],
    queryFn: () => getQueueStats(rangeParams),
    refetchInterval: 5000,
  });

  // Job crawl breakdown (filtered by date range)
  const { data: crawlBreakdown, isLoading: crawlLoading } = useQuery<CrawlBreakdownData>({
    queryKey: ['job-crawl-breakdown', rangeParams],
    queryFn: () => getJobCrawlBreakdown(rangeParams),
    refetchInterval: 5000,
  });

  // Auto-refresh countdown
  const REFRESH_INTERVAL = 5;
  const [countdown, setCountdown] = useState(REFRESH_INTERVAL);
  const [lastUpdated, setLastUpdated] = useState<Date>(new Date());

  // Reset countdown when data refreshes
  useEffect(() => {
    if (dataUpdatedAt) {
      setCountdown(REFRESH_INTERVAL);
      setLastUpdated(new Date());
    }
  }, [dataUpdatedAt]);

  // Tick countdown every second
  useEffect(() => {
    const timer = setInterval(() => {
      setCountdown(prev => (prev <= 1 ? REFRESH_INTERVAL : prev - 1));
    }, 1000);
    return () => clearInterval(timer);
  }, []);

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

  function selectQuickRange(label: string, fromFn: () => string) {
    setActiveQuick(label);
    setRange({ from: fromFn(), to: new Date().toISOString() });
  }

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
            <p className="text-sm text-gray-500">Live overview of all pipeline queues</p>
            <div className="flex items-center gap-4 mt-0.5">
              <span className="text-xs text-gray-400">
                Last updated: {lastUpdated.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })} - {lastUpdated.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
              </span>
              <span className="text-xs text-gray-300">·</span>
              <span className="text-xs text-gray-400">
                Next update in <span className="font-mono font-medium text-gray-500">{countdown}s</span>
              </span>
            </div>
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

      {/* Date range selector */}
      <div className="card p-4">
        <div className="flex flex-wrap items-center gap-3">
          <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">Range:</span>
          <div className="flex flex-wrap gap-1">
            {QUICK_RANGES.map(qr => (
              <button
                key={qr.label}
                onClick={() => selectQuickRange(qr.label, qr.from)}
                className={`px-2.5 py-1 text-xs rounded-md font-medium transition-colors ${
                  activeQuick === qr.label
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {qr.label}
              </button>
            ))}
          </div>
          <div className="h-5 w-px bg-gray-200 mx-1" />
          <div className="flex items-center gap-2 text-xs">
            <label className="text-gray-500">From</label>
            <input
              type="datetime-local"
              className="border border-gray-200 rounded px-2 py-1 text-xs"
              value={toLocalInput(range.from)}
              onChange={e => {
                setActiveQuick('');
                setRange(r => ({ ...r, from: fromLocalInput(e.target.value) }));
              }}
            />
            <label className="text-gray-500">To</label>
            <input
              type="datetime-local"
              className="border border-gray-200 rounded px-2 py-1 text-xs"
              value={toLocalInput(range.to)}
              onChange={e => {
                setActiveQuick('');
                setRange(r => ({ ...r, to: fromLocalInput(e.target.value) }));
              }}
            />
          </div>
        </div>
      </div>

      {/* Overall summary row — 4 cards */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: 'Total Pending',  value: totals.pending,    icon: Clock,       color: 'text-amber-600' },
          { label: 'Processing Now', value: totals.processing, icon: Loader2,     color: 'text-blue-600' },
          { label: 'Completed',      value: totals.done,       icon: CheckCircle, color: 'text-green-600' },
          { label: 'Failed',         value: totals.failed,     icon: XCircle,     color: 'text-red-500' },
        ].map(({ label, value, icon: Icon, color }) => (
          <div key={label} className="card p-4 flex items-center gap-3">
            <Icon className={`w-5 h-5 flex-shrink-0 ${color}`} />
            <div>
              <div className={`text-xl font-bold ${color}`}>
                {isLoading ? '...' : value.toLocaleString()}
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

      {/* Jobs Crawled section */}
      <div className="space-y-4">
        <div className="flex items-center gap-3 px-4 py-3 bg-gray-50 rounded-xl">
          <Briefcase className="w-5 h-5 text-gray-600" />
          <h2 className="text-lg font-semibold text-gray-900">Jobs Crawled</h2>
          <span className="text-xs text-gray-400 ml-2">Extraction results for selected date range</span>
        </div>

        {crawlLoading || !crawlBreakdown ? (
          <div className="flex items-center justify-center py-12 text-gray-400">
            <Loader2 className="w-5 h-5 animate-spin mr-2" />
            Loading job breakdown...
          </div>
        ) : (
          <>
            {/* Row 1: 4 stat cards */}
            <div className="grid grid-cols-4 gap-4">
              <div className="card p-4">
                <div className="flex items-center gap-2 mb-2">
                  <Zap className="w-4 h-4 text-blue-600" />
                  <span className="text-xs text-gray-500">Total Extracted</span>
                </div>
                <div className="text-2xl font-bold text-gray-900">
                  {crawlBreakdown.total_extracted.toLocaleString()}
                </div>
              </div>

              <div className="card p-4">
                <div className="flex items-center gap-2 mb-2">
                  <FileX className="w-4 h-4 text-orange-500" />
                  <span className="text-xs text-gray-500">Failed (Core Fields)</span>
                </div>
                <div className="text-2xl font-bold text-orange-600">
                  {crawlBreakdown.failed_core_fields.toLocaleString()}
                </div>
              </div>

              <div className="card p-4">
                <div className="flex items-center gap-2 mb-2">
                  <Ban className="w-4 h-4 text-red-500" />
                  <span className="text-xs text-gray-500">Failed (Bad Words)</span>
                </div>
                <div className="text-2xl font-bold text-red-500">
                  {crawlBreakdown.failed_bad_words.toLocaleString()}
                </div>
              </div>

              <div className="card p-4">
                <div className="flex items-center gap-2 mb-2">
                  <Copy className="w-4 h-4 text-purple-500" />
                  <span className="text-xs text-gray-500">Failed (Duplicates)</span>
                </div>
                <div className="text-2xl font-bold text-purple-600">
                  {crawlBreakdown.failed_duplicates.toLocaleString()}
                </div>
              </div>
            </div>

            {/* Row 2: 4 stat cards */}
            <div className="grid grid-cols-4 gap-4">
              <div className="card p-4">
                <div className="flex items-center gap-2 mb-2">
                  <CalendarOff className="w-4 h-4 text-gray-500" />
                  <span className="text-xs text-gray-500">Failed (Expired)</span>
                </div>
                <div className="text-2xl font-bold text-gray-600">
                  {crawlBreakdown.failed_expired.toLocaleString()}
                </div>
              </div>

              <div className="card p-4">
                <div className="flex items-center gap-2 mb-2">
                  <ShieldAlert className="w-4 h-4 text-red-600" />
                  <span className="text-xs text-gray-500">Failed (Scam)</span>
                </div>
                <div className="text-2xl font-bold text-red-600">
                  {crawlBreakdown.failed_scam.toLocaleString()}
                </div>
              </div>

              <div className="card p-4">
                <div className="flex items-center gap-2 mb-2">
                  <CheckCircle className="w-4 h-4 text-green-600" />
                  <span className="text-xs text-gray-500">Live Jobs</span>
                </div>
                <div className="text-2xl font-bold text-green-600">
                  {crawlBreakdown.live_jobs.toLocaleString()}
                </div>
              </div>

              <div className="card p-4">
                <div className="flex items-center gap-2 mb-2">
                  <Activity className="w-4 h-4 text-indigo-600" />
                  <span className="text-xs text-gray-500">Quality Breakdown</span>
                </div>
                <QualityBar breakdown={crawlBreakdown.quality_breakdown} />
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
