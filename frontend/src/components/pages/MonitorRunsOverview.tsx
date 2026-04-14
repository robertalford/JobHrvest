/**
 * Monitor Runs Overview — one-page summary of all 4 run-type queues
 * with date range filtering and jobs crawled breakdown.
 */
import { useState, useMemo, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getQueueStats, getJobCrawlBreakdown, triggerRun, getLiveTimeline, getQueuePauseState, pauseQueue, resumeQueue } from '../../lib/api';
import { Link } from 'react-router-dom';
import {
  Globe, Building2, Search, Activity, Briefcase,
  Clock, Loader2, CheckCircle, XCircle, Play, Pause, ArrowRight,
  FileX, Ban, Copy, CalendarOff, ShieldAlert, TrendingUp, AlertCircle,
} from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

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
  { label: 'All',   from: () => '2000-01-01T00:00:00.000Z' },
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

function fmtDate(d: Date): string {
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })
    + ' - '
    + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
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

function QueueCard({ qt, stats, onTrigger, isTriggering, isPaused, onPause, onResume, isPauseToggling }: {
  qt: QueueTypeDef;
  stats: Record<string, number> | undefined;
  onTrigger: () => void;
  isTriggering: boolean;
  isPaused: boolean;
  onPause: () => void;
  onResume: () => void;
  isPauseToggling: boolean;
}) {
  const Icon = qt.icon;
  const pending    = stats?.pending    ?? 0;
  const processing = stats?.processing ?? 0;
  const done       = stats?.done       ?? 0;
  const failed     = stats?.failed     ?? 0;
  const total      = pending + processing + done + failed;
  const completedPct = total > 0 ? Math.round(((done + failed) / total) * 100) : 0;
  const failPct      = total > 0 ? Math.round((failed / total) * 100) : 0;
  const failRate     = (done + failed) > 0 ? Math.round((failed / (done + failed)) * 100) : 0;
  const isStalled    = pending > 0 && processing === 0 && !isPaused;

  return (
    <div className={`card p-5 flex flex-col gap-4 ${isPaused ? 'opacity-60 border-dashed' : ''} ${failed > 0 && failRate > 20 ? 'ring-1 ring-red-200' : ''}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0"
            style={{ backgroundColor: qt.iconColor + '18' }}>
            <Icon className="w-4 h-4" style={{ color: qt.iconColor }} />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-semibold text-gray-900">{qt.label}</span>
              {isPaused && (
                <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-bold uppercase bg-amber-100 text-amber-700">
                  <Pause className="w-2.5 h-2.5" /> Paused
                </span>
              )}
              {isStalled && (
                <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-bold uppercase bg-orange-100 text-orange-700 animate-pulse">
                  <AlertCircle className="w-2.5 h-2.5" /> Stalled
                </span>
              )}
              {failRate > 20 && (done + failed) > 10 && (
                <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-bold uppercase bg-red-100 text-red-700">
                  <XCircle className="w-2.5 h-2.5" /> {failRate}% failing
                </span>
              )}
            </div>
            <div className="text-xs text-gray-400">{qt.description}</div>
          </div>
        </div>
        <div className="flex items-center gap-1.5">
          {isPaused ? (
            <button
              onClick={onResume}
              disabled={isPauseToggling}
              className="btn-primary text-xs flex items-center gap-1 px-3 py-1.5"
              title="Start processing"
            >
              {isPauseToggling ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
              Start
            </button>
          ) : (
            <>
              <button
                onClick={onTrigger}
                disabled={isTriggering}
                className="btn-secondary text-xs flex items-center gap-1 px-2 py-1"
                title="Trigger a new run"
              >
                {isTriggering ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
                Run
              </button>
              <button
                onClick={onPause}
                disabled={isPauseToggling}
                className="btn-secondary text-xs flex items-center gap-1 px-2 py-1 text-amber-600 hover:bg-amber-50"
                title="Pause processing"
              >
                {isPauseToggling ? <Loader2 className="w-3 h-3 animate-spin" /> : <Pause className="w-3 h-3" />}
                Pause
              </button>
            </>
          )}
        </div>
      </div>

      <div className="grid grid-cols-4 gap-2 py-3 border-y border-gray-100">
        <StatPill value={pending}    label="Pending"    colorClass="text-amber-600" />
        <StatPill value={processing} label="Processing" colorClass="text-blue-600" />
        <StatPill value={done}       label="Done"       colorClass="text-green-600" />
        <StatPill value={failed}     label="Failed"     colorClass={failed > 0 ? "text-red-500 font-bold" : "text-red-500"} />
      </div>

      <div>
        <div className="flex justify-between text-xs text-gray-400 mb-1">
          <span>{completedPct}% processed</span>
          <span>{total.toLocaleString()} total</span>
        </div>
        <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
          {total > 0 && (
            <div className="h-full flex">
              <div className="h-full bg-green-400 transition-all" style={{ width: `${completedPct - failPct}%` }} />
              {failPct > 0 && (
                <div className="h-full bg-red-400 transition-all" style={{ width: `${failPct}%` }} />
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

interface QualityBreakdownEntry { count: number; pct: number; }

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
    B_good: QualityBreakdownEntry;
    C_fair: QualityBreakdownEntry;
    D_poor: QualityBreakdownEntry;
    total: number;
  };
}

function QualityBreakdownCard({ breakdown }: { breakdown: CrawlBreakdownData['quality_breakdown'] }) {
  const bands = [
    { key: 'A', label: 'A - Excellent',          data: breakdown.A_complete,          color: 'bg-green-500', dot: 'bg-green-500' },
    { key: 'B', label: 'B - Good',  data: breakdown.B_good,  color: 'bg-blue-500',  dot: 'bg-blue-500' },
    { key: 'C', label: 'C - Fair',              data: breakdown.C_fair,              color: 'bg-yellow-500', dot: 'bg-yellow-500' },
    { key: 'D', label: 'D - Minimal',              data: breakdown.D_poor,              color: 'bg-red-500',   dot: 'bg-red-500' },
  ];

  return (
    <div className="flex flex-col gap-3">
      {/* Stacked bar */}
      <div className="h-4 rounded-full overflow-hidden flex bg-gray-100">
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
      {/* Legend row */}
      <div className="flex items-center gap-6 flex-wrap">
        {bands.map(b => (
          <div key={b.key} className="flex items-center gap-1.5 text-sm">
            <span className={`w-2.5 h-2.5 rounded-full ${b.dot} flex-shrink-0`} />
            <span className="text-gray-600">{b.label}</span>
            <span className="font-semibold text-gray-900 ml-1">{b.data.count.toLocaleString()}</span>
            <span className="text-gray-400">({b.data.pct}%)</span>
          </div>
        ))}
      </div>
      <div className="text-xs text-gray-400 text-right">
        {breakdown.total.toLocaleString()} total live jobs
      </div>
    </div>
  );
}

/* ── Main component ──────────────────────────────────────────────── */


function ChartTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-white border border-gray-200 rounded-lg shadow-lg px-3 py-2 text-sm">
      <div className="font-medium text-gray-900">{label}</div>
      <div className="text-green-600">{payload[0]?.value?.toLocaleString()} live jobs</div>
    </div>
  );
}

export function MonitorRunsOverview() {
  const qc = useQueryClient();
  const [chartMinutes, setChartMinutes] = useState(0);

  const CHART_PERIODS = [
    { label: '5m', value: 5 },
    { label: '15m', value: 15 },
    { label: '30m', value: 30 },
    { label: '1h', value: 60 },
    { label: '3h', value: 180 },
    { label: '6h', value: 360 },
    { label: '12h', value: 720 },
    { label: '24h', value: 1440 },
    { label: '3d', value: 4320 },
    { label: '7d', value: 10080 },
    { label: 'All', value: 0 },
  ];

  const { data: timelineData } = useQuery({
    queryKey: ['live-timeline', chartMinutes],
    queryFn: () => getLiveTimeline(chartMinutes),
    refetchInterval: 5000,
  });

  const chartData = useMemo(() => {
    if (!timelineData?.data) return [];
    const bucket = timelineData.bucket || 'minute';
    return timelineData.data.map((d: { minute: string; live: number; total: number }) => {
      const dt = new Date(d.minute);
      let time: string;
      if (bucket === 'day') {
        time = dt.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
      } else if (bucket === 'hour') {
        time = dt.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' })
          + ' ' + dt.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
      } else {
        time = dt.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
      }
      return { time, live: d.live, total: d.total };
    });
  }, [timelineData]);


  const [range, setRange] = useState<{ from: string; to: string }>(() => {
    return { from: '2000-01-01T00:00:00.000Z', to: new Date().toISOString() };
  });
  const [activeQuick, setActiveQuick] = useState<string>('All');

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

  useEffect(() => {
    if (dataUpdatedAt) {
      setCountdown(REFRESH_INTERVAL);
      setLastUpdated(new Date());
    }
  }, [dataUpdatedAt]);

  useEffect(() => {
    const timer = setInterval(() => {
      setCountdown(prev => (prev <= 1 ? REFRESH_INTERVAL : prev - 1));
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  // Pause state
  const { data: pauseState } = useQuery<Record<string, boolean>>({
    queryKey: ['queue-pause-state'],
    queryFn: () => getQueuePauseState().catch(() => ({})),
    refetchInterval: 5000,
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

  const pauseMuts = Object.fromEntries(
    QUEUE_TYPES.map(qt => [
      qt.runType,
      // eslint-disable-next-line react-hooks/rules-of-hooks
      useMutation({
        mutationFn: () => pauseQueue(qt.runType),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['queue-pause-state'] }),
      }),
    ])
  );

  const resumeMuts = Object.fromEntries(
    QUEUE_TYPES.map(qt => [
      qt.runType,
      // eslint-disable-next-line react-hooks/rules-of-hooks
      useMutation({
        mutationFn: () => resumeQueue(qt.runType),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['queue-pause-state'] }),
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

  // Computed crawl stats
  const totalExtracted = crawlBreakdown?.total_extracted ?? 0;
  const totalFailed = (crawlBreakdown?.failed_core_fields ?? 0)
    + (crawlBreakdown?.failed_bad_words ?? 0)
    + (crawlBreakdown?.failed_duplicates ?? 0)
    + (crawlBreakdown?.failed_expired ?? 0)
    + (crawlBreakdown?.failed_scam ?? 0);
  const totalSucceeded = totalExtracted - totalFailed;
  const succeededPct = totalExtracted > 0 ? Math.round((totalSucceeded / totalExtracted) * 100) : 0;
  const failedPct = totalExtracted > 0 ? Math.round((totalFailed / totalExtracted) * 100) : 0;

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
          </div>
        </div>
        {/* Last updated + countdown — top right */}
        <div className="text-right">
          <div className="text-sm font-medium text-gray-700">
            Last updated: {fmtDate(lastUpdated)}
          </div>
          <div className="text-sm text-gray-500">
            Next update in <span className="font-mono font-semibold text-gray-700">{countdown}s</span>
          </div>
        </div>
      </div>

      {/* Date range selector */}
      <div className="card p-4">
        <div className="flex flex-wrap items-center gap-3">
          <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">Range:</span>
          <div className="flex gap-1">
            {QUICK_RANGES.map(qr => (
              <button
                key={qr.label}
                onClick={() => selectQuickRange(qr.label, qr.from)}
                className={`w-14 py-1 text-xs rounded-md font-medium text-center transition-colors ${
                  activeQuick === qr.label
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {qr.label}
              </button>
            ))}
            {/* Custom button */}
            <button
              onClick={() => setActiveQuick('Custom')}
              className={`w-14 py-1 text-xs rounded-md font-medium text-center transition-colors ${
                activeQuick === 'Custom'
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              Custom
            </button>
          </div>
          {/* Custom date pickers — only visible when Custom is selected */}
          {activeQuick === 'Custom' && (
            <>
              <div className="h-5 w-px bg-gray-200 mx-1" />
              <div className="flex items-center gap-2 text-xs">
                <label className="text-gray-500">From</label>
                <input
                  type="datetime-local"
                  className="border border-gray-200 rounded px-2 py-1 text-xs"
                  value={toLocalInput(range.from)}
                  onChange={e => setRange(r => ({ ...r, from: fromLocalInput(e.target.value) }))}
                />
                <label className="text-gray-500">To</label>
                <input
                  type="datetime-local"
                  className="border border-gray-200 rounded px-2 py-1 text-xs"
                  value={toLocalInput(range.to)}
                  onChange={e => setRange(r => ({ ...r, to: fromLocalInput(e.target.value) }))}
                />
              </div>
            </>
          )}
        </div>
      </div>

      {/* Pipeline Runs section header */}
      <div className="flex items-center gap-3 px-4 py-3 bg-gray-50 rounded-xl">
        <Activity className="w-5 h-5 text-gray-600" />
        <h2 className="text-lg font-semibold text-gray-900">Pipeline Runs</h2>
        <span className="text-xs text-gray-400 ml-2">Queue processing status for selected date range</span>
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
            isPaused={!!pauseState?.[qt.key]}
            onPause={() => pauseMuts[qt.runType].mutate()}
            onResume={() => resumeMuts[qt.runType].mutate()}
            isPauseToggling={pauseMuts[qt.runType].isPending || resumeMuts[qt.runType].isPending}
          />
        ))}
      </div>

      {/* ── Jobs Crawled section ───────────────────────────────────── */}
      <div className="space-y-4">
        <div className="flex items-center gap-3 px-4 py-3 bg-gray-50 rounded-xl">
          <Briefcase className="w-5 h-5 text-gray-600" />
          <h2 className="text-lg font-semibold text-gray-900">Jobs Crawled</h2>
          <span className="text-xs text-gray-400 ml-2">Extraction results for selected date range</span>
        </div>

        {/* Live Jobs Per Minute Chart */}
        <div className="card p-5">
          {/* Period selector */}
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <TrendingUp className="w-4 h-4 text-green-600" />
              <span className="text-sm font-medium text-gray-700">
                Live Jobs Per {timelineData?.bucket === 'day' ? 'Day' : timelineData?.bucket === 'hour' ? 'Hour' : 'Minute'}
              </span>
            </div>
            <div className="flex gap-1">
              {CHART_PERIODS.map(p => (
                <button
                  key={p.value}
                  onClick={() => setChartMinutes(p.value)}
                  className={`w-10 py-1 text-xs rounded-md font-medium text-center transition-colors ${
                    chartMinutes === p.value
                      ? 'bg-green-600 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  {p.label}
                </button>
              ))}
            </div>
          </div>
          {/* Chart */}
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={chartData} margin={{ top: 5, right: 20, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="time"
                tick={{ fontSize: 11, fill: '#9ca3af' }}
                interval={Math.max(0, Math.floor(chartData.length / 10) - 1)}
              />
              <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} />
              <Tooltip content={<ChartTooltip />} />
              <Line
                type="monotone"
                dataKey="live"
                stroke="#16a34a"
                strokeWidth={2}
                dot={{ r: 3, fill: '#16a34a', strokeWidth: 0 }}
                activeDot={{ r: 5, fill: '#16a34a', stroke: '#fff', strokeWidth: 2 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>



        {crawlLoading || !crawlBreakdown ? (
          <div className="flex items-center justify-center py-12 text-gray-400">
            <Loader2 className="w-5 h-5 animate-spin mr-2" />
            Loading job breakdown...
          </div>
        ) : (
          <>
            {/* Row 1: Succeeded (25%) + Quality Breakdown (75%) */}
            <div className="grid gap-4" style={{ gridTemplateColumns: '1fr 3fr' }}>
              {/* Succeeded summary */}
              <div className="card p-5 flex flex-col justify-center">
                <div className="flex items-center gap-2 mb-3">
                  <CheckCircle className="w-5 h-5 text-green-600" />
                  <span className="text-sm font-medium text-gray-600">Succeeded</span>
                </div>
                <div className="text-3xl font-bold text-green-600">
                  {totalSucceeded.toLocaleString()}
                </div>
                <div className="text-sm text-gray-500 mt-1">
                  / {totalExtracted.toLocaleString()} ({succeededPct}%) succeeded
                </div>
              </div>

              {/* Quality Breakdown */}
              <div className="card p-5">
                <div className="flex items-center gap-2 mb-3">
                  <Activity className="w-5 h-5 text-indigo-600" />
                  <span className="text-sm font-medium text-gray-600">Quality Breakdown of Live Jobs</span>
                </div>
                <QualityBreakdownCard breakdown={crawlBreakdown.quality_breakdown} />
              </div>
            </div>

            {/* Row 2: Failed total (25%) + failure reasons (75%) */}
            <div className="grid gap-4" style={{ gridTemplateColumns: '1fr 3fr' }}>
              {/* Failed summary */}
              <div className="card p-5 flex flex-col justify-center">
                <div className="flex items-center gap-2 mb-3">
                  <XCircle className="w-5 h-5 text-red-500" />
                  <span className="text-sm font-medium text-gray-600">Failed</span>
                </div>
                <div className="text-3xl font-bold text-red-500">
                  {totalFailed.toLocaleString()}
                </div>
                <div className="text-sm text-gray-500 mt-1">
                  / {totalExtracted.toLocaleString()} ({failedPct}%) failed
                </div>
              </div>

              {/* Failure reason breakdown */}
              <div className="grid grid-cols-5 gap-3">
                <div className="card p-4 flex flex-col">
                  <div className="flex items-center gap-1.5 mb-2">
                    <FileX className="w-4 h-4 text-orange-500" />
                    <span className="text-xs text-gray-500">Core Fields</span>
                  </div>
                  <div className="text-xl font-bold text-orange-600">
                    {crawlBreakdown.failed_core_fields.toLocaleString()}
                  </div>
                </div>

                <div className="card p-4 flex flex-col">
                  <div className="flex items-center gap-1.5 mb-2">
                    <Ban className="w-4 h-4 text-red-500" />
                    <span className="text-xs text-gray-500">Bad Words</span>
                  </div>
                  <div className="text-xl font-bold text-red-500">
                    {crawlBreakdown.failed_bad_words.toLocaleString()}
                  </div>
                </div>

                <div className="card p-4 flex flex-col">
                  <div className="flex items-center gap-1.5 mb-2">
                    <Copy className="w-4 h-4 text-purple-500" />
                    <span className="text-xs text-gray-500">Duplicates</span>
                  </div>
                  <div className="text-xl font-bold text-purple-600">
                    {crawlBreakdown.failed_duplicates.toLocaleString()}
                  </div>
                </div>

                <div className="card p-4 flex flex-col">
                  <div className="flex items-center gap-1.5 mb-2">
                    <CalendarOff className="w-4 h-4 text-gray-500" />
                    <span className="text-xs text-gray-500">Expired</span>
                  </div>
                  <div className="text-xl font-bold text-gray-600">
                    {crawlBreakdown.failed_expired.toLocaleString()}
                  </div>
                </div>

                <div className="card p-4 flex flex-col">
                  <div className="flex items-center gap-1.5 mb-2">
                    <ShieldAlert className="w-4 h-4 text-red-600" />
                    <span className="text-xs text-gray-500">Scam</span>
                  </div>
                  <div className="text-xl font-bold text-red-600">
                    {crawlBreakdown.failed_scam.toLocaleString()}
                  </div>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
