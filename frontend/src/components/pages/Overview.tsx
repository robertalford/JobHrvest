import { useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getOverview, getSystemHealth, triggerQualityScoring } from '../../lib/api';
import { StatCard } from '../ui/StatCard';
import { CheckCircle, XCircle } from 'lucide-react';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  Cell, PieChart, Pie, BarChart, Bar,
} from 'recharts';

const BAND_COLORS: Record<string, string> = {
  excellent: '#0e8136',
  good: '#22c55e',
  fair: '#eab308',
  poor: '#f97316',
  disqualified: '#ef4444',
};

export function Overview() {
  const qc = useQueryClient();

  // Single request returns all dashboard data — cached 15s server-side
  const { data, isLoading } = useQuery({
    queryKey: ['overview'],
    queryFn: getOverview,
    refetchInterval: 30000,
  });

  // System health is lightweight — keep its own fast poll
  const { data: health } = useQuery({
    queryKey: ['health'],
    queryFn: getSystemHealth,
    refetchInterval: 30000,
  });

  const scoreMutation = useMutation({
    mutationFn: triggerQualityScoring,
    onSuccess: () => setTimeout(() => qc.invalidateQueries({ queryKey: ['overview'] }), 3000),
  });

  const stats = data?.job_stats;
  const markets: { market: string; jobs: number; companies: number; avg_quality: number | null }[] = data?.markets ?? [];
  const coverage = data?.coverage;
  const quality = data?.quality;
  const trends = data?.trends ?? [];
  const qualitySites = data?.quality_sites ?? [];
  const totalJobs = markets.reduce((s, m) => s + m.jobs, 0);

  // Fill all 30 days so the chart always has a full x-axis (missing days → 0)
  const trendData = useMemo(() => {
    const map = new Map(trends.map((t: { day: string; count: number }) => [t.day, t.count]));
    return Array.from({ length: 30 }, (_, i) => {
      const d = new Date();
      d.setDate(d.getDate() - (29 - i));
      const key = d.toISOString().slice(0, 10);
      return { day: key.slice(5), count: map.get(key) || 0 };
    });
  }, [trends]);

  const bandData = quality?.bands
    ? Object.entries(quality.bands).map(([band, d]: [string, any]) => ({
        name: band.charAt(0).toUpperCase() + band.slice(1),
        count: d.count,
        pct: d.pct,
        color: BAND_COLORS[band],
      }))
    : [];
  const pieData = bandData.filter(d => d.count > 0);
  const topSites = qualitySites.slice(0, 10);
  const bottomSites = qualitySites.slice(-10).reverse();

  const fieldGroups = coverage ? [
    {
      heading: 'Core data',
      fields: [
        { label: 'Company Name', pct: coverage.company_name_pct },
        { label: 'Role Title', pct: coverage.title_pct },
        { label: 'Description', pct: coverage.description_pct },
        { label: 'Location', pct: coverage.location_pct },
      ],
    },
    {
      heading: 'High quality attributes',
      fields: [
        { label: 'Employment Type', pct: coverage.employment_type_pct },
        { label: 'Salary', pct: coverage.salary_pct },
      ],
    },
    {
      heading: 'Very high quality attributes',
      fields: [
        { label: 'Seniority', pct: coverage.seniority_pct },
        { label: 'Requirements', pct: coverage.requirements_pct },
        { label: 'Benefits', pct: coverage.benefits_pct },
      ],
    },
  ] : [];

  if (isLoading) {
    return (
      <div className="p-6 space-y-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Overview</h1>
          <p className="text-sm text-gray-500 mt-1">JobHarvest — global job market intelligence</p>
        </div>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="card p-5 animate-pulse h-20 bg-gray-50" />
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Overview</h1>
        <p className="text-sm text-gray-500 mt-1">JobHarvest — global job market intelligence</p>
      </div>

      {/* System Health — top */}
      <div className="card p-5">
        <h2 className="text-base font-semibold text-gray-900 mb-4">System Health</h2>
        <div className="flex flex-wrap gap-8">
          {health?.services && Object.entries(health.services).map(([svc, status]) => (
            <div key={svc} className="flex items-center gap-2">
              {status === 'ok'
                ? <CheckCircle className="w-4 h-4 text-green-500" />
                : <XCircle className="w-4 h-4 text-red-500" />}
              <div>
                <div className="text-xs font-medium text-gray-700 capitalize">{svc}</div>
                <div className={`text-xs ${status === 'ok' ? 'text-green-600' : 'text-red-600'}`}>{String(status)}</div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Unique Active Jobs" value={stats?.unique_active ?? stats?.active ?? '—'} />
        <StatCard label="New Today" value={stats?.new_today ?? '—'} />
        <StatCard label="New This Week" value={stats?.new_this_week ?? '—'} />
        <StatCard label="Duplicates Suppressed" value={stats?.duplicates ?? '—'} />
      </div>

      {/* Market breakdown */}
      {markets.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="card p-5">
            <h2 className="font-semibold text-gray-900 mb-4">Jobs by Market</h2>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={markets} layout="vertical" margin={{ left: 8, right: 16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 11 }} />
                <YAxis dataKey="market" type="category" tick={{ fontSize: 12, fontWeight: 600 }} width={36} />
                <Tooltip formatter={(v: unknown) => typeof v === 'number' ? v.toLocaleString() : String(v)} />
                <Bar dataKey="jobs" fill="#0e8136" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="card p-5">
            <h2 className="font-semibold text-gray-900 mb-3">Market Summary</h2>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100">
                    <th className="text-left py-2 text-xs font-semibold text-gray-500">Market</th>
                    <th className="text-right py-2 text-xs font-semibold text-gray-500">Jobs</th>
                    <th className="text-right py-2 text-xs font-semibold text-gray-500">Share</th>
                    <th className="text-right py-2 text-xs font-semibold text-gray-500">Cos</th>
                    <th className="text-right py-2 text-xs font-semibold text-gray-500">Avg Q</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {markets.map(m => (
                    <tr key={m.market}>
                      <td className="py-2 font-mono font-semibold text-gray-800">{m.market}</td>
                      <td className="py-2 text-right text-gray-700">{m.jobs.toLocaleString()}</td>
                      <td className="py-2 text-right text-gray-500">{totalJobs > 0 ? ((m.jobs / totalJobs) * 100).toFixed(1) : 0}%</td>
                      <td className="py-2 text-right text-gray-500">{m.companies.toLocaleString()}</td>
                      <td className="py-2 text-right">
                        {m.avg_quality != null ? (
                          <span className={`font-semibold ${m.avg_quality >= 60 ? 'text-green-600' : m.avg_quality >= 40 ? 'text-amber-500' : 'text-red-500'}`}>
                            {m.avg_quality}
                          </span>
                        ) : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Jobs over time */}
      <div className="card p-5">
        <h2 className="font-semibold text-gray-900 mb-4">Jobs Added (last 30 days)</h2>
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={trendData} margin={{ left: 0, right: 8, top: 4, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" vertical={false} />
            <XAxis dataKey="day" tick={{ fontSize: 10 }} interval={4} axisLine={false} tickLine={false} />
            <YAxis tick={{ fontSize: 11 }} width={42} axisLine={false} tickLine={false} />
            <Tooltip formatter={(v) => [Number(v).toLocaleString(), 'Jobs added']} contentStyle={{ fontSize: 12, borderRadius: 8 }} />
            <Area type="monotone" dataKey="count" stroke="#0e8136" fill="#0e813618" strokeWidth={2} dot={false} activeDot={{ r: 4 }} />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Quality distribution + flags */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="card p-5">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="font-semibold text-gray-900">Job Quality Distribution</h2>
              {quality && (
                <p className="text-xs text-gray-400 mt-0.5">
                  {quality.total_scored?.toLocaleString()} scored / {quality.total_unscored?.toLocaleString()} pending
                  {quality.average_score > 0 && ` · avg ${quality.average_score}`}
                </p>
              )}
            </div>
            <button
              onClick={() => scoreMutation.mutate()}
              disabled={scoreMutation.isPending}
              className="text-xs px-2 py-1 bg-brand/10 text-brand rounded hover:bg-brand/20"
            >
              {scoreMutation.isPending ? 'Scoring...' : 'Score Jobs'}
            </button>
          </div>
          {pieData.length > 0 ? (
            <div className="flex items-center gap-4">
              <ResponsiveContainer width={160} height={160}>
                <PieChart>
                  <Pie data={pieData} dataKey="count" nameKey="name" cx="50%" cy="50%" outerRadius={70}>
                    {pieData.map((entry, i) => <Cell key={i} fill={entry.color} />)}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <div className="flex-1 space-y-2">
                {bandData.map(({ name, count, pct, color }) => (
                  <div key={name} className="flex items-center justify-between text-sm">
                    <div className="flex items-center gap-2">
                      <span className="w-2.5 h-2.5 rounded-full inline-block" style={{ backgroundColor: color }} />
                      <span className="text-gray-700">{name}</span>
                    </div>
                    <span className="text-gray-500">{count.toLocaleString()} ({pct}%)</span>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <p className="text-sm text-gray-400">No scored jobs yet. Click "Score Jobs" to begin.</p>
          )}
        </div>

        <div className="card p-5">
          <h2 className="font-semibold text-gray-900 mb-4">Quality Issues Detected</h2>
          {quality?.flags ? (
            <div className="space-y-4">
              {[
                { key: 'scam_detected', label: 'Scam patterns', description: 'Jobs with wire transfer, MLM, or payment requests', color: '#ef4444' },
                { key: 'discrimination_detected', label: 'Discrimination language', description: 'Age, gender, ethnicity, religion preferences', color: '#f97316' },
                { key: 'bad_words_detected', label: 'Inappropriate language', description: 'Profanity or obscene content', color: '#eab308' },
              ].map(({ key, label, description, color }) => {
                const count = quality.flags[key] || 0;
                const pct = quality.total_scored ? ((count / quality.total_scored) * 100).toFixed(1) : '0.0';
                return (
                  <div key={key} className="flex items-start gap-3">
                    <div className="w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: color + '20' }}>
                      <span className="text-xs font-bold" style={{ color }}>{count > 0 ? '!' : '✓'}</span>
                    </div>
                    <div className="flex-1">
                      <div className="flex justify-between">
                        <span className="text-sm font-medium text-gray-800">{label}</span>
                        <span className="text-sm font-medium" style={{ color: count > 0 ? color : '#6b7280' }}>
                          {count.toLocaleString()} ({pct}%)
                        </span>
                      </div>
                      <p className="text-xs text-gray-400 mt-0.5">{description}</p>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <p className="text-sm text-gray-400">Score jobs to see issue breakdown.</p>
          )}
        </div>
      </div>

      {/* Top/Bottom sites by quality */}
      {qualitySites.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="card p-5">
            <h2 className="font-semibold text-gray-900 mb-4">Top Quality Sites</h2>
            <div className="space-y-2">
              {topSites.map((site: any) => (
                <div key={site.id} className="flex items-center justify-between text-sm">
                  <span className="text-gray-700 truncate max-w-[200px]">{site.name}</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-400">{site.job_count} jobs</span>
                    <span className="px-2 py-0.5 rounded-full text-xs font-medium text-white"
                      style={{ backgroundColor: site.quality_score >= 80 ? '#0e8136' : site.quality_score >= 60 ? '#22c55e' : site.quality_score >= 40 ? '#eab308' : '#ef4444' }}>
                      {site.quality_score}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div className="card p-5">
            <h2 className="font-semibold text-gray-900 mb-4">Lowest Quality Sites</h2>
            <div className="space-y-2">
              {bottomSites.map((site: any) => (
                <div key={site.id} className="flex items-center justify-between text-sm">
                  <span className="text-gray-700 truncate max-w-[200px]">{site.name}</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-400">{site.job_count} jobs</span>
                    <span className="px-2 py-0.5 rounded-full text-xs font-medium text-white"
                      style={{ backgroundColor: site.quality_score >= 80 ? '#0e8136' : site.quality_score >= 60 ? '#22c55e' : site.quality_score >= 40 ? '#eab308' : '#ef4444' }}>
                      {site.quality_score}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Field coverage */}
      {coverage && (
        <div className="card p-5">
          <h2 className="font-semibold text-gray-900 mb-1">Field Coverage</h2>
          <p className="text-xs text-gray-400 mb-4">% of active jobs with each field populated</p>
          <div className="space-y-6">
            {fieldGroups.map(({ heading, fields }) => (
              <div key={heading}>
                <div className="text-[10px] font-bold uppercase tracking-widest text-gray-400 mb-3">{heading}</div>
                <div className="space-y-3">
                  {fields.map(({ label, pct }) => (
                    <div key={label}>
                      <div className="flex justify-between text-sm mb-1">
                        <span className="text-gray-700">{label}</span>
                        <span className="font-medium text-gray-900">{pct?.toFixed(1) ?? 0}%</span>
                      </div>
                      <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                        <div className="h-full rounded-full"
                          style={{ width: `${pct ?? 0}%`, backgroundColor: pct >= 90 ? '#0e8136' : pct >= 70 ? '#eab308' : '#ef4444' }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
