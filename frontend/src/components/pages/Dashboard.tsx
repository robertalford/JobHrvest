import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getOverview, getSystemHealth, triggerQualityScoring } from '../../lib/api';
import { StatCard } from '../ui/StatCard';
import { CheckCircle, XCircle } from 'lucide-react';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  BarChart, Bar, PieChart, Pie, Cell,
} from 'recharts';

const BAND_COLORS = ['#16a34a', '#22c55e', '#eab308', '#f97316', '#ef4444'];

function ScoreBadge({ score }: { score: number | null }) {
  if (score == null) return <span className="text-xs text-gray-400">—</span>;
  const bg = score >= 80 ? 'bg-green-100 text-green-800'
    : score >= 60 ? 'bg-green-100 text-green-700'
    : score >= 40 ? 'bg-yellow-100 text-yellow-800'
    : 'bg-red-100 text-red-800';
  return (
    <span className={`text-xs font-semibold px-1.5 py-0.5 rounded ${bg}`}>
      {score.toFixed(1)}
    </span>
  );
}

export function Dashboard() {
  const { data: overview } = useQuery({ queryKey: ['overview'], queryFn: getOverview, refetchInterval: 15000 });
  const { data: health } = useQuery({ queryKey: ['health'], queryFn: getSystemHealth, refetchInterval: 10000 });

  type MarketRow = { market: string; jobs: number; companies: number; avg_quality: number | null };
  type SiteRow = { id: string; name: string; quality_score: number | null; job_count: number; market_code: string };

  const stats = overview?.job_stats;
  const markets: MarketRow[] = overview?.markets || [];
  const coverage = overview?.coverage;
  const quality = overview?.quality;
  const qualitySites: SiteRow[] = overview?.quality_sites || [];

  // Fill in all 30 days, including zero-count days missing from the DB
  const trendData = useMemo(() => {
    const raw: Array<{ day: string; count: number }> = overview?.trends || [];
    const map = new Map(raw.map(t => [t.day, t.count]));
    const result = [];
    for (let i = 29; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      result.push({ day: key.slice(5), count: map.get(key) || 0 }); // "MM-DD"
    }
    return result;
  }, [overview?.trends]);

  const totalJobs = markets.reduce((s, m) => s + m.jobs, 0) || 1;

  const topSites = qualitySites.slice(0, 10);
  const bottomSites = [...qualitySites]
    .sort((a, b) => (a.quality_score || 0) - (b.quality_score || 0))
    .slice(0, 10);

  const bands = quality?.bands
    ? [
        { name: 'Excellent',    ...quality.bands.excellent },
        { name: 'Good',         ...quality.bands.good },
        { name: 'Fair',         ...quality.bands.fair },
        { name: 'Poor',         ...quality.bands.poor },
        { name: 'Disqualified', ...quality.bands.disqualified },
      ]
    : [];

  const fieldGroups = coverage
    ? [
        {
          heading: 'Core data',
          fields: [
            { label: 'Company Name',    pct: coverage.company_name_pct },
            { label: 'Role Title',      pct: coverage.title_pct },
            { label: 'Description',     pct: coverage.description_pct },
            { label: 'Location',        pct: coverage.location_pct },
          ],
        },
        {
          heading: 'High quality attributes',
          fields: [
            { label: 'Employment Type', pct: coverage.employment_type_pct },
            { label: 'Salary',          pct: coverage.salary_pct },
          ],
        },
        {
          heading: 'Very high quality attributes',
          fields: [
            { label: 'Seniority',       pct: coverage.seniority_pct },
            { label: 'Requirements',    pct: coverage.requirements_pct },
            { label: 'Benefits',        pct: coverage.benefits_pct },
          ],
        },
      ]
    : [];

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
                <div className={`text-xs ${status === 'ok' ? 'text-green-600' : 'text-red-600'}`}>
                  {String(status)}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard label="Unique Active Jobs" value={stats?.unique_active ?? '—'} />
        <StatCard label="New Today"          value={stats?.new_today ?? '—'} />
        <StatCard label="New This Week"      value={stats?.new_this_week ?? '—'} />
        <StatCard label="Duplicates Suppressed" value={stats?.duplicates ?? '—'} />
      </div>

      {/* Jobs by Market + Market Summary */}
      {markets.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="card p-5">
            <h2 className="text-base font-semibold text-gray-900 mb-4">Jobs by Market</h2>
            <ResponsiveContainer width="100%" height={Math.max(markets.length * 44, 120)}>
              <BarChart layout="vertical" data={markets} margin={{ left: 0, right: 16, top: 0, bottom: 0 }}>
                <XAxis type="number" tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="market" tick={{ fontSize: 12, fontWeight: 600 }} width={30} axisLine={false} tickLine={false} />
                <Tooltip
                  formatter={(v) => [Number(v).toLocaleString(), 'Jobs']}
                  contentStyle={{ fontSize: 12, borderRadius: 8 }}
                />
                <Bar dataKey="jobs" fill="#0e8136" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="card p-5">
            <h2 className="text-base font-semibold text-gray-900 mb-4">Market Summary</h2>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left text-xs font-semibold text-gray-500 pb-2">Market</th>
                  <th className="text-right text-xs font-semibold text-gray-500 pb-2">Jobs</th>
                  <th className="text-right text-xs font-semibold text-gray-500 pb-2">Share</th>
                  <th className="text-right text-xs font-semibold text-gray-500 pb-2">Cos</th>
                  <th className="text-right text-xs font-semibold text-gray-500 pb-2">Avg Q</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {markets.map(m => (
                  <tr key={m.market}>
                    <td className="py-2 font-semibold text-gray-800">{m.market}</td>
                    <td className="py-2 text-right text-gray-600">{m.jobs.toLocaleString()}</td>
                    <td className="py-2 text-right text-gray-500">{((m.jobs / totalJobs) * 100).toFixed(1)}%</td>
                    <td className="py-2 text-right text-gray-500">{m.companies.toLocaleString()}</td>
                    <td className="py-2 text-right">
                      {m.avg_quality
                        ? <span className="font-semibold text-green-700">{m.avg_quality.toFixed(1)}</span>
                        : <span className="text-gray-400">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Jobs Added (last 30 days) */}
      <div className="card p-5">
        <h2 className="text-base font-semibold text-gray-900 mb-4">Jobs Added (last 30 days)</h2>
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={trendData} margin={{ left: 0, right: 8, top: 4, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" vertical={false} />
            <XAxis dataKey="day" tick={{ fontSize: 10 }} interval={4} axisLine={false} tickLine={false} />
            <YAxis tick={{ fontSize: 11 }} width={42} axisLine={false} tickLine={false} />
            <Tooltip
              formatter={(v) => [Number(v).toLocaleString(), 'Jobs added']}
              contentStyle={{ fontSize: 12, borderRadius: 8 }}
            />
            <Area
              type="monotone"
              dataKey="count"
              stroke="#0e8136"
              fill="#0e813618"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Job Quality Distribution + Quality Issues */}
      {quality && quality.total_scored > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="card p-5">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h2 className="text-base font-semibold text-gray-900">Job Quality Distribution</h2>
                <p className="text-xs text-gray-400 mt-0.5">
                  {quality.total_scored.toLocaleString()} scored / {quality.total_unscored.toLocaleString()} pending · avg {quality.average_score}
                </p>
              </div>
              <button
                onClick={() => triggerQualityScoring()}
                className="text-xs px-3 py-1.5 bg-brand text-white rounded-lg hover:bg-brand/90 transition-colors flex-shrink-0"
              >
                Score Jobs
              </button>
            </div>
            <div className="flex items-center gap-6">
              <PieChart width={130} height={130}>
                <Pie data={bands} dataKey="count" innerRadius={36} outerRadius={60} paddingAngle={2} startAngle={90} endAngle={-270}>
                  {bands.map((_, i) => <Cell key={i} fill={BAND_COLORS[i]} />)}
                </Pie>
              </PieChart>
              <div className="space-y-1.5">
                {bands.map((b, i) => (
                  <div key={b.name} className="flex items-center gap-2 text-sm">
                    <span className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ background: BAND_COLORS[i] }} />
                    <span className="text-gray-700">{b.name}</span>
                    <span className="text-gray-400 text-xs ml-1">{b.count.toLocaleString()} ({b.pct}%)</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="card p-5">
            <h2 className="text-base font-semibold text-gray-900 mb-4">Quality Issues Detected</h2>
            <div className="space-y-4">
              {[
                { label: 'Scam patterns',           desc: 'Jobs with wire transfer, MLM, or payment requests', key: 'scam_detected' },
                { label: 'Discrimination language', desc: 'Age, gender, ethnicity, religion preferences',       key: 'discrimination_detected' },
                { label: 'Inappropriate language',  desc: 'Profanity or obscene content',                       key: 'bad_words_detected' },
              ].map(({ label, desc, key }) => {
                const count = quality.flags?.[key] || 0;
                const pct = quality.total_scored ? ((count / quality.total_scored) * 100).toFixed(1) : '0.0';
                return (
                  <div key={key} className="flex items-start gap-3">
                    <CheckCircle className={`w-4 h-4 mt-0.5 flex-shrink-0 ${count === 0 ? 'text-green-500' : 'text-red-400'}`} />
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-gray-800">{label}</div>
                      <div className="text-xs text-gray-400">{desc}</div>
                    </div>
                    <span className={`text-sm font-semibold flex-shrink-0 ${count > 0 ? 'text-red-600' : 'text-gray-500'}`}>
                      {count} ({pct}%)
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* Top / Lowest quality sites */}
      {qualitySites.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="card p-5">
            <h2 className="text-base font-semibold text-gray-900 mb-3">Top Quality Sites</h2>
            <div className="space-y-1">
              {topSites.map(s => (
                <div key={s.id} className="flex items-center justify-between text-sm py-0.5">
                  <div className="truncate text-gray-700 flex-1 min-w-0 pr-3">{s.name}</div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <span className="text-xs text-gray-400">{s.job_count} jobs</span>
                    <ScoreBadge score={s.quality_score} />
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="card p-5">
            <h2 className="text-base font-semibold text-gray-900 mb-3">Lowest Quality Sites</h2>
            <div className="space-y-1">
              {bottomSites.map(s => (
                <div key={s.id} className="flex items-center justify-between text-sm py-0.5">
                  <div className="truncate text-gray-700 flex-1 min-w-0 pr-3">{s.name}</div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <span className="text-xs text-gray-400">{s.job_count} jobs</span>
                    <ScoreBadge score={s.quality_score} />
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Field Coverage */}
      {fieldGroups.length > 0 && (
        <div className="card p-5">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Field Coverage</h2>
          <p className="text-xs text-gray-400 mb-4">% of active jobs with each field populated</p>
          <div className="space-y-5">
            {fieldGroups.map(group => (
              <div key={group.heading}>
                <div className="text-[10px] font-bold uppercase tracking-widest text-gray-400 mb-2">
                  {group.heading}
                </div>
                <div className="space-y-2">
                  {group.fields.map(f => (
                    <div key={f.label}>
                      <div className="flex justify-between text-sm mb-1">
                        <span className="text-gray-700">{f.label}</span>
                        <span className="text-gray-500 font-medium">{(f.pct ?? 0).toFixed(1)}%</span>
                      </div>
                      <div className="w-full bg-gray-100 rounded-full h-1.5">
                        <div
                          className="h-1.5 rounded-full transition-all"
                          style={{
                            width: `${f.pct ?? 0}%`,
                            background: (f.pct ?? 0) >= 90 ? '#16a34a' : (f.pct ?? 0) >= 50 ? '#eab308' : '#ef4444',
                          }}
                        />
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
