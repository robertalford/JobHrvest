import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getFieldCoverage, getTrends, getQualityDistribution, getQualityBySite, triggerQualityScoring } from '../../lib/api';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, PieChart, Pie } from 'recharts';

const BAND_COLORS: Record<string, string> = {
  excellent: '#0e8136',
  good: '#22c55e',
  fair: '#eab308',
  poor: '#f97316',
  disqualified: '#ef4444',
};

export function Analytics() {
  const qc = useQueryClient();
  const { data: coverage } = useQuery({ queryKey: ['field-coverage'], queryFn: getFieldCoverage });
  const { data: trends } = useQuery({ queryKey: ['trends'], queryFn: getTrends });
  const { data: quality } = useQuery({ queryKey: ['quality-distribution'], queryFn: getQualityDistribution, refetchInterval: 15000 });
  const { data: qualitySites } = useQuery({ queryKey: ['quality-by-site'], queryFn: getQualityBySite });

  const scoreMutation = useMutation({
    mutationFn: triggerQualityScoring,
    onSuccess: () => setTimeout(() => qc.invalidateQueries({ queryKey: ['quality-distribution'] }), 3000),
  });

  const fields = coverage ? [
    { label: 'Title', pct: coverage.title_pct },
    { label: 'Description', pct: coverage.description_pct },
    { label: 'Location', pct: coverage.location_pct },
    { label: 'Salary', pct: coverage.salary_pct },
    { label: 'Employment Type', pct: coverage.employment_type_pct },
    { label: 'Seniority', pct: coverage.seniority_pct },
    { label: 'Requirements', pct: coverage.requirements_pct },
    { label: 'Benefits', pct: coverage.benefits_pct },
  ] : [];

  const bandData = quality?.bands
    ? Object.entries(quality.bands).map(([band, data]: [string, any]) => ({
        name: band.charAt(0).toUpperCase() + band.slice(1),
        count: data.count,
        pct: data.pct,
        color: BAND_COLORS[band],
      }))
    : [];

  const pieData = bandData.filter(d => d.count > 0);

  const topSites = (qualitySites || []).slice(0, 10);
  const bottomSites = (qualitySites || []).slice(-10).reverse();

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Extraction Analytics</h1>

      {/* Jobs over time */}
      <div className="card p-5">
        <h2 className="font-semibold text-gray-900 mb-4">Jobs Added (last 30 days)</h2>
        {trends?.length ? (
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={trends}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis dataKey="day" tick={{ fontSize: 11 }} tickFormatter={(v: string) => v.slice(5)} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Line type="monotone" dataKey="count" stroke="#0e8136" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <p className="text-sm text-gray-400">No data yet.</p>
        )}
      </div>

      {/* Quality section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Quality distribution */}
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
                    {pieData.map((entry, i) => (
                      <Cell key={i} fill={entry.color} />
                    ))}
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

        {/* Quality flags */}
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
      {qualitySites && qualitySites.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="card p-5">
            <h2 className="font-semibold text-gray-900 mb-4">Top Quality Sites</h2>
            <div className="space-y-2">
              {topSites.map((site: any) => (
                <div key={site.id} className="flex items-center justify-between text-sm">
                  <span className="text-gray-700 truncate max-w-[200px]">{site.name}</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-400">{site.job_count} jobs</span>
                    <span
                      className="px-2 py-0.5 rounded-full text-xs font-medium text-white"
                      style={{
                        backgroundColor: site.quality_score >= 80 ? '#0e8136'
                          : site.quality_score >= 60 ? '#22c55e'
                          : site.quality_score >= 40 ? '#eab308'
                          : '#ef4444'
                      }}
                    >
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
                    <span
                      className="px-2 py-0.5 rounded-full text-xs font-medium text-white"
                      style={{
                        backgroundColor: site.quality_score >= 80 ? '#0e8136'
                          : site.quality_score >= 60 ? '#22c55e'
                          : site.quality_score >= 40 ? '#eab308'
                          : '#ef4444'
                      }}
                    >
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
      <div className="card p-5">
        <h2 className="font-semibold text-gray-900 mb-1">Field Coverage</h2>
        <p className="text-xs text-gray-400 mb-4">% of active jobs with each field populated</p>
        <div className="space-y-3">
          {fields.map(({ label, pct }) => (
            <div key={label}>
              <div className="flex justify-between text-sm mb-1">
                <span className="text-gray-700">{label}</span>
                <span className="font-medium text-gray-900">{pct?.toFixed(1) ?? 0}%</span>
              </div>
              <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                <div
                  className="h-full rounded-full"
                  style={{ width: `${pct ?? 0}%`, backgroundColor: pct >= 90 ? '#0e8136' : pct >= 70 ? '#eab308' : '#ef4444' }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
