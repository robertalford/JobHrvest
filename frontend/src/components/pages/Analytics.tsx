import { useQuery } from '@tanstack/react-query';
import { getFieldCoverage, getTrends } from '../../lib/api';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export function Analytics() {
  const { data: coverage } = useQuery({ queryKey: ['field-coverage'], queryFn: getFieldCoverage });
  const { data: trends } = useQuery({ queryKey: ['trends'], queryFn: getTrends });

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
