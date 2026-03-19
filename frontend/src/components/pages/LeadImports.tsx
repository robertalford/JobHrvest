import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getLeadImportSummary, getLeadImports, triggerLeadImport } from '../../lib/api';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const STATUS_COLORS: Record<string, string> = {
  success: '#0e8136',
  failed: '#ef4444',
  blocked: '#f97316',
  skipped: '#6b7280',
  pending: '#3b82f6',
};

const COUNTRY_LABELS: Record<string, string> = {
  AU: '🇦🇺 Australia',
  SG: '🇸🇬 Singapore',
  PH: '🇵🇭 Philippines',
  NZ: '🇳🇿 New Zealand',
  MY: '🇲🇾 Malaysia',
  ID: '🇮🇩 Indonesia',
  TH: '🇹🇭 Thailand',
  HK: '🇭🇰 Hong Kong',
};

export function LeadImports() {
  const qc = useQueryClient();
  const [filterCountry, setFilterCountry] = useState('');
  const [filterStatus, setFilterStatus] = useState('');
  const [filterCategory, setFilterCategory] = useState('');
  const [page, setPage] = useState(0);
  const limit = 50;

  const { data: summary } = useQuery({
    queryKey: ['lead-import-summary'],
    queryFn: getLeadImportSummary,
    refetchInterval: 10000,
  });

  const { data: leads, isLoading: leadsLoading } = useQuery({
    queryKey: ['lead-imports', filterCountry, filterStatus, filterCategory, page],
    queryFn: () => getLeadImports({
      country: filterCountry || undefined,
      status: filterStatus || undefined,
      category: filterCategory || undefined,
      limit,
      offset: page * limit,
    }),
    refetchInterval: 15000,
  });

  const triggerMutation = useMutation({
    mutationFn: triggerLeadImport,
    onSuccess: () => {
      setTimeout(() => qc.invalidateQueries({ queryKey: ['lead-import-summary'] }), 2000);
    },
  });

  const byStatus = summary?.by_status ?? {};
  const byCountry = summary?.by_country ?? {};
  const byCategory = summary?.by_category ?? {};

  const total = summary?.total ?? 0;
  const successCount = byStatus.success ?? 0;
  const failedCount = byStatus.failed ?? 0;
  const blockedCount = byStatus.blocked ?? 0;
  const pendingCount = byStatus.pending ?? 0;

  const countryChartData = Object.entries(byCountry).map(([code, data]: [string, any]) => ({
    name: code,
    success: data.by_status.success ?? 0,
    failed: data.by_status.failed ?? 0,
    blocked: data.by_status.blocked ?? 0,
    skipped: data.by_status.skipped ?? 0,
  })).sort((a, b) => (b.success + b.failed) - (a.success + a.failed));

  const categoryChartData = Object.entries(byCategory)
    .map(([cat, data]: [string, any]) => ({
      name: cat === 'unknown' ? 'Unknown' : cat,
      total: data.total,
      success: data.by_status.success ?? 0,
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 15);

  const countries = Object.keys(byCountry).sort();
  const categories = Object.keys(byCategory).sort();

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Lead Imports</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            CSV pipeline from ad_gap_data_all_markets.csv — {total.toLocaleString()} leads tracked
          </p>
        </div>
        <button
          onClick={() => triggerMutation.mutate({})}
          disabled={triggerMutation.isPending}
          className="btn-primary flex items-center gap-2"
        >
          {triggerMutation.isPending ? 'Starting...' : 'Run Import'}
        </button>
      </div>

      {triggerMutation.isSuccess && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-3 text-sm text-green-700">
          Import started in background. Stats will refresh automatically.
        </div>
      )}

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        {[
          { label: 'Total Leads', value: total, color: '#3b82f6' },
          { label: 'Imported', value: successCount, color: '#0e8136' },
          { label: 'Failed', value: failedCount, color: '#ef4444' },
          { label: 'Blocked', value: blockedCount, color: '#f97316' },
          { label: 'Pending', value: pendingCount, color: '#6b7280' },
        ].map(({ label, value, color }) => (
          <div key={label} className="card p-4">
            <div className="text-2xl font-bold" style={{ color }}>{value.toLocaleString()}</div>
            <div className="text-xs text-gray-500 mt-0.5">{label}</div>
          </div>
        ))}
      </div>

      {/* Country breakdown chart */}
      {countryChartData.length > 0 && (
        <div className="card p-5">
          <h2 className="font-semibold text-gray-900 mb-4">Leads by Country / Market</h2>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={countryChartData} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis dataKey="name" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Bar dataKey="success" name="Imported" stackId="a" fill="#0e8136" />
              <Bar dataKey="failed" name="Failed" stackId="a" fill="#ef4444" />
              <Bar dataKey="blocked" name="Blocked" stackId="a" fill="#f97316" />
              <Bar dataKey="skipped" name="Skipped" stackId="a" fill="#d1d5db" />
            </BarChart>
          </ResponsiveContainer>

          {/* Country detail table */}
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left py-2 text-gray-500 font-medium">Market</th>
                  <th className="text-right py-2 text-gray-500 font-medium">Total</th>
                  <th className="text-right py-2 text-gray-500 font-medium">Imported</th>
                  <th className="text-right py-2 text-gray-500 font-medium">Failed</th>
                  <th className="text-right py-2 text-gray-500 font-medium">Blocked</th>
                  <th className="text-right py-2 text-gray-500 font-medium">Jobs Found</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(byCountry)
                  .sort((a: any, b: any) => b[1].total - a[1].total)
                  .map(([code, data]: [string, any]) => (
                    <tr key={code} className="border-b border-gray-50 hover:bg-gray-50">
                      <td className="py-2">{COUNTRY_LABELS[code] ?? code}</td>
                      <td className="py-2 text-right font-medium">{data.total.toLocaleString()}</td>
                      <td className="py-2 text-right text-green-600">{(data.by_status.success ?? 0).toLocaleString()}</td>
                      <td className="py-2 text-right text-red-500">{(data.by_status.failed ?? 0).toLocaleString()}</td>
                      <td className="py-2 text-right text-orange-500">{(data.by_status.blocked ?? 0).toLocaleString()}</td>
                      <td className="py-2 text-right text-gray-700">{data.jobs_extracted.toLocaleString()}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Category breakdown */}
      {categoryChartData.length > 0 && (
        <div className="card p-5">
          <h2 className="font-semibold text-gray-900 mb-4">Top Categories</h2>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={categoryChartData} layout="vertical" margin={{ left: 60, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={60} />
              <Tooltip />
              <Bar dataKey="success" name="Imported" fill="#0e8136" />
              <Bar dataKey="total" name="Total" fill="#e5e7eb" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Leads table with filters */}
      <div className="card">
        <div className="p-4 border-b border-gray-100 flex flex-wrap gap-3 items-center">
          <select
            value={filterCountry}
            onChange={e => { setFilterCountry(e.target.value); setPage(0); }}
            className="text-sm border border-gray-200 rounded px-2 py-1.5"
          >
            <option value="">All Markets</option>
            {countries.map(c => (
              <option key={c} value={c}>{COUNTRY_LABELS[c] ?? c}</option>
            ))}
          </select>
          <select
            value={filterStatus}
            onChange={e => { setFilterStatus(e.target.value); setPage(0); }}
            className="text-sm border border-gray-200 rounded px-2 py-1.5"
          >
            <option value="">All Statuses</option>
            {['success', 'failed', 'blocked', 'skipped', 'pending'].map(s => (
              <option key={s} value={s}>{s.charAt(0).toUpperCase() + s.slice(1)}</option>
            ))}
          </select>
          <select
            value={filterCategory}
            onChange={e => { setFilterCategory(e.target.value); setPage(0); }}
            className="text-sm border border-gray-200 rounded px-2 py-1.5"
          >
            <option value="">All Categories</option>
            {categories.map(c => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
          <span className="text-xs text-gray-400 ml-auto">
            {leads?.total?.toLocaleString() ?? '...'} matching
          </span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="text-left px-4 py-3 text-gray-500 font-medium">Company</th>
                <th className="text-left px-4 py-3 text-gray-500 font-medium">Market</th>
                <th className="text-left px-4 py-3 text-gray-500 font-medium">Category</th>
                <th className="text-left px-4 py-3 text-gray-500 font-medium">Domain</th>
                <th className="text-center px-4 py-3 text-gray-500 font-medium">Status</th>
                <th className="text-right px-4 py-3 text-gray-500 font-medium">Expected Jobs</th>
                <th className="text-right px-4 py-3 text-gray-500 font-medium">Jobs Found</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {leadsLoading ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">Loading...</td></tr>
              ) : leads?.items?.length === 0 ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No leads found. Run an import first.</td></tr>
              ) : (
                leads?.items?.map((lead: any) => (
                  <tr key={lead.id} className="hover:bg-gray-50">
                    <td className="px-4 py-2.5 font-medium text-gray-900 truncate max-w-xs">{lead.advertiser_name}</td>
                    <td className="px-4 py-2.5 text-gray-500">{COUNTRY_LABELS[lead.country_id] ?? lead.country_id}</td>
                    <td className="px-4 py-2.5 text-gray-500 truncate max-w-[120px]">{lead.ad_origin_category ?? '—'}</td>
                    <td className="px-4 py-2.5">
                      {lead.sample_linkout_url ? (
                        <a href={lead.sample_linkout_url} target="_blank" rel="noreferrer"
                          className="text-brand hover:underline truncate block max-w-[180px]">
                          {lead.origin_domain}
                        </a>
                      ) : (
                        <span className="text-gray-500">{lead.origin_domain}</span>
                      )}
                    </td>
                    <td className="px-4 py-2.5 text-center">
                      <span
                        className="inline-block px-2 py-0.5 rounded-full text-xs font-medium text-white"
                        style={{ backgroundColor: STATUS_COLORS[lead.status] ?? '#6b7280' }}
                      >
                        {lead.status}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-right text-gray-500">{lead.expected_job_count ?? '—'}</td>
                    <td className="px-4 py-2.5 text-right font-medium">{lead.jobs_extracted ?? 0}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {leads && leads.total > limit && (
          <div className="px-4 py-3 border-t border-gray-100 flex items-center justify-between">
            <button
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={page === 0}
              className="text-sm text-gray-600 disabled:opacity-40 hover:text-gray-900"
            >
              Previous
            </button>
            <span className="text-xs text-gray-400">
              Page {page + 1} of {Math.ceil(leads.total / limit)}
            </span>
            <button
              onClick={() => setPage(p => p + 1)}
              disabled={(page + 1) * limit >= leads.total}
              className="text-sm text-gray-600 disabled:opacity-40 hover:text-gray-900"
            >
              Next
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
