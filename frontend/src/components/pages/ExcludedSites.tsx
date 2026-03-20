import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Ban, Search, ChevronLeft, ChevronRight, Plus, Trash2, X, Save } from 'lucide-react';
import { getExcludedSites, getExcludedSiteStats, addExcludedSite, removeExcludedSite } from '../../lib/api';

type ExcludedSite = {
  id: string;
  domain: string;
  company_name: string | null;
  site_url: string | null;
  site_type: string | null;
  country_code: string | null;
  expected_job_count: number | null;
  reason: string | null;
  source_file: string | null;
  created_at: string | null;
};

const REASON_COLOURS: Record<string, string> = {
  hardcoded_block: 'bg-red-100 text-red-700',
  disabled_state:  'bg-orange-100 text-orange-700',
  manual:          'bg-purple-100 text-purple-700',
};

export function ExcludedSites() {
  const qc = useQueryClient();
  const [page, setPage] = useState(1);
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const [country, setCountry] = useState('');
  const [siteType, setSiteType] = useState('');
  const pageSize = 50;

  // Add-domain form state
  const [showAdd, setShowAdd] = useState(false);
  const [newDomain, setNewDomain] = useState('');
  const [newCompany, setNewCompany] = useState('');
  const [newReason, setNewReason] = useState('');

  const { data, isLoading } = useQuery({
    queryKey: ['excluded-sites', page, search, country, siteType],
    queryFn: () => getExcludedSites(page, pageSize, search, country, siteType),
  });

  const { data: stats } = useQuery({
    queryKey: ['excluded-sites-stats'],
    queryFn: getExcludedSiteStats,
  });

  const addMut = useMutation({
    mutationFn: (d: { domain: string; company_name?: string; reason?: string }) =>
      addExcludedSite(d),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['excluded-sites'] });
      qc.invalidateQueries({ queryKey: ['excluded-sites-stats'] });
      setShowAdd(false);
      setNewDomain('');
      setNewCompany('');
      setNewReason('');
    },
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => removeExcludedSite(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['excluded-sites'] });
      qc.invalidateQueries({ queryKey: ['excluded-sites-stats'] });
    },
  });

  const totalPages = data ? Math.ceil(data.total / pageSize) : 1;

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    setSearch(searchInput);
    setPage(1);
  }

  function handleAdd(e: React.FormEvent) {
    e.preventDefault();
    if (!newDomain.trim()) return;
    addMut.mutate({
      domain: newDomain.trim().toLowerCase(),
      company_name: newCompany.trim() || undefined,
      reason: newReason.trim() || 'manual',
    });
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-red-100 flex items-center justify-center">
            <Ban className="w-5 h-5 text-red-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Excluded Sites</h1>
            <p className="text-sm text-gray-500">Domains blocked from crawling — imported disabled sites, hardcoded brands, and manual additions</p>
          </div>
        </div>
        <button
          className="btn-primary flex items-center gap-1.5 text-sm"
          onClick={() => setShowAdd(v => !v)}
        >
          <Plus className="w-3.5 h-3.5" /> Add Domain
        </button>
      </div>

      {/* Add-domain form */}
      {showAdd && (
        <div className="card p-4 border border-gray-200 bg-gray-50 space-y-3">
          <h3 className="text-sm font-semibold text-gray-700">Manually block a domain</h3>
          <form onSubmit={handleAdd} className="flex flex-wrap gap-3 items-end">
            <div className="flex-1 min-w-[180px]">
              <label className="text-xs font-medium text-gray-500 mb-1 block">Domain *</label>
              <input
                type="text"
                value={newDomain}
                onChange={e => setNewDomain(e.target.value)}
                placeholder="example.com"
                required
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
              />
            </div>
            <div className="flex-1 min-w-[150px]">
              <label className="text-xs font-medium text-gray-500 mb-1 block">Company name</label>
              <input
                type="text"
                value={newCompany}
                onChange={e => setNewCompany(e.target.value)}
                placeholder="Optional"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
              />
            </div>
            <div className="flex-1 min-w-[150px]">
              <label className="text-xs font-medium text-gray-500 mb-1 block">Reason</label>
              <input
                type="text"
                value={newReason}
                onChange={e => setNewReason(e.target.value)}
                placeholder="manual"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
              />
            </div>
            <div className="flex gap-2">
              <button
                type="submit"
                disabled={!newDomain.trim() || addMut.isPending}
                className="btn-primary flex items-center gap-1.5 text-sm"
              >
                <Save className="w-3.5 h-3.5" /> Save
              </button>
              <button type="button" className="btn-secondary text-sm" onClick={() => setShowAdd(false)}>
                <X className="w-3.5 h-3.5 inline mr-1" />Cancel
              </button>
            </div>
          </form>
          {addMut.isError && (
            <p className="text-xs text-red-600">
              {(addMut.error as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? 'Failed to add domain'}
            </p>
          )}
        </div>
      )}

      {/* Stats row */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="card p-4">
            <div className="text-2xl font-bold text-gray-900">{(stats.total as number).toLocaleString()}</div>
            <div className="text-xs text-gray-500 mt-0.5">Total blocked</div>
          </div>
          {Object.entries((stats.by_type as Record<string, number>) ?? {}).slice(0, 3).map(([type, count]) => (
            <div key={type} className="card p-4">
              <div className="text-2xl font-bold text-gray-900">{(count as number).toLocaleString()}</div>
              <div className="text-xs text-gray-500 mt-0.5 capitalize">{type.replace('_', ' ')}</div>
            </div>
          ))}
        </div>
      )}

      {/* Search & filters */}
      <div className="card p-4">
        <form onSubmit={handleSearch} className="flex flex-wrap gap-3 items-end">
          <div className="flex-1 min-w-[200px]">
            <label className="text-xs font-medium text-gray-500 mb-1 block">Search domain or company</label>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                value={searchInput}
                onChange={e => setSearchInput(e.target.value)}
                placeholder="e.g. seek.com or Seek"
                className="w-full pl-9 pr-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>
          <div>
            <label className="text-xs font-medium text-gray-500 mb-1 block">Country</label>
            <input
              type="text"
              value={country}
              onChange={e => { setCountry(e.target.value.toUpperCase()); setPage(1); }}
              placeholder="AU"
              maxLength={2}
              className="w-20 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 uppercase"
            />
          </div>
          <div>
            <label className="text-xs font-medium text-gray-500 mb-1 block">Type</label>
            <select
              value={siteType}
              onChange={e => { setSiteType(e.target.value); setPage(1); }}
              className="px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">All types</option>
              <option value="employer">Employer</option>
              <option value="job_board">Job board</option>
              <option value="recruiter">Recruiter</option>
            </select>
          </div>
          <button
            type="submit"
            className="px-4 py-2 bg-[#0e8136] text-white text-sm font-medium rounded-lg hover:bg-[#0a6b2c] transition-colors"
          >
            Search
          </button>
          {(search || country || siteType) && (
            <button
              type="button"
              onClick={() => { setSearch(''); setSearchInput(''); setCountry(''); setSiteType(''); setPage(1); }}
              className="px-4 py-2 text-sm text-gray-500 border border-gray-200 rounded-lg hover:bg-gray-50"
            >
              Clear
            </button>
          )}
        </form>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-100 flex items-center justify-between">
          <span className="text-sm font-medium text-gray-700">
            {data ? `${data.total.toLocaleString()} sites` : 'Loading…'}
          </span>
          {data && totalPages > 1 && (
            <span className="text-xs text-gray-400">Page {page} of {totalPages}</span>
          )}
        </div>

        {isLoading ? (
          <div className="p-8 text-center text-gray-400 text-sm">Loading…</div>
        ) : !data?.items?.length ? (
          <div className="p-8 text-center text-gray-400 text-sm">No excluded sites found</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-100">
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Domain</th>
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Company</th>
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Type</th>
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Country</th>
                  <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Est. Jobs</th>
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Reason</th>
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Added</th>
                  <th className="py-3 px-4" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {(data.items as ExcludedSite[]).map(site => (
                  <tr key={site.id} className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-3 font-mono text-xs text-gray-900">{site.domain}</td>
                    <td className="px-4 py-3 text-gray-700">{site.company_name ?? <span className="text-gray-300">—</span>}</td>
                    <td className="px-4 py-3">
                      {site.site_type ? (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600 capitalize">
                          {site.site_type.replace('_', ' ')}
                        </span>
                      ) : <span className="text-gray-300">—</span>}
                    </td>
                    <td className="px-4 py-3">
                      {site.country_code ? (
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-blue-50 text-blue-700">{site.country_code}</span>
                      ) : <span className="text-gray-300">—</span>}
                    </td>
                    <td className="px-4 py-3 text-right text-gray-600">
                      {site.expected_job_count != null ? site.expected_job_count.toLocaleString() : <span className="text-gray-300">—</span>}
                    </td>
                    <td className="px-4 py-3">
                      {site.reason ? (
                        <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${REASON_COLOURS[site.reason] ?? 'bg-gray-100 text-gray-600'}`}>
                          {site.reason.replace('_', ' ')}
                        </span>
                      ) : <span className="text-gray-300">—</span>}
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-400">
                      {site.created_at ? new Date(site.created_at).toLocaleDateString() : '—'}
                    </td>
                    <td className="px-4 py-3 text-right">
                      {/* Only allow deletion of manually-added or migrated entries, not hardcoded seeds */}
                      <button
                        className="p-1 text-gray-300 hover:text-red-600 transition-colors"
                        title="Remove exclusion"
                        onClick={() => {
                          if (window.confirm(`Remove ${site.domain} from exclusion list?`)) {
                            deleteMut.mutate(site.id);
                          }
                        }}
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Pagination */}
        {data && totalPages > 1 && (
          <div className="px-4 py-3 border-t border-gray-100 flex items-center justify-between">
            <button
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
              className="flex items-center gap-1 px-3 py-1.5 text-sm border border-gray-200 rounded-lg disabled:opacity-40 hover:bg-gray-50"
            >
              <ChevronLeft className="w-4 h-4" /> Prev
            </button>
            <span className="text-xs text-gray-500">{page} / {totalPages}</span>
            <button
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="flex items-center gap-1 px-3 py-1.5 text-sm border border-gray-200 rounded-lg disabled:opacity-40 hover:bg-gray-50"
            >
              Next <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
