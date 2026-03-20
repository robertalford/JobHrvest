import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getDiscoverySources, createDiscoverySource, updateDiscoverySource, deleteDiscoverySource } from '../../lib/api';
import { Search, Plus, ExternalLink, Edit2, Trash2, X, Check, Globe2 } from 'lucide-react';

type Source = {
  id: string;
  name: string;
  base_url: string;
  market: string;
  is_active: boolean;
  purpose: string;
  last_link_harvest_at: string | null;
};

const MARKETS = ['AU', 'US', 'UK', 'global'];

function EditRow({ source, onSave, onCancel }: { source: Source; onSave: (data: Partial<Source>) => void; onCancel: () => void }) {
  const [name, setName] = useState(source.name);
  const [base_url, setBaseUrl] = useState(source.base_url);
  const [market, setMarket] = useState(source.market);
  const [is_active, setIsActive] = useState(source.is_active);

  return (
    <tr className="bg-blue-50">
      <td className="px-4 py-2">
        <input value={name} onChange={e => setName(e.target.value)}
          className="w-full border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none focus:ring-1 focus:ring-brand" />
      </td>
      <td className="px-4 py-2">
        <input value={base_url} onChange={e => setBaseUrl(e.target.value)}
          className="w-full border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none focus:ring-1 focus:ring-brand" />
      </td>
      <td className="px-4 py-2">
        <select value={market} onChange={e => setMarket(e.target.value)}
          className="border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none">
          {MARKETS.map(m => <option key={m} value={m}>{m}</option>)}
        </select>
      </td>
      <td className="px-4 py-2">
        <select value={String(is_active)} onChange={e => setIsActive(e.target.value === 'true')}
          className="border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none">
          <option value="true">Active</option>
          <option value="false">Inactive</option>
        </select>
      </td>
      <td className="px-4 py-2 text-center">—</td>
      <td className="px-4 py-2">
        <div className="flex gap-1">
          <button onClick={() => onSave({ name, base_url, market, is_active })}
            className="btn-primary text-xs px-2 py-1 flex items-center gap-1">
            <Check className="w-3 h-3" /> Save
          </button>
          <button onClick={onCancel} className="btn-secondary text-xs px-2 py-1 flex items-center gap-1">
            <X className="w-3 h-3" /> Cancel
          </button>
        </div>
      </td>
    </tr>
  );
}

export function DiscoverySources() {
  const qc = useQueryClient();
  const [search, setSearch] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [marketFilter, setMarketFilter] = useState('');
  const [activeFilter, setActiveFilter] = useState('');
  const [page, setPage] = useState(1);
  const pageSize = 50;
  const [editingId, setEditingId] = useState<string | null>(null);
  const [showAdd, setShowAdd] = useState(false);
  const [newName, setNewName] = useState('');
  const [newUrl, setNewUrl] = useState('');
  const [newMarket, setNewMarket] = useState('AU');

  const { data, isLoading } = useQuery<{ items: Source[]; total: number }>({
    queryKey: ['discovery-sources', page, search, marketFilter, activeFilter],
    queryFn: () => getDiscoverySources({ page, page_size: pageSize, search: search || undefined, market: marketFilter || undefined, is_active: activeFilter || undefined }),
    placeholderData: (prev) => prev,
  });

  const createMut = useMutation({
    mutationFn: (d: { name: string; base_url: string; market: string }) => createDiscoverySource(d),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['discovery-sources'] }); setShowAdd(false); setNewName(''); setNewUrl(''); setNewMarket('AU'); },
  });

  const updateMut = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Source> }) => updateDiscoverySource(id, data),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['discovery-sources'] }); setEditingId(null); },
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => deleteDiscoverySource(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['discovery-sources'] }),
  });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  const applySearch = () => { setSearch(searchInput); setPage(1); };

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-amber-50 flex items-center justify-center">
            <Globe2 className="w-5 h-5 text-amber-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Link Discovery</h1>
            <p className="text-sm text-gray-500">Aggregator sources used to discover company career pages</p>
          </div>
        </div>
        <button onClick={() => setShowAdd(v => !v)} className="btn-primary flex items-center gap-2">
          <Plus className="w-4 h-4" /> Add Source
        </button>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-3 gap-4">
        <div className="card p-4">
          <div className="text-xl font-bold text-gray-900">{total}</div>
          <div className="text-xs text-gray-500">Total sources</div>
        </div>
        <div className="card p-4">
          <div className="text-xl font-bold text-green-600">{items.filter(s => s.is_active).length}</div>
          <div className="text-xs text-gray-500">Active (page)</div>
        </div>
        <div className="card p-4">
          <div className="text-xl font-bold text-gray-900">{[...new Set(items.map(s => s.market))].length}</div>
          <div className="text-xs text-gray-500">Markets covered</div>
        </div>
      </div>

      {/* Add form */}
      {showAdd && (
        <div className="card p-4 border border-brand/30 space-y-3">
          <h3 className="font-semibold text-gray-800">Add Discovery Source</h3>
          <div className="grid grid-cols-3 gap-3">
            <input placeholder="Source name (e.g. Indeed AU)" value={newName} onChange={e => setNewName(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-brand" />
            <input placeholder="Base URL (e.g. https://au.indeed.com)" value={newUrl} onChange={e => setNewUrl(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-brand" />
            <select value={newMarket} onChange={e => setNewMarket(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none">
              {MARKETS.map(m => <option key={m} value={m}>{m}</option>)}
            </select>
          </div>
          <div className="flex gap-2">
            <button onClick={() => createMut.mutate({ name: newName, base_url: newUrl, market: newMarket })}
              disabled={!newName.trim() || !newUrl.trim() || createMut.isPending}
              className="btn-primary text-sm flex items-center gap-1.5">
              <Plus className="w-3.5 h-3.5" /> Add
            </button>
            <button onClick={() => setShowAdd(false)} className="btn-secondary text-sm">Cancel</button>
          </div>
        </div>
      )}

      {/* Filters + Table */}
      <div className="card">
        <div className="p-4 border-b border-gray-100 flex items-center gap-3">
          <div className="flex-1 flex items-center gap-2">
            <div className="relative flex-1 max-w-xs">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                value={searchInput}
                onChange={e => setSearchInput(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && applySearch()}
                placeholder="Search sources…"
                className="w-full pl-9 pr-3 py-1.5 border border-gray-200 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-brand"
              />
            </div>
            <button onClick={applySearch} className="btn-secondary text-sm px-3 py-1.5">Search</button>
          </div>
          <select value={marketFilter} onChange={e => { setMarketFilter(e.target.value); setPage(1); }}
            className="border border-gray-200 rounded text-xs px-2 py-1.5 text-gray-600 focus:outline-none">
            <option value="">All markets</option>
            {MARKETS.map(m => <option key={m} value={m}>{m}</option>)}
          </select>
          <select value={activeFilter} onChange={e => { setActiveFilter(e.target.value); setPage(1); }}
            className="border border-gray-200 rounded text-xs px-2 py-1.5 text-gray-600 focus:outline-none">
            <option value="">All statuses</option>
            <option value="true">Active</option>
            <option value="false">Inactive</option>
          </select>
          {total > 0 && <span className="text-xs text-gray-400">{total.toLocaleString()} total</span>}
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                {['Name', 'URL', 'Market', 'Status', 'Last Harvest', 'Actions'].map(h => (
                  <th key={h} className="text-left px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wide">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {isLoading ? (
                <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
              ) : items.length === 0 ? (
                <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">No sources found.</td></tr>
              ) : items.map(src => (
                editingId === src.id ? (
                  <EditRow
                    key={src.id}
                    source={src}
                    onSave={data => updateMut.mutate({ id: src.id, data })}
                    onCancel={() => setEditingId(null)}
                  />
                ) : (
                  <tr key={src.id} className="hover:bg-gray-50">
                    <td className="px-4 py-3 font-medium text-gray-800">{src.name}</td>
                    <td className="px-4 py-3 text-gray-500 text-xs">
                      <a href={src.base_url} target="_blank" rel="noopener noreferrer"
                        className="flex items-center gap-1 text-brand hover:underline max-w-[220px] truncate">
                        {src.base_url.replace(/^https?:\/\/(www\.)?/, '')}
                        <ExternalLink className="w-3 h-3 flex-shrink-0" />
                      </a>
                    </td>
                    <td className="px-4 py-3">
                      <span className="badge-gray text-xs">{src.market}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`badge text-xs ${src.is_active ? 'badge-green' : 'badge-gray'}`}>
                        {src.is_active ? 'Active' : 'Inactive'}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-gray-400 text-xs">
                      {src.last_link_harvest_at ? new Date(src.last_link_harvest_at).toLocaleString() : '—'}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex gap-1">
                        <button onClick={() => setEditingId(src.id)}
                          className="btn-secondary text-xs px-2 py-1 flex items-center gap-1">
                          <Edit2 className="w-3 h-3" />
                        </button>
                        <button onClick={() => { if (confirm(`Delete "${src.name}"?`)) deleteMut.mutate(src.id); }}
                          className="btn-secondary text-xs px-2 py-1 text-red-500 hover:bg-red-50 flex items-center gap-1">
                          <Trash2 className="w-3 h-3" />
                        </button>
                      </div>
                    </td>
                  </tr>
                )
              ))}
            </tbody>
          </table>
        </div>

        {totalPages > 1 && (
          <div className="p-3 border-t border-gray-100 flex items-center justify-between">
            <span className="text-xs text-gray-500">Page {page} of {totalPages} · {total.toLocaleString()} records</span>
            <div className="flex gap-1">
              <button onClick={() => setPage(1)} disabled={page === 1} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">«</button>
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="btn-secondary text-xs px-3 py-1 disabled:opacity-40">Prev</button>
              <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn-secondary text-xs px-3 py-1 disabled:opacity-40">Next</button>
              <button onClick={() => setPage(totalPages)} disabled={page >= totalPages} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">»</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
