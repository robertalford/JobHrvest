import { useState, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus, Trash2, Edit2, Upload, Save, X } from 'lucide-react';
import { getWordFilters, createWordFilter, updateWordFilter, deleteWordFilter, importWordFilters } from '../../lib/api';

export const ALL_MARKETS = ['AU', 'NZ', 'MY', 'PH', 'ID', 'SG', 'TH', 'HK'];

export const MARKET_NAMES: Record<string, string> = {
  AU: 'Australia', NZ: 'New Zealand', MY: 'Malaysia', PH: 'Philippines',
  ID: 'Indonesia', SG: 'Singapore', TH: 'Thailand', HK: 'Hong Kong',
};

export function MarketBadges({ markets }: { markets: string[] }) {
  return (
    <div className="flex flex-wrap gap-1">
      {markets.map(m => (
        <span key={m} className="badge-blue text-xs">{m}</span>
      ))}
    </div>
  );
}

export function MarketCheckboxGrid({ selected, onChange }: { selected: string[]; onChange: (v: string[]) => void }) {
  const toggle = (m: string) =>
    onChange(selected.includes(m) ? selected.filter(x => x !== m) : [...selected, m]);
  return (
    <div className="grid grid-cols-4 gap-2">
      {ALL_MARKETS.map(m => (
        <label key={m} className="flex items-center gap-1.5 text-sm cursor-pointer">
          <input type="checkbox" checked={selected.includes(m)} onChange={() => toggle(m)} className="rounded" />
          <span>{m}</span>
        </label>
      ))}
    </div>
  );
}

export function WordFilterSection({ filterType, title }: { filterType: string; title: string }) {
  const qc = useQueryClient();
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [addWord, setAddWord] = useState('');
  const [addMarkets, setAddMarkets] = useState<string[]>(ALL_MARKETS);
  const [showAdd, setShowAdd] = useState(false);
  const [editId, setEditId] = useState<string | null>(null);
  const [editWord, setEditWord] = useState('');
  const [editMarkets, setEditMarkets] = useState<string[]>([]);
  const fileRef = useRef<HTMLInputElement>(null);

  const { data } = useQuery({
    queryKey: ['word-filters', filterType, page, debouncedSearch],
    queryFn: () => getWordFilters(filterType, page, debouncedSearch),
    placeholderData: (prev) => prev,
  });

  const createMut = useMutation({
    mutationFn: (d: { word: string; filter_type: string; markets: string[] }) => createWordFilter(d),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['word-filters', filterType] }); setShowAdd(false); setAddWord(''); setAddMarkets(ALL_MARKETS); },
  });

  const updateMut = useMutation({
    mutationFn: ({ id, data }: { id: string; data: { word?: string; markets?: string[] } }) => updateWordFilter(id, data),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['word-filters', filterType] }); setEditId(null); },
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => deleteWordFilter(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['word-filters', filterType] }),
  });

  const importMut = useMutation({
    mutationFn: ({ ft, file }: { ft: string; file: File }) => importWordFilters(ft, file),
    onSuccess: (res) => { qc.invalidateQueries({ queryKey: ['word-filters', filterType] }); alert(`Imported ${res?.imported ?? 'some'} words.`); },
  });

  const handleSearch = (v: string) => { setSearch(v); setPage(1); setDebouncedSearch(v); };
  const startEdit = (item: Record<string, unknown>) => { setEditId(item.id as string); setEditWord(item.word as string); setEditMarkets((item.markets as string[]) ?? []); };
  const handleDelete = (id: string, word: string) => { if (window.confirm(`Remove '${word}' from ${title.toLowerCase()}?`)) deleteMut.mutate(id); };

  const items: Record<string, unknown>[] = data?.items ?? [];
  const total: number = data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / 50));

  return (
    <div className="card">
      <div className="p-4 border-b border-gray-100 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h2 className="font-semibold text-gray-900">{title}</h2>
          <span className="badge-gray text-xs">{total} words</span>
        </div>
        <div className="flex gap-2">
          <button className="btn-secondary flex items-center gap-1.5 text-sm" onClick={() => fileRef.current?.click()}>
            <Upload className="w-3.5 h-3.5" /> Upload CSV
          </button>
          <input ref={fileRef} type="file" accept=".csv" className="hidden" onChange={e => { const file = e.target.files?.[0]; if (file) importMut.mutate({ ft: filterType, file }); e.target.value = ''; }} />
          <button className="btn-primary flex items-center gap-1.5 text-sm" onClick={() => setShowAdd(v => !v)}>
            <Plus className="w-3.5 h-3.5" /> Add Word
          </button>
        </div>
      </div>

      {showAdd && (
        <div className="border-b border-gray-100 p-4 bg-gray-50 space-y-3">
          <input type="text" placeholder="Word or phrase" value={addWord} onChange={e => setAddWord(e.target.value)} className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-green-500" />
          <MarketCheckboxGrid selected={addMarkets} onChange={setAddMarkets} />
          <div className="flex gap-2">
            <button className="btn-primary flex items-center gap-1.5 text-sm" disabled={!addWord.trim() || createMut.isPending} onClick={() => createMut.mutate({ word: addWord.trim(), filter_type: filterType, markets: addMarkets })}>
              <Save className="w-3.5 h-3.5" /> Save
            </button>
            <button className="btn-secondary text-sm" onClick={() => setShowAdd(false)}><X className="w-3.5 h-3.5 inline mr-1" />Cancel</button>
          </div>
        </div>
      )}

      <div className="p-4 pb-2">
        <input type="text" placeholder="Search words…" value={search} onChange={e => handleSearch(e.target.value)} className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-green-500" />
      </div>

      {/* Table with aligned market columns */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-y border-gray-100">
            <tr>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide">Word / Phrase</th>
              {ALL_MARKETS.map(m => (
                <th key={m} className="px-2 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide text-center">{m}</th>
              ))}
              <th className="px-4 py-2.5 text-xs font-medium text-gray-500 uppercase tracking-wide text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {items.length === 0 && (
              <tr><td colSpan={ALL_MARKETS.length + 2} className="px-4 py-8 text-center text-gray-400">No words found.</td></tr>
            )}
            {items.map(item => (
              <tr key={item.id as string} className="hover:bg-gray-50">
                {editId === item.id ? (
                  <td colSpan={ALL_MARKETS.length + 2} className="p-4 bg-gray-50">
                    <div className="space-y-3">
                      <input type="text" value={editWord} onChange={e => setEditWord(e.target.value)} className="w-full border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-green-500" />
                      <MarketCheckboxGrid selected={editMarkets} onChange={setEditMarkets} />
                      <div className="flex gap-2">
                        <button className="btn-primary flex items-center gap-1.5 text-sm" disabled={updateMut.isPending} onClick={() => editId && updateMut.mutate({ id: editId, data: { word: editWord, markets: editMarkets } })}>
                          <Save className="w-3.5 h-3.5" /> Save
                        </button>
                        <button className="btn-secondary text-sm" onClick={() => setEditId(null)}><X className="w-3.5 h-3.5 inline mr-1" />Cancel</button>
                      </div>
                    </div>
                  </td>
                ) : (
                  <>
                    <td className="px-4 py-2.5 font-medium text-gray-800">{item.word as string}</td>
                    {ALL_MARKETS.map(m => {
                      const active = ((item.markets as string[]) ?? []).includes(m);
                      return (
                        <td key={m} className="px-2 py-2.5 text-center">
                          {active
                            ? <span className="text-green-600 font-bold text-base leading-none">✓</span>
                            : <span className="text-gray-200 text-sm">—</span>}
                        </td>
                      );
                    })}
                    <td className="px-4 py-2.5 text-right">
                      <div className="flex items-center gap-1 justify-end">
                        <button className="p-1 text-gray-400 hover:text-gray-700" onClick={() => startEdit(item)}><Edit2 className="w-3.5 h-3.5" /></button>
                        <button className="p-1 text-gray-400 hover:text-red-600" onClick={() => handleDelete(item.id as string, item.word as string)}><Trash2 className="w-3.5 h-3.5" /></button>
                      </div>
                    </td>
                  </>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100">
          <span className="text-xs text-gray-500">Page {page} of {totalPages} — {total} total</span>
          <div className="flex gap-2">
            <button className="btn-secondary text-xs" disabled={page <= 1} onClick={() => setPage(p => p - 1)}>Prev</button>
            <button className="btn-secondary text-xs" disabled={page >= totalPages} onClick={() => setPage(p => p + 1)}>Next</button>
          </div>
        </div>
      )}
    </div>
  );
}
