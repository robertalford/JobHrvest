import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Beaker, Lock, CheckCircle2, AlertCircle, Search, ExternalLink, RefreshCcw } from 'lucide-react';
import {
  getGoldHoldoutSets,
  getGoldHoldoutDomains,
  type GoldHoldoutSet,
} from '../../lib/api';

function fmt(n: number | null | undefined): string {
  if (n == null) return '—';
  return n.toLocaleString();
}

function fmtDate(iso: string | null): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

export function TestData() {
  const setsQuery = useQuery({
    queryKey: ['gold-holdout', 'sets'],
    queryFn: getGoldHoldoutSets,
    refetchInterval: 30_000,
  });

  const sets = setsQuery.data?.sets ?? [];
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const selected = useMemo<GoldHoldoutSet | undefined>(
    () => sets.find(s => s.id === (selectedId ?? sets[0]?.id)),
    [sets, selectedId],
  );

  const [search, setSearch] = useState('');
  const [page, setPage] = useState(1);
  const pageSize = 50;

  const domainsQuery = useQuery({
    queryKey: ['gold-holdout', 'domains', selected?.id, page, search],
    queryFn: () =>
      getGoldHoldoutDomains(selected!.id, { page, page_size: pageSize, search: search || undefined }),
    enabled: !!selected,
  });

  return (
    <div className="max-w-7xl mx-auto p-8">
      <div className="mb-6 flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900 flex items-center gap-2">
            <Beaker className="w-6 h-6 text-brand" /> Test Data
          </h1>
          <p className="mt-1 text-sm text-gray-600">
            Frozen GOLD holdout sets used to evaluate the site-config champion and challenger models —
            one row per company career domain, stratified by market and ATS.
          </p>
        </div>
        <button
          onClick={() => setsQuery.refetch()}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs border border-gray-200 rounded-md hover:bg-gray-50"
        >
          <RefreshCcw className="w-3.5 h-3.5" /> Refresh
        </button>
      </div>

      {setsQuery.isLoading ? (
        <div className="bg-white border border-gray-200 rounded-lg p-8 text-center text-sm text-gray-500">
          Loading holdout sets…
        </div>
      ) : sets.length === 0 ? (
        <div className="bg-white border border-gray-200 rounded-lg p-8 text-center text-sm text-gray-500">
          No holdout sets yet. Seed one with:
          <pre className="mt-3 inline-block text-left px-3 py-2 bg-gray-50 border border-gray-200 rounded text-xs">
            docker exec -w /app jobharvest-api python -m scripts.build_gold_holdout \{'\n'}
            {'  '}--name au_baseline_v1 --market AU --max-domains 100
          </pre>
        </div>
      ) : (
        <>
          {/* Set summary cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
            {sets.map(s => (
              <button
                key={s.id}
                onClick={() => { setSelectedId(s.id); setPage(1); }}
                className={`text-left bg-white border rounded-lg p-4 transition hover:border-brand/60 hover:shadow-sm ${
                  selected?.id === s.id ? 'border-brand ring-1 ring-brand/30' : 'border-gray-200'
                }`}
              >
                <div className="flex items-start justify-between gap-2 mb-2">
                  <div className="font-medium text-gray-900 flex items-center gap-2">
                    {s.name}
                    {s.is_frozen && (
                      <span className="inline-flex items-center gap-1 text-[10px] uppercase tracking-wide text-gray-500">
                        <Lock className="w-3 h-3" /> Frozen
                      </span>
                    )}
                  </div>
                  <span className="text-xs text-gray-500">{s.market_id ?? '—'}</span>
                </div>
                {s.description && (
                  <p className="text-xs text-gray-500 mb-3 line-clamp-2">{s.description}</p>
                )}
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <div className="text-gray-400">Domains</div>
                    <div className="font-semibold text-gray-800">{fmt(s.stats.domains)}</div>
                  </div>
                  <div>
                    <div className="text-gray-400">Snapshots</div>
                    <div className="font-semibold text-gray-800">{fmt(s.stats.snapshots)}</div>
                  </div>
                  <div>
                    <div className="text-gray-400">Verified</div>
                    <div className={`font-semibold ${s.stats.verified_domains > 0 ? 'text-green-700' : 'text-amber-600'}`}>
                      {fmt(s.stats.verified_domains)} / {fmt(s.stats.domains)}
                    </div>
                  </div>
                  <div>
                    <div className="text-gray-400">Ground-truth jobs</div>
                    <div className="font-semibold text-gray-800">{fmt(s.stats.ground_truth_jobs)}</div>
                  </div>
                </div>
                <div className="mt-3 text-[11px] text-gray-400">
                  Frozen {fmtDate(s.frozen_at)} · source {s.source}
                </div>
              </button>
            ))}
          </div>

          {/* Verification readiness banner */}
          {selected && selected.stats.verified_domains < selected.stats.domains && (
            <div className="mb-4 flex items-start gap-2 px-4 py-3 bg-amber-50 border border-amber-200 rounded-md text-sm text-amber-800">
              <AlertCircle className="w-4 h-4 mt-0.5 flex-shrink-0" />
              <div>
                <div className="font-medium">Manual verification pending</div>
                <div className="text-xs">
                  {selected.stats.verified_domains} of {selected.stats.domains} domains have been human-verified.
                  Classifier-level metrics (precision/recall/F1) work without this, but extraction-accuracy
                  evaluation (against the ground-truth job list) requires verified domains.
                </div>
              </div>
            </div>
          )}

          {/* Domain table */}
          {selected && (
            <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
              <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between gap-3">
                <div className="text-sm font-medium text-gray-700">
                  {selected.name} — {fmt(domainsQuery.data?.total ?? 0)} domains
                </div>
                <div className="relative">
                  <Search className="w-4 h-4 absolute left-2 top-1/2 -translate-y-1/2 text-gray-400" />
                  <input
                    type="search"
                    value={search}
                    onChange={e => { setSearch(e.target.value); setPage(1); }}
                    placeholder="Filter domain or advertiser…"
                    className="pl-7 pr-3 py-1.5 text-xs border border-gray-200 rounded-md focus:outline-none focus:border-brand"
                  />
                </div>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 text-xs uppercase tracking-wide text-gray-500">
                    <tr>
                      <th className="px-4 py-2 text-left">Domain</th>
                      <th className="px-4 py-2 text-left">Advertiser</th>
                      <th className="px-4 py-2 text-right">Expected jobs</th>
                      <th className="px-4 py-2 text-right">Snapshot</th>
                      <th className="px-4 py-2 text-right">Ground truth</th>
                      <th className="px-4 py-2 text-left">Verification</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {domainsQuery.isLoading ? (
                      <tr>
                        <td colSpan={6} className="px-4 py-6 text-center text-xs text-gray-500">
                          Loading…
                        </td>
                      </tr>
                    ) : (
                      (domainsQuery.data?.items ?? []).map(d => (
                        <tr key={d.id} className="hover:bg-gray-50">
                          <td className="px-4 py-2">
                            <a
                              href={`https://${d.domain}`}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-brand hover:underline inline-flex items-center gap-1"
                            >
                              {d.domain} <ExternalLink className="w-3 h-3" />
                            </a>
                          </td>
                          <td className="px-4 py-2 text-gray-700">{d.advertiser_name ?? '—'}</td>
                          <td className="px-4 py-2 text-right text-gray-700">{fmt(d.expected_job_count)}</td>
                          <td className="px-4 py-2 text-right">
                            {d.snapshot_count > 0 ? (
                              <span className="inline-flex items-center gap-1 text-green-700 text-xs">
                                <CheckCircle2 className="w-3.5 h-3.5" />
                                {d.snapshot_count}
                              </span>
                            ) : (
                              <span className="text-gray-400 text-xs">—</span>
                            )}
                          </td>
                          <td className="px-4 py-2 text-right text-gray-700">{d.ground_truth_job_count}</td>
                          <td className="px-4 py-2">
                            {d.verification_status === 'verified' ? (
                              <span className="inline-flex items-center gap-1 px-2 py-0.5 text-[11px] rounded bg-green-50 text-green-700">
                                <CheckCircle2 className="w-3 h-3" /> Verified
                              </span>
                            ) : (
                              <span className="inline-flex items-center gap-1 px-2 py-0.5 text-[11px] rounded bg-gray-100 text-gray-500">
                                Unverified
                              </span>
                            )}
                          </td>
                        </tr>
                      ))
                    )}
                    {!domainsQuery.isLoading && (domainsQuery.data?.items ?? []).length === 0 && (
                      <tr>
                        <td colSpan={6} className="px-4 py-6 text-center text-xs text-gray-500">
                          No domains match the current filter.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
              {/* Pagination */}
              {domainsQuery.data && domainsQuery.data.total > pageSize && (
                <div className="px-4 py-3 border-t border-gray-200 flex items-center justify-between text-xs text-gray-500">
                  <div>
                    Page {domainsQuery.data.page} of {Math.ceil(domainsQuery.data.total / pageSize)}
                  </div>
                  <div className="flex gap-2">
                    <button
                      disabled={page <= 1}
                      onClick={() => setPage(p => Math.max(1, p - 1))}
                      className="px-3 py-1 border border-gray-200 rounded disabled:opacity-40 hover:bg-gray-50"
                    >
                      Previous
                    </button>
                    <button
                      disabled={page * pageSize >= domainsQuery.data.total}
                      onClick={() => setPage(p => p + 1)}
                      className="px-3 py-1 border border-gray-200 rounded disabled:opacity-40 hover:bg-gray-50"
                    >
                      Next
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
