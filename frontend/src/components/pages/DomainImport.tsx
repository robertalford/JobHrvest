import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { api } from '../../lib/api';
import { Database, Globe, RefreshCw, CheckCircle, Building2 } from 'lucide-react';

const getImportStats = () => api.get('/domain-imports/stats/').then(r => r.data);
const triggerTranco  = () => api.post('/domain-imports/trigger/tranco/').then(r => r.data);
const triggerMajestic = () => api.post('/domain-imports/trigger/majestic/').then(r => r.data);
const triggerWikidata = (markets?: string[]) =>
  api.post('/domain-imports/trigger/wikidata/', null, {
    params: markets?.length ? { markets } : undefined,
  }).then(r => r.data);

const ALL_MARKETS = ['AU', 'NZ', 'SG', 'MY', 'HK', 'PH', 'ID', 'TH'];

const SOURCE_META: Record<string, { label: string; description: string; color: string }> = {
  csv_import:          { label: 'CSV Import',    description: 'Manual company uploads',                       color: 'bg-blue-100 text-blue-700' },
  tranco_domain_list:  { label: 'Tranco Top-1M', description: 'High-traffic sites by country TLD',           color: 'bg-green-100 text-green-700' },
  majestic_domain_list:{ label: 'Majestic Million', description: 'High-authority sites by country TLD',     color: 'bg-purple-100 text-purple-700' },
  wikidata:            { label: 'Wikidata',      description: 'Company register — includes .com domains',    color: 'bg-amber-100 text-amber-700' },
  asic_registry:       { label: 'ASIC',          description: 'Australian company register',                 color: 'bg-orange-100 text-orange-700' },
  seed:                { label: 'Seed',           description: 'System seed data',                            color: 'bg-gray-100 text-gray-600' },
};

function SourceBadge({ source }: { source: string }) {
  const m = SOURCE_META[source] ?? { label: source, description: '', color: 'bg-gray-100 text-gray-600' };
  return (
    <span className={`inline-block text-xs font-medium px-2 py-0.5 rounded-full ${m.color}`}>
      {m.label}
    </span>
  );
}

function ImportCard({
  icon: Icon,
  title,
  description,
  detail,
  onTrigger,
  isPending,
  lastResult,
}: {
  icon: React.ElementType;
  title: string;
  description: string;
  detail: string;
  onTrigger: () => void;
  isPending: boolean;
  lastResult?: { new?: number; skipped?: number } | null;
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-lg bg-gray-50 flex items-center justify-center">
            <Icon className="w-5 h-5 text-gray-500" />
          </div>
          <div>
            <div className="font-semibold text-gray-900">{title}</div>
            <div className="text-xs text-gray-500 mt-0.5">{description}</div>
          </div>
        </div>
        <button
          onClick={onTrigger}
          disabled={isPending}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-brand text-white rounded-lg hover:bg-brand/90 disabled:opacity-50 whitespace-nowrap"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${isPending ? 'animate-spin' : ''}`} />
          {isPending ? 'Queued…' : 'Run Import'}
        </button>
      </div>
      <p className="mt-3 text-xs text-gray-400">{detail}</p>
      {lastResult && (
        <div className="mt-3 flex items-center gap-2 text-xs">
          <CheckCircle className="w-3.5 h-3.5 text-green-500" />
          <span className="text-green-700 font-medium">{(lastResult.new ?? 0).toLocaleString()} new companies added</span>
          <span className="text-gray-400">· {(lastResult.skipped ?? 0).toLocaleString()} already existed</span>
        </div>
      )}
    </div>
  );
}

export function DomainImport() {
  const qc = useQueryClient();
  const [selectedMarkets, setSelectedMarkets] = useState<string[]>([]);

  const { data: stats } = useQuery({
    queryKey: ['domain-import-stats'],
    queryFn: getImportStats,
    refetchInterval: 15_000,
  });

  const tranco   = useMutation({ mutationFn: triggerTranco,   onSuccess: () => qc.invalidateQueries({ queryKey: ['domain-import-stats'] }) });
  const majestic = useMutation({ mutationFn: triggerMajestic, onSuccess: () => qc.invalidateQueries({ queryKey: ['domain-import-stats'] }) });
  const wikidata = useMutation({ mutationFn: () => triggerWikidata(selectedMarkets.length ? selectedMarkets : undefined), onSuccess: () => qc.invalidateQueries({ queryKey: ['domain-import-stats'] }) });

  function toggleMarket(m: string) {
    setSelectedMarkets(prev => prev.includes(m) ? prev.filter(x => x !== m) : [...prev, m]);
  }

  return (
    <div className="p-6 space-y-5">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-blue-50 flex items-center justify-center">
          <Database className="w-5 h-5 text-blue-600" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Bulk Domain Import</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            Seed the company database from public domain lists and company registries.
          </p>
        </div>
      </div>

      {/* Total stat */}
      <div className="bg-white rounded-lg border border-gray-200 p-4 flex items-center gap-3">
        <Building2 className="w-5 h-5 text-gray-400" />
        <span className="text-2xl font-bold text-gray-900">{(stats?.total_companies ?? 0).toLocaleString()}</span>
        <span className="text-sm text-gray-500">total companies in database</span>
      </div>

      {/* Source breakdown */}
      {stats?.by_source?.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-5 py-3 border-b border-gray-100">
            <span className="text-sm font-semibold text-gray-700">Companies by Source</span>
          </div>
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="px-4 py-2 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide">Source</th>
                <th className="px-4 py-2 text-right text-xs font-semibold text-gray-500 uppercase tracking-wide">Total</th>
                <th className="px-4 py-2 text-right text-xs font-semibold text-gray-500 uppercase tracking-wide">Pending Config</th>
                <th className="px-4 py-2 text-right text-xs font-semibold text-gray-500 uppercase tracking-wide">Configured</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {stats.by_source.map((row: any) => (
                <tr key={row.source} className="hover:bg-gray-50/50">
                  <td className="px-4 py-2.5"><SourceBadge source={row.source} /></td>
                  <td className="px-4 py-2.5 text-right font-medium text-gray-900">{row.total.toLocaleString()}</td>
                  <td className="px-4 py-2.5 text-right text-amber-600">{row.pending_config.toLocaleString()}</td>
                  <td className="px-4 py-2.5 text-right text-green-600">{row.configured.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Import cards */}
      <div className="space-y-3">
        <h2 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Available Importers</h2>

        <ImportCard
          icon={Globe}
          title="Tranco Top-1M"
          description="High-traffic domains filtered by country TLD (.com.au, .co.nz, .com.sg, etc.)"
          detail="Downloads the daily Tranco list (~1M domains) and imports all domains whose TLD matches a target market. Best for well-established, high-traffic businesses. New companies are auto-enqueued for crawling."
          onTrigger={() => tranco.mutate()}
          isPending={tranco.isPending}
          lastResult={tranco.data}
        />

        <ImportCard
          icon={Globe}
          title="Majestic Million"
          description="High-authority domains (by backlinks) filtered by country TLD"
          detail="Downloads the Majestic Million list and imports country-TLD domains. Complementary to Tranco — ranks by link authority rather than traffic, so it finds different companies."
          onTrigger={() => majestic.mutate()}
          isPending={majestic.isPending}
          lastResult={majestic.data}
        />

        {/* Wikidata — with market selector */}
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-center gap-3">
              <div className="w-9 h-9 rounded-lg bg-gray-50 flex items-center justify-center">
                <Database className="w-5 h-5 text-gray-500" />
              </div>
              <div>
                <div className="font-semibold text-gray-900">Wikidata</div>
                <div className="text-xs text-gray-500 mt-0.5">
                  Company register — captures .com and non-TLD domains that Tranco/Majestic miss
                </div>
              </div>
            </div>
            <button
              onClick={() => wikidata.mutate()}
              disabled={wikidata.isPending}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-brand text-white rounded-lg hover:bg-brand/90 disabled:opacity-50 whitespace-nowrap"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${wikidata.isPending ? 'animate-spin' : ''}`} />
              {wikidata.isPending ? 'Queued…' : 'Run Import'}
            </button>
          </div>
          <p className="mt-3 text-xs text-gray-400">
            Queries Wikidata SPARQL for organisations with official websites registered in each target country.
            Runs at ~1,000 results/request with a polite 1s delay. Up to 10,000 results per market.
            This is the best source for large companies that use .com domains.
          </p>
          {/* Market filter */}
          <div className="mt-4">
            <div className="text-xs font-medium text-gray-600 mb-2">
              Markets to import {selectedMarkets.length === 0 ? '(all)' : `(${selectedMarkets.join(', ')})`}:
            </div>
            <div className="flex flex-wrap gap-1.5">
              {ALL_MARKETS.map(m => (
                <button
                  key={m}
                  onClick={() => toggleMarket(m)}
                  className={`px-2.5 py-1 text-xs font-mono rounded border transition-colors ${
                    selectedMarkets.includes(m)
                      ? 'bg-brand text-white border-brand'
                      : 'bg-white text-gray-600 border-gray-200 hover:border-brand/50'
                  }`}
                >
                  {m}
                </button>
              ))}
              {selectedMarkets.length > 0 && (
                <button
                  onClick={() => setSelectedMarkets([])}
                  className="px-2 py-1 text-xs text-gray-400 hover:text-gray-600"
                >
                  Clear (all)
                </button>
              )}
            </div>
          </div>
          {wikidata.data && (
            <div className="mt-3 flex items-center gap-2 text-xs">
              <CheckCircle className="w-3.5 h-3.5 text-green-500" />
              <span className="text-green-700 font-medium">{(wikidata.data.new ?? 0).toLocaleString()} new companies added</span>
              <span className="text-gray-400">· {(wikidata.data.skipped ?? 0).toLocaleString()} already existed</span>
            </div>
          )}
        </div>
      </div>

      <p className="text-xs text-gray-400">
        All imports are idempotent — re-running any importer will not create duplicates (ON CONFLICT DO NOTHING by domain).
        New companies are automatically enqueued for career page discovery and crawling.
      </p>
    </div>
  );
}
