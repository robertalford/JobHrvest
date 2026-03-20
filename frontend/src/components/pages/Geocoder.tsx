import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { MapPin, Database, Search, Zap, Globe, Map, Building, Home, RefreshCw, ChevronLeft, ChevronRight } from 'lucide-react';
import { api } from '../../lib/api';

// ── API helpers ───────────────────────────────────────────────────────────────
const ALL_MARKETS = ['AU', 'NZ', 'SG', 'MY', 'HK', 'PH', 'ID', 'TH'];

const getGeoStats       = () => api.get('/geocoder/stats/').then(r => r.data);
const getGeoLocations   = (p: Record<string, unknown>) => api.get('/geocoder/', { params: p }).then(r => r.data);
const getGeoCache       = (p: Record<string, unknown>) => api.get('/geocoder/cache/', { params: p }).then(r => r.data);
const testGeocode       = (text: string, market_code: string) =>
  api.post('/geocoder/test/', { text, market_code }).then(r => r.data);
const triggerSeed       = (countries?: string[]) =>
  api.post('/geocoder/seed/', null, { params: countries?.length ? { countries } : undefined }).then(r => r.data);
const triggerRetro      = (retry_failed: boolean) =>
  api.post('/geocoder/retro/', null, { params: { retry_failed } }).then(r => r.data);

// ── level config ──────────────────────────────────────────────────────────────
const LEVEL_LABEL: Record<number, string> = { 1: 'Country', 2: 'Region', 3: 'City', 4: 'Suburb' };
const LEVEL_ICON: Record<number, React.ElementType> = { 1: Globe, 2: Map, 3: Building, 4: Home };
const LEVEL_COLOR: Record<number, string> = {
  1: 'bg-blue-100 text-blue-700',
  2: 'bg-purple-100 text-purple-700',
  3: 'bg-green-100 text-green-700',
  4: 'bg-orange-100 text-orange-700',
};
const METHOD_COLOR: Record<string, string> = {
  exact: 'text-green-600', fuzzy: 'text-blue-600',
  llm: 'text-purple-600', llm_fuzzy: 'text-purple-500',
  unresolved: 'text-red-400', cached: 'text-gray-400',
};

// ── sub-components ────────────────────────────────────────────────────────────
function StatCard({ icon: Icon, label, value, sub }: {
  icon: React.ElementType; label: string; value: number | string; sub?: string;
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center gap-2 mb-2">
        <Icon className="w-4 h-4 text-gray-400" />
        <span className="text-xs text-gray-500 font-medium uppercase tracking-wide">{label}</span>
      </div>
      <div className="text-2xl font-bold text-gray-900">{typeof value === 'number' ? value.toLocaleString() : value}</div>
      {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
    </div>
  );
}

function LevelBadge({ level }: { level: number }) {
  const Icon = LEVEL_ICON[level] || MapPin;
  return (
    <span className={`inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded-full ${LEVEL_COLOR[level] || 'bg-gray-100 text-gray-600'}`}>
      <Icon className="w-3 h-3" />{LEVEL_LABEL[level] || `L${level}`}
    </span>
  );
}

function Pagination({ page, total, pageSize, onChange }: {
  page: number; total: number; pageSize: number; onChange: (p: number) => void;
}) {
  const pages = Math.ceil(total / pageSize);
  return (
    <div className="flex items-center justify-between px-4 py-2 border-t border-gray-100 text-sm text-gray-500">
      <span>{total.toLocaleString()} total</span>
      <div className="flex items-center gap-2">
        <button onClick={() => onChange(page - 1)} disabled={page <= 1}
          className="p-1 rounded hover:bg-gray-100 disabled:opacity-40"><ChevronLeft className="w-4 h-4" /></button>
        <span>Page {page} of {pages || 1}</span>
        <button onClick={() => onChange(page + 1)} disabled={page >= pages}
          className="p-1 rounded hover:bg-gray-100 disabled:opacity-40"><ChevronRight className="w-4 h-4" /></button>
      </div>
    </div>
  );
}

// ── tabs ──────────────────────────────────────────────────────────────────────
type Tab = 'locations' | 'cache' | 'test';

// ── LocationsTab ──────────────────────────────────────────────────────────────
function LocationsTab() {
  const [search, setSearch] = useState('');
  const [level, setLevel] = useState('');
  const [market, setMarket] = useState('');
  const [page, setPage] = useState(1);

  const { data, isLoading } = useQuery({
    queryKey: ['geo-locations', search, level, market, page],
    queryFn: () => getGeoLocations({
      search: search || undefined,
      level: level || undefined,
      market_code: market || undefined,
      page, page_size: 50,
    }),
  });

  return (
    <div>
      {/* Filter bar */}
      <div className="flex items-center gap-3 p-4 border-b border-gray-100">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
          <input
            value={search} onChange={e => { setSearch(e.target.value); setPage(1); }}
            placeholder="Search locations…"
            className="w-full pl-8 pr-3 py-1.5 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-brand"
          />
        </div>
        <select value={level} onChange={e => { setLevel(e.target.value); setPage(1); }}
          className="text-sm border border-gray-200 rounded-md px-3 py-1.5 focus:outline-none focus:ring-1 focus:ring-brand">
          <option value="">All levels</option>
          {[1,2,3,4].map(l => <option key={l} value={l}>{LEVEL_LABEL[l]}</option>)}
        </select>
        <select value={market} onChange={e => { setMarket(e.target.value); setPage(1); }}
          className="text-sm border border-gray-200 rounded-md px-3 py-1.5 focus:outline-none focus:ring-1 focus:ring-brand">
          <option value="">All markets</option>
          {['AU','NZ','SG','MY','HK','PH','ID','TH'].map(m => <option key={m} value={m}>{m}</option>)}
        </select>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Level</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Name</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Full Path</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Market</th>
              <th className="px-4 py-2 text-right font-medium text-gray-500">Population</th>
              <th className="px-4 py-2 text-center font-medium text-gray-500">Coordinates</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {isLoading ? (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
            ) : (data?.items || []).map((loc: any) => (
              <tr key={loc.id} className="hover:bg-gray-50/50">
                <td className="px-4 py-2.5"><LevelBadge level={loc.level} /></td>
                <td className="px-4 py-2.5">
                  <div className="font-medium text-gray-900">{loc.name}</div>
                  {loc.ascii_name && loc.ascii_name !== loc.name && (
                    <div className="text-xs text-gray-400">{loc.ascii_name}</div>
                  )}
                </td>
                <td className="px-4 py-2.5 text-gray-500 max-w-xs truncate">{loc.full_path}</td>
                <td className="px-4 py-2.5">
                  <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded font-mono">{loc.market_code}</span>
                </td>
                <td className="px-4 py-2.5 text-right text-gray-500">
                  {loc.population ? loc.population.toLocaleString() : '—'}
                </td>
                <td className="px-4 py-2.5 text-center text-xs text-gray-400 font-mono">
                  {loc.lat && loc.lng ? `${Number(loc.lat).toFixed(4)}, ${Number(loc.lng).toFixed(4)}` : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data && <Pagination page={page} total={data.total} pageSize={50} onChange={setPage} />}
    </div>
  );
}

// ── CacheTab ──────────────────────────────────────────────────────────────────
function CacheTab() {
  const [method, setMethod] = useState('');
  const [market, setMarket] = useState('');
  const [page, setPage] = useState(1);

  const { data, isLoading } = useQuery({
    queryKey: ['geo-cache', method, market, page],
    queryFn: () => getGeoCache({
      method: method || undefined,
      market_code: market || undefined,
      page, page_size: 50,
    }),
  });

  return (
    <div>
      <div className="flex items-center gap-3 p-4 border-b border-gray-100">
        <select value={method} onChange={e => { setMethod(e.target.value); setPage(1); }}
          className="text-sm border border-gray-200 rounded-md px-3 py-1.5 focus:outline-none focus:ring-1 focus:ring-brand">
          <option value="">All methods</option>
          {['exact','fuzzy','llm','llm_fuzzy','unresolved'].map(m => (
            <option key={m} value={m}>{m}</option>
          ))}
        </select>
        <select value={market} onChange={e => { setMarket(e.target.value); setPage(1); }}
          className="text-sm border border-gray-200 rounded-md px-3 py-1.5 focus:outline-none focus:ring-1 focus:ring-brand">
          <option value="">All markets</option>
          {['AU','NZ','SG','MY','HK','PH','ID','TH'].map(m => <option key={m} value={m}>{m}</option>)}
        </select>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Raw Text</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Resolved To</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Method</th>
              <th className="px-4 py-2 text-right font-medium text-gray-500">Confidence</th>
              <th className="px-4 py-2 text-right font-medium text-gray-500">Uses</th>
              <th className="px-4 py-2 text-left font-medium text-gray-500">Market</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {isLoading ? (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
            ) : (data?.items || []).map((entry: any) => (
              <tr key={entry.id} className="hover:bg-gray-50/50">
                <td className="px-4 py-2.5">
                  <span className="font-mono text-gray-700 text-xs bg-gray-50 px-1.5 py-0.5 rounded">{entry.raw_text}</span>
                </td>
                <td className="px-4 py-2.5">
                  {entry.resolved_name ? (
                    <div>
                      <div className="font-medium text-gray-900">{entry.resolved_name}</div>
                      <div className="text-xs text-gray-400 truncate max-w-xs">{entry.resolved_path}</div>
                    </div>
                  ) : (
                    <span className="text-red-400 text-xs italic">unresolvable</span>
                  )}
                </td>
                <td className="px-4 py-2.5">
                  <span className={`text-xs font-medium ${METHOD_COLOR[entry.resolution_method] || 'text-gray-500'}`}>
                    {entry.resolution_method}
                  </span>
                </td>
                <td className="px-4 py-2.5 text-right text-gray-500">
                  {entry.confidence != null ? `${(entry.confidence * 100).toFixed(0)}%` : '—'}
                </td>
                <td className="px-4 py-2.5 text-right text-gray-500">{entry.use_count}</td>
                <td className="px-4 py-2.5">
                  <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded font-mono">{entry.market_code || '—'}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data && <Pagination page={page} total={data.total} pageSize={50} onChange={setPage} />}
    </div>
  );
}

// ── TestTab ───────────────────────────────────────────────────────────────────
function TestTab() {
  const [text, setText] = useState('');
  const [market, setMarket] = useState('AU');

  const { mutate, data: result, isPending, reset } = useMutation({
    mutationFn: () => testGeocode(text, market),
  });

  return (
    <div className="p-6 max-w-lg">
      <p className="text-sm text-gray-500 mb-4">
        Test the geocoder resolver against a raw location string to see how it would be resolved.
      </p>
      <div className="flex gap-2 mb-4">
        <input
          value={text}
          onChange={e => { setText(e.target.value); reset(); }}
          placeholder="e.g. Surry Hills, NSW or Kuala Lumpur"
          className="flex-1 px-3 py-2 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-brand"
        />
        <select value={market} onChange={e => setMarket(e.target.value)}
          className="text-sm border border-gray-200 rounded-md px-3 py-2 focus:outline-none focus:ring-1 focus:ring-brand">
          {['AU','NZ','SG','MY','HK','PH','ID','TH'].map(m => <option key={m} value={m}>{m}</option>)}
        </select>
        <button
          onClick={() => mutate()}
          disabled={!text.trim() || isPending}
          className="px-4 py-2 bg-brand text-white text-sm rounded-md hover:bg-brand/90 disabled:opacity-50 flex items-center gap-1.5"
        >
          <Zap className="w-3.5 h-3.5" />
          {isPending ? 'Testing…' : 'Test'}
        </button>
      </div>

      {result && (
        <div className={`rounded-lg border p-4 ${result.resolved ? 'border-green-200 bg-green-50' : 'border-red-200 bg-red-50'}`}>
          {result.resolved ? (
            <>
              <div className="flex items-center gap-2 mb-3">
                <MapPin className="w-4 h-4 text-green-600" />
                <span className="font-semibold text-green-800">Resolved</span>
                <LevelBadge level={result.level} />
                <span className={`text-xs font-medium ml-auto ${METHOD_COLOR[result.method] || 'text-gray-500'}`}>
                  via {result.method}
                </span>
              </div>
              <div className="space-y-1.5 text-sm">
                <div><span className="text-gray-500 w-24 inline-block">Name:</span><span className="font-medium">{result.name}</span></div>
                <div><span className="text-gray-500 w-24 inline-block">Full path:</span><span>{result.full_path}</span></div>
                <div><span className="text-gray-500 w-24 inline-block">Confidence:</span><span>{(result.confidence * 100).toFixed(1)}%</span></div>
                {result.lat && (
                  <div><span className="text-gray-500 w-24 inline-block">Coordinates:</span>
                    <span className="font-mono text-xs">{result.lat.toFixed(5)}, {result.lng.toFixed(5)}</span>
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="flex items-center gap-2 text-red-700">
              <MapPin className="w-4 h-4" />
              <span className="font-semibold">Could not resolve</span>
              <span className="text-sm text-red-500 ml-1">— no matching location found</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── main page ─────────────────────────────────────────────────────────────────
export function Geocoder() {
  const [tab, setTab] = useState<Tab>('locations');

  const { data: stats } = useQuery({
    queryKey: ['geo-stats'],
    queryFn: getGeoStats,
    refetchInterval: 30_000,
  });

  const seedMutation = useMutation({ mutationFn: (countries?: string[]) => triggerSeed(countries) });
  const retroMutation = useMutation({ mutationFn: () => triggerRetro(true) });

  const seededMarkets = Object.keys(stats?.locations?.by_market ?? {});
  const missingMarkets = ALL_MARKETS.filter(m => !seededMarkets.includes(m));

  const TABS: { id: Tab; label: string }[] = [
    { id: 'locations', label: 'Locations' },
    { id: 'cache',     label: 'Resolution Cache' },
    { id: 'test',      label: 'Test Geocoder' },
  ];

  return (
    <div className="p-6 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-green-50 flex items-center justify-center">
            <MapPin className="w-5 h-5 text-green-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Geocoder</h1>
            <p className="text-sm text-gray-500 mt-0.5">
              Hierarchical location database seeded from GeoNames for all 8 supported markets.
            </p>
          </div>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => retroMutation.mutate()}
            disabled={retroMutation.isPending}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-600 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
          >
            <RefreshCw className={`w-4 h-4 ${retroMutation.isPending ? 'animate-spin' : ''}`} />
            Retro Geocode Jobs
          </button>
          <button
            onClick={() => seedMutation.mutate(undefined)}
            disabled={seedMutation.isPending}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium bg-brand text-white rounded-lg hover:bg-brand/90 disabled:opacity-50 transition-colors"
          >
            <Database className={`w-4 h-4 ${seedMutation.isPending ? 'animate-spin' : ''}`} />
            {seedMutation.isPending ? 'Seeding…' : 'Seed GeoNames'}
          </button>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
        <StatCard icon={Globe}    label="Countries" value={stats?.locations?.by_level?.countries ?? '—'} />
        <StatCard icon={Map}      label="Regions"   value={stats?.locations?.by_level?.regions   ?? '—'} />
        <StatCard icon={Building} label="Cities"    value={stats?.locations?.by_level?.cities    ?? '—'} />
        <StatCard icon={Home}     label="Suburbs"   value={stats?.locations?.by_level?.suburbs   ?? '—'} />
        <StatCard icon={Database} label="Cache Entries" value={stats?.cache?.total ?? '—'}
          sub={`${stats?.cache?.total_lookups?.toLocaleString() ?? 0} lookups`} />
        <StatCard icon={MapPin}   label="Jobs Geocoded" value={stats?.jobs?.resolved ?? '—'}
          sub={`${stats?.jobs?.pending ?? 0} pending`} />
      </div>

      {/* Missing markets warning */}
      {missingMarkets.length > 0 && (
        <div className="flex items-center justify-between bg-amber-50 border border-amber-200 rounded-lg px-4 py-3">
          <div className="flex items-center gap-2 text-sm text-amber-800">
            <span className="font-medium">Missing markets:</span>
            {missingMarkets.map(m => (
              <span key={m} className="font-mono bg-amber-100 px-1.5 py-0.5 rounded text-xs">{m}</span>
            ))}
            <span className="text-amber-600">— seed failed or not yet run</span>
          </div>
          <button
            onClick={() => seedMutation.mutate(missingMarkets)}
            disabled={seedMutation.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-amber-600 text-white rounded-md hover:bg-amber-700 disabled:opacity-50"
          >
            <Database className="w-3 h-3" />
            Reseed {missingMarkets.join(', ')}
          </button>
        </div>
      )}

      {/* Job geocoding progress */}
      {stats?.jobs && (
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-700">Job Geocoding Progress</span>
            <span className="text-xs text-gray-400">
              {stats.jobs.resolved} resolved · {stats.jobs.failed} failed · {stats.jobs.pending} pending
            </span>
          </div>
          {(() => {
            const total = (stats.jobs.resolved + stats.jobs.failed + stats.jobs.pending) || 1;
            const resolvedPct = (stats.jobs.resolved / total) * 100;
            const failedPct = (stats.jobs.failed / total) * 100;
            return (
              <div className="h-2 bg-gray-100 rounded-full overflow-hidden flex">
                <div className="bg-green-500 h-full transition-all" style={{ width: `${resolvedPct}%` }} />
                <div className="bg-red-300 h-full transition-all" style={{ width: `${failedPct}%` }} />
              </div>
            );
          })()}
        </div>
      )}

      {/* Tabs + content */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="flex border-b border-gray-100">
          {TABS.map(t => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                tab === t.id
                  ? 'border-brand text-brand'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        {tab === 'locations' && <LocationsTab />}
        {tab === 'cache'     && <CacheTab />}
        {tab === 'test'      && <TestTab />}
      </div>
    </div>
  );
}
