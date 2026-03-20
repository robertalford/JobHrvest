import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Globe2, Save } from 'lucide-react';
import { getSetting, updateSetting } from '../../lib/api';
import { ALL_MARKETS, MARKET_NAMES } from './settings-shared';

export function Markets() {
  const qc = useQueryClient();
  const { data: marketsData } = useQuery({ queryKey: ['settings', 'markets'], queryFn: () => getSetting('markets') });
  const [marketToggles, setMarketToggles] = useState<Record<string, boolean> | null>(null);
  const marketsMut = useMutation({
    mutationFn: (value: unknown) => updateSetting('markets', value),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['settings', 'markets'] }); },
  });
  const effectiveMarkets: Record<string, boolean> = marketToggles ?? (marketsData?.markets ?? {});
  const toggleMarket = (code: string) =>
    setMarketToggles(prev => ({ ...(prev ?? marketsData?.markets ?? {}), [code]: !(prev ?? marketsData?.markets ?? {})[code] }));
  const saveMarkets = () => marketsMut.mutate({ markets: effectiveMarkets });

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-blue-100 flex items-center justify-center">
          <Globe2 className="w-5 h-5 text-blue-600" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Markets</h1>
          <p className="text-sm text-gray-500">Enable or disable crawling markets</p>
        </div>
      </div>
      <div className="card p-5">
        <div className="flex items-center justify-between mb-4">
          <h2 className="font-semibold text-gray-900">Active Markets</h2>
          <button className="btn-primary flex items-center gap-1.5 text-sm" onClick={saveMarkets} disabled={marketsMut.isPending}>
            <Save className="w-3.5 h-3.5" /> Save
          </button>
        </div>
        <div className="space-y-2">
          {ALL_MARKETS.map(code => {
            const active = !!effectiveMarkets[code];
            return (
              <div key={code} className="flex items-center justify-between py-2 border-b border-gray-100 last:border-0">
                <div className="flex items-center gap-3">
                  <span className="font-mono text-sm font-semibold text-gray-700 w-8">{code}</span>
                  <span className="text-sm text-gray-600">{MARKET_NAMES[code]}</span>
                </div>
                <div className="flex items-center gap-3">
                  <span className={active ? 'badge-green text-xs' : 'badge-gray text-xs'}>{active ? 'Active' : 'Inactive'}</span>
                  <button className="btn-secondary text-xs" onClick={() => toggleMarket(code)}>Toggle</button>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
