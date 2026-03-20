import { useQuery } from '@tanstack/react-query';
import { CheckCircle, XCircle, Settings2 } from 'lucide-react';
import { getSystemHealth } from '../../lib/api';

export function Settings() {
  const { data: health } = useQuery({ queryKey: ['health'], queryFn: getSystemHealth, refetchInterval: 15000 });

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-gray-100 flex items-center justify-center">
          <Settings2 className="w-5 h-5 text-gray-600" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">System Health</h1>
          <p className="text-sm text-gray-500">System status and configuration</p>
        </div>
      </div>

      <div className="card p-5">
        <h2 className="font-semibold text-gray-900 mb-4">System Status</h2>
        <div className="space-y-3">
          {health?.services && Object.entries(health.services).map(([svc, status]) => (
            <div key={svc} className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                {status === 'ok'
                  ? <CheckCircle className="w-5 h-5 text-green-500" />
                  : <XCircle className="w-5 h-5 text-red-500" />
                }
                <span className="font-medium text-gray-800 capitalize">{svc}</span>
              </div>
              <span className={`text-sm ${status === 'ok' ? 'text-green-600' : 'text-red-600'}`}>
                {String(status)}
              </span>
            </div>
          ))}
          {!health?.services && <p className="text-sm text-gray-400">Loading…</p>}
        </div>
      </div>
    </div>
  );
}
