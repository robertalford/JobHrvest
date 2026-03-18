import { useQuery } from '@tanstack/react-query';
import { getSystemHealth } from '../../lib/api';
import { CheckCircle, XCircle } from 'lucide-react';

export function Settings() {
  const { data: health } = useQuery({ queryKey: ['health'], queryFn: getSystemHealth, refetchInterval: 15000 });

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Settings</h1>

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
        </div>
      </div>

      <div className="card p-5">
        <h2 className="font-semibold text-gray-900 mb-4">Manual Controls</h2>
        <div className="space-y-3">
          <button
            onClick={() => fetch('/api/v1/system/retrain-classifier', { method: 'POST' }).then(() => alert('Retraining queued'))}
            className="btn-secondary w-full text-left"
          >
            Retrain Page Classifier
          </button>
          <button
            onClick={() => fetch('/api/v1/system/rebuild-templates', { method: 'POST' }).then(() => alert('Template rebuild queued'))}
            className="btn-secondary w-full text-left"
          >
            Rebuild Extraction Templates
          </button>
        </div>
      </div>

      <div className="card p-5">
        <h2 className="font-semibold text-gray-900 mb-2">About</h2>
        <div className="text-sm text-gray-500 space-y-1">
          <div>Version: 0.1.0 — Phase 1</div>
          <div>Market: Australia (AU)</div>
          <div>Blocked domains: SEEK, Jora, Jobstreet, JobsDB</div>
          <div>Link discovery sources: Indeed AU, LinkedIn, Glassdoor AU, CareerOne, Adzuna AU</div>
        </div>
      </div>
    </div>
  );
}
