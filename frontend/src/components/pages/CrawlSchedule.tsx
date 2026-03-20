import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getScheduleSettings, updateScheduleSettings, triggerRun } from '../../lib/api';
import { Clock, Edit2, Save, X, Play, Loader2, Search, Building2, Globe, Briefcase } from 'lucide-react';

interface RunConfig {
  enabled: boolean;
  interval_hours: number;
}

interface ScheduleSettings {
  discovery: RunConfig;
  company_config: RunConfig;
  site_config: RunConfig;
  job_crawling: RunConfig;
}

const RUN_META = {
  discovery: {
    label: 'Discovery Runs',
    desc: 'Harvest new company career page links from aggregators (Indeed AU, LinkedIn)',
    icon: Search,
    color: '#f59e0b',
    runType: 'discovery',
  },
  company_config: {
    label: 'Company Config Runs',
    desc: 'Identify or re-identify career page URLs for non-OK companies',
    icon: Building2,
    color: '#8b5cf6',
    runType: 'company_config',
  },
  site_config: {
    label: 'Site Config Runs',
    desc: 'Map job listing structure on career pages with non-OK status',
    icon: Globe,
    color: '#0ea5e9',
    runType: 'site_config',
  },
  job_crawling: {
    label: 'Job Crawling Runs',
    desc: 'Crawl company career pages and extract job listings',
    icon: Briefcase,
    color: '#0e8136',
    runType: 'job_crawling',
  },
} as const;

type RunKey = keyof typeof RUN_META;

function nextRunLabel(intervalHours: number): string {
  const now = new Date();
  const next = new Date(now.getTime() + intervalHours * 60 * 60 * 1000);
  return next.toLocaleString('en-AU', { hour: '2-digit', minute: '2-digit', day: 'numeric', month: 'short' });
}

function RunCard({
  runKey,
  config,
  onSave,
  isSaving,
}: {
  runKey: RunKey;
  config: RunConfig;
  onSave: (key: RunKey, val: RunConfig) => void;
  isSaving: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<RunConfig>(config);
  const [triggering, setTriggering] = useState(false);
  const meta = RUN_META[runKey];
  const Icon = meta.icon;

  useEffect(() => { setDraft(config); }, [config]);

  const handleSave = () => {
    onSave(runKey, draft);
    setEditing(false);
  };

  const handleTrigger = async () => {
    setTriggering(true);
    try {
      await triggerRun(meta.runType);
    } finally {
      setTriggering(false);
    }
  };

  return (
    <div className="card p-5 space-y-4">
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl flex items-center justify-center flex-shrink-0"
            style={{ backgroundColor: meta.color + '18' }}>
            <Icon className="w-5 h-5" style={{ color: meta.color }} />
          </div>
          <div>
            <div className="font-semibold text-gray-900">{meta.label}</div>
            <div className="text-xs text-gray-400 mt-0.5">{meta.desc}</div>
          </div>
        </div>
        <button
          onClick={handleTrigger}
          disabled={triggering}
          className="btn-secondary flex items-center gap-1.5 text-xs flex-shrink-0"
        >
          {triggering ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
          Run Now
        </button>
      </div>

      {/* Settings */}
      {editing ? (
        <div className="bg-gray-50 rounded-lg p-4 space-y-3">
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={draft.enabled}
              onChange={e => setDraft(d => ({ ...d, enabled: e.target.checked }))}
              className="rounded"
            />
            <span className="text-sm font-medium text-gray-700">Scheduler enabled</span>
          </label>
          <label className="block">
            <span className="block text-sm font-medium text-gray-700 mb-1">Hourly interval</span>
            <div className="flex items-center gap-2">
              <input
                type="number"
                min={1}
                max={168}
                value={draft.interval_hours}
                onChange={e => setDraft(d => ({ ...d, interval_hours: Number(e.target.value) }))}
                className="w-24 border border-gray-300 rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-green-500"
              />
              <span className="text-sm text-gray-500">hours</span>
            </div>
          </label>
          <div className="flex gap-2">
            <button
              className="btn-primary flex items-center gap-1.5 text-sm"
              disabled={isSaving}
              onClick={handleSave}
            >
              <Save className="w-3.5 h-3.5" /> Save
            </button>
            <button className="btn-secondary text-sm" onClick={() => { setEditing(false); setDraft(config); }}>
              <X className="w-3.5 h-3.5 inline mr-1" />Cancel
            </button>
          </div>
        </div>
      ) : (
        <div className="flex items-center justify-between bg-gray-50 rounded-lg px-4 py-3">
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <span className={`inline-flex items-center gap-1 text-xs font-semibold px-2 py-0.5 rounded-full ${config.enabled ? 'bg-green-100 text-green-700' : 'bg-gray-200 text-gray-500'}`}>
                {config.enabled ? '● Enabled' : '○ Disabled'}
              </span>
              <span className="text-sm text-gray-600">every <strong>{config.interval_hours}h</strong></span>
            </div>
            {config.enabled && (
              <div className="flex items-center gap-1 text-xs text-gray-400">
                <Clock className="w-3 h-3" />
                Next run approx. {nextRunLabel(config.interval_hours)}
              </div>
            )}
          </div>
          <button
            onClick={() => setEditing(true)}
            className="text-gray-400 hover:text-gray-700 p-1"
            title="Edit schedule"
          >
            <Edit2 className="w-4 h-4" />
          </button>
        </div>
      )}
    </div>
  );
}

export function CrawlSchedule() {
  const qc = useQueryClient();

  const { data: rawSettings } = useQuery<ScheduleSettings>({
    queryKey: ['schedule-settings'],
    queryFn: getScheduleSettings,
  });

  const [localSettings, setLocalSettings] = useState<ScheduleSettings | null>(null);
  useEffect(() => { if (rawSettings) setLocalSettings(rawSettings); }, [rawSettings]);

  const eff: ScheduleSettings = localSettings ?? rawSettings ?? {
    discovery: { enabled: true, interval_hours: 2 },
    company_config: { enabled: true, interval_hours: 2 },
    site_config: { enabled: true, interval_hours: 2 },
    job_crawling: { enabled: true, interval_hours: 2 },
  };

  const saveMut = useMutation({
    mutationFn: (s: ScheduleSettings) => updateScheduleSettings(s as unknown as Record<string, unknown>),
    onSuccess: (data) => {
      qc.invalidateQueries({ queryKey: ['schedule-settings'] });
      setLocalSettings(data as ScheduleSettings);
    },
  });

  const handleSave = (key: RunKey, val: RunConfig) => {
    const next = { ...eff, [key]: val };
    setLocalSettings(next);
    saveMut.mutate(next);
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-orange-100 flex items-center justify-center">
          <Clock className="w-5 h-5 text-orange-600" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Scheduled Runs</h1>
          <p className="text-sm text-gray-500">Configure automated run intervals for each pipeline stage</p>
        </div>
      </div>

      {saveMut.isSuccess && (
        <div className="rounded-lg bg-green-50 border border-green-200 px-4 py-3 text-sm text-green-700">
          Schedule saved successfully.
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {(Object.keys(RUN_META) as RunKey[]).map(key => (
          <RunCard
            key={key}
            runKey={key}
            config={eff[key]}
            onSave={handleSave}
            isSaving={saveMut.isPending}
          />
        ))}
      </div>
    </div>
  );
}
