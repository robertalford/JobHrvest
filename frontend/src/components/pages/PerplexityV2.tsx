import { useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  AlertCircle,
  CheckCircle2,
  ChevronRight,
  Download,
  FileSpreadsheet,
  Loader2,
  Upload,
  XCircle,
} from 'lucide-react';
import {
  downloadCompanyEnrichmentRun,
  getCompanyEnrichmentWorkerHealth,
  getCompanyEnrichmentRun,
  getCompanyEnrichmentRunRows,
  getCompanyEnrichmentRuns,
  startCompanyEnrichmentRun,
  uploadCompanyEnrichmentRun,
} from '../../lib/api';

const STATUS_COLORS: Record<string, string> = {
  pending: '#6b7280',
  processing: '#3b82f6',
  running: '#3b82f6',
  completed: '#0e8136',
  failed: '#ef4444',
  skipped: '#f59e0b',
};

function fmtDate(iso?: string | null) {
  return iso ? new Date(iso).toLocaleString() : '—';
}

function fmtDuration(seconds?: number | null) {
  if (seconds == null || Number.isNaN(seconds)) return '—';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  if (mins < 60) return `${mins}m ${secs}s`;
  const hours = Math.floor(mins / 60);
  return `${hours}h ${mins % 60}m`;
}

function RunBadge({ status }: { status: string }) {
  const cls =
    status === 'completed' ? 'badge-green'
      : status === 'failed' ? 'badge-red'
        : status === 'running' ? 'badge-blue'
          : 'badge-gray';
  return <span className={`${cls} text-xs`}>{status}</span>;
}

function RowStatusBadge({ status }: { status: string }) {
  return (
    <span
      className="inline-block px-2 py-0.5 rounded-full text-xs font-medium text-white"
      style={{ backgroundColor: STATUS_COLORS[status] ?? '#6b7280' }}
    >
      {status}
    </span>
  );
}

function UploadSection({ onUploaded }: { onUploaded: (runId: string) => void }) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [uploaded, setUploaded] = useState<Record<string, unknown> | null>(null);
  const qc = useQueryClient();
  const { data: workerHealth } = useQuery({
    queryKey: ['company-enrichment-worker-health'],
    queryFn: getCompanyEnrichmentWorkerHealth,
    refetchInterval: 5000,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
  });

  const uploadMutation = useMutation({
    mutationFn: uploadCompanyEnrichmentRun,
    onSuccess: (data) => {
      setUploaded(data);
      onUploaded(String(data.id));
      qc.invalidateQueries({ queryKey: ['company-enrichment-runs'] });
    },
  });

  const startMutation = useMutation({
    mutationFn: startCompanyEnrichmentRun,
    onSuccess: (_, runId) => {
      setUploaded((prev) => prev ? { ...prev, run_status: 'running', id: runId } : prev);
      qc.invalidateQueries({ queryKey: ['company-enrichment-runs'] });
      qc.invalidateQueries({ queryKey: ['company-enrichment-run', runId] });
    },
  });

  const handleFile = (file: File) => {
    setUploaded(null);
    uploadMutation.mutate(file);
  };

  return (
    <div className="card p-5 space-y-4">
      <div>
        <h2 className="font-semibold text-gray-900 flex items-center gap-2">
          <FileSpreadsheet className="w-5 h-5 text-brand" />
          Perplexity v2
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Upload a CSV with <code>company,country</code>. JobHarvest will run the Codex-backed
          enrichment workflow and return the same legacy CSV shape:
          <code className="ml-1">company,country,job_page_url,job_count,comment</code>.
        </p>
      </div>

      {workerHealth && (
        <div className={`rounded-lg border px-3 py-2 text-sm ${workerHealth.alive ? 'bg-green-50 border-green-100 text-green-700' : 'bg-amber-50 border-amber-100 text-amber-700'}`}>
          {workerHealth.alive
            ? `Codex worker online - ${Number(workerHealth.active_workers ?? 0)} active of ${Number(workerHealth.global_max_concurrency ?? 0)} across ${Number(workerHealth.worker_processes ?? 1)} process${Number(workerHealth.worker_processes ?? 1) === 1 ? '' : 'es'}${workerHealth.last_heartbeat ? ` - heartbeat ${fmtDate(String(workerHealth.last_heartbeat))}` : ''}`
            : String(workerHealth.message ?? 'Codex worker offline')}
        </div>
      )}

      <div
        onClick={() => fileRef.current?.click()}
        className="border-2 border-dashed border-gray-200 rounded-xl p-8 flex flex-col items-center gap-3 cursor-pointer transition-colors hover:border-brand/40 hover:bg-gray-50"
      >
        <Upload className="w-8 h-8 text-gray-300" />
        <div className="text-center">
          <div className="text-sm font-medium text-gray-700">Choose a CSV file to upload</div>
          <div className="text-xs text-gray-400 mt-1">Required headers: company, country</div>
        </div>
        <input
          ref={fileRef}
          type="file"
          accept=".csv,text/csv"
          className="hidden"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) handleFile(file);
          }}
        />
      </div>

      {uploadMutation.isPending && (
        <div className="flex items-center gap-2 text-sm text-gray-500">
          <Loader2 className="w-4 h-4 animate-spin text-blue-500" />
          Uploading and validating…
        </div>
      )}

      {uploadMutation.isError && (
        <div className="flex items-start gap-2 text-sm text-red-600 bg-red-50 rounded-lg p-3">
          <XCircle className="w-4 h-4 mt-0.5 flex-shrink-0" />
          Upload failed: {String((uploadMutation.error as Error)?.message ?? 'Unknown error')}
        </div>
      )}

      {uploaded && (
        <div className={`rounded-xl border p-4 ${uploaded.validation_status === 'valid' ? 'bg-green-50 border-green-100' : 'bg-red-50 border-red-100'}`}>
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-start gap-3">
              {uploaded.validation_status === 'valid'
                ? <CheckCircle2 className="w-5 h-5 text-green-600 mt-0.5 flex-shrink-0" />
                : <AlertCircle className="w-5 h-5 text-red-500 mt-0.5 flex-shrink-0" />}
              <div>
                <div className="font-medium text-sm text-gray-900">
                  {uploaded.validation_status === 'valid' ? 'Validation passed' : 'Validation failed'}
                  {' · '}{String(uploaded.original_filename)}
                  {' · '}{Number(uploaded.total_rows ?? 0).toLocaleString()} rows
                </div>
                {Array.isArray(uploaded.validation_errors) && uploaded.validation_errors.length > 0 && (
                  <ul className="mt-1.5 space-y-0.5">
                    {(uploaded.validation_errors as string[]).map((err, idx) => (
                      <li key={idx} className="text-xs text-red-600">• {err}</li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
            {uploaded.validation_status === 'valid' && uploaded.run_status === 'pending' && (
              <button
                onClick={() => startMutation.mutate(String(uploaded.id))}
                disabled={startMutation.isPending || workerHealth?.alive === false}
                className="btn-primary text-sm px-4 py-1.5 flex-shrink-0"
              >
                {startMutation.isPending ? 'Starting…' : 'Start Run'}
              </button>
            )}
            {uploaded.run_status === 'running' && (
              <span className="text-sm text-blue-600 font-medium flex-shrink-0">Running…</span>
            )}
          </div>
        </div>
      )}

      {startMutation.isError && (
        <div className="flex items-start gap-2 text-sm text-red-600 bg-red-50 rounded-lg p-3">
          <XCircle className="w-4 h-4 mt-0.5 flex-shrink-0" />
          Start failed: {String((startMutation.error as Error)?.message ?? 'Unknown error')}
        </div>
      )}
    </div>
  );
}

function RunDetail({ runId }: { runId: string }) {
  const [statusFilter, setStatusFilter] = useState('');
  const [page, setPage] = useState(1);

  const { data: run, isLoading: runLoading } = useQuery({
    queryKey: ['company-enrichment-run', runId],
    queryFn: () => getCompanyEnrichmentRun(runId),
    refetchInterval: (q) => {
      const status = q.state.data?.run_status;
      return status === 'completed' || status === 'failed' ? false : 4000;
    },
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
  });

  const { data: rows, isLoading: rowsLoading } = useQuery({
    queryKey: ['company-enrichment-run-rows', runId, page, statusFilter],
    queryFn: () => getCompanyEnrichmentRunRows(runId, page, 50, statusFilter || undefined),
    refetchInterval: run?.run_status === 'completed' || run?.run_status === 'failed' ? false : 4000,
    placeholderData: (prev) => prev,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
  });

  if (runLoading) {
    return <div className="card p-8 text-center text-gray-400">Loading…</div>;
  }
  if (!run) return null;

  const byStatus = (run.by_status as Record<string, number>) ?? {};
  const totalPages = Math.max(1, Math.ceil(Number(rows?.total ?? 0) / 50));

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
        {[
          { label: 'Total Rows', value: Number(run.total_rows ?? 0), color: '#111827' },
          { label: 'Completed', value: Number(byStatus.completed ?? 0), color: '#0e8136' },
          { label: 'Processing', value: Number(byStatus.processing ?? 0), color: '#3b82f6' },
          { label: 'Pending', value: Number(byStatus.pending ?? 0), color: '#6b7280' },
          { label: 'Failed', value: Number(byStatus.failed ?? 0), color: '#ef4444' },
          { label: 'Cached', value: Number(run.cached_rows ?? 0), color: '#7c3aed' },
        ].map((card) => (
          <div key={card.label} className="card p-4">
            <div className="text-2xl font-bold" style={{ color: card.color }}>{card.value.toLocaleString()}</div>
            <div className="text-xs text-gray-500 mt-0.5">{card.label}</div>
          </div>
        ))}
      </div>

      <div className="card p-5">
        <div className="flex flex-wrap items-center gap-3 justify-between">
          <div>
            <h3 className="font-semibold text-gray-900">Run Summary</h3>
            <p className="text-xs text-gray-500 mt-1">
              Uploaded {fmtDate(run.created_at)} • Started {fmtDate(run.run_started_at)} • Completed {fmtDate(run.run_completed_at)}
            </p>
            <p className="text-xs text-gray-400 mt-1">
              Avg row {fmtDuration(Number(run.avg_completed_seconds ?? NaN))} • Throughput {run.processing_rate_per_min ? `${Number(run.processing_rate_per_min).toFixed(2)}/min` : '—'} • ETA {fmtDuration(Number(run.eta_seconds ?? NaN))}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <RunBadge status={String(run.run_status)} />
            {run.download_url && (
              <button
                onClick={() => downloadCompanyEnrichmentRun(runId)}
                className="btn-primary text-sm px-3 py-1.5 inline-flex items-center gap-2"
              >
                <Download className="w-4 h-4" />
                Download CSV
              </button>
            )}
          </div>
        </div>

        <div className="mt-4">
          <div className="flex items-center justify-between text-xs text-gray-500 mb-1">
            <span>Progress</span>
            <span>{Number(run.progress_pct ?? 0).toFixed(1)}%</span>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
            <div className="h-full bg-brand rounded-full" style={{ width: `${Number(run.progress_pct ?? 0)}%` }} />
          </div>
        </div>

        {run.error_message && (
          <div className="mt-4 flex items-start gap-2 text-sm text-red-600 bg-red-50 rounded-lg p-3">
            <AlertCircle className="w-4 h-4 mt-0.5 flex-shrink-0" />
            {String(run.error_message)}
          </div>
        )}
      </div>

      <div className="card">
        <div className="p-4 border-b border-gray-100 flex flex-wrap gap-2 items-center">
          <span className="text-sm font-semibold text-gray-800">Row Results</span>
          <span className="text-xs text-gray-400">Cached {Number(run.cached_rows ?? 0).toLocaleString()}</span>
          <span className="text-xs text-gray-400">Throughput {run.processing_rate_per_min ? `${Number(run.processing_rate_per_min).toFixed(2)}/min` : '—'}</span>
          <select
            value={statusFilter}
            onChange={(e) => {
              setStatusFilter(e.target.value);
              setPage(1);
            }}
            className="text-xs border border-gray-200 rounded px-2 py-1 ml-2"
          >
            <option value="">All statuses</option>
            {['pending', 'processing', 'completed', 'failed', 'skipped'].map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <span className="text-xs text-gray-400 ml-auto">{Number(rows?.total ?? 0).toLocaleString()} rows</span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                {['#', 'Company', 'Country', 'Job Page URL', 'Job Count', 'Comment', 'Status'].map((h) => (
                  <th key={h} className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase tracking-wide">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {rowsLoading ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
              ) : !rows?.items?.length ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No rows yet.</td></tr>
              ) : rows.items.map((row: Record<string, unknown>) => (
                <tr key={String(row.id)} className="hover:bg-gray-50">
                  <td className="px-4 py-2.5 text-xs text-gray-400">{String(row.row_number)}</td>
                  <td className="px-4 py-2.5 font-medium text-gray-900 max-w-[220px] truncate">{String(row.company)}</td>
                  <td className="px-4 py-2.5 text-gray-500 text-xs">{String(row.country)}</td>
                  <td className="px-4 py-2.5 text-xs max-w-[260px]">
                    {row.job_page_url && String(row.job_page_url) !== 'not found' ? (
                      <a href={String(row.job_page_url)} target="_blank" rel="noreferrer" className="text-brand hover:underline truncate block">
                        {String(row.job_page_url)}
                      </a>
                    ) : (
                      <span className="text-gray-400">{String(row.job_page_url ?? 'not found')}</span>
                    )}
                  </td>
                  <td className="px-4 py-2.5 text-xs text-gray-600">{String(row.job_count ?? 'not found')}</td>
                  <td className="px-4 py-2.5 text-xs text-gray-500 max-w-[260px]">
                    <div className="truncate" title={String(row.comment ?? row.error_message ?? '')}>
                      {String(row.comment ?? row.error_message ?? '—')}
                    </div>
                  </td>
                  <td className="px-4 py-2.5"><RowStatusBadge status={String(row.status)} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {totalPages > 1 && (
          <div className="p-3 border-t border-gray-100 flex items-center justify-between">
            <span className="text-xs text-gray-400">Page {page} of {totalPages}</span>
            <div className="flex gap-1">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">Prev</button>
              <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">Next</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export function PerplexityV2() {
  const [page, setPage] = useState(1);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const qc = useQueryClient();

  const { data: runs, isLoading } = useQuery({
    queryKey: ['company-enrichment-runs', page],
    queryFn: () => getCompanyEnrichmentRuns(page, 20),
    refetchInterval: 4000,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
  });

  const handleUploaded = (runId: string) => {
    setSelectedRunId(runId);
    qc.invalidateQueries({ queryKey: ['company-enrichment-runs'] });
  };

  const items: Record<string, unknown>[] = runs?.items ?? [];
  const totalPages = Math.max(1, Math.ceil(Number(runs?.total ?? 0) / 20));

  return (
    <div className="p-6 space-y-6">
      <UploadSection onUploaded={handleUploaded} />

      <div className="card">
        <div className="p-4 border-b border-gray-100 flex items-center justify-between">
          <h2 className="font-semibold text-gray-900">Run History</h2>
          <span className="text-xs text-gray-400">{Number(runs?.total ?? 0).toLocaleString()} runs</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                {['File', 'Rows', 'Validation', 'Run', 'Completed', 'Failed', 'Uploaded'].map((h) => (
                  <th key={h} className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase tracking-wide">{h}</th>
                ))}
                <th className="px-4 py-2.5" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {isLoading ? (
                <tr><td colSpan={8} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
              ) : items.length === 0 ? (
                <tr><td colSpan={8} className="px-4 py-10 text-center text-gray-400">No runs yet. Upload a CSV above to get started.</td></tr>
              ) : items.map((run) => {
                const isSelected = selectedRunId === String(run.id);
                return (
                  <tr
                    key={String(run.id)}
                    onClick={() => setSelectedRunId(isSelected ? null : String(run.id))}
                    className={`cursor-pointer transition-colors ${isSelected ? 'bg-brand/5 border-l-2 border-brand' : 'hover:bg-gray-50'}`}
                  >
                    <td className="px-4 py-3 font-medium text-gray-800 max-w-[240px]">
                      <div className="truncate">{String(run.original_filename)}</div>
                    </td>
                    <td className="px-4 py-3 text-gray-500 text-xs">{Number(run.total_rows ?? 0).toLocaleString()}</td>
                    <td className="px-4 py-3">
                      {String(run.validation_status) === 'valid'
                        ? <span className="badge-green text-xs">valid</span>
                        : <span className="badge-red text-xs">{String(run.validation_status)}</span>}
                    </td>
                    <td className="px-4 py-3"><RunBadge status={String(run.run_status)} /></td>
                    <td className="px-4 py-3 text-green-600 text-xs font-medium">{Number(run.completed_rows ?? 0).toLocaleString()}</td>
                    <td className="px-4 py-3 text-red-500 text-xs">{Number(run.failed_rows ?? 0).toLocaleString()}</td>
                    <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">{fmtDate(String(run.created_at ?? ''))}</td>
                    <td className="px-4 py-3">
                      <ChevronRight className={`w-4 h-4 transition-transform ${isSelected ? 'rotate-90 text-brand' : 'text-gray-300'}`} />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {totalPages > 1 && (
          <div className="p-3 border-t border-gray-100 flex items-center justify-between">
            <span className="text-xs text-gray-400">Page {page} of {totalPages}</span>
            <div className="flex gap-1">
              <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">Prev</button>
              <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">Next</button>
            </div>
          </div>
        )}
      </div>

      {selectedRunId && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <div className="h-px flex-1 bg-gray-100" />
            <span className="text-xs text-gray-400 uppercase tracking-wide font-medium">Run Detail</span>
            <div className="h-px flex-1 bg-gray-100" />
          </div>
          <RunDetail runId={selectedRunId} />
        </div>
      )}
    </div>
  );
}
