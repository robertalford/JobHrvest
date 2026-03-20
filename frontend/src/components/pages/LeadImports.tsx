import { useState, useRef, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getLeadBatches, uploadLeadBatch, importLeadBatch, getLeadBatch, getLeadBatchLeads } from '../../lib/api';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { Upload, CheckCircle, XCircle, Clock, AlertTriangle, FileText, ChevronRight } from 'lucide-react';

const STATUS_COLORS: Record<string, string> = {
  success: '#0e8136',
  failed: '#ef4444',
  blocked: '#f97316',
  skipped: '#6b7280',
  pending: '#3b82f6',
};

const COUNTRY_LABELS: Record<string, string> = {
  AU: '🇦🇺 Australia', SG: '🇸🇬 Singapore', PH: '🇵🇭 Philippines',
  NZ: '🇳🇿 New Zealand', MY: '🇲🇾 Malaysia', ID: '🇮🇩 Indonesia',
  TH: '🇹🇭 Thailand', HK: '🇭🇰 Hong Kong',
};

function fmtBytes(n: number) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

function fmtDate(iso: string | null | undefined) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString();
}

function ValidationBadge({ status }: { status: string }) {
  if (status === 'valid') return <span className="badge-green text-xs">Valid</span>;
  if (status === 'invalid') return <span className="badge-red text-xs">Invalid</span>;
  return <span className="badge-gray text-xs">Pending</span>;
}

function ImportBadge({ status }: { status: string }) {
  const map: Record<string, string> = {
    pending: 'badge-gray', importing: 'badge-blue', completed: 'badge-green', failed: 'badge-red',
  };
  return <span className={`${map[status] ?? 'badge-gray'} text-xs`}>{status}</span>;
}

// ── Upload section ───────────────────────────────────────────────────────────
function UploadSection({ onUploaded }: { onUploaded: (batch: Record<string, unknown>) => void }) {
  const [dragging, setDragging] = useState(false);
  const [uploadedBatch, setUploadedBatch] = useState<Record<string, unknown> | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const qc = useQueryClient();

  const uploadMutation = useMutation({
    mutationFn: uploadLeadBatch,
    onSuccess: (data) => {
      setUploadedBatch(data);
      onUploaded(data);
      qc.invalidateQueries({ queryKey: ['lead-batches'] });
    },
  });

  const importMutation = useMutation({
    mutationFn: (batchId: string) => importLeadBatch(batchId),
    onSuccess: () => {
      setUploadedBatch(prev => prev ? { ...prev, import_status: 'importing' } : prev);
      qc.invalidateQueries({ queryKey: ['lead-batches'] });
    },
  });

  const handleFile = useCallback((file: File) => {
    setUploadedBatch(null);
    uploadMutation.mutate(file);
  }, [uploadMutation]);

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault(); setDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  };

  return (
    <div className="card p-5 space-y-4">
      <h2 className="font-semibold text-gray-900">Upload Lead CSV</h2>

      {/* Drop zone */}
      <div
        onDragOver={e => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        onClick={() => fileRef.current?.click()}
        className={`border-2 border-dashed rounded-xl p-8 flex flex-col items-center gap-3 cursor-pointer transition-colors
          ${dragging ? 'border-brand bg-brand/5' : 'border-gray-200 hover:border-brand/40 hover:bg-gray-50'}`}
      >
        <Upload className={`w-8 h-8 ${dragging ? 'text-brand' : 'text-gray-300'}`} />
        <div className="text-center">
          <div className="text-sm font-medium text-gray-700">Drop CSV file here or click to browse</div>
          <div className="text-xs text-gray-400 mt-1">Required columns: country_id, advertiser_name, origin</div>
        </div>
        <input ref={fileRef} type="file" accept=".csv" className="hidden"
          onChange={e => { const f = e.target.files?.[0]; if (f) handleFile(f); }} />
      </div>

      {uploadMutation.isPending && (
        <div className="flex items-center gap-2 text-sm text-gray-500">
          <Clock className="w-4 h-4 animate-pulse text-blue-500" />
          Uploading and validating…
        </div>
      )}

      {uploadMutation.isError && (
        <div className="flex items-center gap-2 text-sm text-red-600 bg-red-50 rounded-lg p-3">
          <XCircle className="w-4 h-4 flex-shrink-0" />
          Upload failed: {String((uploadMutation.error as Error)?.message ?? 'Unknown error')}
        </div>
      )}

      {/* Validation result */}
      {uploadedBatch && (
        <div className={`rounded-xl p-4 border ${uploadedBatch.validation_status === 'valid' ? 'bg-green-50 border-green-100' : 'bg-red-50 border-red-100'}`}>
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-start gap-3">
              {uploadedBatch.validation_status === 'valid'
                ? <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                : <AlertTriangle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />}
              <div>
                <div className="font-medium text-sm text-gray-900">
                  {uploadedBatch.validation_status === 'valid' ? 'Validation passed' : 'Validation failed'}
                  {' · '}{String(uploadedBatch.original_filename)}
                  {' · '}{Number(uploadedBatch.total_rows).toLocaleString()} rows
                </div>
                {Array.isArray(uploadedBatch.validation_errors) && uploadedBatch.validation_errors.length > 0 && (
                  <ul className="mt-1.5 space-y-0.5">
                    {(uploadedBatch.validation_errors as string[]).map((e, i) => (
                      <li key={i} className="text-xs text-red-600">• {e}</li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
            {uploadedBatch.validation_status === 'valid' && uploadedBatch.import_status === 'pending' && (
              <button
                onClick={() => importMutation.mutate(String(uploadedBatch.id))}
                disabled={importMutation.isPending}
                className="btn-primary text-sm px-4 py-1.5 flex-shrink-0"
              >
                {importMutation.isPending ? 'Starting…' : 'Import Now'}
              </button>
            )}
            {uploadedBatch.import_status === 'importing' && (
              <span className="text-sm text-blue-600 font-medium flex-shrink-0">Importing…</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Batch detail panel ───────────────────────────────────────────────────────
function BatchDetail({ batchId }: { batchId: string }) {
  const [leadsPage, setLeadsPage] = useState(1);
  const [leadsStatus, setLeadsStatus] = useState('');
  const [leadsCountry, setLeadsCountry] = useState('');

  const { data: batch, isLoading: batchLoading } = useQuery({
    queryKey: ['lead-batch', batchId],
    queryFn: () => getLeadBatch(batchId),
    refetchInterval: 10000,
  });

  const { data: leads, isLoading: leadsLoading } = useQuery({
    queryKey: ['lead-batch-leads', batchId, leadsPage, leadsStatus, leadsCountry],
    queryFn: () => getLeadBatchLeads(batchId, leadsPage, 50, leadsStatus || undefined, leadsCountry || undefined),
    placeholderData: (prev) => prev,
  });

  if (batchLoading) return <div className="card p-8 text-center text-gray-400">Loading…</div>;
  if (!batch) return null;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const batchAny = batch as any;
  const byStatus = (batchAny.by_status as Record<string, number>) ?? {};
  const byCountry = (batchAny.by_country as Record<string, { total: number; by_status: Record<string, number>; jobs_extracted: number }>) ?? {};
  const topCats = (batchAny.top_categories as { category: string; total: number; jobs: number }[]) ?? [];
  const totalLeads = (batchAny.total_leads as number) ?? 0;

  const countryChartData = Object.entries(byCountry).map(([code, d]) => ({
    name: code,
    success: d.by_status.success ?? 0,
    failed: d.by_status.failed ?? 0,
    blocked: d.by_status.blocked ?? 0,
    skipped: d.by_status.skipped ?? 0,
  })).sort((a, b) => (b.success + b.failed) - (a.success + a.failed));

  const totalPages = Math.ceil((leads?.total ?? 0) / 50);

  return (
    <div className="space-y-4">
      {/* Status cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        {[
          { label: 'Total Leads', value: totalLeads, color: '#3b82f6' },
          { label: 'Imported', value: byStatus.success ?? 0, color: '#0e8136' },
          { label: 'Failed', value: byStatus.failed ?? 0, color: '#ef4444' },
          { label: 'Blocked', value: byStatus.blocked ?? 0, color: '#f97316' },
          { label: 'Pending', value: byStatus.pending ?? 0, color: '#6b7280' },
        ].map(({ label, value, color }) => (
          <div key={label} className="card p-4">
            <div className="text-2xl font-bold" style={{ color }}>{Number(value).toLocaleString()}</div>
            <div className="text-xs text-gray-500 mt-0.5">{label}</div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* By country chart + table */}
        {countryChartData.length > 0 && (
          <div className="card p-5">
            <h3 className="font-semibold text-gray-900 mb-3 text-sm">Leads by Market</h3>
            <ResponsiveContainer width="100%" height={160}>
              <BarChart data={countryChartData} margin={{ top: 0, right: 0, left: -10, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 10 }} />
                <Tooltip />
                <Bar dataKey="success" name="Imported" stackId="a" fill="#0e8136" />
                <Bar dataKey="failed" name="Failed" stackId="a" fill="#ef4444" />
                <Bar dataKey="blocked" name="Blocked" stackId="a" fill="#f97316" />
                <Bar dataKey="skipped" name="Skipped" stackId="a" fill="#d1d5db" />
              </BarChart>
            </ResponsiveContainer>
            <table className="w-full text-xs mt-3">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left py-1.5 text-gray-400 font-medium">Market</th>
                  <th className="text-right py-1.5 text-gray-400 font-medium">Total</th>
                  <th className="text-right py-1.5 text-gray-400 font-medium">Imported</th>
                  <th className="text-right py-1.5 text-gray-400 font-medium">Blocked</th>
                  <th className="text-right py-1.5 text-gray-400 font-medium">Jobs</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(byCountry).sort((a, b) => b[1].total - a[1].total).map(([code, d]) => (
                  <tr key={code} className="border-b border-gray-50">
                    <td className="py-1.5">{COUNTRY_LABELS[code] ?? code}</td>
                    <td className="py-1.5 text-right font-medium">{d.total.toLocaleString()}</td>
                    <td className="py-1.5 text-right text-green-600">{(d.by_status.success ?? 0).toLocaleString()}</td>
                    <td className="py-1.5 text-right text-orange-500">{(d.by_status.blocked ?? 0).toLocaleString()}</td>
                    <td className="py-1.5 text-right text-gray-600">{d.jobs_extracted.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Top categories */}
        {topCats.length > 0 && (
          <div className="card p-5">
            <h3 className="font-semibold text-gray-900 mb-3 text-sm">Top Categories</h3>
            <div className="space-y-1.5">
              {topCats.map(cat => (
                <div key={cat.category} className="flex items-center gap-2 text-xs">
                  <div className="w-28 text-gray-600 truncate flex-shrink-0">{cat.category}</div>
                  <div className="flex-1 bg-gray-100 rounded-full h-1.5 overflow-hidden">
                    <div className="h-full bg-brand rounded-full"
                      style={{ width: `${Math.min(100, (cat.total / (topCats[0]?.total || 1)) * 100)}%` }} />
                  </div>
                  <div className="w-10 text-right text-gray-500">{cat.total.toLocaleString()}</div>
                  {cat.jobs > 0 && <div className="text-green-600 w-12 text-right">{cat.jobs}j</div>}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Leads table */}
      <div className="card">
        <div className="p-4 border-b border-gray-100 flex flex-wrap gap-2 items-center">
          <span className="text-sm font-semibold text-gray-800">Imported Leads</span>
          <select value={leadsStatus} onChange={e => { setLeadsStatus(e.target.value); setLeadsPage(1); }}
            className="text-xs border border-gray-200 rounded px-2 py-1 ml-2">
            <option value="">All statuses</option>
            {['success', 'failed', 'blocked', 'skipped', 'pending'].map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <select value={leadsCountry} onChange={e => { setLeadsCountry(e.target.value); setLeadsPage(1); }}
            className="text-xs border border-gray-200 rounded px-2 py-1">
            <option value="">All markets</option>
            {Object.keys(byCountry).sort().map(c => (
              <option key={c} value={c}>{COUNTRY_LABELS[c] ?? c}</option>
            ))}
          </select>
          <span className="text-xs text-gray-400 ml-auto">{(leads?.total ?? 0).toLocaleString()} leads</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                {['Company', 'Market', 'Category', 'Domain', 'Status', 'Exp. Jobs', 'Extracted'].map(h => (
                  <th key={h} className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase tracking-wide">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {leadsLoading ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
              ) : !leads?.items?.length ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No leads in this batch yet.</td></tr>
              ) : leads.items.map((lead: Record<string, unknown>) => (
                <tr key={String(lead.id)} className="hover:bg-gray-50">
                  <td className="px-4 py-2.5 font-medium text-gray-900 max-w-[180px] truncate">{String(lead.advertiser_name)}</td>
                  <td className="px-4 py-2.5 text-gray-500 text-xs">{COUNTRY_LABELS[String(lead.country_id)] ?? String(lead.country_id)}</td>
                  <td className="px-4 py-2.5 text-gray-400 text-xs max-w-[120px] truncate">{String(lead.ad_origin_category ?? '—')}</td>
                  <td className="px-4 py-2.5 text-xs">
                    {lead.sample_linkout_url ? (
                      <a href={String(lead.sample_linkout_url)} target="_blank" rel="noreferrer"
                        className="text-brand hover:underline max-w-[150px] truncate block">
                        {String(lead.origin_domain)}
                      </a>
                    ) : <span className="text-gray-500">{String(lead.origin_domain)}</span>}
                  </td>
                  <td className="px-4 py-2.5">
                    <span className="inline-block px-2 py-0.5 rounded-full text-xs font-medium text-white"
                      style={{ backgroundColor: STATUS_COLORS[String(lead.status)] ?? '#6b7280' }}>
                      {String(lead.status)}
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-right text-gray-400 text-xs">{lead.expected_job_count ? String(lead.expected_job_count) : '—'}</td>
                  <td className="px-4 py-2.5 text-right font-medium text-xs">{String(lead.jobs_extracted ?? 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {totalPages > 1 && (
          <div className="p-3 border-t border-gray-100 flex items-center justify-between">
            <span className="text-xs text-gray-400">Page {leadsPage} of {totalPages}</span>
            <div className="flex gap-1">
              <button onClick={() => setLeadsPage(1)} disabled={leadsPage === 1} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">«</button>
              <button onClick={() => setLeadsPage(p => Math.max(1, p - 1))} disabled={leadsPage === 1} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">Prev</button>
              <button onClick={() => setLeadsPage(p => Math.min(totalPages, p + 1))} disabled={leadsPage >= totalPages} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">Next</button>
              <button onClick={() => setLeadsPage(totalPages)} disabled={leadsPage >= totalPages} className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">»</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Main page ────────────────────────────────────────────────────────────────
export function LeadImports() {
  const [batchesPage, setBatchesPage] = useState(1);
  const [selectedBatchId, setSelectedBatchId] = useState<string | null>(null);
  const qc = useQueryClient();

  const { data: batches, isLoading } = useQuery({
    queryKey: ['lead-batches', batchesPage],
    queryFn: () => getLeadBatches(batchesPage, 20),
    refetchInterval: 10000,
  });

  const handleUploaded = (batch: Record<string, unknown>) => {
    setSelectedBatchId(String(batch.id));
    qc.invalidateQueries({ queryKey: ['lead-batches'] });
  };

  const totalBatchPages = Math.ceil((batches?.total ?? 0) / 20);
  const items: Record<string, unknown>[] = batches?.items ?? [];

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Lead Imports</h1>
        <p className="text-sm text-gray-500 mt-0.5">Upload CSV files to batch-import leads and trigger company crawls</p>
      </div>

      {/* Upload section */}
      <UploadSection onUploaded={handleUploaded} />

      {/* Batch history table */}
      <div className="card">
        <div className="p-4 border-b border-gray-100 flex items-center justify-between">
          <h2 className="font-semibold text-gray-900">Import History</h2>
          <span className="text-xs text-gray-400">{(batches?.total ?? 0).toLocaleString()} batches</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                {['File', 'Rows', 'Validation', 'Import', 'Imported', 'Failed', 'Blocked', 'Uploaded'].map(h => (
                  <th key={h} className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase tracking-wide">{h}</th>
                ))}
                <th className="px-4 py-2.5" />
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {isLoading ? (
                <tr><td colSpan={9} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-4 py-10 text-center">
                    <FileText className="w-8 h-8 text-gray-200 mx-auto mb-2" />
                    <div className="text-gray-400 text-sm">No imports yet. Upload a CSV above to get started.</div>
                  </td>
                </tr>
              ) : items.map(b => {
                const isSelected = selectedBatchId === String(b.id);
                return (
                  <tr
                    key={String(b.id)}
                    onClick={() => setSelectedBatchId(isSelected ? null : String(b.id))}
                    className={`cursor-pointer transition-colors ${isSelected ? 'bg-brand/5 border-l-2 border-brand' : 'hover:bg-gray-50'}`}
                  >
                    <td className="px-4 py-3 font-medium text-gray-800 max-w-[200px]">
                      <div className="truncate">{String(b.original_filename)}</div>
                      <div className="text-xs text-gray-400">{b.file_size_bytes ? fmtBytes(Number(b.file_size_bytes)) : ''}</div>
                    </td>
                    <td className="px-4 py-3 text-gray-500 text-xs">{b.total_rows ? Number(b.total_rows).toLocaleString() : '—'}</td>
                    <td className="px-4 py-3"><ValidationBadge status={String(b.validation_status)} /></td>
                    <td className="px-4 py-3"><ImportBadge status={String(b.import_status)} /></td>
                    <td className="px-4 py-3 text-green-600 text-xs font-medium">{Number(b.imported_leads ?? 0).toLocaleString()}</td>
                    <td className="px-4 py-3 text-red-500 text-xs">{Number(b.failed_leads ?? 0).toLocaleString()}</td>
                    <td className="px-4 py-3 text-orange-500 text-xs">{Number(b.blocked_leads ?? 0).toLocaleString()}</td>
                    <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">{fmtDate(b.created_at as string)}</td>
                    <td className="px-4 py-3">
                      <ChevronRight className={`w-4 h-4 transition-transform ${isSelected ? 'rotate-90 text-brand' : 'text-gray-300'}`} />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {totalBatchPages > 1 && (
          <div className="p-3 border-t border-gray-100 flex items-center justify-between">
            <span className="text-xs text-gray-400">Page {batchesPage} of {totalBatchPages}</span>
            <div className="flex gap-1">
              <button onClick={() => setBatchesPage(p => Math.max(1, p - 1))} disabled={batchesPage === 1}
                className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">Prev</button>
              <button onClick={() => setBatchesPage(p => Math.min(totalBatchPages, p + 1))} disabled={batchesPage >= totalBatchPages}
                className="btn-secondary text-xs px-2 py-1 disabled:opacity-40">Next</button>
            </div>
          </div>
        )}
      </div>

      {/* Batch detail */}
      {selectedBatchId && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <div className="h-px flex-1 bg-gray-100" />
            <span className="text-xs text-gray-400 uppercase tracking-wide font-medium">Batch Detail</span>
            <div className="h-px flex-1 bg-gray-100" />
          </div>
          <BatchDetail batchId={selectedBatchId} />
        </div>
      )}
    </div>
  );
}
