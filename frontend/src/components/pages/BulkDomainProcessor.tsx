import { useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { UploadCloud, Download, FileSpreadsheet, Loader2, AlertCircle } from 'lucide-react';
import { api } from '../../lib/api';

type Schema = { columns: string[]; default_confidence_threshold: number };

export function BulkDomainProcessor() {
  const fileRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [threshold, setThreshold] = useState<number>(0.8);
  const [isRunning, setIsRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastRun, setLastRun] = useState<{ rows: number; filename: string } | null>(null);

  const { data: schema } = useQuery<Schema>({
    queryKey: ['bulk-domain-process-schema'],
    queryFn: () => api.get('/bulk-domain-process/schema').then((r) => r.data),
  });

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    if (!file) {
      setError('Choose a CSV file first.');
      return;
    }
    setIsRunning(true);
    try {
      const form = new FormData();
      form.append('file', file);
      const res = await api.post('/bulk-domain-process/run', form, {
        params: { confidence_threshold: threshold },
        responseType: 'blob',
      });
      const blob = new Blob([res.data], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      const filename = `bulk_domain_selectors_${new Date().toISOString().slice(0, 10)}.csv`;
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      const rows = (await blob.text()).split('\n').filter(Boolean).length - 1;
      setLastRun({ rows: Math.max(rows, 0), filename });
    } catch (err) {
      const detail =
        (err as { response?: { data?: { detail?: string } } }).response?.data?.detail ||
        (err as Error).message;
      setError(`Run failed: ${detail}`);
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto p-8">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-gray-900 flex items-center gap-2">
          <FileSpreadsheet className="w-6 h-6 text-brand" /> Bulk Domain Processor
        </h1>
        <p className="mt-1 text-sm text-gray-600">
          Upload a CSV of domains. The current champion site-config model runs over each domain and
          returns a CSV with the discovered career URL and extraction selectors. Selector columns
          are only populated where the model's confidence meets the threshold.
        </p>
      </div>

      <form onSubmit={onSubmit} className="bg-white border border-gray-200 rounded-lg p-6 mb-6">
        <label className="block">
          <span className="text-sm font-medium text-gray-700">Input CSV</span>
          <span className="block text-xs text-gray-500 mt-0.5">
            One domain per row. Optional <code>domain</code> header. URLs are accepted — scheme and
            path are stripped.
          </span>
          <div className="mt-2 flex items-center gap-3">
            <input
              ref={fileRef}
              type="file"
              accept=".csv,text/csv"
              onChange={(e) => setFile(e.target.files?.[0] ?? null)}
              className="block text-sm text-gray-700 file:mr-3 file:py-1.5 file:px-3 file:rounded-md file:border-0 file:text-sm file:font-medium file:bg-brand/10 file:text-brand hover:file:bg-brand/20"
            />
            {file && <span className="text-xs text-gray-500">{file.name}</span>}
          </div>
        </label>

        <label className="block mt-5">
          <span className="text-sm font-medium text-gray-700">
            Confidence threshold: <span className="text-brand">{threshold.toFixed(2)}</span>
          </span>
          <span className="block text-xs text-gray-500 mt-0.5">
            Selectors are only emitted when the model's confidence is at or above this value. Rows
            below the threshold still appear in the output with a <code>low_confidence</code>{' '}
            status so you can review them.
          </span>
          <input
            type="range"
            min={0}
            max={1}
            step={0.05}
            value={threshold}
            onChange={(e) => setThreshold(Number(e.target.value))}
            className="mt-2 w-full accent-brand"
          />
        </label>

        <div className="mt-5 flex items-center gap-3">
          <button
            type="submit"
            disabled={isRunning || !file}
            className="inline-flex items-center gap-2 px-4 py-2 bg-brand text-white text-sm font-medium rounded-md hover:bg-brand/90 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isRunning ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" /> Running model…
              </>
            ) : (
              <>
                <UploadCloud className="w-4 h-4" /> Run champion model
              </>
            )}
          </button>
          {lastRun && !isRunning && (
            <span className="inline-flex items-center gap-1 text-sm text-gray-600">
              <Download className="w-4 h-4" /> Downloaded{' '}
              <span className="font-medium">{lastRun.filename}</span> ({lastRun.rows} rows)
            </span>
          )}
        </div>

        {error && (
          <div className="mt-4 flex items-start gap-2 text-sm text-red-700 bg-red-50 border border-red-200 rounded-md p-3">
            <AlertCircle className="w-4 h-4 mt-0.5 flex-shrink-0" />
            {error}
          </div>
        )}
      </form>

      {schema && (
        <div className="bg-white border border-gray-200 rounded-lg p-6">
          <h2 className="text-sm font-semibold text-gray-900">Output CSV columns</h2>
          <p className="text-xs text-gray-500 mt-1">
            Aligned to the production import schema so you can feed the output straight in.
          </p>
          <div className="mt-3 flex flex-wrap gap-1.5">
            {schema.columns.map((c) => (
              <span
                key={c}
                className="text-xs font-mono bg-gray-50 border border-gray-200 rounded px-2 py-0.5 text-gray-700"
              >
                {c}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
