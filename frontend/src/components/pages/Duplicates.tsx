import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Copy, CheckCircle, XCircle, ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react';
import { getDuplicateQueue, submitDuplicateFeedback } from '../../lib/api';

type Job = {
  id: string;
  title: string;
  description: string | null;
  location_raw: string | null;
  employment_type: string | null;
  salary_raw: string | null;
  source_url: string;
  company_name: string | null;
  company_domain: string | null;
  first_seen_at: string | null;
  dedup_score: number | null;
};

type DupPair = {
  duplicate: Job;
  canonical: Job | null;
  dedup_score: number | null;
};

function fieldClass(a: string | null, b: string | null) {
  if (!a && !b) return 'text-gray-300';
  if (a === b) return 'text-gray-700';
  return 'text-amber-700 bg-amber-50 rounded px-1';
}

function JobCard({ job, label, highlight }: { job: Job; label: string; highlight?: boolean }) {
  return (
    <div className={`flex-1 rounded-xl p-4 space-y-3 border ${highlight ? 'border-amber-300 bg-amber-50' : 'border-gray-200 bg-gray-50'}`}>
      <div className="flex items-center justify-between gap-2">
        <span className={`text-xs font-semibold uppercase tracking-wide ${highlight ? 'text-amber-700' : 'text-gray-500'}`}>{label}</span>
        <a href={job.source_url} target="_blank" rel="noopener noreferrer" className="text-gray-400 hover:text-gray-600"><ExternalLink className="w-3.5 h-3.5" /></a>
      </div>
      <div>
        <div className="text-sm font-semibold text-gray-900">{job.title}</div>
        <div className="text-xs text-gray-500">{job.company_name ?? job.company_domain}</div>
      </div>
      <div className="space-y-1 text-xs">
        <div className="flex gap-2"><span className="text-gray-400 w-20 flex-shrink-0">Location:</span><span className={fieldClass(job.location_raw, null)}>{job.location_raw ?? '—'}</span></div>
        <div className="flex gap-2"><span className="text-gray-400 w-20 flex-shrink-0">Type:</span><span>{job.employment_type ?? '—'}</span></div>
        <div className="flex gap-2"><span className="text-gray-400 w-20 flex-shrink-0">Salary:</span><span>{job.salary_raw ?? '—'}</span></div>
        <div className="flex gap-2"><span className="text-gray-400 w-20 flex-shrink-0">First seen:</span><span>{job.first_seen_at ? new Date(job.first_seen_at).toLocaleDateString() : '—'}</span></div>
      </div>
      {job.description && (
        <div className="text-xs text-gray-600 bg-white rounded-lg p-2 max-h-40 overflow-y-auto leading-relaxed border border-gray-100">
          {job.description.slice(0, 600)}{job.description.length > 600 ? '…' : ''}
        </div>
      )}
    </div>
  );
}

function PairDetail({ pair, onConfirm, onOverrule, isPending }: {
  pair: DupPair;
  onConfirm: () => void;
  onOverrule: () => void;
  isPending: boolean;
}) {
  const score = pair.dedup_score ?? pair.duplicate.dedup_score;
  return (
    <div className="space-y-4">
      {/* Similarity score */}
      {score != null && (
        <div className="flex items-center gap-3 bg-gray-50 rounded-xl px-4 py-3">
          <div className="text-sm text-gray-500">Similarity score:</div>
          <div className={`text-lg font-bold ${score > 0.85 ? 'text-red-600' : score > 0.7 ? 'text-orange-500' : 'text-gray-700'}`}>
            {(score * 100).toFixed(1)}%
          </div>
          <div className="flex-1 bg-gray-200 rounded-full h-2">
            <div className="h-2 rounded-full bg-red-400 transition-all" style={{ width: `${Math.min(100, (score ?? 0) * 100)}%` }} />
          </div>
        </div>
      )}

      {/* Side-by-side */}
      <div className="flex gap-3">
        <JobCard job={pair.duplicate} label="Potential duplicate" highlight />
        {pair.canonical ? (
          <JobCard job={pair.canonical} label="Original (canonical)" />
        ) : (
          <div className="flex-1 rounded-xl border border-gray-200 bg-gray-50 p-4 flex items-center justify-center text-sm text-gray-400">
            Original not found
          </div>
        )}
      </div>

      {/* Signals */}
      <div className="text-xs text-gray-500 space-y-1 bg-gray-50 rounded-xl p-3">
        <div className="font-semibold text-gray-700 mb-1">Detection signals</div>
        <div>Title match: <span className={pair.duplicate.title === pair.canonical?.title ? 'text-red-600 font-semibold' : 'text-gray-600'}>
          {pair.duplicate.title === pair.canonical?.title ? 'Exact match' : 'Similar'}
        </span></div>
        <div>Same company: <span className={pair.duplicate.company_name === pair.canonical?.company_name ? 'text-red-600 font-semibold' : 'text-gray-600'}>
          {pair.duplicate.company_name === pair.canonical?.company_name ? 'Yes' : 'No / different'}
        </span></div>
      </div>

      {/* Actions */}
      <div className="flex gap-3 pt-2 border-t border-gray-100">
        <button
          onClick={onConfirm}
          disabled={isPending}
          className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 border-2 border-red-200 text-red-700 bg-red-50 rounded-xl font-medium text-sm hover:bg-red-100 disabled:opacity-50 transition-colors"
        >
          <XCircle className="w-4 h-4" />
          Confirm duplicate
        </button>
        <button
          onClick={onOverrule}
          disabled={isPending}
          className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 border-2 border-green-200 text-green-700 bg-green-50 rounded-xl font-medium text-sm hover:bg-green-100 disabled:opacity-50 transition-colors"
        >
          <CheckCircle className="w-4 h-4" />
          Overrule — not a duplicate
        </button>
      </div>
    </div>
  );
}

export function Duplicates() {
  const [page, setPage] = useState(1);
  const [selectedIdx, setSelectedIdx] = useState<number | null>(null);
  const qc = useQueryClient();
  const pageSize = 20;

  const { data, isLoading } = useQuery({
    queryKey: ['duplicate-queue', page],
    queryFn: () => getDuplicateQueue(page, pageSize),
    placeholderData: (prev) => prev,
  });

  const feedbackMut = useMutation({
    mutationFn: ({ jobId, decision }: { jobId: string; decision: string }) => submitDuplicateFeedback(jobId, decision),
    onSuccess: (_, { jobId }) => {
      const items: DupPair[] = data?.items ?? [];
      const idx = items.findIndex(p => p.duplicate.id === jobId);
      const nextIdx = idx + 1 < items.length ? idx + 1 : idx > 0 ? idx - 1 : null;
      setSelectedIdx(nextIdx);
      qc.invalidateQueries({ queryKey: ['duplicate-queue'] });
    },
  });

  const items: DupPair[] = data?.items ?? [];
  const total: number = data?.total ?? 0;
  const totalPages = Math.ceil(total / pageSize) || 1;
  const selectedPair = selectedIdx != null ? items[selectedIdx] ?? null : null;

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-purple-100 flex items-center justify-center">
          <Copy className="w-5 h-5 text-purple-600" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Duplicate Review</h1>
          <p className="text-sm text-gray-500">{total.toLocaleString()} potential duplicates awaiting review</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[280px_1fr] gap-6 items-start">
        {/* Left: list */}
        <div className="card overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
            <span className="text-sm font-medium text-gray-700">{total.toLocaleString()} pairs</span>
            {totalPages > 1 && (
              <div className="flex items-center gap-1">
                <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="p-1 rounded hover:bg-gray-100 disabled:opacity-40"><ChevronLeft className="w-3.5 h-3.5" /></button>
                <span className="text-xs text-gray-400">{page}/{totalPages}</span>
                <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page === totalPages} className="p-1 rounded hover:bg-gray-100 disabled:opacity-40"><ChevronRight className="w-3.5 h-3.5" /></button>
              </div>
            )}
          </div>
          {isLoading ? (
            <div className="p-8 text-center text-gray-400 text-sm">Loading…</div>
          ) : items.length === 0 ? (
            <div className="p-8 text-center text-gray-400 text-sm">
              <CheckCircle className="w-8 h-8 text-green-400 mx-auto mb-2" />
              No duplicates to review
            </div>
          ) : (
            <div className="divide-y divide-gray-50">
              {items.map((pair, idx) => (
                <button
                  key={pair.duplicate.id}
                  onClick={() => setSelectedIdx(selectedIdx === idx ? null : idx)}
                  className={`w-full text-left px-4 py-3 hover:bg-gray-50 transition-colors ${selectedIdx === idx ? 'bg-purple-50 border-l-2 border-purple-400' : ''}`}
                >
                  <div className="text-sm font-medium text-gray-900 truncate">{pair.duplicate.title}</div>
                  <div className="text-xs text-gray-400 mt-0.5">{pair.duplicate.company_name ?? pair.duplicate.company_domain}</div>
                  {pair.dedup_score != null && (
                    <div className={`text-xs font-semibold mt-1 ${pair.dedup_score > 0.85 ? 'text-red-600' : 'text-orange-500'}`}>
                      {(pair.dedup_score * 100).toFixed(0)}% similar
                    </div>
                  )}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Right: detail */}
        {selectedPair ? (
          <div className="card p-5">
            <PairDetail
              pair={selectedPair}
              onConfirm={() => feedbackMut.mutate({ jobId: selectedPair.duplicate.id, decision: 'confirm' })}
              onOverrule={() => feedbackMut.mutate({ jobId: selectedPair.duplicate.id, decision: 'overrule' })}
              isPending={feedbackMut.isPending}
            />
          </div>
        ) : (
          <div className="card p-8 text-center text-gray-400 text-sm">
            <Copy className="w-8 h-8 text-gray-300 mx-auto mb-2" />
            Select a pair to compare
          </div>
        )}
      </div>
    </div>
  );
}
