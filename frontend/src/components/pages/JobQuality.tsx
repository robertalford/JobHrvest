import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Star, CheckCircle, XCircle, ChevronLeft, ChevronRight, AlertTriangle, ExternalLink } from 'lucide-react';
import { getQualityQueue, submitQualityFeedback } from '../../lib/api';

type Job = {
  id: string;
  title: string;
  description: string | null;
  location_raw: string | null;
  employment_type: string | null;
  salary_raw: string | null;
  requirements: string | null;
  date_posted: string | null;
  source_url: string;
  extraction_method: string | null;
  quality_score: number | null;
  quality_issues: string[] | null;
  quality_flags: Record<string, unknown> | null;
  company_name: string | null;
  company_domain: string | null;
  first_seen_at: string | null;
};

function QualityScoreBar({ score }: { score: number | null }) {
  if (score == null) return null;
  const pct = Math.min(100, Math.max(0, score));
  const color = pct >= 60 ? '#0e8136' : pct >= 40 ? '#f59e0b' : '#ef4444';
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-gray-100 rounded-full h-2">
        <div className="h-2 rounded-full transition-all" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
      <span className="text-sm font-semibold" style={{ color }}>{score.toFixed(1)}</span>
    </div>
  );
}

function JobDetail({ job, onConfirm, onOverrule, isPending }: {
  job: Job;
  onConfirm: () => void;
  onOverrule: () => void;
  isPending: boolean;
}) {
  const issues = job.quality_issues ?? [];
  const flags = job.quality_flags ?? {};

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-lg font-semibold text-gray-900">{job.title}</h3>
          <div className="text-sm text-gray-500 mt-0.5">{job.company_name ?? job.company_domain}</div>
        </div>
        <a href={job.source_url} target="_blank" rel="noopener noreferrer" className="text-gray-400 hover:text-gray-600 flex-shrink-0">
          <ExternalLink className="w-4 h-4" />
        </a>
      </div>

      {/* Quality score */}
      <div>
        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">Quality Score</div>
        <QualityScoreBar score={job.quality_score} />
      </div>

      {/* Issues */}
      {issues.length > 0 && (
        <div>
          <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Issues Detected</div>
          <div className="space-y-1">
            {issues.map((issue, i) => (
              <div key={i} className="flex items-start gap-2 text-sm text-red-700 bg-red-50 rounded-lg px-3 py-1.5">
                <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" />
                {issue}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Field completeness */}
      <div>
        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Field Completeness</div>
        <div className="grid grid-cols-2 gap-2 text-xs">
          {[
            ['Title', job.title],
            ['Location', job.location_raw],
            ['Employment type', job.employment_type],
            ['Salary', job.salary_raw],
            ['Requirements', job.requirements],
            ['Date posted', job.date_posted],
          ].map(([label, value]) => (
            <div key={label as string} className={`flex items-center gap-1.5 px-2 py-1 rounded-md ${value ? 'bg-green-50 text-green-700' : 'bg-gray-50 text-gray-400'}`}>
              {value ? <CheckCircle className="w-3 h-3" /> : <XCircle className="w-3 h-3" />}
              <span>{label as string}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Description preview */}
      <div>
        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">
          Description ({job.description ? `${job.description.length} chars` : 'missing'})
        </div>
        {job.description ? (
          <div className={`text-sm text-gray-700 bg-gray-50 rounded-lg p-3 max-h-48 overflow-y-auto whitespace-pre-wrap leading-relaxed ${
            (flags.description_too_short as boolean) ? 'border border-orange-200 bg-orange-50' : ''
          }`}>
            {job.description.slice(0, 800)}{job.description.length > 800 ? '…' : ''}
          </div>
        ) : (
          <div className="text-sm text-gray-400 bg-red-50 border border-red-100 rounded-lg p-3">No description extracted</div>
        )}
      </div>

      {/* Extraction info */}
      <div className="flex gap-3 text-xs text-gray-400">
        <span>Method: <span className="text-gray-600">{job.extraction_method ?? '—'}</span></span>
        {job.first_seen_at && <span>First seen: <span className="text-gray-600">{new Date(job.first_seen_at).toLocaleDateString()}</span></span>}
      </div>

      {/* Actions */}
      <div className="flex gap-3 pt-2 border-t border-gray-100">
        <button
          onClick={onConfirm}
          disabled={isPending}
          className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 border-2 border-red-200 text-red-700 bg-red-50 rounded-xl font-medium text-sm hover:bg-red-100 disabled:opacity-50 transition-colors"
        >
          <XCircle className="w-4 h-4" />
          Confirm poor quality
        </button>
        <button
          onClick={onOverrule}
          disabled={isPending}
          className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 border-2 border-green-200 text-green-700 bg-green-50 rounded-xl font-medium text-sm hover:bg-green-100 disabled:opacity-50 transition-colors"
        >
          <CheckCircle className="w-4 h-4" />
          Overrule — it's quality
        </button>
      </div>
    </div>
  );
}

export function JobQuality() {
  const [page, setPage] = useState(1);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const qc = useQueryClient();
  const pageSize = 20;

  const { data, isLoading } = useQuery({
    queryKey: ['quality-queue', page],
    queryFn: () => getQualityQueue(page, pageSize),
    placeholderData: (prev) => prev,
  });

  const feedbackMut = useMutation({
    mutationFn: ({ jobId, decision }: { jobId: string; decision: string }) => submitQualityFeedback(jobId, decision),
    onSuccess: (_, { jobId }) => {
      // Advance to next item
      const items: Job[] = data?.items ?? [];
      const idx = items.findIndex((j: Job) => j.id === jobId);
      const next = items[idx + 1] ?? items[idx - 1] ?? null;
      setSelectedId(next?.id ?? null);
      qc.invalidateQueries({ queryKey: ['quality-queue'] });
    },
  });

  const items: Job[] = data?.items ?? [];
  const total: number = data?.total ?? 0;
  const totalPages = Math.ceil(total / pageSize) || 1;
  const selectedJob = items.find(j => j.id === selectedId) ?? null;

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-amber-100 flex items-center justify-center">
          <Star className="w-5 h-5 text-amber-600" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Job Quality Review</h1>
          <p className="text-sm text-gray-500">{total.toLocaleString()} jobs awaiting review</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[1fr_420px] gap-6 items-start">
        {/* Left: table */}
        <div className="card overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
            <span className="text-sm font-medium text-gray-700">{total.toLocaleString()} pending · lowest score first</span>
            {totalPages > 1 && (
              <div className="flex items-center gap-2">
                <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="p-1 rounded hover:bg-gray-100 disabled:opacity-40"><ChevronLeft className="w-4 h-4" /></button>
                <span className="text-xs text-gray-500">{page}/{totalPages}</span>
                <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page === totalPages} className="p-1 rounded hover:bg-gray-100 disabled:opacity-40"><ChevronRight className="w-4 h-4" /></button>
              </div>
            )}
          </div>
          {isLoading ? (
            <div className="p-8 text-center text-gray-400 text-sm">Loading…</div>
          ) : items.length === 0 ? (
            <div className="p-8 text-center text-gray-400 text-sm">
              <CheckCircle className="w-8 h-8 text-green-400 mx-auto mb-2" />
              All jobs reviewed!
            </div>
          ) : (
            <div className="divide-y divide-gray-50">
              {items.map(job => (
                <button
                  key={job.id}
                  onClick={() => setSelectedId(selectedId === job.id ? null : job.id)}
                  className={`w-full text-left px-4 py-3 hover:bg-gray-50 transition-colors ${selectedId === job.id ? 'bg-amber-50 border-l-2 border-amber-400' : ''}`}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <div className="text-sm font-medium text-gray-900 truncate">{job.title}</div>
                      <div className="text-xs text-gray-400 mt-0.5">{job.company_name ?? job.company_domain}</div>
                    </div>
                    <div className="flex-shrink-0 text-right">
                      <div className={`text-sm font-bold ${(job.quality_score ?? 0) < 20 ? 'text-red-600' : 'text-orange-500'}`}>
                        {job.quality_score?.toFixed(1) ?? '—'}
                      </div>
                      <div className="text-xs text-gray-400">{(job.quality_issues ?? []).length} issues</div>
                    </div>
                  </div>
                  {(job.quality_issues ?? []).length > 0 && (
                    <div className="mt-1.5 flex flex-wrap gap-1">
                      {(job.quality_issues ?? []).slice(0, 2).map((issue, i) => (
                        <span key={i} className="text-xs bg-red-50 text-red-600 rounded px-1.5 py-0.5 truncate max-w-[180px]">{issue}</span>
                      ))}
                      {(job.quality_issues ?? []).length > 2 && (
                        <span className="text-xs text-gray-400">+{(job.quality_issues ?? []).length - 2} more</span>
                      )}
                    </div>
                  )}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Right: detail panel */}
        {selectedJob ? (
          <div className="card p-5 sticky top-6">
            <JobDetail
              job={selectedJob}
              onConfirm={() => feedbackMut.mutate({ jobId: selectedJob.id, decision: 'confirm' })}
              onOverrule={() => feedbackMut.mutate({ jobId: selectedJob.id, decision: 'overrule' })}
              isPending={feedbackMut.isPending}
            />
          </div>
        ) : (
          <div className="card p-8 text-center text-gray-400 text-sm sticky top-6">
            <Star className="w-8 h-8 text-gray-300 mx-auto mb-2" />
            Select a job to review
          </div>
        )}
      </div>
    </div>
  );
}
