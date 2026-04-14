/**
 * Models — ML model experiments with A/B validation against known-good test data.
 * Table shows 3 result columns: Test-data baseline, Live/Control model, New variant model.
 */
import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getMLModels, deleteMLModel,
  executeMLModelTestRun, getMLModelTestRuns,
  getFeedback, createFeedback, updateFeedback, deleteFeedback, uploadFeedbackScreenshot,
  triggerAutoImprove, getAutoImproveLog, getAutoImproveActivity,
  getImprovementRuns, getRecentTestRuns,
} from '../../lib/api';
import {
  Brain, Loader2, ChevronLeft, ChevronRight, Trash2,
  CheckCircle, Clock, FlaskConical, Play, Eye, Zap,
  Plus, Pencil, X, Sparkles, Info, ChevronDown, ChevronUp, Square,
  Wrench, AlertCircle,
} from 'lucide-react';

/* ── Status badges ── */
const STATUS_BADGES: Record<string, { label: string; cls: string; icon: React.ElementType }> = {
  new:     { label: 'New',     cls: 'bg-gray-100 text-gray-600',                                icon: Clock },
  defined: { label: 'Defined', cls: 'bg-blue-100 text-blue-700',                                icon: FlaskConical },
  tested:  { label: 'Tested',  cls: 'bg-green-100 text-green-700',                              icon: CheckCircle },
  live:    { label: 'Live',    cls: 'bg-emerald-100 text-emerald-800 ring-1 ring-emerald-300',   icon: Zap },
};

const MATCH_LABELS: Record<string, { label: string; cls: string }> = {
  model_equal_or_better: { label: 'Model ≥ Baseline', cls: 'text-green-700 bg-green-50' },
  model_only:            { label: 'Model only',        cls: 'text-green-600 bg-green-50' },
  partial:               { label: 'Partial',           cls: 'text-amber-600 bg-amber-50' },
  model_worse:           { label: 'Model worse',       cls: 'text-red-600 bg-red-50' },
  model_failed:          { label: 'Model failed',      cls: 'text-red-500 bg-red-50' },
  both_failed:           { label: 'Both failed',       cls: 'text-gray-500 bg-gray-100' },
  http_error:            { label: 'HTTP error',        cls: 'text-gray-400 bg-gray-50' },
};

/* ── Types ── */
/* eslint-disable @typescript-eslint/no-explicit-any */
interface StatSig {
  p: number; ci_low: number; ci_high: number;
  margin_of_error: number; significance: string;
}

interface ColSummary {
  sites_tested: number; sites_extracted: number; total_jobs: number;
  sites_extracted_quality?: number; total_jobs_quality?: number;
  core_complete: number; quality_score: number; quality_warnings?: number;
  stat_sig?: StatSig;
}

interface MLModel {
  id: string; name: string; model_type: string; description: string | null;
  status: string; version: number; is_active: boolean;
  created_at: string; updated_at: string;
  latest_test_run?: any;
  baseline_summary?: ColSummary | null;
  champion_summary?: ColSummary | null;
  model_summary?: ColSummary | null;
  labels?: { baseline: string; champion: string | null; challenger: string } | null;
}

interface ExtractedJob {
  title?: string; source_url?: string; location_raw?: string; salary_raw?: string;
  employment_type?: string; description?: string; closing_date?: string;
  listed_date?: string; department?: string; extraction_method?: string;
  extraction_confidence?: number; has_detail_page?: boolean;
}

interface PhaseData {
  jobs: number; jobs_quality?: number; fields: any; sample_titles: string[]; selectors_used?: any;
  url_used?: string; full_wrapper?: Record<string, any>;
  tier_used?: string | null; error?: string | null;
  url_found?: string | null; discovery_method?: string | null;
  extracted_jobs?: ExtractedJob[];
  quality_warning?: string; real_jobs?: number;
}

interface SiteResult {
  url: string; domain?: string; company: string; http_ok: boolean; match: string;
  baseline: PhaseData;
  champion?: PhaseData | null;
  model: PhaseData;
}

interface PhaseSummary {
  total_sites: number; model_extracted: number; model_failed: number; accuracy: number;
  match_breakdown: Record<string, number>; tier_breakdown: Record<string, number>;
  jobs: { baseline_total: number; model_total: number; ratio: number };
  quality: { baseline_core_complete: number; model_core_complete: number };
}

interface Summary extends PhaseSummary {
  regression?: PhaseSummary | null;
  exploration?: PhaseSummary | null;
}

interface ImprovementRun {
  id: string;
  source_model_id: string | null;
  test_run_id: string | null;
  output_model_id: string | null;
  status: string;
  description: string | null;
  source_model_name: string | null;
  output_model_name: string | null;
  test_winner: string | null;
  error_message: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string | null;
}

type TableTab = 'all' | 'improvement' | 'test';

interface TestRunData { sites: SiteResult[]; summary: Summary; progress?: { done: number; total: number } }
interface TestRun {
  id: string; test_name: string | null; total_tests: number;
  tests_passed: number; tests_failed: number; accuracy: number | null;
  status: string; started_at: string | null; completed_at: string | null;
  results_detail: TestRunData | null;
  baseline_summary?: ColSummary | null; model_summary?: ColSummary | null;
}

/* ── Mini-stat column component ── */

function ResultCol({ data, label, accent, compositeScore, isWinner, isBaseline, completeJobCount, pValue }: {
  data: ColSummary | null | undefined; label: string; accent: string;
  compositeScore?: number | null; isWinner?: boolean; isBaseline?: boolean;
  completeJobCount?: number | null; pValue?: number | null;
}) {
  if (!data) return <td className="px-3 py-3 text-left text-xs text-gray-300">—</td>;
  // Only count complete jobs (all 4 core fields). Use client-computed count if available, else backend quality count.
  const jobCount = completeJobCount ?? data.total_jobs_quality ?? data.total_jobs;
  const siteCount = data.sites_extracted_quality ?? data.sites_extracted;
  const passRate = data.sites_tested > 0 ? Math.round((siteCount / data.sites_tested) * 100) : 0;
  const displayLabel = isBaseline ? 'Jobstream Wrapper' : label;
  const hasComposite = compositeScore != null && typeof compositeScore === 'number';
  const successColor = hasComposite
    ? (compositeScore >= 60 ? 'text-green-600' : compositeScore >= 40 ? 'text-amber-600' : 'text-red-500')
    : 'text-gray-400';
  const qw = data.quality_warnings;

  // Format p-value display (-1 = baseline "n/a", null = not computed, 0+ = real value)
  const pDisplay = pValue === -1 ? 'n/a'
    : pValue != null ? (pValue < 0.001 ? '<0.001' : pValue < 0.01 ? pValue.toFixed(3) : pValue.toFixed(2))
    : null;
  const pColor = pValue === -1 ? 'text-gray-400'
    : pValue != null ? (pValue < 0.05 ? 'text-green-600 font-medium' : pValue < 0.10 ? 'text-amber-600' : 'text-gray-500')
    : 'text-gray-400';

  return (
    <td className="px-3 py-3">
      <div className="space-y-0.5 text-xs whitespace-nowrap">
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400 w-[72px] flex-shrink-0">Model:</span>
          <span className={`font-medium ${accent} truncate`}>{displayLabel}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400 w-[72px] flex-shrink-0">Sites:</span>
          <span className="font-medium text-gray-700">{siteCount}/{data.sites_tested} | {passRate}%</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400 w-[72px] flex-shrink-0">Jobs:</span>
          <span className="font-medium text-gray-700">{jobCount}</span>
          {qw != null && qw > 0 && <span className="text-red-400 text-[10px]">({qw} warnings)</span>}
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400 w-[72px] flex-shrink-0">Quality:</span>
          <span className={`font-medium ${data.quality_score >= 50 ? 'text-green-600' : data.quality_score >= 25 ? 'text-amber-600' : 'text-red-500'}`}>
            {data.quality_score}%
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400 w-[72px] flex-shrink-0">Score:</span>
          {hasComposite ? (
            <span className={`font-bold ${successColor}`}>{compositeScore!.toFixed(1)}</span>
          ) : (
            <span className="text-gray-400">—</span>
          )}
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400 w-[72px] flex-shrink-0">Result:</span>
          {isWinner ? (
            <span className="font-bold text-green-600">Winner</span>
          ) : (
            <span className="text-gray-400">—</span>
          )}
        </div>
        {pDisplay && (
          <div className="flex items-center gap-1.5">
            <span className="text-gray-400 w-[72px] flex-shrink-0">p-value:</span>
            <span className={pColor}>{pDisplay}</span>
          </div>
        )}
      </div>
    </td>
  );
}

/** Count jobs with all 4 core fields: title, source_url, location_raw, description (>50 chars) */
function countCompleteJobs(jobs: ExtractedJob[] | undefined): number {
  if (!jobs) return 0;
  return jobs.filter(j =>
    j.title && j.title.length > 2 &&
    j.source_url && j.source_url.length > 5 &&
    j.location_raw && j.location_raw.length > 1 &&
    j.description && j.description.length > 50
  ).length;
}

/* ── Improvement status badge ── */
const IMPROVEMENT_STATUS: Record<string, { label: string; cls: string; icon: React.ElementType }> = {
  analysing:    { label: 'Analysing',    cls: 'bg-amber-100 text-amber-700',   icon: Clock },
  running_codex:{ label: 'Running Codex',cls: 'bg-purple-100 text-purple-700', icon: Sparkles },
  deploying:    { label: 'Deploying',    cls: 'bg-blue-100 text-blue-700',     icon: Zap },
  testing:      { label: 'Testing',      cls: 'bg-cyan-100 text-cyan-700',     icon: FlaskConical },
  completed:    { label: 'Completed',    cls: 'bg-green-100 text-green-700',   icon: CheckCircle },
  failed:       { label: 'Failed',       cls: 'bg-red-100 text-red-600',       icon: AlertCircle },
  skipped:      { label: 'Skipped',      cls: 'bg-gray-100 text-gray-600',     icon: Clock },
  cancelled:    { label: 'Cancelled',    cls: 'bg-gray-100 text-gray-500',     icon: AlertCircle },
};

function ImprovementStatusBadge({ status }: { status: string }) {
  // Unknown status → show its raw label rather than silently falling back to
  // "Analysing" (which incorrectly suggests in-progress for cancelled rows).
  const s = IMPROVEMENT_STATUS[status] ?? {
    label: (status || 'Unknown').replace(/_/g, ' '),
    cls: 'bg-gray-100 text-gray-600',
    icon: AlertCircle,
  };
  const Icon = s.icon;
  const inProgress = ['analysing', 'running_codex', 'deploying', 'testing'].includes(status);
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${s.cls}`}>
      {inProgress ? <Loader2 className="w-3 h-3 animate-spin" /> : <Icon className="w-3 h-3" />}
      {s.label}
    </span>
  );
}

/* ── Timestamp display for run rows ── */
function RunTimestamp({ startedAt, completedAt, status }: {
  startedAt?: string | null;
  completedAt?: string | null;
  status?: string | null;
}) {
  if (!startedAt) return <span className="text-xs text-gray-400">—</span>;
  const start = new Date(startedAt);
  const end = completedAt ? new Date(completedAt) : null;
  const durationStr = end ? (() => {
    const ms = end.getTime() - start.getTime();
    const mins = Math.floor(ms / 60000);
    const secs = Math.floor((ms % 60000) / 1000);
    return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
  })() : null;
  // Only show "In progress…" for genuinely in-progress statuses. Cancelled
  // / failed / skipped runs don't always have completed_at populated, so
  // relying on `!end` alone produced the misleading "Cancelled + In
  // progress…" combination on the Models page.
  const inProgress = !end && (
    !status ||
    ['analysing', 'running_codex', 'deploying', 'testing', 'running'].includes(status)
  );
  const stoppedNoEnd = !end && !inProgress;
  return (
    <div className="text-[10px] text-gray-400 space-y-0">
      <div>Start: {start.toLocaleString()}</div>
      {end && <div>End: {end.toLocaleString()}</div>}
      {durationStr && <div>Duration: {durationStr}</div>}
      {inProgress && <div className="text-amber-500 font-medium">In progress...</div>}
      {stoppedNoEnd && <div className="text-gray-400">{status?.replace(/_/g, ' ') || 'stopped'}</div>}
    </div>
  );
}

/* ── Compact test summary for the All tab ── */
function AllTabTestSummary({ m }: { m: MLModel }) {
  const rd = m.latest_test_run?.results_detail;
  const summary = rd?.summary;
  const champScore = (summary as any)?.champion_composite?.composite;
  const challScore = (summary as any)?.challenger_composite?.composite;
  const bs = m.baseline_summary;
  const cs = m.champion_summary;
  const ms = m.model_summary;
  const labels = m.labels;
  if (champScore == null && challScore == null && !bs) {
    return <span className="text-xs text-gray-400">No results</span>;
  }
  const winner = champScore != null && challScore != null
    ? (champScore > challScore ? 'Champion' : challScore > champScore ? 'Challenger' : 'Tie')
    : null;

  const scoreCls = (s: number) => s >= 60 ? 'text-green-600' : s >= 40 ? 'text-amber-600' : 'text-red-500';

  const SummaryLine = ({ label, col, score, isWinner }: { label: string; col: ColSummary | null | undefined; score?: number | null; isWinner?: boolean }) => {
    if (!col) return null;
    const sites = col.sites_extracted_quality ?? col.sites_extracted;
    const pct = col.sites_tested > 0 ? Math.round((sites / col.sites_tested) * 100) : 0;
    const jobs = col.total_jobs_quality ?? col.total_jobs;
    return (
      <div className="flex items-center gap-2 whitespace-nowrap">
        <span className="text-gray-400 w-[70px] flex-shrink-0">{label}:</span>
        {score != null && <span className={`font-medium ${scoreCls(score)}`}>{score.toFixed(1)}</span>}
        <span className="text-gray-500">{sites}/{col.sites_tested} ({pct}%) sites</span>
        <span className="text-gray-500">{jobs} jobs</span>
        <span className={`${col.quality_score >= 50 ? 'text-green-600' : col.quality_score >= 25 ? 'text-amber-600' : 'text-red-500'}`}>{col.quality_score}% quality</span>
        {isWinner && <span className="text-green-600 font-bold text-[10px]">WINNER</span>}
      </div>
    );
  };

  return (
    <div className="text-xs space-y-0.5">
      <SummaryLine label="Baseline" col={bs} />
      <SummaryLine label={labels?.champion || 'Champion'} col={cs} score={champScore} isWinner={winner === 'Champion'} />
      <SummaryLine label={labels?.challenger || 'Challenger'} col={ms} score={challScore} isWinner={winner === 'Challenger'} />
    </div>
  );
}

/* ── Extracted test runs table (reusable for Test tab and All tab fallback) ── */
function TestRunsTable({ items, isLoading, modalModelId, setModalModelId, setModelDetailsId, deleteMut }: {
  items: MLModel[]; isLoading: boolean;
  modalModelId: string | null; setModalModelId: (id: string | null) => void;
  setModelDetailsId: (id: string | null) => void;
  deleteMut: { mutate: (id: string) => void };
}) {
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-gray-100 bg-gray-50">
          <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase" style={{ width: 260 }}>Model</th>
          <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[70px]">Status</th>
          <th className="text-left px-3 py-2 text-xs font-medium text-amber-600 uppercase bg-amber-50/50 whitespace-nowrap" style={{ minWidth: 220 }}>Jobstream Wrapper</th>
          <th className="text-left px-3 py-2 text-xs font-medium text-emerald-600 uppercase bg-emerald-50/50 whitespace-nowrap" style={{ minWidth: 220 }}>Champion</th>
          <th className="text-left px-3 py-2 text-xs font-medium text-blue-600 uppercase bg-blue-50/50 whitespace-nowrap" style={{ minWidth: 220 }}>Challenger</th>
          <th className="text-right px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[90px]"></th>
        </tr>
      </thead>
      <tbody>
        {isLoading ? (
          <tr><td colSpan={6} className="text-center py-12 text-gray-400"><Loader2 className="w-5 h-5 animate-spin inline mr-2" /> Loading...</td></tr>
        ) : items.length === 0 ? (
          <tr><td colSpan={6} className="text-center py-12 text-gray-400">No models yet.</td></tr>
        ) : items.map((m) => {
          const badge = STATUS_BADGES[m.status] ?? STATUS_BADGES.new;
          const B = badge.icon;
          const testRunning = m.latest_test_run?.status === 'running';
          return (
            <tr key={m.id} className={`border-b border-gray-50 ${testRunning ? 'bg-blue-50/40' : 'hover:bg-gray-50'}`}>
              <td className="px-3 py-3" style={{ maxWidth: 260 }}>
                <div className="font-medium text-gray-900 text-sm truncate">{m.name}</div>
                {m.description && <div className="text-xs text-gray-400 mt-0.5 line-clamp-2">{m.description}</div>}
                {m.latest_test_run && (
                  <RunTimestamp startedAt={m.latest_test_run.started_at} completedAt={m.latest_test_run.completed_at} status={m.latest_test_run.status} />
                )}
              </td>
              <td className="px-4 py-3">
                <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${badge.cls}`}><B className="w-3 h-3" /> {badge.label}</span>
              </td>
              {(() => {
                const rd = m.latest_test_run?.results_detail;
                const sites: SiteResult[] = rd?.sites ?? [];
                const summary = rd?.summary;
                const baselineComposite = null;
                const championComposite = (summary as any)?.champion_composite?.composite ?? null;
                const challengerComposite = (summary as any)?.challenger_composite?.composite ?? null;

                let baselineComplete = 0, championComplete = 0, challengerComplete = 0;
                for (const s of sites) {
                  baselineComplete += countCompleteJobs(s.baseline?.extracted_jobs);
                  championComplete += countCompleteJobs(s.champion?.extracted_jobs);
                  challengerComplete += countCompleteJobs(s.model?.extracted_jobs);
                }

                const scores = [
                  { key: 'baseline', val: baselineComposite },
                  { key: 'champion', val: championComposite },
                  { key: 'challenger', val: challengerComposite },
                ].filter(s => s.val != null);
                const maxScore = scores.length > 0 ? Math.max(...scores.map(s => s.val!)) : null;
                const winnerKey = maxScore != null ? scores.find(s => s.val === maxScore)?.key : null;

                const challengerPValue = (() => {
                  const cs = m.model_summary, ch = m.champion_summary;
                  if (!cs || !ch || cs.sites_tested < 5) return null;
                  const n1 = cs.sites_tested, x1 = cs.sites_extracted_quality ?? cs.sites_extracted;
                  const n2 = ch.sites_tested, x2 = ch.sites_extracted_quality ?? ch.sites_extracted;
                  if (n2 === 0) return null;
                  const p1 = x1 / n1, p2 = x2 / n2;
                  const pPool = (x1 + x2) / (n1 + n2);
                  const se = Math.sqrt(pPool * (1 - pPool) * (1 / n1 + 1 / n2));
                  if (se === 0) return p1 === p2 ? 1.0 : 0.0;
                  const z = Math.abs(p1 - p2) / se;
                  const t = 1 / (1 + 0.2316419 * z);
                  const d = 0.3989423 * Math.exp(-z * z / 2);
                  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
                  return Math.min(1, 2 * p);
                })();

                const championPValue = (() => {
                  const cs = m.champion_summary, bl = m.baseline_summary;
                  if (!cs || !bl || cs.sites_tested < 5) return null;
                  const n1 = cs.sites_tested, x1 = cs.sites_extracted_quality ?? cs.sites_extracted;
                  const n2 = bl.sites_tested, x2 = bl.sites_extracted;
                  if (n2 === 0) return null;
                  const p1 = x1 / n1, p2 = x2 / n2;
                  const pPool = (x1 + x2) / (n1 + n2);
                  const se = Math.sqrt(pPool * (1 - pPool) * (1 / n1 + 1 / n2));
                  if (se === 0) return p1 === p2 ? 1.0 : 0.0;
                  const z = Math.abs(p1 - p2) / se;
                  const t = 1 / (1 + 0.2316419 * z);
                  const d = 0.3989423 * Math.exp(-z * z / 2);
                  const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
                  return Math.min(1, 2 * p);
                })();

                return (
                  <>
                    <ResultCol data={m.baseline_summary} label={m.labels?.baseline || "Test Data"} accent="text-amber-600" isBaseline compositeScore={baselineComposite} isWinner={winnerKey === 'baseline'} completeJobCount={sites.length > 0 ? baselineComplete : null} pValue={-1} />
                    <ResultCol data={m.champion_summary} label={m.labels?.champion || "Champion"} accent="text-emerald-600" compositeScore={championComposite} isWinner={winnerKey === 'champion'} completeJobCount={sites.length > 0 ? championComplete : null} pValue={championPValue} />
                    <ResultCol data={m.model_summary} label={m.labels?.challenger || m.name} accent="text-blue-600" compositeScore={challengerComposite} isWinner={winnerKey === 'challenger'} completeJobCount={sites.length > 0 ? challengerComplete : null} pValue={challengerPValue} />
                  </>
                );
              })()}
              <td className="px-3 py-2 text-right">
                <div className="flex flex-col gap-1 items-end">
                  <button onClick={() => setModalModelId(modalModelId === m.id ? null : m.id)}
                    className="btn-secondary text-xs flex items-center gap-1 px-2 py-1"><Eye className="w-3 h-3" /> Results</button>
                  <button onClick={() => setModelDetailsId(m.id)}
                    className="btn-secondary text-xs flex items-center gap-1 px-2 py-1"><Info className="w-3 h-3" /> Models</button>
                  <button onClick={() => { if (confirm('Delete this model?')) deleteMut.mutate(m.id); }}
                    className="text-xs flex items-center gap-1 px-2 py-1 rounded text-red-400 hover:text-red-600 hover:bg-red-50">
                    <Trash2 className="w-3 h-3" /> Delete</button>
                </div>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

/* ── Main component ── */
export function Models() {
  const qc = useQueryClient();
  const [page, setPage] = useState(1);
  const [modalModelId, setModalModelId] = useState<string | null>(null);
  const [sampleSize, setSampleSize] = useState(50);
  const [feedbackTarget, setFeedbackTarget] = useState<{ runId: string; siteUrl: string; company: string } | null>(null);
  const [showCodexLog, setShowCodexLog] = useState(false);
  const [jobDetailPopup, setJobDetailPopup] = useState<{
    company: string; phase: string; jobs: ExtractedJob[];
    siteUrl?: string; wrapper?: Record<string, any> | null;
  } | null>(null);
  const [expandedJobs, setExpandedJobs] = useState<Set<number>>(new Set());
  const [codexLogLines, setCodexLogLines] = useState<string[]>([]);
  const [codexLogOffset, setCodexLogOffset] = useState(0);
  const [codexRunning, setCodexRunning] = useState(false);
  const codexLogRef = React.useRef<HTMLDivElement>(null);
  const [filterStatuses, setFilterStatuses] = useState<Set<string>>(new Set());
  const [codexGlobalRunning, setCodexGlobalRunning] = useState(false);
  const [modelDetailsId, setModelDetailsId] = useState<string | null>(null);
  const [modelDetailTab, setModelDetailTab] = useState<'baseline' | 'champion' | 'challenger'>('baseline');
  const [tableTab, setTableTab] = useState<TableTab>('all');
  const PAGE_SIZE = 20;

  // Poll codex running status — only every 15s, and only fetch running flag (offset=0, minimal data)
  React.useEffect(() => {
    // Initial check
    getAutoImproveActivity(0).then(d => setCodexGlobalRunning(d.running)).catch(() => {});
    const poll = setInterval(async () => {
      try {
        const data = await getAutoImproveActivity(0);
        setCodexGlobalRunning(data.running);
      } catch {}
    }, 15000);
    return () => clearInterval(poll);
  }, []);

  // Build query params — pass status filter to server
  const statusFilter = filterStatuses.size > 0 ? Array.from(filterStatuses).join(',') : undefined;

  // Fetch models page — auto-refresh every 15s
  const { data: allData, isLoading } = useQuery({
    queryKey: ['ml-models', page, statusFilter],
    queryFn: () => getMLModels({ page, page_size: PAGE_SIZE, status: statusFilter }).catch(() => ({ items: [], total: 0 })),
    refetchInterval: 5000,
  });

  // Fetch improvement runs
  const { data: improvementData } = useQuery({
    queryKey: ['improvement-runs'],
    queryFn: () => getImprovementRuns({ page: 1, page_size: 200 }).catch(() => ({ items: [], total: 0 })),
    refetchInterval: 5000,
  });
  const improvementRuns: ImprovementRun[] = improvementData?.items ?? [];

  // Fetch all recent test runs (not just latest per model) for the unified timeline
  const { data: recentTestRunsData } = useQuery({
    queryKey: ['recent-test-runs'],
    queryFn: () => getRecentTestRuns(50).catch(() => ({ items: [] })),
    refetchInterval: 5000,
  });
  const recentTestRuns = recentTestRunsData?.items ?? [];

  // Refresh countdown
  const [countdown, setCountdown] = useState(5);
  React.useEffect(() => {
    const timer = setInterval(() => {
      setCountdown(prev => prev <= 1 ? 5 : prev - 1);
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  const deleteMut = useMutation({ mutationFn: (id: string) => deleteMLModel(id), onSuccess: () => qc.invalidateQueries({ queryKey: ['ml-models'] }) });

  // Items are already filtered server-side
  const data = {
    items: allData?.items ?? [],
    total: allData?.total ?? 0,
  };
  const executeMut = useMutation({
    mutationFn: (modelId: string) => {
      // Find champion (live model) to test alongside
      const liveModel = items.find(m => m.status === 'live' && m.id !== modelId);
      return executeMLModelTestRun(modelId, sampleSize, liveModel?.id);
    },
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['ml-models'] }); qc.invalidateQueries({ queryKey: ['ml-test-runs'] }); },
  });

  const { data: testRuns } = useQuery({
    queryKey: ['ml-test-runs', modalModelId],
    queryFn: () => modalModelId ? getMLModelTestRuns(modalModelId, 1, 5) : null,
    enabled: !!modalModelId, refetchInterval: modalModelId ? 5000 : false,
  });

  // Feedback for current test run
  const latestRunId = testRuns?.items?.[0]?.id;
  const { data: allFeedback } = useQuery({
    queryKey: ['ml-feedback', modalModelId, latestRunId],
    queryFn: () => (modalModelId && latestRunId) ? getFeedback(modalModelId, latestRunId) : [],
    enabled: !!modalModelId && !!latestRunId,
  });
  const siteFeedbackCounts: Record<string, number> = {};
  for (const fb of (allFeedback || [])) {
    siteFeedbackCounts[fb.site_url] = (siteFeedbackCounts[fb.site_url] || 0) + 1;
  }

  // Feedback for sub-modal
  const { data: siteFeedbackItems, refetch: refetchSiteFeedback } = useQuery({
    queryKey: ['ml-feedback-site', modalModelId, feedbackTarget?.runId, feedbackTarget?.siteUrl],
    queryFn: () => (modalModelId && feedbackTarget) ? getFeedback(modalModelId, feedbackTarget.runId, feedbackTarget.siteUrl) : [],
    enabled: !!feedbackTarget,
  });

  // Poll Codex log when visible
  React.useEffect(() => {
    if (!showCodexLog || !modalModelId) return;
    const poll = setInterval(async () => {
      try {
        const data = await getAutoImproveLog(modalModelId, codexLogOffset);
        if (data.lines.length > 0) {
          setCodexLogLines(prev => [...prev, ...data.lines]);
          setCodexLogOffset(data.offset);
          // Auto-scroll to bottom
          setTimeout(() => codexLogRef.current?.scrollTo(0, codexLogRef.current.scrollHeight), 50);
        }
        setCodexRunning(data.running);
        if (!data.running && data.lines.length === 0) {
          // No new lines and not running — stop polling
        }
      } catch {}
    }, 2000);
    return () => clearInterval(poll);
  }, [showCodexLog, modalModelId, codexLogOffset]);

  // Auto-refresh model list when any test run completes (updates the 3 columns)
  const latestRunStatus = testRuns?.items?.[0]?.status;
  const prevRunStatus = React.useRef<string | undefined>(undefined);
  React.useEffect(() => {
    if (prevRunStatus.current === 'running' && latestRunStatus === 'completed') {
      qc.invalidateQueries({ queryKey: ['ml-models'] });
    }
    prevRunStatus.current = latestRunStatus;
  }, [latestRunStatus, qc]);

  const items: MLModel[] = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-purple-50 flex items-center justify-center"><Brain className="w-5 h-5 text-purple-600" /></div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Models</h1>
            <p className="text-sm text-gray-500">Experiment with extraction models, A/B validate against known-good test data</p>
          </div>
        </div>
        <div>
          {codexGlobalRunning ? (
            <button
              onClick={async () => {
                if (confirm('Stop the running Codex auto-improve process?')) {
                  try {
                    await fetch('/api/v1/ml-models/auto-improve/stop', { method: 'POST' });
                    setCodexGlobalRunning(false);
                  } catch {}
                }
              }}
              className="text-sm flex items-center gap-1.5 px-4 py-2 rounded-lg bg-red-50 text-red-700 hover:bg-red-100 border border-red-200 font-medium"
            >
              <Square className="w-3.5 h-3.5" /> Stop Auto-Improve
            </button>
          ) : (
            <button
              onClick={() => {
                const testedModel = items.find(m => m.status === 'tested') || items[0];
                if (testedModel) {
                  if (confirm('Launch Codex auto-improve? This will analyse failures, create a new model version, and test it.')) {
                    triggerAutoImprove(testedModel.id);
                    setCodexGlobalRunning(true);
                  }
                }
              }}
              className="text-sm flex items-center gap-1.5 px-4 py-2 rounded-lg bg-purple-50 text-purple-700 hover:bg-purple-100 border border-purple-200 font-medium"
            >
              <Sparkles className="w-3.5 h-3.5" /> Start Auto-Improve
            </button>
          )}
        </div>
      </div>

      {/* Codex Live Activity — only show when auto-improve is running */}
      {codexGlobalRunning && <CodexActivityPanel />}

      {/* Status filter pills */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-gray-500 mr-1">Filter:</span>
        {Object.entries(STATUS_BADGES).map(([key, badge]) => {
          const active = filterStatuses.has(key);
          return (
            <button
              key={key}
              onClick={() => {
                const next = new Set(filterStatuses);
                if (active) next.delete(key); else next.add(key);
                setFilterStatuses(next);
                setPage(1);
              }}
              className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium transition-all ${
                active
                  ? badge.cls + ' ring-2 ring-offset-1'
                  : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
              }`}
            >
              {badge.label}
            </button>
          );
        })}
        {filterStatuses.size > 0 && (
          <button
            onClick={() => { setFilterStatuses(new Set()); setPage(1); }}
            className="text-xs text-gray-400 hover:text-gray-600 ml-1"
          >
            Clear
          </button>
        )}
      </div>

      {/* Tabs + Refresh countdown */}
      <div className="flex items-center justify-between mb-1">
        <div className="flex gap-1">
          {([
            { key: 'all' as TableTab, label: 'All' },
            { key: 'improvement' as TableTab, label: 'Codex Improvement Runs', icon: Wrench },
            { key: 'test' as TableTab, label: 'Test Runs', icon: FlaskConical },
          ]).map(tab => {
            const Icon = tab.icon;
            const active = tableTab === tab.key;
            return (
              <button
                key={tab.key}
                onClick={() => { setTableTab(tab.key); setPage(1); }}
                className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-t-lg text-xs font-medium border border-b-0 transition-all ${
                  active
                    ? 'bg-white text-gray-900 border-gray-200'
                    : 'bg-gray-50 text-gray-500 border-transparent hover:bg-gray-100'
                }`}
              >
                {Icon && <Icon className="w-3 h-3" />}
                {tab.label}
              </button>
            );
          })}
        </div>
        <span className="text-xs text-gray-400">Updates in... {countdown}s</span>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        {/* === Test Runs table (shown on 'test' tab and 'all' tab) === */}
        {(tableTab === 'test' || tableTab === 'all') && (
          <>
            {tableTab === 'all' && items.length > 0 && improvementRuns.length > 0 ? (
              /* ── ALL tab: merge test runs and improvement runs sorted by date ── */
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100 bg-gray-50">
                    <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[50px]">Type</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase" style={{ width: 200 }}>Details</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[80px]">Status</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase" style={{ minWidth: 250 }}>Summary</th>
                    <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[180px]">Time</th>
                    <th className="text-right px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[90px]"></th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    // Build unified row list using ALL recent test runs (not just latest per model)
                    type UnifiedRow = { type: 'test'; data: MLModel; sortDate: number } | { type: 'improvement'; data: ImprovementRun; sortDate: number };
                    const rows: UnifiedRow[] = [];

                    // Use recent test runs if available, otherwise fall back to one-per-model
                    if (recentTestRuns.length > 0) {
                      const seenRunIds = new Set<string>();
                      for (const tr of recentTestRuns) {
                        if (seenRunIds.has(tr.id)) continue;
                        seenRunIds.add(tr.id);
                        // Build an MLModel-compatible object from the test run
                        const syntheticModel: MLModel = {
                          id: tr.model_id,
                          name: tr.model_name || 'Unknown',
                          description: tr.model_description,
                          model_type: 'tiered_extractor',
                          status: tr.status === 'running' ? 'new' : (tr.model_status || 'tested'),
                          created_at: tr.created_at,
                          latest_test_run: tr,
                          baseline_summary: tr.baseline_summary,
                          champion_summary: tr.champion_summary,
                          model_summary: tr.model_summary,
                          labels: tr.labels,
                        } as MLModel;
                        const d = tr.started_at || tr.created_at;
                        rows.push({ type: 'test', data: syntheticModel, sortDate: d ? new Date(d).getTime() : 0 });
                      }
                    } else {
                      for (const m of items) {
                        const d = m.latest_test_run?.started_at || m.latest_test_run?.created_at || m.created_at;
                        rows.push({ type: 'test', data: m, sortDate: d ? new Date(d).getTime() : 0 });
                      }
                    }
                    for (const ir of improvementRuns) {
                      const d = ir.started_at || ir.created_at;
                      rows.push({ type: 'improvement', data: ir, sortDate: d ? new Date(d).getTime() : 0 });
                    }

                    rows.sort((a, b) => b.sortDate - a.sortDate);

                    return rows.map(row => {
                      if (row.type === 'improvement') {
                        const ir = row.data;
                        const inProgress = ['analysing', 'running_codex', 'deploying', 'testing'].includes(ir.status);
                        return (
                          <tr key={`imp-${ir.id}`} className={`border-b border-gray-50 ${inProgress ? 'bg-purple-50/40' : 'hover:bg-gray-50'}`}>
                            <td className="px-3 py-3">
                              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-purple-100 text-purple-700">
                                <Wrench className="w-2.5 h-2.5" /> Improve
                              </span>
                            </td>
                            <td className="px-3 py-3" style={{ maxWidth: 200 }}>
                              <div className="text-sm font-medium text-gray-900">
                                {ir.source_model_name || '—'} → {ir.output_model_name || <span className="text-gray-400 italic">pending</span>}
                              </div>
                              {ir.test_winner && (
                                <div className="text-[10px] text-gray-400 mt-0.5">
                                  Test winner: <span className="font-medium text-gray-600">{ir.test_winner}</span>
                                </div>
                              )}
                            </td>
                            <td className="px-3 py-3">
                              <ImprovementStatusBadge status={ir.status} />
                            </td>
                            <td className="px-3 py-3">
                              {ir.description ? (
                                <div className="text-xs text-gray-600 line-clamp-2">{ir.description}</div>
                              ) : ir.error_message ? (
                                <div className="text-xs text-red-500 line-clamp-2">{ir.error_message}</div>
                              ) : (
                                <span className="text-xs text-gray-400 italic">{inProgress ? 'Running...' : '—'}</span>
                              )}
                            </td>
                            <td className="px-3 py-3">
                              <RunTimestamp startedAt={ir.started_at} completedAt={ir.completed_at} status={ir.status} />
                            </td>
                            <td className="px-3 py-2 text-right">
                              {inProgress && <Loader2 className="w-4 h-4 animate-spin text-purple-500 inline" />}
                            </td>
                          </tr>
                        );
                      } else {
                        const m = row.data;
                        const badge = STATUS_BADGES[m.status] ?? STATUS_BADGES.new;
                        const B = badge.icon;
                        const testRunning = m.latest_test_run?.status === 'running';
                        return (
                          <tr key={`test-${m.id}`} className={`border-b border-gray-50 ${testRunning ? 'bg-blue-50/40' : 'hover:bg-gray-50'}`}>
                            <td className="px-3 py-3">
                              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700">
                                <FlaskConical className="w-2.5 h-2.5" /> Test
                              </span>
                            </td>
                            <td className="px-3 py-3" style={{ maxWidth: 200 }}>
                              <div className="font-medium text-gray-900 text-sm truncate">{m.name}</div>
                              {m.description && <div className="text-[10px] text-gray-400 mt-0.5 line-clamp-1">{m.description}</div>}
                            </td>
                            <td className="px-3 py-3">
                              <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${badge.cls}`}>
                                <B className="w-3 h-3" /> {badge.label}
                              </span>
                            </td>
                            <td className="px-3 py-3">
                              <AllTabTestSummary m={m} />
                            </td>
                            <td className="px-3 py-3">
                              <RunTimestamp startedAt={m.latest_test_run?.started_at} completedAt={m.latest_test_run?.completed_at} status={m.latest_test_run?.status} />
                            </td>
                            <td className="px-3 py-2 text-right">
                              <div className="flex flex-col gap-1 items-end">
                                <button onClick={() => setModalModelId(modalModelId === m.id ? null : m.id)}
                                  className="btn-secondary text-xs flex items-center gap-1 px-2 py-1"><Eye className="w-3 h-3" /> Results</button>
                              </div>
                            </td>
                          </tr>
                        );
                      }
                    });
                  })()}
                </tbody>
              </table>
            ) : tableTab === 'all' ? (
              /* ALL tab but only test runs exist (no improvement runs yet) — fall through to standard table */
              <TestRunsTable
                items={items} isLoading={isLoading}
                modalModelId={modalModelId} setModalModelId={setModalModelId}
                setModelDetailsId={setModelDetailsId}
                deleteMut={deleteMut}
              />
            ) : (
              /* TEST tab — standard table */
              <TestRunsTable
                items={items} isLoading={isLoading}
                modalModelId={modalModelId} setModalModelId={setModalModelId}
                setModelDetailsId={setModelDetailsId}
                deleteMut={deleteMut}
              />
            )}
          </>
        )}

        {/* === Improvement Runs tab === */}
        {tableTab === 'improvement' && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 bg-gray-50">
                <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase" style={{ width: 220 }}>Test Run Analysed</th>
                <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[100px]">New Model</th>
                <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[90px]">Status</th>
                <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase" style={{ minWidth: 280 }}>Key Changes</th>
                <th className="text-left px-3 py-2 text-xs font-medium text-gray-500 uppercase w-[190px]">Time</th>
              </tr>
            </thead>
            <tbody>
              {improvementRuns.length === 0 ? (
                <tr><td colSpan={5} className="text-center py-12 text-gray-400">No improvement runs yet.</td></tr>
              ) : improvementRuns.map(ir => {
                const inProgress = ['analysing', 'running_codex', 'deploying', 'testing'].includes(ir.status);
                return (
                  <tr key={ir.id} className={`border-b border-gray-50 ${inProgress ? 'bg-purple-50/40' : 'hover:bg-gray-50'}`}>
                    <td className="px-3 py-3">
                      <div className="text-sm font-medium text-gray-900">{ir.source_model_name || '—'}</div>
                      {ir.test_winner && (
                        <div className="text-[10px] text-gray-400 mt-0.5">
                          Winner: <span className={`font-medium ${ir.test_winner === 'champion' ? 'text-emerald-600' : ir.test_winner === 'challenger' ? 'text-blue-600' : 'text-gray-600'}`}>{ir.test_winner}</span>
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-3">
                      {ir.output_model_name ? (
                        <span className="font-medium text-purple-700 text-sm">{ir.output_model_name}</span>
                      ) : (
                        <span className="text-xs text-gray-400 italic">{inProgress ? 'Building...' : '—'}</span>
                      )}
                    </td>
                    <td className="px-3 py-3">
                      <ImprovementStatusBadge status={ir.status} />
                    </td>
                    <td className="px-3 py-3">
                      {ir.description ? (
                        <div className="text-xs text-gray-600 line-clamp-2">{ir.description}</div>
                      ) : ir.error_message ? (
                        <div className="text-xs text-red-500 line-clamp-2">{ir.error_message}</div>
                      ) : (
                        <span className="text-xs text-gray-400 italic">{inProgress ? 'In progress...' : '—'}</span>
                      )}
                    </td>
                    <td className="px-3 py-3">
                      <RunTimestamp startedAt={ir.started_at} completedAt={ir.completed_at} status={ir.status} />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}

        {/* Pagination (test tab + all tab) */}
        {tableTab !== 'improvement' && total > PAGE_SIZE && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100">
            <span className="text-xs text-gray-500">{total} models</span>
            <div className="flex gap-1">
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page <= 1} className="btn-secondary px-2 py-1 text-xs disabled:opacity-40"><ChevronLeft className="w-3 h-3" /></button>
              <span className="px-3 py-1 text-xs text-gray-600">{page}/{totalPages}</span>
              <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages} className="btn-secondary px-2 py-1 text-xs disabled:opacity-40"><ChevronRight className="w-3 h-3" /></button>
            </div>
          </div>
        )}
      </div>

      {/* ── Results Modal (80vh) ── */}
      {modalModelId && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={() => setModalModelId(null)}>
          <div className="bg-white rounded-2xl shadow-2xl w-[92vw] max-w-6xl h-[80vh] flex flex-col" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
              <div className="flex items-center gap-3">
                <FlaskConical className="w-5 h-5 text-indigo-600" />
                <h2 className="font-semibold text-gray-900">A/B Test Detail</h2>
                <span className="text-xs text-gray-400">Baseline (known wrapper) vs Model (tiered extractor)</span>
              </div>
              <div className="flex items-center gap-3">
                <label className="text-xs text-gray-500">Sites:</label>
                <input type="number" min={1} max={200} value={sampleSize} onChange={e => setSampleSize(Math.min(200, Math.max(1, +e.target.value)))}
                  className="w-14 border border-gray-200 rounded px-2 py-1 text-xs" />
                <button onClick={() => executeMut.mutate(modalModelId!)} disabled={executeMut.isPending}
                  className="btn-primary text-xs flex items-center gap-1 px-3 py-1.5">
                  {executeMut.isPending ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />} Run Test</button>
                {codexLogLines.length > 0 && (
                  <button onClick={() => setShowCodexLog(!showCodexLog)}
                    className={`text-xs px-2 py-1 rounded ${showCodexLog ? 'bg-gray-800 text-green-400' : 'bg-gray-100 text-gray-500'}`}>
                    {codexRunning && <Loader2 className="w-3 h-3 animate-spin inline mr-1" />}
                    Log
                  </button>
                )}
                <button onClick={() => setModalModelId(null)} className="text-gray-400 hover:text-gray-600 text-xl px-2">&times;</button>
              </div>
            </div>
            <div className="flex-1 overflow-y-auto p-6 space-y-6">
              {testRuns?.items?.map((run: TestRun) => {
                const rd = run.results_detail;
                const sites = rd?.sites ?? [];
                const s = rd?.summary;
                const progress = rd?.progress;
                const isRunning = run.status === 'running';
                const pctDone = progress ? Math.round((progress.done / progress.total) * 100) : (run.status === 'completed' ? 100 : 0);

                return (
                  <div key={run.id} className="space-y-4">
                    {/* Header + progress */}
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${
                            run.status === 'completed' ? 'bg-green-100 text-green-700' :
                            isRunning ? 'bg-blue-100 text-blue-700 animate-pulse' : 'bg-gray-100 text-gray-600'
                          }`}>
                            {isRunning ? <Loader2 className="w-3 h-3 animate-spin" /> :
                             run.status === 'completed' ? <CheckCircle className="w-3 h-3" /> : <Clock className="w-3 h-3" />}
                            {run.status}
                          </span>
                          <span className="text-xs text-gray-500">{run.test_name}</span>
                          {isRunning && progress && <span className="text-xs text-blue-600 font-medium">{progress.done}/{progress.total} sites</span>}
                        </div>
                        {run.accuracy !== null && (
                          <span className={`text-xl font-bold ${run.accuracy > 0.5 ? 'text-green-600' : 'text-red-500'}`}>
                            {(run.accuracy * 100).toFixed(0)}% model success
                          </span>
                        )}
                      </div>
                      {(isRunning || pctDone > 0) && (
                        <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                          <div className={`h-full rounded-full transition-all duration-500 ${isRunning ? 'bg-blue-500' : 'bg-green-500'}`} style={{ width: `${pctDone}%` }} />
                        </div>
                      )}
                    </div>

                    {/* Summary cards */}
                    {s && (
                      <div className="grid grid-cols-4 gap-3">
                        <div className="card p-3 text-center">
                          <div className="text-2xl font-bold text-gray-900">{s.total_sites}</div>
                          <div className="text-xs text-gray-500">Sites tested</div>
                        </div>
                        <div className="card p-3 text-center">
                          <div className="text-2xl font-bold text-green-600">{s.model_extracted}</div>
                          <div className="text-xs text-gray-500">Model extracted</div>
                        </div>
                        <div className="card p-3 text-center">
                          <div className="text-xs text-gray-500 mb-1">Jobs found</div>
                          <div className="flex items-center justify-center gap-2 text-sm">
                            <span className="text-amber-600 font-medium">B: {s.jobs.baseline_total}</span>
                            <span className="text-gray-300">|</span>
                            <span className="text-blue-600 font-medium">M: {s.jobs.model_total}</span>
                          </div>
                          <div className="text-xs text-gray-400 mt-0.5">Ratio: {s.jobs.ratio}x</div>
                        </div>
                        <div className="card p-3 text-center">
                          <div className="text-xs text-gray-500 mb-1">Core fields</div>
                          <div className="flex items-center justify-center gap-2 text-sm">
                            <span className="text-amber-600 font-medium">B: {s.quality.baseline_core_complete}</span>
                            <span className="text-gray-300">|</span>
                            <span className="text-blue-600 font-medium">M: {s.quality.model_core_complete}</span>
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Phase breakdown */}
                    {s && (s.regression || s.exploration) && (
                      <div className="flex gap-3">
                        {s.regression && (
                          <div className="flex-1 p-3 bg-amber-50 rounded-lg border border-amber-200">
                            <div className="text-xs font-medium text-amber-700 mb-1">Regression Suite (fixed sites)</div>
                            <div className="flex items-center gap-3 text-sm">
                              <span className={`font-bold ${s.regression.accuracy >= 0.6 ? 'text-green-600' : 'text-red-500'}`}>
                                {(s.regression.accuracy * 100).toFixed(0)}%
                              </span>
                              <span className="text-gray-500 text-xs">{s.regression.model_extracted}/{s.regression.total_sites} sites</span>
                              <span className="text-gray-500 text-xs">{s.regression.jobs?.model_total || 0} jobs</span>
                            </div>
                          </div>
                        )}
                        {s.exploration && (
                          <div className="flex-1 p-3 bg-blue-50 rounded-lg border border-blue-200">
                            <div className="text-xs font-medium text-blue-700 mb-1">Exploration Suite (unseen sites)</div>
                            <div className="flex items-center gap-3 text-sm">
                              <span className={`font-bold ${s.exploration.accuracy >= 0.5 ? 'text-green-600' : 'text-amber-500'}`}>
                                {(s.exploration.accuracy * 100).toFixed(0)}%
                              </span>
                              <span className="text-gray-500 text-xs">{s.exploration.model_extracted}/{s.exploration.total_sites} sites</span>
                              <span className="text-gray-500 text-xs">{s.exploration.jobs?.model_total || 0} jobs</span>
                            </div>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Match + Tier breakdown */}
                    {s && (
                      <div className="flex gap-3">
                        <div className="flex-1 p-3 bg-gray-50 rounded-lg">
                          <div className="text-xs font-medium text-gray-600 mb-2">Match Outcome</div>
                          <div className="flex flex-wrap gap-2">
                            {Object.entries(s.match_breakdown).map(([k, v]) => {
                              const ml = MATCH_LABELS[k] || { label: k, cls: 'text-gray-500 bg-gray-100' };
                              return <span key={k} className={`px-2 py-0.5 rounded text-xs font-medium ${ml.cls}`}>{ml.label}: {v}</span>;
                            })}
                          </div>
                        </div>
                        <div className="flex-1 p-3 bg-gray-50 rounded-lg">
                          <div className="text-xs font-medium text-gray-600 mb-2">Tier Used</div>
                          <div className="flex flex-wrap gap-2">
                            {Object.entries(s.tier_breakdown).map(([k, v]) => (
                              <span key={k} className={`px-2 py-0.5 rounded text-xs font-mono font-medium ${
                                k.startsWith('tier1') ? 'bg-green-100 text-green-700' :
                                k.startsWith('tier2') ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600'
                              }`}>{k}: {v}</span>
                            ))}
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Per-site table */}
                    {sites.length > 0 && (
                      <div className="overflow-x-auto border border-gray-200 rounded-lg">
                        <table className="w-full text-xs">
                          <thead><tr className="bg-gray-50 border-b border-gray-200">
                            <th className="text-left py-1.5 px-3 w-8">Match</th>
                            <th className="text-left py-1.5 px-3">Company</th>
                            <th className="text-left py-1.5 px-3">Tier</th>
                            <th className="text-center py-1.5 px-2 bg-amber-50/50">Baseline</th>
                            <th className="text-center py-1.5 px-2 bg-emerald-50/50">Champ</th>
                            <th className="text-center py-1.5 px-2 bg-blue-50/50">Challenger</th>
                            <th className="text-left py-1.5 px-3">Details / Reason</th>
                            <th className="text-right py-1.5 px-3 w-10">Notes</th>
                          </tr></thead>
                          <tbody>
                            {sites.map((r, i) => {
                              const ml = MATCH_LABELS[r.match] || { label: r.match, cls: 'text-gray-500' };
                              return (
                                <tr key={i} className={`border-b border-gray-100 ${
                                  r.match === 'model_equal_or_better' || r.match === 'model_only' ? '' :
                                  r.match === 'http_error' || r.match === 'both_failed' ? 'bg-gray-50/50' : 'bg-red-50/30'
                                }`}>
                                  <td className="py-1.5 px-3"><span className={`px-1.5 py-0.5 rounded text-[10px] font-medium whitespace-nowrap ${ml.cls}`}>{ml.label}</span></td>
                                  <td className="py-1.5 px-3 max-w-[140px]">
                                    <a href={r.url} target="_blank" rel="noopener noreferrer"
                                      className="text-blue-600 hover:underline truncate block" title={r.url}>{r.company}</a>
                                    {r.domain && <div className="text-[10px] text-gray-400 truncate">{r.domain}</div>}
                                    {r.model.url_found && r.model.url_found !== r.url && (
                                      <div className="text-[10px] text-indigo-400 truncate" title={r.model.url_found}>
                                        Found: {r.model.discovery_method?.split(':')[0]}
                                      </div>
                                    )}
                                  </td>
                                  <td className="py-1.5 px-3">
                                    {r.model.tier_used ? (
                                      <span className={`font-mono text-[10px] px-1 py-0.5 rounded ${
                                        r.model.tier_used.startsWith('tier1') ? 'bg-green-100 text-green-700' :
                                        r.model.tier_used.startsWith('tier2') ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600'
                                      }`}>{r.model.tier_used}</span>
                                    ) : <span className="text-gray-300">-</span>}
                                  </td>
                                  <td className="py-1.5 px-2 text-center font-medium text-amber-600 bg-amber-50/30">
                                    {(() => {
                                      const complete = r.baseline?.fields?._core_complete ?? r.baseline?.jobs ?? 0;
                                      if (complete > 0) return (
                                        <button className="hover:underline cursor-pointer"
                                          onClick={() => setJobDetailPopup({ company: r.company, phase: 'Baseline', jobs: r.baseline.extracted_jobs || [], siteUrl: r.url, wrapper: r.baseline.full_wrapper })}>
                                          {complete}
                                        </button>
                                      );
                                      return r.baseline?.jobs ? <span className="text-gray-400">{r.baseline.jobs}*</span> : '-';
                                    })()}
                                  </td>
                                  <td className="py-1.5 px-2 text-center font-medium text-emerald-600 bg-emerald-50/30">
                                    {(() => {
                                      const complete = r.champion?.fields?._core_complete ?? 0;
                                      const raw = r.champion?.jobs ?? 0;
                                      if (complete > 0) return (
                                        <button className="hover:underline cursor-pointer"
                                          onClick={() => setJobDetailPopup({ company: r.company, phase: 'Champion', jobs: r.champion?.extracted_jobs || [] })}>
                                          {complete}{raw > complete ? <span className="text-gray-400 text-[10px] ml-0.5">/{raw}</span> : null}
                                        </button>
                                      );
                                      if (raw > 0) return <span className="text-gray-400" title="No core fields complete">{raw}*</span>;
                                      return '-';
                                    })()}
                                  </td>
                                  <td className="py-1.5 px-2 text-center font-medium text-blue-600 bg-blue-50/30">
                                    {(() => {
                                      const complete = r.model?.fields?._core_complete ?? 0;
                                      const raw = r.model?.jobs ?? 0;
                                      if (complete > 0) return (
                                        <button className="hover:underline cursor-pointer"
                                          onClick={() => setJobDetailPopup({ company: r.company, phase: 'Challenger', jobs: r.model.extracted_jobs || [] })}>
                                          {complete}{raw > complete ? <span className="text-gray-400 text-[10px] ml-0.5">/{raw}</span> : null}
                                        </button>
                                      );
                                      if (raw > 0) return <span className="text-gray-400" title="No core fields complete">{raw}*</span>;
                                      return '-';
                                    })()}
                                  </td>
                                  <td className="py-1.5 px-3 text-gray-500 max-w-[280px]">
                                    {(() => {
                                      // Explain why this match outcome occurred
                                      if (r.match === 'both_failed') {
                                        if (!r.http_ok) return <span className="text-red-400">Site unreachable</span>;
                                        const bSel = r.baseline?.selectors_used?.boundary || '';
                                        return <span className="text-gray-400">
                                          Neither baseline nor model found jobs. Likely JS-rendered (needs Playwright).
                                          {bSel && <span className="block text-[10px] font-mono mt-0.5 truncate">Known selector: {bSel}</span>}
                                        </span>;
                                      }
                                      if (r.match === 'http_error')
                                        return <span className="text-red-400">{r.model.error || 'HTTP error'}</span>;
                                      if (r.match === 'model_failed')
                                        return <span className="text-red-400">
                                          Baseline found {r.baseline?.fields?._core_complete ?? r.baseline.jobs} jobs but model tiers failed.
                                          {r.model.url_found && r.model.url_found !== r.url && (
                                            <span className="block text-[10px] mt-0.5 text-amber-500">
                                              Model discovered: {r.model.url_found?.substring(0, 50)}
                                              {r.model.url_found !== r.baseline.url_used && ' (different URL!)'}
                                            </span>
                                          )}
                                          <span className="block text-[10px]">B: {r.baseline.sample_titles?.slice(0, 2).join(', ')}</span>
                                        </span>;
                                      {/* Use complete job counts (core fields) for all details text */}
                                      const mc = r.model?.fields?._core_complete ?? 0;
                                      const mr = r.model?.jobs ?? 0;
                                      const bc = r.baseline?.fields?._core_complete ?? r.baseline?.jobs ?? 0;
                                      const mLabel = mc === mr ? `${mc}` : `${mc} complete of ${mr}`;
                                      if (r.match === 'model_worse')
                                        return <span className="text-amber-600">
                                          Model {mLabel} vs baseline {bc}.
                                          <span className="block text-[10px] mt-0.5">M: {r.model.sample_titles?.slice(0, 2).join(', ')}</span>
                                          <span className="block text-[10px]">B: {r.baseline.sample_titles?.slice(0, 2).join(', ')}</span>
                                        </span>;
                                      if (r.match === 'model_equal_or_better')
                                        return <span className="text-green-600">
                                          Model {mc} {mc > bc ? '>' : '='} baseline {bc}.
                                          <span className="block text-[10px] mt-0.5 text-gray-500">M: {r.model.sample_titles?.slice(0, 2).join(', ')}</span>
                                        </span>;
                                      if (r.match === 'model_only')
                                        return <span className="text-green-600">
                                          Model found {mLabel} jobs, baseline found none.
                                          <span className="block text-[10px] mt-0.5 text-gray-500">{r.model.sample_titles?.slice(0, 2).join(', ')}</span>
                                        </span>;
                                      if (r.match === 'partial')
                                        return <span className="text-amber-500">
                                          Model found {mLabel} of {bc} baseline jobs ({bc > 0 ? Math.round(mc / bc * 100) : 0}%).
                                          <span className="block text-[10px] mt-0.5">M: {r.model.sample_titles?.slice(0, 2).join(', ')}</span>
                                        </span>;
                                      return <span className="text-gray-400">{r.model.error || '-'}</span>;
                                    })()}
                                  </td>
                                  <td className="py-1.5 px-3 text-right">
                                    <button
                                      onClick={() => setFeedbackTarget({ runId: run.id, siteUrl: r.url, company: r.company })}
                                      className="text-gray-400 hover:text-indigo-600 transition-colors"
                                      title="Add/edit feedback"
                                    >
                                      {siteFeedbackCounts[r.url] ? <Pencil className="w-3.5 h-3.5" /> : <Plus className="w-3.5 h-3.5" />}
                                    </button>
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                );
              })}
              {(!testRuns?.items || testRuns.items.length === 0) && !showCodexLog && (
                <div className="text-sm text-gray-400 py-12 text-center">No test runs yet. Click "Run Test" to validate.</div>
              )}

              {/* Codex Log Panel */}
              {showCodexLog && (
                <div className="mt-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <Sparkles className="w-4 h-4 text-purple-500" />
                      <span className="text-sm font-medium text-gray-700">Codex Auto-Improve Log</span>
                      {codexRunning && <span className="text-xs text-green-500 animate-pulse flex items-center gap-1"><Loader2 className="w-3 h-3 animate-spin" /> Running</span>}
                      {!codexRunning && codexLogLines.length > 1 && <span className="text-xs text-gray-400">Completed</span>}
                    </div>
                    <button onClick={() => setShowCodexLog(false)} className="text-xs text-gray-400 hover:text-gray-600">Hide</button>
                  </div>
                  <div
                    ref={codexLogRef}
                    className="bg-gray-900 text-green-400 font-mono text-xs p-4 rounded-lg overflow-y-auto max-h-[300px] whitespace-pre-wrap"
                  >
                    {codexLogLines.map((line, i) => (
                      <div key={i} className={
                        line.includes('error') || line.includes('Error') || line.includes('FAIL') ? 'text-red-400' :
                        line.includes('Starting') || line.includes('completed') || line.includes('SUCCESS') ? 'text-green-300' :
                        line.includes('[') && line.includes(']') ? 'text-blue-300' :
                        'text-gray-300'
                      }>{line}</div>
                    ))}
                    {codexRunning && <div className="text-green-500 animate-pulse mt-1">_</div>}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* ── Job Detail Popup ── */}
      {jobDetailPopup && (() => {
        const isComplete = (job: ExtractedJob) =>
          !!job.title && !!job.source_url && !!job.location_raw && !!job.description && job.description.length > 50;
        const completeJobs = jobDetailPopup.jobs.filter(isComplete);
        const incompleteJobs = jobDetailPopup.jobs.filter(j => !isComplete(j));
        const toggleExpand = (idx: number) => {
          setExpandedJobs(prev => {
            const next = new Set(prev);
            if (next.has(idx)) next.delete(idx); else next.add(idx);
            return next;
          });
        };
        const renderJobCard = (job: ExtractedJob, idx: number) => {
          const isExpanded = expandedJobs.has(idx);
          const descPreview = job.description
            ? (job.description.length > 150 ? job.description.slice(0, 150) + '...' : job.description)
            : null;
          return (
            <div key={idx} className="border border-gray-200 rounded-lg p-3 hover:bg-gray-50">
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1 min-w-0">
                  <div className="font-medium text-gray-900 text-sm">{job.title || '(no title)'}</div>
                  <div className="text-xs text-gray-600 mt-0.5">{jobDetailPopup.company}</div>
                  <div className="text-xs mt-0.5">
                    {job.location_raw ? (
                      <span className="text-gray-600">{job.location_raw}</span>
                    ) : (
                      <span className="text-gray-400 italic">Not extracted</span>
                    )}
                  </div>
                  {!isExpanded && descPreview && (
                    <div className="mt-1 text-xs text-gray-500">{descPreview}</div>
                  )}
                  {isExpanded && (
                    <div className="mt-2 space-y-2 text-xs">
                      {job.description && (
                        <div className="text-gray-600 bg-gray-50 rounded p-2 whitespace-pre-wrap">{job.description}</div>
                      )}
                      <div className="flex flex-wrap gap-x-4 gap-y-1">
                        {job.salary_raw && (
                          <span className="text-green-700"><span className="text-gray-400">Salary:</span> {job.salary_raw}</span>
                        )}
                        {job.employment_type && (
                          <span className="text-blue-600"><span className="text-gray-400">Type:</span> {job.employment_type}</span>
                        )}
                        {job.department && (
                          <span className="text-purple-600"><span className="text-gray-400">Dept:</span> {job.department}</span>
                        )}
                        {job.closing_date && (
                          <span className="text-orange-600"><span className="text-gray-400">Closes:</span> {job.closing_date}</span>
                        )}
                        {job.listed_date && (
                          <span className="text-gray-500"><span className="text-gray-400">Listed:</span> {job.listed_date}</span>
                        )}
                        {job.extraction_method && (
                          <span className="text-gray-500"><span className="text-gray-400">Method:</span> <span className="font-mono">{job.extraction_method}</span></span>
                        )}
                        {job.extraction_confidence != null && (
                          <span className="text-gray-500"><span className="text-gray-400">Confidence:</span> {(job.extraction_confidence * 100).toFixed(0)}%</span>
                        )}
                      </div>
                    </div>
                  )}
                </div>
                <div className="flex flex-col items-end gap-1 flex-shrink-0">
                  {job.source_url && (
                    <a href={job.source_url} target="_blank" rel="noopener noreferrer"
                      className="text-[10px] text-blue-500 hover:underline">View External Job Page</a>
                  )}
                  <button onClick={() => toggleExpand(idx)} className="text-gray-400 hover:text-gray-600 p-0.5" title={isExpanded ? 'Collapse' : 'Expand'}>
                    {isExpanded ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
                  </button>
                </div>
              </div>
            </div>
          );
        };
        return (
          <div className="fixed inset-0 z-[55] flex items-center justify-center bg-black/30" onClick={() => { setJobDetailPopup(null); setExpandedJobs(new Set()); }}>
            <div className="bg-white rounded-xl shadow-2xl w-[800px] max-h-[80vh] flex flex-col" onClick={e => e.stopPropagation()}>
              <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200">
                <div>
                  <h3 className="font-semibold text-gray-900 text-sm">{jobDetailPopup.phase} — Extracted Jobs</h3>
                  <p className="text-xs text-gray-500">{jobDetailPopup.company} — {completeJobs.length} complete, {incompleteJobs.length} incomplete of {jobDetailPopup.jobs.length} total</p>
                </div>
                <div className="flex items-center gap-2">
                  {jobDetailPopup.phase === 'Baseline' && jobDetailPopup.wrapper && (
                    <button
                      onClick={() => {
                        const w = jobDetailPopup.wrapper || {};
                        const siteUrl = jobDetailPopup.siteUrl || '';
                        // Build a detailed wrapper view showing each selector and what it maps to
                        const selectorFields = [
                          { key: 'record_boundary_path', label: 'Job Card Boundary', desc: 'CSS/XPath selector for each job card container' },
                          { key: 'min_container_path', label: 'Container', desc: 'Parent container holding all job cards' },
                          { key: 'job_title_path', label: 'Job Title', desc: 'Selector for job title text (relative to boundary)' },
                          { key: 'row_details_page_link_path', label: 'Detail Page Link', desc: 'Selector for the link to job detail page' },
                          { key: 'job_title_url_pattern', label: 'URL Pattern', desc: 'Regex pattern for matching job URLs' },
                          { key: 'row_location_paths', label: 'Location', desc: 'Selector(s) for job location' },
                          { key: 'row_salary_paths', label: 'Salary', desc: 'Selector(s) for salary information' },
                          { key: 'row_description_paths', label: 'Description', desc: 'Selector(s) for job description/summary' },
                          { key: 'row_job_type_paths', label: 'Job Type', desc: 'Selector(s) for employment type' },
                          { key: 'row_closing_date_path', label: 'Closing Date', desc: 'Selector for application deadline' },
                          { key: 'row_listed_date_path', label: 'Listed Date', desc: 'Selector for posting date' },
                          { key: 'next_page_path', label: 'Next Page', desc: 'Selector for pagination link' },
                          { key: 'details_page_description_paths', label: 'Detail: Description', desc: 'Description selector on detail page' },
                          { key: 'details_page_location_paths', label: 'Detail: Location', desc: 'Location selector on detail page' },
                          { key: 'details_page_salary_path', label: 'Detail: Salary', desc: 'Salary selector on detail page' },
                          { key: 'details_page_job_type_paths', label: 'Detail: Job Type', desc: 'Job type selector on detail page' },
                        ];
                        const rows = selectorFields.map(f => {
                          const val = w[f.key];
                          const isEmpty = !val || val === 'null' || val === '' || (Array.isArray(val) && val.length === 0);
                          const display = isEmpty ? '<span style="color:#6c7086">— not configured —</span>'
                            : `<code style="background:#313244;padding:2px 6px;border-radius:4px;color:#a6e3a1">${Array.isArray(val) ? val.join(', ') : val}</code>`;
                          return `<tr><td style="padding:6px 12px;border-bottom:1px solid #313244;color:#cba6f7;font-weight:600;white-space:nowrap">${f.label}</td><td style="padding:6px 12px;border-bottom:1px solid #313244">${display}</td><td style="padding:6px 12px;border-bottom:1px solid #313244;color:#6c7086;font-size:11px">${f.desc}</td></tr>`;
                        }).join('');
                        // Show extracted jobs with field attribution
                        const jobRows = (jobDetailPopup.jobs || []).slice(0, 20).map((j, i) => {
                          const fields = [
                            { name: 'Title', val: j.title, sel: w.job_title_path },
                            { name: 'URL', val: j.source_url?.substring(0, 60), sel: w.row_details_page_link_path || w.job_title_url_pattern },
                            { name: 'Location', val: j.location_raw, sel: Array.isArray(w.row_location_paths) ? w.row_location_paths.join(', ') : w.row_location_paths },
                            { name: 'Salary', val: j.salary_raw, sel: Array.isArray(w.row_salary_paths) ? w.row_salary_paths.join(', ') : w.row_salary_paths },
                            { name: 'Type', val: j.employment_type, sel: Array.isArray(w.row_job_type_paths) ? w.row_job_type_paths.join(', ') : w.row_job_type_paths },
                            { name: 'Description', val: j.description ? j.description.substring(0, 80) + '...' : null, sel: Array.isArray(w.row_description_paths) ? w.row_description_paths.join(', ') : w.row_description_paths },
                          ];
                          const fieldRows = fields.map(f => {
                            const status = f.val ? `<span style="color:#a6e3a1">✓</span> ${String(f.val).substring(0, 80).replace(/</g, '&lt;')}` : `<span style="color:#f38ba8">✗ not extracted</span>`;
                            const selDisplay = f.sel && f.sel !== 'null' ? `<code style="color:#89b4fa;font-size:10px">${f.sel}</code>` : '<span style="color:#6c7086;font-size:10px">no selector</span>';
                            return `<tr><td style="padding:3px 8px;color:#cba6f7;font-size:11px">${f.name}</td><td style="padding:3px 8px;font-size:11px">${status}</td><td style="padding:3px 8px">${selDisplay}</td></tr>`;
                          }).join('');
                          return `<div style="margin-bottom:16px"><h4 style="color:#f5e0dc;margin:0 0 4px">${i + 1}. ${(j.title || 'Untitled').replace(/</g, '&lt;')}</h4><table style="width:100%;border-collapse:collapse">${fieldRows}</table></div>`;
                        }).join('');
                        const html = `<html><head><title>Wrapper: ${jobDetailPopup.company}</title><style>body{font-family:system-ui,sans-serif;padding:24px;background:#1e1e2e;color:#cdd6f4;max-width:1200px;margin:0 auto}h2{color:#f5c2e7;margin-bottom:4px}h3{color:#89b4fa;margin-top:24px}a{color:#89b4fa}code{font-family:'SF Mono',Monaco,monospace;font-size:12px}table{border-collapse:collapse;width:100%}</style></head><body><h2>Wrapper Config: ${jobDetailPopup.company}</h2><p style="color:#6c7086">Site URL: <a href="${siteUrl}">${siteUrl}</a></p><h3>Selector Configuration</h3><table>${rows}</table><h3>Extracted Jobs — Field Attribution</h3><p style="color:#6c7086;font-size:12px">Shows which wrapper selector produced each field value (or failed to)</p>${jobRows || '<p style="color:#6c7086">No job data available</p>'}</body></html>`;
                        const blob = new Blob([html], { type: 'text/html' });
                        window.open(URL.createObjectURL(blob), '_blank');
                      }}
                      className="text-xs flex items-center gap-1 px-2 py-1 rounded bg-amber-50 text-amber-700 hover:bg-amber-100 border border-amber-200"
                      title="View Jobstream wrapper selectors and field attribution for this site"
                    >
                      View Wrapper Details
                    </button>
                  )}
                  <button onClick={() => { setJobDetailPopup(null); setExpandedJobs(new Set()); }} className="text-gray-400 hover:text-gray-600 text-lg">&times;</button>
                </div>
              </div>
              <div className="flex-1 overflow-y-auto p-4">
                {jobDetailPopup.jobs.length === 0 ? (
                  <div className="text-center text-gray-400 py-8">No job data stored for this test run. Re-run the test to capture job details.</div>
                ) : (
                  <div className="space-y-3">
                    {completeJobs.map((job, i) => renderJobCard(job, i))}
                    {incompleteJobs.length > 0 && (
                      <>
                        <div className="border-t border-gray-200 pt-3 mt-4">
                          <div className="text-xs font-medium text-amber-600 mb-2">Incomplete Extractions ({incompleteJobs.length})</div>
                        </div>
                        {incompleteJobs.map((job, i) => renderJobCard(job, completeJobs.length + i))}
                      </>
                    )}
                  </div>
                )}
              </div>
              <div className="px-5 py-2 border-t border-gray-100 text-right">
                <span className="text-xs text-gray-400">{jobDetailPopup.jobs.filter(j => j.description).length}/{jobDetailPopup.jobs.length} have descriptions, {jobDetailPopup.jobs.filter(j => j.location_raw).length} have location, {jobDetailPopup.jobs.filter(j => j.salary_raw).length} have salary</span>
              </div>
            </div>
          </div>
        );
      })()}

      {/* ── Model Details Modal ── */}
      {modelDetailsId && (() => {
        const model = items.find(m => m.id === modelDetailsId);
        const run = model?.latest_test_run;
        const rd = run?.results_detail;
        const summary = rd?.summary;
        const champComposite = summary?.champion_composite;
        const challComposite = summary?.challenger_composite;

        const Section = ({ title, children }: { title: string; children: React.ReactNode }) => (
          <div className="mb-6">
            <h4 className="text-sm font-semibold text-gray-800 mb-2 border-b border-gray-100 pb-1">{title}</h4>
            <div className="text-sm text-gray-600 leading-relaxed space-y-2">{children}</div>
          </div>
        );
        const Code = ({ children }: { children: React.ReactNode }) => (
          <code className="text-xs bg-gray-100 text-gray-700 px-1.5 py-0.5 rounded font-mono">{children}</code>
        );
        const Row = ({ label, value, color }: { label: string; value: string | number; color?: string }) => (
          <div className="flex items-center gap-2 py-0.5">
            <span className="text-gray-500 w-40 flex-shrink-0">{label}</span>
            <span className={`font-medium ${color || 'text-gray-700'}`}>{value}</span>
          </div>
        );

        const baselineSummary = model?.baseline_summary;
        const champSummary = model?.champion_summary;
        const challSummary = model?.model_summary;

        return (
          <div className="fixed inset-0 z-[55] flex items-center justify-center bg-black/30" onClick={() => setModelDetailsId(null)}>
            <div className="bg-white rounded-2xl shadow-2xl flex flex-col" style={{ width: '80vw', height: '80vh' }} onClick={e => e.stopPropagation()}>
              <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
                <div className="flex items-center gap-3">
                  <Info className="w-5 h-5 text-indigo-600" />
                  <h2 className="font-semibold text-gray-900">Model Details</h2>
                  <span className="text-xs text-gray-400">{model?.name}</span>
                </div>
                <button onClick={() => setModelDetailsId(null)} className="text-gray-400 hover:text-gray-600 text-xl px-2">&times;</button>
              </div>
              <div className="flex border-b border-gray-200">
                {(['baseline', 'champion', 'challenger'] as const).map(tab => (
                  <button
                    key={tab}
                    onClick={() => setModelDetailTab(tab)}
                    className={`px-6 py-2.5 text-sm font-medium capitalize transition-colors ${
                      modelDetailTab === tab
                        ? 'border-b-2 border-indigo-600 text-indigo-700 bg-indigo-50/30'
                        : 'text-gray-500 hover:text-gray-700 hover:bg-gray-50'
                    }`}
                  >
                    {tab === 'baseline' ? 'Jobstream Wrapper' : tab.charAt(0).toUpperCase() + tab.slice(1)}
                  </button>
                ))}
              </div>
              <div className="flex-1 overflow-y-auto p-6">

                {/* ═══ BASELINE TAB ═══ */}
                {modelDetailTab === 'baseline' && (
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900 mb-1">Jobstream Wrapper (Baseline)</h3>
                    <p className="text-sm text-gray-500 mb-6">Pre-configured wrapper selectors imported from the production Jobstream crawling system. These are the "ground truth" used to evaluate model performance.</p>

                    <Section title="How It Works">
                      <p>Each test site has a known set of CSS/XPath selectors stored in the <Code>site_wrapper_test_data</Code> table. These selectors were originally configured by the Jobstream production team and represent the "correct" way to extract jobs from each specific site.</p>
                      <p>During a test run, the baseline phase applies these selectors directly to the test URL's HTML to extract jobs. This represents the best possible extraction for that specific site — the selectors are custom-tuned per site.</p>
                    </Section>

                    <Section title="Selector Fields">
                      <div className="bg-gray-50 rounded-lg p-3 font-mono text-xs space-y-1">
                        <div><span className="text-purple-600">boundary</span> — Container element holding a single job card</div>
                        <div><span className="text-purple-600">title</span> — Job title selector (relative to boundary)</div>
                        <div><span className="text-purple-600">url</span> — Detail page link selector</div>
                        <div><span className="text-purple-600">location</span> — Location text selector</div>
                        <div><span className="text-purple-600">salary</span> — Salary text selector</div>
                        <div><span className="text-purple-600">description</span> — Description/summary selector</div>
                        <div><span className="text-purple-600">next_page</span> — Pagination link selector</div>
                        <div><span className="text-purple-600">employment_type</span> — Full-time/Part-time selector</div>
                      </div>
                    </Section>

                    <Section title="Inputs">
                      <p><strong>URL:</strong> The known career page URL from <Code>site_url_test_data</Code></p>
                      <p><strong>HTML:</strong> Fetched from the URL via HTTP (no Playwright rendering)</p>
                      <p><strong>Selectors:</strong> Pre-configured CSS/XPath paths from <Code>site_wrapper_test_data</Code></p>
                    </Section>

                    <Section title="Outputs">
                      <p>A list of structured job objects, each containing: title, source_url, location_raw, salary_raw, employment_type, description. These are compared 1:1 against model/champion outputs to evaluate extraction quality.</p>
                    </Section>

                    <Section title="Limitations">
                      <p><strong>Stale selectors:</strong> Sites redesign over time. Selectors configured months ago may no longer work if the site's HTML structure changed.</p>
                      <p><strong>No discovery:</strong> The baseline doesn't discover the career page — it uses a pre-configured URL. If the URL has changed, baseline extraction fails.</p>
                      <p><strong>No JS rendering:</strong> Baseline uses static HTTP fetch. JS-heavy sites (React, Angular, Next.js) may return empty/minimal HTML.</p>
                    </Section>

                    {baselineSummary && (
                      <Section title="Latest Test Results">
                        <Row label="Sites with extractions" value={`${baselineSummary.sites_extracted}/${baselineSummary.sites_tested}`} />
                        <Row label="Total jobs extracted" value={baselineSummary.total_jobs} />
                        <Row label="Field completeness" value={`${baselineSummary.quality_score}%`} color={baselineSummary.quality_score >= 50 ? 'text-green-600' : 'text-amber-600'} />
                        <Row label="Core fields complete" value={baselineSummary.core_complete} />
                      </Section>
                    )}
                  </div>
                )}

                {/* ═══ CHAMPION TAB ═══ */}
                {modelDetailTab === 'champion' && (
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900 mb-1">{model?.labels?.champion || 'Champion'}</h3>
                    <p className="text-sm text-gray-500 mb-6">The current live/production extraction model. Deployed and actively extracting jobs from company career pages.</p>

                    <Section title="Architecture">
                      <p>The champion is a <strong>tiered extraction engine</strong> that inherits from TieredExtractorV16 (the stable base). It runs a multi-stage pipeline on each career page:</p>
                      <div className="bg-gray-50 rounded-lg p-4 space-y-3 mt-2">
                        <div className="flex gap-3">
                          <span className="bg-emerald-100 text-emerald-700 px-2 py-0.5 rounded text-xs font-bold flex-shrink-0">Tier 1</span>
                          <div><strong>ATS Templates</strong> — Pattern-matched extraction for known ATS platforms (Greenhouse, Oracle CX, Salesforce, Workday, Lever, Breezy HR, Dayforce, UltiPro, GrowHire). Uses platform-specific APIs and DOM structures. Highest accuracy (~95%) when the platform is detected.</div>
                        </div>
                        <div className="flex gap-3">
                          <span className="bg-blue-100 text-blue-700 px-2 py-0.5 rounded text-xs font-bold flex-shrink-0">Tier 2</span>
                          <div><strong>Heuristic Extraction</strong> — Container scoring based on: apply-button count matching, repeating child signatures, job-class CSS patterns, Elementor/card detection. Scores candidate containers and extracts from the highest-scoring one. Includes vocabulary validation — &ge;30% of titles must contain a job-title noun.</div>
                        </div>
                        <div className="flex gap-3">
                          <span className="bg-purple-100 text-purple-700 px-2 py-0.5 rounded text-xs font-bold flex-shrink-0">Tier 0</span>
                          <div><strong>Structured Data</strong> — JSON-LD <Code>JobPosting</Code> schema, embedded <Code>__NEXT_DATA__</Code> state, <Code>window.__remixContext</Code>. Parsed from script tags. Strict validation rejects taxonomy/category labels.</div>
                        </div>
                        <div className="flex gap-3">
                          <span className="bg-gray-200 text-gray-700 px-2 py-0.5 rounded text-xs font-bold flex-shrink-0">Fallback</span>
                          <div><strong>DOM Fallbacks</strong> — Job links, accordion sections, heading rows, repeating CSS-class rows. Lower confidence, only used when Tiers 0-2 produce &lt;3 jobs.</div>
                        </div>
                      </div>
                    </Section>

                    <Section title="Discovery Phase">
                      <p>Before extraction, the <strong>CareerPageFinder</strong> discovers the career/jobs listing page URL from just a domain name. It uses:</p>
                      <ul className="list-disc pl-5 space-y-1 mt-1">
                        <li><strong>Common path probing:</strong> /careers, /jobs, /job-openings, /requisitions, localized paths (/lowongan, /karir, /kerjaya)</li>
                        <li><strong>Homepage link crawling:</strong> Follows links matching career-related keywords</li>
                        <li><strong>ATS path detection:</strong> Greenhouse embed boards, Oracle CandidateExperience, Salesforce fRecruit, Dayforce CandidatePortal</li>
                        <li><strong>Bad target rejection:</strong> Penalizes 404 pages, job detail pages, RSS feeds, login pages, PDFs</li>
                        <li><strong>Sub-page promotion:</strong> If the discovered page is a career hub, follows links to the actual listing page</li>
                      </ul>
                    </Section>

                    <Section title="Candidate Selection">
                      <p>When multiple extraction methods produce results, the system picks the best candidate set using:</p>
                      <ul className="list-disc pl-5 space-y-1 mt-1">
                        <li><strong>Jobset validation:</strong> Title uniqueness &gt;60%, reject if &gt;35% match nav/CMS patterns, require title+URL+apply evidence</li>
                        <li><strong>Scoring formula:</strong> count&times;3.2 + title_hits&times;2.3 + url_hits&times;1.7 + apply_hits&times;1.5 + unique_titles&times;0.7 - reject_hits&times;3.5 - nav_hits&times;4.2</li>
                        <li><strong>Coverage-first:</strong> Prefers larger validated sets. Parent v1.6 output only wins if its score is strictly higher.</li>
                      </ul>
                    </Section>

                    <Section title="Title Validation">
                      <p>Every extracted title passes through multi-layer validation:</p>
                      <ul className="list-disc pl-5 space-y-1 mt-1">
                        <li><strong>Reject patterns:</strong> "Apply Now", "Read More", "Job Alerts", "Open Jobs", "Career Opportunities", blog/CMS artifacts, department names, phone numbers, contact info</li>
                        <li><strong>Boundary-aware checks:</strong> "design intern" is NOT rejected (even though "sign in" is a substring) — uses word-boundary regex</li>
                        <li><strong>Length limits:</strong> 1-14 words. Single words require strong job signal. &gt;14 words = likely description, not title.</li>
                        <li><strong>Post-extraction vocabulary check:</strong> &ge;30% of titles must contain a job-title noun (engineer, manager, analyst, nurse, etc.)</li>
                      </ul>
                    </Section>

                    <Section title="Quality Controls">
                      <p><strong>Type 1 error prevention (false positives):</strong> The <Code>_count_real_jobs()</Code> function validates each extracted job title against a 200+ noun vocabulary and URL patterns. If &lt;50% of titles look like real jobs, the job count is penalized in comparisons.</p>
                      <p><strong>Type 2 error detection (false negatives):</strong> Measured by comparing job count against baseline. Sites where the model finds 0 jobs but baseline finds &gt;0 are flagged as failures.</p>
                    </Section>

                    <Section title="Inputs &amp; Outputs">
                      <p><strong>Input:</strong> Domain name + company name &rarr; CareerPageFinder discovers URL &rarr; fetches HTML (with Playwright fallback for JS-heavy sites: 5s wait, cookie dismissal, scroll-to-load)</p>
                      <p><strong>Output:</strong> List of structured jobs: title, source_url, location_raw, salary_raw, employment_type, description, extraction_method, extraction_confidence (0-1)</p>
                    </Section>

                    {champComposite && (
                      <Section title="Composite Score Breakdown">
                        <Row label="Discovery rate (20%)" value={`${champComposite.discovery}%`} color={champComposite.discovery >= 80 ? 'text-green-600' : 'text-amber-600'} />
                        <Row label="Quality extraction (30%)" value={`${champComposite.quality_extraction}%`} color={champComposite.quality_extraction >= 60 ? 'text-green-600' : 'text-amber-600'} />
                        <Row label="Field completeness (25%)" value={`${champComposite.field_completeness}%`} color={champComposite.field_completeness >= 50 ? 'text-green-600' : 'text-amber-600'} />
                        <Row label="Volume accuracy (25%)" value={`${champComposite.volume_accuracy}%`} color={champComposite.volume_accuracy >= 70 ? 'text-green-600' : 'text-amber-600'} />
                        <div className="border-t border-gray-200 mt-2 pt-2">
                          <Row label="Composite score" value={champComposite.composite} color={champComposite.composite >= 60 ? 'text-green-700 font-bold' : 'text-amber-700 font-bold'} />
                        </div>
                      </Section>
                    )}

                    {champSummary && (
                      <Section title="Latest Test Results">
                        <Row label="Sites with extractions" value={`${champSummary.sites_extracted}/${champSummary.sites_tested}`} />
                        <Row label="Total jobs extracted" value={champSummary.total_jobs} />
                        <Row label="Field completeness" value={`${champSummary.quality_score}%`} color={champSummary.quality_score >= 50 ? 'text-green-600' : 'text-amber-600'} />
                        {champSummary.quality_warnings != null && <Row label="Quality warnings" value={champSummary.quality_warnings} color={champSummary.quality_warnings > 0 ? 'text-red-500' : 'text-green-600'} />}
                      </Section>
                    )}
                  </div>
                )}

                {/* ═══ CHALLENGER TAB ═══ */}
                {modelDetailTab === 'challenger' && (
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900 mb-1">{model?.labels?.challenger || model?.name || 'Challenger'}</h3>
                    <p className="text-sm text-gray-500 mb-2">{model?.description || 'The challenger model being evaluated.'}</p>
                    <p className="text-sm text-gray-500 mb-6">Same architecture as the champion (see Champion tab for full documentation). Below are the differences and improvements specific to this version.</p>

                    <Section title="What Changed vs Champion">
                      <p>Each challenger version inherits the same tiered extraction architecture but adds or modifies specific components. Common improvement categories:</p>
                      <div className="bg-gray-50 rounded-lg p-3 space-y-2 mt-2">
                        <div><span className="font-medium text-gray-700">New ATS handler:</span> Added dedicated extraction for a specific ATS platform (e.g. Breezy HR, Dayforce) that previously fell through to heuristic extraction</div>
                        <div><span className="font-medium text-gray-700">Title validation fix:</span> Tightened or loosened specific rejection patterns to reduce false positives or false negatives</div>
                        <div><span className="font-medium text-gray-700">Discovery improvement:</span> Better career page URL detection for specific URL patterns or ATS platforms</div>
                        <div><span className="font-medium text-gray-700">Playwright rendering:</span> Improved JS rendering with longer waits, cookie dismissal, scroll triggers</div>
                        <div><span className="font-medium text-gray-700">Candidate arbitration:</span> Changed how the system picks between competing extraction results</div>
                      </div>
                    </Section>

                    <Section title="Promotion Criteria (Composite Scoring)">
                      <p>A challenger replaces the champion only when it achieves a higher <strong>composite score</strong> across 4 weighted axes:</p>
                      <div className="bg-gray-50 rounded-lg p-3 mt-2">
                        <table className="w-full text-xs">
                          <thead><tr className="border-b border-gray-200">
                            <th className="text-left py-1 pr-3">Axis</th>
                            <th className="text-left py-1 pr-3">Weight</th>
                            <th className="text-left py-1">What It Measures</th>
                          </tr></thead>
                          <tbody>
                            <tr className="border-b border-gray-100"><td className="py-1.5 pr-3 font-medium">Discovery rate</td><td className="py-1.5 pr-3">20%</td><td className="py-1.5">% of sites where a career page was found</td></tr>
                            <tr className="border-b border-gray-100"><td className="py-1.5 pr-3 font-medium">Quality extraction</td><td className="py-1.5 pr-3">30%</td><td className="py-1.5">% of sites with real jobs extracted (minus Type 1 errors)</td></tr>
                            <tr className="border-b border-gray-100"><td className="py-1.5 pr-3 font-medium">Field completeness</td><td className="py-1.5 pr-3">25%</td><td className="py-1.5">Avg core fields populated per job (title, URL, location, description, salary, type)</td></tr>
                            <tr><td className="py-1.5 pr-3 font-medium">Volume accuracy</td><td className="py-1.5 pr-3">25%</td><td className="py-1.5">How close to baseline job count (penalizes both over- and under-extraction)</td></tr>
                          </tbody>
                        </table>
                      </div>
                      <p className="mt-2"><strong>Additional gate:</strong> Challenger must maintain &ge;60% accuracy on the fixed regression test suite to prevent catastrophic regressions.</p>
                    </Section>

                    <Section title="Quality Validation Pipeline">
                      <p>Every extracted job passes through:</p>
                      <ol className="list-decimal pl-5 space-y-1 mt-1">
                        <li><strong>Title normalization:</strong> Strip metadata suffixes (deadline, location, posting date), clean HTML entities, remove PDF/button artifacts</li>
                        <li><strong>Title validation:</strong> Reject nav labels, section headings, department names, blog artifacts, CMS noise, contact info, product names</li>
                        <li><strong>Jobset validation:</strong> Check the entire set for uniqueness, title vocabulary density, URL evidence, apply context</li>
                        <li><strong>Real job count:</strong> Post-extraction, validate &ge;50% of titles match the 200+ job noun vocabulary. If not, penalize the set in comparisons.</li>
                      </ol>
                    </Section>

                    <Section title="Supported ATS Platforms (Dedicated Handlers)">
                      <div className="grid grid-cols-2 gap-2 mt-1">
                        {[
                          { name: 'Oracle CandidateExperience', method: 'Requisitions REST API with siteNumber variants' },
                          { name: 'Greenhouse', method: 'Boards API (boards-api.greenhouse.io)' },
                          { name: 'Salesforce Recruit', method: 'fRecruit__ApplyJobList table parsing' },
                          { name: 'MartianLogic / MyRecruitmentPlus', method: 'API probing from __NEXT_DATA__ context' },
                          { name: 'Breezy HR', method: 'JSON API + position card HTML parsing' },
                          { name: 'Dayforce HCM', method: 'CandidatePortal Search API + HTML cards' },
                          { name: 'UltiPro', method: 'Job board API + opportunity card parsing' },
                          { name: 'GrowHire', method: 'API endpoint + job card HTML parsing' },
                          { name: 'AcquireTM', method: 'Table row + card layout HTML parsing' },
                        ].map(ats => (
                          <div key={ats.name} className="bg-gray-50 rounded p-2">
                            <div className="font-medium text-gray-700 text-xs">{ats.name}</div>
                            <div className="text-[10px] text-gray-500">{ats.method}</div>
                          </div>
                        ))}
                      </div>
                    </Section>

                    {challComposite && (
                      <Section title="Composite Score Breakdown">
                        <Row label="Discovery rate (20%)" value={`${challComposite.discovery}%`} color={challComposite.discovery >= 80 ? 'text-green-600' : 'text-amber-600'} />
                        <Row label="Quality extraction (30%)" value={`${challComposite.quality_extraction}%`} color={challComposite.quality_extraction >= 60 ? 'text-green-600' : 'text-amber-600'} />
                        <Row label="Field completeness (25%)" value={`${challComposite.field_completeness}%`} color={challComposite.field_completeness >= 50 ? 'text-green-600' : 'text-amber-600'} />
                        <Row label="Volume accuracy (25%)" value={`${challComposite.volume_accuracy}%`} color={challComposite.volume_accuracy >= 70 ? 'text-green-600' : 'text-amber-600'} />
                        <div className="border-t border-gray-200 mt-2 pt-2">
                          <Row label="Composite score" value={challComposite.composite} color={challComposite.composite >= 60 ? 'text-green-700 font-bold' : 'text-amber-700 font-bold'} />
                        </div>
                        {champComposite && (
                          <div className="mt-3 p-3 rounded-lg bg-gray-50">
                            <div className="text-xs font-medium text-gray-700 mb-1">vs Champion</div>
                            <div className="flex items-center gap-2">
                              <span className={`text-lg font-bold ${challComposite.composite > champComposite.composite ? 'text-green-600' : challComposite.composite === champComposite.composite ? 'text-gray-500' : 'text-red-500'}`}>
                                {challComposite.composite > champComposite.composite ? '▲' : challComposite.composite === champComposite.composite ? '=' : '▼'}
                                {' '}{Math.abs(challComposite.composite - champComposite.composite).toFixed(1)} pts
                              </span>
                              <span className="text-xs text-gray-500">
                                ({challComposite.composite.toFixed(1)} vs {champComposite.composite.toFixed(1)})
                              </span>
                            </div>
                          </div>
                        )}
                      </Section>
                    )}

                    {challSummary && (
                      <Section title="Latest Test Results">
                        <Row label="Sites with extractions" value={`${challSummary.sites_extracted}/${challSummary.sites_tested}`} />
                        <Row label="Total jobs extracted" value={challSummary.total_jobs} />
                        <Row label="Field completeness" value={`${challSummary.quality_score}%`} color={challSummary.quality_score >= 50 ? 'text-green-600' : 'text-amber-600'} />
                        {challSummary.quality_warnings != null && <Row label="Quality warnings" value={challSummary.quality_warnings} color={challSummary.quality_warnings > 0 ? 'text-red-500' : 'text-green-600'} />}
                      </Section>
                    )}

                    <Section title="Historical Context">
                      <p className="text-xs text-gray-500">The auto-improve system has tested 40+ model versions. Key milestones:</p>
                      <div className="mt-2 text-xs space-y-1">
                        <div className="flex gap-2"><span className="text-gray-400 w-12">v1.6</span><span>66% accuracy (376 lines) — Baseline with apply-button matching + vocab validation</span></div>
                        <div className="flex gap-2"><span className="text-gray-400 w-12">v2.6</span><span className="text-green-600 font-medium">82% accuracy (1,102 lines) — Peak. JSON-LD + embedded state + MartianLogic probing</span></div>
                        <div className="flex gap-2"><span className="text-gray-400 w-12">v3-5</span><span className="text-red-500">50-68% accuracy (3,000+ lines) — Regression from complexity explosion</span></div>
                        <div className="flex gap-2"><span className="text-gray-400 w-12">v6.0</span><span className="text-blue-600 font-medium">Consolidated clean-slate: v2.6 base + dedicated ATS handlers + boundary-aware validation</span></div>
                      </div>
                    </Section>
                  </div>
                )}

              </div>
            </div>
          </div>
        );
      })()}

      {/* ── Feedback Sub-Modal ── */}
      {feedbackTarget && modalModelId && (
        <FeedbackModal
          modelId={modalModelId}
          runId={feedbackTarget.runId}
          siteUrl={feedbackTarget.siteUrl}
          company={feedbackTarget.company}
          items={siteFeedbackItems || []}
          onClose={() => { setFeedbackTarget(null); }}
          onChanged={() => { refetchSiteFeedback(); qc.invalidateQueries({ queryKey: ['ml-feedback'] }); }}
        />
      )}
    </div>
  );
}


/* ── Codex Activity Panel (always visible on /models page) ── */
function CodexActivityPanel() {
  const [lines, setLines] = React.useState<string[]>([]);
  const [offset, setOffset] = React.useState(0);
  const [running, setRunning] = React.useState(false);
  const [daemonAlive, setDaemonAlive] = React.useState(false);
  const [daemonMessage, setDaemonMessage] = React.useState<string | null>(null);
  const [collapsed, setCollapsed] = React.useState(false);
  const scrollRef = React.useRef<HTMLDivElement>(null);

  const lastLogFile = React.useRef<string>('');
  const prevRunning = React.useRef(false);

  // Auto-collapse when codex stops running
  React.useEffect(() => {
    if (prevRunning.current && !running) {
      setCollapsed(true);
    }
    prevRunning.current = running;
  }, [running]);

  React.useEffect(() => {
    const poll = setInterval(async () => {
      try {
        const data = await getAutoImproveActivity(offset);

        // Detect new log file (new Codex session) — reset everything
        if (data.log_file && data.log_file !== lastLogFile.current && lastLogFile.current !== '') {
          setLines([]);
          setOffset(0);
          lastLogFile.current = data.log_file;
          return; // Next poll will fetch from offset 0
        }
        lastLogFile.current = data.log_file || '';

        if (data.lines.length > 0) {
          setLines(prev => [...prev.slice(-500), ...data.lines]);
          setOffset(data.offset);
          setTimeout(() => scrollRef.current?.scrollTo(0, scrollRef.current.scrollHeight), 50);
        }
        // `running` now means Codex itself is iterating right now; the
        // daemon-alive signal is surfaced separately so the panel can show
        // an honest "idle / supervising" state when Codex isn't running.
        setRunning(Boolean(data.running));
        setDaemonAlive(Boolean(data.daemon_alive));
        setDaemonMessage(data.daemon_message ?? null);
      } catch {}
    }, 2000);
    return () => clearInterval(poll);
  }, [offset]);

  // Don't show if no activity ever AND nothing supervising
  if (lines.length === 0 && !running && !daemonAlive) {
    return null;
  }

  return (
    <div className="card overflow-hidden">
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="w-full flex items-center justify-between px-4 py-2.5 bg-gray-900 text-left hover:bg-gray-800 transition-colors"
      >
        <div className="flex items-center gap-2">
          <Sparkles className="w-4 h-4 text-purple-400" />
          <span className="text-sm font-medium text-gray-200">Codex Auto-Improve</span>
          {running && (
            <span className="flex items-center gap-1 text-xs text-green-400 animate-pulse">
              <Loader2 className="w-3 h-3 animate-spin" /> Codex running
            </span>
          )}
          {!running && daemonAlive && (
            <span className="flex items-center gap-1 text-xs text-blue-400">
              <span className="w-2 h-2 rounded-full bg-blue-400" /> Daemon idle
              {daemonMessage ? ` — ${daemonMessage}` : ''}
            </span>
          )}
          {!running && !daemonAlive && lines.length > 0 && (
            <span className="text-xs text-gray-500">Stopped</span>
          )}
        </div>
        <span className="text-xs text-gray-500">{collapsed ? '▸' : '▾'}</span>
      </button>
      {!collapsed && (
        <div
          ref={scrollRef}
          className="bg-gray-950 text-sm px-4 py-3 overflow-y-auto transition-all space-y-1"
          style={{ maxHeight: 300 }}
        >
          {lines.map((line, i) => {
            // Command lines ($ or ⏳ Running:)
            if (line.includes('$ ') || line.includes('⏳ Running:')) {
              const cmd = line.replace(/^\[[\d:]+\]\s*(\$|⏳ Running:)\s*/, '');
              const ts = line.match(/^\[[\d:]+\]/)?.[0] || '';
              return (
                <div key={i} className="flex items-start gap-2">
                  <span className="text-gray-600 text-xs font-mono flex-shrink-0 mt-0.5 w-16">{ts}</span>
                  <div className="bg-gray-800/80 rounded px-2.5 py-1 font-mono text-xs text-amber-300 flex-1 overflow-x-auto">
                    <span className="text-gray-500 mr-1">$</span>{cmd}
                  </div>
                </div>
              );
            }
            // Thinking/agent messages (🤖)
            if (line.includes('🤖')) {
              const msg = line.replace(/^\[[\d:]+\]\s*🤖\s*/, '');
              const ts = line.match(/^\[[\d:]+\]/)?.[0] || '';
              return (
                <div key={i} className="flex items-start gap-2 py-0.5">
                  <span className="text-gray-600 text-xs font-mono flex-shrink-0 mt-0.5 w-16">{ts}</span>
                  <div className="flex-1 text-gray-200 leading-relaxed">
                    <span className="mr-1.5">🤖</span>{msg}
                  </div>
                </div>
              );
            }
            // File operations (📝 ✏️)
            if (line.includes('📝') || line.includes('✏️')) {
              const ts = line.match(/^\[[\d:]+\]/)?.[0] || '';
              const rest = line.replace(/^\[[\d:]+\]\s*/, '');
              return (
                <div key={i} className="flex items-start gap-2 py-0.5">
                  <span className="text-gray-600 text-xs font-mono flex-shrink-0 mt-0.5 w-16">{ts}</span>
                  <span className="text-blue-300">{rest}</span>
                </div>
              );
            }
            // Status events (🚀 🔄 ✅ 🏁)
            if (/[🚀🔄✅🏁⚙️]/.test(line)) {
              const ts = line.match(/^\[[\d:]+\]/)?.[0] || '';
              const rest = line.replace(/^\[[\d:]+\]\s*/, '');
              return (
                <div key={i} className="flex items-start gap-2 py-0.5">
                  <span className="text-gray-600 text-xs font-mono flex-shrink-0 mt-0.5 w-16">{ts}</span>
                  <span className="text-green-400 font-medium">{rest}</span>
                </div>
              );
            }
            // Errors
            if (line.includes('❌') || line.includes('ERROR') || line.includes('FAIL')) {
              return <div key={i} className="text-red-400 font-medium py-0.5 pl-[72px]">{line}</div>;
            }
            // Timestamps / system
            if (line.startsWith('[2026') || line.startsWith('[202')) {
              return <div key={i} className="text-gray-500 text-xs py-0.5 pl-[72px]">{line}</div>;
            }
            // Default
            if (!line.trim()) return null;
            return <div key={i} className="text-gray-500 text-xs pl-[72px]">{line}</div>;
          })}
          {running && <div className="text-green-400 animate-pulse mt-2 pl-[72px] text-xs">Codex is working...</div>}
          {!running && daemonAlive && lines.length > 0 && (
            <div className="text-blue-400 mt-2 pl-[72px] text-xs">
              Codex finished. Daemon idle{daemonMessage ? ` — ${daemonMessage}` : ''}.
            </div>
          )}
          {!running && !daemonAlive && lines.length > 0 && <div className="text-gray-500 mt-2 pl-[72px] text-xs">Codex stopped (daemon not running)</div>}
          {lines.length === 0 && <div className="text-gray-600 text-xs pl-[72px]">Waiting for activity...</div>}
        </div>
      )}
    </div>
  );
}


/* ── Feedback Modal Component ── */
interface FeedbackItem { id: string; site_url: string; comment: string; screenshot_path: string | null; screenshots?: string[] }

interface Draft { id?: string; comment: string; screenshots: string[]; saving?: boolean }

function FeedbackModal({ modelId, runId, siteUrl, company, items, onClose, onChanged }: {
  modelId: string; runId: string; siteUrl: string; company: string;
  items: FeedbackItem[]; onClose: () => void; onChanged: () => void;
}) {
  // Use ref for drafts to avoid stale closures in async handlers
  const [drafts, setDrafts] = useState<Draft[]>(() => {
    if (items.length > 0) return items.map(fb => ({
      id: fb.id, comment: fb.comment,
      screenshots: fb.screenshots || (fb.screenshot_path ? [fb.screenshot_path] : []),
    }));
    return [{ comment: '', screenshots: [] }];
  });
  const draftsRef = React.useRef(drafts);
  draftsRef.current = drafts;

  // Only initialize once from server data — never overwrite local edits
  const initialized = React.useRef(false);
  React.useEffect(() => {
    if (!initialized.current && items.length > 0) {
      initialized.current = true;
      setDrafts(items.map(fb => ({
        id: fb.id, comment: fb.comment,
        screenshots: fb.screenshots || (fb.screenshot_path ? [fb.screenshot_path] : []),
      })));
    }
  }, [items]);

  // Auto-save: debounced save after text changes
  const saveTimerRef = React.useRef<ReturnType<typeof setTimeout>>(undefined);
  const autoSave = React.useCallback(async (idx: number) => {
    const draft = draftsRef.current[idx];
    if (!draft || !draft.comment.trim()) return;
    try {
      if (draft.id) {
        await updateFeedback(modelId, runId, draft.id, draft.comment);
      } else {
        const created = await createFeedback(modelId, runId, siteUrl, draft.comment);
        setDrafts(d => d.map((item, i) => i === idx ? { ...item, id: created.id } : item));
      }
    } catch { /* ignore auto-save errors */ }
  }, [modelId, runId, siteUrl]);

  const updateText = React.useCallback((idx: number, text: string) => {
    setDrafts(d => d.map((item, i) => i === idx ? { ...item, comment: text } : item));
    // Debounced auto-save (1.5s after last keystroke)
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
    saveTimerRef.current = setTimeout(() => autoSave(idx), 1500);
  }, [autoSave]);

  const addDraft = () => setDrafts(d => [...d, { comment: '', screenshots: [] }]);

  const removeDraft = async (idx: number) => {
    const draft = draftsRef.current[idx];
    if (draft?.id) {
      try { await deleteFeedback(modelId, runId, draft.id); } catch {}
    }
    setDrafts(d => d.filter((_, i) => i !== idx));
    onChanged();
  };

  const handlePaste = React.useCallback(async (idx: number, e: React.ClipboardEvent) => {
    const clipItems = e.clipboardData?.items;
    if (!clipItems) return;

    for (const item of Array.from(clipItems)) {
      if (item.type.startsWith('image/')) {
        e.preventDefault();
        const file = item.getAsFile();
        if (!file) continue;

        // Read current draft state from ref (never stale)
        const current = draftsRef.current[idx];
        if (!current) continue;

        // Ensure feedback is saved first
        let fbId = current.id;
        if (!fbId) {
          try {
            const created = await createFeedback(modelId, runId, siteUrl, current.comment || '');
            fbId = created.id;
            setDrafts(d => d.map((d2, i) => i === idx ? { ...d2, id: fbId } : d2));
          } catch { continue; }
        }

        // Upload screenshot and append to list (not replace)
        try {
          const result = await uploadFeedbackScreenshot(modelId, runId, fbId!, file);
          setDrafts(d => d.map((d2, i) =>
            i === idx ? { ...d2, screenshots: [...d2.screenshots, result.screenshot_path] } : d2
          ));
        } catch {}
        break;
      }
    }
  }, [modelId, runId, siteUrl]);

  const handleClose = async () => {
    // Final save before closing
    for (let i = 0; i < draftsRef.current.length; i++) {
      const d = draftsRef.current[i];
      if (d.comment.trim() || d.screenshots.length > 0) {
        try {
          if (d.id) {
            await updateFeedback(modelId, runId, d.id, d.comment);
          } else if (d.comment.trim()) {
            await createFeedback(modelId, runId, siteUrl, d.comment);
          }
        } catch {}
      }
    }
    onChanged();
    onClose();
  };

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/30" onClick={handleClose}>
      <div className="bg-white rounded-xl shadow-2xl w-[650px] max-h-[75vh] flex flex-col" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200">
          <div>
            <h3 className="font-semibold text-gray-900 text-sm">Feedback</h3>
            <p className="text-xs text-gray-500 truncate max-w-[450px]">{company} — {siteUrl}</p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[10px] text-gray-400">Auto-saves as you type</span>
            <button onClick={handleClose} className="text-gray-400 hover:text-gray-600 text-lg">&times;</button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-5 space-y-4">
          {drafts.map((draft, idx) => (
            <div key={draft.id || `new-${idx}`} className="border border-gray-200 rounded-lg p-3 space-y-2">
              <div className="flex items-start gap-2">
                <textarea
                  value={draft.comment}
                  onChange={e => updateText(idx, e.target.value)}
                  onPaste={e => handlePaste(idx, e)}
                  placeholder="Add feedback... (paste screenshots with Ctrl+V)"
                  className="flex-1 border border-gray-100 rounded px-3 py-2 text-sm resize-y min-h-[70px] focus:border-indigo-300 focus:ring-1 focus:ring-indigo-200 bg-gray-50"
                  rows={3}
                />
                <button onClick={() => removeDraft(idx)} className="text-gray-300 hover:text-red-500 p-1 mt-1" title="Delete">
                  <X className="w-4 h-4" />
                </button>
              </div>
              {/* Screenshots (multiple) */}
              {draft.screenshots.length > 0 && (
                <div className="flex flex-wrap gap-2 mt-1">
                  {draft.screenshots.map((path, si) => (
                    <img
                      key={si} src={path} alt={`screenshot ${si + 1}`}
                      className="max-h-28 rounded border border-gray-200 cursor-pointer hover:opacity-80 hover:ring-2 hover:ring-indigo-300"
                      onClick={() => window.open(path, '_blank')}
                    />
                  ))}
                </div>
              )}
              {draft.id && <div className="text-[10px] text-green-500 flex items-center gap-1"><CheckCircle className="w-2.5 h-2.5" /> Saved</div>}
            </div>
          ))}
        </div>

        <div className="flex items-center justify-between px-5 py-3 border-t border-gray-200">
          <button onClick={addDraft} className="text-xs flex items-center gap-1 text-indigo-600 hover:text-indigo-800">
            <Plus className="w-3 h-3" /> Add another feedback
          </button>
          <button onClick={handleClose} className="btn-primary text-xs px-4 py-1.5">Done</button>
        </div>
      </div>
    </div>
  );
}
