# V10 Auto-Improve: Full Autonomy LLM Extraction System

You are improving the v10 LLM-based job extraction system. You have **full autonomy** — you can modify any part of the system to achieve the goal.

## Goal

Increase the percentage of test sites where v10 successfully extracts jobs matching the Jobstream baseline. Currently the system works end-to-end but has low accuracy — your job is to make it dramatically better.

## System Architecture

The v10 extraction pipeline:

1. **Career Page Finder** (working well at ~100% discovery) finds the careers page URL and fetches HTML
2. **Extractor** (`backend/app/crawlers/tiered_extractor_v100.py`) processes the HTML:
   - Cleans/truncates HTML
   - Writes a prompt + HTML to `/storage/v10_queue/{id}.prompt`
   - Waits for result at `/storage/v10_queue/{id}.result`
3. **LLM Worker** (`backend/scripts/v10_llm_worker.py`) runs on the host:
   - Watches `/storage/v10_queue/` for `.prompt` files
   - Runs `codex exec` with each prompt
   - Codex writes JSON result to an output file
   - Worker reads the result and writes it as `.result`
4. **Extraction Prompt** (`storage/v10_extraction_prompt.md`) — the core prompt template

## What You Can Modify

You have **full autonomy** to modify anything:

### High-impact targets:
- **`storage/v10_extraction_prompt.md`** — the extraction prompt. This is the most direct lever. Make it more specific, add examples, handle edge cases.
- **`backend/app/crawlers/tiered_extractor_v100.py`** — the extractor code. Add pre-processing (detect ATS patterns, extract JSON-LD, parse embedded API data), improve HTML cleaning, add post-processing validation.
- **`backend/scripts/v10_llm_worker.py`** — the LLM worker. Change the Codex prompt structure, add multi-step extraction (first analyse, then extract), change how output is captured.

### Structural changes you can make:
- Add heuristic pre-extraction that handles known ATS patterns (Lever, Greenhouse, Workday) before falling back to LLM
- Add a structured data detector that finds JSON-LD, embedded JSON, or API endpoints in the HTML
- Create helper scripts that the LLM worker can call
- Change the prompt to be multi-step: first "what type of page is this?", then "extract jobs using X strategy"
- Add HTML pre-processing to strip noise and highlight job-relevant content
- Create a site classification system (ATS type detection → specialised extraction)

### What NOT to do:
- Don't use Playwright, Docker, curl, or API calls (sandbox restrictions)
- Don't break the queue interface (`.prompt` → `.result` flow)
- Don't modify files outside the v10 system (no changes to v8.x extractors)

## How the Test System Works

- 50 fixed test sites are tested (same sites every run, deterministic order)
- For each site: baseline (Jobstream known-good selectors) vs v10 (your LLM system)
- A site "passes" if v10 extracts ≥90% of baseline's jobs (by count, quality-adjusted)
- Composite score weights: 20% discovery, 30% quality extraction, 25% field completeness, 25% volume accuracy

## Self-Review Process

BEFORE making changes, review:
1. **Your previous run's log** (provided below) — what did you try? What worked? What failed?
2. **The failure context HTML** — read the actual HTML files to understand page structures
3. **The current extraction prompt** — identify what's missing or misleading
4. **Pattern analysis** — group failures by type (ATS platform, page structure, error category)

Then make **targeted, high-impact changes** that address the largest failure categories first.
