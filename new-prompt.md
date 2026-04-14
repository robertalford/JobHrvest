# JobHarvest Auto-Improve Agent Prompt

You are a senior/principal engineer working on JobHarvest's job-extraction models. The iteration caller prepends a **CURRENT CHAMPION** block to this file — read it first, then follow the rules below.

> Canonical project rules (promotion gate, inheritance limits, ≤200 LOC budget, market constraints) live in [`agent-instructions.md`](agent-instructions.md) §Site Config. This file is operational guidance only. In a conflict, `agent-instructions.md` wins.

---

## Your Mission

Close the gap between the model's blind extraction and the Jobstream hand-tuned baseline — and eventually exceed it — through general intelligence (platform detection, structural analysis, enrichment), never site-specific rules.

The single yardstick for promotion is the **4-axis composite** (see CURRENT CHAMPION block for the target):

| Axis | Weight | What it measures | Ceiling |
|------|--------|------------------|---------|
| Discovery | 20% | % of sites where the career page URL was found | 100 |
| Quality Extraction | 30% | % of sites with real jobs extracted, minus `quality_warning` | 100 |
| Field Completeness | 25% | Avg fields/job out of 6 (title, source_url, location_raw, salary_raw, employment_type, description) | 100 |
| Volume Accuracy | 25% | Ratio of model job count to baseline (peaks at 1.0, penalty when >1.5) | 100 |

Promotion requires **all four** gates to pass — a single-metric win never promotes:

1. **Global composite > champion composite** and **≥60% regression accuracy**.
2. **Cluster gate** — per-ATS composite, for every cluster with ≥3 sites, must not drop more than 2.0 points vs champion. Worst-gate-eligible-cluster composite must not regress. *Enforced in `backend/app/tasks/ml_tasks.py` `_aggregate`.*
3. **Ever-passed gate** — monotonic set of every site any version has previously passed. Challenger must still pass them (±15 % volume slack). Closes the ratcheting-loss gap when the champion rolls forward. *Enforced against `ever_passed_sites` table.*
4. **Oscillation gate** — sites that have flipped pass/fail ≥2 times in the last 5 runs are "unstable". Challenger must not be failing any of them. *Enforced from `site_result_history` via `app.ml.champion_challenger.stability`.*

Layered against the three legacy gates, **fixes that help one ATS by breaking another will not pass.** Your change must generalise across every gate-eligible cluster, not just the one highlighted in the brief.

---

## Version Resolution — DO NOT hardcode

Before doing anything, resolve the current champion:

```python
# Preferred (DB-authoritative):
# SELECT name FROM ml_models WHERE status='live' AND model_type='tiered_extractor';
```

Then inherit from the stable base — never the current champion:

- **Extractor:** `TieredExtractorV16` (stable base, do not modify).
- **Finder:** `CareerPageFinderV26` (stable discovery, do not modify).
- Write your challenger as `backend/app/crawlers/tiered_extractor_vNN.py` (and paired finder). Max 1 level of inheritance — stable base → your challenger.

Old iteration files from before the 2026-04-14 reset are archived under `backend/app/crawlers/_archive/`. Do not reference them.

---

## Pattern Cards, not per-site examples (universality-first brief)

The per-iteration brief now leads with **pattern cards** — one card per cluster of ≥3 failing/partial sites. Each card shows the ATS, the aggregate baseline vs model volume, a shared baseline wrapper hint, and anonymised HTML filenames. The per-site identity (company name, domain, test URL) is intentionally de-emphasised — we noticed the previous prompt biased Codex toward narrow fixes for the specific named domains.

Three extractions still run per site:

- **Baseline** — Jobstream hand-tuned selectors on the live HTML. The ceiling.
- **Champion** — current live model extracting blindly. The floor your change must beat.
- **Challenger** — your new version extracting blindly.

The `results_detail.sites` payload contains the per-phase extractions plus `baseline_full_wrapper` (the exact selector config that solved each site). Use the wrapper as a **diagnostic** — NOT a template.

| ✅ DO | ❌ DON'T |
|-------|---------|
| "5 Workday sites fail → add a Workday ATS extractor" | "example.com jobs are in `div.careers-grid` → add a rule for `div.careers-grid`" |
| "12 accordion sites extract 0 → improve accordion detection" | "acme.com returns 0 → add acme.com's specific API" |
| "Baseline gets location from detail pages on 30 sites → add general detail enrichment" | "salary is in `span.comp-range` on one site → add that selector" |
| "JSON-LD parser misses nested arrays on 8 sites → fix the parser" | "Fix helps 3 sites, could break 5 → ship it anyway" |

**Every change must help 3+ sites.** The cluster gate now enforces this — a change that targets a ≤2-site cluster cannot clear the gate on its own. Long-tail (1–2 site) failures are shown in the prompt for root-cause analysis but are not promotion-blocking until they cluster to 3.

---

## Error Types (from most to least severe)

| Type | Definition | Severity |
|------|-----------|----------|
| **1 — False Positive** | Nav label, section heading, or CMS artefact extracted as a job | **Critical** — destroys data trust |
| **2 — False Negative** | Real job missed | Important — reduces coverage |
| **3 — Quality Gap** | Title correct but location/description/salary missing | Medium |
| **4 — Description Noise** | Garbled text: `\t`, `\n`, HTML entities, nav/footer boilerplate | Medium — pervasive and cumulative |

Quality over quantity: 10 real jobs > 15 where 5 are garbage.

---

## Description Quality — the hidden axis

Descriptions are the largest contributor to `field_completeness`. Baseline descriptions are clean, markdown-like plaintext; model descriptions too often carry raw whitespace and HTML.

| ✅ Clean | ❌ Noisy |
|--------|---------|
| `We are looking for a Senior SWE to join our team.\n\nRequirements:\n- 5+ years...` | `\t\t\n\n  Senior Software Engineer\n\t\t\n\n    We are looking...\t\t\n\n\t\t  Requirements:\n\t\t\n\t\t\n\t\t    \t\t...` |

Required passes for every extracted description:
1. HTML → clean plaintext/markdown (no raw tags, `&amp;`, `&nbsp;`, CDATA)
2. Collapse runs of `\t\n ` → single spaces / meaningful newlines
3. Preserve paragraph breaks and bullets (`- ` prefix)
4. Strip cookie notices, nav, footer, "Apply Now" buttons, social links
5. Extract from the job-detail container, NOT the whole page body

---

## Process — Think, Critique, Build

### 1. Analyse
- Read `storage/auto_improve_memory.json` for banned approaches + anti-patterns.
- Read the per-iteration **failure brief** at the top of your prompt: weakest axis + top ATS clusters + detail-page candidates.
- For each failure, compare baseline/champion/challenger outputs side-by-side.

### 2. Classify failures by ROOT CAUSE (not by site)

| Category | Signal | Typical fix |
|----------|--------|-------------|
| Discovery failure | `url_found` is homepage/error/PDF | Finder path-probing or bad-target rejection |
| ATS not handled | Runs Greenhouse/Oracle/Workday but falls through | New dedicated ATS extractor |
| JS-rendering needed | Lots of `<script>`, minimal visible text | SPA detection / Playwright trigger |
| Title validation too strict | Real titles rejected across many sites | Relax one specific rule with evidence gate |
| Title validation too loose | Nav/CMS labels accepted | Add rejection patterns |
| Container wrong | Jobs exist but wrong container picked | Improve container scoring |
| Pagination missed | Page 1 works, subsequent pages missed | Fix pagination detection |
| Field coverage gap | Title/URL only, missing location/desc | Detail-page enrichment |
| Description noise | Tabs/newlines/boilerplate vs clean baseline | HTML→text conversion, container targeting |

### 3. Generate 2–3 high-impact ideas

Each idea must:
- Target a category that affects 3+ sites
- Have a clear mechanism (what code, what it improves)
- Be independently implementable
- Optimise for **broad utility**

### 4. Devil's advocate — before coding, ask:

- What Type 1 errors could this introduce?
- Blast radius — how many sites affected?
- Simpler alternative that gets 80% for 20% of the complexity?
- Does this repeat a **banned approach** (see head of prompt)?
- Net impact: fixes N, might regress M — is N − M > 0 on the composite?

Kill the ideas that don't survive. Merge overlaps.

### 5. Implement (≤200 LOC added)

- Keep extraction priority: parent v1.6 → structured data → ATS extractors → DOM fallbacks.
- Finder changes only if discovery is the weakest axis or hitting wrong pages.
- Update `storage/auto_improve_memory.json` via `memory_store.append_promotion` / `append_rejection` only if you understand the schema — otherwise leave it.

### 6. Validate with pytest BEFORE declaring done

```bash
cd /path/to/jobharvest
python -m pytest backend/tests/test_extractor_smoke.py -v --tb=short
```

Quick manual sanity check against a failure HTML file from the per-iteration context dir:

```python
import sys; sys.path.insert(0, 'backend')
from app.crawlers.tiered_extractor_vNN import TieredExtractorVNN
ext = TieredExtractorVNN()
with open('storage/auto_improve_context/vNN/failure_1_example.html') as f:
    html = f.read()
class P: url='https://example.com/careers'; requires_js_rendering=False
class C: name='Test'; ats_platform=None
import asyncio
jobs = asyncio.run(ext.extract(P(), C(), html))
for j in jobs[:3]:
    print(j.get('title','?'), '|', j.get('location_raw',''), '|', len(j.get('description','')), 'chars')
```

Check: module imports, `extract()` doesn't crash, titles are real, descriptions are clean text.

---

## Known Hard Patterns (recurring)

- **Config-only Next.js shells** — `__NEXT_DATA__` present but zero rendered DOM. Parse JSON state or probe API endpoint.
- **Oracle CandidateExperience** — multiple tenant site IDs (`CX`, `CX_1001`, …). Probe requisitions API with each variant.
- **Elementor / CMS career grids** — heading + generic CTA. Pair role-heading with card-local CTA.
- **Multilingual** — Indonesian `lowongan/karir`, Malay `kerjaya/jawatan`, Spanish `vacantes/empleo`. Discovery must try localised paths.
- **ATS migration drift** — selectors from months ago may be wrong now; platform detection should key off live signals.
- **Career hubs** — marketing page with "Join Our Team" CTA linking to the real listing page. Traverse, don't extract from the hub.
- **Detail-page enrichment** — many listings have only title + link; full metadata is on the detail page. Baseline follows these. So must the model.

---

## Memory — What Works / What Doesn't

### Works
Apply-button container matching · JSON-LD JobPosting extraction · Dedicated ATS API calls (Greenhouse, Oracle) · Coverage-first superset preference · Strict title-vocabulary validation · Parent v1.6 fallback arbitration · Detail-page enrichment for field coverage.

### Doesn't
50+ probe URL permutations for one ATS · Deep inheritance chains · Site-specific CSS hacks · Relaxing title validation to fix false negatives (always creates more Type 1s than it fixes) · Adding ever-more fallback paths (complexity grows, accuracy doesn't) · Copying baseline selectors (defeats the purpose).

---

## Output — After the iteration, report

1. Current champion + axis scorecard (from the head of your prompt).
2. Root-cause analysis — categories × sites affected.
3. Jobstream gap — biggest general capability missing.
4. 2–3 improvement ideas + self-critique + the survivors.
5. Code changes: what + why-generic (must help 3+ sites).
6. Type 1 audit: sample titles are real jobs.
7. Expected axis deltas + estimated sites improved vs at-risk.
8. pytest output + fixture-harness score.
