# V10 Auto-Improve: Prompt Engineering for LLM-Based Extraction

You are improving the **extraction prompt** used by the v10 LLM-based job extractor. The extractor sends page HTML + this prompt to an LLM, which returns structured job data as JSON.

## Your mission

Improve the extraction prompt at `storage/v10_extraction_prompt.md` to increase job extraction accuracy. You are NOT modifying code — you are engineering a better prompt.

## How the system works

1. The test system provides a company's career page URL and its HTML
2. The HTML (cleaned, truncated) is appended to the extraction prompt
3. An LLM reads the prompt + HTML and returns a JSON object with jobs + wrapper
4. The extracted jobs are compared against a known-good baseline (Jobstream wrappers)

## What makes a good extraction prompt

- **Precise output format**: The LLM must return valid JSON. Any extra text = parse failure = 0 jobs.
- **Comprehensive extraction**: Must find ALL jobs, not just the first few. Count matters.
- **Structural awareness**: Should handle various page layouts (tables, cards, lists, accordions).
- **ATS detection**: Many sites use standard ATS platforms (Lever, Greenhouse, Workday, etc.) with predictable HTML patterns.
- **Anti-noise**: Must distinguish real jobs from navigation, blog posts, team pages.
- **Detail extraction**: Location, salary, employment type should be extracted when present.
- **URL handling**: Relative URLs must be made absolute using the page URL.

## Improvement process

1. Review the test results below — especially failures and gaps
2. Read the current prompt at `storage/v10_extraction_prompt.md`
3. Analyze WHY the LLM failed on specific sites (read their context HTML files)
4. Identify patterns: Are failures due to prompt ambiguity? Missing instructions? Wrong output format?
5. Edit `storage/v10_extraction_prompt.md` with targeted improvements
6. DO NOT change any Python code — only the prompt file

## Common failure patterns to address

- **JSON parse failures**: LLM adds markdown formatting or explanation around the JSON
- **Missing jobs**: LLM only extracts a few visible jobs, misses pagination or hidden listings
- **False positives**: LLM extracts navigation items or team profiles as jobs
- **Missing fields**: LLM doesn't extract location/salary even when they're in the HTML
- **Relative URLs**: LLM returns relative paths instead of absolute URLs
- **ATS blindness**: LLM doesn't recognize standard ATS patterns (JSON-LD, embedded JSON data)

## Sandbox rules

- ONLY modify `storage/v10_extraction_prompt.md`
- Do NOT modify any `.py` files
- Do NOT run Playwright, Docker, or API calls
- You CAN read context HTML files to understand what the LLM sees
