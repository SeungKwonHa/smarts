# 05 — LLM Integration (LongCat API)

## Principles

1. **One gateway.** All inference goes through `core/llm/client.py`. Agents never
   import an SDK directly or reference model names — they request a **task tier**.
2. **Provider-agnostic.** LongCat exposes an OpenAI-compatible chat completions
   API; the gateway is built on the `openai` python client with a custom
   `base_url`. Swapping/adding a provider (e.g., Claude API for a hard task later)
   = config change + optional per-tier override, zero agent code changes.
3. **LLM writes prose and labels; code computes numbers.** Prices, margins,
   thresholds, FSM transitions are deterministic code. The LLM never does arithmetic
   that reaches production data.
4. **Structured output or it didn't happen.** Every production call returns JSON
   validated against a pydantic model; on validation failure → 1 repair retry with
   the error message → fail the task (never "best-effort parse").

## Configuration (env — VERIFY at build time)

```
LLM_PROVIDER=longcat
LLM_BASE_URL=            # LongCat OpenAI-compatible endpoint from platform console
LLM_API_KEY=             # from LongCat console (prepaid 50B-token pack attached)
LLM_MODEL_T0=            # fast/cheap chat model id
LLM_MODEL_T1=            # standard chat model id (Korean generation quality)
LLM_MODEL_T2=            # thinking/reasoning model id
LLM_TIMEOUT_S=60         # T2 may need 120
LLM_MAX_RETRIES=2
LLM_DAILY_BUDGET_TOKENS=30000000   # circuit breaker, adjustable
```

**Build task for Claude Code (M1, day 1):** confirm against the current LongCat
platform docs — exact base URL, available model IDs (flash / chat / thinking
variants), context window, whether JSON mode / `response_format` and vision input
are supported, rate limits, and how prepaid-pack usage is reported. Record findings
in `docs/notes/longcat_api_verified.md` and set env accordingly. **Do not hardcode
assumptions from this doc.** If vision input is unavailable, RiskFilter/ContentAgent
image checks fall back to: OCR (tesseract/paddle) + text screening + perceptual-hash
matching against a licensed-character reference set, and flag `REVIEW` more often.

## Tier routing

| Tier | Use | Typical tasks | Latency/cost posture |
|---|---|---|---|
| **T0** | classify / extract / normalize | entity extraction from scraped titles, keyword generation, inquiry classification, category mapping, variant normalization, image-overlay check | cheapest, high volume, temp 0–0.2 |
| **T1** | generate quality Korean | listing titles, detail sections, CS reply drafts, weekly report narrative, IP screen with rationale | mid, temp 0.3–0.7 by task |
| **T2** | multi-step reasoning | claim triage resolution, brand dossier synthesis, ambiguous risk adjudication support | expensive, low volume, may use thinking model |

Routing table lives in `core/llm/tiers.py` as `TASK_TIER: dict[TaskName, Tier]`
with per-task params (temperature, max_tokens, json_schema). Adding a task = one
dict entry + prompt file.

## Prompt management

- Templates in `prompts/<agent>/<task>_v<N>.j2` (jinja2). Rendered with typed
  context objects. Never f-string prompts inline in agent code.
- Every template starts with a contract header comment: purpose, input vars,
  output schema name, tier.
- Korean-output tasks: system prompt pins register (존댓말/합쇼체 for CS, 상품명
  규격 for titles), bans emoji/superlatives per the L3 spec, and includes 2–3
  few-shot exemplars stored beside the template (`*_examples.json`).
- Version bump (v2, v3…) on any behavior change; `llm_calls.task` logs template
  version so quality regressions are diffable.

## Caching & dedup

- `core/llm/cache.py`: content-addressed cache `sha256(template_v + rendered_input)`
  → response JSON, stored in Postgres (`llm_cache` table, TTL per task).
  Longtail listing generation hits many near-identical variant products — expect
  ≥20% hit rate on T0 normalization and category mapping.
- Batch where the API allows: group T0 classifications into one call with an array
  schema (up to ~20 items) when latency-insensitive (nightly sweeps).

## Cost & budget control

- Every call logged to `llm_calls` (tokens, task, tier, cache_hit, trace_id →
  Langfuse). Reporter shows daily tokens by agent vs. budget.
- **Circuit breaker:** daily token budget per tier; on breach → non-critical tasks
  (longtail content gen, brand scan) pause and queue; critical tasks (CS drafts,
  risk screening for live orders) continue; operator alerted.
- Planning envelope (validate in M1 with real numbers):
  - ContentAgent ≈ 2.5–4k tokens/SKU (in+out) → 30k SKUs ≈ 75–120M tokens one-time.
  - Steady state (monitoring ambiguity checks, CS, reports) ≈ 1–3M tokens/day.
  - Against a 50B-token prepaid pack these are rounding errors — the constraint is
    **rate limits and latency**, not the pack. Design for throughput (async
    concurrency limits per tier: T0=16, T1=8, T2=2 initial).

## Reliability

- Timeouts + retry (exponential, jittered) on 429/5xx; on repeated failure the
  gateway opens the provider circuit → tasks queue → operator alert.
- Determinism-sensitive tasks (risk screening) pin temperature 0 and log the full
  rendered prompt hash for audit.
- Red-team note: scraped page text and customer messages are **untrusted input** —
  templates instruct the model to treat quoted material as data; outputs that
  request actions (e.g., a "refund" appearing inside a product description) are
  ignored because agents only act on validated JSON fields, never free text.

## Evaluation hooks (M3)

- Golden sets: 100 labeled candidates for RiskFilter (block/pass/review), 50
  title-quality pairs, 100 inquiry classifications. `pytest -m llm_eval` runs the
  suite against current templates and reports accuracy deltas — run before any
  template or model-id change ships.
