# LongCat API — Verification Notes

> **Status: INCOMPLETE — operator must fill in sections marked TODO before M1 day 1.**
>
> Per docs/05_LLM_INTEGRATION.md: "Do not hardcode assumptions from this doc."
> This file records what was verified at build time vs. what must be confirmed live.

## What we know (from docs/05)

- LongCat exposes an **OpenAI-compatible chat completions API** (`/v1/chat/completions`)
- The client is built on `openai` Python SDK with `base_url` override
- Prepaid 50B-token pack — tracking constraint is **rate limits + latency**, not token count
- T0 concurrency: 16 parallel requests; T1: 8; T2: 2 (initial, tunable in client.py)

## TODO: Fill in after LongCat console access

### 1. Endpoint

```
LLM_BASE_URL=<fill in from LongCat console>
```

Expected format: `https://<host>/v1` or similar OpenAI-compatible base URL.

### 2. Available model IDs

| Tier | Purpose | Model ID to set |
|---|---|---|
| T0 | Fast classify/extract | `LLM_MODEL_T0=<fill in>` |
| T1 | Korean generation (listing titles, CS) | `LLM_MODEL_T1=<fill in>` |
| T2 | Reasoning/thinking (claim triage, brand dossier) | `LLM_MODEL_T2=<fill in>` |

### 3. Features to confirm

- [ ] **`response_format: {"type": "json_object"}`** supported? (JSON mode — required for structured output)
  - If NO: fall back to prompt-level JSON enforcement + post-parse
- [ ] **Vision input** (image_url in messages) supported?
  - If NO: RiskFilter image checks fall back to OCR + perceptual-hash (already implemented in agents)
  - ContentAgent image-overlay check falls back to phash-only
- [ ] **Context window size** for T1/T2 (minimum needed: 8k tokens)
- [ ] **Rate limits**: requests/min per tier
- [ ] **Usage reporting**: how to query remaining prepaid tokens
- [ ] **Streaming**: not required for production agents (we use non-streaming for structured output)

### 4. Prepaid pack tracking

```
Initial pack: 50B tokens
Burn rate estimate:
  - ContentAgent (30k SKUs): ~120M tokens (one-time)
  - Steady state: ~1–3M tokens/day
  - Daily budget ceiling: 30M tokens (set in app_config and .env)
```

Check remaining balance at: `<LongCat console URL>`

### 5. Rate limit mitigation

If T0 rate limits are hit during longtail expand batches:
- Batch T0 classification calls (up to 20 items/call using array schema)
- `llm_daily_budget_tokens` circuit breaker pauses non-critical tasks
- `LLM_TIMEOUT_S=60` for T0/T1; set to 120 for T2 if thinking model is slow

### 6. Fallback plan (if LongCat is unavailable)

The gateway in `core/llm/client.py` is provider-agnostic (OpenAI-compatible).
Switching to another provider = change `.env` only:

```
LLM_BASE_URL=https://api.anthropic.com/v1   # example
LLM_MODEL_T0=claude-haiku-4-5-20251001
LLM_MODEL_T1=claude-sonnet-4-6
LLM_MODEL_T2=claude-opus-4-6
```

Note: Anthropic API uses `anthropic-version` header + different auth. If switching
to a non-OpenAI-compatible provider, update `core/llm/client.py` `_get_oai()` method.

## Verification checklist (complete before first M1 listing pipeline run)

- [ ] `.env` filled: `LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL_T0`, `LLM_MODEL_T1`, `LLM_MODEL_T2`
- [ ] Smoke test: `pytest tests/test_llm_smoke.py -m live` passes (needs `RELAY_LIVE=1`)
- [ ] JSON mode confirmed working on T0 model
- [ ] Vision availability noted above
- [ ] Rate limits recorded and concurrency settings tuned in `client.py` if needed
- [ ] Cost per token updated in `_log_call()` method (currently uses placeholder estimate)
