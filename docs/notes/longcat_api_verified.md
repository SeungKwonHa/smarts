# LongCat API — Verified Configuration (2026-07-15)

## Status: ✅ WORKING — all issues resolved

API integration fully tested and operational. Korean SEO title generation,
product categorization, and brand extraction all working correctly.

## Verified Configuration

```
LLM_BASE_URL=https://api.longcat.chat/openai/v1
LLM_API_KEY=<from console>
LLM_MODEL_T0=LongCat-2.0
LLM_MODEL_T1=LongCat-2.0
LLM_MODEL_T2=LongCat-Flash-Thinking
```

## Critical Gotcha #1: JSON Mode NOT Supported

LongCat's OpenAI-compatible endpoint **does NOT support**
`response_format={"type": "json_object"}`. When used, the API returns
`choices[0].message.content = None` even though `finish_reason = "stop"`.

**Fix:** JSON formatting is handled via prompt injection (appending instruction
to system message). Client strips markdown code fences and extracts JSON.

**Code:** `src/relay/core/llm/client.py` — `_inject_json_instruction()`

## Critical Gotcha #2: Reasoning Token Overhead

LongCat-2.0 is a **reasoning model** — it uses internal thinking tokens
(`reasoning_content` field) before producing visible output. If `max_tokens`
is too small, all tokens are consumed by reasoning and `content` is `None`.

| Tier | Old max_tokens | New max_tokens | Notes |
|------|---------------|----------------|-------|
| T0 classify | 128-512 | 256-1024 | batch jobs need headroom |
| T0 entity extract | 512 | 1024 | multi-field output |
| T1 title gen | 256 | 512 | short output but reasoning eats ~100 tokens |
| T1 detail gen | 2048 | 2048 | already sufficient |
| T2 reasoning | 2048-4096 | 2048-4096 | unchanged |

**Client code** (`tiers.py`): all `max_tokens` values raised accordingly.

## Verified Working (2026-07-15)

| Test | Result | Latency |
|------|--------|---------|
| Simple Korean (user only) | `안녕하세요!` | ~2.6s |
| System + User (English) | `안녕하세요 (Annyeonghaseyo)` | ~2.1s |
| JSON categorization (Japanese input) | `{"category": "other", "brand": "G01"}` | ~3.2s |
| Korean SEO title generation | `G01 매트리스커버 코튼100% 박스시트` | ~2.1s |
| Math (showed reasoning overhead issue) | `4` (needs min 256 max_tokens) | ~1.5s |

## Response Structure Differences from Standard OpenAI

```json
{
  "choices": [{
    "message": {
      "content": "actual visible output",
      "reasoning_content": "\ninternal thinking steps...",
      "role": "assistant"
    },
    "finish_reason": "stop"
  }],
  "usage": {
    "completion_tokens": 30,
    "completion_tokens_details": {
      "reasoning_tokens": 26
    }
  }
}
```

- `reasoning_content`: LongCat's internal chain-of-thought (not in standard OpenAI)
- `completion_tokens_details.reasoning_tokens`: tokens used for thinking
- Visible output tokens = `completion_tokens - reasoning_tokens`

## Rate Limits (observed)

- Sustainable: ~1 req/s for T0
- No 429 in testing with 5s spacing

## Files Updated

- `.env` — LongCat credentials filled, inline comments removed
- `src/relay/core/config.py` — added field_validator to strip inline comments
- `src/relay/core/llm/tiers.py` — max_tokens raised for reasoning overhead
- `src/relay/core/llm/client.py` — JSON prompt injection, response_format removal, robust JSON parsing
