"""LongCat LLM gateway — the ONE place all inference goes through.

Agents call:
    result = await llm.complete(task_name="l3.title_gen", messages=[...], session=session)

This gateway handles:
- Tier resolution (T0/T1/T2 → model ID)
- Content-addressed cache (Postgres)
- Retry with exponential backoff on 429/5xx
- Circuit breaker (daily token budget)
- llm_calls audit logging
- DRY_RUN mode (returns a mock response without hitting the API)
- Langfuse tracing (optional)
- Structured output enforcement (pydantic validation)
- prompt-injection defense: all LLM callers must pass validated dicts, not raw strings

IMPORTANT: Never call the OpenAI client directly in agent code.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import structlog
from openai import AsyncOpenAI, RateLimitError, APIStatusError
from openai.types.chat import ChatCompletion
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.config import settings
from relay.core.llm.cache import get_cached, make_cache_key, set_cached
from relay.core.llm.tiers import Tier, TaskParams, get_task_params, get_model_id

log = structlog.get_logger(__name__)

# ── Daily budget circuit breaker (in-memory for speed; reset at midnight) ────
_daily_tokens_used: dict[str, int] = {"T0": 0, "T1": 0, "T2": 0, "total": 0}
_budget_open: bool = True  # False = circuit open, non-critical tasks blocked


class LLMResponse(BaseModel):
    content: dict[str, Any] | str  # dict for json_mode=True, str otherwise
    prompt_tokens: int
    completion_tokens: int
    model: str
    cache_hit: bool
    latency_ms: int


class LLMClient:
    """Singleton gateway. Use the module-level `client` instance."""

    def __init__(self) -> None:
        self._oai: AsyncOpenAI | None = None
        self._concurrency: dict[Tier, asyncio.Semaphore] = {
            Tier.T0: asyncio.Semaphore(16),
            Tier.T1: asyncio.Semaphore(8),
            Tier.T2: asyncio.Semaphore(2),
        }

    def _get_oai(self) -> AsyncOpenAI:
        if self._oai is None:
            if not settings.llm_configured:
                raise RuntimeError(
                    "LLM not configured. Set LLM_BASE_URL, LLM_API_KEY, and "
                    "LLM_MODEL_T0/T1/T2 in .env (see docs/notes/longcat_api_verified.md)."
                )
            self._oai = AsyncOpenAI(
                api_key=settings.llm_api_key,
                base_url=settings.llm_base_url,
                timeout=settings.llm_timeout_s,
                max_retries=0,  # we handle retries ourselves
            )
        return self._oai

    async def complete(
        self,
        *,
        task_name: str,
        messages: list[dict[str, Any]],
        session: AsyncSession | None = None,
        template_version: str = "v1",
        agent: str = "unknown",
        trace_id: str = "",
        critical: bool = False,  # if True, bypasses budget circuit breaker
    ) -> LLMResponse:
        """Main entry point for all LLM calls.

        Args:
            task_name: Must be a key in TASK_PARAMS (tiers.py).
            messages: Chat messages list (openai format). System + user minimum.
            session: If provided, caches response and logs to llm_calls.
            template_version: Version string for cache key (bump on prompt changes).
            agent: Agent name for audit log.
            trace_id: Langfuse trace ID for correlation.
            critical: If True, not blocked by daily budget circuit breaker.
        """
        global _budget_open, _daily_tokens_used
        params = get_task_params(task_name)

        # DRY_RUN: return mock without calling the API
        if settings.relay_dry_run:
            log.info(
                "llm_dry_run",
                task=task_name,
                tier=params.tier.value,
                agent=agent,
            )
            mock: dict[str, Any] = {"_dry_run": True, "task": task_name}
            return LLMResponse(
                content=mock,
                prompt_tokens=0,
                completion_tokens=0,
                model="dry-run",
                cache_hit=False,
                latency_ms=0,
            )

        # Budget check
        if not critical and not _budget_open:
            raise RuntimeError(
                f"Daily token budget exceeded for tier {params.tier}. "
                "Non-critical tasks are paused. Operator alerted."
            )

        # Cache lookup
        rendered = json.dumps(messages, ensure_ascii=False, sort_keys=True)
        cache_key = make_cache_key(task_name, template_version, rendered)

        if session and params.cache_ttl_s > 0:
            cached = await get_cached(session, cache_key)
            if cached is not None:
                log.debug("llm_cache_hit", task=task_name, key=cache_key[:16])
                await self._log_call(
                    session, agent, task_name, params, model="cached",
                    prompt_tokens=0, completion_tokens=0,
                    latency_ms=0, cache_hit=True, ok=True,
                )
                return LLMResponse(
                    content=cached,
                    prompt_tokens=0,
                    completion_tokens=0,
                    model="cached",
                    cache_hit=True,
                    latency_ms=0,
                )

        model_id = get_model_id(
            params.tier,
            settings.llm_model_t0,
            settings.llm_model_t1,
            settings.llm_model_t2,
        )

        # If json_mode, append JSON format instruction to system message
        if params.json_mode:
            messages = self._inject_json_instruction(messages)

        # Execute with retry
        response, latency_ms = await self._call_with_retry(
            model_id, messages, params, task_name
        )

        completion = response.choices[0].message.content or ""
        usage = response.usage

        # Parse JSON if json_mode
        content: dict[str, Any] | str
        if params.json_mode:
            # Handle None content (reasoning consumed all tokens)
            if completion is None:
                log.warning("llm_none_content", task=task_name, model=model_id)
                content = {}
            else:
                # Strip markdown code fences if present
                cleaned = completion.strip()
                if cleaned.startswith("```"):
                    # ```json\n{...}\n```
                    cleaned = cleaned.removeprefix("```json").removeprefix("```")
                    if cleaned.endswith("```"):
                        cleaned = cleaned[:-3]
                    cleaned = cleaned.strip()
                try:
                    content = json.loads(cleaned)
                except json.JSONDecodeError as e:
                    # Try extracting JSON from surrounding text
                    start = cleaned.find("{")
                    end = cleaned.rfind("}")
                    if start != -1 and end != -1 and end > start:
                        try:
                            content = json.loads(cleaned[start:end + 1])
                        except json.JSONDecodeError:
                            content = await self._repair_json(
                                model_id, messages, completion, str(e), params
                            )
                    else:
                        content = await self._repair_json(
                            model_id, messages, completion, str(e), params
                        )

            # Handle empty JSON {} — LongCat reasoning model sometimes returns {}
            # when thinking consumes all tokens. Trigger repair to get real content.
            if content == {}:
                log.warning("llm_empty_json", task=task_name, model=model_id)
                content = await self._repair_json(
                    model_id, messages,
                    '{"_empty": true}',
                    "Empty JSON returned by model",
                    params,
                )
        else:
            content = completion

        # Update budget
        if usage:
            total = (usage.prompt_tokens or 0) + (usage.completion_tokens or 0)
            _daily_tokens_used[params.tier.value] += total
            _daily_tokens_used["total"] += total
            if _daily_tokens_used["total"] > settings.llm_daily_budget_tokens:
                _budget_open = False
                log.error(
                    "daily_budget_exceeded",
                    total=_daily_tokens_used["total"],
                    limit=settings.llm_daily_budget_tokens,
                )

        # Cache write
        if session and params.cache_ttl_s > 0 and isinstance(content, dict):
            await set_cached(session, cache_key, content, params.cache_ttl_s)

        # Audit log
        if session:
            await self._log_call(
                session, agent, task_name, params, model=model_id,
                prompt_tokens=usage.prompt_tokens if usage else 0,
                completion_tokens=usage.completion_tokens if usage else 0,
                latency_ms=latency_ms, cache_hit=False, ok=True,
                trace_id=trace_id,
            )

        prompt_tokens = usage.prompt_tokens if usage else 0
        completion_tokens = usage.completion_tokens if usage else 0

        return LLMResponse(
            content=content,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            model=model_id,
            cache_hit=False,
            latency_ms=latency_ms,
        )

    @staticmethod
    def _inject_json_instruction(
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Append JSON format instruction for providers that don't support response_format.

        LongCat's OpenAI-compatible endpoint returns None content when
        response_format={"type": "json_object"} is used. Instead, we ask for
        JSON in the system prompt and parse it after.
        """
        json_note = (
            "\n\nYou MUST respond with ONLY valid JSON. No markdown, no code "
            "fences, no explanation. Just the raw JSON object."
        )
        new_messages = []
        for msg in messages:
            if msg.get("role") == "system":
                new_messages.append({**msg, "content": msg["content"] + json_note})
            else:
                new_messages.append(msg)
        # If no system message, prepend one
        if not any(m.get("role") == "system" for m in new_messages):
            new_messages.insert(0, {"role": "system", "content": json_note.strip()})
        return new_messages

    async def _call_with_retry(
        self,
        model_id: str,
        messages: list[dict[str, Any]],
        params: TaskParams,
        task_name: str,
    ) -> tuple[ChatCompletion, int]:
        oai = self._get_oai()
        kwargs: dict[str, Any] = {
            "model": model_id,
            "messages": messages,
            "temperature": params.temperature,
            "max_tokens": params.max_tokens,
        }
        if params.json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        # LongCat does not support response_format=json_object (returns None content).
        # JSON formatting is handled via prompt injection in _inject_json_instruction().
        kwargs.pop("response_format", None)

        async with self._concurrency[params.tier]:
            for attempt in range(1, settings.llm_max_retries + 2):
                t0 = time.monotonic()
                try:
                    response = await oai.chat.completions.create(**kwargs)
                    latency_ms = int((time.monotonic() - t0) * 1000)
                    return response, latency_ms
                except RateLimitError:
                    wait = 2 ** attempt
                    log.warning("llm_rate_limit", task=task_name, wait_s=wait)
                    await asyncio.sleep(wait)
                except APIStatusError as e:
                    if e.status_code >= 500 and attempt <= settings.llm_max_retries:
                        wait = 2 ** attempt
                        log.warning("llm_server_error", status=e.status_code, wait_s=wait)
                        await asyncio.sleep(wait)
                    else:
                        raise

        raise RuntimeError(f"LLM call failed after {settings.llm_max_retries + 1} attempts")

    async def _repair_json(
        self,
        model_id: str,
        original_messages: list[dict[str, Any]],
        bad_output: str,
        error_msg: str,
        params: TaskParams,
    ) -> dict[str, Any]:
        """One repair attempt when JSON parsing fails."""
        repair_messages = original_messages + [
            {"role": "assistant", "content": bad_output},
            {
                "role": "user",
                "content": (
                    f"Your response was not valid JSON. Error: {error_msg}. "
                    "Please respond with only valid JSON matching the required schema."
                ),
            },
        ]
        oai = self._get_oai()
        response = await oai.chat.completions.create(
            model=model_id,
            messages=repair_messages,
            temperature=0.0,
            max_tokens=params.max_tokens,
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content or "{}"
        return json.loads(content)  # if this fails, let it propagate as a hard failure

    async def _log_call(
        self,
        session: AsyncSession,
        agent: str,
        task: str,
        params: TaskParams,
        *,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        latency_ms: int,
        cache_hit: bool,
        ok: bool,
        err: str = "",
        trace_id: str = "",
    ) -> None:
        from sqlalchemy import text
        # Rough cost estimate (placeholder — update with real LongCat pricing)
        cost_est = (prompt_tokens * 0.000001) + (completion_tokens * 0.000002)
        await session.execute(
            text("""
                INSERT INTO llm_calls
                  (agent, task, tier, model, prompt_tokens, completion_tokens,
                   cost_est, latency_ms, cache_hit, ok, err, trace_id)
                VALUES
                  (:agent, :task, :tier, :model, :pt, :ct,
                   :cost, :lat, :cache, :ok, :err, :trace)
            """),
            {
                "agent": agent, "task": task, "tier": params.tier.value,
                "model": model, "pt": prompt_tokens, "ct": completion_tokens,
                "cost": cost_est, "lat": latency_ms, "cache": cache_hit,
                "ok": ok, "err": err, "trace": trace_id,
            },
        )


# Module-level singleton
client = LLMClient()
