"""Content-addressed LLM response cache.

Key: sha256(task_name + template_version + rendered_input_json)
Store: llm_cache table in Postgres (TTL enforced by expires_at column).

Expected cache hit rate: ≥20% on T0 normalization/category tasks
(many longtail products share the same category/attributes).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def make_cache_key(task_name: str, template_version: str, rendered_input: str) -> str:
    payload = f"{task_name}:{template_version}:{rendered_input}"
    return hashlib.sha256(payload.encode()).hexdigest()


async def get_cached(
    session: AsyncSession,
    cache_key: str,
) -> dict[str, Any] | None:
    """Return cached response JSON or None if miss/expired."""
    row = await session.execute(
        text("""
            SELECT response
            FROM llm_cache
            WHERE cache_key = :key AND expires_at > now()
        """),
        {"key": cache_key},
    )
    result = row.first()
    if result is None:
        return None
    payload = result[0]
    return payload if isinstance(payload, dict) else json.loads(payload)


async def set_cached(
    session: AsyncSession,
    cache_key: str,
    response: dict[str, Any],
    ttl_s: int,
) -> None:
    """Upsert a cached response with TTL."""
    if ttl_s <= 0:
        return
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_s)
    await session.execute(
        text("""
            INSERT INTO llm_cache (cache_key, response, expires_at)
            VALUES (:key, CAST(:response AS JSONB), :expires_at)
            ON CONFLICT (cache_key) DO UPDATE
            SET response = EXCLUDED.response,
                expires_at = EXCLUDED.expires_at
        """),
        {
            "key": cache_key,
            "response": json.dumps(response),
            "expires_at": expires_at,
        },
    )
