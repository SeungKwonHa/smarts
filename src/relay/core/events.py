"""Event bus — Redis Streams publish/consume/DLQ + outbox relay.

Architecture:
- Producers: write to event_outbox in the same DB transaction as state changes.
- Outbox relay: reads unpublished outbox rows, XADDs to Redis, marks published.
- Consumers: XREADGROUP from their consumer group; on failure → XAUTOCLAIM retry
  × 3 → relay:dlq with error payload.

Every event wraps in the standard envelope (see docs/04_EVENT_CONTRACTS.md).
"""

from __future__ import annotations

import asyncio
import json
import traceback
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import redis.asyncio as aioredis
from sqlalchemy import text

from relay.core.config import settings
from relay.core.db import AsyncSessionLocal

import structlog

log = structlog.get_logger(__name__)

# ── Stream names (doc 04) ────────────────────────────────────────────────────
STREAM_TICKS       = "relay:ticks"
STREAM_INTEL       = "relay:intel"
STREAM_LISTING     = "relay:listing"
STREAM_OPS         = "relay:ops"
STREAM_CS          = "relay:cs"
STREAM_ANALYTICS   = "relay:analytics"
STREAM_APPROVALS   = "relay:approvals"
STREAM_DLQ         = "relay:dlq"

ALL_STREAMS = [
    STREAM_TICKS, STREAM_INTEL, STREAM_LISTING, STREAM_OPS,
    STREAM_CS, STREAM_ANALYTICS, STREAM_APPROVALS, STREAM_DLQ,
]

# ── Retry policy ──────────────────────────────────────────────────────────────
MAX_RETRIES = 3
CLAIM_IDLE_MS = 5 * 60 * 1000  # 5 minutes before XAUTOCLAIM picks it up


def _make_redis() -> aioredis.Redis:
    return aioredis.from_url(settings.redis_url, decode_responses=True)


def build_envelope(
    *,
    event_type: str,
    producer: str,
    idempotency_key: str,
    correlation_id: str,
    payload: dict[str, Any],
    version: int = 1,
) -> dict[str, Any]:
    """Build the standard event envelope (doc 04)."""
    return {
        "event_id": str(uuid4()),
        "type": event_type,
        "version": version,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "producer": producer,
        "idempotency_key": idempotency_key,
        "correlation_id": correlation_id,
        "payload": payload,
    }


async def setup_consumer_groups(redis: aioredis.Redis) -> None:
    """Create all streams + consumer groups at startup (idempotent)."""
    # Map stream → list of consumer group names (one group per agent)
    groups: dict[str, list[str]] = {
        STREAM_TICKS:     ["scheduler", "trend_scout", "brand_scout", "longtail_expand",
                           "stock_monitor", "fx_refresh", "order_poll", "inquiry_poll",
                           "tracking_poll", "daily_report", "weekly_promotion"],
        STREAM_INTEL:     ["gap_analyzer", "risk_filter", "source_matcher", "brand_scout"],
        STREAM_LISTING:   ["pricing", "content", "publisher", "analytics"],
        STREAM_OPS:       ["order_agent", "logistics", "stock_monitor", "cs", "analytics"],
        STREAM_CS:        ["inquiry", "claim_triage"],
        STREAM_ANALYTICS: ["promotion", "reporter", "brand_scout"],
        STREAM_APPROVALS: ["order_agent", "publisher", "claim_triage", "brand_scout"],
        STREAM_DLQ:       ["dashboard"],
    }
    for stream, group_names in groups.items():
        for group in group_names:
            try:
                await redis.xgroup_create(stream, group, id="$", mkstream=True)
                log.debug("consumer_group_created", stream=stream, group=group)
            except aioredis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    pass  # already exists — idempotent
                else:
                    raise


class EventPublisher:
    """Thin wrapper; agents should use write_outbox() instead (safe with transactions).

    This class is for the outbox relay loop and for tests.
    """

    def __init__(self, redis: aioredis.Redis) -> None:
        self._r = redis

    async def publish(self, stream: str, envelope: dict[str, Any]) -> str:
        """XADD to Redis; return the stream entry ID."""
        flat: dict[str, str] = {"data": json.dumps(envelope)}
        entry_id: str = await self._r.xadd(stream, flat)
        return entry_id


class OutboxRelay:
    """Background task: polls event_outbox → publishes to Redis → marks sent.

    Runs as a tight loop inside each worker process. In production, all workers
    run this loop (safe because of the SELECT ... FOR UPDATE SKIP LOCKED pattern).
    """

    def __init__(self, redis: aioredis.Redis, interval_s: float = 0.5) -> None:
        self._redis = redis
        self._publisher = EventPublisher(redis)
        self._interval = interval_s
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("outbox_relay_started")
        while self._running:
            try:
                await self._relay_batch()
            except Exception:
                log.exception("outbox_relay_error")
            await asyncio.sleep(self._interval)

    async def _relay_batch(self) -> None:
        async with AsyncSessionLocal() as session:
            rows = await session.execute(
                text("""
                    SELECT id, stream, type, idempotency_key, payload
                    FROM event_outbox
                    WHERE NOT published
                    ORDER BY id
                    LIMIT 50
                    FOR UPDATE SKIP LOCKED
                """)
            )
            items = rows.fetchall()
            if not items:
                return

            for row_id, stream, event_type, ikey, payload in items:
                envelope = build_envelope(
                    event_type=event_type,
                    producer="outbox",
                    idempotency_key=ikey,
                    correlation_id=ikey,
                    payload=payload if isinstance(payload, dict) else json.loads(payload),
                )
                await self._publisher.publish(stream, envelope)
                await session.execute(
                    text("""
                        UPDATE event_outbox
                        SET published = true, published_at = now()
                        WHERE id = :id
                    """),
                    {"id": row_id},
                )
                log.debug("outbox_relayed", type=event_type, key=ikey)

            await session.commit()

    def stop(self) -> None:
        self._running = False


class EventConsumer:
    """Read from a Redis Stream consumer group with retry/DLQ logic."""

    def __init__(
        self,
        redis: aioredis.Redis,
        stream: str,
        group: str,
        consumer_name: str,
        block_ms: int = 5000,
    ) -> None:
        self._r = redis
        self.stream = stream
        self.group = group
        self.consumer_name = consumer_name
        self.block_ms = block_ms

    async def read(self) -> AsyncGenerator[tuple[str, dict[str, Any]], None]:
        """Yield (entry_id, envelope). Call ack() after processing."""
        # First, reclaim any messages idle > CLAIM_IDLE_MS
        await self._reclaim_idle()

        results = await self._r.xreadgroup(
            groupname=self.group,
            consumername=self.consumer_name,
            streams={self.stream: ">"},
            count=10,
            block=self.block_ms,
        )
        if not results:
            return

        for _stream, messages in results:
            for entry_id, fields in messages:
                raw = fields.get("data", "{}")
                envelope: dict[str, Any] = json.loads(raw)
                yield entry_id, envelope

    async def ack(self, entry_id: str) -> None:
        await self._r.xack(self.stream, self.group, entry_id)

    async def send_to_dlq(
        self,
        redis: aioredis.Redis,
        entry_id: str,
        envelope: dict[str, Any],
        error: Exception,
        attempts: int,
    ) -> None:
        dlq_envelope = build_envelope(
            event_type="dlq.failed",
            producer="retry_wrapper",
            idempotency_key=f"dlq:{entry_id}",
            correlation_id=envelope.get("correlation_id", ""),
            payload={
                "original": envelope,
                "error": str(error),
                "traceback_summary": traceback.format_exc()[-1000:],
                "attempts": attempts,
                "source_stream": self.stream,
                "source_entry_id": entry_id,
            },
        )
        await redis.xadd(STREAM_DLQ, {"data": json.dumps(dlq_envelope)})
        await self.ack(entry_id)
        log.error(
            "event_sent_to_dlq",
            type=envelope.get("type"),
            entry_id=entry_id,
            error=str(error),
        )

    async def _reclaim_idle(self) -> None:
        try:
            result = await self._r.xautoclaim(
                self.stream,
                self.group,
                self.consumer_name,
                CLAIM_IDLE_MS,
                start_id="0-0",
                count=10,
            )
            # result = (next_id, [(entry_id, fields), ...], [deleted_ids])
            reclaimed = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else []
            if reclaimed:
                log.info("events_reclaimed", count=len(reclaimed), stream=self.stream)
        except Exception:
            pass  # XAUTOCLAIM not supported on older Redis; skip
