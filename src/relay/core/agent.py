"""BaseAgent and run loop.

Every agent is one class implementing:
    handle(event: dict) -> list[dict]  (emitted events to write to outbox)

The agent is a pure function of (event, DB state) → (DB writes, outbox events).
No in-memory state between calls.

The run loop (run_consumer_loop) wraps each agent with:
- Idempotency check (processed_events table)
- DB transaction wrapping handle()
- Retry × MAX_RETRIES → DLQ
- Metrics increments
- DRY_RUN mode: handle() is called but external writes are suppressed
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import redis.asyncio as aioredis
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.config import settings
from relay.core.db import AsyncSessionLocal, mark_event_processed, write_outbox
from relay.core.events import EventConsumer, MAX_RETRIES, _make_redis, setup_consumer_groups

log = structlog.get_logger(__name__)


class BaseAgent(ABC):
    """Contract every agent must implement."""

    #: Name used in logs, metrics, and processed_events.consumer column
    name: str

    @abstractmethod
    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Process one event.

        Args:
            event: The full envelope dict from the event bus.
            session: Open async DB session (do NOT commit; the run loop commits).

        Returns:
            List of outbox event dicts: each must have keys
            {stream, type, idempotency_key, payload}.
            The run loop writes them all to event_outbox inside the same transaction.
        """
        ...


async def run_consumer_loop(
    agent: BaseAgent,
    stream: str,
    group: str,
    *,
    redis: aioredis.Redis | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Continuously read from a stream, dispatch to agent, handle retries/DLQ."""
    if redis is None:
        redis = _make_redis()

    await setup_consumer_groups(redis)

    consumer = EventConsumer(
        redis=redis,
        stream=stream,
        group=group,
        consumer_name=agent.name,
    )

    log.info("agent_started", agent=agent.name, stream=stream, group=group)

    while stop_event is None or not stop_event.is_set():
        try:
            async for entry_id, envelope in consumer.read():
                await _dispatch(agent, consumer, redis, entry_id, envelope)
        except asyncio.CancelledError:
            break
        except Exception:
            log.exception("consumer_loop_error", agent=agent.name)
            await asyncio.sleep(2)

    log.info("agent_stopped", agent=agent.name)


async def _dispatch(
    agent: BaseAgent,
    consumer: EventConsumer,
    redis: aioredis.Redis,
    entry_id: str,
    envelope: dict[str, Any],
) -> None:
    ikey = envelope.get("idempotency_key", entry_id)
    event_type = envelope.get("type", "unknown")

    # Track attempt count in a simple in-process dict (good enough for retry × 3)
    # For a persistent count we'd use the message metadata but this covers 99% of cases.
    attempt = 0
    last_error: Exception | None = None

    while attempt < MAX_RETRIES:
        attempt += 1
        try:
            async with AsyncSessionLocal() as session:
                # Consumer-side idempotency
                is_new = await mark_event_processed(
                    session, consumer=agent.name, idempotency_key=ikey
                )
                if not is_new:
                    log.debug(
                        "event_skipped_duplicate",
                        agent=agent.name,
                        key=ikey,
                        type=event_type,
                    )
                    await consumer.ack(entry_id)
                    await session.commit()
                    return

                if settings.relay_dry_run:
                    log.info(
                        "DRY_RUN handle",
                        agent=agent.name,
                        type=event_type,
                        key=ikey,
                    )

                # Call the agent (may do DB writes on the session)
                outbox_events = await agent.handle(envelope, session)

                # Write emitted events to outbox in same transaction
                for ev in outbox_events:
                    await write_outbox(
                        session,
                        stream=ev["stream"],
                        event_type=ev["type"],
                        idempotency_key=ev["idempotency_key"],
                        payload=ev.get("payload", {}),
                    )

                # Increment agent success counter
                await _increment_metric(session, f"agent.{agent.name}.ok")
                await session.commit()

            await consumer.ack(entry_id)
            log.debug(
                "event_handled",
                agent=agent.name,
                type=event_type,
                attempt=attempt,
            )
            return

        except Exception as e:
            last_error = e
            backoff = 2 ** attempt
            log.warning(
                "event_handling_error",
                agent=agent.name,
                type=event_type,
                attempt=attempt,
                error=str(e),
                retry_in_s=backoff,
            )
            await asyncio.sleep(backoff)

    # All retries exhausted → DLQ
    assert last_error is not None
    await consumer.send_to_dlq(redis, entry_id, envelope, last_error, MAX_RETRIES)
    await _increment_metric_raw(redis, f"agent.{agent.name}.dlq")


async def _increment_metric(session: AsyncSession, key: str) -> None:
    """Upsert a simple integer counter into app_config for dashboard use."""
    await session.execute(
        text("""
            INSERT INTO app_config (key, value, updated_by, updated_at)
            VALUES (:key, CAST('1' AS JSONB), 'system', now())
            ON CONFLICT (key) DO UPDATE
            SET value = to_jsonb((app_config.value::int + 1)),
                updated_at = now()
        """),
        {"key": f"metric:{key}"},
    )


async def _increment_metric_raw(redis: aioredis.Redis, key: str) -> None:
    await redis.incr(f"relay:metric:{key}")
