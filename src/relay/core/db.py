"""Database engine, session factory, and outbox helpers.

Outbox pattern: agents write domain events to event_outbox in the SAME
transaction as their state changes. A relay loop (see events.py) reads
unpublished outbox rows and forwards them to Redis Streams.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from relay.core.config import settings

engine = create_async_engine(
    settings.database_url,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def write_outbox(
    session: AsyncSession,
    *,
    stream: str,
    event_type: str,
    idempotency_key: str,
    payload: dict[str, Any],
) -> None:
    """Append one event to event_outbox (must be called inside an open transaction).

    The outbox relay process picks this up and publishes to Redis, then marks
    published=true. This guarantees no event is lost even if Redis is down at
    write time.
    """
    await session.execute(
        text("""
            INSERT INTO event_outbox (stream, type, idempotency_key, payload)
            VALUES (:stream, :type, :key, CAST(:payload AS JSONB))
            ON CONFLICT (idempotency_key) DO NOTHING
        """),
        {
            "stream": stream,
            "type": event_type,
            "key": idempotency_key,
            "payload": json.dumps(payload),
        },
    )


async def mark_event_processed(
    session: AsyncSession,
    *,
    consumer: str,
    idempotency_key: str,
) -> bool:
    """Insert into processed_events for consumer-side idempotency.

    Returns True if this is the first time we're processing this key (proceed),
    False if it's a duplicate (skip).
    """
    result = await session.execute(
        text("""
            INSERT INTO processed_events (consumer, idempotency_key)
            VALUES (:consumer, :key)
            ON CONFLICT DO NOTHING
            RETURNING consumer
        """),
        {"consumer": consumer, "key": idempotency_key},
    )
    return result.first() is not None


async def healthcheck() -> bool:
    """Return True if Postgres is reachable."""
    try:
        async with get_session() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
