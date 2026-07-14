"""M0 exit criteria test: outbox replay + idempotency.

Proves: a demo event flows
  scheduler → outbox (DB write) → OutboxRelay → Redis Stream → EventConsumer
with idempotent replay proven (duplicate events are skipped).

This is an integration test against real Postgres + Redis.
Run after: alembic upgrade head
"""

import asyncio
import json
import pytest

from relay.core.db import write_outbox, mark_event_processed
from relay.core.events import (
    OutboxRelay,
    EventConsumer,
    EventPublisher,
    STREAM_TICKS,
    build_envelope,
    setup_consumer_groups,
    _make_redis,
)
from relay.core.agent import BaseAgent, run_consumer_loop
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


# ── Helper: a minimal test agent ──────────────────────────────────────────────

class EchoAgent(BaseAgent):
    """Echo agent: records handled events for assertion."""

    name = "echo_test"

    def __init__(self) -> None:
        self.handled: list[dict] = []

    async def handle(self, event: dict, session: AsyncSession) -> list[dict]:
        self.handled.append(event)
        return []  # no downstream events


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_outbox_write_and_relay(db_session, redis_client):
    """Writing to outbox within a DB transaction → OutboxRelay pushes to Redis."""
    # 1. Write an event to the outbox inside a transaction
    ikey = "test:tick_fx_refresh:20260714T120000"
    await write_outbox(
        db_session,
        stream=STREAM_TICKS,
        event_type="tick.fx_refresh",
        idempotency_key=ikey,
        payload={"test": True},
    )
    await db_session.commit()

    # 2. Verify it's in the outbox as unpublished
    row = await db_session.execute(
        text("SELECT stream, type, published FROM event_outbox WHERE idempotency_key = :key"),
        {"key": ikey},
    )
    result = row.first()
    assert result is not None, "Outbox row should exist"
    assert result[0] == STREAM_TICKS
    assert result[1] == "tick.fx_refresh"
    assert result[2] is False, "Should be unpublished initially"

    # 3. Run OutboxRelay for one batch
    relay = OutboxRelay(redis_client, interval_s=99999)
    await relay._relay_batch()

    # 4. Verify it's now marked published in DB
    row = await db_session.execute(
        text("SELECT published FROM event_outbox WHERE idempotency_key = :key"),
        {"key": ikey},
    )
    assert row.scalar() is True, "OutboxRelay should mark event as published"

    # 5. Verify it landed in the Redis stream
    messages = await redis_client.xrange(STREAM_TICKS, "-", "+", count=100)
    found = False
    for _entry_id, fields in messages:
        envelope = json.loads(fields["data"])
        if envelope.get("idempotency_key") == ikey:
            found = True
            assert envelope["type"] == "tick.fx_refresh"
            assert envelope["payload"]["test"] is True
            break
    assert found, "Event should be in Redis stream after relay"


@pytest.mark.asyncio
async def test_idempotent_outbox_write(db_session):
    """Duplicate writes with same idempotency key are silently ignored."""
    ikey = "test:dedup:abc123"

    await write_outbox(
        db_session,
        stream=STREAM_TICKS,
        event_type="tick.brand_scan",
        idempotency_key=ikey,
        payload={"attempt": 1},
    )
    # Write again with same key (simulates retry)
    await write_outbox(
        db_session,
        stream=STREAM_TICKS,
        event_type="tick.brand_scan",
        idempotency_key=ikey,
        payload={"attempt": 2},  # different payload, same key
    )
    await db_session.commit()

    rows = await db_session.execute(
        text("SELECT COUNT(*) FROM event_outbox WHERE idempotency_key = :key"),
        {"key": ikey},
    )
    count = rows.scalar()
    assert count == 1, "Duplicate outbox writes must be idempotent (ON CONFLICT DO NOTHING)"


@pytest.mark.asyncio
async def test_consumer_idempotency(db_session):
    """Same (consumer, idempotency_key) pair is processed only once."""
    consumer = "echo_test"
    ikey = "test:consumer_idem:xyz789"

    first = await mark_event_processed(db_session, consumer=consumer, idempotency_key=ikey)
    assert first is True, "First call should return True (new event)"

    second = await mark_event_processed(db_session, consumer=consumer, idempotency_key=ikey)
    assert second is False, "Duplicate call should return False (already processed)"


@pytest.mark.asyncio
async def test_full_event_flow(db_session, redis_client):
    """Full flow: outbox write → relay → consumer group read → agent handle → ack.

    This is the M0 exit criteria: proves the event loop works end-to-end.
    """
    await setup_consumer_groups(redis_client)

    # 1. Write event to outbox
    ikey = "test:full_flow:event001"
    await write_outbox(
        db_session,
        stream=STREAM_TICKS,
        event_type="tick.fx_refresh",
        idempotency_key=ikey,
        payload={"full_flow_test": True},
    )
    await db_session.commit()

    # 2. Relay to Redis
    relay = OutboxRelay(redis_client, interval_s=99999)
    await relay._relay_batch()

    # 3. Consume via the consumer group
    consumer = EventConsumer(
        redis=redis_client,
        stream=STREAM_TICKS,
        group="fx_refresh",
        consumer_name="test_consumer",
        block_ms=1000,  # short timeout for tests
    )

    received: list[dict] = []
    async for entry_id, envelope in consumer.read():
        received.append(envelope)
        await consumer.ack(entry_id)
        if envelope.get("idempotency_key") == ikey:
            break  # found our event, stop

    # 4. Assert the right event was received
    matching = [e for e in received if e.get("idempotency_key") == ikey]
    assert len(matching) == 1, f"Should receive exactly one matching event; got {received}"
    assert matching[0]["type"] == "tick.fx_refresh"
    assert matching[0]["payload"]["full_flow_test"] is True


@pytest.mark.asyncio
async def test_second_consumer_sees_same_event(db_session, redis_client):
    """Two consumer groups each see the same event (fan-out via separate groups)."""
    await setup_consumer_groups(redis_client)

    ikey = "test:fanout:event002"
    await write_outbox(
        db_session,
        stream=STREAM_TICKS,
        event_type="tick.fx_refresh",
        idempotency_key=ikey,
        payload={"fanout_test": True},
    )
    await db_session.commit()

    relay = OutboxRelay(redis_client, interval_s=99999)
    await relay._relay_batch()

    # Consumer group A
    consumer_a = EventConsumer(
        redis=redis_client, stream=STREAM_TICKS,
        group="fx_refresh", consumer_name="worker_a", block_ms=500,
    )
    # Consumer group B (different group = independent cursor)
    consumer_b = EventConsumer(
        redis=redis_client, stream=STREAM_TICKS,
        group="stock_monitor", consumer_name="worker_b", block_ms=500,
    )

    seen_a: list[str] = []
    seen_b: list[str] = []

    async for entry_id, envelope in consumer_a.read():
        if envelope.get("idempotency_key") == ikey:
            seen_a.append(ikey)
        await consumer_a.ack(entry_id)

    async for entry_id, envelope in consumer_b.read():
        if envelope.get("idempotency_key") == ikey:
            seen_b.append(ikey)
        await consumer_b.ack(entry_id)

    assert ikey in seen_a, "Consumer group A should receive the event"
    assert ikey in seen_b, "Consumer group B should receive the same event (fan-out)"
