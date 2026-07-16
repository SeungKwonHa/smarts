"""Relay Scheduler — emits tick events on configured cadences.

This is the single place that owns timing. All agents are triggered by events,
never by internal timers. Cadences are:

  tick.trend_scan      6h (jitter ±20m)
  tick.brand_scan      daily 03:00 KST
  tick.longtail_expand daily 04:00 KST
  tick.stock_scan      daily 05:00 KST full sweep (+ tier-1 every 6h)
  tick.fx_refresh      1h
  tick.order_poll      5m
  tick.inquiry_poll    10m
  tick.tracking_poll   4h
  tick.daily_report    07:00 KST
  tick.weekly_promotion Mon 06:00 KST
  tick.weekly_narrative Mon 08:00 KST

In DRY_RUN mode: ticks are emitted to the outbox (so the pipeline can be
walked manually) but external API calls in agents are suppressed.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from relay.core.config import settings
from relay.core.db import AsyncSessionLocal, write_outbox
from relay.core.events import STREAM_TICKS, OutboxRelay, _make_redis, setup_consumer_groups

log = structlog.get_logger(__name__)

KST_OFFSET = 9  # UTC+9


async def emit_tick(event_type: str, payload: dict | None = None) -> None:
    """Write a tick event to the outbox (outbox relay will push to Redis)."""
    ikey = f"{event_type}:{datetime.now(timezone.utc).strftime('%Y%m%d%H%M')}"
    async with AsyncSessionLocal() as session:
        await write_outbox(
            session,
            stream=STREAM_TICKS,
            event_type=event_type,
            idempotency_key=ikey,
            payload=payload or {},
        )
        await session.commit()
    log.info("tick_emitted", type=event_type)


def _jitter(seconds: int) -> int:
    """Add up to seconds of random jitter to avoid thundering-herd."""
    return random.randint(0, seconds)


async def main() -> None:
    log.info("scheduler_starting", dry_run=settings.relay_dry_run)

    redis = _make_redis()
    await setup_consumer_groups(redis)

    # Start outbox relay alongside scheduler
    outbox = OutboxRelay(redis)
    outbox_task = asyncio.create_task(outbox.run())

    scheduler = AsyncIOScheduler(timezone="UTC")

    # ── FX refresh — every 1h ─────────────────────────────────────────────────
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.fx_refresh")),
        IntervalTrigger(hours=1),
        id="fx_refresh",
        max_instances=1,
    )

    # ── Order poll — every 5m ─────────────────────────────────────────────────
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.order_poll")),
        IntervalTrigger(minutes=5),
        id="order_poll",
        max_instances=1,
    )

    # ── Inquiry poll — every 10m ──────────────────────────────────────────────
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.inquiry_poll")),
        IntervalTrigger(minutes=10),
        id="inquiry_poll",
        max_instances=1,
    )

    # ── Tracking poll — every 4h ──────────────────────────────────────────────
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.tracking_poll")),
        IntervalTrigger(hours=4),
        id="tracking_poll",
        max_instances=1,
    )

    # ── Trend scan — every 6h with ±20m jitter ───────────────────────────────
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.trend_scan")),
        IntervalTrigger(hours=6, jitter=20 * 60),
        id="trend_scan",
        max_instances=1,
    )

    # ── Tier-1 stock scan (hot SKUs) — every 6h ──────────────────────────────
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.stock_scan", {"tier": 1})),
        IntervalTrigger(hours=6, jitter=5 * 60),
        id="stock_scan_tier1",
        max_instances=1,
    )

    # ── Daily jobs (KST = UTC - 9h, so KST 03:00 = UTC 18:00 prev day) ───────
    # brand_scan   03:00 KST = 18:00 UTC
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.brand_scan")),
        CronTrigger(hour=18, minute=0, timezone="UTC"),
        id="brand_scan",
        max_instances=1,
    )
    # longtail_expand  04:00 KST = 19:00 UTC
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.longtail_expand")),
        CronTrigger(hour=19, minute=0, timezone="UTC"),
        id="longtail_expand",
        max_instances=1,
    )
    # stock_scan full  05:00 KST = 20:00 UTC
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.stock_scan", {"tier": "all"})),
        CronTrigger(hour=20, minute=0, timezone="UTC"),
        id="stock_scan_full",
        max_instances=1,
    )
    # daily_report  07:00 KST = 22:00 UTC
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.daily_report")),
        CronTrigger(hour=22, minute=0, timezone="UTC"),
        id="daily_report",
        max_instances=1,
    )
    # weekly_promotion  Mon 06:00 KST = Sun 21:00 UTC
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.weekly_promotion")),
        CronTrigger(day_of_week="sun", hour=21, minute=0, timezone="UTC"),
        id="weekly_promotion",
        max_instances=1,
    )
    # weekly_narrative  Mon 08:00 KST = Sun 23:00 UTC
    scheduler.add_job(
        lambda: asyncio.ensure_future(emit_tick("tick.weekly_narrative")),
        CronTrigger(day_of_week="sun", hour=23, minute=0, timezone="UTC"),
        id="weekly_narrative",
        max_instances=1,
    )

    scheduler.start()
    log.info("scheduler_started", jobs=len(scheduler.get_jobs()))

    try:
        await asyncio.Event().wait()  # run forever
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        scheduler.shutdown()
        outbox.stop()
        outbox_task.cancel()
        await redis.aclose()
        log.info("scheduler_stopped")


if __name__ == "__main__":
    import structlog
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ]
    )
    asyncio.run(main())
