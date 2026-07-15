"""Manual TrendScout runner — triggers a real trend scan outside dry_run.

Usage:
    python scripts/run_trend_scout.py            # real Rakuten fetch
    python scripts/run_trend_scout.py --dry      # dry run (log only)
    DRY_RUN=1 python scripts/run_trend_scout.py  # env override

What it does:
    1. Creates a tick.trend_scan event
    2. Runs TrendScoutAgent.handle() with real Rakuten credentials
    3. Prints discovered candidates + their scores

Requires:
    - PostgreSQL running (port 5433 via docker, or local :5432)
    - RAKUTEN_APP_ID + RAKUTEN_ACCESS_KEY in .env
    - Alembic migrations applied (trend_candidates table exists)
"""

from __future__ import annotations

import asyncio
import os
import sys

# Force import of everything before async init
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run TrendScout manually")
    parser.add_argument("--dry", action="store_true", help="Dry run mode (no external fetch)")
    parser.add_argument("--limit", type=int, default=20, help="Max candidates to emit (default 20)")
    parser.add_argument("--threshold", type=float, default=None, help="Override score threshold")
    args = parser.parse_args()

    # Override dry_run based on flag/env
    if args.dry or os.environ.get("DRY_RUN"):
        os.environ["RELAY_DRY_RUN"] = "1"

    # Late imports so env override takes effect
    from sqlalchemy import text

    from relay.core.config import settings
    from relay.core.db import AsyncSessionLocal
    from relay.intelligence.trend_scout import TrendScoutAgent

    print(f"═══ TrendScout Manual Runner ═══")
    print(f"  dry_run:       {settings.relay_dry_run}")
    print(f"  rakuten_id:    {'SET' if settings.rakuten_app_id else 'MISSING'}")
    print(f"  rakuten_key:   {'SET' if settings.rakuten_access_key else 'MISSING'}")
    print(f"  limit:         {args.limit}")
    print()

    async with AsyncSessionLocal() as session:
        # Override threshold if requested
        if args.threshold is not None:
            await session.execute(
                text("""
                    INSERT INTO app_config (key, value)
                    VALUES ('trend.score_threshold', :val)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """),
                {"val": f'{{"value": {args.threshold}}}'},
            )
            await session.commit()

        # Create tick event
        event = {
            "type": "tick.trend_scan",
            "payload": {},
            "idempotency_key": f"manual:trend_scan:{asyncio.get_event_loop().time()}",
            "correlation_id": "manual_run",
        }

        agent = TrendScoutAgent()
        print("  ⏳ Running trend scan...\n")
        emitted = await agent.handle(event, session)
        await session.commit()

        # Report results
        if settings.relay_dry_run:
            print("  ℹ️  DRY RUN — no external data fetched")
            print()

        if not emitted:
            print("  📭 No candidates emitted (above threshold)")
            print()
        else:
            print(f"  📡 Emitted {len(emitted)} candidate.discovered events:")
            print(f"  {'ID':>6}  {'Score':>6}  Name")
            print(f"  {'─'*6}  {'─'*6}  {'─'*50}")
            for e in emitted:
                p = e["payload"]
                print(f"  {p['candidate_id']:>6}  {p['accel_score']:>6.2f}  {p['name']}")
            print()

        # Show DB summary
        row = await session.execute(
            text("SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'DISCOVERED'), COUNT(*) FILTER (WHERE status = 'SCORED') FROM trend_candidates")
        )
        total, discovered, scored = row.first()  # type: ignore[misc]
        row = await session.execute(
            text("SELECT COUNT(DISTINCT source) FROM trend_candidates")
        )
        sources = row.scalar_one()
        print(f"  📊 DB status: {total} total, {discovered} unscored, {scored} scanned, {sources} sources")


if __name__ == "__main__":
    asyncio.run(main())
