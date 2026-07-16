"""SKUManager — A1 agent.

Trigger: tick.daily_report (runs after Reporter).

Does: Rolling stats per listing → lifecycle policies:
- No sale & no click in N days → retire (delist) → emit sku.retire
- Risers (sold in last 7d) → raise scan_tier to 1 (hot) → emit sku.tier_change
- Chronic source instability (3+ OOS events in 30d) → retire → emit sku.retire

Keeps live-SKU count within account health budget.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.config import settings
from relay.core.events import STREAM_ANALYTICS

log = structlog.get_logger(__name__)


class SKUManagerAgent(BaseAgent):
    """A1 — manages listing lifecycle: retire dead SKUs, promote risers."""

    name = "sku_manager"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        if event.get("type") != "tick.daily_report":
            return []

        return await self._analyze(session)

    async def _analyze(self, session: AsyncSession) -> list[dict[str, Any]]:
        """Run lifecycle policies on all LIVE listings."""
        emitted: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        # Load config thresholds
        no_sale_days = await self._get_config_int(
            session, "sku.retire.no_sale_days", 30
        )
        oos_threshold = await self._get_config_int(
            session, "sku.retire.oos_event_threshold", 3
        )
        riser_orders = await self._get_config_int(
            session, "sku.tier.riser_orders_7d", 1
        )

        # Get all LIVE listings with their recent order stats
        listings = await self._get_live_listings_stats(session)
        if not listings:
            log.info("sku_manager_no_live_listings")
            return []

        log.info("sku_manager_analysis_start", live_count=len(listings))

        for listing in listings:
            listing_id = listing["id"]
            orders_30d = listing["orders_30d"]
            orders_7d = listing["orders_7d"]
            created_at = listing["created_at"]
            scan_tier = listing["scan_tier"]
            oos_events_30d = listing["oos_events_30d"]

            # Policy 1: Chronic source instability → retire
            if oos_events_30d >= oos_threshold:
                await self._retire_listing(
                    session, listing_id, "chronic_oos", oos_events_30d
                )
                emitted.append({
                    "stream": STREAM_ANALYTICS,
                    "type": "sku.retire",
                    "idempotency_key": f"sku:{listing_id}:retire:oos:{_today()}",
                    "payload": {
                        "listing_id": listing_id,
                        "reason": "chronic_oos",
                        "oos_events_30d": oos_events_30d,
                    },
                })
                continue

            # Policy 2: Dead SKU (no sales in N days + old enough)
            age_days = (now - created_at.replace(tzinfo=timezone.utc)).days
            if orders_30d == 0 and age_days >= no_sale_days:
                await self._retire_listing(
                    session, listing_id, "no_sales", age_days
                )
                emitted.append({
                    "stream": STREAM_ANALYTICS,
                    "type": "sku.retire",
                    "idempotency_key": f"sku:{listing_id}:retire:dead:{_today()}",
                    "payload": {
                        "listing_id": listing_id,
                        "reason": "no_sales",
                        "age_days": age_days,
                    },
                })
                continue

            # Policy 3: Riser (sold recently) → promote to tier 1 (hot scan)
            if orders_7d >= riser_orders and scan_tier == 2:
                await self._change_tier(session, listing_id, 1)
                emitted.append({
                    "stream": STREAM_ANALYTICS,
                    "type": "sku.tier_change",
                    "idempotency_key": f"sku:{listing_id}:tier_up:{_today()}",
                    "payload": {
                        "listing_id": listing_id,
                        "scan_tier": 1,
                        "reason": "riser",
                        "orders_7d": orders_7d,
                    },
                })
                continue

        log.info(
            "sku_manager_analysis_done",
            live_count=len(listings),
            events=len(emitted),
        )
        return emitted

    async def _get_live_listings_stats(
        self, session: AsyncSession
    ) -> list[dict[str, Any]]:
        """Get all LIVE listings with order stats and OOS event count."""
        row = await session.execute(text("""
            SELECT
                l.id,
                l.created_at,
                l.scan_tier,
                COALESCE(o30.cnt, 0) AS orders_30d,
                COALESCE(o7.cnt, 0) AS orders_7d,
                COALESCE(oos.cnt, 0) AS oos_events_30d
            FROM listings l
            LEFT JOIN (
                SELECT listing_id, COUNT(*) AS cnt
                FROM orders
                WHERE created_at > now() - INTERVAL '30 days'
                  AND status NOT IN ('CANCELLED', 'HOLD_STOCKOUT')
                GROUP BY listing_id
            ) o30 ON o30.listing_id = l.id
            LEFT JOIN (
                SELECT listing_id, COUNT(*) AS cnt
                FROM orders
                WHERE created_at > now() - INTERVAL '7 days'
                  AND status NOT IN ('CANCELLED', 'HOLD_STOCKOUT')
                GROUP BY listing_id
            ) o7 ON o7.listing_id = l.id
            LEFT JOIN (
                SELECT ps.product_id, COUNT(*) AS cnt
                FROM product_sources ps
                WHERE ps.stock_state = 'OOS'
                  AND ps.last_checked_at > now() - INTERVAL '30 days'
                GROUP BY ps.product_id
            ) oos ON oos.product_id = l.product_id
            WHERE l.status = 'LIVE'
              AND l.marketplace = 'naver'
        """))
        return [
            {
                "id": r[0],
                "created_at": r[1],
                "scan_tier": r[2],
                "orders_30d": r[3],
                "orders_7d": r[4],
                "oos_events_30d": r[5],
            }
            for r in row.fetchall()
        ]

    async def _retire_listing(
        self,
        session: AsyncSession,
        listing_id: int,
        reason: str,
        metric: int,
    ) -> None:
        """Retire a listing: set status to RETIRED."""
        await session.execute(
            text("UPDATE listings SET status = 'RETIRED' WHERE id = :id"),
            {"id": listing_id},
        )
        log.info(
            "sku_retired",
            listing_id=listing_id,
            reason=reason,
            metric=metric,
        )

    async def _change_tier(
        self,
        session: AsyncSession,
        listing_id: int,
        new_tier: int,
    ) -> None:
        """Update listing scan tier."""
        await session.execute(
            text("UPDATE listings SET scan_tier = :tier WHERE id = :id"),
            {"tier": new_tier, "id": listing_id},
        )
        log.info(
            "sku_tier_changed",
            listing_id=listing_id,
            new_tier=new_tier,
        )

    async def _get_config_int(
        self,
        session: AsyncSession,
        key: str,
        default: int,
    ) -> int:
        """Read integer config from app_config."""
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :key"),
            {"key": key},
        )
        result = row.first()
        if result is None:
            return default
        value = result[0]
        if isinstance(value, dict):
            return int(value.get("value", default))
        if isinstance(value, (int, float)):
            return int(value)
        return default


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")
