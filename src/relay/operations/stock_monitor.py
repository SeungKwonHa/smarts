"""StockMonitor — O1 agent. Highest-priority agent in the system.

Trigger: tick.stock_scan (full daily sweep at 05:00 KST; tier-1 every 6h).

For every LIVE listing:
- Re-checks source price and stock state.
- Price moved beyond tolerance → price.reprice_required.
- OOS → immediately suspend listing via Naver API → status=SUSPENDED_STOCKOUT.
- Back in stock → reactivate.

SLA: any LIVE listing with source data older than 36h = ALERT (dashboard red).
This agent failing silently is the #1 kill risk in zero-inventory.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.config import settings
from relay.core.events import STREAM_LISTING, STREAM_OPS
from relay.integrations.naver.client import suspend_product, reactivate_product
from relay.integrations.rakuten.client import get_item as rakuten_get_item
from relay.integrations.amazon_jp.client import get_product as amazon_jp_get_product, extract_asin_from_url

log = structlog.get_logger(__name__)

# Price change tolerance — reprice if source price moved more than ±5%
_PRICE_TOLERANCE = 0.05
# Staleness threshold
_STALENESS_HOURS = settings.stock_staleness_alert_hours


class StockMonitorAgent(BaseAgent):
    """O1 — sweeps all LIVE listings' sources for price/stock changes."""

    name = "stock_monitor"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        if event_type == "tick.stock_scan":
            return await self._sweep(payload, session)
        return []

    async def _sweep(
        self,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        tier = payload.get("tier", "all")

        # Select sources for LIVE listings (filter by scan_tier if partial sweep)
        if tier == 1:
            # Hot SKUs only (tier-1 = sold recently)
            query = text("""
                SELECT ps.id, ps.product_id, ps.marketplace, ps.url,
                       ps.price_minor, ps.currency, ps.stock_state, ps.last_checked_at,
                       l.id AS listing_id, l.status AS listing_status,
                       l.sell_price_krw
                FROM product_sources ps
                JOIN products p ON p.id = ps.product_id
                JOIN listings l ON l.product_id = p.id
                WHERE l.status IN ('LIVE', 'SUSPENDED_STOCKOUT')
                  AND l.scan_tier = 1
                  AND l.marketplace = 'naver'
                ORDER BY ps.last_checked_at NULLS FIRST
                LIMIT 200
            """)
        else:
            # Full sweep
            query = text("""
                SELECT ps.id, ps.product_id, ps.marketplace, ps.url,
                       ps.price_minor, ps.currency, ps.stock_state, ps.last_checked_at,
                       l.id AS listing_id, l.status AS listing_status,
                       l.sell_price_krw
                FROM product_sources ps
                JOIN products p ON p.id = ps.product_id
                JOIN listings l ON l.product_id = p.id
                WHERE l.status IN ('LIVE', 'SUSPENDED_STOCKOUT')
                  AND l.marketplace = 'naver'
                ORDER BY ps.last_checked_at NULLS FIRST
                LIMIT 1000
            """)

        rows = await session.execute(query)
        sources = rows.fetchall()

        if not sources:
            log.info("stock_monitor_sweep_empty", tier=tier)
            return []

        log.info("stock_monitor_sweep_start", tier=tier, count=len(sources))

        emitted: list[dict[str, Any]] = []
        stale_count = 0
        now = datetime.now(timezone.utc)

        for row in sources:
            (
                source_id, product_id, marketplace, url,
                old_price, currency, old_state, last_checked,
                listing_id, listing_status, sell_price_krw,
            ) = row

            # Check staleness
            if last_checked:
                age_h = (now - last_checked.replace(tzinfo=timezone.utc)).total_seconds() / 3600
                if age_h > _STALENESS_HOURS:
                    stale_count += 1

            # Fetch current price + stock
            try:
                new_price, new_state = await self._fetch_source(marketplace, url)
            except Exception as e:
                log.error("stock_check_fetch_error", source_id=source_id, error=str(e))
                continue

            # Update last_checked
            await session.execute(
                text("""
                    UPDATE product_sources
                    SET price_minor = :price, stock_state = :state, last_checked_at = now()
                    WHERE id = :id
                """),
                {"price": new_price or old_price, "state": new_state, "id": source_id},
            )

            # Handle state changes
            events = await self._handle_change(
                session=session,
                source_id=source_id,
                product_id=product_id,
                listing_id=listing_id,
                listing_status=listing_status,
                old_price=old_price,
                new_price=new_price,
                currency=currency,
                old_state=old_state,
                new_state=new_state,
            )
            emitted.extend(events)

        if stale_count > 0:
            log.error(
                "stock_monitor_stale_sources",
                stale_count=stale_count,
                threshold_hours=_STALENESS_HOURS,
            )

        log.info(
            "stock_monitor_sweep_done",
            tier=tier,
            checked=len(sources),
            events=len(emitted),
            stale=stale_count,
        )
        return emitted

    async def _fetch_source(
        self,
        marketplace: str,
        url: str,
    ) -> tuple[int | None, str]:
        """Fetch current price and stock state from source."""
        if settings.relay_dry_run:
            return None, "IN_STOCK"  # No external calls in dry run

        if marketplace == "rakuten":
            from relay.integrations.rakuten.client import extract_item_code_from_url
            item_code = extract_item_code_from_url(url)
            if not item_code:
                return None, "UNKNOWN"
            item = await rakuten_get_item(item_code, cache_s=0)  # no cache for stock check
            if item is None:
                return None, "UNKNOWN"
            return item.price_jpy, item.stock_state

        elif marketplace == "amazon_jp":
            asin = extract_asin_from_url(url)
            if not asin:
                return None, "UNKNOWN"
            item = await amazon_jp_get_product(asin, cache_s=0)
            if item is None:
                return None, "UNKNOWN"
            return item.price_jpy, item.stock_state

        return None, "UNKNOWN"

    async def _handle_change(
        self,
        *,
        session: AsyncSession,
        source_id: int,
        product_id: int,
        listing_id: int,
        listing_status: str,
        old_price: int,
        new_price: int | None,
        currency: str,
        old_state: str,
        new_state: str,
    ) -> list[dict[str, Any]]:
        emitted: list[dict[str, Any]] = []

        # Stock state change
        if new_state != old_state:
            if new_state == "OOS" and listing_status == "LIVE":
                # Suspend listing: DB + Naver API (zero-inventory kill-risk fix)
                await session.execute(
                    text("""
                        UPDATE listings
                        SET status = 'SUSPENDED_STOCKOUT'
                        WHERE id = :id AND status = 'LIVE'
                    """),
                    {"id": listing_id},
                )
                # Also suspend on Naver side so it stops showing in search
                if not settings.relay_dry_run:
                    try:
                        row = await session.execute(
                            text("SELECT remote_product_id FROM listings WHERE id = :id"),
                            {"id": listing_id},
                        )
                        remote_id = row.scalar()
                        if remote_id and remote_id != "DRY_RUN_0":
                            await suspend_product(str(remote_id))
                            log.info(
                                "naver_oos_synced",
                                listing_id=listing_id,
                                remote_id=remote_id,
                            )
                    except Exception as e:
                        log.error(
                            "naver_oos_sync_failed",
                            listing_id=listing_id,
                            error=str(e),
                        )
                log.warning(
                    "listing_suspended_oos",
                    listing_id=listing_id,
                    source_id=source_id,
                )
                emitted.append({
                    "stream": STREAM_OPS,
                    "type": "stock.changed",
                    "idempotency_key": f"source:{source_id}:oos:{_today()}",
                    "payload": {
                        "source_id": source_id,
                        "product_id": product_id,
                        "state": "oos",
                        "old": old_state,
                        "new": new_state,
                        "listing_id": listing_id,
                    },
                })

            elif new_state == "IN_STOCK" and listing_status == "SUSPENDED_STOCKOUT":
                # Reactivate listing: DB + Naver API
                await session.execute(
                    text("""
                        UPDATE listings
                        SET status = 'LIVE'
                        WHERE id = :id AND status = 'SUSPENDED_STOCKOUT'
                    """),
                    {"id": listing_id},
                )
                # Reactivate on Naver side
                if not settings.relay_dry_run:
                    try:
                        row = await session.execute(
                            text("SELECT remote_product_id FROM listings WHERE id = :id"),
                            {"id": listing_id},
                        )
                        remote_id = row.scalar()
                        if remote_id and remote_id != "DRY_RUN_0":
                            await reactivate_product(str(remote_id))
                            log.info(
                                "naver_restock_synced",
                                listing_id=listing_id,
                                remote_id=remote_id,
                            )
                    except Exception as e:
                        log.error(
                            "naver_restock_sync_failed",
                            listing_id=listing_id,
                            error=str(e),
                        )
                log.info(
                    "listing_reactivated",
                    listing_id=listing_id,
                    source_id=source_id,
                )
                emitted.append({
                    "stream": STREAM_OPS,
                    "type": "stock.changed",
                    "idempotency_key": f"source:{source_id}:restock:{_today()}",
                    "payload": {
                        "source_id": source_id,
                        "product_id": product_id,
                        "state": "restock",
                        "old": old_state,
                        "new": new_state,
                        "listing_id": listing_id,
                    },
                })

        # Price change
        if new_price and old_price and old_price > 0:
            delta = abs(new_price - old_price) / old_price
            if delta > _PRICE_TOLERANCE:
                log.info(
                    "price_moved",
                    source_id=source_id,
                    old=old_price,
                    new=new_price,
                    delta_pct=round(delta * 100, 1),
                )
                emitted.append({
                    "stream": STREAM_LISTING,
                    "type": "price.reprice_required",
                    "idempotency_key": f"listing:{listing_id}:reprice:{_today()}",
                    "payload": {
                        "listing_id": listing_id,
                        "cause": "src_price",
                        "delta": round(delta, 4),
                        "old_price": old_price,
                        "new_price": new_price,
                    },
                })

        return emitted


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")
