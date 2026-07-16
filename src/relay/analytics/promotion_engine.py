"""PromotionEngine — A2 agent.

Trigger: tick.weekly_promotion (Monday 06:00 KST).

Does:
- Base→Middle: SKUs with ≥X orders/30d & stable source & margin band →
  nominate pre-order/group-buy campaign.
- Drafts campaign params: batch window, target qty, discounted price with
  batch shipping economics.
- HITL: campaign launch always human-approved (never auto in M3).
- Emit approval.requested(kind=preorder_launch).

M3: live.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.approval import request_approval
from relay.core.events import STREAM_ANALYTICS

log = structlog.get_logger(__name__)


class PromotionEngineAgent(BaseAgent):
    """A2 — nominates SKUs for pre-order (Middle tier) campaigns."""

    name = "promotion_engine"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        if event.get("type") != "tick.weekly_promotion":
            return []

        return await self._promote(session)

    async def _promote(self, session: AsyncSession) -> list[dict[str, Any]]:
        """Find qualifying SKUs and nominate campaigns."""
        emitted: list[dict[str, Any]] = []

        # Load thresholds from config
        min_orders_30d = await self._get_config_int(
            session, "promo.min_orders_30d", 5
        )
        min_margin_rate = await self._get_config_float(
            session, "promo.min_margin_rate", 0.12
        )
        oos_threshold = await self._get_config_int(
            session, "promo.max_oos_events", 1
        )

        # Find qualifying LIVE listings
        rows = await session.execute(
            text("""
                SELECT
                    l.id AS listing_id,
                    l.product_id,
                    l.sell_price_krw,
                    l.margin_krw,
                    l.margin_rate,
                    p.canonical_name_ko,
                    COALESCE(o30.cnt, 0) AS orders_30d,
                    COALESCE(oos.cnt, 0) AS oos_events_30d
                FROM listings l
                JOIN products p ON p.id = l.product_id
                LEFT JOIN (
                    SELECT listing_id, COUNT(*) AS cnt
                    FROM orders
                    WHERE created_at > now() - INTERVAL '30 days'
                      AND status NOT IN ('CANCELLED', 'HOLD_STOCKOUT', 'HOLD_PCCC')
                    GROUP BY listing_id
                ) o30 ON o30.listing_id = l.id
                LEFT JOIN (
                    SELECT ps.product_id, COUNT(*) AS cnt
                    FROM product_sources ps
                    WHERE ps.stock_state = 'OOS'
                      AND ps.last_checked_at > now() - INTERVAL '30 days'
                    GROUP BY ps.product_id
                ) oos ON oos.product_id = l.product_id
                WHERE l.status = 'LIVE'
                  AND l.marketplace = 'naver'
                  AND l.margin_rate >= :min_margin
                  AND COALESCE(o30.cnt, 0) >= :min_orders
                  AND COALESCE(oos.cnt, 0) <= :oos_threshold
                ORDER BY o30.cnt DESC
                LIMIT 10
            """),
            {
                "min_orders": min_orders_30d,
                "min_margin": min_margin_rate,
                "oos_threshold": oos_threshold,
            },
        )

        candidates = rows.fetchall()
        if not candidates:
            log.info("promotion_engine_no_candidates")
            return []

        log.info("promotion_engine_candidates", count=len(candidates))

        for (
            listing_id, product_id, sell_price_krw, margin_krw,
            margin_rate, name_ko, orders_30d, _oos_events,
        ) in candidates:
            # Draft campaign params
            campaign = self._draft_campaign(
                listing_id=listing_id,
                sell_price_krw=sell_price_krw,
                margin_krw=margin_krw,
                orders_30d=orders_30d,
            )

            # Create preorder_campaigns row
            import json
            await session.execute(
                text("""
                    INSERT INTO preorder_campaigns
                      (product_id, window_start, window_end, target_qty,
                       campaign_price_krw, batch_economics, status)
                    VALUES
                      (:pid, :wstart, :wend, :target, :price, :econ, 'PROPOSED')
                    ON CONFLICT DO NOTHING
                """),
                {
                    "pid": product_id,
                    "wstart": campaign["window_start"],
                    "wend": campaign["window_end"],
                    "target": campaign["target_qty"],
                    "price": campaign["campaign_price_krw"],
                    "econ": json.dumps(campaign["batch_economics"]),
                },
            )
            await session.commit()

            # Request HITL approval for campaign launch
            approval_id = await request_approval(
                session,
                kind="preorder_launch",
                ref_table="listings",
                ref_id=listing_id,
                summary=(
                    f"Pre-order nomination: {name_ko or 'SKU#' + str(listing_id)} "
                    f"({orders_30d} orders/30d, margin {margin_rate:.0%})"
                ),
                evidence={
                    "listing_id": listing_id,
                    "product_id": product_id,
                    "orders_30d": orders_30d,
                    "sell_price_krw": sell_price_krw,
                    "margin_rate": float(margin_rate) if margin_rate else 0,
                    "campaign": {
                        "listing_id": campaign["listing_id"],
                        "window_start": campaign["window_start"].isoformat(),
                        "window_end": campaign["window_end"].isoformat(),
                        "target_qty": campaign["target_qty"],
                        "campaign_price_krw": campaign["campaign_price_krw"],
                        "discount_pct": campaign["discount_pct"],
                    },
                },
                proposed_action={
                    "action": "launch_preorder",
                    "listing_id": listing_id,
                    "campaign_price_krw": campaign["campaign_price_krw"],
                    "target_qty": campaign["target_qty"],
                    "window_start": campaign["window_start"].isoformat(),
                    "window_end": campaign["window_end"].isoformat(),
                },
                correlation_id=f"promo:{listing_id}",
                expires_hours=72,
            )
            emitted.append({
                "stream": STREAM_ANALYTICS,
                "type": "campaign.nominated",
                "idempotency_key": f"promo:{listing_id}:nominated",
                "payload": {
                    "listing_id": listing_id,
                    "campaign": campaign,
                    "approval_id": approval_id,
                },
            })
            log.info(
                "campaign_nominated",
                listing_id=listing_id,
                approval_id=approval_id,
                target_qty=campaign["target_qty"],
                price=campaign["campaign_price_krw"],
            )

        return emitted

    def _draft_campaign(
        self,
        listing_id: int,
        sell_price_krw: int,
        margin_krw: int,
        orders_30d: int,
    ) -> dict[str, Any]:
        """Draft campaign parameters based on current performance."""
        # Batch discount: 5-10% off depending on margin room
        discount_pct = 0.05 if margin_krw > sell_price_krw * 0.15 else 0.03
        campaign_price = int(sell_price_krw * (1 - discount_pct) / 100) * 100

        # Target qty: project 2x monthly orders for batch window
        target_qty = max(10, orders_30d * 2)

        # Batch window: 7 days starting tomorrow
        window_start = date.today() + timedelta(days=1)
        window_end = window_start + timedelta(days=7)

        return {
            "listing_id": listing_id,
            "window_start": window_start,
            "window_end": window_end,
            "target_qty": target_qty,
            "campaign_price_krw": campaign_price,
            "discount_pct": discount_pct,
            "batch_economics": {
                "sell_price_krw": sell_price_krw,
                "margin_krw": margin_krw,
                "campaign_price_krw": campaign_price,
                "est_batch_shipping_savings": int(sell_price_krw * 0.02),
            },
        }

    async def _get_config_int(
        self, session: AsyncSession, key: str, default: int
    ) -> int:
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

    async def _get_config_float(
        self, session: AsyncSession, key: str, default: float
    ) -> float:
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :key"),
            {"key": key},
        )
        result = row.first()
        if result is None:
            return default
        value = result[0]
        if isinstance(value, dict):
            return float(value.get("value", default))
        if isinstance(value, (int, float)):
            return float(value)
        return default
