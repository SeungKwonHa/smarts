"""LogisticsTracker — O3 agent (v0).

v0 scope (M1):
- Drives FSM: PURCHASED → INBOUND_TO_FORWARDER → FORWARDER_RECEIVED → INTL_SHIPPING
  → CUSTOMS → DOMESTIC_SHIPPING → DELIVERED → SETTLED.
- Manual tracking paste (operator inputs tracking numbers via web UI or command).
- Registers domestic tracking to Naver (발송처리 API).
- Stall detection: if no movement for > threshold hours → shipment.delayed.

Full automation (Playwright forwarder portal, 17TRACK aggregator) is M2.
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
from relay.core.events import STREAM_OPS
from relay.integrations.naver.client import dispatch_order

log = structlog.get_logger(__name__)

# Stage stall thresholds (hours with no movement → alert)
_STALL_THRESHOLDS: dict[str, int] = {
    "INBOUND_TO_FORWARDER": 72,   # 3 days to reach forwarder
    "FORWARDER_RECEIVED":   48,   # 2 days for forwarder to process
    "INTL_SHIPPING":       168,   # 7 days for international leg
    "CUSTOMS":              96,   # 4 days stuck in customs → alert
    "DOMESTIC_SHIPPING":    48,   # 2 days domestic
}

# Valid FSM stage sequence
_STAGE_ORDER = [
    "PURCHASED",
    "INBOUND_TO_FORWARDER",
    "FORWARDER_RECEIVED",
    "INTL_SHIPPING",
    "CUSTOMS",
    "DOMESTIC_SHIPPING",
    "DELIVERED",
    "SETTLED",
]


class LogisticsAgent(BaseAgent):
    """O3 — tracks shipments through FSM stages, calls 발송처리 API."""

    name = "logistics"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        if event_type == "purchase.completed":
            return await self._init_shipment(payload, event, session)

        if event_type == "tick.tracking_poll":
            return await self._poll_stalls(session)

        if event_type == "shipment.updated":
            return await self._advance_stage(payload, event, session)

        return []

    # ── Shipment initialization ────────────────────────────────────────────────

    async def _init_shipment(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Create shipment record when order is purchased."""
        order_id = payload["order_id"]

        existing = await session.execute(
            text("SELECT id FROM shipments WHERE order_id = :oid"),
            {"oid": order_id},
        )
        if existing.first():
            return []

        await session.execute(
            text("""
                INSERT INTO shipments (order_id, stage, last_movement_at, stalled, events)
                VALUES (:oid, 'INBOUND_TO_FORWARDER', now(), false, CAST('[]' AS JSONB))
            """),
            {"oid": order_id},
        )

        # Transition order FSM
        await session.execute(
            text("""
                UPDATE orders SET status = 'INBOUND_TO_FORWARDER'
                WHERE id = :id AND status = 'PURCHASED'
            """),
            {"id": order_id},
        )
        await session.execute(
            text("""
                INSERT INTO order_events (order_id, from_status, to_status, actor)
                VALUES (:oid, 'PURCHASED', 'INBOUND_TO_FORWARDER', 'agent:logistics')
            """),
            {"oid": order_id},
        )

        log.info("shipment_initialized", order_id=order_id)
        return []

    # ── Stage advance (triggered by operator updating tracking UI) ────────────

    async def _advance_stage(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Move shipment to next stage. Called from web UI or webhook."""
        order_id = payload["order_id"]
        new_stage = payload.get("stage", "")
        tracking = payload.get("tracking", {})

        if new_stage not in _STAGE_ORDER:
            log.warning("logistics_invalid_stage", stage=new_stage)
            return []

        # Load current shipment
        row = await session.execute(
            text("SELECT id, stage FROM shipments WHERE order_id = :oid LIMIT 1"),
            {"oid": order_id},
        )
        rec = row.first()
        if rec is None:
            return []
        shipment_id, current_stage = rec

        # Update stage
        update_fields: dict[str, Any] = {
            "stage": new_stage,
            "last_movement_at": datetime.now(timezone.utc),
            "stalled": False,
        }
        if "kr_tracking" in tracking:
            update_fields["kr_tracking"] = tracking["kr_tracking"]
            update_fields["kr_carrier"] = tracking.get("kr_carrier", "")
        if "intl_tracking" in tracking:
            update_fields["intl_tracking"] = tracking["intl_tracking"]
        if "forwarder_ref" in tracking:
            update_fields["forwarder_ref"] = tracking["forwarder_ref"]

        await session.execute(
            text("""
                UPDATE shipments
                SET stage = :stage,
                    last_movement_at = now(),
                    stalled = false,
                    kr_tracking = COALESCE(:kr_tracking, kr_tracking),
                    kr_carrier  = COALESCE(:kr_carrier,  kr_carrier),
                    intl_tracking = COALESCE(:intl_tracking, intl_tracking),
                    forwarder_ref = COALESCE(:forwarder_ref, forwarder_ref)
                WHERE id = :id
            """),
            {
                "stage": new_stage,
                "kr_tracking": tracking.get("kr_tracking"),
                "kr_carrier": tracking.get("kr_carrier"),
                "intl_tracking": tracking.get("intl_tracking"),
                "forwarder_ref": tracking.get("forwarder_ref"),
                "id": shipment_id,
            },
        )

        # Update order FSM
        try:
            await session.execute(
                text("""
                    UPDATE orders SET status = CAST(:stage AS order_status) WHERE id = :oid
                """),
                {"stage": new_stage, "oid": order_id},
            )
            await session.execute(
                text("""
                    INSERT INTO order_events (order_id, from_status, to_status, actor, detail)
                    VALUES (:oid, :from, :to, 'agent:logistics', CAST(:detail AS JSONB))
                """),
                {
                    "oid": order_id,
                    "from": current_stage,
                    "to": new_stage,
                    "detail": json.dumps({"tracking": tracking}),
                },
            )
        except Exception as e:
            log.warning("logistics_order_update_failed", order_id=order_id, error=str(e))

        # 발송처리 (register domestic tracking to Naver) when we have domestic tracking
        emitted: list[dict[str, Any]] = []
        if new_stage == "DOMESTIC_SHIPPING" and tracking.get("kr_tracking"):
            await self._dispatch_to_naver(order_id, tracking, session)

        # Delivered
        if new_stage == "DELIVERED":
            emitted.append({
                "stream": STREAM_OPS,
                "type": "order.delivered",
                "idempotency_key": f"order:{order_id}:delivered",
                "payload": {"order_id": order_id},
            })
            log.info("order_delivered", order_id=order_id)

        emitted.append({
            "stream": STREAM_OPS,
            "type": "shipment.updated",
            "idempotency_key": f"shipment:{shipment_id}:{new_stage}",
            "payload": {
                "order_id": order_id,
                "stage": new_stage,
                "tracking": tracking,
            },
        })
        return emitted

    # ── Stall detection ────────────────────────────────────────────────────────

    async def _poll_stalls(self, session: AsyncSession) -> list[dict[str, Any]]:
        """Check for stalled shipments (no movement > threshold hours)."""
        now = datetime.now(timezone.utc)
        emitted: list[dict[str, Any]] = []

        rows = await session.execute(
            text("""
                SELECT s.id, s.order_id, s.stage, s.last_movement_at, s.stalled
                FROM shipments s
                WHERE s.stage NOT IN ('DELIVERED', 'SETTLED')
                  AND s.last_movement_at IS NOT NULL
            """)
        )

        for shipment_id, order_id, stage, last_movement, already_stalled in rows.fetchall():
            threshold_h = _STALL_THRESHOLDS.get(stage, 168)
            if last_movement is None:
                continue

            age_h = (now - last_movement.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            is_stalled = age_h > threshold_h

            if is_stalled and not already_stalled:
                # Mark stalled
                await session.execute(
                    text("UPDATE shipments SET stalled = true WHERE id = :id"),
                    {"id": shipment_id},
                )
                log.warning(
                    "shipment_stalled",
                    order_id=order_id,
                    stage=stage,
                    stalled_hours=round(age_h, 1),
                )
                emitted.append({
                    "stream": STREAM_OPS,
                    "type": "shipment.delayed",
                    "idempotency_key": f"shipment:{shipment_id}:stalled:{_today()}",
                    "payload": {
                        "order_id": order_id,
                        "stage": stage,
                        "stalled_hours": round(age_h, 1),
                    },
                })
            elif not is_stalled and already_stalled:
                # Unstall
                await session.execute(
                    text("UPDATE shipments SET stalled = false WHERE id = :id"),
                    {"id": shipment_id},
                )

        return emitted

    # ── Naver 발송처리 ────────────────────────────────────────────────────────

    async def _dispatch_to_naver(
        self,
        order_id: int,
        tracking: dict[str, Any],
        session: AsyncSession,
    ) -> None:
        """Call Naver dispatch API to register domestic tracking number."""
        row = await session.execute(
            text("SELECT remote_order_id, remote_order_item_id FROM orders WHERE id = :id"),
            {"id": order_id},
        )
        rec = row.first()
        if rec is None:
            return

        remote_order_id, remote_item_id = rec
        kr_tracking = tracking.get("kr_tracking", "")
        kr_carrier = tracking.get("kr_carrier", "04")  # default CJ대한통운

        success = await dispatch_order(
            order_id=remote_order_id,
            product_order_id=remote_item_id,
            carrier_code=kr_carrier,
            tracking_number=kr_tracking,
        )
        if success:
            log.info(
                "naver_dispatched",
                order_id=order_id,
                tracking=kr_tracking,
            )


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")
