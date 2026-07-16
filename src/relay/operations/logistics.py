"""LogisticsTracker — O3 agent (v1).

v1 scope (M2):
- Drives FSM: PURCHASED → INBOUND_TO_FORWARDER → FORWARDER_RECEIVED → INTL_SHIPPING
  → CUSTOMS → DOMESTIC_SHIPPING → DELIVERED → SETTLED.
- Registers domestic tracking to Naver (발송처리 API).
- Stall detection: if no movement for > threshold hours → shipment.delayed.
- Proactive delay draft: emits cs_draft approval for operator to notify buyer
  before the customer complains.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.approval import request_approval
from relay.core.config import settings
from relay.core.events import STREAM_OPS, STREAM_CS
from relay.core.llm.client import client as llm
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
            await self._dispatch_to_naver(order_id, tracking, session)  # returns bool, logs events

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

                # Proactive delay draft → Approval Queue (M2: cs_draft)
                draft = await self._draft_delay_message(
                    session, order_id, stage, round(age_h, 1)
                )
                await request_approval(
                    session,
                    kind="cs_draft",
                    ref_table="shipments",
                    ref_id=shipment_id,
                    summary=(
                        f"Order #{order_id}: shipment stalled at {stage} "
                        f"for {round(age_h, 0):.0f}h — send delay notice?"
                    ),
                    evidence={
                        "order_id": order_id,
                        "shipment_id": shipment_id,
                        "stage": stage,
                        "stalled_hours": round(age_h, 1),
                        "threshold_hours": threshold_h,
                    },
                    proposed_action={
                        "action": "send_delay_notice",
                        "message_template": draft,
                        "channel": "naver_talk",
                    },
                    correlation_id=f"shipment:{shipment_id}:stall",
                    expires_hours=24,
                )
                # Mark that we've requested a draft (avoid duplicate requests)
                await session.execute(
                    text("UPDATE shipments SET events = events || :entry WHERE id = :id"),
                    {
                        "entry": json.dumps([{
                            "action": "delay_draft_requested",
                            "at": datetime.now(timezone.utc).isoformat(),
                        }]),
                        "id": shipment_id,
                    },
                )
            elif not is_stalled and already_stalled:
                # Unstall
                await session.execute(
                    text("UPDATE shipments SET stalled = false WHERE id = :id"),
                    {"id": shipment_id},
                )

        return emitted

    # ── Proactive delay drafting ──────────────────────────────────────────────

    async def _draft_delay_message(
        self,
        session: AsyncSession,
        order_id: int,
        stage: str,
        stalled_hours: float,
    ) -> str:
        """Draft a proactive delay notification message for the buyer."""
        # Gather shipment/order facts
        row = await session.execute(
            text("""
                SELECT l.title, s.kr_tracking, s.kr_carrier, s.last_movement_at
                FROM shipments s
                JOIN orders o ON o.id = s.order_id
                LEFT JOIN listings l ON l.id = o.listing_id
                WHERE s.order_id = :oid
                ORDER BY s.id DESC
                LIMIT 1
            """),
            {"oid": order_id},
        )
        rec = row.first()
        title = rec[0] if rec else "상품"
        kr_tracking = rec[1] if rec else ""

        if not settings.llm_configured or settings.relay_dry_run:
            return self._template_delay_message(stage, title, kr_tracking, stalled_hours)

        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 네이버 스마트스토어 고객 응대 전문가입니다. "
                    "배송 지연에 대한 사과 메시지를 정중하게 작성하세요. "
                    "구매대행 상품이므로 통관/국제배송으로 인한 지연임을 안내하세요. "
                    "JSON 형식으로 응답: {\"message\": \"...\"}"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"상품명: {title}\n"
                    f"배송 단계: {stage}\n"
                    f"지연 시간: {stalled_hours:.0f}시간\n"
                    f"운송장번호: {kr_tracking or '미등록'}\n"
                    f"위 정보를 바탕으로 고객에게 보낼 배송 지연 안내 메시지를 작성하세요."
                ),
            },
        ]

        try:
            resp = await llm.complete(
                task_name="c1.inquiry_reply",  # Reuse T1 reply tier
                messages=messages,
                session=session,
                agent=self.name,
            )
            raw = resp.content
            if isinstance(raw, dict) and "_dry_run" in raw:
                return self._template_delay_message(stage, title, kr_tracking, stalled_hours)
            return raw.get("message", self._template_delay_message(stage, title, kr_tracking, stalled_hours))
        except Exception as e:
            log.warning("delay_draft_error", error=str(e))
            return self._template_delay_message(stage, title, kr_tracking, stalled_hours)

    def _template_delay_message(
        self,
        stage: str,
        title: str,
        kr_tracking: str,
        stalled_hours: float,
    ) -> str:
        """Template-based delay message (DRY_RUN fallback)."""
        stage_labels = {
            "INBOUND_TO_FORWARDER": "배송 준비",
            "FORWARDER_RECEIVED": "해외 발송",
            "INTL_SHIPPING": "국제 배송",
            "CUSTOMS": "통관",
            "DOMESTIC_SHIPPING": "국내 배송",
        }
        stage_label = stage_labels.get(stage, stage)

        tracking_part = f" 운송장번호: {kr_tracking}" if kr_tracking else ""
        return (
            f"안녕하세요. 고객님께서 주문하신 '{title}' 상품의 배송이 지연되고 있어 안내드립니다. "
            f"현재 {stage_label} 단계에서 평소보다 시간이 더 소요되고 있습니다."
            f"{tracking_part} "
            f"해외 구매대행 상품으로 통관 및 국제 배송 과정에서 예상치 못한 지연이 발생할 수 있습니다. "
            f"최대한 빠르게 배송될 수 있도록 최선을 다하겠습니다. 감사합니다."
        )

    # ── Naver 발송처리 ────────────────────────────────────────────────────────

    async def _dispatch_to_naver(
        self,
        order_id: int,
        tracking: dict[str, Any],
        session: AsyncSession,
    ) -> bool:
        """Call Naver dispatch API to register domestic tracking number.

        Idempotent: checks if already dispatched to avoid duplicate API calls.
        Logs result to shipments.events JSONB.
        """
        row = await session.execute(
            text("SELECT remote_order_id, remote_order_item_id FROM orders WHERE id = :id"),
            {"id": order_id},
        )
        rec = row.first()
        if rec is None:
            return False

        remote_order_id, remote_item_id = rec
        kr_tracking = tracking.get("kr_tracking", "")
        kr_carrier = tracking.get("kr_carrier", "CJGLS")  # default CJ대한통운 (Naver string code)

        # Idempotency: check if already dispatched
        ship_row = await session.execute(
            text("SELECT events FROM shipments WHERE order_id = :oid"),
            {"oid": order_id},
        )
        ship_rec = ship_row.first()
        if ship_rec:
            events = ship_rec[0] if isinstance(ship_rec[0], list) else json.loads(ship_rec[0] or "[]")
            already_dispatched = any(
                e.get("action") == "naver_dispatch" for e in events
            )
            if already_dispatched:
                log.info("naver_dispatch_skip_already_done", order_id=order_id)
                return True

        success = await dispatch_order(
            order_id=remote_order_id,
            product_order_id=remote_item_id,
            carrier_code=kr_carrier,
            tracking_number=kr_tracking,
        )

        # Append result to shipment events
        event_entry = {
            "action": "naver_dispatch",
            "success": success,
            "tracking": kr_tracking,
            "carrier": kr_carrier,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        await session.execute(
            text("""
                UPDATE shipments
                SET events = events || CAST(:event AS JSONB)
                WHERE order_id = :oid
            """),
            {"event": json.dumps(event_entry), "oid": order_id},
        )

        if success:
            log.info(
                "naver_dispatched",
                order_id=order_id,
                tracking=kr_tracking,
            )
        return success


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")
