"""OrderAgent — O2 agent.

Trigger: order.created (from Naver order poll every 5m).

Does:
1. Create orders row (FSM NEW).
2. Snapshot source price/URL.
3. Re-verify source availability (last-second check).
4. Compute real-time margin.
5. Prepare purchase instruction.
6. Auto-pay (M3) via PaymentExecutor, or → Approval Queue "PAY" step (HITL in M1–M2).

Also handles:
- approval.granted(kind=purchase_pay) → emit purchase.completed (human executed purchase).
- tick.order_poll → poll Naver for new orders.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.approval import is_auto_approved, request_approval
from relay.core.config import settings
from relay.core.events import STREAM_OPS
from relay.integrations.naver.client import poll_orders
from relay.listing.pricing import compute_price
from relay.operations.payment import PaymentInstruction, execute_and_record

log = structlog.get_logger(__name__)

_APPROVAL_KIND = "purchase_pay"

# Forwarder warehouse address (operator fills this in app_config)
_FORWARDER_ADDRESS_KEY = "forwarder.warehouse_address.jp"


class OrderAgent(BaseAgent):
    """O2 — processes new orders through to HITL purchase approval."""

    name = "order_agent"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        if event_type == "tick.order_poll":
            return await self._poll_orders(session)

        if event_type == "order.created":
            return await self._process_new_order(payload, event, session)

        if event_type == "approval.granted" and payload.get("kind") == _APPROVAL_KIND:
            return await self._complete_purchase(payload, event, session)

        if event_type == "approval.denied" and payload.get("kind") == _APPROVAL_KIND:
            return await self._handle_purchase_denied(payload, session)

        if event_type == "order.pccc_received":
            return await self._resume_from_hold_pccc(payload, session)

        return []

    # ── Order polling ──────────────────────────────────────────────────────────

    async def _poll_orders(self, session: AsyncSession) -> list[dict[str, Any]]:
        """Poll Naver for recently changed (PAYED) orders."""
        now = datetime.now(UTC)
        since = (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
        until = now.strftime("%Y-%m-%dT%H:%M:%S")

        try:
            raw_orders = await poll_orders(
                last_changed_from=since,
                last_changed_to=until,
            )
        except Exception as e:
            log.error("order_poll_failed", error=str(e))
            return []

        emitted: list[dict[str, Any]] = []
        for raw in raw_orders:
            events = await self._ingest_raw_order(raw, session)
            emitted.extend(events)
        return emitted

    async def _ingest_raw_order(
        self,
        raw: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Convert a raw Naver order to our order row + emit order.created."""
        remote_order_id = str(raw.get("orderId", ""))
        # Naver groups items under one order; handle first item
        product_orders = raw.get("productOrders", [])
        if not product_orders:
            return []

        emitted = []
        for po in product_orders:
            remote_item_id = str(po.get("productOrderId", ""))
            if not remote_item_id:
                continue

            # Dedup check
            existing = await session.execute(
                text("SELECT id FROM orders WHERE marketplace='naver' AND remote_order_item_id = :rid"),
                {"rid": remote_item_id},
            )
            if existing.first():
                continue  # already ingested

            # Find our listing by remote_product_id
            remote_product_id = str(po.get("productId", ""))
            listing_row = await session.execute(
                text("SELECT id FROM listings WHERE remote_product_id = :rpid AND marketplace = 'naver'"),
                {"rpid": remote_product_id},
            )
            listing_rec = listing_row.first()
            listing_id = listing_rec[0] if listing_rec else None

            qty = int(po.get("quantity", 1))
            unit_sell_krw = int(po.get("unitPrice", 0))

            # Insert order
            order_row = await session.execute(
                text("""
                    INSERT INTO orders
                      (marketplace, remote_order_id, remote_order_item_id,
                       listing_id, qty, unit_sell_krw, status)
                    VALUES
                      ('naver', :roid, :riid, :lid, :qty, :price, 'NEW')
                    ON CONFLICT (marketplace, remote_order_item_id) DO NOTHING
                    RETURNING id
                """),
                {
                    "roid": remote_order_id,
                    "riid": remote_item_id,
                    "lid": listing_id,
                    "qty": qty,
                    "price": unit_sell_krw,
                },
            )
            order_id_row = order_row.first()
            if order_id_row is None:
                continue
            order_id = order_id_row[0]

            log.info(
                "order_ingested",
                order_id=order_id,
                remote_order_id=remote_order_id,
                qty=qty,
            )

            emitted.append({
                "stream": STREAM_OPS,
                "type": "order.created",
                "idempotency_key": f"order:{remote_item_id}:created",
                "payload": {
                    "order_id": order_id,
                    "marketplace": "naver",
                    "listing_id": listing_id,
                    "qty": qty,
                },
            })

        return emitted

    # ── Order processing ───────────────────────────────────────────────────────

    async def _process_new_order(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        order_id = payload["order_id"]
        listing_id = payload.get("listing_id")
        correlation_id = f"order:{order_id}"

        if listing_id is None:
            log.error("order_no_listing", order_id=order_id)
            return []

        # Load source
        row = await session.execute(
            text("""
                SELECT ps.id, ps.marketplace, ps.url, ps.price_minor,
                       ps.currency, ps.weight_g, ps.stock_state, ps.variant_map
                FROM listings l
                JOIN products p ON p.id = l.product_id
                JOIN product_sources ps ON ps.product_id = p.id
                WHERE l.id = :lid AND ps.stock_state = 'IN_STOCK'
                ORDER BY ps.rank LIMIT 1
            """),
            {"lid": listing_id},
        )
        src = row.first()

        if src is None:
            # Source unavailable — auto-cancel
            await self._transition_order(session, order_id, "NEW", "HOLD_STOCKOUT", "order_agent")
            log.warning("order_source_unavailable", order_id=order_id, listing_id=listing_id)
            return []

        source_id, src_marketplace, src_url, price_minor, currency, weight_g, stock_state, variant_map = src

        # Last-second stock re-verify
        if not settings.relay_dry_run and stock_state != "IN_STOCK":
            await self._transition_order(session, order_id, "NEW", "HOLD_STOCKOUT", "order_agent")
            log.warning("order_oos_at_purchase", order_id=order_id)
            return []

        # Compute real-time margin
        pricing = await compute_price(
            src_price_minor=price_minor,
            currency=currency or "JPY",
            weight_g=weight_g or 300,
            session=session,
        )

        if pricing is None:
            # Margin gone — pre-ship cancel
            await self._transition_order(session, order_id, "NEW", "HOLD_STOCKOUT", "order_agent")
            log.warning("order_margin_gone", order_id=order_id)
            return []

        # Snapshot margin at order time
        margin_snapshot = {
            **pricing,
            "src_url": src_url,
            "src_marketplace": src_marketplace,
        }
        await session.execute(
            text("""
                UPDATE orders
                SET margin_snapshot = CAST(:snap AS JSONB), status = 'PURCHASE_PENDING'
                WHERE id = :id AND status = 'NEW'
            """),
            {"snap": json.dumps(margin_snapshot), "id": order_id},
        )

        # Log FSM transition
        await self._log_transition(session, order_id, "NEW", "PURCHASE_PENDING", "order_agent")

        # Check PCCC (개인통관고유부호) — required for overseas purchase customs clearance
        pccc_row = await session.execute(
            text("SELECT pccc FROM orders WHERE id = :id"),
            {"id": order_id},
        )
        pccc_value = pccc_row.scalar()
        if not pccc_value:
            # Hold order — operator must request PCCC from buyer
            await self._transition_order(
                session, order_id, "PURCHASE_PENDING", "HOLD_PCCC", "order_agent"
            )
            approval_id = await request_approval(
                session,
                kind="cs_draft",
                ref_table="orders",
                ref_id=order_id,
                summary=f"Order #{order_id}: PCCC missing — request from buyer before purchase",
                evidence={
                    "order_id": order_id,
                    "buyer_name": None,  # populated if available
                },
                proposed_action={
                    "action": "request_pccc",
                    "message_template": "개인통관고유부호를 보내주세요. 택배 발송에 필요합니다.",
                },
                correlation_id=correlation_id,
                expires_hours=72,
            )
            log.info(
                "order_hold_pccc",
                order_id=order_id,
                approval_id=approval_id,
            )
            return []

        # Get forwarder warehouse address
        forwarder_addr = await self._get_forwarder_address(session)

        # M3: Check auto-pay eligibility (amount ≤ limit AND daily cap OK)
        auto_pay_eligible = await self._check_auto_pay(session, pricing.get("landed_krw", 0))

        # Check HITL graduation flag
        auto = await is_auto_approved(_APPROVAL_KIND, session)

        if auto_pay_eligible or auto:
            # Auto-pay: execute payment via PaymentExecutor, then complete.
            log.info(
                "order_auto_pay",
                order_id=order_id,
                landed_krw=pricing.get("landed_krw", 0),
                marketplace=src_marketplace,
            )
            instruction = PaymentInstruction(
                source_url=src_url,
                marketplace=src_marketplace,
                price_minor=price_minor,
                currency=currency or "JPY",
                variant_map=variant_map if isinstance(variant_map, dict) else {},
                forwarder_address=forwarder_addr,
                order_memo=f"Order ID: {order_id}",
            )

            # Track daily auto-pay spend BEFORE executing
            await self._track_auto_pay_spend(
                session, pricing.get("landed_krw", 0)
            )

            result = await execute_and_record(
                session=session,
                order_id=order_id,
                instruction=instruction,
            )

            if result.ok:
                return await self._complete_purchase(
                    {"order_id": order_id, "approval_id": None},
                    {},
                    session,
                    src_order_id=result.src_order_id,
                )

            # Payment failed — fall back to HITL so the operator can pay manually
            log.warning(
                "auto_pay_failed_fallback_hitl",
                order_id=order_id,
                error=result.error,
            )
            await self._transition_order(
                session, order_id, "PURCHASE_PENDING", "PURCHASE_PENDING", "payment_executor"
            )

        # Create PREPARED purchase row so the operator has a record to update
        # after manual payment. Idempotent on (order_id, source_id).
        await session.execute(
            text("""
                INSERT INTO purchases (order_id, source_id, paid_minor, currency, status)
                VALUES (:oid, :sid, :price, :cur, 'PREPARED')
                ON CONFLICT DO NOTHING
            """),
            {
                "oid": order_id,
                "sid": source_id,
                "price": price_minor,
                "cur": currency or "JPY",
            },
        )

        # HITL: Request manual purchase approval
        approval_id = await request_approval(
            session,
            kind=_APPROVAL_KIND,
            ref_table="orders",
            ref_id=order_id,
            summary=f"Order #{order_id}: purchase ¥{price_minor} from {src_marketplace}",
            evidence={
                "order_id": order_id,
                "source_url": src_url,
                "source_price_jpy": price_minor,
                "margin_krw": pricing["margin_krw"],
                "margin_rate": pricing["margin_rate"],
            },
            proposed_action={
                "action": "purchase",
                "source_url": src_url,
                "source_marketplace": src_marketplace,
                "forwarder_address": forwarder_addr,
                "variant_map": variant_map if isinstance(variant_map, dict) else {},
                "purchase_price_jpy": price_minor,
                "order_memo": f"Order ID: {order_id}",
            },
            correlation_id=correlation_id,
            expires_hours=24,
        )
        log.info(
            "order_purchase_approval_requested",
            order_id=order_id,
            approval_id=approval_id,
        )
        return [
            {
                "stream": STREAM_OPS,
                "type": "order.purchase_required",
                "idempotency_key": f"order:{order_id}:purchase_required",
                "payload": {
                    "order_id": order_id,
                    "source_id": source_id,
                    "est_cost_minor": price_minor,
                    "margin_krw": pricing["margin_krw"],
                },
            }
        ]

    async def _check_auto_pay(
        self, session: AsyncSession, landed_krw: int
    ) -> bool:
        """Check if order qualifies for M3 auto-pay.

        Conditions:
        1. landed_krw ≤ auto_pay_limit_krw (per-order limit)
        2. Today's auto-pay total + landed_krw ≤ auto_pay_daily_cap_krw
        3. hitl.auto.purchase_pay flag enabled in config
        """
        # Check global flag first
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": "hitl.auto.purchase_pay"},
        )
        result = row.first()
        if result is None:
            return False
        value = result[0]
        if isinstance(value, dict):
            if not value.get("enabled", False):
                return False
        elif not value:
            return False

        if landed_krw <= 0:
            return False

        # Per-order limit
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": "auto_pay_limit_krw"},
        )
        result = row.first()
        limit_krw = 50_000  # default
        if result:
            v = result[0]
            if isinstance(v, dict):
                limit_krw = int(v.get("value", limit_krw))
            elif isinstance(v, (int, float)):
                limit_krw = int(v)

        if landed_krw > limit_krw:
            log.info("auto_pay_over_limit", landed_krw=landed_krw, limit=limit_krw)
            return False

        # Daily cap check
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": "auto_pay_daily_cap_krw"},
        )
        result = row.first()
        daily_cap = 500_000  # default
        if result:
            v = result[0]
            if isinstance(v, dict):
                daily_cap = int(v.get("value", daily_cap))
            elif isinstance(v, (int, float)):
                daily_cap = int(v)

        # Track today's spend
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": "metric:auto_pay_spent_today"},
        )
        result = row.first()
        spent_today = 0
        if result:
            v = result[0]
            if isinstance(v, dict):
                spent_today = int(v.get("krw", 0))
                # Reset if it's a new day
                last_date = v.get("date", "")
                if last_date != datetime.now(UTC).strftime("%Y-%m-%d"):
                    spent_today = 0
            elif isinstance(v, (int, float)):
                spent_today = int(v)

        if spent_today + landed_krw > daily_cap:
            log.info(
                "auto_pay_daily_cap",
                spent_today=spent_today,
                landed_krw=landed_krw,
                daily_cap=daily_cap,
            )
            return False

        return True

    async def _track_auto_pay_spend(
        self, session: AsyncSession, krw: int
    ) -> None:
        """Add to today's auto-pay spend tracker."""
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": "metric:auto_pay_spent_today"},
        )
        result = row.first()
        current = 0
        if result:
            v = result[0]
            if isinstance(v, dict) and v.get("date") == today:
                current = int(v.get("krw", 0))

        new_total = current + krw
        await session.execute(
            text("""
                INSERT INTO app_config (key, value)
                VALUES (:k, :v)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """),
            {
                "k": "metric:auto_pay_spent_today",
                "v": json.dumps({"date": today, "krw": new_total}),
            },
        )

    async def _resume_from_hold_pccc(
        self,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Resume order from HOLD_PCCC after PCCC is received from buyer."""
        order_id = payload.get("order_id")
        if order_id is None:
            return []

        # Verify PCCC is now set
        row = await session.execute(
            text("SELECT pccc FROM orders WHERE id = :id AND status = 'HOLD_PCCC'"),
            {"id": order_id},
        )
        pccc = row.scalar()
        if not pccc:
            log.warning("pccc_resume_missing", order_id=order_id)
            return []

        await self._transition_order(
            session, order_id, "HOLD_PCCC", "PURCHASE_PENDING", "operator"
        )
        log.info("order_resumed_pccc_received", order_id=order_id)

        # Re-process the order from PURCHASE_PENDING
        return await self._process_new_order(
            {"order_id": order_id},
            {"correlation_id": f"order:{order_id}:pccc_resume"},
            session,
        )

    async def _complete_purchase(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
        src_order_id: str = "",
    ) -> list[dict[str, Any]]:
        """Mark order as PURCHASED after payment (auto or HITL-confirmed).

        src_order_id: the source marketplace's order confirmation number,
        filled by PaymentExecutor (auto) or by operator on approval.granted.
        """
        order_id = payload.get("order_id") or payload.get("ref_id")
        approval_id = payload.get("approval_id")

        if order_id is None:
            return []

        # HITL path: operator provides src_order_id in approval evidence
        if not src_order_id and approval_id:
            row = await session.execute(
                text("SELECT evidence FROM approval_requests WHERE id = :aid"),
                {"aid": approval_id},
            )
            rec = row.first()
            if rec and isinstance(rec[0], dict):
                src_order_id = rec[0].get("src_order_id", "")

        # Load source info
        row = await session.execute(
            text("""
                SELECT o.remote_order_id, l.id, ps.id AS source_id,
                       ps.price_minor, ps.currency
                FROM orders o
                JOIN listings l ON l.id = o.listing_id
                JOIN products p ON p.id = l.product_id
                JOIN product_sources ps ON ps.product_id = p.id
                WHERE o.id = :oid ORDER BY ps.rank LIMIT 1
            """),
            {"oid": order_id},
        )
        rec = row.first()
        if rec is None:
            return []

        remote_order_id, listing_id, source_id, price_minor, currency = rec

        # Auto-pay path: purchases row was already written by execute_and_record
        # (status=CHARGED). HITL path: row was PREPARED → update to PAID here.
        purchase_row = await session.execute(
            text("""
                UPDATE purchases
                SET status = 'PAID',
                    src_order_id = :src_oid,
                    paid_at = now()
                WHERE order_id = :oid
                  AND source_id = :sid
                  AND status IN ('CHARGED', 'PREPARED', 'FAILED')
                RETURNING id
            """),
            {
                "src_oid": src_order_id,
                "oid": order_id,
                "sid": source_id,
            },
        )
        purchase_rec = purchase_row.first()
        if purchase_rec is not None:
            purchase_id = purchase_rec[0]
        else:
            # Fallback: insert if somehow missing
            purchase_row = await session.execute(
                text("""
                    INSERT INTO purchases
                      (order_id, source_id, paid_minor, currency, src_order_id, status)
                    VALUES (:oid, :sid, :price, :cur, :src_oid, 'PAID')
                    RETURNING id
                """),
                {
                    "oid": order_id,
                    "sid": source_id,
                    "price": price_minor,
                    "cur": currency or "JPY",
                    "src_oid": src_order_id,
                },
            )
            purchase_id = purchase_row.scalar_one()

        await session.execute(
            text("UPDATE orders SET status = 'PURCHASED' WHERE id = :id AND status = 'PURCHASE_PENDING'"),
            {"id": order_id},
        )
        await self._log_transition(session, order_id, "PURCHASE_PENDING", "PURCHASED", "human")

        log.info(
            "purchase_completed",
            order_id=order_id,
            purchase_id=purchase_id,
            src_order_id=src_order_id or None,
        )

        return [
            {
                "stream": STREAM_OPS,
                "type": "purchase.completed",
                "idempotency_key": f"order:{order_id}:purchase:completed",
                "payload": {
                    "order_id": order_id,
                    "purchase_id": purchase_id,
                    "src_order_id": src_order_id,
                },
            }
        ]

    async def _handle_purchase_denied(
        self,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Handle denial of purchase payment approval.

        Transitions order PURCHASE_PENDING → CANCELLED and emits
        claim.opened (pre-shipment cancel) so the customer can be notified.
        """
        order_id = payload.get("order_id") or payload.get("ref_id")
        if order_id is None:
            return []

        # Verify order is still in PURCHASE_PENDING
        row = await session.execute(
            text("SELECT status FROM orders WHERE id = :id"),
            {"id": order_id},
        )
        status = row.scalar()
        if status != "PURCHASE_PENDING":
            log.info("purchase_deny_skip", order_id=order_id, status=status)
            return []

        await self._transition_order(
            session, order_id, "PURCHASE_PENDING", "CANCELLED", "operator"
        )
        log.info("purchase_denied_order_cancelled", order_id=order_id)

        return [
            {
                "stream": STREAM_OPS,
                "type": "claim.opened",
                "idempotency_key": f"order:{order_id}:purchase_denied",
                "payload": {
                    "order_id": order_id,
                    "claim_type": "cancel",
                    "reason": " Purchase payment denied by operator",
                },
            }
        ]

    # ── Helpers ────────────────────────────────────────────────────────────────

    async def _transition_order(
        self,
        session: AsyncSession,
        order_id: int,
        from_status: str,
        to_status: str,
        actor: str,
    ) -> None:
        await session.execute(
            text("UPDATE orders SET status = CAST(:to AS order_status) WHERE id = :id"),
            {"to": to_status, "id": order_id},
        )
        await self._log_transition(session, order_id, from_status, to_status, actor)

    async def _log_transition(
        self,
        session: AsyncSession,
        order_id: int,
        from_status: str,
        to_status: str,
        actor: str,
    ) -> None:
        await session.execute(
            text("""
                INSERT INTO order_events (order_id, from_status, to_status, actor)
                VALUES (:oid, :from, :to, :actor)
            """),
            {"oid": order_id, "from": from_status, "to": to_status, "actor": actor},
        )

    async def _get_forwarder_address(self, session: AsyncSession) -> str:
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": _FORWARDER_ADDRESS_KEY},
        )
        result = row.first()
        if result and isinstance(result[0], dict):
            addr = result[0].get("address", "")
            return str(addr) if addr else "FORWARDER ADDRESS NOT CONFIGURED — see app_config"
        return "FORWARDER ADDRESS NOT CONFIGURED — set app_config key: forwarder.warehouse_address.jp"
