"""OrderAgent — O2 agent.

Trigger: order.created (from Naver order poll every 5m).

Does:
1. Create orders row (FSM NEW).
2. Snapshot source price/URL.
3. Re-verify source availability (last-second check).
4. Compute real-time margin.
5. Prepare purchase instruction.
6. → Approval Queue "PAY" step (HITL in M1–M2).

Also handles:
- approval.granted(kind=purchase_pay) → emit purchase.completed (human executed purchase).
- tick.order_poll → poll Naver for new orders.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.approval import is_auto_approved, request_approval
from relay.core.config import settings
from relay.core.events import STREAM_OPS, STREAM_APPROVALS
from relay.integrations.naver.client import poll_orders
from relay.listing.pricing import compute_price

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

        return []

    # ── Order polling ──────────────────────────────────────────────────────────

    async def _poll_orders(self, session: AsyncSession) -> list[dict[str, Any]]:
        """Poll Naver for recently changed (PAYED) orders."""
        now = datetime.now(timezone.utc)
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

        # Get forwarder warehouse address
        forwarder_addr = await self._get_forwarder_address(session)

        # Request HITL PAY approval
        auto = await is_auto_approved(_APPROVAL_KIND, session)
        if not auto:
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

        # Auto-approved (M3+ only after trust thresholds met)
        return await self._complete_purchase(
            {"order_id": order_id, "approval_id": None},
            {},
            session,
        )

    async def _complete_purchase(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Human confirmed payment — mark as PURCHASED."""
        order_id = payload.get("order_id") or payload.get("ref_id")
        approval_id = payload.get("approval_id")

        if order_id is None:
            return []

        # Load order to get margin snapshot
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

        # Create purchase record
        purchase_row = await session.execute(
            text("""
                INSERT INTO purchases
                  (order_id, source_id, paid_minor, currency, status)
                VALUES (:oid, :sid, :price, :cur, 'PAID')
                RETURNING id
            """),
            {
                "oid": order_id,
                "sid": source_id,
                "price": price_minor,
                "cur": currency or "JPY",
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
        )

        return [
            {
                "stream": STREAM_OPS,
                "type": "purchase.completed",
                "idempotency_key": f"order:{order_id}:purchase:completed",
                "payload": {
                    "order_id": order_id,
                    "purchase_id": purchase_id,
                    "src_order_id": "",  # operator fills this in after placing order
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
