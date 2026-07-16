"""ClaimTriage — C2 agent.

Trigger: claim.opened (cancel/return/refund/dispute events from Naver).

Does (T2 LangGraph-style flow):
1. Gather order timeline (events, shipments, purchases).
2. Classify fault: seller / customer / carrier / customs.
3. Propose resolution per policy table:
   - Pre-shipment cancel (seller fault) → full refund, auto-cancel on Naver.
   - Post-delivery return (customer fault) → return shipping paid by customer.
   - Carrier damage → file claim with carrier + refund customer.
   - Customs hold (PCCC missing) → request PCCC or cancel.
4. Draft customer message (T1) + marketplace action plan.
5. Money-out actions → approval.requested (HITL until M4).

M2: all money-out actions require approval. Pre-shipment cancel drafts only.
M3+: pre-shipment cancellations auto after trust threshold.
"""

from __future__ import annotations

import json
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.approval import request_approval
from relay.core.config import settings
from relay.core.events import STREAM_CS, STREAM_APPROVALS
from relay.core.llm.client import client as llm

log = structlog.get_logger(__name__)

# Fault categories
_FAULT_SELLER = "seller"
_FAULT_CUSTOMER = "customer"
_FAULT_CARRIER = "carrier"
_FAULT_CUSTOMS = "customs"
_FAULT_UNKNOWN = "unknown"

# Resolution types
_RESOL_REFUND_FULL = "refund_full"
_RESOL_REFUND_PARTIAL = "refund_partial"
_RESOL_RETURN = "return"
_RESOL_RESHIP = "reship"
_RESOL_CANCEL_PRE_SHIP = "cancel_pre_ship"
_RESOL_WAIT_PCCC = "wait_pccc"
_ESCALATE = "escalate"

# Money-out resolutions that require HITL approval
_MONEY_OUT_RESOLUTIONS = {_RESOL_REFUND_FULL, _RESOL_REFUND_PARTIAL, _RESOL_RESHIP}


class ClaimTriageAgent(BaseAgent):
    """C2 — triages claims (cancel/return/refund/dispute) and proposes resolutions."""

    name = "claim_triage"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        if event_type == "claim.opened":
            return await self._triage_claim(payload, session)

        return []

    async def _triage_claim(
        self,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Run the triage flow: gather → classify → resolve → draft → escalate."""
        claim_id = payload.get("claim_id")
        order_id = payload.get("order_id")
        claim_type = payload.get("claim_type", "")  # cancel, return, refund, dispute
        reason = payload.get("reason", "")

        if not order_id:
            log.warning("claim_triage_no_order", claim_id=claim_id)
            return []

        # 1. Gather order timeline
        timeline = await self._gather_timeline(order_id, session)

        # 2. Classify fault (T2 LLM or rule-based)
        fault = await self._classify_fault(claim_type, reason, timeline, session)

        # 3. Propose resolution per policy table
        resolution = self._propose_resolution(claim_type, fault, timeline)

        # 4. Draft customer message
        message = await self._draft_message(
            claim_type, fault, resolution, timeline, session
        )

        # 5. Save claim record (reason stored inside resolution JSONB — no dedicated column)
        await session.execute(
            text("""
                INSERT INTO claims
                  (order_id, kind, fault, resolution, draft_message, status)
                VALUES
                  (:oid, :ctype, :fault, :resolution, :msg, 'OPEN')
                ON CONFLICT DO NOTHING
            """),
            {
                "oid": order_id,
                "ctype": claim_type,
                "fault": fault,
                "resolution": json.dumps({
                    "type": resolution,
                    "claim_type": claim_type,
                    "fault": fault,
                    "reason": reason[:500],
                }),
                "msg": message,
            },
        )
        await session.commit()

        # 6. Route: money-out → HITL; otherwise emit claim.triaged
        emitted: list[dict[str, Any]] = []

        if resolution in _MONEY_OUT_RESOLUTIONS:
            approval_id = await request_approval(
                session,
                kind="claim_refund",
                ref_table="orders",
                ref_id=order_id,
                summary=f"Claim #{claim_id}: {resolution} ({fault} fault) — {reason[:100]}",
                evidence={
                    "claim_id": claim_id,
                    "order_id": order_id,
                    "claim_type": claim_type,
                    "fault": fault,
                    "resolution": resolution,
                    "timeline": timeline,
                },
                proposed_action={
                    "action": resolution,
                    "order_id": order_id,
                    "customer_message": message,
                },
                correlation_id=f"claim:{claim_id}",
                expires_hours=48,
            )
            log.info(
                "claim_triage_money_out",
                claim_id=claim_id,
                order_id=order_id,
                resolution=resolution,
                approval_id=approval_id,
            )
        else:
            # Non-money-out: emit triaged event for operator awareness
            emitted.append({
                "stream": STREAM_CS,
                "type": "claim.triaged",
                "idempotency_key": f"claim:{claim_id}:triaged",
                "payload": {
                    "claim_id": claim_id,
                    "order_id": order_id,
                    "fault": fault,
                    "resolution": resolution,
                    "draft_message": message,
                },
            })
            log.info(
                "claim_triage_done",
                claim_id=claim_id,
                order_id=order_id,
                fault=fault,
                resolution=resolution,
            )

        return emitted

    async def _gather_timeline(
        self,
        order_id: int,
        session: AsyncSession,
    ) -> dict[str, Any]:
        """Pull order timeline facts for fault classification."""
        timeline: dict[str, Any] = {"order_id": order_id}

        # Order basics
        row = await session.execute(
            text("""
                SELECT o.status, o.qty, o.unit_sell_krw, o.created_at,
                       o.pccc, l.title
                FROM orders o
                LEFT JOIN listings l ON l.id = o.listing_id
                WHERE o.id = :oid
            """),
            {"oid": order_id},
        )
        rec = row.first()
        if rec:
            timeline["order_status"] = rec[0]
            timeline["qty"] = rec[1]
            timeline["unit_sell_krw"] = rec[2]
            timeline["order_created_at"] = str(rec[3])
            timeline["pccc"] = rec[4]
            timeline["listing_title"] = rec[5]

        # Order events (FSM history)
        rows = await session.execute(
            text("""
                SELECT from_status, to_status, actor, at
                FROM order_events
                WHERE order_id = :oid
                ORDER BY at
            """),
            {"oid": order_id},
        )
        timeline["events"] = [
            {"from": r[0], "to": r[1], "actor": r[2], "at": str(r[3])}
            for r in rows.fetchall()
        ]

        # Shipment info
        row = await session.execute(
            text("""
                SELECT s.stage, s.kr_tracking, s.kr_carrier, s.forwarder_ref,
                       s.last_movement_at, s.stalled
                FROM shipments s
                WHERE s.order_id = :oid
                ORDER BY s.created_at DESC
                LIMIT 1
            """),
            {"oid": order_id},
        )
        rec = row.first()
        if rec:
            timeline["ship_stage"] = rec[0]
            timeline["kr_tracking"] = rec[1]
            timeline["kr_carrier"] = rec[2]
            timeline["forwarder_ref"] = rec[3]
            timeline["last_movement_at"] = str(rec[4]) if rec[4] else None
            timeline["stalled"] = rec[5]

        # Purchase info
        row = await session.execute(
            text("""
                SELECT p.paid_minor, p.currency, p.status, p.src_order_id
                FROM purchases p
                WHERE p.order_id = :oid
                ORDER BY p.created_at DESC
                LIMIT 1
            """),
            {"oid": order_id},
        )
        rec = row.first()
        if rec:
            timeline["purchase_paid_minor"] = rec[0]
            timeline["purchase_currency"] = rec[1]
            timeline["purchase_status"] = rec[2]
            timeline["src_order_id"] = rec[3]

        return timeline

    async def _classify_fault(
        self,
        claim_type: str,
        reason: str,
        timeline: dict[str, Any],
        session: AsyncSession,
    ) -> str:
        """Classify who is at fault. T2 LLM or rule-based fallback."""
        if not settings.llm_configured or settings.relay_dry_run:
            return self._rule_classify_fault(claim_type, reason, timeline)

        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 네이버 스마트스토어 클레임 분류기입니다. "
                    "클레임 내용과 주문 타임라인을 분석하여 책임자를 판단하세요.\n"
                    "카테고리:\n"
                    "- seller: 판매자 과실 (상품 불량, 잘못된 상품, 미발송)\n"
                    "- customer: 고객 과실 (단순 변심, 주문 실수)\n"
                    "- carrier: 택배사 과실 (파손, 분실, 지연)\n"
                    "- customs: 통관 문제 (PCCC 누락, 관세 미납)\n"
                    "- unknown: 판단 불가\n"
                    "JSON 형식으로만 응답: {\"fault\": \"...\", \"reasoning\": \"...\"}"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"클레임 유형: {claim_type}\n"
                    f"클레임 사유: {reason}\n"
                    f"주문 상태: {timeline.get('order_status')}\n"
                    f"배송 단계: {timeline.get('ship_stage')}\n"
                    f"PCCC: {timeline.get('pccc', '없음')}\n"
                    f"주문 이벤트: {timeline.get('events', [])}"
                ),
            },
        ]

        try:
            resp = await llm.complete(
                task_name="c2.claim_triage",
                messages=messages,
                session=session,
                agent=self.name,
            )
            raw = resp.content
            if isinstance(raw, dict) and "_dry_run" in raw:
                return self._rule_classify_fault(claim_type, reason, timeline)
            fault = raw.get("fault", _FAULT_UNKNOWN)
            if fault in {
                _FAULT_SELLER, _FAULT_CUSTOMER,
                _FAULT_CARRIER, _FAULT_CUSTOMS, _FAULT_UNKNOWN,
            }:
                return fault
            return _FAULT_UNKNOWN
        except Exception as e:
            log.warning("claim_triage_classify_error", error=str(e))
            return self._rule_classify_fault(claim_type, reason, timeline)

    def _rule_classify_fault(
        self,
        claim_type: str,
        reason: str,
        timeline: dict[str, Any],
    ) -> str:
        """Rule-based fault classification (DRY_RUN fallback)."""
        reason_lower = reason.lower()
        order_status = timeline.get("order_status", "")

        # Customs/PCCC issues
        if any(k in reason_lower for k in ["통관", "pccc", "관세", "고유부호"]):
            return _FAULT_CUSTOMS

        # Pre-shipment cancel → seller hasn't shipped yet
        if claim_type == "cancel" and order_status in (
            "NEW", "PURCHASE_PENDING", "PURCHASED", "INBOUND_TO_FORWARDER",
        ):
            # If seller hasn't even purchased yet → seller fault (can cancel easily)
            return _FAULT_SELLER

        # Carrier damage/loss
        if any(k in reason_lower for k in ["파손", "분실", "깨졌", "damaged", "lost"]):
            return _FAULT_CARRIER

        # Wrong/defective product → seller fault
        if any(k in reason_lower for k in ["불량", "하자", "잘못", "다른", "defective", "wrong"]):
            return _FAULT_SELLER

        # Customer remorse
        if any(k in reason_lower for k in ["변심", "마음", "안 씀", "remorse", "don't want"]):
            return _FAULT_CUSTOMER

        # Refund after delivery → could be seller (defective) or customer (remorse)
        if claim_type == "refund" and order_status in ("DOMESTIC_SHIPPING", "DELIVERED"):
            if any(k in reason_lower for k in ["불량", "하자", "깨졌", "고장"]):
                return _FAULT_SELLER
            return _FAULT_CUSTOMER

        return _FAULT_UNKNOWN

    def _propose_resolution(
        self,
        claim_type: str,
        fault: str,
        timeline: dict[str, Any],
    ) -> str:
        """Propose resolution based on policy table."""
        order_status = timeline.get("order_status", "")

        # Pre-shipment cancel
        if claim_type == "cancel":
            if order_status in ("NEW", "PURCHASE_PENDING", "PURCHASED", "INBOUND_TO_FORWARDER"):
                return _RESOL_CANCEL_PRE_SHIP
            # Already shipped → need to wait or intercept
            if order_status in ("FORWARDER_RECEIVED", "INTL_SHIPPING"):
                return _ESCALATE

        # Customs hold
        if fault == _FAULT_CUSTOMS:
            return _RESOL_WAIT_PCCC

        # Return requests
        if claim_type == "return":
            if fault == _FAULT_SELLER:
                return _RESOL_RETURN  # seller pays return shipping
            if fault == _FAULT_CUSTOMER:
                return _RESOL_RETURN  # customer pays return shipping
            return _ESCALATE

        # Refund requests
        if claim_type == "refund":
            if fault == _FAULT_SELLER:
                return _RESOL_REFUND_FULL
            if fault == _FAULT_CARRIER:
                return _RESOL_REFUND_FULL
            if fault == _FAULT_CUSTOMER:
                return _ESCALATE  # partial refund → human decides
            return _ESCALATE

        # Dispute
        if claim_type == "dispute":
            return _ESCALATE

        return _ESCALATE

    async def _draft_message(
        self,
        claim_type: str,
        fault: str,
        resolution: str,
        timeline: dict[str, Any],
        session: AsyncSession,
    ) -> str:
        """Draft a customer-facing message. T1 LLM or template."""
        if not settings.llm_configured or settings.relay_dry_run:
            return self._template_message(claim_type, fault, resolution, timeline)

        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 네이버 스마트스토어 고객 응대 전문가입니다. "
                    "구매대행 상품이라는 점을 감안하여 정중하고 공감적인 메시지를 작성하세요. "
                    "JSON 형식으로 응답: {\"message\": \"...\"}"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"클레임 유형: {claim_type}\n"
                    f"책임자: {fault}\n"
                    f"해결방안: {resolution}\n"
                    f"주문 상태: {timeline.get('order_status')}\n"
                    f"배송 단계: {timeline.get('ship_stage', 'N/A')}\n"
                    f"상품명: {timeline.get('listing_title', 'N/A')}"
                ),
            },
        ]

        try:
            resp = await llm.complete(
                task_name="c2.claim_triage",
                messages=messages,
                session=session,
                agent=self.name,
            )
            raw = resp.content
            if isinstance(raw, dict) and "_dry_run" in raw:
                return self._template_message(claim_type, fault, resolution, timeline)
            return raw.get("message", self._template_message(claim_type, fault, resolution, timeline))
        except Exception as e:
            log.warning("claim_triage_draft_error", error=str(e))
            return self._template_message(claim_type, fault, resolution, timeline)

    def _template_message(
        self,
        claim_type: str,
        fault: str,
        resolution: str,
        timeline: dict[str, Any],
    ) -> str:
        """Template-based customer message (DRY_RUN fallback)."""
        title = timeline.get("listing_title", "상품")

        if resolution == _RESOL_CANCEL_PRE_SHIP:
            return (
                f"안녕하세요. 고객님의 취소 요청이 정상적으로 접수되었습니다. "
                f"'{title}' 상품은 아직 발송 전 상태로, 전액 환불 처리해 드리겠습니다. "
                f"환불은 평일 기준 1-3일 내 완료됩니다. 감사합니다."
            )

        if resolution == _RESOL_REFUND_FULL:
            if fault == _FAULT_SELLER:
                return (
                    f"안녕하세요. 고객님께서 주문하신 '{title}' 상품에 문제가 있었던 점 사과드립니다. "
                    f"전액 환불 처리해 드리겠습니다. 환불은 평일 기준 1-3일 내 완료됩니다. "
                    f"불편을 드려 죄송합니다."
                )
            if fault == _FAULT_CARRIER:
                return (
                    f"안녕하세요. 배송 중 문제가 발생한 점 사과드립니다. "
                    f"'{title}' 상품에 대해 전액 환불 처리해 드리겠습니다. "
                    f"택배사에 클레임을 진행하겠습니다."
                )

        if resolution == _RESOL_RETURN:
            if fault == _FAULT_SELLER:
                return (
                    f"안녕하세요. 반품 접수가 완료되었습니다. "
                    f"판매자 과실로 인한 반품이므로 반품 배송비는 판매자가 부담합니다. "
                    f"상품 수거 후 전액 환불 처리해 드리겠습니다."
                )
            if fault == _FAULT_CUSTOMER:
                return (
                    f"안녕하세요. 반품 접수가 완료되었습니다. "
                    f"반품 배송비는 고객님 부담입니다. "
                    f"상품 수거 후 환불 처리해 드리겠습니다."
                )

        if resolution == _RESOL_WAIT_PCCC:
            return (
                f"안녕하세요. 해외 구매대행 상품은 개인통관고유부호(PCCC)가 필요합니다. "
                f"관세청 유니패스 앱(unipass.customs.go.kr)에서 발급 후 회신해 주시면 "
                f"통관을 진행하겠습니다. 발급이 어려우신 경우 취소 가능합니다."
            )

        # Escalate / generic
        return (
            f"안녕하세요. 고객님의 문의 확인 후 답변드리겠습니다. "
            f"24시간 내에 답변 드리겠습니다."
        )
