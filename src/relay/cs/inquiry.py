"""InquiryAgent — C1 agent.

Trigger: inquiry.received (from Naver product Q&A polling via tick.inquiry_poll).

Does:
1. Classify inquiry (T0): tracking / spec / pre_purchase / pccc / cancel_refund / other
2. For tracking/PCCC/spec: compose grounded answer (T1) using DB facts only
3. Confidence < threshold or category=other → escalate to Approval Queue

M2: draft-only (no auto-send). Auto-send in M3 for tracking/PCCC classes.
"""

from __future__ import annotations

from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.approval import request_approval
from relay.core.config import settings
from relay.core.events import STREAM_CS
from relay.core.llm.client import client as llm
from relay.integrations.naver.client import answer_inquiry

log = structlog.get_logger(__name__)

# Inquiry classification categories
_CLASS_TRACKING = "tracking"
_CLASS_SPEC = "spec"
_CLASS_PRE_PURCHASE = "pre_purchase"
_CLASS_PCCC = "pccc"
_CLASS_CANCEL_REFUND = "cancel_refund"
_CLASS_OTHER = "other"

# Categories that get auto-draft (high confidence only)
_AUTO_DRAFT_CLASSES = {_CLASS_TRACKING, _CLASS_SPEC, _CLASS_PCCC}

# Minimum confidence to auto-draft (below → escalate)
_CONFIDENCE_THRESHOLD = 0.7


class InquiryAgent(BaseAgent):
    """C1 — classifies marketplace inquiries and drafts answers."""

    name = "inquiry"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        if event_type == "inquiry.received":
            return await self._handle_inquiry(payload, session)

        return []

    async def _handle_inquiry(
        self,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Classify + draft or escalate."""
        inquiry_id = payload.get("inquiry_id")
        remote_inquiry_id = payload.get("remote_inquiry_id", "")
        question = payload.get("question", "")
        order_id = payload.get("order_id")
        listing_id = payload.get("listing_id")

        if not question:
            log.warning("inquiry_empty_question", inquiry_id=inquiry_id)
            return []

        # 1. Classify (T0)
        klass, confidence = await self._classify(question, session)

        # 2. Create inquiry row
        await session.execute(
            text("""
                INSERT INTO inquiries
                  (marketplace, remote_inquiry_id, order_id, listing_id,
                   question, klass, confidence, status)
                VALUES
                  ('naver', :remote_id, :oid, :lid, :q, :klass, :conf, 'OPEN')
                ON CONFLICT (remote_inquiry_id) DO UPDATE
                  SET klass = EXCLUDED.klass,
                      confidence = EXCLUDED.confidence
                RETURNING id
            """),
            {
                "remote_id": remote_inquiry_id or f"manual:{inquiry_id}",
                "oid": order_id,
                "lid": listing_id,
                "q": question,
                "klass": klass,
                "conf": confidence,
            },
        )
        await session.commit()

        # Reload to get the inquiry row id
        row = await session.execute(
            text("SELECT id FROM inquiries WHERE remote_inquiry_id = :rid"),
            {"rid": remote_inquiry_id or f"manual:{inquiry_id}"},
        )
        db_inquiry_id = row.scalar_one()

        # 3. Route based on classification
        if klass in _AUTO_DRAFT_CLASSES and confidence >= _CONFIDENCE_THRESHOLD:
            # Auto-draft answer (T1)
            draft = await self._compose_answer(
                db_inquiry_id, klass, order_id, listing_id, question, session
            )
            if draft:
                await session.execute(
                    text("UPDATE inquiries SET draft_answer = :draft WHERE id = :id"),
                    {"draft": draft, "id": db_inquiry_id},
                )
                await session.commit()

                # M3: Auto-send for tracking/PCCC if enabled
                auto_sent = False
                if await self._is_auto_send_enabled(session, klass):
                    auto_sent = await self._auto_send(
                        remote_inquiry_id, draft, session
                    )
                    if auto_sent:
                        await session.execute(
                            text("""
                                UPDATE inquiries
                                SET sent_answer = :answer, auto_sent = true
                                WHERE id = :id
                            """),
                            {"answer": draft, "id": db_inquiry_id},
                        )
                        await session.commit()

                log.info(
                    "inquiry_draft_composed",
                    inquiry_id=db_inquiry_id,
                    klass=klass,
                    confidence=confidence,
                    auto_sent=auto_sent,
                )
                return [
                    {
                        "stream": STREAM_CS,
                        "type": "inquiry.answered",
                        "idempotency_key": f"inquiry:{db_inquiry_id}:answered",
                        "payload": {
                            "inquiry_id": db_inquiry_id,
                            "klass": klass,
                            "auto_sent": auto_sent,
                        },
                    }
                ]

        # Escalate to Approval Queue
        await self._escalate(
            db_inquiry_id, klass, confidence, question, order_id, session
        )
        return []

    async def _is_auto_send_enabled(
        self, session: AsyncSession, klass: str
    ) -> bool:
        """Check if auto-send is enabled for this inquiry class (M3 HITL graduation)."""
        # Global toggle
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": "inquiry.auto_send"},
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

        # Per-class check
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": "inquiry.auto_send_classes"},
        )
        result = row.first()
        if result is None:
            # Default: tracking + pccc only
            return klass in {"tracking", "pccc"}
        value = result[0]
        if isinstance(value, dict):
            classes = value.get("classes", ["tracking", "pccc"])
            return klass in classes
        return klass in {"tracking", "pccc"}

    async def _auto_send(
        self,
        remote_inquiry_id: str,
        answer_text: str,
        session: AsyncSession,
    ) -> bool:
        """Send answer via Naver API. Returns True on success."""
        if not remote_inquiry_id or remote_inquiry_id.startswith("manual:"):
            return False
        try:
            success = await answer_inquiry(remote_inquiry_id, answer_text)
            if success:
                log.info("inquiry_auto_sent", remote_inquiry_id=remote_inquiry_id)
            return success
        except Exception as e:
            log.error("inquiry_auto_send_failed", error=str(e))
            return False

    async def _classify(
        self,
        question: str,
        session: AsyncSession,
    ) -> tuple[str, float]:
        """Classify inquiry text. Returns (klass, confidence).

        Uses T0 LLM when available, otherwise falls back to keyword matching.
        """
        if not settings.llm_configured or settings.relay_dry_run:
            return self._keyword_classify(question)

        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 네이버 스마트스토어 문의 분류기입니다. "
                    "고객 문의를 다음 카테고리 중 하나로 분류하세요:\n"
                    "- tracking: 배송 조회/현재 위치/언제 도착\n"
                    "- spec: 제품 스펙/사이즈/색상/재질 문의\n"
                    "- pre_purchase: 구매 전 일반 문의\n"
                    "- pccc: 개인통관고유부호 관련\n"
                    "- cancel_refund: 취소/반품/환불 의사\n"
                    "- other: 해당 없음\n"
                    "JSON 형식으로만 응답: {\"klass\": \"...\", \"confidence\": 0.0-1.0}"
                ),
            },
            {"role": "user", "content": question},
        ]

        try:
            resp = await llm.complete(
                task_name="c1.inquiry_classify",
                messages=messages,
                session=session,
                agent=self.name,
            )
            raw = resp.content
            if isinstance(raw, dict) and "_dry_run" in raw:
                return self._keyword_classify(question)
            klass = raw.get("klass", _CLASS_OTHER)
            confidence = float(raw.get("confidence", 0.0))
            if klass not in {
                _CLASS_TRACKING, _CLASS_SPEC, _CLASS_PRE_PURCHASE,
                _CLASS_PCCC, _CLASS_CANCEL_REFUND, _CLASS_OTHER,
            }:
                klass = _CLASS_OTHER
            return klass, confidence
        except Exception as e:
            log.warning("inquiry_classify_error", error=str(e))
            return self._keyword_classify(question)

    def _keyword_classify(self, question: str) -> tuple[str, float]:
        """Fallback keyword-based classification (no LLM needed)."""
        q = question.lower()

        # Tracking keywords
        if any(k in q for k in ["배송", "언제", "도착", "어디", "운송장", "tracking", "deliver"]):
            return _CLASS_TRACKING, 0.8

        # PCCC keywords
        if any(k in q for k in ["통관", "pccc", "개인통관", "관세", "고유부호"]):
            return _CLASS_PCCC, 0.8

        # Cancel/refund keywords
        if any(k in q for k in ["취소", "반품", "환불", "cancel", "refund", "return"]):
            return _CLASS_CANCEL_REFUND, 0.6  # Lower confidence → escalate

        # Spec keywords
        if any(k in q for k in ["사이즈", "크기", "색상", "재질", "스펙", "spec", "무게"]):
            return _CLASS_SPEC, 0.7

        # Pre-purchase keywords
        if any(k in q for k in ["구매", "있나", "가격", "할인", "재고", "order", "buy"]):
            return _CLASS_PRE_PURCHASE, 0.5  # Lower confidence → escalate

        return _CLASS_OTHER, 0.3

    async def _compose_answer(
        self,
        inquiry_id: int,
        klass: str,
        order_id: int | None,
        listing_id: int | None,
        question: str,
        session: AsyncSession,
    ) -> str | None:
        """Compose a grounded answer using DB facts. T1 LLM or template."""
        # Gather facts from DB
        facts = await self._gather_facts(order_id, listing_id, session)

        if not settings.llm_configured or settings.relay_dry_run:
            return self._template_answer(klass, facts)

        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 네이버 스마트스토어 고객 응대 전문가입니다. "
                    "구매대행 상품인 점을 감안하여 정중하게 답변하세요. "
                    "사실에 기반한 답변만 작성하고, 확인되지 않은 정보는 말하지 마세요. "
                    "JSON 형식으로 응답: {\"answer\": \"...\"}"
                ),
            },
            {
                "role": "user",
                "content": self._build_answer_prompt(klass, facts, question),
            },
        ]

        try:
            resp = await llm.complete(
                task_name="c1.inquiry_reply",
                messages=messages,
                session=session,
                agent=self.name,
            )
            raw = resp.content
            if isinstance(raw, dict) and "_dry_run" in raw:
                return self._template_answer(klass, facts)
            return raw.get("answer", self._template_answer(klass, facts))
        except Exception as e:
            log.warning("inquiry_answer_error", error=str(e))
            return self._template_answer(klass, facts)

    async def _gather_facts(
        self,
        order_id: int | None,
        listing_id: int | None,
        session: AsyncSession,
    ) -> dict[str, Any]:
        """Pull relevant facts from DB for grounding."""
        facts: dict[str, Any] = {}

        if order_id:
            row = await session.execute(
                text("""
                    SELECT o.status, o.qty, o.unit_sell_krw,
                           s.kr_tracking, s.kr_carrier, s.stage AS ship_stage
                    FROM orders o
                    LEFT JOIN shipments s ON s.order_id = o.id
                    WHERE o.id = :oid
                    ORDER BY s.created_at DESC NULLS LAST
                    LIMIT 1
                """),
                {"oid": order_id},
            )
            rec = row.first()
            if rec:
                facts["order_status"] = rec[0]
                facts["order_qty"] = rec[1]
                facts["order_price_krw"] = rec[2]
                facts["tracking_no"] = rec[3]
                facts["carrier"] = rec[4]
                facts["ship_stage"] = rec[5]

        if listing_id:
            row = await session.execute(
                text("""
                    SELECT l.title, l.sell_price_krw, p.attributes
                    FROM listings l
                    JOIN products p ON p.id = l.product_id
                    WHERE l.id = :lid
                    LIMIT 1
                """),
                {"lid": listing_id},
            )
            rec = row.first()
            if rec:
                facts["listing_title"] = rec[0]
                facts["listing_price_krw"] = rec[1]
                facts["attributes"] = rec[2]

        return facts

    def _build_answer_prompt(
        self,
        klass: str,
        facts: dict[str, Any],
        question: str,
    ) -> str:
        """Build context for T1 answer generation."""
        parts = [f"고객 문의: {question}", f"분류: {klass}"]

        if facts.get("tracking_no"):
            parts.append(
                f"운송장번호: {facts['tracking_no']} "
                f"(택배사: {facts.get('carrier', '미지정')})"
            )
        if facts.get("ship_stage"):
            parts.append(f"배송 단계: {facts['ship_stage']}")
        if facts.get("order_status"):
            parts.append(f"주문 상태: {facts['order_status']}")
        if facts.get("listing_title"):
            parts.append(f"상품명: {facts['listing_title']}")

        return "\n".join(parts)

    def _template_answer(self, klass: str, facts: dict[str, Any]) -> str:
        """Template-based answer (DRY_RUN fallback)."""
        if klass == _CLASS_TRACKING:
            tracking = facts.get("tracking_no", "")
            carrier = facts.get("carrier", "CJ대한통운")
            if tracking:
                return (
                    f"안녕하세요. 고객님의 상품은 현재 배송 중입니다. "
                    f"운송장번호: {tracking} ({carrier})입니다. "
                    f"해외 구매대행 상품으로 통관 절차를 거치고 있어 "
                    f"평소보다 배송이 다소 지연될 수 있습니다. "
                    f"추가 문의는 채팅 문의 부탁드립니다."
                )
            return (
                "안녕하세요. 고객님의 주문은 현재 준비 중입니다. "
                "해외 구매대행 상품으로, 일본 현지 구매 후 배송이 시작됩니다. "
                "평일 기준 3-5일 소요될 예정입니다."
            )

        if klass == _CLASS_PCCC:
            return (
                "안녕하세요. 해외 구매대행 상품은 개인통관고유부호(PCCC)가 필요합니다. "
                "관세청 유니패스 앱(unipass.customs.go.kr)에서 발급 가능합니다. "
                "발급 후 고유부호를 회신해 주시면 통관을 진행하겠습니다."
            )

        if klass == _CLASS_SPEC:
            title = facts.get("listing_title", "")
            return (
                f"안녕하세요. '{title}' 상품에 대한 안내드립니다. "
                f"정확한 스펙은 상세페이지에 기재되어 있으며, "
                f"추가로 궁금하신 사항은 상세 스펙을 참고해 주세요."
            )

        return (
            "안녕하세요. 고객님의 문의 확인 후 답변드리겠습니다. "
            "24시간 내에 답변 드리겠습니다."
        )

    async def _escalate(
        self,
        inquiry_id: int,
        klass: str,
        confidence: float,
        question: str,
        order_id: int | None,
        session: AsyncSession,
    ) -> None:
        """Escalate to Approval Queue for operator review."""
        await request_approval(
            session,
            kind="cs_draft",
            ref_table="inquiries",
            ref_id=inquiry_id,
            summary=f"문의 분류: {klass} (신뢰도: {confidence:.0%}) — 확인 필요",
            evidence={
                "inquiry_id": inquiry_id,
                "klass": klass,
                "confidence": confidence,
                "question": question[:200],
                "order_id": order_id,
            },
            proposed_action={
                "action": "review_inquiry",
                "message_template": "고객 문의 검토 후 답변을 전송하세요.",
            },
            correlation_id=f"inquiry:{inquiry_id}",
            expires_hours=48,
        )
        await session.commit()
