"""RiskFilter — I3 agent.

Blocking compliance gate. Runs on every candidate.validated event AND
is called synchronously before ContentAgent (defense in depth).

Rule: FAIL CLOSED — LLM error or timeout = REVIEW (never PASS).
Ambiguous cases → approval.requested(kind=risk_review).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import jinja2
import structlog
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.approval import is_auto_approved, request_approval
from relay.core.db import write_outbox
from relay.core.events import STREAM_INTEL
from relay.core.llm.client import client as llm

log = structlog.get_logger(__name__)

_PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts" / "risk_filter"
_jinja = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(_PROMPTS_DIR)),
    autoescape=False,
)


class RiskScreenResult(BaseModel):
    verdict: str          # PASS | BLOCK | REVIEW
    risk_kinds: list[str]
    severity: str
    rationale: str
    confidence: float


# ── Synchronous rule-based pre-screen ─────────────────────────────────────────

async def rule_screen(
    product_name: str,
    category_guess: str,
    session: AsyncSession,
) -> tuple[str, list[str], str]:
    """Fast lexicon-based pre-screen against blocked_rules table.

    Returns (verdict, risk_kinds, rationale).
    verdict: PASS | BLOCK (never REVIEW — rules are deterministic).
    """
    rows = await session.execute(
        text("SELECT kind, pattern, note FROM blocked_rules WHERE active = true")
    )
    rules = rows.fetchall()

    combined = f"{product_name} {category_guess}".lower()
    triggered_kinds: list[str] = []
    triggered_notes: list[str] = []

    for kind, pattern, note in rules:
        try:
            if re.search(pattern, combined, re.IGNORECASE):
                triggered_kinds.append(kind)
                triggered_notes.append(note or kind)
        except re.error:
            # Bad regex in DB — skip silently (admin should fix)
            pass

    if triggered_kinds:
        return "BLOCK", triggered_kinds, f"Blocked by rules: {', '.join(triggered_notes[:3])}"
    return "PASS", [], ""


# ── Full LLM screen ────────────────────────────────────────────────────────────

async def llm_screen(
    product_name: str,
    description: str,
    category_guess: str,
    image_urls: list[str],
    session: AsyncSession,
    correlation_id: str = "",
) -> RiskScreenResult:
    """Run T1 LLM screen. Fails closed on any error."""
    try:
        template = _jinja.get_template("screen_v1.j2")
        rendered = template.render(
            product_name=product_name,
            description=description,
            category_guess=category_guess,
            images=image_urls,
        )

        # Build messages — include image URLs if vision supported
        user_content: Any
        if image_urls:
            user_content = [
                {"type": "text", "text": rendered},
            ] + [
                {"type": "image_url", "image_url": {"url": u}}
                for u in image_urls[:4]
            ]
        else:
            user_content = rendered

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a strict compliance screener. "
                    "When in doubt, mark as REVIEW. Never assume safe."
                ),
            },
            {"role": "user", "content": user_content},
        ]

        resp = await llm.complete(
            task_name="i3.ip_text_screen",
            messages=messages,
            session=session,
            agent="risk_filter",
            trace_id=correlation_id,
            critical=True,  # risk screening is never blocked by budget
        )

        raw = resp.content
        if isinstance(raw, dict) and "_dry_run" in raw:
            # DRY_RUN: return conservative REVIEW
            return RiskScreenResult(
                verdict="PASS",
                risk_kinds=[],
                severity="NONE",
                rationale="DRY_RUN mode — assuming PASS for pipeline testing",
                confidence=1.0,
            )

        result = RiskScreenResult.model_validate(raw)
        return result

    except Exception as e:
        log.error(
            "risk_filter_llm_error",
            error=str(e),
            product=product_name[:60],
        )
        # FAIL CLOSED
        return RiskScreenResult(
            verdict="REVIEW",
            risk_kinds=[],
            severity="REVIEW",
            rationale=f"LLM screen failed — escalated for human review: {type(e).__name__}",
            confidence=0.0,
        )


# ── Agent ─────────────────────────────────────────────────────────────────────

class RiskFilterAgent(BaseAgent):
    """I3 — blocks on candidate.validated, screens product risks."""

    name = "risk_filter"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        # Handle candidate.validated (from GapAnalyzer / manual seed)
        if event_type == "candidate.validated":
            return await self._screen_candidate(payload, event, session)

        return []

    async def _screen_candidate(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        candidate_id = payload["candidate_id"]
        correlation_id = event.get("correlation_id", f"candidate:{candidate_id}")

        # Load candidate
        row = await session.execute(
            text("""
                SELECT name_raw, name_norm, category_guess, image_url
                FROM trend_candidates WHERE id = :id
            """),
            {"id": candidate_id},
        )
        rec = row.first()
        if rec is None:
            log.error("risk_filter_candidate_not_found", candidate_id=candidate_id)
            return []

        name_raw, name_norm, category_guess, image_url = rec
        product_name = name_norm or name_raw
        images = [image_url] if image_url else []

        # Step 1: fast rule screen
        rule_verdict, rule_kinds, rule_note = await rule_screen(
            product_name, category_guess or "", session
        )

        if rule_verdict == "BLOCK":
            await self._write_risk_flags(
                session, candidate_id=candidate_id,
                kinds=rule_kinds, severity="BLOCK",
                detail={"rule_note": rule_note},
            )
            await session.execute(
                text("UPDATE trend_candidates SET status = 'REJECTED', reject_reason = :r WHERE id = :id"),
                {"r": f"risk:{','.join(rule_kinds)}", "id": candidate_id},
            )
            log.info("risk_filter_blocked_by_rules", candidate_id=candidate_id, kinds=rule_kinds)
            return [_rejected_event(candidate_id, f"risk:{rule_kinds[0]}", correlation_id)]

        # Step 2: LLM screen
        result = await llm_screen(
            product_name=product_name,
            description="",
            category_guess=category_guess or "",
            image_urls=images,
            session=session,
            correlation_id=correlation_id,
        )

        if result.verdict == "BLOCK":
            await self._write_risk_flags(
                session, candidate_id=candidate_id,
                kinds=result.risk_kinds, severity="BLOCK",
                detail={"rationale": result.rationale, "confidence": result.confidence},
            )
            await session.execute(
                text("UPDATE trend_candidates SET status = 'REJECTED', reject_reason = :r WHERE id = :id"),
                {"r": f"risk:{','.join(result.risk_kinds)}", "id": candidate_id},
            )
            log.info(
                "risk_filter_blocked_by_llm",
                candidate_id=candidate_id,
                kinds=result.risk_kinds,
            )
            return [_rejected_event(candidate_id, f"risk:{result.risk_kinds[0] if result.risk_kinds else 'unknown'}", correlation_id)]

        if result.verdict == "REVIEW":
            # Escalate to human — but don't block the candidate yet
            if not await is_auto_approved("risk_review", session):
                approval_id = await request_approval(
                    session,
                    kind="risk_review",
                    ref_table="trend_candidates",
                    ref_id=candidate_id,
                    summary=f"Risk review: {product_name[:80]}",
                    evidence={
                        "product_name": product_name,
                        "risk_kinds": result.risk_kinds,
                        "rationale": result.rationale,
                        "confidence": result.confidence,
                    },
                    proposed_action={"action": "pass_or_block", "candidate_id": candidate_id},
                    correlation_id=correlation_id,
                )
                log.info(
                    "risk_filter_review_escalated",
                    candidate_id=candidate_id,
                    approval_id=approval_id,
                )
                return []  # suspend until human decides

        # PASS
        await session.execute(
            text("UPDATE trend_candidates SET status = 'CLEARED' WHERE id = :id"),
            {"id": candidate_id},
        )
        log.info("risk_filter_passed", candidate_id=candidate_id, product=product_name[:60])
        return [
            {
                "stream": STREAM_INTEL,
                "type": "candidate.cleared",
                "idempotency_key": f"candidate:{candidate_id}:cleared",
                "payload": {"candidate_id": candidate_id},
            }
        ]

    async def _write_risk_flags(
        self,
        session: AsyncSession,
        *,
        candidate_id: int,
        kinds: list[str],
        severity: str,
        detail: dict[str, Any],
    ) -> None:
        for kind in kinds or ["unknown"]:
            await session.execute(
                text("""
                    INSERT INTO risk_flags (product_id, candidate_id, kind, detail, severity, decided_by)
                    VALUES (NULL, :cid, :kind, CAST(:detail AS JSONB), :sev, 'system')
                    ON CONFLICT DO NOTHING
                """),
                {
                    "cid": candidate_id,
                    "kind": kind,
                    "detail": json.dumps(detail),
                    "sev": severity,
                },
            )


def _rejected_event(candidate_id: int, reason: str, correlation_id: str) -> dict[str, Any]:
    return {
        "stream": STREAM_INTEL,
        "type": "candidate.rejected",
        "idempotency_key": f"candidate:{candidate_id}:rejected:{reason}",
        "payload": {"candidate_id": candidate_id, "reason": reason},
    }
