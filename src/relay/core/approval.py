"""HITL (Human-In-The-Loop) approval gate helper.

Agents call request_approval() to create an approval_request row and emit
approval.requested. The agent then exits; a separate handler for
approval.granted resumes the flow.

No in-memory waiting — state is persisted in Postgres.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.db import write_outbox
from relay.core.events import STREAM_APPROVALS
import structlog

log = structlog.get_logger(__name__)

DEFAULT_EXPIRES_HOURS = 48  # approval requests expire after 48h if not acted on


async def request_approval(
    session: AsyncSession,
    *,
    kind: str,
    ref_table: str,
    ref_id: int,
    summary: str,
    evidence: dict[str, Any],
    proposed_action: dict[str, Any],
    correlation_id: str,
    expires_hours: int = DEFAULT_EXPIRES_HOURS,
) -> int:
    """Create an approval_request and emit approval.requested event.

    Returns the new approval_request.id.
    """
    expires_at = datetime.now(timezone.utc) + timedelta(hours=expires_hours)

    row = await session.execute(
        text("""
            INSERT INTO approval_requests
              (kind, ref_table, ref_id, summary, evidence, proposed_action,
               status, expires_at)
            VALUES
              (:kind, :ref_table, :ref_id, :summary, CAST(:evidence AS JSONB),
               CAST(:proposed AS JSONB), 'PENDING', :expires_at)
            RETURNING id
        """),
        {
            "kind": kind,
            "ref_table": ref_table,
            "ref_id": ref_id,
            "summary": summary,
            "evidence": _jsonb(evidence),
            "proposed": _jsonb(proposed_action),
            "expires_at": expires_at,
        },
    )
    approval_id = row.scalar_one()

    await write_outbox(
        session,
        stream=STREAM_APPROVALS,
        event_type="approval.requested",
        idempotency_key=f"approval:{approval_id}:requested",
        payload={
            "approval_id": approval_id,
            "kind": kind,
            "ref_table": ref_table,
            "ref_id": ref_id,
            "summary": summary,
        },
    )

    log.info(
        "approval_requested",
        approval_id=approval_id,
        kind=kind,
        ref_id=ref_id,
    )
    return approval_id


async def is_auto_approved(kind: str, session: AsyncSession) -> bool:
    """Check app_config to see if this approval kind has been graduated to auto.

    Config key: f"hitl.auto.{kind}" → {"enabled": true}
    """
    row = await session.execute(
        text("SELECT value FROM app_config WHERE key = :key"),
        {"key": f"hitl.auto.{kind}"},
    )
    result = row.first()
    if result is None:
        return False
    value = result[0]
    if isinstance(value, dict):
        return bool(value.get("enabled", False))
    return False


def _jsonb(d: dict[str, Any]) -> str:
    import json
    return json.dumps(d, ensure_ascii=False)
