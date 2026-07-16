"""FastAPI web application — Approval Queue + Dashboard + webhooks.

Routes:
  GET  /                → dashboard (agent health, DLQ, outbox lag, metrics)
  GET  /approvals       → approval queue list
  POST /approvals/{id}/approve  → approve an item
  POST /approvals/{id}/deny     → deny an item
  GET  /health          → health check (DB + Redis)
  GET  /events          → SSE stream of real-time agent activity
  POST /webhooks/naver/orders   → Naver order webhook (future; polls for now)
"""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text

from relay.core.config import settings
from relay.core.db import AsyncSessionLocal, healthcheck as db_health
from relay.core.events import (
    ALL_STREAMS, STREAM_DLQ, _make_redis, setup_consumer_groups,
)

log = structlog.get_logger(__name__)

templates = Jinja2Templates(
    directory=str(__import__("pathlib").Path(__file__).parent / "templates")
)

_redis = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis
    _redis = _make_redis()
    await setup_consumer_groups(_redis)
    log.info("web_app_started", dry_run=settings.relay_dry_run)
    yield
    if _redis:
        await _redis.aclose()
    log.info("web_app_stopped")


app = FastAPI(title="RELAY Operations", lifespan=lifespan)


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> JSONResponse:
    db_ok = await db_health()
    redis_ok = False
    if _redis:
        try:
            await _redis.ping()
            redis_ok = True
        except Exception:
            pass
    status = "ok" if (db_ok and redis_ok) else "degraded"
    return JSONResponse(
        content={"status": status, "db": db_ok, "redis": redis_ok},
        status_code=200 if status == "ok" else 503,
    )


# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    async with AsyncSessionLocal() as session:
        # DLQ count
        dlq_count = 0
        if _redis:
            dlq_count = await _redis.xlen(STREAM_DLQ)

        # Outbox lag (unpublished events)
        lag_row = await session.execute(
            text("SELECT COUNT(*) FROM event_outbox WHERE NOT published")
        )
        outbox_lag = lag_row.scalar() or 0

        # Pending approvals
        ap_row = await session.execute(
            text("SELECT COUNT(*) FROM approval_requests WHERE status = 'PENDING'")
        )
        pending_approvals = ap_row.scalar() or 0

        # Recent LLM spend (last 24h)
        llm_row = await session.execute(
            text("""
                SELECT COALESCE(SUM(prompt_tokens + completion_tokens), 0),
                       COALESCE(SUM(cost_est), 0)
                FROM llm_calls
                WHERE at > now() - interval '24 hours'
            """)
        )
        llm_result = llm_row.first()
        llm_tokens_24h = llm_result[0] if llm_result else 0
        llm_cost_24h = float(llm_result[1]) if llm_result else 0.0

        # Live listings count
        listing_row = await session.execute(
            text("SELECT COUNT(*) FROM listings WHERE status = 'LIVE'")
        )
        live_listings = listing_row.scalar() or 0

        # Orders today
        order_row = await session.execute(
            text("""
                SELECT COUNT(*) FROM orders
                WHERE created_at > now() - interval '24 hours'
            """)
        )
        orders_today = order_row.scalar() or 0

        # Agent metrics from app_config
        metrics_row = await session.execute(
            text("SELECT key, value FROM app_config WHERE key LIKE 'metric:agent.%'")
        )
        agent_metrics = {row[0]: row[1] for row in metrics_row.fetchall()}

    context = {
        "request": request,
        "dry_run": settings.relay_dry_run,
        "dlq_count": dlq_count,
        "outbox_lag": outbox_lag,
        "pending_approvals": pending_approvals,
        "llm_tokens_24h": llm_tokens_24h,
        "llm_cost_24h": llm_cost_24h,
        "live_listings": live_listings,
        "orders_today": orders_today,
        "agent_metrics": agent_metrics,
    }
    return templates.TemplateResponse("dashboard.html", context)


# ── Approval Queue ─────────────────────────────────────────────────────────────

@app.get("/approvals", response_class=HTMLResponse)
async def approval_list(request: Request, kind: str = "") -> HTMLResponse:
    async with AsyncSessionLocal() as session:
        where_kind = "AND kind = :kind" if kind else ""
        rows = await session.execute(
            text(f"""
                SELECT id, kind, ref_table, ref_id, summary, evidence,
                       proposed_action, status, expires_at, created_at
                FROM approval_requests
                WHERE status = 'PENDING' {where_kind}
                ORDER BY created_at ASC
                LIMIT 100
            """),
            {"kind": kind} if kind else {},
        )
        items = [
            {
                "id": r[0], "kind": r[1], "ref_table": r[2], "ref_id": r[3],
                "summary": r[4],
                "evidence": r[5] if isinstance(r[5], dict) else json.loads(r[5] or "{}"),
                "proposed_action": r[6] if isinstance(r[6], dict) else json.loads(r[6] or "{}"),
                "status": r[7], "expires_at": r[8], "created_at": r[9],
            }
            for r in rows.fetchall()
        ]

        # Count by kind for nav
        kind_counts_row = await session.execute(
            text("""
                SELECT kind, COUNT(*) FROM approval_requests
                WHERE status = 'PENDING'
                GROUP BY kind ORDER BY kind
            """)
        )
        kind_counts = {r[0]: r[1] for r in kind_counts_row.fetchall()}

    return templates.TemplateResponse("approvals.html", {
        "request": request,
        "items": items,
        "kind_counts": kind_counts,
        "current_kind": kind,
        "dry_run": settings.relay_dry_run,
    })


@app.post("/approvals/{approval_id}/approve")
async def approve(
    approval_id: int,
    note: str = Form(default=""),
    decided_by: str = Form(default="operator"),
) -> JSONResponse:
    async with AsyncSessionLocal() as session:
        row = await session.execute(
            text("SELECT status, kind, ref_table, ref_id FROM approval_requests WHERE id = :id"),
            {"id": approval_id},
        )
        item = row.first()
        if not item:
            raise HTTPException(status_code=404, detail="Approval request not found")
        if item[0] != "PENDING":
            raise HTTPException(status_code=409, detail=f"Already {item[0]}")

        await session.execute(
            text("""
                UPDATE approval_requests
                SET status = 'APPROVED', decided_by = :by, decided_at = now()
                WHERE id = :id
            """),
            {"id": approval_id, "by": decided_by},
        )

        # Emit approval.granted event so waiting agents can resume
        from relay.core.db import write_outbox
        from relay.core.events import STREAM_APPROVALS
        await write_outbox(
            session,
            stream=STREAM_APPROVALS,
            event_type="approval.granted",
            idempotency_key=f"approval:{approval_id}:granted",
            payload={
                "approval_id": approval_id,
                "kind": item[1],
                "ref_table": item[2],
                "ref_id": item[3],
                "decided_by": decided_by,
                "note": note,
            },
        )
        await session.commit()

    log.info("approval_granted", id=approval_id, by=decided_by)
    return JSONResponse({"ok": True, "approval_id": approval_id, "status": "APPROVED"})


@app.post("/approvals/{approval_id}/deny")
async def deny(
    approval_id: int,
    note: str = Form(default=""),
    decided_by: str = Form(default="operator"),
) -> JSONResponse:
    async with AsyncSessionLocal() as session:
        row = await session.execute(
            text("SELECT status, kind, ref_table, ref_id FROM approval_requests WHERE id = :id"),
            {"id": approval_id},
        )
        item = row.first()
        if not item:
            raise HTTPException(status_code=404, detail="Approval request not found")
        if item[0] != "PENDING":
            raise HTTPException(status_code=409, detail=f"Already {item[0]}")

        await session.execute(
            text("""
                UPDATE approval_requests
                SET status = 'DENIED', decided_by = :by, decided_at = now()
                WHERE id = :id
            """),
            {"id": approval_id, "by": decided_by},
        )

        from relay.core.db import write_outbox
        from relay.core.events import STREAM_APPROVALS
        await write_outbox(
            session,
            stream=STREAM_APPROVALS,
            event_type="approval.denied",
            idempotency_key=f"approval:{approval_id}:denied",
            payload={
                "approval_id": approval_id,
                "kind": item[1],
                "ref_table": item[2],
                "ref_id": item[3],
                "decided_by": decided_by,
                "note": note,
            },
        )
        await session.commit()

    log.info("approval_denied", id=approval_id, by=decided_by)
    return JSONResponse({"ok": True, "approval_id": approval_id, "status": "DENIED"})


# ── Bulk approve (for publish_batch) ──────────────────────────────────────────

@app.post("/approvals/bulk-approve")
async def bulk_approve(
    request: Request,
    decided_by: str = Form(default="operator"),
) -> JSONResponse:
    form = await request.form()
    ids = [int(v) for k, v in form.multi_items() if k == "approval_ids"]
    if not ids:
        raise HTTPException(status_code=400, detail="No approval_ids provided")

    approved = []
    async with AsyncSessionLocal() as session:
        for approval_id in ids:
            row = await session.execute(
                text("SELECT status FROM approval_requests WHERE id = :id"),
                {"id": approval_id},
            )
            item = row.first()
            if not item or item[0] != "PENDING":
                continue
            await session.execute(
                text("""
                    UPDATE approval_requests
                    SET status = 'APPROVED', decided_by = :by, decided_at = now()
                    WHERE id = :id
                """),
                {"id": approval_id, "by": decided_by},
            )
            from relay.core.db import write_outbox
            from relay.core.events import STREAM_APPROVALS
            await write_outbox(
                session,
                stream=STREAM_APPROVALS,
                event_type="approval.granted",
                idempotency_key=f"approval:{approval_id}:granted",
                payload={"approval_id": approval_id, "decided_by": decided_by},
            )
            approved.append(approval_id)
        await session.commit()

    log.info("bulk_approval_granted", count=len(approved), by=decided_by)
    return JSONResponse({"ok": True, "approved": approved})


# ── Real-time Event Stream (SSE) ──────────────────────────────────────────────

@app.get("/events", response_class=FileResponse)
async def events_page() -> FileResponse:
    """Real-time agent activity monitor UI (static page + SSE)."""
    # Static HTML page — JS connects to /events/stream via SSE
    events_html = Path(__file__).parent / "templates" / "events_static.html"
    return FileResponse(str(events_html), media_type="text/html")


@app.get("/events/stream")
async def events_stream() -> StreamingResponse:
    """SSE endpoint: streams real-time events from Redis Streams."""

    async def event_generator():
        redis = _make_redis()
        try:
            # Send initial connected event
            yield f"event: connected\ndata: {json.dumps({'time': __import__('datetime').datetime.now().isoformat()})}\n\n"

            # Track last-seen ID per stream (start from '0' = read history + new)
            last_ids = {s: "0" for s in ALL_STREAMS}

            while True:
                try:
                    # XREAD from all streams, non-blocking after initial
                    results = await redis.xread(
                        streams={s: last_ids[s] for s in ALL_STREAMS},
                        count=10,
                        block=1000,
                    )
                    if results:
                        for stream_name, messages in results:
                            for entry_id, fields in messages:
                                last_ids[stream_name] = entry_id
                                raw = fields.get("data", "{}")
                                try:
                                    envelope = json.loads(raw)
                                    stream_short = stream_name.replace("relay:", "")
                                    yield f"event: {envelope.get('type', 'unknown')}\ndata: {json.dumps({'stream': stream_short, 'entry_id': entry_id, 'envelope': envelope}, default=str)}\n\n"
                                except json.JSONDecodeError:
                                    pass
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
                    await asyncio.sleep(2)
        finally:
            await redis.aclose()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ── Workflow Visualization ───────────────────────────────────────────────────

@app.get("/workflow", response_class=FileResponse)
async def workflow_page() -> FileResponse:
    """Agent workflow visualization — how pipelines connect end-to-end."""
    workflow_html = Path(__file__).parent / "templates" / "workflow_static.html"
    return FileResponse(str(workflow_html), media_type="text/html")


def main() -> None:
    import uvicorn
    uvicorn.run("relay.apps.web.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
