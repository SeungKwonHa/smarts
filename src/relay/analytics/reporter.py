"""Reporter — A3 agent (v0).

Trigger: tick.daily_report (07:00 KST).

v0: SQL-computed daily digest to structlog (visible in dashboard).
Full: T1 narrative over tables + email/Telegram delivery (M2).

Reports:
- Listing funnel (sourced/priced/content/live/failed counts)
- Order funnel (new/purchased/shipped/delivered/cancelled)
- P&L snapshot (estimated revenue, margin)
- DLQ count
- LLM cost vs. budget
- Approval queue backlog
- StockMonitor SLA (stale sources)
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.events import STREAM_ANALYTICS

log = structlog.get_logger(__name__)


class ReporterAgent(BaseAgent):
    """A3 — generates daily digest from SQL aggregates."""

    name = "reporter"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        if event.get("type") != "tick.daily_report":
            return []

        report_date = date.today() - timedelta(days=1)  # yesterday's data
        report = await self._build_report(report_date, session)
        self._emit_report(report, report_date)

        return [
            {
                "stream": STREAM_ANALYTICS,
                "type": "report.daily_ready",
                "idempotency_key": f"report:daily:{report_date.isoformat()}",
                "payload": {
                    "date": report_date.isoformat(),
                    "summary_ref": f"report:daily:{report_date.isoformat()}",
                },
            }
        ]

    async def _build_report(self, report_date: date, session: AsyncSession) -> dict[str, Any]:
        """Run all report SQL queries."""
        # Listing funnel
        listing = await self._listing_funnel(session)
        # Order funnel
        orders = await self._order_funnel(report_date, session)
        # P&L snapshot
        pnl = await self._pnl_snapshot(report_date, session)
        # DLQ count
        dlq = await self._dlq_count(report_date, session)
        # LLM cost
        llm_cost = await self._llm_cost(report_date, session)
        # Approval queue backlog
        approval_backlog = await self._approval_backlog(session)
        # StockMonitor SLA
        stale = await self._stale_sources(session)

        return {
            "date": report_date.isoformat(),
            "listings": listing,
            "orders": orders,
            "pnl": pnl,
            "dlq": dlq,
            "llm": llm_cost,
            "approvals": approval_backlog,
            "stock_sla": stale,
        }

    async def _listing_funnel(self, session: AsyncSession) -> dict[str, int]:
        row = await session.execute(text("""
            SELECT
              COUNT(*) FILTER (WHERE status = 'DRAFT') AS draft,
              COUNT(*) FILTER (WHERE status = 'CONTENT_READY') AS content_ready,
              COUNT(*) FILTER (WHERE status = 'PENDING_PUBLISH') AS pending_publish,
              COUNT(*) FILTER (WHERE status = 'LIVE') AS live,
              COUNT(*) FILTER (WHERE status = 'SUSPENDED_STOCKOUT') AS suspended,
              COUNT(*) FILTER (WHERE status = 'RETIRED') AS retired,
              COUNT(*) FILTER (WHERE status = 'FAILED') AS failed
            FROM listings
            WHERE marketplace = 'naver'
        """))
        rec = row.first()
        if rec is None:
            return {}
        cols = ["draft", "content_ready", "pending_publish", "live", "suspended", "retired", "failed"]
        return dict(zip(cols, rec))

    async def _order_funnel(self, report_date: date, session: AsyncSession) -> dict[str, Any]:
        row = await session.execute(
            text("""
                SELECT
                  COUNT(*) FILTER (WHERE DATE(created_at) = :d) AS new_today,
                  COUNT(*) FILTER (WHERE status = 'PURCHASED') AS purchased,
                  COUNT(*) FILTER (WHERE status IN ('INBOUND_TO_FORWARDER','FORWARDER_RECEIVED','INTL_SHIPPING','CUSTOMS','DOMESTIC_SHIPPING')) AS in_transit,
                  COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered,
                  COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled,
                  COUNT(*) AS total_open
                FROM orders
                WHERE status NOT IN ('SETTLED')
            """),
            {"d": report_date},
        )
        rec = row.first()
        if rec is None:
            return {}
        return {
            "new_today": rec[0],
            "purchased": rec[1],
            "in_transit": rec[2],
            "delivered": rec[3],
            "cancelled": rec[4],
            "total_open": rec[5],
        }

    async def _pnl_snapshot(self, report_date: date, session: AsyncSession) -> dict[str, Any]:
        row = await session.execute(
            text("""
                SELECT
                  COUNT(*) AS order_count,
                  COALESCE(SUM(o.qty * o.unit_sell_krw), 0) AS gross_revenue_krw,
                  COALESCE(SUM(
                    (o.margin_snapshot->>'margin_krw')::NUMERIC * o.qty
                  ), 0) AS est_margin_krw
                FROM orders o
                WHERE DATE(o.created_at) = :d
                  AND o.status NOT IN ('CANCELLED', 'HOLD_STOCKOUT')
            """),
            {"d": report_date},
        )
        rec = row.first()
        if rec is None:
            return {}
        return {
            "order_count": int(rec[0]),
            "gross_revenue_krw": int(rec[1]),
            "est_margin_krw": int(rec[2]),
        }

    async def _dlq_count(self, report_date: date, session: AsyncSession) -> dict[str, int]:
        # DLQ items show in event_outbox with type containing 'dlq' OR in approval_requests
        row = await session.execute(
            text("""
                SELECT COUNT(*) FROM approval_requests
                WHERE status = 'PENDING'
                  AND kind = 'risk_review'
            """)
        )
        risk_reviews = row.scalar() or 0
        # For actual DLQ, we'd query Redis relay:dlq stream length — approximated via app_config metric
        row2 = await session.execute(
            text("SELECT value FROM app_config WHERE key = :k"),
            {"k": "metric:agent.risk_filter.dlq"},
        )
        dlq_count_raw = row2.first()
        dlq_count = 0
        if dlq_count_raw and isinstance(dlq_count_raw[0], (int, float)):
            dlq_count = int(dlq_count_raw[0])

        return {"dlq_total": dlq_count, "risk_reviews_pending": risk_reviews}

    async def _llm_cost(self, report_date: date, session: AsyncSession) -> dict[str, Any]:
        row = await session.execute(
            text("""
                SELECT
                  COUNT(*) AS call_count,
                  COALESCE(SUM(prompt_tokens + completion_tokens), 0) AS total_tokens,
                  COALESCE(SUM(cost_est), 0) AS total_cost_est,
                  COUNT(*) FILTER (WHERE cache_hit) AS cache_hits,
                  COUNT(*) FILTER (WHERE NOT ok) AS errors
                FROM llm_calls
                WHERE DATE(at) = :d
            """),
            {"d": report_date},
        )
        rec = row.first()
        if rec is None:
            return {}
        return {
            "call_count": int(rec[0]),
            "total_tokens": int(rec[1]),
            "est_cost_usd": round(float(rec[2]), 4),
            "cache_hits": int(rec[3]),
            "errors": int(rec[4]),
        }

    async def _approval_backlog(self, session: AsyncSession) -> dict[str, int]:
        row = await session.execute(
            text("""
                SELECT kind, COUNT(*) FROM approval_requests
                WHERE status = 'PENDING'
                GROUP BY kind
            """)
        )
        backlog = {row_kind: int(cnt) for row_kind, cnt in row.fetchall()}
        backlog["total"] = sum(backlog.values())
        return backlog

    async def _stale_sources(self, session: AsyncSession) -> dict[str, int]:
        from relay.core.config import settings as cfg
        row = await session.execute(
            text("""
                SELECT COUNT(*) FROM product_sources ps
                JOIN listings l ON l.product_id = ps.product_id
                WHERE l.status = 'LIVE'
                  AND (
                    ps.last_checked_at IS NULL
                    OR ps.last_checked_at < now() - INTERVAL '1 hour' * :hours
                  )
            """),
            {"hours": cfg.stock_staleness_alert_hours},
        )
        stale_count = row.scalar() or 0
        return {"stale_live_sources": int(stale_count)}

    def _emit_report(self, report: dict[str, Any], report_date: date) -> None:
        """Log report to structlog (visible in dashboard / stdout)."""
        listings = report.get("listings", {})
        orders = report.get("orders", {})
        pnl = report.get("pnl", {})

        log.info(
            "daily_report",
            date=report_date.isoformat(),
            # Listing funnel
            listings_live=listings.get("live", 0),
            listings_pending_publish=listings.get("pending_publish", 0),
            listings_content_ready=listings.get("content_ready", 0),
            listings_failed=listings.get("failed", 0),
            # Orders
            orders_new_today=orders.get("new_today", 0),
            orders_in_transit=orders.get("in_transit", 0),
            orders_delivered=orders.get("delivered", 0),
            orders_cancelled=orders.get("cancelled", 0),
            # P&L
            gross_revenue_krw=pnl.get("gross_revenue_krw", 0),
            est_margin_krw=pnl.get("est_margin_krw", 0),
            # DLQ
            dlq_total=report.get("dlq", {}).get("dlq_total", 0),
            risk_reviews_pending=report.get("dlq", {}).get("risk_reviews_pending", 0),
            # LLM
            llm_tokens=report.get("llm", {}).get("total_tokens", 0),
            llm_cost_usd=report.get("llm", {}).get("est_cost_usd", 0),
            # Approvals
            approval_backlog=report.get("approvals", {}).get("total", 0),
            # Stock SLA
            stale_sources=report.get("stock_sla", {}).get("stale_live_sources", 0),
        )
