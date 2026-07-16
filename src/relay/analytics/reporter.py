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
from relay.core.llm.client import client as llm

log = structlog.get_logger(__name__)


class ReporterAgent(BaseAgent):
    """A3 — generates daily digest + weekly narrative from SQL aggregates."""

    name = "reporter"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")

        if event_type == "tick.daily_report":
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

        if event_type == "tick.weekly_narrative":
            return await self._weekly_narrative(session)

        return []

    async def _weekly_narrative(self, session: AsyncSession) -> list[dict[str, Any]]:
        """Generate a T1 LLM narrative over the weekly report tables.

        Numbers are computed in SQL; LLM writes prose only.
        """
        report_date = date.today() - timedelta(days=1)
        daily = await self._build_report(report_date, session)

        # Build 7-day trend summary
        row = await session.execute(text("""
            SELECT
              COUNT(*) FILTER (WHERE status NOT IN ('CANCELLED', 'HOLD_STOCKOUT', 'SETTLED')) AS open_orders,
              COUNT(*) FILTER (WHERE created_at > now() - INTERVAL '7 days') AS orders_7d,
              COALESCE(SUM(qty * unit_sell_krw) FILTER (WHERE created_at > now() - INTERVAL '7 days'), 0) AS revenue_7d,
              COUNT(*) FILTER (WHERE status = 'CANCELLED' AND created_at > now() - INTERVAL '7 days') AS cancels_7d,
              (SELECT COUNT(*) FROM listings WHERE status = 'LIVE') AS live_listings
            FROM orders
        """))
        rec = row.first()
        weekly_stats = {
            "open_orders": int(rec[0]) if rec else 0,
            "orders_7d": int(rec[1]) if rec else 0,
            "revenue_7d_krw": int(rec[2]) if rec else 0,
            "cancels_7d": int(rec[3]) if rec else 0,
            "live_listings": int(rec[4]) if rec else 0,
        }

        # Generate narrative via T1 LLM
        narrative = await self._generate_narrative(daily, weekly_stats, session)

        log.info(
            "weekly_narrative_generated",
            date=report_date.isoformat(),
            orders_7d=weekly_stats["orders_7d"],
            revenue_7d=weekly_stats["revenue_7d_krw"],
        )

        return [
            {
                "stream": STREAM_ANALYTICS,
                "type": "report.weekly_ready",
                "idempotency_key": f"report:weekly:{report_date.isoformat()}",
                "payload": {
                    "date": report_date.isoformat(),
                    "narrative": narrative,
                    "stats": weekly_stats,
                },
            }
        ]

    async def _generate_narrative(
        self,
        daily: dict[str, Any],
        weekly_stats: dict[str, Any],
        session: AsyncSession,
    ) -> str:
        """Generate T1 LLM prose narrative over report tables."""
        from relay.core.config import settings as cfg
        if not cfg.llm_configured or cfg.relay_dry_run:
            return self._template_narrative(daily, weekly_stats)

        prompt = (
            f"주간 리포트 (일자: {daily.get('date', '')})\n\n"
            f"📦 상품 현황\n"
            f"- LIVE 리스팅: {weekly_stats['live_listings']}개\n"
            f"- 신규(DRAFT): {daily.get('listings', {}).get('draft', 0)}개\n"
            f"- 일시중단: {daily.get('listings', {}).get('suspended', 0)}개\n\n"
            f"📊 주문 현황 (최근 7일)\n"
            f"- 주문 수: {weekly_stats['orders_7d']}건\n"
            f"- 매출: {weekly_stats['revenue_7d_krw']:,}원\n"
            f"- 취소: {weekly_stats['cancels_7d']}건\n"
            f"- 진행 중: {weekly_stats['open_orders']}건\n\n"
            f"💰 손익\n"
            f"- 일일 매출: {daily.get('pnl', {}).get('gross_revenue_krw', 0):,}원\n"
            f"- 일일 마진: {daily.get('pnl', {}).get('est_margin_krw', 0):,}원\n\n"
            f"⚠️ 알림\n"
            f"- 재고 지연: {daily.get('stock_sla', {}).get('stale_live_sources', 0)}건\n"
            f"- PCCC 대기: {daily.get('holds', {}).get('hold_pccc', 0)}건\n"
            f"- 승인 대기: {daily.get('approvals', {}).get('total', 0)}건\n\n"
            f"위 수치를 바탕으로 운영자에게 전달할 간결한 주간 브리핑을 한국어로 작성하세요. "
            f"수치 중심으로 3-5문장, 각 항목별 핵심만 전달하세요."
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 이커머스 운영 분석가입니다. "
                    "수치 데이터를 바탕으로 간결하고 실행 가능한 주간 브리핑을 작성하세요."
                ),
            },
            {"role": "user", "content": prompt},
        ]

        try:
            resp = await llm.complete(
                task_name="a3.weekly_narrative",
                messages=messages,
                session=session,
                agent=self.name,
            )
            raw = resp.content
            if isinstance(raw, dict) and "_dry_run" in raw:
                return self._template_narrative(daily, weekly_stats)
            if isinstance(raw, str):
                return raw
            return raw.get("narrative", self._template_narrative(daily, weekly_stats))
        except Exception as e:
            log.warning("weekly_narrative_error", error=str(e))
            return self._template_narrative(daily, weekly_stats)

    def _template_narrative(
        self, daily: dict[str, Any], weekly_stats: dict[str, Any]
    ) -> str:
        """Template-based weekly narrative (DRY_RUN fallback)."""
        orders_7d = weekly_stats["orders_7d"]
        revenue = weekly_stats["revenue_7d_krw"]
        cancels = weekly_stats["cancels_7d"]
        live = weekly_stats["live_listings"]
        stale = daily.get("stock_sla", {}).get("stale_live_sources", 0)
        pccc = daily.get("holds", {}).get("hold_pccc", 0)

        parts = [
            f"📊 주간 요약: 최근 7일간 {orders_7d}건의 주문이 있었으며, 총 매출은 {revenue:,}원입니다.",
        ]
        if cancels > 0:
            parts.append(f"취소는 {cancels}건 발생했습니다.")
        parts.append(f"현재 LIVE 리스팅은 {live}개입니다.")
        if stale > 0:
            parts.append(f"⚠️ 재고 데이터가 {stale}건 지연되고 있어 점검이 필요합니다.")
        if pccc > 0:
            parts.append(f"PCCC 대기 주문이 {pccc}건 있습니다.")

        return " ".join(parts)

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
        # Order holds (PCCC, stockout)
        holds = await self._order_hold_counts(session)
        # Open inquiries
        inquiries = await self._inquiry_counts(session)

        return {
            "date": report_date.isoformat(),
            "listings": listing,
            "orders": orders,
            "pnl": pnl,
            "dlq": dlq,
            "llm": llm_cost,
            "approvals": approval_backlog,
            "stock_sla": stale,
            "holds": holds,
            "inquiries": inquiries,
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

    async def _order_hold_counts(self, session: AsyncSession) -> dict[str, int]:
        """Count orders in HOLD states (PCCC, STOCKOUT)."""
        row = await session.execute(text("""
            SELECT
              COUNT(*) FILTER (WHERE status = 'HOLD_PCCC') AS hold_pccc,
              COUNT(*) FILTER (WHERE status = 'HOLD_STOCKOUT') AS hold_stockout
            FROM orders
        """))
        rec = row.first()
        if rec is None:
            return {"hold_pccc": 0, "hold_stockout": 0}
        return {"hold_pccc": rec[0], "hold_stockout": rec[1]}

    async def _inquiry_counts(self, session: AsyncSession) -> dict[str, int]:
        """Count open inquiries."""
        row = await session.execute(text("""
            SELECT
              COUNT(*) FILTER (WHERE status = 'OPEN') AS open,
              COUNT(*) FILTER (WHERE status = 'ESCALATED') AS escalated
            FROM inquiries
        """))
        rec = row.first()
        if rec is None:
            return {"open": 0, "escalated": 0}
        return {"open": rec[0], "escalated": rec[1]}

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
            # Holds
            hold_pccc=report.get("holds", {}).get("hold_pccc", 0),
            hold_stockout=report.get("holds", {}).get("hold_stockout", 0),
            # Inquiries
            inquiries_open=report.get("inquiries", {}).get("open", 0),
            inquiries_escalated=report.get("inquiries", {}).get("escalated", 0),
        )
