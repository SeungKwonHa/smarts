"""PublishAgent — L4 agent.

Creates listings via Naver Commerce API (M1).
HITL by default: batches to Approval Queue while publish_auto=false.
EXPORT mode: generates bulk-upload CSV when API not yet approved.

Daily publish rate limit is enforced via app_config key 'publish_rate_daily'.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.approval import is_auto_approved, request_approval
from relay.core.config import settings
from relay.core.events import STREAM_LISTING
from relay.integrations.naver.client import NaverProduct, create_product, generate_export_csv

log = structlog.get_logger(__name__)

_APPROVAL_KIND = "publish_batch"


class PublishAgent(BaseAgent):
    """L4 — publishes CONTENT_READY listings to Naver SmartStore."""

    name = "publisher"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        if event_type == "listing.content_ready":
            return await self._enqueue_for_publish(payload, event, session)

        if event_type == "approval.granted" and payload.get("kind") == _APPROVAL_KIND:
            return await self._publish_batch(payload, event, session)

        return []

    async def _enqueue_for_publish(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        listing_id = payload["listing_id"]
        correlation_id = event.get("correlation_id", f"listing:{listing_id}")

        # Check daily rate limit
        if not await self._within_rate_limit(session):
            log.warning("publish_rate_limit_reached", listing_id=listing_id)
            # Leave as CONTENT_READY; next day's scheduler will pick it up
            return []

        # Mark as PENDING_PUBLISH
        await session.execute(
            text("UPDATE listings SET status = 'PENDING_PUBLISH' WHERE id = :id AND status = 'CONTENT_READY'"),
            {"id": listing_id},
        )

        # Check publish mode
        publish_mode = await self._get_publish_mode(session)

        if publish_mode == "export":
            # EXPORT mode: accumulate, generate CSV on approval
            log.info("listing_queued_export_mode", listing_id=listing_id)
            return []

        # API mode: check HITL gate
        auto = await is_auto_approved(_APPROVAL_KIND, session)
        if not auto:
            approval_id = await request_approval(
                session,
                kind=_APPROVAL_KIND,
                ref_table="listings",
                ref_id=listing_id,
                summary=await self._get_listing_summary(listing_id, session),
                evidence={"listing_id": listing_id},
                proposed_action={"action": "publish", "listing_id": listing_id},
                correlation_id=correlation_id,
            )
            log.info(
                "listing_pending_approval",
                listing_id=listing_id,
                approval_id=approval_id,
            )
            return []

        # Auto-approved: publish immediately
        return await self._do_publish(listing_id, session, correlation_id)

    async def _publish_batch(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Resume after human approves the publish batch."""
        approval_id = payload.get("approval_id")
        ref_id = payload.get("ref_id")
        if ref_id is None:
            return []

        correlation_id = event.get("correlation_id", f"listing:{ref_id}")
        return await self._do_publish(int(ref_id), session, correlation_id)

    async def _do_publish(
        self,
        listing_id: int,
        session: AsyncSession,
        correlation_id: str,
    ) -> list[dict[str, Any]]:
        """Actually push the listing to Naver."""
        row = await session.execute(
            text("""
                SELECT l.title, l.sell_price_krw, l.content, p.category_naver, p.images
                FROM listings l
                JOIN products p ON p.id = l.product_id
                WHERE l.id = :id
            """),
            {"id": listing_id},
        )
        rec = row.first()
        if rec is None:
            return [_failed_event(listing_id, "publish", "listing_not_found")]

        title, sell_price, content, cat_naver, images_raw = rec
        content_dict = content if isinstance(content, dict) else json.loads(content or "{}")
        imgs_raw = images_raw if isinstance(images_raw, list) else json.loads(images_raw or "[]")
        img_urls = [
            (i.get("url") if isinstance(i, dict) else str(i))
            for i in imgs_raw if i
        ]

        product = NaverProduct(
            name=title or "상품명 없음",
            category_id=cat_naver or "50000803",
            sell_price=sell_price or 0,
            images=content_dict.get("images", img_urls)[:8],
            detail_html=content_dict.get("detail_html", ""),
        )

        try:
            result = await create_product(product)
        except Exception as e:
            log.error("naver_publish_failed", listing_id=listing_id, error=str(e))
            return [_failed_event(listing_id, "publish", str(e)[:100])]

        remote_product_id = str(
            result.get("smartstoreChannelProduct", {}).get("channelProductNo", "")
            or result.get("channelProductNo", "")
        )
        remote_url = (
            f"https://smartstore.naver.com/{settings.naver_seller_id}/products/{remote_product_id}"
            if remote_product_id and not result.get("_dry_run")
            else ""
        )

        await session.execute(
            text("""
                UPDATE listings
                SET status = 'LIVE',
                    remote_product_id = :rpid,
                    remote_url = :rurl
                WHERE id = :id
            """),
            {"rpid": remote_product_id, "rurl": remote_url, "id": listing_id},
        )

        log.info(
            "listing_published",
            listing_id=listing_id,
            remote_product_id=remote_product_id,
        )

        return [
            {
                "stream": STREAM_LISTING,
                "type": "listing.published",
                "idempotency_key": f"listing:{listing_id}:published",
                "payload": {
                    "listing_id": listing_id,
                    "remote_product_id": remote_product_id,
                    "remote_url": remote_url,
                },
            }
        ]

    async def _within_rate_limit(self, session: AsyncSession) -> bool:
        """Check if we're within today's publish rate limit."""
        today = date.today()
        row = await session.execute(
            text("""
                SELECT COUNT(*) FROM listings
                WHERE status IN ('LIVE', 'PENDING_PUBLISH')
                AND DATE(created_at) = :today
                AND marketplace = 'naver'
            """),
            {"today": today},
        )
        count = row.scalar() or 0
        limit = settings.publish_rate_daily
        return count < limit

    async def _get_publish_mode(self, session: AsyncSession) -> str:
        """Read publish mode from app_config: 'api' (default) or 'export'."""
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = 'publish.mode'")
        )
        result = row.first()
        if result and isinstance(result[0], dict):
            return result[0].get("mode", "api")
        return "api"

    async def _get_listing_summary(self, listing_id: int, session: AsyncSession) -> str:
        row = await session.execute(
            text("SELECT title, sell_price_krw, margin_krw FROM listings WHERE id = :id"),
            {"id": listing_id},
        )
        rec = row.first()
        if rec:
            return f"{rec[0] or '(no title)'} | ₩{rec[1]:,} sell | ₩{rec[2]:,} margin"
        return f"Listing #{listing_id}"


def _failed_event(listing_id: int, stage: str, reason: str) -> dict[str, Any]:
    return {
        "stream": STREAM_LISTING,
        "type": "listing.failed",
        "idempotency_key": f"listing:{listing_id}:failed:{stage}",
        "payload": {"listing_id": listing_id, "stage": stage, "reason": reason},
    }
