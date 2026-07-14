"""SourceMatcher — L1 agent.

Two trigger paths:
  1. tick.longtail_expand: scheduled category sweeps of Rakuten/AmazonJP bestsellers.
     This is the BASE path — feeds the longtail net from pre-seeded category list.
  2. candidate.cleared: trend path from Intelligence (RiskFilter passed).

For each product found:
  - Dedup by (marketplace, url) via product_sources UNIQUE constraint.
  - LLM T0 normalizes raw product name → canonical attributes.
  - Creates products + product_sources rows.
  - Emits product.sourced.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jinja2
import structlog
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.db import write_outbox
from relay.core.events import STREAM_INTEL, STREAM_LISTING
from relay.core.llm.client import client as llm
from relay.integrations.rakuten.client import RakutenItem, get_ranking, search

log = structlog.get_logger(__name__)

_PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts" / "source_matcher"
_jinja = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(_PROMPTS_DIR)),
    autoescape=False,
)

# Seeded longtail category list: (rakuten_genre_id, label)
# Certification-free zones as per 06_ROADMAP.md M1 scope:
# kitchen gadgets, stationery/desk, hobby/collectible accessories, camping small goods
_LONGTAIL_CATEGORIES: list[tuple[str, str]] = [
    ("100804",  "kitchen_gadgets"),       # Rakuten: キッチン用品
    ("101240",  "stationery"),            # Rakuten: 文房具・オフィス用品
    ("101213",  "hobby_accessories"),     # Rakuten: ホビー
    ("100628",  "camping_small_goods"),   # Rakuten: アウトドア
]

_MAX_ITEMS_PER_CATEGORY = 30
_MIN_PRICE_JPY = 300
_MAX_PRICE_JPY = 30_000


class NormalizedProduct(BaseModel):
    canonical_name_src: str
    brand: str = ""
    weight_g: int = 0
    material: str = ""
    color_options: list[str] = []
    size_options: list[str] = []
    category_internal: str = "other"


class SourceMatcherAgent(BaseAgent):
    """L1 — sources products from Rakuten/AmazonJP for the listing pipeline."""

    name = "source_matcher"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        if event_type == "tick.longtail_expand":
            return await self._longtail_sweep(payload, event, session)

        if event_type == "candidate.cleared":
            return await self._match_candidate(payload, event, session)

        return []

    # ── Longtail path ──────────────────────────────────────────────────────────

    async def _longtail_sweep(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Daily sweep: fetch bestsellers from each seeded category."""
        emitted: list[dict[str, Any]] = []
        correlation_id = event.get("correlation_id", "longtail")

        for genre_id, label in _LONGTAIL_CATEGORIES:
            log.info("source_matcher_category_sweep", genre_id=genre_id, label=label)
            try:
                items = await get_ranking(genre_id, hits=_MAX_ITEMS_PER_CATEGORY)
            except Exception as e:
                log.error("rakuten_ranking_error", genre_id=genre_id, error=str(e))
                continue

            for item in items:
                if not (_MIN_PRICE_JPY <= item.price_jpy <= _MAX_PRICE_JPY):
                    continue
                events = await self._upsert_from_rakuten(
                    item, label, session, correlation_id
                )
                emitted.extend(events)

        log.info("source_matcher_longtail_done", sourced=len(emitted))
        return emitted

    # ── Trend path ─────────────────────────────────────────────────────────────

    async def _match_candidate(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Find purchasable source for a trend candidate."""
        candidate_id = payload["candidate_id"]
        correlation_id = event.get("correlation_id", f"candidate:{candidate_id}")

        row = await session.execute(
            text("SELECT name_norm, name_raw, category_guess FROM trend_candidates WHERE id = :id"),
            {"id": candidate_id},
        )
        rec = row.first()
        if rec is None:
            return []

        name_norm, name_raw, category_guess = rec
        search_kw = name_norm or name_raw

        try:
            items = await search(search_kw, hits=10)
        except Exception as e:
            log.error("source_matcher_search_error", kw=search_kw, error=str(e))
            return [_unsourceable_event(candidate_id, correlation_id)]

        if not items:
            log.info("source_matcher_no_results", kw=search_kw)
            return [_unsourceable_event(candidate_id, correlation_id)]

        emitted: list[dict[str, Any]] = []
        for item in items[:3]:  # top 3 matches
            events = await self._upsert_from_rakuten(
                item, category_guess or "other", session, correlation_id,
                candidate_id=candidate_id,
            )
            emitted.extend(events)

        if not emitted:
            return [_unsourceable_event(candidate_id, correlation_id)]
        return emitted

    # ── Core upsert logic ──────────────────────────────────────────────────────

    async def _upsert_from_rakuten(
        self,
        item: RakutenItem,
        category_label: str,
        session: AsyncSession,
        correlation_id: str,
        *,
        candidate_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Normalize + upsert product and source, emit product.sourced if new."""

        # Normalize via LLM T0
        norm = await self._normalize(item, session)

        # Upsert product (by source URL as natural key — products can have multi-source)
        # First check if product_source already exists
        existing = await session.execute(
            text("""
                SELECT ps.id, ps.product_id
                FROM product_sources ps
                WHERE ps.marketplace = 'rakuten' AND ps.url = :url
            """),
            {"url": item.url},
        )
        existing_row = existing.first()

        if existing_row:
            source_id, product_id = existing_row
            # Update price + stock in case it changed
            await session.execute(
                text("""
                    UPDATE product_sources
                    SET price_minor = :price, stock_state = :state,
                        last_checked_at = now()
                    WHERE id = :id
                """),
                {"price": item.price_jpy, "state": item.stock_state, "id": source_id},
            )
            return []  # already sourced; StockMonitor handles reprice

        # Create product
        product_row = await session.execute(
            text("""
                INSERT INTO products
                  (candidate_id, origin_route, canonical_name_ko, canonical_name_src,
                   brand, category_internal, attributes, images, risk_status, status)
                VALUES
                  (:cid, :route, '', :name_src, :brand, :cat,
                   CAST(:attrs AS JSONB), CAST(:images AS JSONB), 'PENDING', 'ACTIVE')
                RETURNING id
            """),
            {
                "cid": candidate_id,
                "route": "trend" if candidate_id else "longtail",
                "name_src": norm.canonical_name_src or item.name[:200],
                "brand": norm.brand,
                "cat": norm.category_internal or category_label,
                "attrs": json.dumps({
                    "weight_g": norm.weight_g,
                    "material": norm.material,
                    "color_options": norm.color_options,
                    "size_options": norm.size_options,
                }),
                "images": json.dumps([
                    {"url": u, "role": "source", "checked": False}
                    for u in item.image_urls[:8]
                ]),
            },
        )
        product_id = product_row.scalar_one()

        # Create product_source
        source_row = await session.execute(
            text("""
                INSERT INTO product_sources
                  (product_id, marketplace, url, seller_name, seller_rating,
                   currency, price_minor, stock_state, weight_g,
                   variant_map, rank, last_checked_at)
                VALUES
                  (:pid, 'rakuten', :url, :seller, :rating,
                   'JPY', :price, :state, :weight,
                   CAST(:variants AS JSONB), 1, now())
                ON CONFLICT (product_id, url) DO UPDATE
                SET price_minor = EXCLUDED.price_minor,
                    stock_state = EXCLUDED.stock_state,
                    last_checked_at = now()
                RETURNING id
            """),
            {
                "pid": product_id,
                "url": item.url,
                "seller": item.seller_name[:100],
                "rating": item.seller_rating,
                "price": item.price_jpy,
                "state": item.stock_state,
                "weight": norm.weight_g,
                "variants": json.dumps({}),
            },
        )
        source_id = source_row.scalar_one()

        log.info(
            "source_matched",
            product_id=product_id,
            source_id=source_id,
            name=norm.canonical_name_src[:60],
            price_jpy=item.price_jpy,
        )

        return [
            {
                "stream": STREAM_LISTING,
                "type": "product.sourced",
                "idempotency_key": f"product:{product_id}:sourced",
                "payload": {
                    "product_id": product_id,
                    "candidate_id": candidate_id,
                    "source_count": 1,
                    "correlation_id": correlation_id,
                },
            }
        ]

    async def _normalize(self, item: RakutenItem, session: AsyncSession) -> NormalizedProduct:
        """T0 LLM normalization of raw product name → canonical attributes."""
        try:
            template = _jinja.get_template("normalize_v1.j2")
            rendered = template.render(
                source="rakuten",
                name=item.name,
                description=item.description,
                category_guess=item.genre_id,
            )
            resp = await llm.complete(
                task_name="l1.variant_normalize",
                messages=[{"role": "user", "content": rendered}],
                session=session,
                agent=self.name,
            )
            raw = resp.content
            if isinstance(raw, dict) and "_dry_run" not in raw:
                return NormalizedProduct.model_validate(raw)
        except Exception as e:
            log.debug("source_matcher_normalize_error", error=str(e))

        # Fallback: minimal normalization without LLM
        return NormalizedProduct(
            canonical_name_src=item.name[:200],
            brand="",
            category_internal="other",
        )


def _unsourceable_event(candidate_id: int, correlation_id: str) -> dict[str, Any]:
    return {
        "stream": STREAM_INTEL,
        "type": "candidate.rejected",
        "idempotency_key": f"candidate:{candidate_id}:rejected:unsourceable",
        "payload": {"candidate_id": candidate_id, "reason": "unsourceable"},
    }
