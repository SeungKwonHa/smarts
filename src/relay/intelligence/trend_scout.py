"""TrendScout — I1 agent.

Trigger: tick.trend_scan (every 6h, jittered).

Does:
- Crawls trend sources: Rakuten ranking deltas, Amazon JP bestsellers.
- Extracts product signals + raw ranking metadata (rank, reviewCount, reviewAverage).
- Computes multi-dimensional acceleration score:
    score = 0.45 * position_score    (exponential decay by rank)
          + 0.30 * velocity_score    (log-normalized review count)
          + 0.25 * quality_score     (reviewAverage / 5.0)
  With cross-genre bonus and acceleration bonus for upward rank movement.
- Writes trend_candidates (dedup by name_norm + image_phash).
- Emits candidate.discovered for candidates above score_threshold.

M3: live with real scoring.
"""

from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.config import settings
from relay.core.events import STREAM_INTEL
from relay.intelligence.entity_extract import extract_entities, extract_entities_batch

log = structlog.get_logger(__name__)

# ── Scoring weights ───────────────────────────────────────────────────────────
_W_POSITION = 0.45   # rank position (exponential decay)
_W_VELOCITY = 0.30   # review count (log scale)
_W_QUALITY  = 0.25   # review average (linear)

# Position decay: rank 1 ≈ 1.0, rank 10 ≈ 0.5, rank 30 ≈ 0.12
_POSITION_DECAY = 0.30

# Log-normalize review counts: ln(max) / ln(max) = 1.0 at ceiling
_REVIEW_COUNT_CEILING = 5_000

# Cross-genre bonus: each additional genre adds this much (max +0.15)
_CROSS_GENRE_BONUS = 0.05

# Acceleration bonus: rank improved by N positions
_ACCEL_BONUS_PER_RANK = 0.01
_ACCEL_BONUS_MAX = 0.10

# Trend scan source categories (Rakuten genre IDs for high-potential categories)
_DEFAULT_GENRE_IDS = [
    "558944",   # キッチン・日用品 (kitchen/daily goods)
    "558885",   # 文房具・事務用品 (stationery)
    "215780",   # ホビー (hobby/collectibles)
    "564500",   # アウトドア (camping/outdoor)
]

# Blacklist keywords that look like trending products but aren't (IP-sensitive)
_NAME_BLACKLIST = re.compile(
    r"disney|pokemon|포켓몬|sanrio|hello kitty|라인프렌즈|bt21|kakao|starbucks",
    re.IGNORECASE,
)


# ── Scoring functions ─────────────────────────────────────────────────────────

def _position_score(rank: int | None) -> float:
    """Exponential decay by ranking position. Rank 1 → ~1.0, rank 30 → ~0.12."""
    if not rank or rank < 1:
        return 0.0
    return round(math.exp(-_POSITION_DECAY * (rank - 1)), 4)


def _velocity_score(review_count: int | None) -> float:
    """Log-normalized review count. Handles 0 → 0.0, 5000+ → ~1.0."""
    if not review_count or review_count <= 0:
        return 0.0
    log_val = math.log1p(review_count)
    log_max = math.log1p(_REVIEW_COUNT_CEILING)
    return round(min(1.0, log_val / log_max), 4)


def _quality_score(review_average: float | None) -> float:
    """Linear map of review average (0-5 scale) → 0-1."""
    if not review_average or review_average <= 0:
        return 0.0
    return round(min(1.0, review_average / 5.0), 4)


def _compute_accel_score(
    *,
    rank_position: int | None,
    review_count: int | None,
    review_average: float | None,
    cross_genre_count: int,
    previous_rank: int | None,
) -> float:
    """Composite acceleration score (0-1).

    Components:
    - position_score (45%): how high it ranks in its genre
    - velocity_score (30%): how many reviews (proxy for sales velocity)
    - quality_score  (25%): how well-rated (proxy for customer satisfaction)
    - cross_genre_bonus: appears in multiple genres
    - accel_bonus: rank improved vs previous scan
    """
    base = (
        _W_POSITION * _position_score(rank_position)
        + _W_VELOCITY * _velocity_score(review_count)
        + _W_QUALITY * _quality_score(review_average)
    )

    # Cross-genre bonus: product appears in multiple genres
    if cross_genre_count > 1:
        base += min(0.15, _CROSS_GENRE_BONUS * (cross_genre_count - 1))

    # Acceleration bonus: rank improved vs previous scan
    if previous_rank and rank_position and previous_rank > rank_position:
        improvement = previous_rank - rank_position
        base += min(_ACCEL_BONUS_MAX, _ACCEL_BONUS_PER_RANK * improvement)

    return round(min(1.0, base), 4)


# ── Agent ─────────────────────────────────────────────────────────────────────

class TrendScoutAgent(BaseAgent):
    """I1 — discovers trending products from source marketplaces."""

    name = "trend_scout"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        if event.get("type") != "tick.trend_scan":
            return []

        return await self._scan(session)

    async def _scan(self, session: AsyncSession) -> list[dict[str, Any]]:
        """Run trend scan: fetch rankings → batch extract → dedup → score → emit."""
        emitted: list[dict[str, Any]] = []

        # Load thresholds from config
        score_threshold = await self._get_config_float(
            session, "trend.score_threshold", 0.3
        )
        max_candidates = await self._get_config_int(
            session, "trend.max_candidates_per_scan", 20
        )

        # Collect raw items from all sources (with ranking metadata)
        raw_items = await self._fetch_all_sources(session)

        # Batch entity extraction — single LLM call (or rule-based in DRY_RUN)
        if raw_items:
            titles = [item["name_raw"] for item in raw_items if item.get("name_raw")]
            entities = await extract_entities_batch(titles, session)
            # Attach entity back to items
            for item, entity in zip(raw_items, entities):
                item["_entity"] = entity

        # Dedup against existing candidates, store ranking metadata
        new_count = 0
        for item in raw_items:
            if new_count >= max_candidates:
                break
            added = await self._upsert_candidate(item, session)
            if added:
                new_count += 1

        # Score all DISCOVERED candidates using multi-dimensional model
        await self._score_candidates(session)

        # Emit candidate.discovered for those above threshold.
        # Use entity product_name from metadata if available, fallback to name_norm.
        rows = await session.execute(
            text("""
                SELECT id, name_norm, accel_score,
                       ranking_metadata->'entity'->>'product_name' AS extracted_name
                FROM trend_candidates
                WHERE status = 'DISCOVERED'
                  AND accel_score >= :threshold
                ORDER BY accel_score DESC
                LIMIT :limit
            """),
            {"threshold": score_threshold, "limit": max_candidates},
        )
        for candidate_id, name_norm, score, extracted_name in rows.fetchall():
            display_name = extracted_name or name_norm
            await session.execute(
                text("UPDATE trend_candidates SET status = 'SCORED' WHERE id = :id"),
                {"id": candidate_id},
            )
            emitted.append({
                "stream": STREAM_INTEL,
                "type": "candidate.discovered",
                "idempotency_key": f"candidate:{candidate_id}:discovered",
                "payload": {
                    "candidate_id": candidate_id,
                    "name": display_name,
                    "accel_score": float(score) if score else 0.0,
                },
            })
            log.info(
                "candidate_discovered",
                candidate_id=candidate_id,
                name=display_name[:60],
                score=float(score) if score else 0.0,
            )

        log.info(
            "trend_scan_complete",
            raw_items=len(raw_items),
            new_candidates=new_count,
            emitted=len(emitted),
        )
        return emitted

    async def _fetch_all_sources(
        self, session: AsyncSession
    ) -> list[dict[str, Any]]:
        """Fetch trending items from all configured sources."""
        items: list[dict[str, Any]] = []

        if not settings.relay_dry_run:
            # Source 1: Rakuten ranking (multiple genres)
            try:
                from relay.integrations.rakuten.client import get_ranking
                for genre_id in _DEFAULT_GENRE_IDS:
                    try:
                        ranking = await get_ranking(genre_id=genre_id, hits=30)
                        for position, r in enumerate(ranking, start=1):
                            # Extract raw fields from the API response
                            raw = r.raw.get("Item", r.raw)
                            items.append({
                                "source": "rakuten_ranking",
                                "external_key": r.item_code,
                                "name_raw": r.name,
                                "image_url": r.image_urls[0] if r.image_urls else "",
                                "price_jpy": r.price_jpy,
                                "category_guess": r.genre_id,
                                "url": r.url,
                                # Ranking metadata
                                "rank": raw.get("rank") or position,
                                "review_count": raw.get("reviewCount"),
                                "review_average": raw.get("reviewAverage"),
                                "genre_id": raw.get("genreId", genre_id),
                            })
                    except Exception as e:
                        log.warning("trend_scout_rakuten_error", genre_id=genre_id, error=str(e))
            except ImportError:
                pass

            # Source 2: Amazon JP (placeholder — needs PA-API or Movers&Shakers scraper)
            # TODO (M4): Implement amazon_jp.get_movers_shakers() for trend discovery
            log.info("trend_scout_amazon_placeholder")
        else:
            log.info("trend_scout_dry_run_no_fetch")

        return items

    async def _upsert_candidate(
        self,
        item: dict[str, Any],
        session: AsyncSession,
    ) -> bool:
        """Insert a trend candidate if not already present. Returns True if new.

        Stores raw ranking metadata (rank, reviewCount, reviewAverage) in
        ranking_metadata JSONB for scoring. On re-appearance, updates rank
        and increments scan_count.
        """
        name_raw = item.get("name_raw", "")
        source = item.get("source", "")
        external_key = item.get("external_key", "")

        if not name_raw or not external_key:
            return False

        # Skip blacklisted (IP-sensitive) names
        if _NAME_BLACKLIST.search(name_raw):
            return False

        # Get entity from batch extraction (already done in _scan), or compute now
        entity = item.get("_entity") or await extract_entities(name_raw, session)
        product_name = entity.get("product_name", "")
        brand = entity.get("brand", "")
        attributes = entity.get("attributes", [])

        if not product_name:
            product_name = name_raw[:60]  # fallback to raw (truncated)

        # Use extracted product_name for dedup (much cleaner than raw title)
        name_norm = self._normalize_name(product_name)

        # Compute image phash placeholder (URL hash for dedup)
        image_url = item.get("image_url", "")
        image_phash = hashlib.md5(image_url.encode()).hexdigest()[:16] if image_url else ""

        # Build ranking metadata to store
        ranking_metadata = {
            "rank": item.get("rank"),
            "review_count": item.get("review_count"),
            "review_average": item.get("review_average"),
            "price_jpy": item.get("price_jpy"),
            "genre_id": item.get("genre_id"),
            "entity": {
                "product_name": product_name,
                "brand": brand,
                "category": entity.get("category_guess", ""),
                "attributes": attributes,
            },
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        # Use entity category if available, fallback to item's genre-based guess
        category = entity.get("category_guess") or item.get("category_guess", "")

        # Dedup check: same (source, external_key) or same name_norm
        existing = await session.execute(
            text("""
                SELECT id, rank_position, scan_count
                FROM trend_candidates
                WHERE (source = :src AND external_key = :key)
                   OR name_norm = :nnorm
                LIMIT 1
            """),
            {"src": source, "key": external_key, "nnorm": name_norm},
        )
        existing_row = existing.first()
        if existing_row:
            # Update metadata: latest rank, cumulative scan count
            existing_id, prev_rank, prev_scan_count = existing_row
            await session.execute(
                text("""
                    UPDATE trend_candidates
                    SET last_seen_at = now(),
                        ranking_metadata = :rmeta,
                        rank_position = :rank,
                        review_count = :rc,
                        scan_count = COALESCE(scan_count, 1) + 1
                    WHERE id = :id
                """),
                {
                    "rmeta": _json(ranking_metadata),
                    "rank": item.get("rank"),
                    "rc": item.get("review_count"),
                    "id": existing_id,
                },
            )
            return False

        # Insert new candidate with ranking metadata + extracted entity
        await session.execute(
            text("""
                INSERT INTO trend_candidates
                  (source, external_key, name_raw, name_norm, image_url, image_phash,
                   category_guess, rank_position, review_count, ranking_metadata, status)
                VALUES
                  (:src, :key, :raw, :norm, :img, :phash, :cat,
                   :rank, :rc, :rmeta, 'DISCOVERED')
                ON CONFLICT DO NOTHING
            """),
            {
                "src": source,
                "key": external_key,
                "raw": name_raw[:500],
                "norm": name_norm[:500],
                "img": image_url[:500],
                "phash": image_phash,
                "cat": category,
                "rank": item.get("rank"),
                "rc": item.get("review_count"),
                "rmeta": _json(ranking_metadata),
            },
        )
        return True

    async def _score_candidates(self, session: AsyncSession) -> None:
        """Compute multi-dimensional acceleration score for DISCOVERED candidates.

        Reads rank_position, review_count, and ranking_metadata, computes
        position/velocity/quality components, adds cross-genre and acceleration
        bonuses, writes accel_score.
        """
        # Bulk score all DISCOVERED candidates using the stored metadata.
        # Cross-genre count: how many genres this name_norm appears in.
        # Acceleration: compare current rank to previous rank stored in metadata.
        await session.execute(text("""
            WITH cross_genre AS (
                SELECT name_norm,
                       COUNT(DISTINCT ranking_metadata->>'genre_id') AS genre_count
                FROM trend_candidates
                WHERE status IN ('DISCOVERED', 'SCORED')
                  AND created_at > now() - interval '7 days'
                GROUP BY name_norm
            ),
            scored AS (
                SELECT
                    tc.id,
                    tc.rank_position,
                    tc.review_count,
                    tc.ranking_metadata->>'review_average' AS review_avg_raw,
                    tg.genre_count,
                    (tc.ranking_metadata->>'prev_rank')::int AS prev_rank,
                    _compute_accel_score(
                        tc.rank_position,
                        tc.review_count,
                        CASE WHEN tc.ranking_metadata->>'review_average' IS NOT NULL
                             THEN (tc.ranking_metadata->>'review_average')::float
                             ELSE NULL END,
                        COALESCE(tg.genre_count, 1)::int,
                        (tc.ranking_metadata->>'prev_rank')::int
                    ) AS new_score
                FROM trend_candidates tc
                LEFT JOIN cross_genre tg ON tg.name_norm = tc.name_norm
                WHERE tc.status = 'DISCOVERED'
            )
            UPDATE trend_candidates
            SET accel_score = scored.new_score,
                ranking_metadata = jsonb_set(
                    trend_candidates.ranking_metadata,
                    '{prev_rank}',
                    to_jsonb(trend_candidates.rank_position)
                )
            FROM scored
            WHERE trend_candidates.id = scored.id
        """))

    def _normalize_name(self, name: str) -> str:
        """Normalize product name for dedup: lowercase, strip quantities/sizes."""
        n = name.lower().strip()
        # Remove common suffixes that vary between sellers
        n = re.sub(r'[\d]+[個点本枚セット個入りパック個]', '', n)
        n = re.sub(r'[\d]+cm|[\d]+mm', '', n)
        n = re.sub(r'[（）()【】\[\]「」]', ' ', n)
        n = re.sub(r'\s+', ' ', n).strip()
        return n

    async def _get_config_float(
        self, session: AsyncSession, key: str, default: float
    ) -> float:
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :key"),
            {"key": key},
        )
        result = row.first()
        if result is None:
            return default
        value = result[0]
        if isinstance(value, dict):
            return float(value.get("value", default))
        if isinstance(value, (int, float)):
            return float(value)
        return default

    async def _get_config_int(
        self, session: AsyncSession, key: str, default: int
    ) -> int:
        row = await session.execute(
            text("SELECT value FROM app_config WHERE key = :key"),
            {"key": key},
        )
        result = row.first()
        if result is None:
            return default
        value = result[0]
        if isinstance(value, dict):
            return int(value.get("value", default))
        if isinstance(value, (int, float)):
            return int(value)
        return default


def _json(d: dict[str, Any]) -> str:
    return _json_dump(d)


import json as _json_mod
_json_dump = _json_mod.dumps
