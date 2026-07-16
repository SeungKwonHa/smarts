"""GapAnalyzer — I2 agent.

Trigger: candidate.discovered (from TrendScout).

Does:
- Generate Korean search keywords from foreign product name (T0 LLM).
- Estimate Korea-side saturation: count of existing Naver listings for same keywords.
- Compute gap_score = demand_signal / (1 + supply_count).
- Write gap_score + saturation snapshot to trend_candidates.
- Emit candidate.validated (gap_score ≥ threshold) or candidate.rejected(reason=saturated).

M3: live (was stub in M1/M2).
"""

from __future__ import annotations

import re
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.config import settings
from relay.core.events import STREAM_INTEL
from relay.core.llm.client import client as llm

log = structlog.get_logger(__name__)


class GapAnalyzerAgent(BaseAgent):
    """I2 — measures Korea-side demand/supply gap for trend candidates."""

    name = "gap_analyzer"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        if event.get("type") != "candidate.discovered":
            return []

        candidate_id = event.get("payload", {}).get("candidate_id")
        if candidate_id is None:
            return []

        return await self._analyze(candidate_id, session)

    async def _analyze(
        self,
        candidate_id: int,
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Analyze gap for a single candidate."""
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
            log.warning("gap_analyzer_candidate_not_found", candidate_id=candidate_id)
            return []

        name_raw, name_norm, category_guess, image_url = rec
        product_name = name_norm or name_raw

        # 1. Generate Korean keywords (T0)
        kr_keywords = await self._generate_keywords(product_name, session)

        # 2. Estimate supply saturation (count our own + external listings)
        supply_count = await self._estimate_supply(kr_keywords, session)

        # 3. Estimate demand signal (Rakuten review count proxy)
        demand_signal = await self._estimate_demand(candidate_id, session)

        # 4. Compute gap_score
        gap_score = demand_signal / (1 + supply_count)

        # 5. Write results
        import json
        threshold = await self._get_config_float(session, "gap.score_threshold", 0.5)

        await session.execute(
            text("""
                UPDATE trend_candidates
                SET gap_score = :score,
                    kr_keywords = :kw,
                    saturation = :sat
                WHERE id = :id
            """),
            {
                "score": round(gap_score, 4),
                "kw": json.dumps({"keywords": kr_keywords}),
                "sat": json.dumps({
                    "supply_count": supply_count,
                    "demand_signal": demand_signal,
                    "threshold": threshold,
                }),
                "id": candidate_id,
            },
        )
        await session.commit()

        # 6. Emit based on threshold
        if gap_score >= threshold:
            log.info(
                "gap_analyzer_validated",
                candidate_id=candidate_id,
                gap_score=round(gap_score, 3),
                supply=supply_count,
            )
            return [
                {
                    "stream": STREAM_INTEL,
                    "type": "candidate.validated",
                    "idempotency_key": f"candidate:{candidate_id}:validated",
                    "payload": {
                        "candidate_id": candidate_id,
                        "gap_score": round(gap_score, 4),
                        "kr_keywords": kr_keywords,
                    },
                }
            ]
        else:
            log.info(
                "gap_analyzer_rejected",
                candidate_id=candidate_id,
                gap_score=round(gap_score, 3),
                reason="saturated",
            )
            await session.execute(
                text("UPDATE trend_candidates SET status = 'REJECTED', reject_reason = :r WHERE id = :id"),
                {"r": "saturated", "id": candidate_id},
            )
            return [
                {
                    "stream": STREAM_INTEL,
                    "type": "candidate.rejected",
                    "idempotency_key": f"candidate:{candidate_id}:rejected:saturated",
                    "payload": {
                        "candidate_id": candidate_id,
                        "reason": "saturated",
                        "gap_score": round(gap_score, 4),
                    },
                }
            ]

    async def _generate_keywords(
        self,
        product_name: str,
        session: AsyncSession,
    ) -> list[str]:
        """Generate Korean search keywords from foreign product name. T0 LLM."""
        if not settings.llm_configured or settings.relay_dry_run:
            return self._rule_keywords(product_name)

        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 네이버 스마트스토어 검색 키워드 생성기입니다. "
                    "외국 상품명에서 한국어 검색 키워드 3-5개를 생성하세요. "
                    "실제 고객이 검색할 만한 자연스러운 키워드여야 합니다. "
                    "JSON 형식으로만 응답: {\"keywords\": [\"...\"]}"
                ),
            },
            {"role": "user", "content": product_name},
        ]

        try:
            resp = await llm.complete(
                task_name="i2.kr_keywords",
                messages=messages,
                session=session,
                agent=self.name,
            )
            raw = resp.content
            if isinstance(raw, dict) and "_dry_run" in raw:
                return self._rule_keywords(product_name)
            keywords = raw.get("keywords", [])
            if isinstance(keywords, list) and keywords:
                return [str(k) for k in keywords[:5]]
            return self._rule_keywords(product_name)
        except Exception as e:
            log.warning("gap_analyzer_keyword_error", error=str(e))
            return self._rule_keywords(product_name)

    def _rule_keywords(self, product_name: str) -> list[str]:
        """Fallback keyword extraction: transliterate + category hints."""
        # Simple approach: extract meaningful tokens
        name = product_name.lower()
        keywords = []

        # Common JP→KR mappings for product types
        translations = {
            "キッチン": "주방",
            "収納": "수납",
            "デスク": "데스크",
            "文房具": "문구",
            "キャンプ": "캠핑",
            "アウトドアウ": "아웃도어",
            "ハンドメイド": "핸드메이드",
            "子供": "아동",
            "犬": "강아지",
            "猫": "고양이",
        }

        for jp, kr in translations.items():
            if jp in name:
                keywords.append(kr)

        # Add cleaned-up product name as keyword
        clean = re.sub(r'[^\w\s]', ' ', name)
        clean = re.sub(r'\s+', ' ', clean).strip()
        if clean:
            keywords.append(clean[:50])

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for k in keywords:
            if k not in seen:
                seen.add(k)
                unique.append(k)

        return unique[:5] or [clean[:50] if product_name else "기타"]

    async def _estimate_supply(
        self,
        keywords: list[str],
        session: AsyncSession,
    ) -> int:
        """Estimate supply saturation from our existing listings.

        M3: counts our own listings with similar keywords.
        M4: extends to Naver DataLab / external scraping.
        """
        if not keywords:
            return 0

        # Count our own LIVE listings whose title contains any of the keywords
        # (simple overlap measure)
        pattern = "|".join(re.escape(k) for k in keywords if len(k) > 1)
        if not pattern:
            return 0

        row = await session.execute(
            text("""
                SELECT COUNT(*) FROM listings
                WHERE status = 'LIVE'
                  AND title ~* :pattern
            """),
            {"pattern": pattern},
        )
        our_count = row.scalar() or 0
        return our_count

    async def _estimate_demand(
        self,
        candidate_id: int,
        session: AsyncSession,
    ) -> float:
        """Estimate demand signal.

        Proxy: use the candidate's accel_score (frequency across ranking scans)
        as a demand proxy. Higher frequency = more trending.
        """
        row = await session.execute(
            text("SELECT accel_score FROM trend_candidates WHERE id = :id"),
            {"id": candidate_id},
        )
        rec = row.first()
        if rec and rec[0]:
            return float(rec[0])
        return 0.1  # Base demand

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
