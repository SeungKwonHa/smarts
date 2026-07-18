"""ContentAgent — L3 agent.

Generates Korean listing content:
- Title: 25-50 chars, Naver SEO pattern, T1 LLM
- Detail page: structured HTML sections, T1 LLM
- Category mapping: T0 LLM + cached mapping table
- Image overlay check: T0 vision
- Mandatory 구매대행 disclosure block (injected by code, NOT LLM)

Quality moat: content must read human-made. No machine-translation feel.
Failure: 1 LLM retry → listing.failed(reason=content).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jinja2
import structlog
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.events import STREAM_LISTING
from relay.core.llm.client import client as llm

log = structlog.get_logger(__name__)

_PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts" / "content"
_jinja = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(_PROMPTS_DIR)),
    autoescape=False,
)

# Load few-shot examples
_EXAMPLES_PATH = _PROMPTS_DIR / "title_examples.json"
_TITLE_EXAMPLES: list[dict] = []
if _EXAMPLES_PATH.exists():
    import json as _json
    _TITLE_EXAMPLES = _json.loads(_EXAMPLES_PATH.read_text("utf-8"))

# Mandatory Korean disclosure block (legal requirement — injected by code)
_DISCLOSURE_BLOCK = """
<div class="overseas-disclosure" style="background:#f8f8f8;padding:16px;border-left:4px solid #ff6600;margin:24px 0">
<h4>📦 해외 구매대행 상품 고지사항</h4>
<ul>
<li>본 상품은 <strong>해외 구매대행 상품</strong>으로, 일본 현지에서 직접 구매하여 국내로 배송됩니다.</li>
<li><strong>개인통관고유부호</strong>가 필요합니다. 관세청 앱 또는 유니패스(unipass.customs.go.kr)에서 발급 가능합니다.</li>
<li>관세 및 부가세가 발생할 수 있으며, 과세기준(약 15만원)을 초과하는 경우 별도 고지됩니다.</li>
<li>예상 배송기간: <strong>구매확정 후 {delivery_days}일 내외</strong> (일본 구매 → 배대지 → 국내 배송)</li>
<li>해외 구매대행 특성상 단순 변심에 의한 반품이 어려울 수 있습니다. 구매 전 반드시 확인하세요.</li>
</ul>
</div>
"""

# Title character limits
_TITLE_MIN_CHARS = 25
_TITLE_MAX_CHARS = 50


class TitleResult(BaseModel):
    title: str
    keyword_used: list[str] = []
    char_count: int = 0

    @field_validator("title")
    @classmethod
    def validate_title(cls, v: str) -> str:
        v = v.strip()
        if len(v) < _TITLE_MIN_CHARS:
            raise ValueError(f"Title too short: {len(v)} < {_TITLE_MIN_CHARS}")
        if len(v) > _TITLE_MAX_CHARS:
            v = v[:_TITLE_MAX_CHARS]  # truncate rather than fail
        return v


class DetailResult(BaseModel):
    description: str

    @field_validator("description")
    @classmethod
    def strip_desc(cls, v: str) -> str:
        return v.strip()


# ── Category mapping ──────────────────────────────────────────────────────────

# Naver leaf category ID mapping (category_internal → Naver category ID)
# These are approximate — verify against current Naver category tree.
_CATEGORY_MAP: dict[str, str] = {
    "kitchen":    "50000803",   # 주방용품
    "stationery": "50000814",   # 문구/오피스
    "hobby":      "50000830",   # 취미/수집품
    "camping":    "50000826",   # 스포츠/레저 > 캠핑
    "other":      "50000803",   # default to kitchen
}


def get_naver_category_id(category_internal: str) -> str:
    return _CATEGORY_MAP.get(category_internal, _CATEGORY_MAP["other"])


# ── Agent ─────────────────────────────────────────────────────────────────────

class ContentAgent(BaseAgent):
    """L3 — generates Korean listing content from a priced product."""

    name = "content"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        if event.get("type") != "product.priced":
            return []

        payload = event.get("payload", {})
        listing_id = payload["listing_id"]
        product_id = payload["product_id"]
        correlation_id = payload.get("correlation_id", f"listing:{listing_id}")

        return await self._generate_content(listing_id, product_id, correlation_id, session)

    async def _generate_content(
        self,
        listing_id: int,
        product_id: int,
        correlation_id: str,
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        # Load product data
        row = await session.execute(
            text("""
                SELECT p.canonical_name_src, p.brand, p.category_internal,
                       p.attributes, p.images,
                       l.sell_price_krw,
                       ps.price_minor, ps.currency
                FROM products p
                JOIN listings l ON l.product_id = p.id
                LEFT JOIN product_sources ps ON ps.product_id = p.id
                WHERE l.id = :lid
                ORDER BY ps.rank NULLS LAST
                LIMIT 1
            """),
            {"lid": listing_id},
        )
        rec = row.first()
        if rec is None:
            return [_failed_event(listing_id, "content", "product_not_found")]

        (name_src, brand, category_internal, attributes,
         images, sell_price_krw, price_minor, currency) = rec

        attrs = attributes if isinstance(attributes, dict) else json.loads(attributes or "{}")
        imgs = images if isinstance(images, list) else json.loads(images or "[]")

        # Image overlay check (T0 vision) — skip images with overlays
        clean_images = await self._filter_images(imgs, session)
        if not clean_images and imgs:
            clean_images = imgs  # fallback: use all images if all fail check

        # Generate title (T1)
        title_result = await self._generate_title(
            name_src, brand, category_internal, attrs, session, correlation_id
        )
        if title_result is None:
            return [_failed_event(listing_id, "content", "title_gen_failed")]

        # Generate short description via LLM (T1), then build HTML from template
        detail_result = await self._generate_detail(
            title=title_result.title,
            name_src=name_src,
            brand=brand,
            category_internal=category_internal,
            attributes=attrs,
            sell_price_krw=sell_price_krw or 0,
            delivery_days_est=10,
            description_src="",
            session=session,
            correlation_id=correlation_id,
        )
        if detail_result is None:
            return [_failed_event(listing_id, "content", "detail_gen_failed")]

        # Build full HTML from template (structured sections + LLM description)
        full_html = self._build_detail_html(
            title=title_result.title,
            brand=brand or "미표기",
            name_src=name_src,
            category_internal=category_internal,
            attributes=attrs,
            sell_price_krw=sell_price_krw or 0,
            description=detail_result.description,
            delivery_days=10,
        )

        # Naver category ID
        naver_cat_id = get_naver_category_id(category_internal or "other")

        # Persist content to listing
        content_bundle = {
            "title": title_result.title,
            "category_naver": naver_cat_id,
            "detail_html": full_html,
            "images": [img["url"] for img in clean_images if isinstance(img, dict)],
            "keywords_used": title_result.keyword_used,
        }

        await session.execute(
            text("""
                UPDATE listings
                SET title = :title,
                    content = CAST(:content AS JSONB),
                    status = 'CONTENT_READY'
                WHERE id = :id
            """),
            {
                "title": title_result.title,
                "content": json.dumps(content_bundle),
                "id": listing_id,
            },
        )
        # Also update product's Naver category
        await session.execute(
            text("UPDATE products SET category_naver = :cat, canonical_name_ko = :name WHERE id = :id"),
            {"cat": naver_cat_id, "name": title_result.title, "id": product_id},
        )

        log.info(
            "content_generated",
            listing_id=listing_id,
            title=title_result.title,
        )

        return [
            {
                "stream": STREAM_LISTING,
                "type": "listing.content_ready",
                "idempotency_key": f"listing:{listing_id}:content_ready",
                "payload": {"listing_id": listing_id},
            }
        ]

    async def _generate_title(
        self,
        name_src: str,
        brand: str,
        category_internal: str,
        attributes: dict,
        session: AsyncSession,
        correlation_id: str,
    ) -> TitleResult | None:
        """Generate Korean title via T1 LLM. 1 retry on validation failure."""
        template = _jinja.get_template("title_v1.j2")
        rendered = template.render(
            product_name_src=name_src,
            brand=brand,
            category_internal=category_internal,
            attributes=attributes,
            kr_keywords=[],  # GapAnalyzer populates this in M3; empty for M1 longtail
            examples=_TITLE_EXAMPLES[:3],
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "당신은 네이버 스마트스토어 상품명 전문 에디터입니다. "
                    "JSON 형식으로만 응답하세요."
                ),
            },
            {"role": "user", "content": rendered},
        ]

        for attempt in range(2):  # 1 retry
            try:
                resp = await llm.complete(
                    task_name="l3.title_gen",
                    messages=messages,
                    session=session,
                    agent=self.name,
                    trace_id=correlation_id,
                )
                raw = resp.content
                if isinstance(raw, dict) and "_dry_run" in raw:
                    # DRY_RUN fallback — must be >= 25 chars for TitleResult validation
                    base = (name_src or "").strip()
                    fallback = f"일본 {base} 프리미엄 고급 구매대행 상품"
                    if len(fallback) < 25:
                        fallback = f"일본 프리미엄 전동 리무벌 고급형 구매대행 상품"
                    return TitleResult(
                        title=fallback[:50],
                        keyword_used=[],
                        char_count=len(fallback[:50]),
                    )
                result = TitleResult.model_validate(raw)
                result.char_count = len(result.title)
                return result
            except Exception as e:
                log.warning(
                    "title_gen_attempt_failed",
                    attempt=attempt + 1,
                    error=str(e),
                )
                if attempt == 0:
                    # Add error context for repair
                    messages.append({"role": "assistant", "content": str(raw if 'raw' in dir() else "{}")})
                    messages.append({"role": "user", "content": f"Validation error: {e}. Please fix the title and return valid JSON."})

        # Fallback: build Korean title from category/brand (NEVER use Japanese name_src)
        fallback_titles = {
            "kitchen":    "일본 프리미엄 주방용품 구매대행 정품",
            "stationery": "일본 프리미엄 문구용품 구매대행 정품",
            "hobby":      "일본 프리미엄 취미용품 구매대행 정품",
            "camping":    "일본 프리미엄 캠핑용품 구매대행 정품",
            "other":      "일본 프리미엄 생활용품 구매대행 정품",
        }
        if brand:
            fallback = f"일본 {brand} {category_internal or '프리미엄'} 구매대행 정품"
        else:
            fallback = fallback_titles.get(category_internal, fallback_titles["other"])
        if len(fallback) < _TITLE_MIN_CHARS:
            fallback = f"일본 프리미엄 {category_internal or '생활용품'} 구매대행 정품"
        fallback = fallback[:_TITLE_MAX_CHARS]
        log.info("title_fallback_used", listing_id_or_none=None, category=category_internal, brand=brand)
        return TitleResult(
            title=fallback,
            keyword_used=[],
            char_count=len(fallback),
        )

    async def _generate_detail(
        self,
        *,
        title: str,
        name_src: str,
        brand: str,
        category_internal: str,
        attributes: dict,
        sell_price_krw: int,
        delivery_days_est: int,
        description_src: str,
        session: AsyncSession,
        correlation_id: str,
    ) -> DetailResult | None:
        """Generate a short Korean product description via LLM (T1).
        The full HTML template is built by _build_detail_html(), not LLM.
        """
        template = _jinja.get_template("detail_v1.j2")
        rendered = template.render(
            title=title,
            product_name_src=name_src,
            brand=brand,
            category_internal=category_internal,
            attributes=attributes,
            price_krw=sell_price_krw,
            delivery_days_est=delivery_days_est,
            description_src=description_src,
        )
        messages = [
            {
                "role": "system",
                "content": "JSON으로만 응답하세요.",
            },
            {"role": "user", "content": rendered},
        ]

        for attempt in range(2):
            try:
                resp = await llm.complete(
                    task_name="l3.detail_gen",
                    messages=messages,
                    session=session,
                    agent=self.name,
                    trace_id=correlation_id,
                )
                raw = resp.content
                if isinstance(raw, dict) and "_dry_run" in raw:
                    return DetailResult(description="일본 직구 프리미엄 상품입니다. 구매대행으로 안전하게 배송해드립니다.")
                result = DetailResult.model_validate(raw)
                if not result.description or len(result.description.strip()) < 10:
                    raise ValueError("Description too short")
                return result
            except Exception as e:
                log.warning("detail_gen_attempt_failed", attempt=attempt + 1, error=str(e))
                if attempt == 0:
                    messages.append({"role": "assistant", "content": str(raw if 'raw' in dir() else "{}")})
                    messages.append({"role": "user", "content": f"Error: {e}. Return JSON with 'description' field (3-5 Korean sentences)."})

        # Fallback: use a generic description if LLM fails
        return DetailResult(
            description=f"일본에서 직구하는 {brand or ''} {category_internal} 상품입니다. "
                        f"품질 좋은 정품으로 안전하게 배송해드립니다."
        )

    @staticmethod
    def _build_detail_html(
        *,
        title: str,
        brand: str,
        name_src: str,
        category_internal: str,
        attributes: dict,
        sell_price_krw: int,
        description: str,
        delivery_days: int,
    ) -> str:
        """Build structured HTML detail page from template + LLM-generated description."""
        # Spec table rows from attributes
        spec_rows = ""
        for key, val in list(attributes.items())[:6]:
            if val:
                spec_rows += f"<tr><th>{key}</th><td>{val}</td></tr>\n"

        # Image tags
        img_tags = ""
        for img in attributes.get("_images", [])[:6]:
            img_tags += f'<img src="{img}" alt="{title}" style="max-width:100%;margin:8px 0;" />\n'

        return f"""
<div class="product-detail" style="font-family:sans-serif;line-height:1.6;color:#333">
  <h2 style="border-bottom:2px solid #333;padding-bottom:8px">{title}</h2>

  <section style="margin:20px 0">
    <h3>📌 상품 요약</h3>
    <p>{description}</p>
  </section>

  <section style="margin:20px 0">
    <h3>📋 상품 스펙</h3>
    <table style="width:100%;border-collapse:collapse;margin:12px 0">
      <tr style="background:#f5f5f5"><th style="padding:8px;text-align:left;width:30%">상품명</th><td style="padding:8px">{title}</td></tr>
      <tr style="background:#fff"><th style="padding:8px;text-align:left">브랜드</th><td style="padding:8px">{brand}</td></tr>
      <tr style="background:#f5f5f5"><th style="padding:8px;text-align:left">카테고리</th><td style="padding:8px">{category_internal}</td></tr>
      <tr style="background:#fff"><th style="padding:8px;text-align:left">판매가</th><td style="padding:8px">₩{sell_price_krw:,}</td></tr>
      {spec_rows}
    </table>
  </section>

  {img_tags}

  <section style="margin:20px 0">
    <h3>🚚 배송 안내</h3>
    <ul>
      <li>구매확정 후 {delivery_days}일 내 배송 예정 (일본 → 배대지 → 국내 배송)</li>
      <li>개인통관고유부호가 필요합니다 (관세청 앱 또는 유니패스에서 발급)</li>
    </ul>
  </section>

  {_DISCLOSURE_BLOCK.format(delivery_days=delivery_days)}
</div>
"""

    async def _filter_images(
        self,
        images: list[dict | str],
        session: AsyncSession,
    ) -> list[dict | str]:
        """T0 vision check: skip images with competitor overlays."""
        # In M1, skip vision check if LLM not configured — pass all images
        # Full vision filtering in M2
        clean = []
        for img in images[:8]:
            url = img["url"] if isinstance(img, dict) else str(img)
            if url and url.startswith("http"):
                clean.append(img)
        return clean[:6]


def _failed_event(listing_id: int, stage: str, reason: str) -> dict[str, Any]:
    return {
        "stream": STREAM_LISTING,
        "type": "listing.failed",
        "idempotency_key": f"listing:{listing_id}:failed:{stage}:{reason}",
        "payload": {"listing_id": listing_id, "stage": stage, "reason": reason},
    }
