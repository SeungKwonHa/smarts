"""PricingAgent — L2 agent (no LLM — deterministic formula).

Formula (doc 02 §L2):
  landed  = src_price * fx * (1 + fx_buffer) + intl_ship_est(weight) + customs_est
  sell    = ceil_to_pricepoint(
                (landed + domestic_ship + fixed_buffer)
                / (1 - platform_fee - target_margin)
            )
  reject  if margin_krw < min_margin_abs or sell > category_price_ceiling

All monetary values in KRW (integers) or source currency minor units.
Config is read from app_config table at runtime (tunable without redeploy).
"""

from __future__ import annotations

import math
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.agent import BaseAgent
from relay.core.events import STREAM_INTEL, STREAM_LISTING
from relay.core.fx import get_rate_with_buffer

log = structlog.get_logger(__name__)


# ── Defaults (overridden by app_config at runtime) ────────────────────────────
_DEFAULTS: dict[str, float] = {
    "platform_fee":          0.059,   # Naver SmartStore ~5.9% standard commission
    "target_margin":         0.15,    # 15% target margin
    "min_margin_abs":        3_000,   # minimum 3,000 KRW margin
    "domestic_ship_krw":     3_000,   # domestic delivery cost to customer
    "fixed_buffer_krw":      2_000,   # forwarding overhead per order
    "category_price_ceiling": 200_000, # cap sell price at 200,000 KRW
    "customs_threshold_krw": 150_000, # below = de minimis (no customs)
    "duty_rate":             0.08,    # 8% duty on landed value above threshold
    "vat_rate":              0.10,    # 10% VAT on dutiable landed value
}

# Estimated intl shipping by weight (JPY → will be converted)
# These are approximate EMS/SAL small parcel rates for Korea route.
_INTL_SHIP_TIERS_JPY: list[tuple[int, int]] = [  # (max_weight_g, flat_jpy)
    (200,  600),
    (500,  900),
    (1000, 1400),
    (2000, 2000),
    (5000, 3500),
]
_INTL_SHIP_DEFAULT_JPY = 5_000  # >5kg


async def _get_config(key: str, session: AsyncSession, default: float) -> float:
    """Read a pricing config value from app_config (runtime-tunable)."""
    row = await session.execute(
        text("SELECT value FROM app_config WHERE key = :k"),
        {"k": f"pricing.{key}"},
    )
    result = row.first()
    if result and isinstance(result[0], (int, float)):
        return float(result[0])
    if result and isinstance(result[0], dict):
        return float(result[0].get("value", default))
    return default


def _intl_ship_est_jpy(weight_g: int) -> int:
    for max_g, fee_jpy in _INTL_SHIP_TIERS_JPY:
        if weight_g <= max_g:
            return fee_jpy
    return _INTL_SHIP_DEFAULT_JPY


def _customs_est_krw(landed_krw: int, duty_rate: float, vat_rate: float, threshold_krw: int) -> int:
    """Estimate customs duty + VAT for Korea import.

    Under de-minimis threshold: 0 (personal import clearance).
    Above threshold: duty on (landed - threshold), then VAT on (landed + duty).
    """
    if landed_krw <= threshold_krw:
        return 0
    dutiable = landed_krw - threshold_krw
    duty = int(dutiable * duty_rate)
    vat = int((landed_krw + duty) * vat_rate)
    return duty + vat


def ceil_to_pricepoint(price: float) -> int:
    """Round up to the nearest clean Korean price point.

    Korean buyers expect prices ending in 0 or 900/500.
    Under 10k: round up to nearest 100.
    10k-100k: round up to nearest 500.
    Over 100k: round up to nearest 1000.
    """
    if price <= 10_000:
        return int(math.ceil(price / 100)) * 100
    elif price <= 100_000:
        return int(math.ceil(price / 500)) * 500
    else:
        return int(math.ceil(price / 1_000)) * 1_000


async def compute_price(
    *,
    src_price_minor: int,   # e.g. price in JPY minor units (JPY has no sub-units: minor == whole)
    currency: str,           # JPY | USD
    weight_g: int,
    session: AsyncSession,
) -> dict[str, Any] | None:
    """Compute sell price and margin. Returns None if product should be rejected."""

    # Read config
    platform_fee     = await _get_config("platform_fee",     session, _DEFAULTS["platform_fee"])
    target_margin    = await _get_config("target_margin",     session, _DEFAULTS["target_margin"])
    min_margin_abs   = await _get_config("min_margin_abs",    session, _DEFAULTS["min_margin_abs"])
    domestic_ship    = await _get_config("domestic_ship_krw", session, _DEFAULTS["domestic_ship_krw"])
    fixed_buffer     = await _get_config("fixed_buffer_krw",  session, _DEFAULTS["fixed_buffer_krw"])
    price_ceiling    = await _get_config("category_price_ceiling", session, _DEFAULTS["category_price_ceiling"])
    customs_thresh   = await _get_config("customs_threshold_krw",  session, _DEFAULTS["customs_threshold_krw"])
    duty_rate        = await _get_config("duty_rate",         session, _DEFAULTS["duty_rate"])
    vat_rate         = await _get_config("vat_rate",          session, _DEFAULTS["vat_rate"])

    # FX with buffer
    pair = f"KRW/{currency}"
    fx = await get_rate_with_buffer(session, pair)  # returns KRW per 1 foreign unit
    if fx <= 0:
        log.error("pricing_fx_unavailable", pair=pair)
        return None

    # Landed cost (KRW)
    src_krw = int(src_price_minor * fx)
    intl_ship_jpy = _intl_ship_est_jpy(weight_g)
    intl_ship_krw = int(intl_ship_jpy * fx) if currency == "JPY" else int(intl_ship_jpy * 9.5)  # rough JPY base
    landed_krw = src_krw + intl_ship_krw

    # Customs estimate
    customs_krw = _customs_est_krw(landed_krw, duty_rate, vat_rate, int(customs_thresh))

    # Total cost basis
    total_cost = landed_krw + customs_krw + int(domestic_ship) + int(fixed_buffer)

    # Sell price formula
    denom = 1 - platform_fee - target_margin
    if denom <= 0:
        log.error("pricing_invalid_denom", platform_fee=platform_fee, target_margin=target_margin)
        return None

    sell_price = ceil_to_pricepoint(total_cost / denom)

    # Margin computation
    revenue_after_fee = sell_price * (1 - platform_fee)
    margin_krw = int(revenue_after_fee - total_cost)
    margin_rate = margin_krw / sell_price if sell_price > 0 else 0.0

    # Rejection checks
    if margin_krw < min_margin_abs:
        log.info(
            "pricing_rejected_margin",
            margin_krw=margin_krw,
            min=min_margin_abs,
            src_price=src_price_minor,
        )
        return None

    if sell_price > price_ceiling:
        log.info(
            "pricing_rejected_ceiling",
            sell_price=sell_price,
            ceiling=price_ceiling,
        )
        return None

    return {
        "src_price_minor": src_price_minor,
        "currency": currency,
        "fx": fx,
        "landed_krw": landed_krw,
        "customs_krw": customs_krw,
        "intl_ship_krw": intl_ship_krw,
        "sell_price_krw": sell_price,
        "margin_krw": margin_krw,
        "margin_rate": round(margin_rate, 4),
    }


# ── Agent ─────────────────────────────────────────────────────────────────────

class PricingAgent(BaseAgent):
    """L2 — prices a sourced product and creates a DRAFT listing."""

    name = "pricing"

    async def handle(
        self,
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        event_type = event.get("type", "")
        payload = event.get("payload", {})

        if event_type == "product.sourced":
            return await self._price_product(payload, event, session)
        if event_type == "price.reprice_required":
            return await self._reprice(payload, event, session)
        return []

    async def _price_product(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        product_id = payload["product_id"]
        correlation_id = payload.get("correlation_id", f"product:{product_id}")

        # Load product + best source
        row = await session.execute(
            text("""
                SELECT ps.id, ps.currency, ps.price_minor, ps.weight_g, p.category_internal
                FROM product_sources ps
                JOIN products p ON p.id = ps.product_id
                WHERE ps.product_id = :pid AND ps.stock_state = 'IN_STOCK'
                ORDER BY ps.rank, ps.price_minor
                LIMIT 1
            """),
            {"pid": product_id},
        )
        rec = row.first()
        if rec is None:
            log.warning("pricing_no_source", product_id=product_id)
            return [_margin_rejected_event(product_id, correlation_id)]

        source_id, currency, price_minor, weight_g, category = rec

        pricing = await compute_price(
            src_price_minor=price_minor,
            currency=currency or "JPY",
            weight_g=weight_g or 300,
            session=session,
        )

        if pricing is None:
            await session.execute(
                text("UPDATE products SET status = 'RETIRED' WHERE id = :id"),
                {"id": product_id},
            )
            return [_margin_rejected_event(product_id, correlation_id)]

        # Create DRAFT listing
        listing_row = await session.execute(
            text("""
                INSERT INTO listings
                  (product_id, marketplace, store_account,
                   sell_price_krw, margin_krw, margin_rate, status)
                VALUES
                  (:pid, 'naver', :account, :price, :margin, :rate, 'DRAFT')
                ON CONFLICT DO NOTHING
                RETURNING id
            """),
            {
                "pid": product_id,
                "account": "default",
                "price": pricing["sell_price_krw"],
                "margin": pricing["margin_krw"],
                "rate": pricing["margin_rate"],
            },
        )
        listing_id_row = listing_row.first()
        if listing_id_row is None:
            log.debug("pricing_listing_already_exists", product_id=product_id)
            return []
        listing_id = listing_id_row[0]

        # Record price history
        await session.execute(
            text("""
                INSERT INTO price_history
                  (listing_id, source_id, src_price_minor, fx, landed_krw, sell_price_krw, reason)
                VALUES (:lid, :sid, :src, :fx, :landed, :sell, 'initial')
            """),
            {
                "lid": listing_id,
                "sid": source_id,
                "src": pricing["src_price_minor"],
                "fx": pricing["fx"],
                "landed": pricing["landed_krw"],
                "sell": pricing["sell_price_krw"],
            },
        )

        log.info(
            "listing_priced",
            listing_id=listing_id,
            sell_price=pricing["sell_price_krw"],
            margin=pricing["margin_krw"],
        )

        return [
            {
                "stream": STREAM_LISTING,
                "type": "product.priced",
                "idempotency_key": f"product:{product_id}:priced",
                "payload": {
                    "product_id": product_id,
                    "listing_id": listing_id,
                    "sell_price_krw": pricing["sell_price_krw"],
                    "margin_rate": pricing["margin_rate"],
                    "correlation_id": correlation_id,
                },
            }
        ]

    async def _reprice(
        self,
        payload: dict[str, Any],
        event: dict[str, Any],
        session: AsyncSession,
    ) -> list[dict[str, Any]]:
        """Re-price an existing LIVE listing due to FX move or source price change."""
        listing_id = payload["listing_id"]
        cause = payload.get("cause", "manual")
        correlation_id = event.get("correlation_id", f"listing:{listing_id}")

        row = await session.execute(
            text("""
                SELECT l.product_id, ps.id, ps.currency, ps.price_minor, ps.weight_g
                FROM listings l
                JOIN products p ON p.id = l.product_id
                JOIN product_sources ps ON ps.product_id = p.id
                WHERE l.id = :lid AND ps.stock_state = 'IN_STOCK'
                ORDER BY ps.rank LIMIT 1
            """),
            {"lid": listing_id},
        )
        rec = row.first()
        if rec is None:
            return []

        product_id, source_id, currency, price_minor, weight_g = rec
        pricing = await compute_price(
            src_price_minor=price_minor,
            currency=currency or "JPY",
            weight_g=weight_g or 300,
            session=session,
        )

        if pricing is None:
            # Margin gone — suspend listing
            await session.execute(
                text("UPDATE listings SET status = 'SUSPENDED_STOCKOUT' WHERE id = :id"),
                {"id": listing_id},
            )
            log.warning("listing_repriced_margin_gone", listing_id=listing_id)
            return []

        await session.execute(
            text("""
                UPDATE listings
                SET sell_price_krw = :price, margin_krw = :margin, margin_rate = :rate
                WHERE id = :id
            """),
            {
                "price": pricing["sell_price_krw"],
                "margin": pricing["margin_krw"],
                "rate": pricing["margin_rate"],
                "id": listing_id,
            },
        )

        await session.execute(
            text("""
                INSERT INTO price_history
                  (listing_id, source_id, src_price_minor, fx, landed_krw, sell_price_krw, reason)
                VALUES (:lid, :sid, :src, :fx, :landed, :sell, :reason)
            """),
            {
                "lid": listing_id, "sid": source_id,
                "src": pricing["src_price_minor"], "fx": pricing["fx"],
                "landed": pricing["landed_krw"], "sell": pricing["sell_price_krw"],
                "reason": cause,
            },
        )
        log.info("listing_repriced", listing_id=listing_id, cause=cause, price=pricing["sell_price_krw"])
        return []


def _margin_rejected_event(product_id: int, correlation_id: str) -> dict[str, Any]:
    return {
        "stream": STREAM_INTEL,
        "type": "candidate.rejected",
        "idempotency_key": f"product:{product_id}:rejected:margin",
        "payload": {"product_id": product_id, "reason": "margin"},
    }
