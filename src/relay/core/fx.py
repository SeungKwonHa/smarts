"""FX rate service.

Fetches KRW/JPY and KRW/USD hourly from a free exchange-rate API.
Stores in fx_rates table. Pricing uses the latest rate with the configured buffer.

Consumed by: PricingAgent, StockMonitor (reprice trigger), Reporter.
"""

from __future__ import annotations

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.config import settings
from relay.core.http import http_client

log = structlog.get_logger(__name__)

# Pairs we care about (base = KRW, quote = foreign currency)
# We store as quote-per-KRW (how many JPY does 1 KRW buy?) — but pricing
# uses KRW-per-JPY, so get_rate() inverts appropriately.
TRACKED_PAIRS = ["JPY", "USD"]


async def refresh_fx_rates(session: AsyncSession) -> dict[str, float]:
    """Fetch latest rates and upsert into fx_rates. Returns {pair: rate}."""
    # open.er-api.com: GET /v6/latest/KRW
    url = f"{settings.fx_api_url}/KRW"
    try:
        data = await http_client.get_json(url, cache_s=0)
        rates_raw: dict[str, float] = data.get("rates", {})
    except Exception as e:
        log.error("fx_refresh_failed", error=str(e))
        raise

    results: dict[str, float] = {}
    for foreign_currency in TRACKED_PAIRS:
        if foreign_currency not in rates_raw:
            continue
        # rates_raw[JPY] = JPY per 1 KRW → invert to get KRW per 1 JPY
        krw_per_unit = 1.0 / rates_raw[foreign_currency]
        pair = f"KRW/{foreign_currency}"  # "KRW per 1 JPY"
        results[pair] = krw_per_unit

        await session.execute(
            text("""
                INSERT INTO fx_rates (pair, rate, at)
                VALUES (:pair, :rate, now())
                ON CONFLICT DO NOTHING
            """),
            {"pair": pair, "rate": krw_per_unit},
        )

    await session.commit()
    log.info("fx_refreshed", pairs=results)
    return results


async def get_rate(session: AsyncSession, pair: str) -> float:
    """Get the latest KRW-per-unit rate for a pair (e.g., 'KRW/JPY').

    Raises RuntimeError if no rate found (should never happen after first refresh).
    """
    row = await session.execute(
        text("""
            SELECT rate FROM fx_rates
            WHERE pair = :pair
            ORDER BY at DESC
            LIMIT 1
        """),
        {"pair": pair},
    )
    result = row.first()
    if result is None:
        raise RuntimeError(
            f"No FX rate found for {pair}. "
            "Ensure tick.fx_refresh has run at least once."
        )
    return float(result[0])


async def get_rate_with_buffer(session: AsyncSession, pair: str) -> float:
    """Return rate × fx_buffer for use in pricing (conservative = higher cost)."""
    rate = await get_rate(session, pair)
    return rate * settings.fx_buffer
