"""Full pipeline LIVE run — real Rakuten, real LLM, real Naver API.

Usage:
    python scripts/live_pipeline.py
    python scripts/live_pipeline.py --verbose
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

# Ensure we're NOT in dry run
os.environ["RELAY_DRY_RUN"] = "0"
os.environ.setdefault("RELAY_ENV", "prod")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _seed_config(session) -> None:
    """Seed all required config values."""
    from sqlalchemy import text
    configs = [
        ("pricing.platform_fee", {"value": 0.059}),
        ("pricing.target_margin", {"value": 0.15}),
        ("pricing.min_margin_abs", {"value": 3000}),
        ("pricing.domestic_ship_krw", {"value": 3000}),
        ("pricing.fixed_buffer_krw", {"value": 2000}),
        ("pricing.category_price_ceiling", {"value": 200000}),
        ("pricing.customs_threshold_krw", {"value": 150000}),
        ("pricing.duty_rate", {"value": 0.08}),
        ("pricing.vat_rate", {"value": 0.10}),
        ("trend.score_threshold", {"value": 0.3}),
        ("trend.max_candidates_per_scan", {"value": 20}),
        ("hitl.auto.publish_batch", {"enabled": True}),
        ("publish.mode", {"mode": "api"})]
    for key, val in configs:
        await session.execute(
            text("""
                INSERT INTO app_config (key, value, updated_by)
                VALUES (:key, CAST(:val AS JSONB), 'live_pipeline')
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """),
            {"key": key, "val": json.dumps(val)},
        )
    await session.commit()


async def _seed_fx_rate(session) -> None:
    """Fetch live FX rate or use fallback."""
    from sqlalchemy import text
    try:
        from relay.core.http import http_client
        data = await http_client.get(
            "https://open.er-api.com/v6/latest/JPY",
            cache_s=0,
        )
        rates = data.get("rates", {})
        krw = rates.get("KRW", 9.25)
    except Exception:
        krw = 9.25

    await session.execute(
        text("""
            INSERT INTO fx_rates (pair, rate, at)
            VALUES ('KRW/JPY', :rate, now())
            ON CONFLICT DO NOTHING
        """),
        {"rate": krw},
    )
    await session.commit()
    print(f"       FX rate: 1 JPY = {krw} KRW")


async def _cleanup_test_data(session) -> None:
    """Clean ALL test data (truncate business tables, keep config/seeds)."""
    from sqlalchemy import text
    # Full cleanup of all transactional tables (order matters for FKs)
    await session.execute(text("""
        TRUNCATE TABLE
            claims, inquiries, shipments, order_events, purchases,
            orders, price_history, listings, product_sources,
            products, risk_flags, trend_candidates,
            event_outbox, processed_events, llm_cache,
            approval_requests, brand_leads, preorder_campaigns
        RESTART IDENTITY CASCADE
    """))
    await session.commit()


async def run_trend_scout(session, verbose: bool = False) -> list[dict]:
    """Step 1: Run TrendScout with REAL Rakuten ranking data."""
    from relay.integrations.rakuten.client import get_ranking
    from relay.intelligence.trend_scout import TrendScoutAgent

    # Patch _fetch_all_sources to use real Rakuten ranking
    original_fetch = TrendScoutAgent._fetch_all_sources

    async def live_fetch(self, session, verbose=False):
        items = []
        categories = [
            ("100804", "kitchen_gadgets"),
            ("101240", "stationery"),
            ("101213", "hobby_accessories"),
            ("100628", "camping_small_goods"),
        ]
        for genre_id, label in categories:
            try:
                results = await get_ranking(genre_id, hits=10)
                for r in results:
                    raw = r.raw.get("Item", r.raw)
                    items.append({
                        "name_raw": r.name,
                        "external_key": r.item_code,
                        "source": "live_rakuten_ranking",
                        "price_jpy": r.price_jpy,
                        "review_count": raw.get("reviewCount"),
                        "review_average": raw.get("reviewAverage"),
                        "rank": raw.get("rank", 0),
                        "genre_id": r.genre_id,
                        "image_url": r.image_urls[0] if r.image_urls else "",
                        "item_url": r.url,
                        "shop_name": r.seller_name,
                    })
            except Exception as e:
                print(f"       ⚠ Rakuten ranking error for {genre_id}: {e}")
        return items

    TrendScoutAgent._fetch_all_sources = live_fetch  # type: ignore[method-assign]

    try:
        event = {
            "type": "tick.trend_scan",
            "payload": {},
            "idempotency_key": f"live:trend_scan:{asyncio.get_event_loop().time()}",
            "correlation_id": "live_pipeline",
        }
        agent = TrendScoutAgent()
        emitted = await agent.handle(event, session)
        await session.commit()
        print(f"       Discovered {len(emitted)} candidates")
        if verbose:
            for e in emitted:
                p = e["payload"]
                print(f"         - #{p['candidate_id']} score={p['accel_score']:.3f} {p['name'][:50]}")
        return emitted
    finally:
        TrendScoutAgent._fetch_all_sources = original_fetch  # type: ignore[method-assign]


async def run_risk_filter(session, discovered_events: list[dict], verbose: bool = False) -> list[dict]:
    """Step 2: Run RiskFilter (real LLM screen)."""
    from sqlalchemy import text
    from relay.intelligence.risk_filter import RiskFilterAgent

    cleared = []
    agent = RiskFilterAgent()

    # Process max 5 candidates to keep run time reasonable
    discovered_events = discovered_events[:5]

    for disc_event in discovered_events:
        candidate_id = disc_event["payload"]["candidate_id"]
        await session.execute(
            text("UPDATE trend_candidates SET status = 'VALIDATED' WHERE id = :id"),
            {"id": candidate_id},
        )
        await session.commit()

        event = {
            "type": "candidate.validated",
            "payload": {"candidate_id": candidate_id},
            "idempotency_key": f"live:candidate:{candidate_id}:validated",
            "correlation_id": "live_pipeline",
        }
        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "candidate.cleared":
                cleared.append(e)
                if verbose:
                    print(f"         - #{candidate_id} ✅ PASSED")
            elif e["type"] == "candidate.rejected":
                reason = e["payload"].get("reason", "unknown")
                print(f"         - #{candidate_id} ❌ REJECTED: {reason}")

    print(f"       Passed: {len(cleared)}, Rejected: {len(discovered_events) - len(cleared)}")
    return cleared


async def run_source_matcher(session, cleared: list[dict], verbose: bool = False) -> list[dict]:
    """Step 3: Run SourceMatcher with REAL Rakuten search."""
    from relay.listing.source_matcher import SourceMatcherAgent

    sourced = []
    agent = SourceMatcherAgent()

    for clr in cleared:
        candidate_id = clr["payload"]["candidate_id"]
        event = {
            "type": "candidate.cleared",
            "payload": {"candidate_id": candidate_id},
            "idempotency_key": f"live:candidate:{candidate_id}:sourced",
            "correlation_id": "live_pipeline",
        }
        from sqlalchemy import text
        await session.execute(
            text("DELETE FROM processed_events WHERE idempotency_key = :k"),
            {"k": event["idempotency_key"]},
        )
        await session.commit()

        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "product.sourced":
                sourced.append(e)
                if verbose:
                    print(f"         - product_id={e['payload']['product_id']}")

    print(f"       Sourced {len(sourced)} products")
    return sourced


async def run_pricing(session, sourced: list[dict], verbose: bool = False) -> list[dict]:
    """Step 4: Run PricingAgent."""
    from sqlalchemy import text
    from relay.listing.pricing import PricingAgent

    priced = []
    agent = PricingAgent()

    for src in sourced:
        event = {
            "type": "product.sourced",
            "payload": src["payload"],
            "idempotency_key": f"live:product:{src['payload']['product_id']}:priced",
            "correlation_id": "live_pipeline",
        }
        await session.execute(
            text("DELETE FROM processed_events WHERE idempotency_key = :k"),
            {"k": event["idempotency_key"]},
        )
        await session.commit()

        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "product.priced":
                priced.append(e)
                if verbose:
                    print(f"         - listing_id={e['payload']['listing_id']} ₩{e['payload']['sell_price_krw']:,}")

    print(f"       Priced {len(priced)} listings")
    return priced


async def run_content(session, priced: list[dict], verbose: bool = False) -> list[dict]:
    """Step 5: Run ContentAgent (real LLM)."""
    from sqlalchemy import text
    from relay.listing.content import ContentAgent

    content_ready = []
    agent = ContentAgent()

    for p in priced:
        event = {
            "type": "product.priced",
            "payload": p["payload"],
            "idempotency_key": f"live:listing:{p['payload']['listing_id']}:content",
            "correlation_id": "live_pipeline",
        }
        await session.execute(
            text("DELETE FROM processed_events WHERE idempotency_key = :k"),
            {"k": event["idempotency_key"]},
        )
        await session.commit()

        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "listing.content_ready":
                content_ready.append(e)
                if verbose:
                    print(f"         - listing_id={e['payload']['listing_id']}")

    print(f"       Content ready: {len(content_ready)} listings")
    return content_ready


async def run_publisher(session, content_ready: list[dict], verbose: bool = False) -> list[dict]:
    """Step 6: Run PublishAgent (real Naver API)."""
    from sqlalchemy import text
    from relay.listing.publisher import PublishAgent

    published = []
    agent = PublishAgent()

    for cr in content_ready:
        listing_id = cr["payload"]["listing_id"]
        event = {
            "type": "listing.content_ready",
            "payload": {"listing_id": listing_id},
            "idempotency_key": f"live:listing:{listing_id}:published",
            "correlation_id": "live_pipeline",
        }
        await session.execute(
            text("DELETE FROM processed_events WHERE idempotency_key = :k"),
            {"k": event["idempotency_key"]},
        )
        await session.commit()

        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "listing.published":
                published.append(e)
                if verbose:
                    print(f"         - listing_id={listing_id} → Naver #{e['payload']['remote_product_id']}")
            elif e["type"] == "listing.failed":
                print(f"         - listing_id={listing_id} ❌ FAILED: {e['payload'].get('reason', 'unknown')}")

    print(f"       Published: {len(published)} listings")
    return published


async def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Full pipeline LIVE run")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    from relay.core.db import AsyncSessionLocal

    print("=" * 70)
    print("  RELAY Full Pipeline — LIVE RUN")
    print("=" * 70)
    print(f"  RELAY_DRY_RUN: {os.environ.get('RELAY_DRY_RUN', 'NOT SET')}")
    print(f"  RELAY_ENV: {os.environ.get('RELAY_ENV', 'NOT SET')}")
    print()

    # ── Setup ─────────────────────────────────────────────────────────────────
    print("[Setup] Seeding config + FX rate...")
    async with AsyncSessionLocal() as session:
        await _seed_fx_rate(session)
        await _seed_config(session)
        await _cleanup_test_data(session)
        print("       Done.")
        print()

    # ── Stage 1: TrendScout ───────────────────────────────────────────────────
    print("[1/6] TREND SCOUT — real Rakuten ranking")
    async with AsyncSessionLocal() as session:
        discovered = await run_trend_scout(session, args.verbose)
        print()

    if not discovered:
        print("  ⚠️ No candidates discovered — check Rakuten API credentials.")
        return

    # ── Stage 2: RiskFilter ───────────────────────────────────────────────────
    print("[2/6] RISK FILTER — rule + LLM screen")
    async with AsyncSessionLocal() as session:
        cleared = await run_risk_filter(session, discovered, args.verbose)
        print()

    if not cleared:
        print("  ⚠️ No candidates cleared RiskFilter.")
        return

    # ── Stage 3: SourceMatcher ────────────────────────────────────────────────
    print("[3/6] SOURCE MATCHER — real Rakuten search")
    async with AsyncSessionLocal() as session:
        sourced = await run_source_matcher(session, cleared, args.verbose)
        print()

    if not sourced:
        print("  ⚠️ No products sourced.")
        return

    # ── Stage 4: PricingAgent ─────────────────────────────────────────────────
    print("[4/6] PRICING AGENT")
    async with AsyncSessionLocal() as session:
        priced = await run_pricing(session, sourced, args.verbose)
        print()

    if not priced:
        print("  ⚠️ No products priced.")
        return

    # ── Stage 5: ContentAgent ─────────────────────────────────────────────────
    print("[5/6] CONTENT AGENT — real LLM")
    async with AsyncSessionLocal() as session:
        content_ready = await run_content(session, priced, args.verbose)
        print()

    if not content_ready:
        print("  ⚠️ No content generated.")
        return

    # ── Stage 6: PublishAgent ─────────────────────────────────────────────────
    print("[6/6] PUBLISH AGENT — real Naver API")
    async with AsyncSessionLocal() as session:
        published = await run_publisher(session, content_ready, args.verbose)
        print()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 70)
    print("  LIVE PIPELINE SUMMARY")
    print("=" * 70)
    print(f"  Discovered:    {len(discovered)}")
    print(f"  Risk cleared:  {len(cleared)}")
    print(f"  Sourced:       {len(sourced)}")
    print(f"  Priced:        {len(priced)}")
    print(f"  Content ready: {len(content_ready)}")
    print(f"  Published:     {len(published)}")
    print()

    if published:
        print("  ✅ PRODUCTS LIVE ON NAVER SMARTSTORE!")
        for pub in published:
            p = pub["payload"]
            print(f"     Listing #{p['listing_id']} → Naver #{p['remote_product_id']}")
    print()
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
