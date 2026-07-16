"""Full end-to-end pipeline dry-run test — chains ALL agents without external I/O.

Simulates the complete flow:
  tick.trend_scan → TrendScout (mock Rakuten data)
    → candidate.discovered events
  candidate.validated → RiskFilter (rule + LLM screen)
    → candidate.cleared / candidate.rejected
  candidate.cleared → SourceMatcher (upsert product + source)
    → product.sourced
  product.sourced → PricingAgent (FX + margin formula)
    → product.priced
  product.priced → ContentAgent (Korean title + detail HTML)
    → listing.content_ready
  listing.content_ready → PublishAgent (Naver API via dry run)
    → listing.published

All external APIs are mocked. LLM calls use DRY_RUN rule-based fallback.
Database writes happen (in test DB) so you can inspect state after.

Usage:
    RELAY_DRY_RUN=1 python scripts/dry_run_pipeline_e2e.py
    python scripts/dry_run_pipeline_e2e.py --verbose
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

# Force dry run for safety
os.environ["RELAY_DRY_RUN"] = "1"
os.environ.setdefault("RELAY_ENV", "test")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Mock data ────────────────────────────────────────────────────────────────

MOCK_RAKUTEN_ITEMS = [
    {
        "rank": 1,
        "itemName": "＼楽天ランキング受賞 ／ 【15%OFFクーポン】THERMOS 水筒 真空断熱 JNL-500 保冷 保温 500ml",
        "itemCode": "thermos-jnl500",
        "itemPrice": 3280,
        "reviewCount": 2450,
        "reviewAverage": 4.3,
        "genreId": 558944,
        "mediumImageUrls": ["https://img.example.com/thermos.jpg"],
        "itemUrl": "https://item.rakuten.co.jp/test/thermos-jnl500/",
        "shopName": "公式ショップ",
        "shopCode": "official-shop",
    },
    {
        "rank": 2,
        "itemName": "【ポイント10倍】象印 マグボトル ワンタッチ 直飲み シームレス ステンレス SM-KA48",
        "itemCode": "zojirushi-smka48",
        "itemPrice": 4180,
        "reviewCount": 1820,
        "reviewAverage": 4.5,
        "genreId": 558944,
        "mediumImageUrls": ["https://img.example.com/zojirushi.jpg"],
        "itemUrl": "https://item.rakuten.co.jp/test/zojirushi-smka48/",
        "shopName": "公式店",
        "shopCode": "formula-store",
    },
    {
        "rank": 3,
        "itemName": "【先着★12点セットが10780円⇒9280円！】CAROTE カローテ フライパン セット IH対応 PFOAフリー",
        "itemCode": "carote-frypan-set",
        "itemPrice": 9280,
        "reviewCount": 890,
        "reviewAverage": 4.1,
        "genreId": 558885,
        "mediumImageUrls": ["https://img.example.com/carote.jpg"],
        "itemUrl": "https://item.rakuten.co.jp/test/carote-frypan/",
        "shopName": "KitchenWorld",
        "shopCode": "kitchen-world",
    },
    {
        "rank": 5,
        "itemName": "VAKUEN 真空保存容器 電動 強力密閉 鮮度長持ち コンテナ タッパー BPAフリー 電子レンジ",
        "itemCode": "vakuen-container",
        "itemPrice": 5980,
        "reviewCount": 3200,
        "reviewAverage": 4.6,
        "genreId": 558885,
        "mediumImageUrls": ["https://img.example.com/vakuen.jpg"],
        "itemUrl": "https://item.rakuten.co.jp/test/vakuen-container/",
        "shopName": "生活雑貨店",
        "shopCode": "life-goods",
    },
    {
        "rank": 8,
        "itemName": "＼楽天1位／ タイガー魔法瓶 水筒 食洗機対応 パッキン一体 らくらくキャップ 真空断熱 ボトル MMZ",
        "itemCode": "tiger-mmz",
        "itemPrice": 4580,
        "reviewCount": 1560,
        "reviewAverage": 4.4,
        "genreId": 558944,
        "mediumImageUrls": ["https://img.example.com/tiger.jpg"],
        "itemUrl": "https://item.rakuten.co.jp/test/tiger-mmz/",
        "shopName": "タイガー公式",
        "shopCode": "tiger-official",
    },
]


# ── Test harness ─────────────────────────────────────────────────────────────

class PipelineStats:
    """Track what happened at each pipeline stage."""

    def __init__(self) -> None:
        self.trend_candidates = 0
        self.discovered: list[dict] = []
        self.risk_passed: list[int] = []
        self.risk_rejected: list[tuple[int, str]] = []
        self.sourced: list[int] = []
        self.priced: list[tuple[int, int, int]] = []  # (product_id, sell_price, margin)
        self.content_ready: list[int] = []
        self.published: list[int] = []
        self.failed: list[tuple[str, int, str]] = []

    def summary(self) -> str:
        lines = [
            f"  Trend candidates:       {self.trend_candidates}",
            f"  Discovered (emitted):   {len(self.discovered)}",
            f"  RiskFilter passed:      {len(self.risk_passed)}",
            f"  RiskFilter rejected:    {len(self.risk_rejected)}",
            f"  Sourced (new products):{len(self.sourced)}",
            f"  Priced (DRAFT listing):{len(self.priced)}",
            f"  Content ready:          {len(self.content_ready)}",
            f"  Published:              {len(self.published)}",
            f"  Failed:                 {len(self.failed)}",
        ]
        return "\n".join(lines)


async def _seed_fx_rate(session) -> None:
    """Insert a mock KRW/JPY rate so PricingAgent can compute."""
    from sqlalchemy import text
    await session.execute(
        text("""
            INSERT INTO fx_rates (pair, rate, at)
            VALUES ('KRW/JPY', 9.25, now())
            ON CONFLICT DO NOTHING
        """)
    )
    await session.commit()


async def _seed_pricing_config(session) -> None:
    """Seed default pricing config so runtime-tunable values are present."""
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
    ]
    for key, val in configs:
        await session.execute(
            text("""
                INSERT INTO app_config (key, value, updated_by)
                VALUES (:key, CAST(:val AS JSONB), 'dry_run_e2e')
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """),
            {"key": key, "val": json.dumps(val)},
        )
    await session.commit()


async def run_trend_scout(session, stats: PipelineStats) -> list[dict]:
    """Step 1: Run TrendScout with mock data."""
    from sqlalchemy import text
    from relay.intelligence.trend_scout import TrendScoutAgent

    # Patch _fetch_all_sources to return mock data
    original_fetch = TrendScoutAgent._fetch_all_sources

    async def mock_fetch(self, session, verbose=False):
        items = []
        for item in MOCK_RAKUTEN_ITEMS:
            items.append({
                "name_raw": item["itemName"],
                "external_key": item["itemCode"],
                "source": "mock_rakuten_ranking",
                "price_jpy": item["itemPrice"],
                "review_count": item["reviewCount"],
                "review_average": item["reviewAverage"],
                "rank": item["rank"],
                "genre_id": item["genreId"],
                "image_url": item["mediumImageUrls"][0],
                "item_url": item["itemUrl"],
                "shop_name": item["shopName"],
            })
        return items

    TrendScoutAgent._fetch_all_sources = mock_fetch  # type: ignore[method-assign]

    try:
        event = {
            "type": "tick.trend_scan",
            "payload": {},
            "idempotency_key": f"e2e:trend_scan:{asyncio.get_event_loop().time()}",
            "correlation_id": "e2e_test",
        }
        agent = TrendScoutAgent()
        emitted = await agent.handle(event, session)
        await session.commit()

        stats.trend_candidates = len(MOCK_RAKUTEN_ITEMS)
        stats.discovered = [
            {
                "candidate_id": e["payload"]["candidate_id"],
                "name": e["payload"]["name"],
                "score": e["payload"]["accel_score"],
            }
            for e in emitted
        ]
        return emitted
    finally:
        TrendScoutAgent._fetch_all_sources = original_fetch  # type: ignore[method-assign]


async def run_risk_filter(session, discovered_events: list[dict], stats: PipelineStats) -> list[dict]:
    """Step 2: Run RiskFilter on each discovered candidate.

    In production, TrendScout emits candidate.discovered, then GapAnalyzer
    emits candidate.validated, then RiskFilter consumes it.
    For e2e test, we synthesize candidate.validated events directly.

    `discovered_events` is the raw emitted list from TrendScout (full event dicts).
    """
    from relay.intelligence.risk_filter import RiskFilterAgent

    cleared = []
    agent = RiskFilterAgent()

    for disc_event in discovered_events:
        candidate_id = disc_event["payload"]["candidate_id"]

        # Update candidate status to VALIDATED (GapAnalyzer would do this)
        from sqlalchemy import text
        await session.execute(
            text("UPDATE trend_candidates SET status = 'VALIDATED' WHERE id = :id"),
            {"id": candidate_id},
        )
        await session.commit()

        event = {
            "type": "candidate.validated",
            "payload": {"candidate_id": candidate_id},
            "idempotency_key": f"e2e:candidate:{candidate_id}:validated",
            "correlation_id": "e2e_test",
        }
        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "candidate.cleared":
                stats.risk_passed.append(candidate_id)
                cleared.append(e)
            elif e["type"] == "candidate.rejected":
                reason = e["payload"].get("reason", "unknown")
                stats.risk_rejected.append((candidate_id, reason))

    return cleared


async def run_source_matcher(session, cleared: list[dict], stats: PipelineStats) -> list[dict]:
    """Step 3: Run SourceMatcher on each cleared candidate.

    For e2e test, we mock the Rakuten search to return predictable items.
    """
    from relay.integrations.rakuten.client import RakutenItem
    from relay.listing.source_matcher import SourceMatcherAgent

    # Patch search to return mock items
    import relay.integrations.rakuten.client as rakuten_mod

    original_search = rakuten_mod.search

    async def mock_search(keyword: str, hits: int = 10):
        """Return 1-2 mock items for the search keyword."""
        return [
            RakutenItem(
                name=keyword or "Mock Product",
                item_code=f"mock-{hash(keyword) % 10000:04d}",
                price_jpy=3280,
                review_count=100,
                review_average=4.2,
                genre_id="558944",
                image_urls=["https://img.example.com/mock.jpg"],
                url=f"https://item.rakuten.co.jp/mock/{hash(keyword) % 10000:04d}/",
                seller_name="Mock Shop",
                seller_rating=4.5,
                stock_state="IN_STOCK",
                description="Mock product for e2e test",
            ),
        ]

    rakuten_mod.search = mock_search  # type: ignore[assignment]

    sourced = []
    agent = SourceMatcherAgent()

    try:
        for clr in cleared:
            candidate_id = clr["payload"]["candidate_id"]
            event = {
                "type": "candidate.cleared",
                "payload": {"candidate_id": candidate_id},
                "idempotency_key": f"e2e:candidate:{candidate_id}:sourced",
                "correlation_id": "e2e_test",
            }
            # Clear idempotency to allow re-processing
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
                    stats.sourced.append(e["payload"]["product_id"])
                    sourced.append(e)
    finally:
        rakuten_mod.search = original_search  # type: ignore[assignment]

    return sourced


async def run_pricing(session, sourced: list[dict], stats: PipelineStats) -> list[dict]:
    """Step 4: Run PricingAgent on each sourced product."""
    from relay.listing.pricing import PricingAgent

    priced = []
    agent = PricingAgent()

    for src in sourced:
        event = {
            "type": "product.sourced",
            "payload": src["payload"],
            "idempotency_key": f"e2e:product:{src['payload']['product_id']}:priced",
            "correlation_id": src["payload"].get("correlation_id", "e2e_test"),
        }
        # Clear idempotency to allow re-processing
        from sqlalchemy import text
        await session.execute(
            text("DELETE FROM processed_events WHERE idempotency_key = :k"),
            {"k": event["idempotency_key"]},
        )
        await session.commit()

        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "product.priced":
                stats.priced.append((
                    e["payload"]["product_id"],
                    e["payload"]["sell_price_krw"],
                    e["payload"]["margin_rate"],
                ))
                priced.append(e)
            elif e["type"] == "candidate.rejected":
                stats.failed.append((
                    "pricing",
                    src["payload"]["product_id"],
                    e["payload"].get("reason", "margin"),
                ))

    return priced


async def run_content(session, priced: list[dict], stats: PipelineStats) -> list[dict]:
    """Step 5: Run ContentAgent on each priced listing."""
    from relay.listing.content import ContentAgent

    content_ready = []
    agent = ContentAgent()

    for p in priced:
        event = {
            "type": "product.priced",
            "payload": p["payload"],
            "idempotency_key": f"e2e:listing:{p['payload']['listing_id']}:content",
            "correlation_id": p["payload"].get("correlation_id", "e2e_test"),
        }
        # Clear idempotency to allow re-processing
        from sqlalchemy import text
        await session.execute(
            text("DELETE FROM processed_events WHERE idempotency_key = :k"),
            {"k": event["idempotency_key"]},
        )
        await session.commit()

        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "listing.content_ready":
                stats.content_ready.append(e["payload"]["listing_id"])
                content_ready.append(e)
            elif e["type"] == "listing.failed":
                stats.failed.append((
                    "content",
                    p["payload"]["listing_id"],
                    e["payload"].get("reason", "unknown"),
                ))

    return content_ready


async def run_publisher(session, content_ready: list[dict], stats: PipelineStats) -> list[dict]:
    """Step 6: Run PublishAgent on each content-ready listing.

    In DRY_RUN, create_product returns mock data so we can verify the chain.
    """
    from relay.listing.publisher import PublishAgent

    published = []
    agent = PublishAgent()

    for cr in content_ready:
        listing_id = cr["payload"]["listing_id"]
        event = {
            "type": "listing.content_ready",
            "payload": {"listing_id": listing_id},
            "idempotency_key": f"e2e:listing:{listing_id}:published",
            "correlation_id": "e2e_test",
        }
        # Clear idempotency to allow re-processing
        from sqlalchemy import text
        await session.execute(
            text("DELETE FROM processed_events WHERE idempotency_key = :k"),
            {"k": event["idempotency_key"]},
        )
        await session.commit()

        emitted = await agent.handle(event, session)
        await session.commit()

        for e in emitted:
            if e["type"] == "listing.published":
                stats.published.append(e["payload"]["listing_id"])
                published.append(e)
            elif e["type"] == "listing.failed":
                stats.failed.append((
                    "publish",
                    listing_id,
                    e["payload"].get("reason", "unknown"),
                ))

    return published


async def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Full pipeline end-to-end dry-run test")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    from sqlalchemy import text
    from relay.core.db import AsyncSessionLocal

    stats = PipelineStats()

    print("=" * 70)
    print("  RELAY Full Pipeline E2E Dry-Run Test")
    print("=" * 70)
    print(f"  RELAY_DRY_RUN: {os.environ.get('RELAY_DRY_RUN', 'NOT SET')}")
    print(f"  Items to process: {len(MOCK_RAKUTEN_ITEMS)}")
    print()

    # ── Setup ─────────────────────────────────────────────────────────────────
    print("[Setup] Seeding config + FX rate...")
    async with AsyncSessionLocal() as session:
        await _seed_fx_rate(session)
        await _seed_pricing_config(session)
        # Clear previous test data
        await session.execute(text("DELETE FROM listings WHERE remote_product_id LIKE 'dry_run_e2e%'"))
        await session.execute(text("DELETE FROM products WHERE canonical_name_src LIKE 'TEST_%' OR canonical_name_src LIKE 'Mock %'"))
        await session.execute(text("DELETE FROM product_sources WHERE url LIKE '%example.com%' OR url LIKE '%mock%'"))
        await session.execute(text("DELETE FROM trend_candidates WHERE source = 'mock_rakuten_ranking'"))
        await session.execute(text("DELETE FROM processed_events WHERE idempotency_key LIKE 'e2e:%'"))
        await session.commit()
        print("       Done.")
        print()

    # ── Stage 1: TrendScout ───────────────────────────────────────────────────
    print("[1/6] TREND SCOUT — tick.trend_scan")
    async with AsyncSessionLocal() as session:
        discovered = await run_trend_scout(session, stats)
        print(f"       Discovered {len(discovered)} candidates above threshold:")
        if args.verbose:
            for d in discovered:
                p = d["payload"]
                print(f"         - #{p['candidate_id']} score={p['accel_score']:.3f} {p['name'][:50]}")
        print()

    # ── Stage 2: RiskFilter ───────────────────────────────────────────────────
    print("[2/6] RISK FILTER — candidate.validated")
    async with AsyncSessionLocal() as session:
        cleared = await run_risk_filter(session, discovered, stats)
        print(f"       Passed: {len(cleared)}, Rejected: {len(stats.risk_rejected)}")
        if args.verbose and stats.risk_rejected:
            for cid, reason in stats.risk_rejected:
                print(f"         - #{cid} rejected: {reason}")
        print()

    if not cleared:
        print("       No candidates cleared RiskFilter — pipeline stops here.")
        print("       This is expected if all candidates are blocked.")
        print()
        print("=" * 70)
        print("  Pipeline halted at RiskFilter stage.")
        print("=" * 70)
        return

    # ── Stage 3: SourceMatcher ────────────────────────────────────────────────
    print("[3/6] SOURCE MATCHER — candidate.cleared")
    async with AsyncSessionLocal() as session:
        sourced = await run_source_matcher(session, cleared, stats)
        print(f"       Sourced {len(sourced)} new products:")
        if args.verbose:
            for s in sourced:
                p = s["payload"]
                print(f"         - product_id={p['product_id']} candidate={p.get('candidate_id', 'N/A')}")
        print()

    if not sourced:
        print("       No products sourced — pipeline stops here.")
        print()
        print("=" * 70)
        print("  Pipeline halted at SourceMatcher stage.")
        print("=" * 70)
        return

    # ── Stage 4: PricingAgent ─────────────────────────────────────────────────
    print("[4/6] PRICING AGENT — product.sourced")
    async with AsyncSessionLocal() as session:
        priced = await run_pricing(session, sourced, stats)
        print(f"       Priced {len(priced)} listings:")
        if args.verbose:
            for p in priced:
                payload = p["payload"]
                print(f"         - listing_id={payload['listing_id']} price=₩{payload['sell_price_krw']:,} margin={payload['margin_rate']:.1%}")
        print()

    if not priced:
        print("       No products priced (all rejected on margin/ceiling) — pipeline stops.")
        print()
        print("=" * 70)
        print("  Pipeline halted at PricingAgent stage.")
        print("=" * 70)
        return

    # ── Stage 5: ContentAgent ─────────────────────────────────────────────────
    print("[5/6] CONTENT AGENT — product.priced")
    async with AsyncSessionLocal() as session:
        content_ready = await run_content(session, priced, stats)
        print(f"       Content generated for {len(content_ready)} listings:")
        if args.verbose:
            # Check content in DB
            for cr in content_ready:
                lid = cr["payload"]["listing_id"]
                row = await session.execute(
                    text("SELECT title, content->>'category_naver' FROM listings WHERE id = :id"),
                    {"id": lid},
                )
                title, cat = row.first()  # type: ignore[misc]
                print(f"         - listing_id={lid} cat={cat}")
                print(f"           title: {title}")
        print()

    if not content_ready:
        print("       No content generated — pipeline stops.")
        print()
        print("=" * 70)
        print("  Pipeline halted at ContentAgent stage.")
        print("=" * 70)
        return

    # ── Stage 6: PublishAgent ─────────────────────────────────────────────────
    print("[6/6] PUBLISH AGENT — listing.content_ready")
    async with AsyncSessionLocal() as session:
        published = await run_publisher(session, content_ready, stats)
        print(f"       Published {len(published)} listings:")
        if args.verbose:
            for pub in published:
                p = pub["payload"]
                print(f"         - listing_id={p['listing_id']} remote_id={p.get('remote_product_id', 'N/A')}")
        print()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("=" * 70)
    print("  PIPELINE E2E SUMMARY")
    print("=" * 70)
    print(stats.summary())
    print()

    if stats.published:
        print("  ✅ FULL PIPELINE COMPLETE — all 6 stages passed!")
        print("     TrendScout → RiskFilter → SourceMatcher → PricingAgent → ContentAgent → PublishAgent")
    elif stats.failed:
        print("  ⚠️  Pipeline completed with failures:")
        for stage, obj_id, reason in stats.failed:
            print(f"     - [{stage}] id={obj_id}: {reason}")

    print()
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
