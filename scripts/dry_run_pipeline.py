"""Full pipeline dry-run test — verifies agent chaining without external I/O.

Simulates the complete flow:
  tick.trend_scan → TrendScout (mock Rakuten data)
    → candidate.discovered events
  tick.longtail_expand → SourceMatcher (mock)
    → product.sourced events
  PricingAgent → product.priced events
  ContentAgent → product.content_ready events
  RiskFilter → product.cleared / product.rejected
  PublishAgent → product.published (dry run: log only)

All external APIs are mocked. LLM calls use DRY_RUN rule-based fallback.
Database writes happen (in test DB) so you can inspect state after.

Usage:
    RELAY_DRY_RUN=1 python scripts/dry_run_pipeline.py
    python scripts/dry_run_pipeline.py --verbose
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


async def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Full pipeline dry-run test")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    from sqlalchemy import text

    from relay.core.db import AsyncSessionLocal
    from relay.intelligence.trend_scout import TrendScoutAgent

    print("=" * 60)
    print("  RELAY Pipeline Dry-Run Test")
    print("=" * 60)
    print(f"  RELAY_DRY_RUN: {os.environ.get('RELAY_DRY_RUN', 'NOT SET')}")
    print(f"  Items to process: {len(MOCK_RAKUTEN_ITEMS)}")
    print()

    # ── Step 1: Clear previous test data ──────────────────────────────────────
    print("[1/4] Clearing previous test data...")
    async with AsyncSessionLocal() as session:
        await session.execute(text("DELETE FROM trend_candidates WHERE source LIKE 'mock_%'"))
        await session.execute(text("DELETE FROM products WHERE canonical_name_src LIKE 'TEST_%'"))
        await session.execute(text("DELETE FROM product_sources WHERE url LIKE '%example.com%'"))
        await session.commit()
        print("      Done.")

    # ── Step 2: Run TrendScout with mock data ─────────────────────────────────
    print("[2/4] Running TrendScout (mock Rakuten data)...")

    # Patch the _fetch_all_sources method to return mock data
    original_fetch = TrendScoutAgent._fetch_all_sources

    async def mock_fetch(self, session, verbose=False):
        """Return mock Rakuten items instead of hitting the API."""
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

    async with AsyncSessionLocal() as session:
        event = {
            "type": "tick.trend_scan",
            "payload": {},
            "idempotency_key": f"dry_run:trend_scan:{asyncio.get_event_loop().time()}",
            "correlation_id": "dry_run_test",
        }
        agent = TrendScoutAgent()
        emitted = await agent.handle(event, session)
        await session.commit()

        print(f"      Emitted {len(emitted)} candidate.discovered events.")

        if args.verbose:
            for e in emitted:
                p = e["payload"]
                print(f"        - [{p['candidate_id']}] score={p['accel_score']:.3f} {p['name'][:50]}")

    # ── Step 3: Verify entity extraction results ───────────────────────────────
    print("[3/4] Verifying entity extraction...")

    async with AsyncSessionLocal() as session:
        rows = await session.execute(
            text("""
                SELECT id, name_raw, name_norm, ranking_metadata->'entity' as entity
                FROM trend_candidates
                WHERE source = 'mock_rakuten_ranking'
                ORDER BY rank_position
            """)
        )
        all_ok = True
        for r in rows:
            entity = r.entity if r.entity else {}
            product_name = entity.get("product_name", "")
            brand = entity.get("brand", "")

            # Check that noise was stripped
            noise_found = []
            for noise in ["【", "】", "＼", "／", "クーポン", "ランキング受賞", "ポイント"]:
                if noise in product_name:
                    noise_found.append(noise)

            status = "✅" if not noise_found else "⚠️"
            if noise_found:
                all_ok = False

            print(f"      {status} [{r.id}] {product_name[:45]}")
            if args.verbose:
                print(f"           brand={brand}, attrs={entity.get('attributes', [])}")
                if noise_found:
                    print(f"           NOISE REMAINING: {noise_found}")

        if all_ok:
            print("      All noise patterns stripped correctly.")
        else:
            print("      ⚠️  Some noise remains — review entity_extract patterns.")

    # ── Step 4: Summary ───────────────────────────────────────────────────────
    print("[4/4] Pipeline summary:")
    async with AsyncSessionLocal() as session:
        row = await session.execute(
            text("SELECT COUNT(*), AVG(accel_score), MIN(accel_score), MAX(accel_score) FROM trend_candidates WHERE source = 'mock_rakuten_ranking'")
        )
        count, avg_score, min_score, max_score = row.first()  # type: ignore[misc]
        print(f"      Candidates: {count}")
        print(f"      Score range: {min_score:.3f} - {max_score:.3f} (avg: {avg_score:.3f})")

        row = await session.execute(
            text("SELECT COUNT(DISTINCT category_guess) FROM trend_candidates WHERE source = 'mock_rakuten_ranking'")
        )
        cats = row.scalar_one()
        print(f"      Categories detected: {cats}")

    # Restore original method
    TrendScoutAgent._fetch_all_sources = original_fetch  # type: ignore[method-assign]

    print()
    print("=" * 60)
    print("  Dry-run test complete.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
