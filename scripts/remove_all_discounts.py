"""Emergency script: remove all fake discounts from Naver products.

Naver sent an abuse warning because our customerBenefit.immediateDiscountPolicy
triggered their dark-pattern detection. This script removes ALL discounts.

Can run in two modes:
  1. With DB: reads LIVE listings from database (default)
  2. Without DB: uses hardcoded product IDs (--no-db flag)

Usage:
    python -m scripts.remove_all_discounts            # DB mode
    python -m scripts.remove_all_discounts --no-db    # No-DB mode (uses known IDs)
"""

import asyncio
import sys

from relay.core.config import settings
from relay.integrations.naver.client import remove_discount

# Known Naver channel product IDs (from previous publishing sessions)
# Used as fallback when DB is not available
KNOWN_PRODUCT_IDS = [
    "13605038404",
    "13605041327",
    "13670537009",
]


async def remove_with_db():
    """Remove discounts using database to find products."""
    from sqlalchemy import text
    from relay.core.db import get_session

    async with get_session() as session:
        rows = await session.execute(
            text("""
                SELECT id, title, remote_product_id
                FROM listings
                WHERE status = 'LIVE'
                  AND remote_product_id IS NOT NULL
                  AND remote_product_id != ''
                ORDER BY id
            """)
        )
        products = rows.all()

        if not products:
            print("No LIVE products found in database.")
            return

        print(f"Found {len(products)} LIVE products:")
        for listing_id, title, remote_id in products:
            print(f"  Listing #{listing_id}: {title[:50]} → Naver #{remote_id}")
        print()

        success_count = 0
        fail_count = 0
        for listing_id, title, remote_id in products:
            print(f"Removing discount from Listing #{listing_id} ({title[:40]})...", end=" ")
            try:
                result = await remove_discount(remote_id)
                print("✅ DONE" if result else "❌ FAILED")
                if result:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                print(f"❌ ERROR: {e}")
                fail_count += 1

        return success_count, fail_count


async def remove_without_db():
    """Remove discounts using known product IDs."""
    print(f"Using {len(KNOWN_PRODUCT_IDS)} known Naver product IDs:")
    for pid in KNOWN_PRODUCT_IDS:
        print(f"  Naver #{pid}")
    print()

    success_count = 0
    fail_count = 0
    for pid in KNOWN_PRODUCT_IDS:
        print(f"Removing discount from Naver #{pid}...", end=" ")
        try:
            result = await remove_discount(pid)
            print("✅ DONE" if result else "❌ FAILED")
            if result:
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            print(f"❌ ERROR: {e}")
            fail_count += 1

    return success_count, fail_count


async def main():
    use_db = "--no-db" not in sys.argv

    print("=" * 60)
    print("EMERGENCY: Remove all fake Naver discounts")
    print("=" * 60)
    print(f"Environment: {settings.relay_env}")
    print(f"Dry run: {settings.relay_dry_run}")
    print(f"Naver configured: {bool(settings.naver_client_id)}")
    print(f"Mode: {'DB' if use_db else 'KNOWN IDs (no DB)'}")
    print()

    if settings.relay_dry_run:
        print("⚠️  DRY RUN MODE — no actual API calls will be made.")
        print("   Set RELAY_DRY_RUN=0 in .env to actually remove discounts.")
        return

    if use_db:
        try:
            success_count, fail_count = await remove_with_db()
        except Exception as e:
            print(f"DB connection failed: {e}")
            print(f"Falling back to known product IDs...")
            print()
            success_count, fail_count = await remove_without_db()
    else:
        success_count, fail_count = await remove_without_db()

    print()
    print(f"Result: {success_count} removed, {fail_count} failed")

    if fail_count == 0:
        print("\n🎉 All discounts removed successfully!")
        print("Naver abuse warning should no longer apply.")
    else:
        print(f"\n⚠️  {fail_count} products may still have discounts. Check errors above.")


if __name__ == "__main__":
    asyncio.run(main())
