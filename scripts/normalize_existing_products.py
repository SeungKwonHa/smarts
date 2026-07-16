"""One-shot script: re-normalize existing products with working LongCat LLM.

Processes products one at a time for reliable LLM responses.

Usage:
    RELAY_DRY_RUN=0 python scripts/normalize_existing_products.py
"""

from __future__ import annotations

import asyncio
import json
import sys

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from relay.core.config import settings
from relay.core.llm.client import client as llm

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(20),
)
log = structlog.get_logger("normalize_script")

VALID_CATS = {
    "kitchen", "stationery", "hobby", "camping", "beauty",
    "electronics", "home", "apparel", "food", "other",
}


async def normalize_one(pid: int, name_src: str, session_maker) -> bool:
    """Normalize a single product. Returns True on success."""
    system = (
        "Normalize Japanese e-commerce product to Korean. "
        "JSON: {\"canonical_name_ko\":\"Korean name\",\"brand\":\"brand or empty\","
        "\"category_internal\":\"kitchen|stationery|hobby|camping|beauty|electronics|home|apparel|food|other\","
        "\"weight_g\":0,\"material\":\"or empty\",\"color_options\":[],\"size_options\":[]}"
    )
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": name_src},
    ]

    for attempt in range(3):
        try:
            resp = await llm.complete(
                task_name="l1.variant_normalize",
                messages=messages,
                agent="normalize_script",
            )
            raw = resp.content
            if not isinstance(raw, dict) or "_dry_run" in raw:
                return False

            # Some responses may have products wrapper or be direct
            result = raw.get("products", [raw])[0] if "products" in raw else raw
            if not result or not isinstance(result, dict):
                await asyncio.sleep(1)
                continue

            name_ko = str(result.get("canonical_name_ko", ""))[:300]
            brand = str(result.get("brand", ""))[:100]
            category = result.get("category_internal", "other")
            if category not in VALID_CATS:
                category = "other"
            weight = int(result.get("weight_g", 0))
            material = str(result.get("material", ""))[:100]
            colors = result.get("color_options", []) or []
            sizes = result.get("size_options", []) or []

            attrs = {
                "weight_g": weight,
                "material": material,
                "color_options": colors,
                "size_options": sizes,
            }

            async with session_maker() as session:
                await session.execute(
                    text(
                        "UPDATE products SET canonical_name_ko = :name_ko, "
                        "brand = :brand, category_internal = :cat, "
                        "attributes = CAST(:attrs AS JSONB) "
                        "WHERE id = :id"
                    ),
                    {
                        "id": pid,
                        "name_ko": name_ko,
                        "brand": brand,
                        "cat": category,
                        "attrs": json.dumps(attrs),
                    },
                )
                await session.commit()
            return True

        except Exception as e:
            log.debug("attempt_failed", pid=pid, attempt=attempt, error=str(e))
            await asyncio.sleep(1)

    return False


async def main():
    if settings.relay_dry_run:
        print("[WARN] DRY_RUN=1 -- set RELAY_DRY_RUN=0 to actually call LLM")
        sys.exit(1)

    if not settings.llm_configured:
        print("[ERROR] LLM not configured in .env")
        sys.exit(1)

    engine = create_async_engine(settings.database_url)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        result = await conn.execute(
            text(
                "SELECT id, canonical_name_src FROM products "
                "WHERE category_internal = 'other' OR brand = '' "
                "ORDER BY id"
            )
        )
        products = [(row.id, row.canonical_name_src) for row in result.fetchall()]

    if not products:
        print("[OK] No products need normalization")
        await engine.dispose()
        return

    print(f"[INFO] Normalizing {len(products)} products...")

    ok = 0
    fail = 0
    for i, (pid, name_src) in enumerate(products):
        success = await normalize_one(pid, name_src, session_maker)
        if success:
            ok += 1
            mark = "OK"
        else:
            fail += 1
            mark = "FAIL"
        print(f"  [{mark}] {i+1}/{len(products)} id={pid}: {name_src[:40]}")
        # Rate limit spacing
        await asyncio.sleep(1.5)

    print(f"\n[DONE] {ok} ok, {fail} fail out of {len(products)}")

    async with engine.begin() as conn:
        cats = await conn.execute(
            text(
                "SELECT category_internal, COUNT(*) FROM products "
                "GROUP BY category_internal ORDER BY COUNT(*) DESC"
            )
        )
        print("\nCategory distribution:")
        for c in cats:
            print(f"  {c[0]}: {c[1]}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
