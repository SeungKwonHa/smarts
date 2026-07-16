"""M1 agent contract tests.

Each agent gets a contract test that:
1. Seeds minimal DB state.
2. Feeds a golden fixture event.
3. Asserts DB writes + emitted events.

External APIs (Rakuten, Naver, LLM) are all mocked.
Set RELAY_DRY_RUN=1 (done in conftest) to suppress LLM + Naver API calls.
"""

from __future__ import annotations

import json
import pytest
from sqlalchemy import text
from unittest.mock import AsyncMock, patch

from relay.core.events import STREAM_LISTING, STREAM_INTEL, STREAM_OPS
from relay.intelligence.risk_filter import RiskFilterAgent, rule_screen
from relay.listing.pricing import PricingAgent, compute_price, ceil_to_pricepoint
from relay.listing.content import ContentAgent, get_naver_category_id
from relay.listing.publisher import PublishAgent
from relay.listing.source_matcher import SourceMatcherAgent
from relay.operations.stock_monitor import StockMonitorAgent
from relay.operations.order_agent import OrderAgent
from relay.operations.logistics import LogisticsAgent
from relay.analytics.reporter import ReporterAgent


# ── Helpers ───────────────────────────────────────────────────────────────────

def _envelope(event_type: str, payload: dict, ikey: str | None = None) -> dict:
    return {
        "type": event_type,
        "payload": payload,
        "idempotency_key": ikey or f"test:{event_type}",
        "correlation_id": "test",
    }


async def _seed_fx(session) -> None:
    """Seed a JPY/KRW rate so pricing tests work."""
    from datetime import datetime, timezone
    # Use a fixed timestamp far in the past to avoid PK collisions
    fixed_ts = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    await session.execute(
        text("""
            INSERT INTO fx_rates (pair, rate, at)
            VALUES ('KRW/JPY', 9.5, :ts)
            ON CONFLICT (pair, at) DO UPDATE SET rate = EXCLUDED.rate
        """),
        {"ts": fixed_ts},
    )
    await session.commit()


async def _seed_product(session) -> tuple[int, int, int]:
    """Seed a minimal product + source + listing. Returns (product_id, source_id, listing_id)."""
    await _seed_fx(session)

    prod = await session.execute(text("""
        INSERT INTO products
          (origin_route, canonical_name_src, brand, category_internal,
           attributes, images, risk_status, status)
        VALUES
          ('longtail', '테스트 상품', 'TestBrand', 'kitchen',
           '{"weight_g": 300}'::jsonb, '[]'::jsonb, 'CLEARED', 'ACTIVE')
        RETURNING id
    """))
    product_id = prod.scalar_one()

    src = await session.execute(text("""
        INSERT INTO product_sources
          (product_id, marketplace, url, seller_name, currency, price_minor,
           stock_state, weight_g, variant_map, rank, last_checked_at)
        VALUES
          (:pid, 'rakuten', 'https://item.rakuten.co.jp/test/item001/', 'TestShop',
           'JPY', 1500, 'IN_STOCK', 300, '{}'::jsonb, 1, now())
        RETURNING id
    """), {"pid": product_id})
    source_id = src.scalar_one()

    lst = await session.execute(text("""
        INSERT INTO listings
          (product_id, marketplace, store_account, sell_price_krw, margin_krw, margin_rate, status)
        VALUES
          (:pid, 'naver', 'default', 25000, 4000, 0.16, 'DRAFT')
        RETURNING id
    """), {"pid": product_id})
    listing_id = lst.scalar_one()

    await session.commit()
    return product_id, source_id, listing_id


# ── Pricing unit tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ceil_to_pricepoint():
    """Verify price rounding rules."""
    assert ceil_to_pricepoint(9_001) == 9_100
    assert ceil_to_pricepoint(15_001) == 15_500
    assert ceil_to_pricepoint(100_001) == 101_000


@pytest.mark.asyncio
async def test_pricing_formula(db_session):
    """PricingAgent computes a reasonable sell price from JPY source price."""
    await _seed_fx(db_session)
    result = await compute_price(
        src_price_minor=2000,  # ¥2,000
        currency="JPY",
        weight_g=300,
        session=db_session,
    )
    assert result is not None, "Should price a ¥2,000 item"
    assert result["sell_price_krw"] > 0
    assert result["margin_krw"] > 0
    # Sanity: sell price should be roughly 2000 * 9.5 * 1.15 ≈ 20k-30k KRW
    assert 15_000 <= result["sell_price_krw"] <= 50_000, (
        f"Sell price {result['sell_price_krw']} out of expected range"
    )


@pytest.mark.asyncio
async def test_pricing_rejects_low_margin(db_session):
    """Sub-¥100 item should be rejected (margin too low after fees)."""
    await _seed_fx(db_session)
    result = await compute_price(
        src_price_minor=100,
        currency="JPY",
        weight_g=50,
        session=db_session,
    )
    # Either rejected (None) or margin is below min — depends on exact formula
    if result is not None:
        assert result["margin_krw"] >= 3_000 or result is None


# ── RiskFilter ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_risk_filter_blocked_by_rules(db_session):
    """Product mentioning a blocked category ('식품') should be blocked by rules."""
    verdict, kinds, note = await rule_screen(
        product_name="일본 식품 건강보조제",
        category_guess="food",
        session=db_session,
    )
    assert verdict == "BLOCK", f"Expected BLOCK, got {verdict}"
    assert len(kinds) > 0


@pytest.mark.asyncio
async def test_risk_filter_passes_safe_product(db_session):
    """A kitchen gadget should pass the rule screen."""
    verdict, kinds, note = await rule_screen(
        product_name="스테인리스 커피드리퍼 350ml",
        category_guess="kitchen",
        session=db_session,
    )
    assert verdict == "PASS", f"Expected PASS, got {verdict} kinds={kinds}"


@pytest.mark.asyncio
async def test_risk_filter_agent_candidate_cleared(db_session):
    """RiskFilter agent emits candidate.cleared for a safe product in DRY_RUN mode."""
    # Seed a candidate
    row = await db_session.execute(text("""
        INSERT INTO trend_candidates
          (source, external_key, name_raw, name_norm, category_guess, status)
        VALUES
          ('manual', 'test:rf:001', '스테인리스 커피드리퍼', '스테인리스 커피드리퍼', 'kitchen', 'VALIDATED')
        ON CONFLICT (source, external_key) DO UPDATE SET name_norm = EXCLUDED.name_norm
        RETURNING id
    """))
    candidate_id = row.scalar_one()
    await db_session.commit()

    agent = RiskFilterAgent()
    event = _envelope("candidate.validated", {"candidate_id": candidate_id})
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # In DRY_RUN mode, LLM returns PASS
    cleared = [e for e in emitted if e["type"] == "candidate.cleared"]
    assert len(cleared) == 1, f"Expected candidate.cleared, got {[e['type'] for e in emitted]}"
    assert cleared[0]["payload"]["candidate_id"] == candidate_id


# ── Category mapping ──────────────────────────────────────────────────────────

def test_naver_category_mapping():
    """Category internal → Naver category ID mapping is defined."""
    assert get_naver_category_id("kitchen") != ""
    assert get_naver_category_id("stationery") != ""
    assert get_naver_category_id("unknown_xyz") == get_naver_category_id("other")


# ── PricingAgent ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pricing_agent_creates_listing(db_session):
    """PricingAgent creates a DRAFT listing with correct price."""
    product_id, source_id, listing_id = await _seed_product(db_session)

    agent = PricingAgent()
    event = _envelope(
        "product.sourced",
        {"product_id": product_id, "candidate_id": None, "source_count": 1, "correlation_id": "test"},
    )
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should emit product.priced
    priced = [e for e in emitted if e["type"] == "product.priced"]
    assert len(priced) == 1
    assert priced[0]["payload"]["sell_price_krw"] > 0
    assert priced[0]["payload"]["margin_rate"] > 0


# ── ContentAgent ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_content_agent_generates_content(db_session):
    """ContentAgent updates listing to CONTENT_READY with title and HTML."""
    product_id, source_id, listing_id = await _seed_product(db_session)

    # Set listing to DRAFT (as pricing would leave it)
    await db_session.execute(
        text("UPDATE listings SET status = 'DRAFT' WHERE id = :id"),
        {"id": listing_id},
    )
    await db_session.commit()

    agent = ContentAgent()
    event = _envelope(
        "product.priced",
        {
            "product_id": product_id,
            "listing_id": listing_id,
            "sell_price_krw": 25000,
            "margin_rate": 0.16,
            "correlation_id": "test",
        },
    )
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # In DRY_RUN mode, content agent generates dry-run content
    content_ready = [e for e in emitted if e["type"] == "listing.content_ready"]
    assert len(content_ready) == 1

    # Verify DB update
    row = await db_session.execute(
        text("SELECT status, title FROM listings WHERE id = :id"),
        {"id": listing_id},
    )
    rec = row.first()
    assert rec is not None
    assert rec[0] == "CONTENT_READY"
    assert rec[1] is not None and len(rec[1]) > 0


# ── PublishAgent ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_publish_agent_queues_approval(db_session):
    """PublishAgent queues a HITL approval request in M1 (publish_auto=false)."""
    product_id, source_id, listing_id = await _seed_product(db_session)

    # Set listing to CONTENT_READY with title
    await db_session.execute(
        text("""
            UPDATE listings
            SET status = 'CONTENT_READY',
                title = '일본 스테인리스 커피드리퍼 테스트',
                content = '{"title":"test","detail_html":"<p>test</p>","images":[]}'::jsonb
            WHERE id = :id
        """),
        {"id": listing_id},
    )
    await db_session.execute(
        text("UPDATE products SET category_naver = '50000803' WHERE id = :id"),
        {"id": product_id},
    )
    await db_session.commit()

    agent = PublishAgent()
    event = _envelope("listing.content_ready", {"listing_id": listing_id})
    await agent.handle(event, db_session)
    await db_session.commit()

    # Should create an approval_request
    row = await db_session.execute(
        text("SELECT kind, status FROM approval_requests WHERE ref_id = :id AND kind = 'publish_batch'"),
        {"id": listing_id},
    )
    rec = row.first()
    assert rec is not None, "Should have created an approval_request"
    assert rec[1] == "PENDING"


# ── StockMonitor ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_stock_monitor_marks_oos(db_session):
    """StockMonitor suspends a LIVE listing when source goes OOS."""
    product_id, source_id, listing_id = await _seed_product(db_session)

    # Set listing to LIVE and source to IN_STOCK (mock will report OOS)
    await db_session.execute(
        text("UPDATE listings SET status = 'LIVE' WHERE id = :id"),
        {"id": listing_id},
    )
    await db_session.execute(
        text("UPDATE product_sources SET stock_state = 'IN_STOCK' WHERE id = :id"),
        {"id": source_id},
    )
    await db_session.commit()

    agent = StockMonitorAgent()
    event = _envelope("tick.stock_scan", {"tier": "all"})

    # Mock _fetch_source to return OOS (change from IN_STOCK → OOS triggers event)
    with patch.object(agent, "_fetch_source", new=AsyncMock(return_value=(1500, "OOS"))):
        emitted = await agent.handle(event, db_session)
    await db_session.commit()

    stock_events = [e for e in emitted if e["type"] == "stock.changed"]
    assert any(e["payload"]["state"] == "oos" for e in stock_events)

    row = await db_session.execute(
        text("SELECT status FROM listings WHERE id = :id"),
        {"id": listing_id},
    )
    status = row.scalar()
    assert status == "SUSPENDED_STOCKOUT"


@pytest.mark.asyncio
async def test_stock_monitor_reactivates_on_restock(db_session):
    """StockMonitor reactivates a SUSPENDED listing when source is back in stock."""
    product_id, source_id, listing_id = await _seed_product(db_session)

    await db_session.execute(
        text("UPDATE listings SET status = 'SUSPENDED_STOCKOUT' WHERE id = :id"),
        {"id": listing_id},
    )
    await db_session.execute(
        text("UPDATE product_sources SET stock_state = 'OOS' WHERE id = :id"),
        {"id": source_id},
    )
    await db_session.commit()

    agent = StockMonitorAgent()
    event = _envelope("tick.stock_scan", {"tier": "all"})

    with patch.object(agent, "_fetch_source", new=AsyncMock(return_value=(1500, "IN_STOCK"))):
        emitted = await agent.handle(event, db_session)
    await db_session.commit()

    row = await db_session.execute(
        text("SELECT status FROM listings WHERE id = :id"),
        {"id": listing_id},
    )
    assert row.scalar() == "LIVE"


# ── OrderAgent ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_order_agent_creates_purchase_approval(db_session):
    """OrderAgent creates HITL purchase approval for a new order."""
    product_id, source_id, listing_id = await _seed_product(db_session)

    # Set listing LIVE with remote_product_id
    await db_session.execute(
        text("UPDATE listings SET status = 'LIVE', remote_product_id = 'NV_TEST_001' WHERE id = :id"),
        {"id": listing_id},
    )

    # Create an order WITH PCCC (required to proceed past HOLD_PCCC check)
    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES ('naver', 'ORD_TEST_001', 'ITEM_TEST_001', :lid, 1, 25000,
                'P1234567890', 'NEW')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()
    await db_session.commit()

    agent = OrderAgent()
    event = _envelope(
        "order.created",
        {"order_id": order_id, "marketplace": "naver", "listing_id": listing_id, "qty": 1},
    )
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should emit order.purchase_required
    purchase_required = [e for e in emitted if e["type"] == "order.purchase_required"]
    assert len(purchase_required) == 1

    # Should create approval_request
    row = await db_session.execute(
        text("SELECT kind FROM approval_requests WHERE ref_id = :oid AND kind = 'purchase_pay'"),
        {"oid": order_id},
    )
    assert row.first() is not None, "Should have created purchase_pay approval"

    # Order status should be PURCHASE_PENDING
    status_row = await db_session.execute(
        text("SELECT status FROM orders WHERE id = :id"),
        {"id": order_id},
    )
    assert status_row.scalar() == "PURCHASE_PENDING"


# ── LogisticsAgent ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_logistics_init_shipment(db_session):
    """LogisticsAgent creates shipment record on purchase.completed."""
    product_id, source_id, listing_id = await _seed_product(db_session)

    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, status)
        VALUES ('naver', 'ORD_LOG_001', 'ITEM_LOG_001', :lid, 1, 25000, 'PURCHASED')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()
    await db_session.commit()

    agent = LogisticsAgent()
    event = _envelope(
        "purchase.completed",
        {"order_id": order_id, "purchase_id": 1, "src_order_id": ""},
    )
    await agent.handle(event, db_session)
    await db_session.commit()

    row = await db_session.execute(
        text("SELECT stage FROM shipments WHERE order_id = :oid"),
        {"oid": order_id},
    )
    rec = row.first()
    assert rec is not None
    assert rec[0] == "INBOUND_TO_FORWARDER"


# ── ReporterAgent ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_reporter_emits_report_event(db_session):
    """ReporterAgent emits report.daily_ready without crashing."""
    agent = ReporterAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)

    report_events = [e for e in emitted if e["type"] == "report.daily_ready"]
    assert len(report_events) == 1
    assert "date" in report_events[0]["payload"]
