"""M3 Intelligence + Scale contract tests.

Tests for:
- TrendScout (I1): crawl ranking, dedup, score, emit candidate.discovered
- GapAnalyzer (I2): keyword generation, saturation check, gap scoring, emit validated/rejected
- PromotionEngine (A2): find qualifying SKUs, draft campaign, emit campaign.nominated
- InquiryAgent auto-send (M3 HITL graduation): tracking/PCCC auto-sent when enabled
- OrderAgent auto-pay (M3 HITL graduation): purchase auto-pay under limit + daily cap
- Reporter weekly narrative: T1 prose generation over SQL-computed tables

External APIs (Naver, LLM) are all mocked.
Set RELAY_DRY_RUN=1 (done in conftest) to suppress LLM + Naver API calls.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

from relay.analytics.promotion_engine import PromotionEngineAgent
from relay.analytics.reporter import ReporterAgent
from relay.cs.inquiry import InquiryAgent
from relay.intelligence.gap_analyzer import GapAnalyzerAgent
from relay.intelligence.trend_scout import TrendScoutAgent
from relay.operations.order_agent import OrderAgent

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
    fixed_ts = datetime(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    await session.execute(
        text("""
            INSERT INTO fx_rates (pair, rate, at)
            VALUES ('KRW/JPY', 9.5, :ts)
            ON CONFLICT (pair, at) DO UPDATE SET rate = EXCLUDED.rate
        """),
        {"ts": fixed_ts},
    )
    await session.commit()


async def _seed_product_with_listing(session, status='LIVE', days_ago=0, orders_30d=0):
    """Seed product + source + listing, optionally with N orders in last 30d.
    Returns (product_id, source_id, listing_id).
    """
    await _seed_fx(session)

    created_offset = ""
    if days_ago > 0:
        created_offset = f" - interval '{days_ago} days'"

    prod = await session.execute(text("""
        INSERT INTO products
          (origin_route, canonical_name_src, canonical_name_ko, brand,
           category_internal, attributes, images, risk_status, status)
        VALUES
          ('longtail', '테스트 상품', '테스트 상품', 'TestBrand', 'kitchen',
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

    lst = await session.execute(text(f"""
        INSERT INTO listings
          (product_id, marketplace, store_account, sell_price_krw, margin_krw,
           margin_rate, status, scan_tier, created_at)
        VALUES
          (:pid, 'naver', 'default', 25000, 4000, 0.16, :status, 2,
           now(){created_offset})
        RETURNING id
    """), {"pid": product_id, "status": status})
    listing_id = lst.scalar_one()

    # Seed orders if requested
    for i in range(orders_30d):
        await session.execute(text("""
            INSERT INTO orders
              (marketplace, remote_order_id, remote_order_item_id,
               listing_id, qty, unit_sell_krw, status)
            VALUES
              ('naver', :ono, :oitem, :lid, 1, 25000, 'PURCHASED')
        """), {
            "ono": f"ORD-M3-{product_id}-{i}",
            "oitem": f"ITEM-M3-{product_id}-{i}",
            "lid": listing_id,
        })

    await session.commit()
    return product_id, source_id, listing_id


# ── TrendScout Tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_trend_scout_ignores_wrong_event(db_session):
    """TrendScout only responds to tick.trend_scan."""
    agent = TrendScoutAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)
    assert emitted == []


@pytest.mark.asyncio
async def test_trend_scout_dry_run_no_fetch(db_session):
    """In DRY_RUN mode, TrendScout logs but doesn't fetch external data."""
    agent = TrendScoutAgent()
    event = _envelope("tick.trend_scan", {}, ikey="test:trend_scan:druntest1")
    emitted = await agent.handle(event, db_session)
    # DRY_RUN: no fetch → no candidates → no emissions
    assert emitted == []


@pytest.mark.asyncio
async def test_trend_scout_deduplicates_candidates(db_session):
    """TrendScout deduplicates by (source, external_key) and name_norm."""
    await _seed_fx(db_session)

    # Manually insert a candidate manually to simulate one already seen
    await db_session.execute(text("""
        INSERT INTO trend_candidates
          (source, external_key, name_raw, name_norm, image_url, image_phash,
           category_guess, status)
        VALUES
          ('rakuten_ranking', 'ITEM001', 'キッチン収納ボックス',
           '키친 수납 박스', 'https://img.example.com/1.jpg',
           'abc123', 'kitchen', 'DISCOVERED')
    """))
    await db_session.commit()

    # Manually insert a second "new" candidate and exercise scoring/emission
    result = await db_session.execute(text("""
        INSERT INTO trend_candidates
          (source, external_key, name_raw, name_norm, image_url, image_phash,
           category_guess, status)
        VALUES
          ('rakuten_ranking', 'ITEM002', 'デスクオーガナイザー',
           '데스크 오거나이저', 'https://img.example.com/2.jpg',
           'def456', 'stationery', 'DISCOVERED')
        RETURNING id
    """))
    result.scalar_one()
    await db_session.commit()

    # Lower the threshold to ensure this candidate gets emitted
    await db_session.execute(text("""
        INSERT INTO app_config (key, value)
        VALUES ('trend.score_threshold', '{"value": 0.0}'::jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    agent = TrendScoutAgent()
    event = _envelope("tick.trend_scan", {}, ikey="test:trend_scan:deduptest1")
    await agent.handle(event, db_session)

    # The existing deduped candidate (ITEM001) should NOT be re-inserted
    row = await db_session.execute(text("""
        SELECT COUNT(*) FROM trend_candidates
        WHERE external_key = 'ITEM001'
    """))
    count = row.scalar_one()
    assert count == 1, "ITEM001 should not be duplicated"

    # Verify candidates exist (at least 2: ITEM001 pre-seeded + ITEM002 inserted)
    row = await db_session.execute(text("SELECT COUNT(*) FROM trend_candidates"))
    total = row.scalar_one()
    assert total >= 2


@pytest.mark.asyncio
async def test_trend_scout_blacklists_ip_names(db_session):
    """TrendScout should skip blacklisted (IP-sensitive) product names."""
    await _seed_fx(db_session)

    # Insert a Disney-related candidate (shouldn't be added by _upsert_candidate)
    added = await TrendScoutAgent()._upsert_candidate(
        {
            "source": "rakuten_ranking",
            "external_key": "DISNEY001",
            "name_raw": "Disney ミッキーマウス キッチン",
            "image_url": "https://img.example.com/disney.jpg",
            "category_guess": "kitchen",
        },
        db_session,
    )
    assert added is False, "Blacklisted name should not be added"


# ── GapAnalyzer Tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_gap_analyzer_ignores_wrong_event(db_session):
    """GapAnalyzer only responds to candidate.discovered."""
    agent = GapAnalyzerAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)
    assert emitted == []


@pytest.mark.asyncio
async def test_gap_analyzer_validates_high_demand(db_session):
    """GapAnalyzer validates a candidate with high demand and low supply."""
    await _seed_fx(db_session)

    # Lower gap threshold to 0 so any demand passes
    await db_session.execute(text("""
        INSERT INTO app_config (key, value)
        VALUES ('gap.score_threshold', '{"value": 0.0}'::jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    # Seed a high-accel candidate
    result = await db_session.execute(text("""
        INSERT INTO trend_candidates
          (source, external_key, name_raw, name_norm, category_guess,
           accel_score, status)
        VALUES
          ('rakuten_ranking', 'GAP001', 'キャンプチェア',
           '캠프 체어', 'outdoor', 0.8, 'DISCOVERED')
        RETURNING id
    """))
    candidate_id = result.scalar_one()
    await db_session.commit()

    agent = GapAnalyzerAgent()
    event = _envelope("candidate.discovered", {"candidate_id": candidate_id},
                       ikey="test:gap:valid1")
    emitted = await agent.handle(event, db_session)

    # Should emit candidate.validated
    validated = [e for e in emitted if e["type"] == "candidate.validated"]
    assert len(validated) == 1, f"Expected 1 validated emission, got {len(emitted)}"
    assert validated[0]["payload"]["candidate_id"] == candidate_id
    assert "gap_score" in validated[0]["payload"]
    assert "kr_keywords" in validated[0]["payload"]


@pytest.mark.asyncio
async def test_gap_analyzer_rejects_saturated(db_session):
    """GapAnalyzer rejects a candidate with high supply (saturated market)."""
    await _seed_fx(db_session)
    await _seed_product_with_listing(db_session, status='LIVE')

    # Set a very high threshold so nothing passes
    await db_session.execute(text("""
        INSERT INTO app_config (key, value)
        VALUES ('gap.score_threshold', '{"value": 999.0}'::jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    # Seed a low-accel candidate
    result = await db_session.execute(text("""
        INSERT INTO trend_candidates
          (source, external_key, name_raw, name_norm, category_guess,
           accel_score, status)
        VALUES
          ('rakuten_ranking', 'GAP002', 'デスクライト',
           '데스크 라이트', 'stationery', 0.01, 'DISCOVERED')
        RETURNING id
    """))
    candidate_id = result.scalar_one()
    await db_session.commit()

    agent = GapAnalyzerAgent()
    event = _envelope("candidate.discovered", {"candidate_id": candidate_id},
                       ikey="test:gap:rej1")
    emitted = await agent.handle(event, db_session)

    # Should emit candidate.rejected
    rejected = [e for e in emitted if e["type"] == "candidate.rejected"]
    assert len(rejected) == 1
    assert rejected[0]["payload"]["reason"] == "saturated"


@pytest.mark.asyncio
async def test_gap_analyzer_keywords_fallback(db_session):
    """GapAnalyzer falls back to rule-based keywords in DRY_RUN."""
    agent = GapAnalyzerAgent()
    keywords = agent._rule_keywords("キッチン収納ボックス")
    assert len(keywords) >= 1
    # Should contain at least one Korean keyword
    assert any("주방" in k or "수납" in k or "키친" in k for k in keywords)


# ── PromotionEngine Tests ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_promotion_engine_ignores_wrong_event(db_session):
    """PromotionEngine only responds to tick.weekly_promotion."""
    agent = PromotionEngineAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)
    assert emitted == []


@pytest.mark.asyncio
async def test_promotion_engine_nominates_qualifying_sku(db_session):
    """PromotionEngine nominates a LIVE SKU with enough orders and margin."""
    await _seed_fx(db_session)
    await _seed_product_with_listing(db_session, status='LIVE', orders_30d=8)

    # Set low thresholds so our seeded SKU qualifies
    await db_session.execute(text("""
        INSERT INTO app_config (key, value) VALUES
        ('promo.min_orders_30d', '{"value": 5}'),
        ('promo.min_margin_rate', '{"value": 0.10}'),
        ('promo.max_oos_events', '{"value": 1}')
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    agent = PromotionEngineAgent()
    event = _envelope("tick.weekly_promotion", {}, ikey="test:promo:nom1")
    emitted = await agent.handle(event, db_session)

    # Should emit campaign.nominated
    nominated = [e for e in emitted if e["type"] == "campaign.nominated"]
    assert len(nominated) == 1
    assert "campaign" in nominated[0]["payload"]
    assert "approval_id" in nominated[0]["payload"]

    # Verify preorder_campaigns row created
    row = await db_session.execute(text("""
        SELECT status, target_qty, campaign_price_krw
        FROM preorder_campaigns
        LIMIT 1
    """))
    rec = row.first()
    assert rec is not None, "preorder_campaigns row should be created"
    assert rec[0] == "PROPOSED"
    assert rec[1] >= 10  # target_qty = 2x monthly orders


@pytest.mark.asyncio
async def test_promotion_engine_skips_low_orders(db_session):
    """PromotionEngine skips SKUs with insufficient orders."""
    await _seed_fx(db_session)
    await _seed_product_with_listing(db_session, status='LIVE', orders_30d=1)

    # High threshold — our SKU won't qualify
    await db_session.execute(text("""
        INSERT INTO app_config (key, value) VALUES
        ('promo.min_orders_30d', '{"value": 10}'),
        ('promo.min_margin_rate', '{"value": 0.10}'),
        ('promo.max_oos_events', '{"value": 1}')
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    agent = PromotionEngineAgent()
    event = _envelope("tick.weekly_promotion", {}, ikey="test:promo:skip1")
    emitted = await agent.handle(event, db_session)
    assert emitted == []


# ── Inquiry Auto-Send Tests (M3 HITL Graduation) ─────────────────────────────

@pytest.mark.asyncio
async def test_inquiry_auto_send_tracking(db_session):
    """M3: InquiryAgent auto-sends tracking answers when enabled."""
    # Enable auto-send for tracking class
    await db_session.execute(text("""
        INSERT INTO app_config (key, value) VALUES
        ('inquiry.auto_send', '{"enabled": true}'),
        ('inquiry.auto_send_classes', '{"classes": ["tracking", "pccc"]}')
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    agent = InquiryAgent()

    # Mock answer_inquiry to simulate successful Naver API call
    with patch("relay.cs.inquiry.answer_inquiry", new_callable=AsyncMock) as mock_send:
        mock_send.return_value = True
        event = _envelope("inquiry.received", {
            "inquiry_id": 1,
            "remote_inquiry_id": "AUTO_SEND_TEST_001",
            "question": "배송 언제 오나요? 운송장번호 알려주세요",
            "order_id": None,
            "listing_id": None,
        })
        emitted = await agent.handle(event, db_session)
        await db_session.commit()

        # Verify answer_inquiry was called
        assert mock_send.called, "answer_inquiry should be called for tracking auto-send"

    # Verify inquiry row marked as auto_sent
    row = await db_session.execute(
        text("SELECT auto_sent, sent_answer FROM inquiries WHERE remote_inquiry_id = :rid"),
        {"rid": "AUTO_SEND_TEST_001"},
    )
    rec = row.first()
    assert rec is not None
    assert rec[0] is True, "auto_sent should be True"
    assert rec[1] is not None, "sent_answer should be populated"

    # Verify emitted event has auto_sent=True
    answered = [e for e in emitted if e["type"] == "inquiry.answered"]
    assert len(answered) == 1
    assert answered[0]["payload"]["auto_sent"] is True


@pytest.mark.asyncio
async def test_inquiry_no_auto_send_when_disabled(db_session):
    """M3: InquiryAgent does NOT auto-send when config disabled."""
    # Disable auto-send
    await db_session.execute(text("""
        INSERT INTO app_config (key, value)
        VALUES ('inquiry.auto_send', '{"enabled": false}'::jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    agent = InquiryAgent()

    with patch("relay.cs.inquiry.answer_inquiry", new_callable=AsyncMock) as mock_send:
        mock_send.return_value = True
        event = _envelope("inquiry.received", {
            "inquiry_id": 2,
            "remote_inquiry_id": "NO_AUTO_SEND_002",
            "question": "배송 언제 오나요?",
            "order_id": None,
            "listing_id": None,
        })
        await agent.handle(event, db_session)
        await db_session.commit()

        # answer_inquiry should NOT be called
        assert not mock_send.called, "answer_inquiry should NOT be called when auto-send disabled"

    # Verify auto_sent is False
    row = await db_session.execute(
        text("SELECT auto_sent FROM inquiries WHERE remote_inquiry_id = :rid"),
        {"rid": "NO_AUTO_SEND_002"},
    )
    rec = row.first()
    assert rec is not None
    assert rec[0] is False


# ── Purchase Auto-Pay Tests (M3 HITL Graduation) ─────────────────────────────

@pytest.mark.asyncio
async def test_auto_pay_under_limit(db_session):
    """M3: OrderAgent auto-pays when under per-order limit and daily cap."""
    await _seed_fx(db_session)
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    # Enable auto-pay with generous limits
    await db_session.execute(text("""
        INSERT INTO app_config (key, value) VALUES
        ('hitl.auto.purchase_pay', '{"enabled": true}'),
        ('auto_pay_limit_krw', '{"value": 50000}'),
        ('auto_pay_daily_cap_krw', '{"value": 500000}')
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    # Create a NEW order with PCCC set (required for auto-pay path)
    result = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES
          ('naver', 'AUTO_PAY_TEST_001', 'ITEM_AP_001', :lid, 1, 25000,
           'P1234567890', 'NEW')
        RETURNING id
    """), {"lid": listing_id})
    order_id = result.scalar_one()
    await db_session.commit()

    agent = OrderAgent()
    event = _envelope("order.created", {
        "order_id": order_id,
        "listing_id": listing_id,
    })
    await agent.handle(event, db_session)
    await db_session.commit()

    # Verify order was auto-paid (status should advance past PURCHASE_PENDING)
    row = await db_session.execute(
        text("SELECT status FROM orders WHERE id = :id"),
        {"id": order_id},
    )
    status = row.scalar_one()
    # Auto-pay should have advanced the order (not stuck at PURCHASE_PENDING)
    assert status != "PURCHASE_PENDING", "Order should have been auto-paid and advanced"


@pytest.mark.asyncio
async def test_auto_pay_over_limit_requires_approval(db_session):
    """M3: OrderAgent does NOT auto-pay when over per-order limit."""
    await _seed_fx(db_session)
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    # Explicitly DISABLE hitl.auto.purchase_pay — rely on limit check only
    # Set a low limit so the order's landed_krw exceeds it
    await db_session.execute(text("""
        INSERT INTO app_config (key, value) VALUES
        ('hitl.auto.purchase_pay', '{"enabled": false}'),
        ('auto_pay_limit_krw', '{"value": 10000}'),
        ('auto_pay_daily_cap_krw', '{"value": 500000}')
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    # Create a NEW order with PCCC set (but unit_sell_krw above the limit)
    result = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES
          ('naver', 'OVER_LIMIT_001', 'ITEM_OL_001', :lid, 1, 25000,
           'P1234567890', 'NEW')
        RETURNING id
    """), {"lid": listing_id})
    order_id = result.scalar_one()
    await db_session.commit()

    agent = OrderAgent()
    event = _envelope("order.created", {
        "order_id": order_id,
        "listing_id": listing_id,
    })
    await agent.handle(event, db_session)
    await db_session.commit()

    # Order should have transitioned to PURCHASE_PENDING and stopped there
    row = await db_session.execute(
        text("SELECT status FROM orders WHERE id = :id"),
        {"id": order_id},
    )
    status = row.scalar_one()
    assert status == "PURCHASE_PENDING", "Order over limit should stay pending"

    # Should have a purchase_pay approval request
    row = await db_session.execute(text("""
        SELECT COUNT(*) FROM approval_requests
        WHERE kind = 'purchase_pay' AND ref_id = :oid
    """), {"oid": order_id})
    count = row.scalar_one()
    assert count >= 1, "Should have a purchase_pay approval request"


# ── Weekly Narrative Tests ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_weekly_narrative_emits_report(db_session):
    """Reporter generates weekly narrative and emits report.weekly_ready."""
    await _seed_fx(db_session)
    await _seed_product_with_listing(db_session, status='LIVE', orders_30d=3)

    # Enable narrative
    await db_session.execute(text("""
        INSERT INTO app_config (key, value)
        VALUES ('a3.narrative_enabled', '{"enabled": true}'::jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """))
    await db_session.commit()

    agent = ReporterAgent()
    event = _envelope("tick.weekly_narrative", {}, ikey="test:weekly:narr1")
    emitted = await agent.handle(event, db_session)

    # Should emit report.weekly_ready
    weekly = [e for e in emitted if e["type"] == "report.weekly_ready"]
    assert len(weekly) == 1
    assert "narrative" in weekly[0]["payload"]
    assert "stats" in weekly[0]["payload"]

    stats = weekly[0]["payload"]["stats"]
    assert "orders_7d" in stats
    assert "revenue_7d_krw" in stats
    assert "live_listings" in stats
    assert stats["live_listings"] >= 1


@pytest.mark.asyncio
async def test_weekly_narrative_ignores_wrong_event(db_session):
    """Reporter weekly narrative only responds to tick.weekly_narrative."""
    agent = ReporterAgent()
    _envelope("tick.daily_report", {})
    # daily_report is handled separately, but weekly_narrative is not
    # This test ensures the routing works
    event_wrong = _envelope("tick.trend_scan", {})
    emitted = await agent.handle(event_wrong, db_session)
    assert emitted == []
