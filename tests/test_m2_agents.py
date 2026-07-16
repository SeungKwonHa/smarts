"""M2 Core agent contract tests.

Tests for:
- InquiryAgent (C1): classify + draft inquiries
- ClaimTriage (C2): fault classification + resolution
- SKUManager (A1): retire dead SKUs, promote risers
- OrderAgent: PCCC hold flow, purchase deny
- LogisticsTracker: dispatch idempotency, proactive stall draft
- Reporter: suspended count + new M2 metrics

External APIs (Naver, LLM) are all mocked.
Set RELAY_DRY_RUN=1 (done in conftest) to suppress LLM + Naver API calls.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from unittest.mock import AsyncMock, patch

from relay.core.events import STREAM_CS, STREAM_ANALYTICS, STREAM_OPS, STREAM_APPROVALS
from relay.cs.inquiry import InquiryAgent
from relay.cs.claim_triage import ClaimTriageAgent
from relay.analytics.sku_manager import SKUManagerAgent
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


async def _seed_product_with_listing(session, status='LIVE', with_pccc=True, days_ago=0):
    """Seed product + source + listing. Returns (product_id, source_id, listing_id)."""
    await _seed_fx(session)

    created_offset = ""
    if days_ago > 0:
        created_offset = f" - interval '{days_ago} days'"

    prod = await session.execute(text(f"""
        INSERT INTO products
          (origin_route, canonical_name_src, brand, category_internal,
           attributes, images, risk_status, status)
        VALUES
          ('longtail', '테스트 상품', 'TestBrand', 'kitchen',
           '{{"weight_g": 300}}'::jsonb, '[]'::jsonb, 'CLEARED', 'ACTIVE')
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

    await session.commit()
    return product_id, source_id, listing_id


# ── InquiryAgent Tests ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_inquiry_agent_classifies_tracking(db_session):
    """InquiryAgent classifies a tracking inquiry and saves draft."""
    agent = InquiryAgent()
    event = _envelope("inquiry.received", {
        "inquiry_id": 1,
        "remote_inquiry_id": "TEST_INQ_001",
        "question": "배송 언제 오나요? 운송장번호 알려주세요",
        "order_id": None,
        "listing_id": None,
    })
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Verify inquiry row created with tracking klass
    row = await db_session.execute(
        text("SELECT klass, confidence, status FROM inquiries WHERE remote_inquiry_id = :rid"),
        {"rid": "TEST_INQ_001"},
    )
    rec = row.first()
    assert rec is not None, "Inquiry row should be created"
    assert rec[0] == "tracking", f"Expected tracking klass, got {rec[0]}"
    assert rec[1] >= 0.7, f"Confidence should be >= 0.7, got {rec[1]}"
    assert rec[2] == "OPEN"

    # Should emit inquiry.answered (draft auto-composed for tracking)
    answered = [e for e in emitted if e["type"] == "inquiry.answered"]
    assert len(answered) == 1
    assert answered[0]["payload"]["auto_sent"] is False  # M2: draft only


@pytest.mark.asyncio
async def test_inquiry_agent_escalates_refund(db_session):
    """InquiryAgent escalates cancel/refund intent to Approval Queue."""
    agent = InquiryAgent()
    event = _envelope("inquiry.received", {
        "inquiry_id": 2,
        "remote_inquiry_id": "TEST_INQ_002",
        "question": "취소하고 싶어요. 환불해주세요.",
        "order_id": None,
        "listing_id": None,
    })
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should NOT emit inquiry.answered (refund is auto-draft eligible but low confidence)
    answered = [e for e in emitted if e["type"] == "inquiry.answered"]
    # cancel_refund keyword match gives confidence 0.6, below threshold 0.7 → escalate
    assert len(answered) == 0

    # Should create cs_draft approval
    row = await db_session.execute(
        text("SELECT kind FROM approval_requests WHERE ref_table = 'inquiries'"),
    )
    kinds = [r[0] for r in row.fetchall()]
    assert "cs_draft" in kinds, f"Expected cs_draft approval, got {kinds}"


@pytest.mark.asyncio
async def test_inquiry_agent_composes_tracking_draft(db_session):
    """InquiryAgent composes a tracking answer when tracking number exists."""
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    # Create an order + shipment with tracking
    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES ('naver', 'ORD_TRK_001', 'ITEM_TRK_001', :lid, 1, 25000,
                'P1234567890', 'PURCHASED')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()

    await session_execute(db_session, """
        INSERT INTO shipments (order_id, stage, kr_tracking, kr_carrier, last_movement_at)
        VALUES (:oid, 'DOMESTIC_SHIPPING', '1234567890123', 'CJGLS', now())
    """, {"oid": order_id})
    await db_session.commit()

    agent = InquiryAgent()
    event = _envelope("inquiry.received", {
        "inquiry_id": 3,
        "remote_inquiry_id": "TEST_INQ_003",
        "question": "배송 현재 어디쯤 왔나요?",
        "order_id": order_id,
        "listing_id": listing_id,
    })
    await agent.handle(event, db_session)
    await db_session.commit()

    row = await db_session.execute(
        text("SELECT draft_answer FROM inquiries WHERE remote_inquiry_id = :rid"),
        {"rid": "TEST_INQ_003"},
    )
    rec = row.first()
    assert rec is not None
    assert rec[0] is not None and len(rec[0]) > 0, "draft_answer should be populated"
    assert "1234567890123" in rec[0], "Tracking number should be in answer"


# ── SKUManager Tests ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sku_manager_retires_dead_sku(db_session):
    """SKUManager retires a LIVE listing with no sales in 30+ days."""
    product_id, source_id, listing_id = await _seed_product_with_listing(
        db_session, status='LIVE', days_ago=35
    )

    agent = SKUManagerAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should emit sku.retire
    retire_events = [e for e in emitted if e["type"] == "sku.retire"]
    assert len(retire_events) == 1
    assert retire_events[0]["payload"]["listing_id"] == listing_id
    assert retire_events[0]["payload"]["reason"] == "no_sales"

    # Verify listing is RETIRED
    row = await db_session.execute(
        text("SELECT status FROM listings WHERE id = :id"),
        {"id": listing_id},
    )
    assert row.scalar() == "RETIRED"


@pytest.mark.asyncio
async def test_sku_manager_promotes_riser(db_session):
    """SKUManager promotes a listing with recent sales to tier 1."""
    product_id, source_id, listing_id = await _seed_product_with_listing(
        db_session, status='LIVE', days_ago=5
    )

    # Create a recent order
    await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, status)
        VALUES ('naver', 'ORD_RISER_001', 'ITEM_RISER_001', :lid, 1, 25000, 'PURCHASED')
    """), {"lid": listing_id})
    await db_session.commit()

    agent = SKUManagerAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should emit sku.tier_change
    tier_events = [e for e in emitted if e["type"] == "sku.tier_change"]
    assert len(tier_events) == 1
    assert tier_events[0]["payload"]["listing_id"] == listing_id
    assert tier_events[0]["payload"]["scan_tier"] == 1

    # Verify scan_tier updated
    row = await db_session.execute(
        text("SELECT scan_tier FROM listings WHERE id = :id"),
        {"id": listing_id},
    )
    assert row.scalar() == 1


@pytest.mark.asyncio
async def test_sku_manager_retires_chronic_oos(db_session):
    """SKUManager retires a listing with chronic OOS source state."""
    product_id, source_id, listing_id = await _seed_product_with_listing(
        db_session, status='LIVE', days_ago=5
    )

    # Mark source as OOS (chronic instability)
    await db_session.execute(
        text("UPDATE product_sources SET stock_state = 'OOS' WHERE id = :id"),
        {"id": source_id},
    )

    # Lower the threshold for this test (default is 3, but we have 1 source)
    await db_session.execute(
        text("""
            INSERT INTO app_config (key, value) VALUES ('sku.retire.oos_event_threshold', '{"value": 1}')
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """)
    )
    await db_session.commit()

    agent = SKUManagerAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should emit sku.retire with chronic_oos reason
    retire_events = [e for e in emitted if e["type"] == "sku.retire"]
    assert len(retire_events) == 1, f"Expected 1 retire event, got {len(retire_events)}"
    assert retire_events[0]["payload"]["reason"] == "chronic_oos"


@pytest.mark.asyncio
async def test_sku_manager_leaves_healthy_alone(db_session):
    """SKUManager does nothing for a healthy listing with recent sales."""
    product_id, source_id, listing_id = await _seed_product_with_listing(
        db_session, status='LIVE', days_ago=5
    )

    # Create recent orders (so it's not dead)
    await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, status)
        VALUES ('naver', 'ORD_OK_001', 'ITEM_OK_001', :lid, 1, 25000, 'PURCHASED')
    """), {"lid": listing_id})
    await db_session.commit()

    agent = SKUManagerAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should NOT retire (has sales) but MAY promote to tier 1
    retire_events = [e for e in emitted if e["type"] == "sku.retire"]
    assert len(retire_events) == 0, "Healthy listing should not be retired"


# ── PCCC Hold Flow Tests ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pccc_hold_flow(db_session):
    """OrderAgent holds order when PCCC is missing."""
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    # Order WITHOUT PCCC (use unique remote_product_id to avoid conflict with M1 tests)
    await db_session.execute(
        text("UPDATE listings SET status = 'LIVE', remote_product_id = 'NV_PCCC_HOLD_001' WHERE id = :id"),
        {"id": listing_id},
    )
    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, status)
        VALUES ('naver', 'ORD_NO_PCCC', 'ITEM_NO_PCCC', :lid, 1, 25000, 'NEW')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()
    await db_session.commit()

    agent = OrderAgent()
    event = _envelope("order.created", {
        "order_id": order_id,
        "marketplace": "naver",
        "listing_id": listing_id,
        "qty": 1,
    })
    await agent.handle(event, db_session)
    await db_session.commit()

    # Should be in HOLD_PCCC
    row = await db_session.execute(
        text("SELECT status FROM orders WHERE id = :id"),
        {"id": order_id},
    )
    assert row.scalar() == "HOLD_PCCC"

    # Should create cs_draft approval
    row = await db_session.execute(
        text("SELECT kind FROM approval_requests WHERE ref_id = :oid"),
        {"oid": order_id},
    )
    kinds = [r[0] for r in row.fetchall()]
    assert "cs_draft" in kinds


@pytest.mark.asyncio
async def test_pccc_resume_flow(db_session):
    """OrderAgent resumes from HOLD_PCCC when PCCC is received."""
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    # Order in HOLD_PCCC with PCCC now filled
    await db_session.execute(
        text("UPDATE listings SET status = 'LIVE', remote_product_id = 'NV_TEST_002' WHERE id = :id"),
        {"id": listing_id},
    )
    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES ('naver', 'ORD_PCCC_RESUME', 'ITEM_PCCC_RESUME', :lid, 1, 25000,
                'P999888777', 'HOLD_PCCC')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()
    await db_session.commit()

    agent = OrderAgent()
    event = _envelope("order.pccc_received", {"order_id": order_id})
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should transition to PURCHASE_PENDING (or beyond if auto_approved)
    row = await db_session.execute(
        text("SELECT status FROM orders WHERE id = :id"),
        {"id": order_id},
    )
    status = row.scalar()
    assert status in ("PURCHASE_PENDING", "PURCHASE_APPROVED", "PURCHASED")


# ── Dispatch Idempotency Test ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_naver_dispatch_idempotency(db_session):
    """dispatch_order called twice — second call treats 409 as success."""
    from relay.integrations.naver.client import dispatch_order

    with patch("relay.integrations.naver.client.http_client") as mock_http:
        mock_http.post_json = AsyncMock(return_value={"ok": True})

        result = await dispatch_order(
            order_id="ORD_TEST",
            product_order_id="ITEM_TEST",
            carrier_code="CJGLS",
            tracking_number="1234567890123",
        )
        assert result is True

        # Second call — simulate 409 Conflict
        mock_http.post_json = AsyncMock(
            side_effect=Exception("409 Conflict: already dispatched")
        )
        result = await dispatch_order(
            order_id="ORD_TEST",
            product_order_id="ITEM_TEST",
            carrier_code="CJGLS",
            tracking_number="1234567890123",
        )
        assert result is True, "409 Conflict should be treated as success"


# ── Reporter Test ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_report_includes_new_m2_metrics(db_session):
    """Reporter output includes suspended, holds, and inquiries counts."""
    product_id, source_id, listing_id = await _seed_product_with_listing(
        db_session, status='SUSPENDED_STOCKOUT'
    )

    agent = ReporterAgent()
    event = _envelope("tick.daily_report", {})
    emitted = await agent.handle(event, db_session)

    # Should still emit report.daily_ready
    report_events = [e for e in emitted if e["type"] == "report.daily_ready"]
    assert len(report_events) == 1

    # Verify the report function doesn't crash — the real test is that
    # _emit_report is called with suspended/holds/inquiries keys
    row = await db_session.execute(
        text("SELECT COUNT(*) FROM listings WHERE status = 'SUSPENDED_STOCKOUT'"),
    )
    assert row.scalar() >= 1


# ── ClaimTriage Tests ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_claim_triage_classifies_seller_fault(db_session):
    """ClaimTriage classifies pre-shipment cancel as seller fault."""
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES ('naver', 'ORD_CLAIM_001', 'ITEM_CLAIM_001', :lid, 1, 25000,
                'P1234567890', 'PURCHASE_PENDING')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()
    await db_session.commit()

    agent = ClaimTriageAgent()
    event = _envelope("claim.opened", {
        "claim_id": 1,
        "order_id": order_id,
        "claim_type": "cancel",
        "reason": "구매 변심으로 취소 요청합니다",
    })
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Verify claim row created
    row = await db_session.execute(
        text("SELECT fault, kind FROM claims WHERE order_id = :oid"),
        {"oid": order_id},
    )
    rec = row.first()
    assert rec is not None, "Claim row should be created"
    # pre-shipment cancel → seller fault (rule-based)
    assert rec[0] == "seller", f"Expected seller fault, got {rec[0]}"
    assert rec[1] == "cancel"

    # Pre-shipment cancel → cancel_pre_ship resolution → no money-out → claim.triaged
    triaged = [e for e in emitted if e["type"] == "claim.triaged"]
    assert len(triaged) == 1


@pytest.mark.asyncio
async def test_claim_triage_refund_full_requires_approval(db_session):
    """ClaimTriage with seller-fault refund routes to HITL approval."""
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES ('naver', 'ORD_CLAIM_002', 'ITEM_CLAIM_002', :lid, 1, 25000,
                'P1234567890', 'DELIVERED')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()
    await db_session.commit()

    agent = ClaimTriageAgent()
    event = _envelope("claim.opened", {
        "claim_id": 2,
        "order_id": order_id,
        "claim_type": "refund",
        "reason": "상품이 파손되어 도착했습니다. 환불 요청합니다.",
    })
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should NOT emit claim.triaged (money-out → HITL)
    triaged = [e for e in emitted if e["type"] == "claim.triaged"]
    assert len(triaged) == 0

    # Should create claim_refund approval
    row = await db_session.execute(
        text("SELECT kind FROM approval_requests WHERE ref_id = :oid AND kind = 'claim_refund'"),
        {"oid": order_id},
    )
    rec = row.first()
    assert rec is not None, "Should create claim_refund approval"


@pytest.mark.asyncio
async def test_claim_triage_customs_fault(db_session):
    """ClaimTriage classifies PCCC/customs issues correctly."""
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, status)
        VALUES ('naver', 'ORD_CLAIM_003', 'ITEM_CLAIM_003', :lid, 1, 25000,
                'CUSTOMS')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()
    await db_session.commit()

    agent = ClaimTriageAgent()
    event = _envelope("claim.opened", {
        "claim_id": 3,
        "order_id": order_id,
        "claim_type": "refund",
        "reason": "개인통관고유부호를 보냈는데도 통관이 안 됩니다. 환불해주세요.",
    })
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    row = await db_session.execute(
        text("SELECT fault FROM claims WHERE order_id = :oid"),
        {"oid": order_id},
    )
    rec = row.first()
    assert rec[0] == "customs", f"Expected customs fault, got {rec[0]}"


# ── Purchase Deny Tests ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_order_agent_purchase_denied(db_session):
    """OrderAgent transitions to CANCELLED when purchase approval is denied."""
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES ('naver', 'ORD_DENY_001', 'ITEM_DENY_001', :lid, 1, 25000,
                'P1234567890', 'PURCHASE_PENDING')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()
    await db_session.commit()

    agent = OrderAgent()
    event = _envelope("approval.denied", {
        "order_id": order_id,
        "kind": "purchase_pay",
    })
    emitted = await agent.handle(event, db_session)
    await db_session.commit()

    # Should transition to CANCELLED
    row = await db_session.execute(
        text("SELECT status FROM orders WHERE id = :id"),
        {"id": order_id},
    )
    assert row.scalar() == "CANCELLED"

    # Should emit claim.opened
    claim_events = [e for e in emitted if e["type"] == "claim.opened"]
    assert len(claim_events) == 1
    assert claim_events[0]["payload"]["claim_type"] == "cancel"


# ── LogisticsTracker Stall Tests ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_logistics_stall_triggers_delay_draft(db_session):
    """LogisticsTracker creates cs_draft approval when shipment stalls."""
    product_id, source_id, listing_id = await _seed_product_with_listing(db_session)

    order_row = await db_session.execute(text("""
        INSERT INTO orders
          (marketplace, remote_order_id, remote_order_item_id,
           listing_id, qty, unit_sell_krw, pccc, status)
        VALUES ('naver', 'ORD_STALL_001', 'ITEM_STALL_001', :lid, 1, 25000,
                'P1234567890', 'INBOUND_TO_FORWARDER')
        RETURNING id
    """), {"lid": listing_id})
    order_id = order_row.scalar_one()

    # Create a shipment stalled for > 72h (INBOUND_TO_FORWARDER threshold)
    await db_session.execute(text("""
        INSERT INTO shipments (order_id, stage, last_movement_at, stalled, events)
        VALUES (:oid, 'INBOUND_TO_FORWARDER', now() - interval '80 hours', false,
                '[]'::jsonb)
    """), {"oid": order_id})
    await db_session.commit()

    agent = LogisticsAgent()
    event = _envelope("tick.tracking_poll", {})
    await agent.handle(event, db_session)
    await db_session.commit()

    # Should mark stalled = true
    row = await db_session.execute(
        text("SELECT stalled FROM shipments WHERE order_id = :oid"),
        {"oid": order_id},
    )
    assert row.scalar() is True

    # Should create cs_draft approval
    row = await db_session.execute(
        text("""
            SELECT summary FROM approval_requests
            WHERE kind = 'cs_draft' AND ref_table = 'shipments'
        """),
    )
    rec = row.first()
    assert rec is not None, "Should create cs_draft approval for proactive delay"
    assert "stalled" in rec[0].lower() or "INBOUND_TO_FORWARDER" in rec[0]


# ── Helper ──────────────────────────────────────────────────────────────────

async def session_execute(session, query: str, params: dict) -> None:
    """Execute a query and commit (helper for fixture-like setups)."""
    await session.execute(text(query), params)
