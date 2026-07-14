"""Baseline schema — all tables from docs/03_DATA_MODEL.md.

Includes:
- trend_candidates, brand_leads (intelligence)
- products, product_sources, risk_flags, blocked_rules (catalog)
- listings, price_history (listing)
- orders (FSM), order_events, purchases, shipments (operations)
- inquiries, claims (CS)
- preorder_campaigns (middle/top tier)
- approval_requests, event_outbox, processed_events, llm_calls,
  llm_cache (not in doc 03 — added), fx_rates, app_config (platform/system)

Revision: 0001
"""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Extensions ─────────────────────────────────────────────────────────────
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto"')

    # ── Triggers helper: auto-update updated_at ────────────────────────────────
    op.execute("""
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # ── Order status ENUM ─────────────────────────────────────────────────────
    op.execute("""
        CREATE TYPE order_status AS ENUM (
            'NEW','PURCHASE_PENDING','PURCHASE_APPROVED','PURCHASED',
            'INBOUND_TO_FORWARDER','FORWARDER_RECEIVED','INTL_SHIPPING','CUSTOMS',
            'DOMESTIC_SHIPPING','DELIVERED','SETTLED',
            'HOLD_STOCKOUT','HOLD_PCCC','CANCELLED','REFUND_IN_PROGRESS','RETURNED'
        )
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # INTELLIGENCE
    # ═══════════════════════════════════════════════════════════════════════════
    op.execute("""
        CREATE TABLE trend_candidates (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source          TEXT NOT NULL,
            external_key    TEXT NOT NULL,
            name_raw        TEXT NOT NULL,
            name_norm       TEXT NOT NULL,
            image_url       TEXT,
            image_phash     TEXT,
            category_guess  TEXT,
            accel_score     NUMERIC,
            gap_score       NUMERIC,
            kr_keywords     JSONB,
            saturation      JSONB,
            status          TEXT NOT NULL DEFAULT 'DISCOVERED',
            reject_reason   TEXT,
            first_seen_at   TIMESTAMPTZ DEFAULT now(),
            last_seen_at    TIMESTAMPTZ DEFAULT now(),
            created_at      TIMESTAMPTZ DEFAULT now(),
            updated_at      TIMESTAMPTZ DEFAULT now(),
            UNIQUE (source, external_key)
        )
    """)
    op.execute("CREATE INDEX ON trend_candidates (status, accel_score DESC)")
    op.execute("""
        CREATE TRIGGER trend_candidates_updated_at
        BEFORE UPDATE ON trend_candidates
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE brand_leads (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            brand_name  TEXT,
            country     TEXT,
            homepage    TEXT,
            dossier     JSONB,
            fit_score   NUMERIC,
            status      TEXT DEFAULT 'FOUND',
            contact     JSONB,
            notes       TEXT,
            created_at  TIMESTAMPTZ DEFAULT now(),
            updated_at  TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TRIGGER brand_leads_updated_at
        BEFORE UPDATE ON brand_leads
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # CATALOG
    # ═══════════════════════════════════════════════════════════════════════════
    op.execute("""
        CREATE TABLE products (
            id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            candidate_id        BIGINT REFERENCES trend_candidates(id) ON DELETE SET NULL,
            origin_route        TEXT NOT NULL,
            canonical_name_ko   TEXT,
            canonical_name_src  TEXT,
            brand               TEXT,
            category_naver      TEXT,
            category_internal   TEXT,
            attributes          JSONB,
            images              JSONB,
            risk_status         TEXT NOT NULL DEFAULT 'PENDING',
            status              TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at          TIMESTAMPTZ DEFAULT now(),
            updated_at          TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TRIGGER products_updated_at
        BEFORE UPDATE ON products
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE product_sources (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            product_id      BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            marketplace     TEXT NOT NULL,
            url             TEXT NOT NULL,
            seller_name     TEXT,
            seller_rating   NUMERIC,
            currency        TEXT,
            price_minor     BIGINT,
            stock_state     TEXT DEFAULT 'UNKNOWN',
            shipping_class  TEXT,
            weight_g        INT,
            variant_map     JSONB,
            rank            SMALLINT DEFAULT 1,
            last_checked_at TIMESTAMPTZ,
            created_at      TIMESTAMPTZ DEFAULT now(),
            updated_at      TIMESTAMPTZ DEFAULT now(),
            UNIQUE (product_id, url)
        )
    """)
    op.execute("CREATE INDEX ON product_sources (last_checked_at)")
    op.execute("CREATE INDEX ON product_sources (stock_state)")
    op.execute("""
        CREATE TRIGGER product_sources_updated_at
        BEFORE UPDATE ON product_sources
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE risk_flags (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            product_id      BIGINT REFERENCES products(id) ON DELETE CASCADE,
            candidate_id    BIGINT REFERENCES trend_candidates(id) ON DELETE CASCADE,
            kind            TEXT NOT NULL,
            detail          JSONB,
            severity        TEXT,
            decided_by      TEXT,
            decided_at      TIMESTAMPTZ,
            created_at      TIMESTAMPTZ DEFAULT now(),
            updated_at      TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TRIGGER risk_flags_updated_at
        BEFORE UPDATE ON risk_flags
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE blocked_rules (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            kind        TEXT NOT NULL,
            pattern     TEXT NOT NULL,
            note        TEXT,
            active      BOOL DEFAULT true,
            created_at  TIMESTAMPTZ DEFAULT now(),
            updated_at  TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TRIGGER blocked_rules_updated_at
        BEFORE UPDATE ON blocked_rules
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # LISTINGS
    # ═══════════════════════════════════════════════════════════════════════════
    op.execute("""
        CREATE TABLE listings (
            id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            product_id          BIGINT NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
            marketplace         TEXT NOT NULL,
            store_account       TEXT NOT NULL,
            remote_product_id   TEXT,
            remote_url          TEXT,
            title               TEXT,
            content             JSONB,
            sell_price_krw      INT,
            margin_krw          INT,
            margin_rate         NUMERIC,
            status              TEXT NOT NULL DEFAULT 'DRAFT',
            publish_batch_id    BIGINT,
            stats               JSONB DEFAULT '{}'::jsonb,
            scan_tier           SMALLINT DEFAULT 2,
            created_at          TIMESTAMPTZ DEFAULT now(),
            updated_at          TIMESTAMPTZ DEFAULT now(),
            UNIQUE (marketplace, store_account, remote_product_id)
        )
    """)
    op.execute("CREATE INDEX ON listings (status, marketplace)")
    op.execute("CREATE INDEX ON listings (scan_tier, status)")
    op.execute("""
        CREATE TRIGGER listings_updated_at
        BEFORE UPDATE ON listings
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE price_history (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            listing_id      BIGINT NOT NULL REFERENCES listings(id) ON DELETE CASCADE,
            source_id       BIGINT REFERENCES product_sources(id) ON DELETE SET NULL,
            src_price_minor BIGINT,
            fx              NUMERIC,
            landed_krw      INT,
            sell_price_krw  INT,
            reason          TEXT,
            at              TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ON price_history (listing_id, at DESC)")

    # ═══════════════════════════════════════════════════════════════════════════
    # ORDERS (FSM)
    # ═══════════════════════════════════════════════════════════════════════════
    op.execute("""
        CREATE TABLE orders (
            id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            marketplace             TEXT NOT NULL,
            remote_order_id         TEXT,
            remote_order_item_id    TEXT,
            listing_id              BIGINT REFERENCES listings(id) ON DELETE RESTRICT,
            qty                     INT NOT NULL DEFAULT 1,
            unit_sell_krw           INT,
            buyer_name              TEXT,
            buyer_contact           TEXT,
            pccc                    TEXT,
            ship_to                 JSONB,
            status                  order_status NOT NULL DEFAULT 'NEW',
            margin_snapshot         JSONB,
            created_at              TIMESTAMPTZ DEFAULT now(),
            updated_at              TIMESTAMPTZ DEFAULT now(),
            UNIQUE (marketplace, remote_order_item_id)
        )
    """)
    op.execute("CREATE INDEX ON orders (status)")
    op.execute("CREATE INDEX ON orders (created_at DESC)")
    op.execute("""
        CREATE TRIGGER orders_updated_at
        BEFORE UPDATE ON orders
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE order_events (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            order_id    BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
            from_status TEXT,
            to_status   TEXT NOT NULL,
            actor       TEXT NOT NULL,
            detail      JSONB,
            at          TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ON order_events (order_id, at DESC)")

    op.execute("""
        CREATE TABLE purchases (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            order_id        BIGINT NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
            source_id       BIGINT REFERENCES product_sources(id) ON DELETE SET NULL,
            src_order_id    TEXT,
            paid_minor      BIGINT,
            currency        TEXT,
            fx              NUMERIC,
            paid_at         TIMESTAMPTZ,
            payment_method  TEXT,
            status          TEXT DEFAULT 'PREPARED',
            created_at      TIMESTAMPTZ DEFAULT now(),
            updated_at      TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TRIGGER purchases_updated_at
        BEFORE UPDATE ON purchases
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE shipments (
            id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            order_id            BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
            forwarder           TEXT,
            forwarder_ref       TEXT,
            src_tracking        TEXT,
            intl_tracking       TEXT,
            kr_tracking         TEXT,
            kr_carrier          TEXT,
            stage               TEXT,
            last_movement_at    TIMESTAMPTZ,
            stalled             BOOL DEFAULT false,
            events              JSONB DEFAULT '[]'::jsonb,
            created_at          TIMESTAMPTZ DEFAULT now(),
            updated_at          TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ON shipments (stalled) WHERE stalled")
    op.execute("CREATE INDEX ON shipments (order_id)")
    op.execute("""
        CREATE TRIGGER shipments_updated_at
        BEFORE UPDATE ON shipments
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # CS
    # ═══════════════════════════════════════════════════════════════════════════
    op.execute("""
        CREATE TABLE inquiries (
            id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            marketplace         TEXT NOT NULL,
            remote_inquiry_id   TEXT UNIQUE,
            order_id            BIGINT REFERENCES orders(id) ON DELETE SET NULL,
            listing_id          BIGINT REFERENCES listings(id) ON DELETE SET NULL,
            question            TEXT,
            klass               TEXT,
            confidence          NUMERIC,
            draft_answer        TEXT,
            sent_answer         TEXT,
            auto_sent           BOOL DEFAULT false,
            status              TEXT DEFAULT 'OPEN',
            created_at          TIMESTAMPTZ DEFAULT now(),
            updated_at          TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TRIGGER inquiries_updated_at
        BEFORE UPDATE ON inquiries
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE claims (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            order_id        BIGINT NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
            kind            TEXT NOT NULL,
            fault           TEXT,
            resolution      JSONB,
            status          TEXT DEFAULT 'OPEN',
            money_out_krw   INT DEFAULT 0,
            created_at      TIMESTAMPTZ DEFAULT now(),
            updated_at      TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TRIGGER claims_updated_at
        BEFORE UPDATE ON claims
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # MIDDLE / TOP TIERS
    # ═══════════════════════════════════════════════════════════════════════════
    op.execute("""
        CREATE TABLE preorder_campaigns (
            id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            product_id          BIGINT NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
            window_start        DATE,
            window_end          DATE,
            target_qty          INT,
            sold_qty            INT DEFAULT 0,
            campaign_price_krw  INT,
            batch_economics     JSONB,
            status              TEXT DEFAULT 'PROPOSED',
            created_at          TIMESTAMPTZ DEFAULT now(),
            updated_at          TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TRIGGER preorder_campaigns_updated_at
        BEFORE UPDATE ON preorder_campaigns
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # PLATFORM / SYSTEM
    # ═══════════════════════════════════════════════════════════════════════════
    op.execute("""
        CREATE TABLE approval_requests (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            kind            TEXT NOT NULL,
            ref_table       TEXT,
            ref_id          BIGINT,
            summary         TEXT,
            evidence        JSONB,
            proposed_action JSONB,
            status          TEXT DEFAULT 'PENDING',
            decided_by      TEXT,
            decided_at      TIMESTAMPTZ,
            expires_at      TIMESTAMPTZ,
            created_at      TIMESTAMPTZ DEFAULT now(),
            updated_at      TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ON approval_requests (status, kind)")
    op.execute("""
        CREATE TRIGGER approval_requests_updated_at
        BEFORE UPDATE ON approval_requests
        FOR EACH ROW EXECUTE FUNCTION set_updated_at()
    """)

    op.execute("""
        CREATE TABLE event_outbox (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            stream          TEXT NOT NULL,
            type            TEXT NOT NULL,
            idempotency_key TEXT UNIQUE NOT NULL,
            payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
            published       BOOL DEFAULT false,
            published_at    TIMESTAMPTZ,
            created_at      TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ON event_outbox (published) WHERE NOT published")

    op.execute("""
        CREATE TABLE processed_events (
            consumer            TEXT NOT NULL,
            idempotency_key     TEXT NOT NULL,
            at                  TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (consumer, idempotency_key)
        )
    """)
    op.execute("CREATE INDEX ON processed_events (at)")

    op.execute("""
        CREATE TABLE llm_calls (
            id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            agent               TEXT NOT NULL,
            task                TEXT NOT NULL,
            tier                TEXT NOT NULL,
            model               TEXT,
            prompt_tokens       INT DEFAULT 0,
            completion_tokens   INT DEFAULT 0,
            cost_est            NUMERIC DEFAULT 0,
            latency_ms          INT DEFAULT 0,
            cache_hit           BOOL DEFAULT false,
            ok                  BOOL DEFAULT true,
            err                 TEXT DEFAULT '',
            trace_id            TEXT DEFAULT '',
            at                  TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ON llm_calls (agent, at DESC)")
    op.execute("CREATE INDEX ON llm_calls (at DESC)")

    # llm_cache table (missing from doc 03, added per gap analysis)
    op.execute("""
        CREATE TABLE llm_cache (
            cache_key   TEXT PRIMARY KEY,
            response    JSONB NOT NULL,
            expires_at  TIMESTAMPTZ NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX ON llm_cache (expires_at)")

    op.execute("""
        CREATE TABLE fx_rates (
            pair    TEXT NOT NULL,
            rate    NUMERIC NOT NULL,
            at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pair, at)
        )
    """)
    op.execute("CREATE INDEX ON fx_rates (pair, at DESC)")

    op.execute("""
        CREATE TABLE app_config (
            key         TEXT PRIMARY KEY,
            value       JSONB NOT NULL,
            updated_by  TEXT DEFAULT 'system',
            updated_at  TIMESTAMPTZ DEFAULT now()
        )
    """)

    # ── Seed app_config with default runtime flags ────────────────────────────
    _seed_app_config(op)

    # ── Seed blocked_rules with initial compliance blocklist ──────────────────
    _seed_blocked_rules(op)


def _seed_app_config(op: op) -> None:
    """Initial runtime-tunable config values (can be changed via Approval Queue UI)."""
    seeds = [
        # HITL graduation flags (all off in M1)
        ("hitl.auto.publish_batch",    '{"enabled": false}'),
        ("hitl.auto.purchase_pay",     '{"enabled": false}'),
        ("hitl.auto.cs_auto_send",     '{"enabled": false}'),
        ("hitl.auto.refund",           '{"enabled": false}'),
        # Rate limits
        ("publish_rate_daily",         '{"value": 300}'),
        ("stock_staleness_alert_hours", '{"value": 36}'),
        # Financial controls
        ("auto_pay_limit_krw",         '{"value": 0}'),
        ("auto_pay_daily_cap_krw",     '{"value": 0}'),
        # LLM budget
        ("llm_daily_budget_tokens",    '{"value": 30000000}'),
        # Cancel rate circuit breaker
        ("cancel_rate_throttle_threshold", '{"value": 0.02}'),
        # FX reprice trigger threshold
        ("fx_reprice_threshold_pct",   '{"value": 1.5}'),
        # Publish throttle if cancel_rate breached
        ("publish_throttle_active",    '{"enabled": false}'),
    ]
    for key, value in seeds:
        op.execute(
            f"INSERT INTO app_config (key, value) VALUES ('{key}', '{value}'::jsonb) "
            "ON CONFLICT DO NOTHING"
        )


def _seed_blocked_rules(op: op) -> None:
    """Initial compliance blocklist rules (doc 07 §2)."""
    rules = [
        # category-level blocks
        ("cert_kc",     "electrical|가전|전기용품|충전기|배터리팩",    "KC certification required"),
        ("cert_radio",  "bluetooth|무선|radio|wifi|블루투스",          "전파법 — radio/BT certification required"),
        ("children",    "어린이|유아|baby|infant|어린이용|toy",        "어린이제품법 certification required"),
        ("food",        "식품|음식|food|snack|과자|supplement|건강기능",  "Food import regulation"),
        ("cosmetics",   "화장품|cosmetic|skincare|스킨케어",           "Cosmetics import regulation"),
        ("medical",     "의료기기|medical device|quasi-drug|의약외품", "Medical device regulation"),
        ("battery",     "리튬배터리|lithium battery standalone",       "Hazardous transport — standalone batteries"),
        ("aerosol",     "aerosol|스프레이캔|가스캔",                   "Hazardous transport"),
        # IP/brand blocks (starter set — expand continuously)
        ("ip",          "disney|포켓몬|pokemon|sanrio|hello kitty|라인프렌즈|bt21|kakao|starbucks logo",
                        "Known character/brand IP — require image + text screen"),
        # Marketplace-banned
        ("mkt_banned",  "도박|성인|adult|alcohol|주류",               "Naver/Coupang marketplace ban"),
    ]
    for kind, pattern, note in rules:
        note_escaped = note.replace("'", "''")
        pattern_escaped = pattern.replace("'", "''")
        op.execute(
            f"INSERT INTO blocked_rules (kind, pattern, note, active) "
            f"VALUES ('{kind}', '{pattern_escaped}', '{note_escaped}', true) "
            "ON CONFLICT DO NOTHING"
        )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app_config CASCADE")
    op.execute("DROP TABLE IF EXISTS fx_rates CASCADE")
    op.execute("DROP TABLE IF EXISTS llm_cache CASCADE")
    op.execute("DROP TABLE IF EXISTS llm_calls CASCADE")
    op.execute("DROP TABLE IF EXISTS processed_events CASCADE")
    op.execute("DROP TABLE IF EXISTS event_outbox CASCADE")
    op.execute("DROP TABLE IF EXISTS approval_requests CASCADE")
    op.execute("DROP TABLE IF EXISTS preorder_campaigns CASCADE")
    op.execute("DROP TABLE IF EXISTS claims CASCADE")
    op.execute("DROP TABLE IF EXISTS inquiries CASCADE")
    op.execute("DROP TABLE IF EXISTS shipments CASCADE")
    op.execute("DROP TABLE IF EXISTS purchases CASCADE")
    op.execute("DROP TABLE IF EXISTS order_events CASCADE")
    op.execute("DROP TABLE IF EXISTS orders CASCADE")
    op.execute("DROP TABLE IF EXISTS price_history CASCADE")
    op.execute("DROP TABLE IF EXISTS listings CASCADE")
    op.execute("DROP TABLE IF EXISTS blocked_rules CASCADE")
    op.execute("DROP TABLE IF EXISTS risk_flags CASCADE")
    op.execute("DROP TABLE IF EXISTS product_sources CASCADE")
    op.execute("DROP TABLE IF EXISTS products CASCADE")
    op.execute("DROP TABLE IF EXISTS brand_leads CASCADE")
    op.execute("DROP TABLE IF EXISTS trend_candidates CASCADE")
    op.execute("DROP TYPE IF EXISTS order_status")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at()")
