# 03 — Data Model (Postgres)

Conventions: `id BIGINT GENERATED ALWAYS AS IDENTITY PK` unless noted;
`created_at/updated_at TIMESTAMPTZ` on every table (trigger-maintained);
enums as Postgres ENUM types; soft business state via `status` FSM columns;
JSONB for provider payload snapshots. All money in KRW integer (`*_krw`) or
source currency minor units with explicit `currency` column.

## Entity overview

```
trend_candidates ─▶ products ─┬▶ product_sources (1..n)
                              ├▶ listings (per marketplace) ─▶ price_history
                              └▶ risk_flags
orders ─▶ purchases ─▶ shipments        orders ─▶ order_events (audit)
inquiries · claims                      brand_leads · preorder_campaigns
approval_requests · event_outbox · llm_calls · fx_rates · blocked_rules
```

## DDL sketches (authoritative shapes; Claude Code writes Alembic migrations)

```sql
-- ================= intelligence =================
CREATE TABLE trend_candidates (
  id BIGINT PK,
  source TEXT NOT NULL,                    -- tiktok_cc | amazon_us_ms | ali_rising | rakuten_rank | manual
  external_key TEXT NOT NULL,              -- source-side id/url
  name_raw TEXT NOT NULL,
  name_norm TEXT NOT NULL,
  image_url TEXT, image_phash TEXT,
  category_guess TEXT,
  accel_score NUMERIC, gap_score NUMERIC,
  kr_keywords JSONB,                       -- [{kw, monthly_search, rank}]
  saturation JSONB,                        -- {smartstore_count, coupang_count, price_band}
  status TEXT NOT NULL DEFAULT 'DISCOVERED',
    -- DISCOVERED→VALIDATED→CLEARED→SOURCED | REJECTED(reason)
  reject_reason TEXT,
  first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ,
  UNIQUE (source, external_key)
);
CREATE INDEX ON trend_candidates (status, accel_score DESC);

CREATE TABLE brand_leads (
  id BIGINT PK, brand_name TEXT, country TEXT, homepage TEXT,
  dossier JSONB, fit_score NUMERIC,
  status TEXT DEFAULT 'FOUND',  -- FOUND→APPROVED_OUTREACH→CONTACTED→NEGOTIATING→SIGNED→DROPPED
  contact JSONB, notes TEXT
);

-- ================= catalog =================
CREATE TABLE products (
  id BIGINT PK,
  candidate_id BIGINT NULL REFERENCES trend_candidates,
  origin_route TEXT NOT NULL,              -- trend | longtail
  canonical_name_ko TEXT, canonical_name_src TEXT,
  brand TEXT, category_naver TEXT, category_internal TEXT,
  attributes JSONB,                        -- normalized {size,color,material,weight_g,...}
  images JSONB,                            -- [{url, role, checked}]
  risk_status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING→CLEARED→BLOCKED
  status TEXT NOT NULL DEFAULT 'ACTIVE'    -- ACTIVE | RETIRED
);

CREATE TABLE product_sources (
  id BIGINT PK, product_id BIGINT REFERENCES products,
  marketplace TEXT NOT NULL,               -- rakuten | amazon_jp | amazon_us | yahoo_jp | ebay
  url TEXT NOT NULL, seller_name TEXT, seller_rating NUMERIC,
  currency TEXT, price_minor BIGINT,       -- last seen price
  stock_state TEXT,                        -- IN_STOCK | OOS | UNKNOWN
  shipping_class TEXT, weight_g INT,
  variant_map JSONB, rank SMALLINT DEFAULT 1,
  last_checked_at TIMESTAMPTZ,
  UNIQUE (product_id, url)
);
CREATE INDEX ON product_sources (last_checked_at);          -- StockMonitor sweep
CREATE INDEX ON product_sources (stock_state);

CREATE TABLE risk_flags (
  id BIGINT PK, product_id BIGINT, candidate_id BIGINT,
  kind TEXT NOT NULL,        -- ip | cert_kc | cert_radio | children | food | cosmetics | battery | mkt_banned
  detail JSONB, severity TEXT,             -- BLOCK | REVIEW
  decided_by TEXT, decided_at TIMESTAMPTZ  -- system | human:<user>
);

CREATE TABLE blocked_rules (               -- versioned compliance rules
  id BIGINT PK, kind TEXT, pattern TEXT, note TEXT, active BOOL DEFAULT true
);

-- ================= listings =================
CREATE TABLE listings (
  id BIGINT PK, product_id BIGINT REFERENCES products,
  marketplace TEXT NOT NULL,               -- naver | coupang
  store_account TEXT NOT NULL,
  remote_product_id TEXT, remote_url TEXT,
  title TEXT, content JSONB,               -- generated content bundle
  sell_price_krw INT, margin_krw INT, margin_rate NUMERIC,
  status TEXT NOT NULL DEFAULT 'DRAFT',
    -- DRAFT→CONTENT_READY→PENDING_PUBLISH→LIVE→SUSPENDED_STOCKOUT→RETIRED | FAILED
  publish_batch_id BIGINT NULL,
  stats JSONB,                             -- rolling {clicks, orders_30d, cancels}
  scan_tier SMALLINT DEFAULT 2,            -- 1=hot(6h) 2=normal(24h)
  UNIQUE (marketplace, store_account, remote_product_id)
);
CREATE INDEX ON listings (status, marketplace);
CREATE INDEX ON listings (scan_tier, status);

CREATE TABLE price_history (
  id BIGINT PK, listing_id BIGINT, source_id BIGINT,
  src_price_minor BIGINT, fx NUMERIC, landed_krw INT, sell_price_krw INT,
  reason TEXT,                             -- initial | fx_move | src_price | manual
  at TIMESTAMPTZ DEFAULT now()
);

-- ================= orders (FSM) =================
CREATE TYPE order_status AS ENUM (
  'NEW','PURCHASE_PENDING','PURCHASE_APPROVED','PURCHASED',
  'INBOUND_TO_FORWARDER','FORWARDER_RECEIVED','INTL_SHIPPING','CUSTOMS',
  'DOMESTIC_SHIPPING','DELIVERED','SETTLED',
  'HOLD_STOCKOUT','HOLD_PCCC','CANCELLED','REFUND_IN_PROGRESS','RETURNED');

CREATE TABLE orders (
  id BIGINT PK,
  marketplace TEXT, remote_order_id TEXT, remote_order_item_id TEXT,
  listing_id BIGINT REFERENCES listings,
  qty INT, unit_sell_krw INT, buyer_name TEXT, buyer_contact TEXT,
  pccc TEXT,                               -- 개인통관고유부호 (encrypted at rest, see 07)
  ship_to JSONB,                           -- encrypted at rest
  status order_status NOT NULL DEFAULT 'NEW',
  margin_snapshot JSONB,                   -- economics at order time
  UNIQUE (marketplace, remote_order_item_id)
);
CREATE INDEX ON orders (status);

CREATE TABLE order_events (                -- append-only audit of FSM transitions
  id BIGINT PK, order_id BIGINT, from_status TEXT, to_status TEXT,
  actor TEXT,                              -- agent:<name> | human:<user> | webhook
  detail JSONB, at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE purchases (
  id BIGINT PK, order_id BIGINT REFERENCES orders,
  source_id BIGINT REFERENCES product_sources,
  src_order_id TEXT, paid_minor BIGINT, currency TEXT, fx NUMERIC,
  paid_at TIMESTAMPTZ, payment_method TEXT,
  status TEXT DEFAULT 'PREPARED'           -- PREPARED→PAID→CONFIRMED→PROBLEM
);

CREATE TABLE shipments (
  id BIGINT PK, order_id BIGINT,
  forwarder TEXT, forwarder_ref TEXT,
  src_tracking TEXT, intl_tracking TEXT, kr_tracking TEXT, kr_carrier TEXT,
  stage TEXT, last_movement_at TIMESTAMPTZ, stalled BOOL DEFAULT false,
  events JSONB
);
CREATE INDEX ON shipments (stalled) WHERE stalled;

-- ================= CS =================
CREATE TABLE inquiries (
  id BIGINT PK, marketplace TEXT, remote_inquiry_id TEXT UNIQUE,
  order_id BIGINT NULL, listing_id BIGINT NULL,
  question TEXT, klass TEXT, confidence NUMERIC,
  draft_answer TEXT, sent_answer TEXT, auto_sent BOOL,
  status TEXT DEFAULT 'OPEN'               -- OPEN→ANSWERED→ESCALATED→CLOSED
);

CREATE TABLE claims (
  id BIGINT PK, order_id BIGINT, kind TEXT,     -- cancel | return | refund | dispute | delay
  fault TEXT, resolution JSONB,
  status TEXT DEFAULT 'OPEN', money_out_krw INT DEFAULT 0
);

-- ================= middle/top tiers =================
CREATE TABLE preorder_campaigns (
  id BIGINT PK, product_id BIGINT,
  window_start DATE, window_end DATE, target_qty INT, sold_qty INT DEFAULT 0,
  campaign_price_krw INT, batch_economics JSONB,
  status TEXT DEFAULT 'PROPOSED'  -- PROPOSED→APPROVED→LIVE→CLOSED→PROCURED→FULFILLED→CANCELLED
);

-- ================= platform / system =================
CREATE TABLE approval_requests (
  id BIGINT PK, kind TEXT NOT NULL,        -- publish_batch | purchase_pay | risk_review | refund | campaign | brand_outreach | cs_draft
  ref_table TEXT, ref_id BIGINT,
  summary TEXT, evidence JSONB, proposed_action JSONB,
  status TEXT DEFAULT 'PENDING',           -- PENDING→APPROVED→DENIED→EXPIRED
  decided_by TEXT, decided_at TIMESTAMPTZ, expires_at TIMESTAMPTZ
);
CREATE INDEX ON approval_requests (status, kind);

CREATE TABLE event_outbox (
  id BIGINT PK, stream TEXT, type TEXT, idempotency_key TEXT UNIQUE,
  payload JSONB, published BOOL DEFAULT false, published_at TIMESTAMPTZ
);
CREATE INDEX ON event_outbox (published) WHERE NOT published;

CREATE TABLE processed_events (            -- consumer-side idempotency
  consumer TEXT, idempotency_key TEXT, at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (consumer, idempotency_key)
);

CREATE TABLE llm_calls (
  id BIGINT PK, agent TEXT, task TEXT, tier TEXT, model TEXT,
  prompt_tokens INT, completion_tokens INT, cost_est NUMERIC,
  latency_ms INT, cache_hit BOOL, ok BOOL, err TEXT,
  trace_id TEXT, at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON llm_calls (agent, at);

CREATE TABLE fx_rates (
  pair TEXT, rate NUMERIC, at TIMESTAMPTZ, PRIMARY KEY (pair, at)
);

CREATE TABLE app_config (                  -- runtime-tunable flags (HITL, limits, thresholds)
  key TEXT PRIMARY KEY, value JSONB, updated_by TEXT, updated_at TIMESTAMPTZ
);
```

## Order FSM — allowed transitions (enforced in code, audited in order_events)

```
NEW ─▶ PURCHASE_PENDING ─▶ PURCHASE_APPROVED ─▶ PURCHASED ─▶ INBOUND_TO_FORWARDER
 │            │                                   │
 │            └─▶ HOLD_STOCKOUT ─▶ CANCELLED      └─(src problem)▶ REFUND_IN_PROGRESS
 ├─▶ HOLD_PCCC ─▶ PURCHASE_PENDING (pccc received)
FORWARDER_RECEIVED ─▶ INTL_SHIPPING ─▶ CUSTOMS ─▶ DOMESTIC_SHIPPING ─▶ DELIVERED ─▶ SETTLED
any pre-PURCHASED state ─▶ CANCELLED (customer/system)
DELIVERED ─▶ RETURNED / REFUND_IN_PROGRESS (claim path)
```

Rules: transitions only via `OrderService.transition(order, to, actor, detail)`;
illegal transition raises + DLQs the event; every transition writes `order_events`
and may emit domain events.

## Data retention & PII

`pccc`, `ship_to`, `buyer_contact` encrypted at rest (pgcrypto or app-level
envelope); purge/anonymize 6 months after SETTLED (config). Marketplace payload
snapshots kept 90 days. See 07 for policy.
