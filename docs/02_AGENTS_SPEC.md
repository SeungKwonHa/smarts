# 02 — Agent Specifications

Every agent implements `BaseAgent.handle(event) -> list[Event]` and is a pure
function of (event, DB state) → (DB writes, emitted events). LLM tier codes
(T0/T1/T2) are defined in `05_LLM_INTEGRATION.md`. Event names reference
`04_EVENT_CONTRACTS.md`.

Legend — **Trigger**: what invokes it · **HITL**: human approval gate (see matrix at end).

---

## Team I — Intelligence ("what to sell")

### I1. TrendScout
- **Trigger:** `tick.trend_scan` (every 6h, jittered).
- **Does:** Crawls trend sources (TikTok Creative Center top ads/hashtags by region,
  Amazon US Movers & Shakers, AliExpress rising rank, Rakuten ranking deltas).
  Extracts product signals; computes **acceleration score** = weighted growth rate
  vs. previous scans (views Δ%, ad-creative count Δ, rank jumps), not absolute volume.
- **LLM:** T0 for entity extraction from noisy titles/captions (product name,
  category guess, attribute set).
- **Writes:** upsert `trend_candidates` (dedup by normalized name + image phash).
- **Emits:** `candidate.discovered` for candidates above `score_threshold`.
- **Failure mode:** source layout change → parse yield drops → alert if yield <50% of trailing avg.

### I2. GapAnalyzer
- **Trigger:** `candidate.discovered`.
- **Does:** Measures Korea-side saturation: Naver search volume for mapped Korean
  keywords (DataLab / ad keyword tool), count + price band of existing
  SmartStore/Coupang listings for the same product (image/title matching).
  Computes `gap_score` = demand_signal / (1 + supply_count).
- **LLM:** T0 to generate candidate Korean search keywords from the foreign product name.
- **Writes:** `trend_candidates.gap_score`, saturation snapshot.
- **Emits:** `candidate.validated` (gap_score ≥ threshold) or `candidate.rejected(reason=saturated)`.

### I3. RiskFilter  ⛔ blocking
- **Trigger:** `candidate.validated` **and** re-screen of every `listing.requested`
  (defense in depth — also called synchronously by Listing team).
- **Does:** Screens for: (a) IP/character/brand infringement — brand-name & character
  lexicon match + T1 vision/text check on images ("does this depict a licensed
  character/logo?"); (b) certification-required categories (electrical, radio/BT,
  children's products); (c) regulated categories (food, supplements, cosmetics,
  medical devices, batteries standalone); (d) marketplace-banned items.
  Category blocklist lives in config + DB table `blocked_rules`, versioned.
- **LLM:** T1 (image+text). Ambiguity → escalate, never pass.
- **Writes:** `risk_flags` rows; candidate status.
- **Emits:** `candidate.cleared` / `candidate.rejected(reason=risk:*)` /
  `approval.requested(kind=risk_review)` for ambiguous cases.
- **Rule:** fail closed. LLM error or timeout = reject to review, not pass.

### I4. BrandScout (Top tier)
- **Trigger:** `tick.brand_scan` (daily); also promoted signals from Analytics
  (`brand.lead_suggested` when one foreign brand yields repeated Base wins).
- **Does:** Builds dossiers on overseas SMB brands: product line, Kickstarter/
  Makuake/Amazon review velocity, social growth, existing KR distribution
  (present? exclusive?), contact channels. Scores fit for Wadiz funding.
- **LLM:** T2 for dossier synthesis and outreach-email draft (English/Japanese).
- **Writes:** `brand_leads` (+ dossier JSONB).
- **Emits:** `approval.requested(kind=brand_outreach)` — human sends/negotiates;
  system drafts everything.

---

## Team L — Listing ("cast the net")

### L1. SourceMatcher
- **Trigger:** `candidate.cleared` (trend path) **or** `tick.longtail_expand`
  (base path: scheduled category sweeps of Rakuten/Amazon JP/US bestseller +
  niche-category crawls to feed the longtail net).
- **Does:** Finds purchasable source URL(s): search source marketplaces, verify
  same-product via title similarity + image embedding/phash, capture price,
  stock state, shipping class, seller rating, variant map (size/color).
- **LLM:** T0 for variant/spec normalization into canonical attribute schema.
- **Writes:** `products` (master, canonical attributes), `product_sources`
  (1..n per product, ranked).
- **Emits:** `product.sourced` or `candidate.rejected(reason=unsourceable)`.

### L2. PricingAgent  (no LLM — deterministic)
- **Trigger:** `product.sourced`; `price.reprice_required` (from StockMonitor/fx).
- **Formula:**
  ```
  landed = src_price*fx*(1+fx_buffer) + intl_ship_est(weight,route) + customs_est(category,landed_declare)
  sell   = ceil_to_pricepoint( (landed + domestic_ship + fixed_buffer)
                               / (1 - platform_fee - target_margin) )
  reject if margin_krw < min_margin_abs or sell > category_price_ceiling
  ```
  `customs_est`: 0 under de-minimis for list-clearance categories; else duty+VAT
  table by HS-group (config). Weight from source page or category default.
- **Writes:** `listings.draft_price`, `price_history`.
- **Emits:** `product.priced` / `candidate.rejected(reason=margin)`.

### L3. ContentAgent  — quality is the anti-abuse moat
- **Trigger:** `product.priced`.
- **Does:** Generates Korean listing content that reads human-made:
  - **Title spec (Naver SEO):** 25–50 Korean chars;
    pattern `[브랜드|원산지 신호] 핵심수요키워드 [핵심속성 1–2] 보조키워드`;
    keywords chosen from GapAnalyzer's ranked Korean keywords (demand-weighted);
    ban list: superlatives/medical claims/"정품보장" style risk words, emoji,
    keyword stuffing (no dup tokens); dedupe vs. our existing titles (trigram sim <0.6).
  - **Detail page:** structured sections (핵심 요약 → 스펙 표 → 사용 장면 →
    배송/통관 안내 → 교환·반품 규정) rendered to marketplace-safe HTML from a
    template; **mandatory 구매대행 disclosure block** (해외 구매대행 상품 고지,
    관세·개인통관고유부호 안내, 배송 기간) — legal requirement, injected by code
    not LLM.
  - Category mapping to Naver leaf category (T0 classify + cached mapping table);
    attribute fields filled; image set: source images passed through
    resize/whitespace pipeline (no watermark theft; skip images containing other
    sellers' overlays — T0 vision check).
- **LLM:** T1 generation, T0 checks. Structured JSON output validated by pydantic;
  1 retry then `listing.failed(reason=content)`.
- **Writes:** `listings` (status=DRAFT, content JSONB).
- **Emits:** `listing.content_ready`.

### L4. PublishAgent
- **Trigger:** `listing.content_ready`; respects daily publish rate limit
  (config `publish_rate_daily`, start 100–300, ramp with account health).
- **Does:** Creates the listing via Naver Commerce API (M1) / Coupang WING (M4).
  Validates response, stores remote product ID, verifies live URL.
- **HITL:** while `publish_auto=false` (M1 default) → batches to Approval Queue
  as spot-checkable list (approve-all UI with sampled previews).
- **Writes:** `listings` (status=LIVE, remote ids).
- **Emits:** `listing.published` / `listing.failed`.

---

## Team O — Operations ("the lifeline of zero-inventory")

### O1. StockMonitor  — highest priority agent in the system
- **Trigger:** `tick.stock_scan` (full sweep daily; hot SKUs — sold in last 14d —
  every 6h; sharded by `product_id % N`).
- **Does:** Re-checks every LIVE listing's source: in stock? price changed?
  variant gone? shipping class changed? Then:
  - price moved beyond tolerance → `price.reprice_required`
  - out of stock → immediately set marketplace listing to sold-out via API,
    status=SUSPENDED_STOCKOUT → `stock.changed(oos)`
  - back in stock → reactivate.
- **LLM:** none (parsers); T0 only when page parse is ambiguous.
- **SLA:** any LIVE listing's source data older than 36h = ALERT (dashboard red +
  operator notification). This agent failing silently is the #1 kill risk.

### O2. OrderAgent
- **Trigger:** `order.created` (Naver order poll/webhook via relay-web).
- **Does:** Creates `orders` row (FSM NEW), snapshots source price/URL, re-verifies
  source availability **now** (last-second check), computes real-time margin;
  prepares purchase instruction (source, variant, qty, forwarder warehouse
  address as shipping destination, order memo conventions).
  - margin still OK & in stock → `order.purchase_required`
  - source dead → auto-suspend listing + `claim.opened(kind=pre_ship_cancel)` path
    (customer notification + cancel per policy) — this must be rare (<2%).
- **Purchase execution:** M1–M2: Approval Queue "PAY" step — human clicks through
  a prefilled purchase (or approves a stored-card purchase task); M3+: auto-purchase
  under `auto_pay_limit_krw` per order and daily cap, above limit → HITL.
  Purchases recorded in `purchases` with source order id.
- **Emits:** `purchase.completed` → FSM PURCHASED.

### O3. LogisticsTracker
- **Trigger:** `tick.tracking_poll` (4h) + forwarder webhook/email parse.
- **Does:** Drives order FSM through
  `PURCHASED → INBOUND_TO_FORWARDER → FORWARDER_RECEIVED → INTL_SHIPPING →
  CUSTOMS → DOMESTIC_SHIPPING → DELIVERED` using source-shop tracking, forwarder
  portal (Playwright if no API), 17TRACK-style aggregator, and KR carrier API.
  Registers domestic tracking number back to the marketplace (발송처리).
  Detects stalls (no movement > threshold per stage) → proactive CS
  (`shipment.delayed`) before the customer complains. Handles PCCC
  (개인통관고유부호) collection state: missing → automated customer request
  message; present → attach to forwarder shipment.
- **Writes:** `shipments`, `order_events`.

---

## Team C — CS

### C1. InquiryAgent
- **Trigger:** `inquiry.received` (marketplace Q&A / talk polling).
- **Does:** Classifies (T0): tracking / spec question / pre-purchase / PCCC /
  cancel-refund intent / other. For tracking+PCCC+spec: composes grounded answer
  (T1) using ONLY DB facts (order state, listing spec) — template-constrained,
  polite 합쇼체, includes 구매대행 lead-time framing. Confidence < threshold or
  category=other → escalate to Approval Queue with a suggested draft.
- **Auto-send:** off in M2 (draft-only), on in M3 for tracking/PCCC classes first.

### C2. ClaimTriage
- **Trigger:** `claim.opened` (cancel/return/refund/dispute events).
- **Does:** LangGraph flow (T2): gather order timeline → classify fault
  (seller/customer/carrier/customs) → propose resolution per policy table
  (auto-approve full refund pre-shipment; partial/return cases → human) →
  draft customer message + marketplace action plan.
- **HITL:** any money-out action (refund) requires approval until M4 trust
  thresholds; pre-shipment cancellations auto after M3.

---

## Team A — Analytics

### A1. SKUManager
- **Trigger:** `tick.daily_report` post-aggregation.
- **Does:** Rolling stats per listing (impressions if available, clicks, orders,
  cancel rate). Policies: no sale & no click in N days → retire (delist) to keep
  account quality; risers → raise `stock_scan` frequency tier; chronic source
  instability → retire. Keeps live-SKU count within account health budget.
- **Emits:** `sku.retire`, `sku.tier_change`.

### A2. PromotionEngine  (the ladder)
- **Trigger:** weekly tick.
- **Does:** Base→Middle: SKUs with ≥X orders/30d & stable source & margin band →
  nominate pre-order/group-buy campaign (drafts campaign params: batch window,
  target qty, discounted price with batch shipping economics).
  Middle→Top: brand with ≥2 winning SKUs → `brand.lead_suggested` to BrandScout.
- **HITL:** campaign launch always human-approved (it's a public commitment).

### A3. Reporter
- **Trigger:** `tick.daily_report` (07:30 KST).
- **Does:** Daily digest to operator (dashboard + optional email/telegram):
  P&L by tier, orders funnel, cancel-rate, DLQ count, LLM spend vs. budget,
  approval queue backlog, top movers, alerts. Weekly deep report (T1 narrative
  over computed tables — numbers computed in SQL, LLM writes prose only).

---

## HITL (human-in-the-loop) matrix & graduation

| Gate | M1 | M2 | M3 | M4 | Auto criteria to graduate |
|---|---|---|---|---|---|
| Publish listings | batch approve | sampled 10% | auto | auto | 2 wks, 0 policy strikes |
| Purchase payment | manual pay | approve-click | auto ≤ `auto_pay_limit` (start 50k KRW) | auto ≤ raised limit | fraud/cancel rate <1% |
| Risk ambiguous | human | human | human | human | never auto |
| CS auto-send | — | drafts only | tracking/PCCC auto | most classes auto | CSAT/complaint watch |
| Refund money-out | human | human | pre-ship auto | policy-table auto | dispute rate stable |
| Pre-order campaign launch | — | — | human | human | never auto |
| Brand outreach send | — | human | human | human | never auto |

Approval Queue UX requirement: every pending item shows agent's proposed action +
evidence + one-click approve/deny/edit; bulk ops; target total ≤30 min/day.
