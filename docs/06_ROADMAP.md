# 06 — Roadmap & Build Order

**Prime directive: the money loop before intelligence.** A trend detector without a
publish/fulfill loop earns 0 KRW. Build order below is binding for Claude Code.

---

## M0 — Bootstrap (week 0–1) · mostly operator tasks, system scaffolding

Operator (checklist, outside code):
- [ ] Business registration: add 통신판매업 신고 + 구매대행 업종 코드 to the existing
      sole proprietorship; **consult a 구매대행-specialized tax accountant on the
      commission-based VAT structure before first sale** (see 07 §Tax).
- [ ] Naver SmartStore seller account + Commerce API application (see 08 —
      approval can take days/weeks → start day 1).
- [ ] Coupang WING seller signup (account aging starts now; integration is M4).
- [ ] Forwarder (배대지) accounts: 1× Japan, 1× US; note portal/API capabilities.
- [ ] LongCat platform: confirm API access on the prepaid pack, issue key.
- [ ] Payment method for source purchases (JPY/USD-capable card; per-day limit set).

Claude Code:
- [ ] Repo scaffold per 01 layout; Compose with postgres+redis; Alembic baseline
      from 03; core: config, db+outbox, events (publish/consume/DLQ), BaseAgent,
      LLM gateway with tier routing + `llm_calls` logging; `RELAY_DRY_RUN` mode.
- [ ] Verify LongCat API per 05 and write `docs/notes/longcat_api_verified.md`.
- [ ] Approval Queue app v0 (list/approve/deny) + health dashboard v0 (DLQ, agents).

**Exit criteria:** `docker compose up` green; a demo event flows
scheduler→consumer→outbox→second consumer with idempotent replay proven in tests.

---

## M1 — Money Loop (weeks 1–4) · goal: first real orders, loop validated

Scope: **SmartStore only, Japan sourcing only, 2 starter categories** (pick from:
kitchen gadgets, stationery/desk, hobby/collectible accessories, camping small
goods — certification-free zones), target **1,000 live SKUs**.

Build (in order):
1. `integrations/rakuten` + `integrations/amazon_jp` product fetchers (search +
   product page parse → canonical product + source).
2. SourceMatcher (longtail path via `tick.longtail_expand` with a category seed list)
   → PricingAgent (full formula, fx service) → ContentAgent (title/detail per L3
   spec, Korean disclosure block) → PublishAgent (Naver Commerce API create;
   HITL batch approve).
3. RiskFilter v1: rule/lexicon layer + T1 text screen (vision if supported) —
   wired as blocking gate before ContentAgent.
4. Order path v1: order poller → OrderAgent (last-second source check, margin
   snapshot, purchase instruction → Approval Queue "PAY" flow with prefilled
   details; operator executes payment) → manual forwarder handoff assisted by
   generated instructions → LogisticsTracker v0 (manual tracking paste UI +
   marketplace 발송처리 API call).
5. Reporter v0: daily digest (listings, orders, margins, DLQ).

**Non-goals:** TrendScout, GapAnalyzer, Coupang, CS automation (answer manually in
seller center), StockMonitor beyond a naive daily price/OOS re-fetch for LIVE SKUs
(naive version IS required — never sell unverified stock).

**Exit criteria (all must hold):**
- 1,000 LIVE listings; publish yield ≥60% of sourced candidates.
- ≥10 real orders fulfilled end-to-end; FSM history complete for each.
- Seller-fault cancellations ≤2 total; zero compliance strikes.
- Unit economics sheet updated with REAL numbers (fees, forwarder, fx) in 00.

---

## M2 — Operations Automation (weeks 5–8) · goal: scale without operator pain

1. **StockMonitor v1 (top priority):** full daily sweep + tier-1 6h sweep; auto
   sold-out/reactivate via API; reprice on fx/src moves; staleness SLA alert.
2. OrderAgent v2: stored-payment purchase task with one-click approve
   (`purchase.approved` resume); forwarder portal automation (Playwright) for
   shipment registration where no API.
3. LogisticsTracker v1: tracking aggregation, stage FSM, stall detection →
   proactive delay messages (drafts); PCCC collection automation.
4. CS: InquiryAgent (classify + drafts, no auto-send); ClaimTriage v1 (policy
   table, pre-shipment cancel drafts).
5. SKUManager v1 (retire dead SKUs, scan tiers). Scale to **5,000 SKUs**, expand
   to 4–6 categories, add US sourcing (Amazon US) if JP loop is stable.

**Exit criteria:** 5,000 LIVE SKUs; operator time ≤60 min/day measured; stock-data
staleness p95 <30h; order touchpoints per order ≤2 clicks; cancel rate <2%.

---

## M3 — Intelligence + Scale (weeks 9–12) · goal: the edge turns on

1. TrendScout + GapAnalyzer live; golden-list flow into existing pipeline;
   **trend→listing latency target <48h.**
2. Analytics: PromotionEngine (Base→Middle nominations), weekly report narrative.
3. CS auto-send for tracking/PCCC classes; purchase auto-pay under limit
   (start 50,000 KRW/order, 500,000 KRW/day).
4. Scale to **10,000+ SKUs**; publish-rate ramp per account health; LLM eval
   suite (05) gating template changes.
5. First pre-order campaign (Middle tier) run manually-assisted end to end.

**Exit criteria:** automation_ratio ≥95%; human ≤30 min/day; ≥3 golden-list SKUs
with ≥5 sales in 14d; first pre-order campaign closed & fulfilled; monthly
contribution ≥4M KRW run-rate.

---

## M4 — Expansion (month 4+) · sequenced, not parallel

1. Coupang WING integration (listing mirror + order path) once account eligible.
2. Top tier: BrandScout live → first 2 brand outreaches → target 1 signed
   exclusive → Wadiz campaign playbook (docs/notes/wadiz_playbook.md to be written
   from the first run).
3. Multi-store architecture (second SmartStore) if category split warrants.
4. Reverse direction study (역직구: Qoo10 JP / Shopee) — reuse pipeline mirrored.
5. **SaaS spin-off evaluation:** if internal tooling is stable, scope selling the
   listing+monitoring pipeline to other sellers (separate product decision —
   requires operator go).

---

## Standing cadence (from M1)

- Weekly: review exit-criteria dashboard vs. this file; adjust thresholds in
  `app_config`, not in code.
- Any incident that costs money or a policy strike → postmortem note in
  `docs/notes/incidents/` + a new automated guard before resuming scale-up.
