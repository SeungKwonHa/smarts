# 00 — Project Brief

## One-line definition

RELAY is a **zero-inventory, agent-operated cross-border commerce system**: it discovers
sellable products from Japan/US marketplaces, lists them on Korean marketplaces at scale,
fulfills each order per-purchase through forwarding agents (배대지), and continuously
promotes proven products up a margin ladder — with a human acting only as a
30-minutes-per-day supervisor.

## Why this can win (competitive thesis)

1. **Automation as the moat.** In longtail cross-border reselling, margin rate is
   capped (~15–25%), so profit = SKU count × turnover. SKU count is bounded by
   operational throughput. Manual sellers manage ~500 SKUs; solution-tool sellers
   ~5,000; a custom agent pipeline manages 30,000–100,000. Almost no Korean
   구매대행 seller can build this in-house. The operator can (AI/ML engineer,
   has shipped a LangGraph multi-agent pipeline before — AANCA).
2. **Geographic time-lag arbitrage.** Products trend on US/CN TikTok 1–3 months
   before reaching Korea. Detecting acceleration abroad + confirming absence in
   Korean marketplaces = a repeatable "golden window."
3. **Ali/Temu-proof positioning.** We deliberately avoid the segment AliExpress/Temu
   killed (cheap Chinese commodities). Sourcing is Japan/US brand + longtail goods,
   plus categories with structural barriers (see 07).

## Business model — three-tier ladder (all zero-inventory)

| Tier | Model | Margin | Role |
|---|---|---|---|
| **Base** | JP/US longtail 구매대행, 30k+ SKUs, SEO-driven, no ads | 15–25% | Cash flow + market sensor |
| **Middle** | Pre-order / group-buy (예약판매·공동구매) of proven SKUs; batch procurement after payment collected | 25–40% | Margin booster, still zero inventory (customer money buys the goods) |
| **Top** | Discover overseas SMB brands early → exclusive Korea distribution → Wadiz crowdfunding (paid pre-orders → then procure) | 30–50% | High margin + defensive moat (exclusivity) |

**Promotion logic:** Base sales data → SKUs with repeat demand auto-nominated to
Middle → brands with repeat product wins nominated to Top outreach.

## Hard constraints (non-negotiable)

- **Zero inventory.** No speculative stock purchases, ever. Every procurement is
  triggered by a paid customer order (Base) or a closed pre-order batch (Middle/Top).
- **Zero-tolerance compliance.** IP-infringing, certification-required
  (KC/radio/children), and regulated (food/supplement/cosmetics/medical) products
  are blocked at the pipeline level. One counterfeit incident can kill the store.
- **Human time budget: ≤30 min/day** at steady state, spent in the Approval Queue.
- **LLM execution runs on the LongCat API** (prepaid 50B-token pack). Architecture
  must keep the LLM provider swappable behind one client.

## Explicit non-goals

- No stocking/사입, no domestic 3PL warehousing, no Rocket Growth in v1.
- No Chinese commodity sourcing (Taobao/1688) for the Base tier.
- No own-brand OEM manufacturing in the first 12 months.
- No paid advertising in M1–M2 (SEO-only validation); small test budgets allowed in M3+.

## Unit economics (planning numbers — validate in M1)

Per Base-tier order (illustrative, JP sourcing, 30,000 KRW basket):

```
Sell price                          30,000 KRW
- Item cost (JP)                   -16,000   (incl. JPY→KRW at fx_rate × 1.03 buffer)
- Intl shipping share (forwarder)   -3,500
- Domestic last-mile                -3,000   (often bundled in forwarder fee)
- Marketplace fee (~6% avg Naver)   -1,800
- Payment/misc buffer                 -700
= Contribution                      ~5,000 KRW (≈17%)
```

Target trajectory:

| Milestone | Live SKUs | Orders/day | Est. monthly contribution |
|---|---|---|---|
| M1 exit (wk 4) | 1,000 | 1–5 | pocket money — goal is loop validation |
| M2 exit (wk 8) | 5,000 | 10–20 | ~2–3M KRW |
| M3 exit (wk 12) | 10,000+ | 25–50 | ~4–8M KRW |
| Month 6–9 | 30,000+ Base + Middle live | 80–150 | 10M+ KRW/mo (net ≥1,000만원 target) |

Key sensitivity: **cancellation rate from stale stock data.** Above ~5% seller-fault
cancellations, marketplace ranking penalties compound. This is why StockMonitor is
priority #1 among operations agents.

## Success metrics (system-level)

- `automation_ratio` = actions executed without human touch / all actions (target ≥95% by M3)
- `human_minutes_per_day` (target ≤30 by M3)
- `seller_fault_cancel_rate` (target <2%)
- `listing_yield` = published listings / candidate products entering pipeline
- `golden_hit_rate` = trend candidates that produce ≥5 sales in 14 days
- `contribution_margin_krw` per tier, daily

## Roles

- **Operator (하승권):** Approval Queue decisions, brand outreach calls (Top tier),
  strategy. Not a data-entry worker.
- **Claude Code:** builds and maintains the system per these docs.
- **LongCat API:** runtime inference for all agents (see 05).
