# 07 — Risk & Compliance (guardrails are product features here)

> Scope note: this file encodes *operating rules for the system*. It is planning
> input, not legal/tax advice — items marked ⚖️ must be confirmed with the
> operator's tax accountant / as-of-today regulations before or during M0/M1,
> and the confirmed values written into `app_config` + this doc.

## 1. Legal & tax (Korea, 구매대행 specifics)

- **통신판매업 신고** required before selling; business registration must include
  the purchasing-agency (구매대행) activity. ⚖️
- **VAT structure — the big one.** A qualifying 구매대행 business can recognize
  **commission only** as revenue (not gross merchandise value). Qualification
  hinges on operating as a true agent: customs cleared in the buyer's name
  (personal import, PCCC), buyer-facing disclosure that this is purchase agency,
  evidence separating goods cost vs. service fee, no inventory ownership.
  System requirements that support this: order records snapshot goods
  cost/fees separately (`margin_snapshot`), listings carry the 구매대행 disclosure
  block (ContentAgent injects it in code), customs always under buyer PCCC.
  Confirm exact documentary requirements with the tax accountant in M0. ⚖️
- **Customs / de minimis:** personal-use imports ≤ USD 150 (≤ USD 200 from US
  goods) generally exempt; system computes `customs_est` per category and never
  structures split-shipments to evade thresholds (합산과세 risk — same buyer,
  same day arrivals are aggregated). Encode: warn & hold orders that would breach
  aggregation rules. ⚖️
- **PCCC (개인통관고유부호):** collected per order, encrypted at rest, used only
  for customs, purged per retention policy.
- **Consumer law:** 전자상거래법 distance-selling rules — clear lead-time
  disclosure, cancellation/refund policy consistent with marketplace policy,
  overseas-agency return terms stated. Templates reviewed once in M1, then frozen.

## 2. Blocked & restricted categories (fail closed)

| Class | Examples | System action |
|---|---|---|
| Certification required | electrical appliances (KC), radio/BT/wireless (전파법), children's products (어린이제품법), helmets | **BLOCK** at RiskFilter; category+keyword rules + LLM screen |
| Regulated import | food, supplements, cosmetics, medical/quasi-drug, contact lenses | **BLOCK** |
| Hazard/transport | standalone lithium batteries, aerosols, knives beyond kitchen norms | **BLOCK** |
| IP risk | character goods, brand logos, replicas, fan merch | **BLOCK**; lexicon + image screen; ambiguous → human REVIEW, never pass |
| Marketplace-banned | per Naver/Coupang policy lists | **BLOCK**; sync list quarterly |

`blocked_rules` is versioned; loosening any rule requires operator approval with a
written rationale row. RiskFilter failure mode = REVIEW, never PASS.
Roadmap note: certification categories are a *future opportunity* (barrier =
moat) but only via formal import — out of scope for the zero-inventory phase.

## 3. Marketplace account health (the asset to protect)

- Publish-rate limits + human-quality content (L3 spec) to avoid bulk-listing
  abuse flags; monitor listing rejections; 3 rejections same-cause → pause
  pipeline, fix template.
- Seller-fault cancellation is the most damaging metric in a zero-inventory model:
  last-second source check in OrderAgent + StockMonitor SLA are the defenses;
  cancel-rate breach >2% weekly → auto-throttle new publishes until fixed.
- Penalty/strike events (IP report, false advertising) = SEV-1 incident: pipeline
  pause, postmortem, rule added.
- Diversification later (Coupang, 2nd store) reduces single-account risk — but
  only after M3 stability.

## 4. Operational risks

| Risk | Mitigation |
|---|---|
| Stale stock/price (top risk) | StockMonitor SLA + alerts; naive checker mandatory even in M1 |
| FX swings | hourly fx refresh; `fx_buffer` in pricing; reprice events on >1.5% moves |
| Forwarder failure/loss | 2 forwarders per route from M2; stall detection; insurance option on high-value |
| Source seller cancels | rank-2 source fallback per product; else fast refund path (protects metrics) |
| Scraper breakage | parse-yield monitoring per source; fixtures for parser tests; Playwright fallback |
| LLM outage/rate-limit | tier circuit breakers; queue non-critical work; CS falls back to human drafts |
| Card/payment blocks | daily purchase caps; bank pre-notification; backup payment method |
| Aggregated customs (합산과세) surprise for buyer | order-hold rule + buyer notice template |

## 5. Data protection

- PII (name, contact, address, PCCC) encrypted at rest; access only via
  OrderAgent/LogisticsTracker code paths; never sent to the LLM except the
  minimum needed inside CS drafts (name only), never PCCC/address in prompts.
- Retention: purge/anonymize PII 6 months post-SETTLED; snapshots 90d.
- Secrets in env/secret store; keys rotated on operator change of device.
- LLM inputs from customers/scraped pages are untrusted (prompt-injection posture
  in 05 §Reliability).

## 6. Financial controls

- Auto-pay limits (per-order, per-day) in `app_config`; every money-out
  (purchase, refund) has an approval or an auto-policy row linked in audit.
- Daily reconciliation job (M2): marketplace settlements vs. orders vs. purchases;
  discrepancies to Approval Queue.
- Monthly books export for the accountant (orders, commissions, goods costs,
  fees) in the structure agreed in M0. ⚖️

## 7. Incident response (lightweight)

SEV-1 = money leak, policy strike, PII exposure, StockMonitor down >12h.
Action: auto-throttle affected pipeline → operator page → fix → postmortem note in
`docs/notes/incidents/YYYY-MM-DD-slug.md` (what/impact/root cause/new guard) →
resume. No scale-up while a SEV-1 guard is unwritten.
