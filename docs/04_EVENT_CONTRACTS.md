# 04 — Event Contracts (Redis Streams)

## Envelope (every message)

```json
{
  "event_id": "uuid7",
  "type": "order.purchase_required",
  "version": 1,
  "occurred_at": "2026-07-14T09:12:33+09:00",
  "producer": "agent:order_agent",
  "idempotency_key": "order:18841:purchase_required",
  "correlation_id": "order:18841",
  "payload": { }
}
```

Rules:
- `idempotency_key` is deterministic from business identity (never random) —
  consumers upsert into `processed_events (consumer, idempotency_key)` and skip dups.
- `correlation_id` threads a business object across teams (candidate:*, product:*,
  order:*) for tracing.
- Producers never publish directly: write to `event_outbox` in the same DB txn as
  state changes; the outbox relay publishes to Redis and flips `published`.
- Payloads carry **IDs + minimal denormalized fields** consumers need for routing;
  consumers read full state from Postgres. No fat payloads.
- Schema changes: additive only within a `version`; breaking change bumps `version`
  and consumers must accept N and N-1 during migration.

## Streams & consumer groups

| Stream | Producers | Consumer groups |
|---|---|---|
| `relay:ticks` | scheduler | each team's tick consumer |
| `relay:intel` | Intelligence | gap_analyzer, risk_filter, source_matcher |
| `relay:listing` | Listing | pricing, content, publisher, analytics |
| `relay:ops` | web(webhooks), Operations | order_agent, logistics, stock_monitor, cs, analytics |
| `relay:cs` | web(polls), CS | inquiry, claim_triage |
| `relay:analytics` | Analytics | promotion, reporter, brand_scout |
| `relay:approvals` | approval app | originating agents (resume on decision) |
| `relay:dlq` | core retry wrapper | dashboard/alerting |

Delivery: at-least-once. Consumer group per agent; `XAUTOCLAIM` reclaims messages
idle > 5 min; per-message retry counter in metadata; after 3 failures → publish to
`relay:dlq` with `{original, error, traceback_summary, attempts}` and ACK original.
DLQ items with `kind in (purchase, refund, publish)` also open an approval_request.

## Tick events (scheduler-owned cadence)

| type | cadence | consumed by |
|---|---|---|
| `tick.trend_scan` | 6h (jitter ±20m) | TrendScout |
| `tick.brand_scan` | daily 03:00 | BrandScout |
| `tick.longtail_expand` | daily 04:00 (category rotation param) | SourceMatcher |
| `tick.stock_scan` | daily 05:00 full; 6h for tier-1 | StockMonitor |
| `tick.fx_refresh` | 1h | core.fx |
| `tick.order_poll` | 5m | web poller → order.created |
| `tick.inquiry_poll` | 10m | web poller → inquiry.received |
| `tick.tracking_poll` | 4h | LogisticsTracker |
| `tick.daily_report` | 07:00 KST | SKUManager→Reporter |
| `tick.weekly_promotion` | Mon 06:00 | PromotionEngine |

## Domain events (payload key fields only)

### Intelligence
- `candidate.discovered` — {candidate_id, source, accel_score}
- `candidate.validated` — {candidate_id, gap_score, kr_keywords_top3}
- `candidate.cleared` — {candidate_id}                      (risk pass)
- `candidate.rejected` — {candidate_id, reason}             (saturated|risk:*|unsourceable|margin)
- `brand.lead_found` — {brand_lead_id, fit_score}
- `brand.lead_suggested` — {brand_name, evidence_skus[]}    (from Analytics)

### Listing
- `product.sourced` — {product_id, candidate_id?, source_count}
- `product.priced` — {product_id, listing_id, sell_price_krw, margin_rate}
- `listing.content_ready` — {listing_id}
- `listing.published` — {listing_id, remote_product_id, remote_url}
- `listing.failed` — {listing_id, stage, reason}
- `price.reprice_required` — {listing_id, cause: fx_move|src_price|manual, delta}

### Operations
- `order.created` — {order_id, marketplace, listing_id, qty}
- `order.purchase_required` — {order_id, source_id, est_cost_minor, margin_krw}
- `purchase.approved` — {order_id, approval_id}             (HITL resume)
- `purchase.completed` — {order_id, purchase_id, src_order_id}
- `stock.changed` — {source_id, product_id, state: oos|restock|price, old, new}
- `shipment.updated` — {order_id, stage, tracking}
- `shipment.delayed` — {order_id, stage, stalled_hours}
- `order.delivered` — {order_id}

### CS
- `inquiry.received` — {inquiry_id, klass?}
- `inquiry.answered` — {inquiry_id, auto_sent}
- `claim.opened` — {claim_id, order_id, kind}
- `claim.resolved` — {claim_id, resolution, money_out_krw}
- `claim.escalated` — {claim_id, reason}

### Analytics / lifecycle
- `sku.retire` — {listing_id, reason}
- `sku.tier_change` — {listing_id, scan_tier}
- `sku.promote_preorder` — {product_id, proposed_campaign}
- `report.daily_ready` — {date, summary_ref}

### Approvals (HITL)
- `approval.requested` — {approval_id, kind, ref, summary}
- `approval.granted` / `approval.denied` — {approval_id, kind, ref, decided_by, note}

Agents that pause on HITL implement resume-on-event: original event handler writes
state=WAITING_APPROVAL + approval_request, and a separate handler for
`approval.granted(kind=X)` continues the flow. No in-memory waiting.

## Testing contract

`tests/fixtures/events/*.json` holds one golden fixture per event type; every
agent's contract test = feed fixture → assert DB writes + emitted events. Adding a
new event type requires: schema doc entry here, fixture, and producer+consumer tests.
