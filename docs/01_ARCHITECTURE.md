# 01 — System Architecture

## Design principles

1. **Event-driven teams, not one mega-graph.** Five agent teams run as independent
   worker processes with different cadences (6-hourly trend scans vs. real-time order
   handling). They communicate only via the event bus and the database.
2. **Postgres = single source of truth.** Agents are stateless workers; all business
   state (products, listings, orders, approvals) lives in Postgres with explicit
   state machines.
3. **LLM as a swappable utility, not the architecture.** One gateway client
   (LongCat now; anything OpenAI-compatible later). Agents request *task tiers*,
   not model names.
4. **Fail closed on compliance, fail loud on operations.** RiskFilter blocks; a
   StockMonitor outage pages the operator.
5. **Boring infra until it hurts.** Single VPS, Docker Compose, Redis Streams,
   Postgres. No Kubernetes/Kafka before 50k SKUs.

## Topology

```
                          ┌──────────────────────────────────────────────┐
                          │                 SCHEDULER (cron)             │
                          │  trend scan 6h · stock scan daily · fx 1h    │
                          └───────┬──────────────────────────┬───────────┘
                                  ▼                          ▼
┌─────────────────┐   events   ┌──────────────────────────────────────────┐
│  INTELLIGENCE   │──────────▶ │            REDIS STREAMS (event bus)     │
│ TrendScout      │            │ relay:intel  relay:listing  relay:ops    │
│ GapAnalyzer     │ ◀──────────│ relay:cs  relay:analytics  relay:approve │
│ RiskFilter      │            │ relay:dlq                                │
│ BrandScout      │            └───┬─────────┬─────────┬─────────┬────────┘
└─────────────────┘                ▼         ▼         ▼         ▼
                          ┌──────────┐ ┌──────────┐ ┌────────┐ ┌───────────┐
                          │ LISTING  │ │OPERATIONS│ │   CS   │ │ ANALYTICS │
                          │ SourceM. │ │ StockMon │ │Inquiry │ │ SKUMgr    │
                          │ Pricing  │ │ OrderAg  │ │ Claim  │ │ PromoEng  │
                          │ Content  │ │ Logisti. │ └────┬───┘ │ Reporter  │
                          │ Publish  │ └────┬─────┘      │     └─────┬─────┘
                          └────┬─────┘      │            │           │
                               ▼            ▼            ▼           ▼
        ┌──────────────────────────────────────────────────────────────────┐
        │                     POSTGRES (source of truth)                   │
        │  products · listings · orders(FSM) · purchases · shipments ·     │
        │  trend_candidates · approvals · llm_calls · price/stock history  │
        └──────────────────────────────────────────────────────────────────┘
               ▲                        ▲                          ▲
        ┌──────┴───────┐        ┌───────┴────────┐         ┌───────┴───────┐
        │ APPROVAL     │        │  DASHBOARD     │         │ EXTERNAL APIs │
        │ QUEUE (web)  │        │  (read-only)   │         │ Naver·Coupang │
        │ human 30m/day│        │  P&L·agents·DLQ│         │ Rakuten·AmzJP │
        └──────────────┘        └────────────────┘         │ forwarder·fx  │
                                                           │ LongCat LLM   │
                                                           └───────────────┘
```

## Tech stack

| Layer | Choice | Rationale |
|---|---|---|
| Language | Python 3.12, asyncio, type-hinted | operator fluency, ecosystem |
| Agent framework | Plain async workers; **LangGraph only** for multi-step LLM flows (ContentAgent, ClaimTriage, BrandScout analysis) | avoid framework tax; operator knows LangGraph from AANCA |
| Event bus | **Redis Streams** + consumer groups | at-least-once, replay, XAUTOCLAIM for retries, zero extra infra |
| DB | Postgres 16 + Alembic | FSMs, JSONB payloads, analytics via SQL |
| HTTP | httpx (async) | |
| Scraping | httpx-first; **Playwright** fallback for JS-heavy pages (forwarder portals, TikTok CC) | |
| Scheduler | APScheduler inside a dedicated `scheduler` process emitting tick events | one place owns cadence |
| LLM | LongCat API via OpenAI-compatible client behind `core/llm` (see 05) | prepaid 50B tokens |
| Observability | **Langfuse** (self-host or cloud) for LLM traces; structlog JSON logs; Prometheus-style counters table for MVP | |
| UI (Approval Queue, Dashboard) | FastAPI + HTMX (server-rendered) | fastest to ship; no SPA build chain |
| Deploy | Docker Compose on 1 VPS (4 vCPU / 16GB) + managed Postgres optional | |
| Secrets/config | `.env` + pydantic-settings; per-env overrides | |

## Process model (Docker Compose services)

```
relay-scheduler      # emits tick events (trend.scan, stock.scan, fx.refresh, report.daily)
relay-intelligence   # consumes intel ticks + candidate events
relay-listing        # consumes listing.* events
relay-operations     # consumes ops ticks, order webhooks/polls, shipment events
relay-cs             # consumes inquiry/claim events
relay-analytics      # consumes daily ticks + sale events
relay-web            # FastAPI: approval queue, dashboard, health, webhook receivers
redis / postgres / langfuse (optional)
```

Each worker = `python -m relay.<team> run` → subscribes to its consumer group,
processes events with `BaseAgent.handle(event) -> list[Event]`, commits DB writes
and emitted events **in one transaction pattern** (outbox table → relay to Redis)
to guarantee no lost events.

## Reliability rules

- **Outbox pattern:** agents write emitted events to `event_outbox` in the same DB
  transaction as their state changes; a relay loop publishes to Redis and marks sent.
- **Idempotency:** every event has `idempotency_key`; consumers upsert on it.
  External writes (create listing, place order) store the remote ID before acting
  again.
- **Retries:** consumer failure → XAUTOCLAIM retry ×3 with backoff → `relay:dlq`
  with error payload. DLQ items appear in the Dashboard and (if money/CS-related)
  in the Approval Queue.
- **Circuit breakers:** per external API (Naver, Rakuten, LongCat) — open after N
  consecutive failures, alert operator, queue work instead of dropping.
- **Backpressure:** listing pipeline pulls at a configured rate (e.g., ≤500 new
  listings/day/store initially) to avoid marketplace anti-abuse triggers.

## Repository layout

```
relay/
├── CLAUDE.md
├── docs/                      # these design docs
├── pyproject.toml
├── docker-compose.yml
├── alembic/                   # migrations
├── prompts/                   # versioned prompt templates (jinja2), by agent
│   ├── content_agent/title_v1.j2
│   ├── content_agent/detail_v1.j2
│   ├── risk_filter/ip_screen_v1.j2
│   └── cs/inquiry_reply_v1.j2
├── src/relay/
│   ├── core/
│   │   ├── config.py          # pydantic-settings
│   │   ├── db.py              # engine, session, outbox helpers
│   │   ├── events.py          # Event envelope, publish/consume, DLQ
│   │   ├── agent.py           # BaseAgent, run loop, retries, metrics
│   │   ├── llm/               # client.py (LongCat gateway), tiers.py, cache.py
│   │   ├── fx.py              # exchange-rate service
│   │   └── approval.py        # HITL gate helper
│   ├── intelligence/          # trend_scout.py gap_analyzer.py risk_filter.py brand_scout.py
│   ├── listing/               # source_matcher.py pricing.py content.py publisher.py
│   ├── operations/            # stock_monitor.py order_agent.py logistics.py
│   ├── cs/                    # inquiry.py claim_triage.py
│   ├── analytics/             # sku_manager.py promotion.py reporter.py
│   ├── integrations/          # naver/ coupang/ rakuten/ amazon_jp/ amazon_us/
│   │                          # forwarders/ tracking/ tiktok_cc/ wadiz/
│   └── apps/                  # web/ (FastAPI: approvals, dashboard, webhooks)
└── tests/                     # per-agent contract tests with fixture events
```

## Environments

- `dev`: local Compose, sandbox seller account, LLM tier forced to cheapest,
  external writes mocked by default (`RELAY_DRY_RUN=1` prints intended actions).
- `prod`: real accounts; HITL flags per agent as in the matrix (02 §HITL).

## Scale path (later, do not pre-build)

30k→100k SKUs: split StockMonitor into sharded workers by `product_id % N`;
move heavy crawl fleet to separate hosts; consider Kafka only if Redis Streams
consumer lag becomes structural; add read replica for analytics.
