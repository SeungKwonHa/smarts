# CLAUDE.md — Project Instructions for Claude Code

## What this project is

**Project codename: RELAY** — a fully automated, zero-inventory (무재고) cross-border
e-commerce operation run by orchestrated AI agents. We source products from Japan/US
marketplaces, list them on Korean marketplaces (Naver SmartStore, Coupang), fulfill
orders per-purchase through forwarding agents (배대지), and continuously discover
trending products before they reach Korea.

Read the docs in this order before writing any code:

1. `docs/00_PROJECT_BRIEF.md` — business model, constraints, success metrics
2. `docs/01_ARCHITECTURE.md` — system topology, tech stack, deployment model
3. `docs/02_AGENTS_SPEC.md` — every agent's contract (input/output/trigger/HITL)
4. `docs/03_DATA_MODEL.md` — Postgres schema (single source of truth)
5. `docs/04_EVENT_CONTRACTS.md` — event bus message schemas
6. `docs/05_LLM_INTEGRATION.md` — LongCat API client, model tiering, cost control
7. `docs/06_ROADMAP.md` — build order with acceptance criteria (FOLLOW THIS ORDER)
8. `docs/07_RISK_COMPLIANCE.md` — legal/platform guardrails (NON-NEGOTIABLE)
9. `docs/08_PLATFORM_APIS.md` — external API notes and bootstrap constraints

## Hard rules

- **Build order is law.** Milestone M1 (money loop) before any intelligence features.
  Do not build TrendScout before PublishAgent works end-to-end. See `06_ROADMAP.md`.
- **Postgres is the single source of truth.** Agents are stateless workers. No agent
  holds business state in memory, files, or LLM context between runs.
- **Every LLM call goes through `core/llm/client.py`** (the LongCat gateway). No
  direct HTTP calls to LLM providers scattered in agent code. Model tier is selected
  by task type, not hardcoded model names. See `05_LLM_INTEGRATION.md`.
- **Every agent action that spends money or publishes publicly** (order payment,
  listing publication in early phase, refunds) goes through the Approval Queue
  (human-in-the-loop) until its auto-approval flag is enabled in config.
  See HITL matrix in `02_AGENTS_SPEC.md`.
- **Compliance filters are blocking, not advisory.** A product that fails
  RiskFilter (IP infringement, KC certification required, regulated category)
  must never reach the listing pipeline. Fail closed.
- **Idempotency everywhere.** Every event consumer must be safe to re-run.
  Every external write (listing creation, order placement) must check for an
  existing record first via idempotency keys.
- **No scraping behavior that violates target sites' ToS beyond ordinary
  rate-limited public-page access.** Respect robots.txt where feasible,
  randomize schedules, cache aggressively, and prefer official APIs when they exist.

## Code conventions

- Language: Python 3.12+, type-hinted throughout. `ruff` + `mypy` clean.
- All code, comments, docstrings, commit messages: **English**.
  Korean appears only in data (product names, CS messages) and prompt templates
  that generate Korean output.
- Package layout: monorepo, `src/relay/` root package. One subpackage per team
  (`intelligence/`, `listing/`, `operations/`, `cs/`, `analytics/`), plus
  `core/` (db, events, llm, config) and `apps/` (approval-queue UI, dashboard).
- Async-first: `asyncio` + `httpx` for all I/O. Crawlers use `playwright` only
  when static fetch fails.
- Config via environment (`pydantic-settings`). Secrets never committed.
- Every agent = one class implementing `core.agent.BaseAgent` with
  `handle(event) -> list[Event]`. Pure function of (event, db state) → (db writes, events).
- Tests: pytest. Every agent gets contract tests with fixture events. External
  APIs mocked; one live smoke test per integration behind a flag.
- Migrations: Alembic. Never hand-edit schema in production.

## What NOT to do

- Do not add Kafka/RabbitMQ/Kubernetes in Phase 1. Redis Streams + a process
  supervisor is enough until >50k SKU. Optimize for iteration speed.
- Do not use LangChain's high-level chains. LangGraph for stateful multi-step
  agent flows only where genuinely needed (ContentAgent, ClaimTriage); plain
  async functions for everything else.
- Do not implement Coupang integration in M1. SmartStore first (see `08_PLATFORM_APIS.md`
  for the API bootstrap constraint).
- Do not generate listings by direct machine translation. ContentAgent must
  follow the SEO title generation spec in `02_AGENTS_SPEC.md` §L3.
- Do not silently swallow failures from StockMonitor. Stale price/stock data is
  the #1 business-killing failure mode in a zero-inventory model — alert loudly.
