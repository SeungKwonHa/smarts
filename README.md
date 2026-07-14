# RELAY — Agent-Operated Zero-Inventory Cross-Border Commerce

Planning package for building RELAY with **Claude Code** (builder) and the
**LongCat API** (runtime LLM). Drop this folder at the root of a new repo.

## Contents

| File | What it locks down |
|---|---|
| `CLAUDE.md` | Rules Claude Code must follow: build order, conventions, hard guardrails |
| `docs/00_PROJECT_BRIEF.md` | Business model (3-tier ladder), zero-inventory constraint, economics, success metrics |
| `docs/01_ARCHITECTURE.md` | Event-driven topology, tech stack, repo layout, reliability (outbox, DLQ, idempotency) |
| `docs/02_AGENTS_SPEC.md` | Contracts for all 15 agents across 5 teams + HITL matrix & graduation criteria |
| `docs/03_DATA_MODEL.md` | Postgres schema, order FSM, PII handling |
| `docs/04_EVENT_CONTRACTS.md` | Event envelope, streams, retry/DLQ policy, every event schema |
| `docs/05_LLM_INTEGRATION.md` | LongCat gateway, T0/T1/T2 tier routing, prompts, structured output, cost control |
| `docs/06_ROADMAP.md` | M0–M4 milestones with binding build order and exit criteria |
| `docs/07_RISK_COMPLIANCE.md` | Legal/tax/platform/PII guardrails (fail-closed rules) |
| `docs/08_PLATFORM_APIS.md` | Naver/Coupang, Rakuten/Amazon, forwarders, fx, tracking — integration expectations |

## Kickoff prompt for Claude Code (copy-paste)

```
Read CLAUDE.md, then docs/00 through docs/08 in order. Then:
1. Confirm your understanding of the M0 scope and list any contradictions or
   gaps you find across the docs (do not start coding yet).
2. Execute M0 (docs/06): repo scaffold per docs/01, Alembic baseline from
   docs/03, core event/outbox/agent/LLM-gateway modules, dry-run mode, and the
   LongCat API verification note (docs/05). Show the outbox replay test passing.
3. Stop at the M0 exit criteria and wait for my review before starting M1.
Constraints reminder: build order is law (money loop before intelligence),
RiskFilter fails closed, all LLM calls via the gateway, everything idempotent.
```

## Operator's parallel M0 checklist (not code — see docs/06 M0)

사업자 업종 추가 + 통신판매업 신고, 구매대행 전문 세무사 상담(수수료 매출 인정 요건),
스마트스토어 셀러 + Commerce API 신청(1일차), 쿠팡 WING 가입(숙성용),
일본/미국 배대지 각 1곳 계약, LongCat API 키 발급, 해외결제 카드 한도 설정.

## Change management

These docs are the source of truth. When implementation reveals a better design,
update the doc in the same PR as the code — divergence between docs and code is a
bug. Numeric thresholds (limits, rates, budgets) live in `app_config`, not in docs
or code.
