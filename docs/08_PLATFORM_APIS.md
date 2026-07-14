# 08 — External Platforms & API Notes

> Every integration below gets a `docs/notes/<name>_verified.md` written by Claude
> Code at build time with: auth flow actually used, endpoints touched, rate limits
> observed, quirks. This file sets expectations and constraints; **verify current
> docs before coding — marketplace APIs change frequently.**

## Selling side

### Naver SmartStore — Commerce API (M1, primary)
- Auth: application registered in the Commerce API center; OAuth2 client-
  credentials style token per seller account. **Application/approval takes time →
  M0 day-1 task.** Until granted, PublishAgent runs in `EXPORT` mode: generates
  bulk-upload spreadsheets matching seller-center mass-registration format so
  M1 listing work isn't blocked. (This bootstrap mode is throwaway-by-design.)
- Needed capabilities: product create/update/status, category & attribute lookup,
  origin-area (원산지) and overseas-shipping fields, order list/detail, dispatch
  (발송처리) with tracking, inquiry list/answer, penalty/quality info if exposed.
- Constraints to respect: image spec (size/format), forbidden words in titles,
  overseas-purchase product flags, per-day registration behavior. Log every 4xx
  with payload for fast template fixes.
- Poll orders every 5m (webhooks not assumed).

### Coupang WING OpenAPI (M4)
- HMAC-signed requests; seller approval + category permissions; stricter listing
  validation and rocket-badge irrelevant here (seller-delivery overseas).
  Deferred: account needs aging + Naver loop must be stable first.

### Wadiz (Top tier, M4)
- No public listing API assumed → campaign creation is human+assisted (BrandScout
  produces the full submission pack: copy, images brief, pricing, logistics plan).

## Sourcing side

### Rakuten Ichiba (JP, M1)
- Official **Rakuten Web Service** APIs exist (item search/ranking) with app-id
  registration — use them first (stable, legal, fast) and fall back to page parse
  only for fields the API lacks (true stock state, variant matrices).
- Note affiliate parameters are irrelevant; we need catalog data + live price.

### Amazon JP / Amazon US (M1 JP, M2 US)
- PA-API requires associate status with sales quota → assume unavailable initially.
  Fallback: respectful page fetch for product pages we already selected (price,
  availability, variants), aggressive caching, low rate, rotate schedules.
  Checkout automation is NOT built (ToS + fragility): purchases happen via
  prefilled human flow (M1–M2) → stored-payment task with approve-click; revisit
  automation options only with a compliant purchasing route.
- Amazon Movers & Shakers pages = TrendScout source (public pages).

### Yahoo! Shopping JP / eBay (optional M3+)
- Yahoo has an open item-search API (app-id) — cheap to add for JP breadth.
- eBay Browse API viable for US niche/collectibles later.

## Trend sources (Intelligence)

- **TikTok Creative Center** top ads/hashtags/products by region — public web,
  JS-heavy → Playwright with cached sessions, 6h cadence, low volume.
- Amazon Movers & Shakers (US/JP), AliExpress rising rankings (signal for what
  will commoditize — used as a *negative* filter for Base and an early-warning
  for exit timing), Rakuten ranking deltas.
- Naver DataLab / searchad keyword tool for KR demand volume (GapAnalyzer):
  searchad API needs an ad account (free to open) — M3 task; before that, use
  DataLab public endpoints + relative signals.

## Forwarders (배대지)

- Selection criteria: JP + US warehouses, per-kg pricing transparency,
  consolidation optional (we mostly ship single-parcel per order), API or at
  least stable web portal, PCCC handling, insurance option, weekend cutoffs.
- Reality: most Korean forwarders have **no public API** → `integrations/forwarders/`
  implements a portal driver interface (`register_shipment`, `get_status`,
  `get_fee_quote`) with a Playwright implementation per forwarder + email-parse
  fallback. Keep drivers thin and fixture-tested; portals change.
- Two forwarders per route by M2 (failover + price check).

## Utilities

- **FX:** hourly KRW/JPY, KRW/USD from a free reliable source (e.g., exchangerate
  host-class API or bank open API); store in `fx_rates`; pricing uses latest + buffer.
- **Tracking aggregation:** 17TRACK-style API for intl legs + KR carrier APIs
  (CJ, Post) for domestic; LogisticsTracker normalizes stages.
- **Image pipeline:** local processing (resize, pad to spec, strip overlays-
  flagged images) — no external service needed initially.

## Rate-limit & etiquette defaults (config)

| Target | Default |
|---|---|
| Rakuten API | per official quota |
| Amazon page fetch | ≤1 req / 3–5s / domain, cache 6h, backoff on 503 |
| TikTok CC | 1 session / 6h cadence |
| Naver Commerce API | per official quota; poll orders 5m, inquiries 10m |
| Forwarder portals | ≤1 action/s, business-hours batching |

All fetchers go through `core/http.py` (shared client: UA policy, retries,
per-domain rate limiter, response cache) — no ad-hoc `httpx.get` in agents.
