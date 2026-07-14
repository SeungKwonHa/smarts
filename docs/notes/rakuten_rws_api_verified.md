# Rakuten RWS API — Verified Configuration (2026-07-14)

## Status: ✅ WORKING

The new Rakuten Web Service (RWS) platform uses a completely different API
stack from the old `app.rakuten.co.jp` endpoints. This is NOT the legacy
Ichiba API — UUID app IDs and `pk_` access keys only work on the new platform.

## Verified Endpoints

| Purpose | URL | Status |
|---------|-----|--------|
| Item Search | `https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260701` | ✅ Working |
| Ranking | `https://openapi.rakuten.co.jp/ichibaranking/api/IchibaItem/Ranking/20220601` | ✅ Working |
| Item Detail (Get) | ❌ Deprecated | Use search with itemCode as keyword |

## Authentication

Two query parameters on every request:
- `applicationId` = UUID-format app ID (e.g., `43d6559f-7ecf-4b21-bcdf-92e3ec7f2a8a`)
- `accessKey` = `pk_`-prefixed key (e.g., `pk_VL0uhGYu460YJMT8shxBQFalKqINJ1wzkzS8ZvKnVAK`)

## Key Differences from Legacy API

1. **Base domain**: `openapi.rakuten.co.jp` (not `app.rakuten.co.jp`)
2. **Path prefix**: `ichibams/api/` for search, `ichibaranking/api/` for ranking
3. **Version**: `20260701` (search), `20220601` (ranking) — NOT `20170706`
4. **Auth**: `applicationId` + `accessKey` query params (no Bearer <REDACTED> headers)
5. **Response format**: Items wrapped in `{"Item": {...}}` (extra nesting level)
6. **Availability semantics**: `1` = IN-STOCK, `0` = OOS (FLIPPED from legacy)
7. **Rate limit**: ~1 req/s per domain (HTTP 429 when exceeded)
8. **No `format=json`** needed — JSON is the default/only format
9. **`IchibaItem/Get`** endpoint is deprecated — use search with itemCode keyword

## Response Structure (Search)

```json
{
  "count": 4740371,
  "page": 1,
  "first": 1,
  "last": 5,
  "hits": 5,
  "pageCount": 100,
  "Items": [
    {
      "Item": {
        "itemName": "...",
        "itemCode": "shop:code",
        "itemPrice": 2780,
        "itemUrl": "https://item.rakuten.co.jp/...",
        "shopName": "...",
        "reviewAverage": 4.5,
        "availability": 1,
        "postageFlag": 0,
        "mediumImageUrls": [{"imageUrl": "https://..."}],
        "itemCaption": "...",
        "genreId": "100804"
      }
    }
  ]
}
```

## Genre IDs (Verified Working 2026-07-14)

| Category | Genre ID | Notes |
|----------|----------|-------|
| Kitchen gadgets | `100804` | キッチン用品・調理器具 |
| Stationery | `101240` | 文房具・オフィス用品 — may include books |
| Hobby | `101213` | ホビー・おもちゃ |
| Camping/Outdoor | ⚠️ Unstable | `100628` returns 404 (genre reorganized) |

Note: Rakuten periodically reorganizes genre IDs. The camping category needs
to be discovered dynamically (search by keyword as fallback).

## Rate Limits

- Search: ~1.5 req/s (safety margin on our side)
- Ranking: ~1 req/s
- HTTP 429 returned when exceeded — retry after 1s

## Files Updated

- `src/relay/integrations/rakuten/client.py` — full rewrite for new RWS
- `src/relay/core/config.py` — added `rakuten_access_key` setting
- `src/relay/core/http.py` — added rate limit for `openapi.rakuten.co.jp`
- `.env` — added `RAKUTEN_ACCESS_KEY`
