"""Rakuten Ichiba sourcing client.

Uses the NEW Rakuten Web Service (RWS) IchibaItem/Search API on the
openapi.rakuten.co.jp platform (UUID app ID + pk_ access key).

Auth:  applicationId (query param) + accessKey (query param)
Base:  https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260701

Docs: https://webservice.rakuten.co.jp/explorer/api/IchibaItem/Search
Rate:  ~1 req/s (429 returned when exceeded)

Falls back to page fetch only for fields the API lacks (true stock state
on some seller types, detailed variant matrices).
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

import structlog

from relay.core.config import settings
from relay.core.http import http_client

log = structlog.get_logger(__name__)

# New RWS IchibaItem/Search endpoint (v20260701, openapi.rakuten.co.jp)
_BASE_URL = "https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260701"
_RANK_URL = "https://openapi.rakuten.co.jp/ichibaranking/api/IchibaItem/Ranking/20220601"

# Naver/Korean search price ceiling (JPY); anything above is likely a bundle
_MAX_PRICE_JPY = 50_000


def _credentials() -> tuple[str, str]:
    if not settings.rakuten_app_id:
        raise RuntimeError(
            "RAKUTEN_APP_ID not set. Register at https://webservice.rakuten.co.jp/"
        )
    if not settings.rakuten_access_key:
        raise RuntimeError(
            "RAKUTEN_ACCESS_KEY not set. Get it from the RWS app dashboard."
        )
    return settings.rakuten_app_id, settings.rakuten_access_key


# ── Product model ─────────────────────────────────────────────────────────────

class RakutenItem:
    """Canonical representation of a single Rakuten product."""

    __slots__ = (
        "item_code", "name", "url", "price_jpy", "seller_name",
        "seller_rating", "in_stock", "shipping_class", "image_urls",
        "description", "genre_id", "raw",
    )

    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = raw
        # New RWS wraps item fields in an inner "Item" dict
        item = raw.get("Item", raw)
        self.item_code: str = item.get("itemCode", "")
        self.name: str = item.get("itemName", "")
        self.url: str = item.get("itemUrl", item.get("affiliateUrl", ""))
        self.price_jpy: int = int(item.get("itemPrice", 0))
        self.seller_name: str = item.get("shopName", "")
        self.seller_rating: float = float(item.get("reviewAverage", 0.0))
        # New RWS: availability=1 means IN-STOCK (opposite of legacy API)
        self.in_stock: bool = item.get("availability", 1) == 1
        # postageFlag: 0=include, 1=exclude
        self.shipping_class: str = "free" if item.get("postageFlag", 0) == 0 else "paid"
        imgs = item.get("mediumImageUrls") or item.get("smallImageUrls") or []
        self.image_urls: list[str] = []
        for i in imgs:
            if isinstance(i, dict):
                url = i.get("imageUrl", "")
            else:
                url = str(i)
            if url:
                # New RWS may return protocol-relative URLs
                if url.startswith("//"):
                    url = "https:" + url
                self.image_urls.append(url)
        self.description: str = item.get("itemCaption", "")[:2000]
        self.genre_id: str = str(item.get("genreId", ""))

    @property
    def stock_state(self) -> str:
        return "IN_STOCK" if self.in_stock else "OOS"

    def __repr__(self) -> str:
        return f"<RakutenItem {self.item_code!r} ¥{self.price_jpy}>"


# ── Search ────────────────────────────────────────────────────────────────────

async def search(
    keyword: str,
    *,
    genre_id: str = "",
    page: int = 1,
    hits: int = 30,
    sort: str = "-reviewCount",   # default: most-reviewed first
    cache_s: int = 0,            # no cache by default (fresh pricing is critical)
) -> list[RakutenItem]:
    """Search Rakuten Ichiba for items matching keyword.

    Args:
        keyword: Search query (Japanese or English).
        genre_id: Rakuten genre ID to scope search (optional).
        page: Page number (1-indexed).
        hits: Results per page (max 30).
        sort: Sort order (-reviewCount | -itemPrice | +itemPrice | standard).
        cache_s: In-memory cache TTL in seconds (0 = no cache).

    Returns:
        List of RakutenItem objects.
    """
    app_id, access_key = _credentials()
    params: dict[str, str] = {
        "applicationId": app_id,
        "accessKey": access_key,
        "keyword": keyword,
        "page": str(page),
        "hits": str(min(hits, 30)),
        "sort": sort,
    }
    if genre_id:
        params["genreId"] = genre_id

    data = await http_client.get_json(_BASE_URL, params=params, cache_s=cache_s)
    items = data.get("Items", [])
    results = []
    for item_wrapper in items:
        try:
            # New RWS wraps each result in {"Item": {...}}
            ri = RakutenItem(item_wrapper)
            if 0 < ri.price_jpy <= _MAX_PRICE_JPY:
                results.append(ri)
        except Exception:
            log.debug("rakuten_item_parse_error", raw=str(item_wrapper)[:200])
    log.debug("rakuten_search", keyword=keyword, count=len(results))
    return results


async def get_item(item_code: str, *, cache_s: int = 0) -> RakutenItem | None:
    """Fetch a single item by itemCode for price/stock refresh.

    The new RWS doesn't expose a separate IchibaItem/Get endpoint,
    so this searches by itemCode (which is a unique enough keyword proxy)
    and returns the first match.
    """
    app_id, access_key = _credentials()
    params = {
        "applicationId": app_id,
        "accessKey": access_key,
        "keyword": item_code,
        "hits": "3",
    }
    try:
        data = await http_client.get_json(_BASE_URL, params=params, cache_s=cache_s)
        items = data.get("Items", [])
        for wrapper in items:
            ri = RakutenItem(wrapper)
            if ri.item_code == item_code:
                return ri
        # Fallback: return first result if exact match not found
        if items:
            return RakutenItem(items[0])
        return None
    except Exception as e:
        log.warning("rakuten_get_item_error", item_code=item_code, error=str(e))
        return None


async def get_ranking(
    genre_id: str = "",
    *,
    page: int = 1,
    hits: int = 30,
    cache_s: int = 0,
) -> list[RakutenItem]:
    """Fetch bestseller ranking for a genre (used by SourceMatcher for longtail)."""
    app_id, access_key = _credentials()
    params: dict[str, str] = {
        "applicationId": app_id,
        "accessKey": access_key,
        "page": str(page),
        "hits": str(min(hits, 30)),
    }
    if genre_id:
        params["genreId"] = genre_id

    data = await http_client.get_json(_RANK_URL, params=params, cache_s=cache_s)
    items = data.get("Items", [])
    results = []
    for item_wrapper in items:
        try:
            ri = RakutenItem(item_wrapper)
            if 0 < ri.price_jpy <= _MAX_PRICE_JPY:
                results.append(ri)
        except Exception:
            pass
    log.debug("rakuten_ranking", genre_id=genre_id, count=len(results))
    return results


# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_item_code_from_url(url: str) -> str | None:
    """Extract Rakuten itemCode from a product URL for use in get_item()."""
    # https://item.rakuten.co.jp/shopname/itemcode/
    m = re.search(r"rakuten\.co\.jp/[^/]+/([^/?#]+)", url)
    if m:
        return m.group(1)
    return None
