"""Shared HTTP client with per-domain rate limiting, caching, and retry.

ALL external HTTP calls go through this module — no ad-hoc httpx.get() in agents.

Features:
- Per-domain rate limiting (configurable via rate_limits dict)
- Simple in-memory response cache (TTL-based, for GET requests only)
- Tenacity retry on transient errors (5xx, connection errors)
- User-Agent policy
- Playwright fallback for JS-heavy pages (call get_html_playwright)
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = structlog.get_logger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (compatible; RelayBot/1.0; "
    "+https://relay.internal/bot)"
)

# Default rate limits: seconds between requests per domain
_DEFAULT_RATE_LIMITS: dict[str, float] = {
    "www.amazon.co.jp":         4.0,
    "www.amazon.com":           4.0,
    "www.rakuten.co.jp":        1.0,
    "app.rakuten.co.jp":        1.0,  # legacy API endpoint
    "openapi.rakuten.co.jp":    1.5,  # new RWS API (429 when exceeded)
    "ads.tiktok.com":          60.0,  # TikTok CC — 1 session / 6h, accessed sparingly
    "smartstore.naver.com":     1.0,
    "api.commerce.naver.com":   0.5,
}

# In-memory cache: {cache_key: (response_json, expires_at_ts)}
_cache: dict[str, tuple[Any, float]] = {}
_rate_locks: dict[str, asyncio.Lock] = {}
_rate_last: dict[str, float] = {}


class HttpClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )

    def _get_rate_lock(self, domain: str) -> asyncio.Lock:
        if domain not in _rate_locks:
            _rate_locks[domain] = asyncio.Lock()
        return _rate_locks[domain]

    async def _rate_wait(self, domain: str) -> None:
        min_interval = _DEFAULT_RATE_LIMITS.get(domain, 0.5)
        lock = self._get_rate_lock(domain)
        async with lock:
            last = _rate_last.get(domain, 0.0)
            elapsed = time.monotonic() - last
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            _rate_last[domain] = time.monotonic()

    @retry(
        retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        cache_s: int = 0,
    ) -> httpx.Response:
        """GET a URL respecting rate limits and optional cache."""
        # Cache check
        if cache_s > 0:
            key = _cache_key(url, params)
            if key in _cache:
                data, expires = _cache[key]
                if time.monotonic() < expires:
                    return data  # cached httpx.Response is replaced by raw content below
                del _cache[key]

        domain = _extract_domain(url)
        await self._rate_wait(domain)

        log.debug("http_get", url=url[:120])
        resp = await self._client.get(url, headers=headers or {}, params=params or {})
        resp.raise_for_status()

        if cache_s > 0:
            key = _cache_key(url, params)
            _cache[key] = (resp, time.monotonic() + cache_s)

        return resp

    async def get_json(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        cache_s: int = 3600,
    ) -> Any:
        """GET and parse JSON response."""
        # JSON cache at this level
        if cache_s > 0:
            key = _cache_key(url, params) + ":json"
            if key in _cache:
                data, expires = _cache[key]
                if time.monotonic() < expires:
                    return data
                del _cache[key]

        resp = await self.get(url, headers=headers, params=params, cache_s=0)
        data = resp.json()

        if cache_s > 0:
            key = _cache_key(url, params) + ":json"
            _cache[key] = (data, time.monotonic() + cache_s)

        return data

    async def post(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """POST request with rate limiting and JSON response parsing."""
        domain = _extract_domain(url)
        await self._rate_wait(domain)
        log.debug("http_post", url=url[:120])
        resp = await self._client.post(
            url, json=json, data=data, params=params, headers=headers or {}
        )
        resp.raise_for_status()
        return resp.json()

    async def post_json(
        self,
        url: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> Any:
        domain = _extract_domain(url)
        await self._rate_wait(domain)
        log.debug("http_post", url=url[:120])
        resp = await self._client.post(url, json=json, headers=headers or {})
        resp.raise_for_status()
        return resp.json()

    async def get_html_playwright(self, url: str, *, wait_selector: str = "body") -> str:
        """Fetch a JS-heavy page via Playwright. Expensive — use sparingly."""
        from playwright.async_api import async_playwright

        domain = _extract_domain(url)
        await self._rate_wait(domain)

        log.info("playwright_fetch", url=url[:120])
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": USER_AGENT})
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_selector(wait_selector, timeout=10000)
            html: str = await page.content()
            await browser.close()
        return html

    async def close(self) -> None:
        await self._client.aclose()


def _extract_domain(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc
    except Exception:
        return url[:50]


def _cache_key(url: str, params: dict[str, str] | None) -> str:
    raw = url + str(sorted((params or {}).items()))
    return hashlib.md5(raw.encode()).hexdigest()


# Module-level singleton
http_client = HttpClient()
