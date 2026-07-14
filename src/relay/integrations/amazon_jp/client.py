"""Amazon Japan product page fetcher.

PA-API requires associate status with sales quota — assumed unavailable initially.
We do respectful page fetch for product pages we already selected, with:
- Aggressive caching (6h)
- Low rate (4s between requests, configured in http.py)
- Parse price, availability, title, images, ASIN
- Playwright fallback for JS-heavy pages (rare)

No automation of checkout or cart — purchases happen via prefilled human flow
as documented in 08_PLATFORM_APIS.md.
"""

from __future__ import annotations

import re
from typing import Any

import structlog
from bs4 import BeautifulSoup

from relay.core.http import http_client

log = structlog.get_logger(__name__)

_DOMAIN = "https://www.amazon.co.jp"
_PRODUCT_URL = "https://www.amazon.co.jp/dp/{asin}"
_MOVERS_URL  = "https://www.amazon.co.jp/gp/movers-and-shakers/{category}"

# Known out-of-stock indicators in Japanese
_OOS_PATTERNS = [
    "現在在庫切れ", "在庫なし", "入荷未定", "お取り扱いできません",
    "Currently unavailable", "out of stock",
]


class AmazonJPItem:
    """Parsed representation of an Amazon JP product."""

    __slots__ = (
        "asin", "title", "url", "price_jpy", "in_stock",
        "seller_name", "image_urls", "description", "rating", "raw_html",
    )

    def __init__(
        self,
        asin: str,
        title: str = "",
        price_jpy: int = 0,
        in_stock: bool = True,
        seller_name: str = "",
        image_urls: list[str] | None = None,
        description: str = "",
        rating: float = 0.0,
        raw_html: str = "",
    ) -> None:
        self.asin = asin
        self.title = title
        self.url = _PRODUCT_URL.format(asin=asin)
        self.price_jpy = price_jpy
        self.in_stock = in_stock
        self.seller_name = seller_name
        self.image_urls = image_urls or []
        self.description = description
        self.rating = rating
        self.raw_html = raw_html

    @property
    def stock_state(self) -> str:
        return "IN_STOCK" if self.in_stock else "OOS"

    def __repr__(self) -> str:
        return f"<AmazonJPItem {self.asin!r} ¥{self.price_jpy}>"


async def get_product(asin: str, *, cache_s: int = 6 * 3600) -> AmazonJPItem | None:
    """Fetch and parse an Amazon JP product page by ASIN.

    Returns None on parse failure or 404.
    Rate limited to 4s/req by http.py config.
    """
    url = _PRODUCT_URL.format(asin=asin)
    try:
        resp = await http_client.get(url, cache_s=cache_s)
        html = resp.text
    except Exception as e:
        log.warning("amazon_jp_fetch_error", asin=asin, error=str(e))
        return None

    return _parse_product_page(asin, html, url)


async def get_product_playwright(asin: str) -> AmazonJPItem | None:
    """Playwright fallback for heavily JS-rendered pages."""
    url = _PRODUCT_URL.format(asin=asin)
    try:
        html = await http_client.get_html_playwright(url, wait_selector="#dp-container")
        return _parse_product_page(asin, html, url)
    except Exception as e:
        log.warning("amazon_jp_playwright_error", asin=asin, error=str(e))
        return None


def _parse_product_page(asin: str, html: str, url: str) -> AmazonJPItem | None:
    """Parse Amazon JP product page HTML."""
    try:
        soup = BeautifulSoup(html, "html.parser")

        # Title
        title_el = soup.select_one("#productTitle, #title")
        title = title_el.get_text(strip=True) if title_el else ""

        # Price — try multiple selectors (Amazon layout varies)
        price_jpy = _parse_price(soup)

        # Stock
        in_stock = _parse_availability(soup, html)

        # Images (thumbnail strip)
        image_urls = _parse_images(soup)

        # Seller / sold by
        seller_el = soup.select_one("#merchant-info, #soldByThirdParty")
        seller_name = seller_el.get_text(strip=True)[:100] if seller_el else "Amazon.co.jp"

        # Rating
        rating_el = soup.select_one("span[data-hook='rating-out-of-text'], #acrPopover")
        rating_text = rating_el.get("title", "") if rating_el else ""
        rating = _parse_float(rating_text[:3])

        # Description (bullet points)
        desc_el = soup.select_one("#feature-bullets, #productDescription")
        description = desc_el.get_text(" ", strip=True)[:1000] if desc_el else ""

        if not title:
            log.debug("amazon_jp_no_title", asin=asin)
            return None

        return AmazonJPItem(
            asin=asin,
            title=title,
            price_jpy=price_jpy,
            in_stock=in_stock,
            seller_name=seller_name,
            image_urls=image_urls,
            description=description,
            rating=rating,
        )
    except Exception as e:
        log.warning("amazon_jp_parse_error", asin=asin, error=str(e))
        return None


def _parse_price(soup: BeautifulSoup) -> int:
    selectors = [
        ".a-price .a-offscreen",
        "#priceblock_ourprice",
        "#priceblock_dealprice",
        ".apexPriceToPay .a-offscreen",
        "#price",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            # Remove ¥, commas, spaces; extract first number
            digits = re.sub(r"[^\d]", "", text.split(".")[0])
            if digits:
                return int(digits)
    return 0


def _parse_availability(soup: BeautifulSoup, html: str) -> bool:
    avail_el = soup.select_one("#availability span, #outOfStock")
    if avail_el:
        text = avail_el.get_text(strip=True)
        if any(p.lower() in text.lower() for p in _OOS_PATTERNS):
            return False
    if any(p in html for p in _OOS_PATTERNS):
        return False
    add_cart = soup.select_one("#add-to-cart-button, #submit.add-to-cart")
    return add_cart is not None


def _parse_images(soup: BeautifulSoup) -> list[str]:
    urls = []
    for el in soup.select("#altImages img, #imageBlock img"):
        src = el.get("src") or el.get("data-src") or ""
        if src and "transparent-pixel" not in src:
            # Upgrade to larger variant
            large = re.sub(r"\._[A-Z0-9_,]+_\.", "._SL500_.", src)
            if large not in urls:
                urls.append(large)
    return urls[:8]


def _parse_float(text: str) -> float:
    try:
        return float(text.strip())
    except ValueError:
        return 0.0


def extract_asin_from_url(url: str) -> str | None:
    """Extract ASIN from various Amazon JP URL formats."""
    # /dp/ASIN or /product/ASIN or /gp/product/ASIN
    m = re.search(r"/(?:dp|product|gp/product)/([A-Z0-9]{10})", url)
    return m.group(1) if m else None
