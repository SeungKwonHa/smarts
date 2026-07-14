"""Naver Commerce API client.

Auth: OAuth2 client-credentials per seller account.
  POST https://api.commerce.naver.com/external/v1/oauth2/token
  → access_token (valid 1h)

EXPORT mode: when API is not yet approved, PublishAgent falls back to
generating bulk-upload spreadsheet rows compatible with Naver SmartStore
mass-registration (seller center > 상품관리 > 엑셀로 상품 등록).
This mode is set by app_config key 'publish.mode' = {"mode": "export"}.

Rate limits: per official quota; we add 0.5s between calls (http.py config).
"""

from __future__ import annotations

import csv
import io
import json
import time
from dataclasses import dataclass, field
from typing import Any

import structlog

from relay.core.config import settings
from relay.core.http import http_client

log = structlog.get_logger(__name__)

_TOKEN_URL   = "https://api.commerce.naver.com/external/v1/oauth2/token"
_PRODUCT_URL = "https://api.commerce.naver.com/external/v2/products"
_ORDER_URL   = "https://api.commerce.naver.com/external/v1/pay-order/seller/orders/query"
_DISPATCH_URL = "https://api.commerce.naver.com/external/v1/pay-order/seller/dispatch"
_INQUIRY_URL = "https://api.commerce.naver.com/external/v1/pay-order/seller/product-questions"

# Simple in-memory token cache
_token_cache: dict[str, Any] = {}


@dataclass
class NaverProduct:
    """Payload for creating a Naver SmartStore product."""
    name: str                          # listing title (max 100 chars)
    category_id: str                   # Naver leaf category ID
    sell_price: int                    # in KRW
    images: list[str]                  # first = representative image URL
    detail_html: str                   # listing detail page HTML
    # Optional fields
    origin_area: str = "일본"          # 원산지
    wholesale_country: str = "일본"
    attributes: dict[str, str] = field(default_factory=dict)
    stock_quantity: int = 99           # 구매대행: set high since per-order purchase
    min_purchase_quantity: int = 1
    max_purchase_quantity: int = 1
    shipping_fee: int = 3000           # standard domestic shipping KRW


async def get_token() -> str:
    """Return a valid Bearer token, refreshing if expired."""
    if not (settings.naver_client_id and settings.naver_client_secret):
        raise RuntimeError(
            "NAVER_CLIENT_ID and NAVER_CLIENT_SECRET not set. "
            "Register at https://developers.naver.com/apps/"
        )

    cached = _token_cache.get("access_token")
    if cached and _token_cache.get("expires_at", 0) > time.time() + 60:
        return cached

    data = await http_client.post_json(
        _TOKEN_URL,
        json={
            "grant_type": "client_credentials",
            "client_id": settings.naver_client_id,
            "client_secret": settings.naver_client_secret,
            "type": "SELF",
        },
    )
    token = data["access_token"]
    _token_cache["access_token"] = token
    _token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
    return token


def _auth_header() -> dict[str, str]:
    # Sync helper — callers must ensure token is fetched first
    token = _token_cache.get("access_token", "")
    return {"Authorization": f"Bearer {token}"}


async def create_product(product: NaverProduct) -> dict[str, Any]:
    """Create a product via Naver Commerce API.

    Returns the response dict (contains smartstoreChannelProduct.channelProductNo).
    Raises httpx.HTTPStatusError on failure (caller should log 4xx payload).
    """
    if settings.relay_dry_run:
        log.info("naver_create_product_dry_run", name=product.name[:40])
        return {"_dry_run": True, "channelProductNo": "DRY_RUN_0"}

    await get_token()
    payload = _build_product_payload(product)
    result = await http_client.post_json(
        _PRODUCT_URL,
        json=payload,
        headers=_auth_header(),
    )
    log.info(
        "naver_product_created",
        name=product.name[:40],
        product_no=result.get("smartstoreChannelProduct", {}).get("channelProductNo"),
    )
    return result


async def poll_orders(
    *,
    last_changed_from: str,   # ISO-8601 datetime
    last_changed_to: str,
    page: int = 1,
    page_size: int = 100,
) -> list[dict[str, Any]]:
    """Poll for recently changed orders (NEW, PAYED statuses).

    Used by order poller in operations/order_agent.py every 5m.
    """
    if settings.relay_dry_run:
        log.debug("naver_poll_orders_dry_run")
        return []

    await get_token()
    params = {
        "lastChangedFrom": last_changed_from,
        "lastChangedTo": last_changed_to,
        "page": str(page),
        "size": str(page_size),
        "orderStatuses": "PAYED",  # Naver: PAYED = 결제완료 = new order
    }
    data = await http_client.get_json(
        _ORDER_URL, params=params, headers=_auth_header(), cache_s=0
    )
    orders = data.get("contents", [])
    log.debug("naver_orders_polled", count=len(orders))
    return orders


async def dispatch_order(
    order_id: str,
    product_order_id: str,
    carrier_code: str,
    tracking_number: str,
) -> bool:
    """Register domestic tracking number (발송처리) for an order item.

    carrier_code: e.g. "CJ대한통운" = "04", "한진택배" = "05"
    """
    if settings.relay_dry_run:
        log.info(
            "naver_dispatch_dry_run",
            order_id=order_id,
            tracking=tracking_number,
        )
        return True

    await get_token()
    payload = {
        "dispatchProductOrders": [
            {
                "productOrderId": product_order_id,
                "deliveryMethod": "PARCEL",
                "deliveryCompanyCode": carrier_code,
                "trackingNumber": tracking_number,
            }
        ]
    }
    try:
        await http_client.post_json(
            _DISPATCH_URL,
            json=payload,
            headers=_auth_header(),
        )
        log.info("naver_dispatched", order_id=order_id, tracking=tracking_number)
        return True
    except Exception as e:
        log.error("naver_dispatch_failed", order_id=order_id, error=str(e))
        return False


async def poll_inquiries(
    *,
    last_answered_type: str = "UNANSWERED",
    page: int = 1,
    page_size: int = 50,
) -> list[dict[str, Any]]:
    """Poll product Q&A inquiries (매 10분)."""
    if settings.relay_dry_run:
        return []

    await get_token()
    params = {
        "answeredType": last_answered_type,
        "page": str(page),
        "size": str(page_size),
    }
    data = await http_client.get_json(
        _INQUIRY_URL, params=params, headers=_auth_header(), cache_s=0
    )
    return data.get("contents", [])


# ── EXPORT mode (bulk upload CSV) ─────────────────────────────────────────────

# Column headers matching Naver seller center bulk upload template.
# Operators import this CSV via 상품관리 > 엑셀로 상품 등록.
_EXPORT_COLUMNS = [
    "카테고리번호", "상품명", "판매가", "재고수량",
    "대표이미지URL", "상세설명", "원산지", "배송비",
    "최소구매수량", "최대구매수량", "구매대행여부",
]


def generate_export_csv(products: list[NaverProduct]) -> str:
    """Generate Naver bulk-upload CSV for EXPORT mode.

    Returns CSV string (UTF-8 with BOM for Excel compatibility).
    """
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_EXPORT_COLUMNS)
    writer.writeheader()
    for p in products:
        writer.writerow({
            "카테고리번호": p.category_id,
            "상품명": p.name[:100],
            "판매가": p.sell_price,
            "재고수량": p.stock_quantity,
            "대표이미지URL": p.images[0] if p.images else "",
            "상세설명": p.detail_html[:10000],
            "원산지": p.origin_area,
            "배송비": p.shipping_fee,
            "최소구매수량": p.min_purchase_quantity,
            "최대구매수량": p.max_purchase_quantity,
            "구매대행여부": "Y",
        })
    # Return with UTF-8 BOM for Excel
    return "\ufeff" + buf.getvalue()


# ── Payload builder ───────────────────────────────────────────────────────────

def _build_product_payload(p: NaverProduct) -> dict[str, Any]:
    """Build the Naver Commerce API product create payload."""
    return {
        "originProduct": {
            "statusType": "SALE",
            "saleType": "NEW",
            "leafCategoryId": p.category_id,
            "name": p.name,
            "detailContent": p.detail_html,
            "images": {
                "representativeImage": {"url": p.images[0] if p.images else ""},
                "optionalImages": [{"url": u} for u in p.images[1:8]],
            },
            "salePrice": p.sell_price,
            "stockQuantity": p.stock_quantity,
            "deliveryInfo": {
                "deliveryType": "DELIVERY",
                "deliveryAttributeType": "NORMAL",
                "deliveryFee": {
                    "deliveryFeeType": "CHARGE",
                    "baseFee": p.shipping_fee,
                },
            },
            "productLogistics": {
                "overseasPurchaseType": "OVERSEAS_PURCHASE",
                "originAreaInfo": {
                    "originNationCode": "00392",  # Japan
                    "importer": "구매대행",
                },
            },
            "minPurchaseQuantity": p.min_purchase_quantity,
            "maxPurchaseQuantityPerId": p.max_purchase_quantity,
        },
        "smartstoreChannelProduct": {
            "naverShoppingRegistration": True,
        },
    }
