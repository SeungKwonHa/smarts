"""Naver Commerce API client.

Auth: OAuth2 client-credentials per seller account.
  POST https://api.commerce.naver.com/external/v1/oauth2/token
  → access_token (valid ~3h)

  SIGNING:
    ts = int((time.time() - 3) * 1000)
    sign = bcrypt.base64encode(
        bcrypt.hashpw(f"{client_id}_{ts}".encode(), client_secret.encode())
    ).decode()
    POST query params (NOT body): client_id, timestamp, client_secret_sign,
    grant_type=client_credentials, type=SELF
    NOTE: client_secret raw value is NOT sent in the request.

EXPORT mode: when API is not yet approved, PublishAgent falls back to
generating bulk-upload spreadsheet rows compatible with Naver SmartStore
mass-registration (seller center > 상품관리 > 엑셀로 상품 등록).
This mode is set by app_config key 'publish.mode' = {"mode": "export"}.

Rate limits: per official quota; we add 0.5s between calls (http.py config).
"""

from __future__ import annotations

import base64
import csv
import io
import time
from dataclasses import dataclass, field
from typing import Any

import bcrypt
import httpx
import structlog

from relay.core.config import settings
from relay.core.http import http_client

log = structlog.get_logger(__name__)

_TOKEN_URL       = "https://api.commerce.naver.com/external/v1/oauth2/token"
_PRODUCT_URL     = "https://api.commerce.naver.com/external/v2/products"
_IMAGE_UPLOAD_URL = "https://api.commerce.naver.com/external/v1/product-images/upload"
_ORDER_URL   = "https://api.commerce.naver.com/external/v1/pay-order/seller/orders/query"
_DISPATCH_URL = "https://api.commerce.naver.com/external/v1/pay-order/seller/dispatch"
_INQUIRY_URL = "https://api.commerce.naver.com/external/v1/pay-order/seller/product-questions"

# Simple in-memory token cache
_token_cache: dict[str, Any] = {}

# Delivery company codes — loaded from Naver API on boot via fetch_delivery_companies()
# Verified 2026-07-15 against live API:
#   Naver Commerce API uses STRING carrier codes (NOT numeric):
#   "CJGLS" = CJ대한통운, "HANJIN" = 한진택배, "LOGEN" = 로젠택배,
#   "POST" = 우체국택배, "LOTTE" = 롯데택배
# IMPORTANT: Naver carrier codes are platform-specific — do NOT mix with Kakao/Gmarket.
_delivery_company_cache: dict[str, str] = {}   # {name: code}
_default_carrier_code: str = "CJGLS"           # CJ대한통운 (verified)


async def fetch_delivery_companies(token: str) -> dict[str, str]:
    """Fetch delivery company codes from Naver Commerce API.

    Returns {companyName: companyCode} mapping.
    Result is cached in _delivery_company_cache.

    Note: The exact Naver Commerce API endpoint for carrier lookup is not yet
    published in public docs. When available, implement the GET call here.
    For now, returns a hardcoded fallback verified against the live API.

    TODO: Replace with actual API call once endpoint is confirmed.
    """
    global _delivery_company_cache

    # Currently using verified fallback values
    # Naver Commerce API carrier codes (STRING codes, verified 2026-07-15):
    #   Public API does not expose a carrier-lookup endpoint — these are
    #   verified against the live product creation API.
    _delivery_company_cache = {
        "CJ대한통운": "CJGLS",
        "한진택배": "HANJIN",
        "로젠택배": "LOGEN",
        "우체국택배": "POST",
        "롯데택배": "LOTTE",
    }
    return _delivery_company_cache


def get_delivery_company_code(name: str) -> str:
    """Get the Naver carrier code for a given carrier name.

    Falls back to "04" (CJ대한통운) if name not found.
    """
    if _delivery_company_cache:
        return _delivery_company_cache.get(name, _default_carrier_code)
    return _default_carrier_code


def set_default_carrier(code: str) -> None:
    """Set default carrier code (e.g. from app_config at boot)."""
    global _default_carrier_code
    _default_carrier_code = code


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
    """Return a valid Bearer token, refreshing if expired.

    Naver Commerce API uses bcrypt-based signing (NOT HMAC):
      ts = int((time.time() - 3) * 1000)
      sign = base64(bcrypt.hashpw(f"{client_id}_{ts}", client_secret))
      POST as query params: client_id, timestamp, client_secret_sign,
                           grant_type=client_credentials, type=SELF
    client_secret raw value is NOT included in the request.
    """
    if not (settings.naver_client_id and settings.naver_client_secret):
        raise RuntimeError(
            "NAVER_CLIENT_ID and NAVER_CLIENT_SECRET not set. "
            "Register at https://developers.naver.com/apps/"
        )

    cached = _token_cache.get("access_token")
    if cached and _token_cache.get("expires_at", 0) > time.time() + 60:
        return cached

    ts = int((time.time() - 3) * 1000)
    sign = base64.standard_b64encode(
        bcrypt.hashpw(
            f"{settings.naver_client_id}_{ts}".encode(),
            settings.naver_client_secret.encode(),
        )
    ).decode()

    params = {
        "client_id": settings.naver_client_id,
        "timestamp": ts,
        "client_secret_sign": sign,
        "grant_type": "client_credentials",
        "type": "SELF",
    }

    # Must use query params, not body
    data = await http_client.post(
        _TOKEN_URL,
        params=params,
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    token = data["access_token"]
    _token_cache["access_token"] = token
    _token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
    log.info("naver_token_refreshed", expires_in=data.get("expires_in"))
    return token


def _auth_header() -> dict[str, str]:
    # Sync helper — callers must ensure token is fetched first
    token = _token_cache.get("access_token", "")
    return {"Authorization": f"Bearer {token}"}


async def upload_images(token: str, image_bytes_list: list[tuple[str, bytes]]) -> list[str]:
    """Upload images to Naver CDN and return their URLs.

    Args:
        token: Naver Commerce API Bearer token
        image_bytes_list: list of (filename, bytes) tuples

    Returns:
        List of Naver CDN URLs (shop-phinf.pstatic.net)

    Note:
        - Field name must be "imageFiles" (all files share same field name)
        - Do NOT set Content-Type header manually (httpx sets it with boundary)
        - Max 10 images per call
    """
    files = [
        ("imageFiles", (filename, data, "image/jpeg"))
        for filename, data in image_bytes_list[:10]
    ]

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
        r = await c.post(
            _IMAGE_UPLOAD_URL,
            headers={"Authorization": f"Bearer {token}"},
            files=files,
        )
    r.raise_for_status()
    urls = [img["url"] for img in r.json()["images"]]
    log.info("naver_images_uploaded", count=len(urls))
    return urls


async def download_and_upload_images(
    token: str, image_urls: list[str],
) -> list[str]:
    """Download images from source (Rakuten etc.) and upload to Naver CDN.

    Pipeline:
    1. Download each image URL → bytes
    2. Upload all bytes to Naver via product-images/upload
    3. Return Naver CDN URLs
    """
    downloaded: list[tuple[str, bytes]] = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
        for i, url in enumerate(image_urls[:10]):
            try:
                resp = await c.get(url)
                resp.raise_for_status()
                downloaded.append((f"image_{i}.jpg", resp.content))
            except Exception as e:
                log.warning("naver_image_download_failed", url=url[:80], error=str(e))

    if not downloaded:
        raise RuntimeError("No images could be downloaded for Naver upload")

    return await upload_images(token, downloaded)


async def create_product(product: NaverProduct) -> dict[str, Any]:
    """Create a product via Naver Commerce API.

    Returns the response dict (contains smartstoreChannelProduct.channelProductNo).
    Raises httpx.HTTPStatusError on failure (caller should log 4xx payload).

    Note:
        If product.images contains non-Naver CDN URLs (e.g. from Rakuten), they are
        automatically downloaded and uploaded to Naver CDN before product creation.
    """
    if settings.relay_dry_run:
        log.info("naver_create_product_dry_run", name=product.name[:40])
        return {"_dry_run": True, "channelProductNo": "DRY_RUN_0"}

    token = await get_token()

    # Auto-upload images if they're not already Naver CDN urls
    naver_cdn_prefix = "shop-phinf.pstatic.net"
    needs_upload = [
        url for url in product.images if url and naver_cdn_prefix not in url
    ]
    if needs_upload:
        log.info("naver_uploading_images_count", count=len(needs_upload))
        try:
            uploaded = await download_and_upload_images(token, needs_upload)
            # Replace external URLs with CDN URLs, keep existing CDN URLs
            cdn_urls = [url for url in product.images if naver_cdn_prefix in url]
            product.images = cdn_urls + uploaded
        except Exception as e:
            log.warning("naver_image_upload_failed", error=str(e)[:100])
            # Continue without images — Naver requires at least 1 image
            # Use placeholder or skip if upload fails
            if not any(naver_cdn_prefix in u for u in product.images):
                raise RuntimeError(
                    f"Image upload failed and no Naver CDN images available: {e}"
                ) from e

    payload = _build_product_payload(product)
    result = await http_client.post(
        _PRODUCT_URL,
        json=payload,
        headers={**_auth_header(), "Content-Type": "application/json"},
    )
    log.info(
        "naver_product_created",
        name=product.name[:40],
        product_no=result.get("smartstoreChannelProduct", {}).get("channelProductNo"),
    )
    return result


async def update_product(
    channel_product_no: str,
    updates: dict[str, Any],
) -> dict[str, Any]:
    """Update an existing Naver product.

    Args:
        channel_product_no: smartstore channel product number
        updates: partial fields to update, e.g. {"sell_price": 12000} or
                 {"stock_quantity": 0} for sold-out sync

    Returns the full updated product dict from the API.

    Valid update paths (verified 2026-07-15):
      - Price change: updates the salePrice field
      - Stock change: updates the stockQuantity field
    """
    if settings.relay_dry_run:
        log.info("naver_update_product_dry_run", channel_product_no=channel_product_no)
        return {"_dry_run": True}

    await get_token()

    # 1. Fetch current product state
    current = await http_client.get(
        f"{_PRODUCT_URL}/channel-products/{channel_product_no}",
        headers=_auth_header(),
        cache_s=0,
    )
    product = current if isinstance(current, dict) else current.json()
    origin = product.get("originProduct", {})

    # 2. Apply updates
    price = updates.get("sell_price")
    if price is not None:
        origin["salePrice"] = price

    stock = updates.get("stock_quantity")
    if stock is not None:
        origin["stockQuantity"] = stock

    name = updates.get("product_name")
    if name is not None:
        origin["name"] = name

    # Instant discount (상시할인) — customerBenefit.immediateDiscountPolicy
    # Naver does NOT support consumerPrice via API; use customerBenefit instead.
    discount_pct = updates.get("discount_percent")
    if discount_pct is not None:
        origin["customerBenefit"] = {
            "immediateDiscountPolicy": {
                "discountMethod": {
                    "value": discount_pct,
                    "unitType": "PERCENT",
                }
            }
        }

    # Detail HTML (상세페이지 내용)
    detail = updates.get("detail_html")
    if detail is not None:
        origin["detailContent"] = detail

    # Fix itemName/modelName in productInfoProvidedNotice (제품명 동기화)
    new_name = updates.get("product_name") or (updates.get("detail_html") and name)
    if new_name:
        etc = origin.setdefault("detailAttribute", {}).setdefault(
            "productInfoProvidedNotice", {}
        ).setdefault("etc", {})
        if etc:
            etc["itemName"] = new_name
            etc["modelName"] = new_name

    # 3. PUT back the full payload
    payload = {
        "originProduct": origin,
        "smartstoreChannelProduct": product.get("smartstoreChannelProduct", {}),
    }

    try:
        result = await http_client.put(
            f"{_PRODUCT_URL}/channel-products/{channel_product_no}",
            json=payload,
            headers={**_auth_header(), "Content-Type": "application/json"},
        )
    except Exception as e:
        if "409" in str(e) or "conflict" in str(e).lower():
            log.info("naver_update_product_idempotent", channel_product_no=channel_product_no)
            return {"_dry_run": False, "idempotent": True}
        raise

    log.info(
        "naver_product_updated",
        channel_product_no=channel_product_no,
        updates=list(updates.keys()),
    )
    return result


async def suspend_product(channel_product_no: str) -> bool:
    """Suspend a Naver product by setting statusType to SOLD_OUT.

    This makes the product invisible in search and stops new purchases.
    Used by StockMonitor when source goes OOS.
    """
    if settings.relay_dry_run:
        log.info("naver_suspend_product_dry_run", channel_product_no=channel_product_no)
        return True

    await get_token()

    # Fetch current state to get full origin payload
    current = await http_client.get(
        f"{_PRODUCT_URL}/channel-products/{channel_product_no}",
        headers=_auth_header(),
        cache_s=0,
    )
    product = current if isinstance(current, dict) else current.json()
    origin = product.get("originProduct", {})
    origin["statusType"] = "SOLD_OUT"

    payload = {
        "originProduct": origin,
        "smartstoreChannelProduct": product.get("smartstoreChannelProduct", {}),
    }

    try:
        await http_client.put(
            f"{_PRODUCT_URL}/channel-products/{channel_product_no}",
            json=payload,
            headers={**_auth_header(), "Content-Type": "application/json"},
        )
        log.info("naver_product_suspended", channel_product_no=channel_product_no)
        return True
    except Exception as e:
        if "409" in str(e) or "conflict" in str(e).lower():
            log.info("naver_suspend_idempotent", channel_product_no=channel_product_no)
            return True
        log.error("naver_suspend_failed", channel_product_no=channel_product_no, error=str(e))
        return False


async def reactivate_product(channel_product_no: str) -> bool:
    """Reactivate a Naver product by setting statusType back to SALE.

    Used by StockMonitor when source is back in stock.
    """
    if settings.relay_dry_run:
        log.info("naver_reactivate_product_dry_run", channel_product_no=channel_product_no)
        return True

    await get_token()

    current = await http_client.get(
        f"{_PRODUCT_URL}/channel-products/{channel_product_no}",
        headers=_auth_header(),
        cache_s=0,
    )
    product = current if isinstance(current, dict) else current.json()
    origin = product.get("originProduct", {})
    origin["statusType"] = "SALE"

    payload = {
        "originProduct": origin,
        "smartstoreChannelProduct": product.get("smartstoreChannelProduct", {}),
    }

    try:
        await http_client.put(
            f"{_PRODUCT_URL}/channel-products/{channel_product_no}",
            json=payload,
            headers={**_auth_header(), "Content-Type": "application/json"},
        )
        log.info("naver_product_reactivated", channel_product_no=channel_product_no)
        return True
    except Exception as e:
        if "409" in str(e) or "conflict" in str(e).lower():
            log.info("naver_reactivate_idempotent", channel_product_no=channel_product_no)
            return True
        log.error("naver_reactivate_failed", channel_product_no=channel_product_no, error=str(e))
        return False


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

    carrier_code: e.g. "CJGLS" (CJ대한통운), "HANJIN" (한진택배) — STRING codes

    Idempotent: returns True on 409 Conflict (already dispatched).
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
        error_str = str(e).lower()
        if "409" in str(e) or "conflict" in error_str or "already" in error_str:
            log.info(
                "naver_dispatch_already_done",
                order_id=order_id,
                tracking=tracking_number,
            )
            return True  # Already dispatched — treat as success
        log.error("naver_dispatch_failed", order_id=order_id, error=str(e))
        return False


async def answer_inquiry(
    inquiry_id: str,
    answer_text: str,
) -> bool:
    """Answer a product Q&A inquiry on Naver.

    Stub for M3 auto-send. In M2 this is draft-only; the operator sends manually.
    When wired up: POST to the product-questions answer endpoint.

    Args:
        inquiry_id: Naver's product question ID
        answer_text: The answer text to post

    Returns True on success.
    """
    if settings.relay_dry_run:
        log.info("naver_answer_inquiry_dry_run", inquiry_id=inquiry_id)
        return True

    await get_token()
    # TODO (M3): Wire up actual Naver product-questions answer endpoint
    # Endpoint: POST /external/v1/pay-order/seller/product-questions/{inquiryId}/answer
    log.info("naver_answer_inquiry_stub", inquiry_id=inquiry_id)
    return True


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
    """Build the Naver Commerce API product create payload.

    Reference: Naver Commerce API v2 — originProduct schema.
    Verified 2026-07-15 against live API (product 13605041327 created).

    Key enums (verified):
      - deliveryFeeType: "FREE" ✅ | "CHARGE" ❌ | "CHARGED" ❌ (only FREE confirmed working)
      - deliveryCompany: STRING carrier code — "CJGLS", "HANJIN", "LOGEN", "POST", "LOTTE"
                       (NOT numeric — "04", "05" etc. do NOT work)
      - channelProductDisplayStatusType: "ON" (판매중) | "OFF" (판매중지)

    Required fields that trip people up:
      - detailAttribute.productInfoProvidedNotice (상품정보제공고시) — REQUIRED
      - detailAttribute.certificationTargetExcludeContent — exclude green cert
      - detailAttribute.originAreaInfo — use
        {"originAreaCode": "00", "content": "<원산지>", "plural": false}
        NOTE: API normalizes to "국산" regardless.
      - detailAttribute.unitCapacity.unitPriceYn — boolean false (required for some categories)

    Note on overseas purchase (해외구매대행):
      - productLogistics[] with overseasPurchaseType requires a store-specific logisticsCompanyId.
        The public API does not expose a carrier-lookup endpoint. For now, create products
        without productLogistics. The overseas purchase flag may need Naver Seller Support
        to enable the logistics integration, or we use 상품 수정 API after creation.

    Delivery company codes must match a carrier registered in the store's
    shipping integration settings (배송연동).
    """
    delivery_fee: dict[str, Any] = {"deliveryFeeType": "FREE"}
    if p.shipping_fee > 0:
        delivery_fee["baseFee"] = p.shipping_fee

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
                "deliveryCompany": get_delivery_company_code("CJ대한통운"),
                "deliveryFee": delivery_fee,
                "claimDeliveryInfo": {
                    "returnDeliveryFee": 2500,
                    "exchangeDeliveryFee": 5000,
                },
            },
            "minPurchaseQuantity": p.min_purchase_quantity,
            "maxPurchaseQuantityPerId": p.max_purchase_quantity,
            "detailAttribute": {
                "minorPurchasable": True,
                "originAreaInfo": {
                    "originAreaCode": "00",
                    "content": p.origin_area,
                    "plural": False,
                },
                "afterServiceInfo": {
                    "afterServiceTelephoneNumber": "01000000000",
                    "afterServiceGuideContent": "판매자에게 문의 바랍니다.",
                },
                "optionInfo": {
                    "simpleOptionSortType": "CREATE",
                    "optionSimple": [],
                    "optionCustom": [],
                    "optionCombinationSortType": "CREATE",
                    "standardOptionGroups": [],
                    "optionStandards": [],
                    "useStockManagement": True,
                    "optionDeliveryAttributes": [],
                },
                "purchaseReviewInfo": {"purchaseReviewExposure": True},
                "taxType": "TAX",
                "certificationTargetExcludeContent": {
                    "greenCertifiedProductExclusionYn": True,
                },
                "sellerCommentUsable": False,
                "productInfoProvidedNotice": {
                    "productInfoProvidedNoticeType": "ETC",
                    "etc": {
                        "returnCostReason": "제품 불량 시 무료 반품",
                        "noRefundReason": "수령 후 7일 초과 시 반품 불가",
                        "qualityAssuranceStandard": "관련 법 및 소비자 분쟁해결 기준에 따름",
                        "compensationProcedure": "관련 법 및 소비자 분쟁해결 기준에 따름",
                        "troubleShootingContents": "판매자에게 문의 바랍니다.",
                        "itemName": p.name[:100],
                        "modelName": p.name[:100],
                        "manufacturer": p.origin_area,
                        "customerServicePhoneNumber": "01000000000",
                    },
                },
                "itselfProductionProductYn": False,
                "unitCapacity": {"unitPriceYn": False},
            },
        },
        "smartstoreChannelProduct": {
            "naverShoppingRegistration": True,
            "channelProductDisplayStatusType": "ON",
        },
    }
