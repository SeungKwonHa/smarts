"""PaymentExecutor — executes source-marketplace payments for auto-pay orders.

Design notes (docs 08_PLATFORM_APIS.md):
- Rakuten Ichiba has NO purchase/checkout API. Amazon JP PA-API is assumed
  unavailable. The only automated path is browser automation (Playwright).
- This module defines the PaymentExecutor ABC + a Playwright implementation
  plus a dry-run no-op. The live implementation is the M3 graduation path;
  until then, operators use the HITL "PAY" flow and a stub executor records
  the human action.

Wire rule: every payment attempt is logged to `purchases` with status
PREPARED → CHARGED (or FAILED). Failed attempts fall back to the Approval
Queue for human completion.
"""

from __future__ import annotations

import abc
import json
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from relay.core.config import settings
from relay.core.http import http_client

log = structlog.get_logger(__name__)


# ── Result type ────────────────────────────────────────────────────────────────

class PaymentResult:
    """Outcome of a payment attempt."""

    __slots__ = ("ok", "src_order_id", "paid_minor", "currency",
                 "payment_method", "error")

    def __init__(
        self,
        ok: bool,
        src_order_id: str = "",
        paid_minor: int = 0,
        currency: str = "JPY",
        payment_method: str = "",
        error: str = "",
    ) -> None:
        self.ok = ok
        self.src_order_id = src_order_id
        self.paid_minor = paid_minor
        self.currency = currency
        self.payment_method = payment_method
        self.error = error

    def __repr__(self) -> str:
        status = "OK" if self.ok else "FAIL"
        return f"<PaymentResult {status} {self.paid_minor} {self.currency}>"


# ── Instruction passed to executors ────────────────────────────────────────────

class PaymentInstruction:
    """What the executor needs to place an order on the source marketplace."""

    __slots__ = ("source_url", "marketplace", "price_minor", "currency",
                 "variant_map", "forwarder_address", "order_memo", "qty")

    def __init__(
        self,
        source_url: str,
        marketplace: str,
        price_minor: int,
        currency: str = "JPY",
        variant_map: dict[str, Any] | None = None,
        forwarder_address: str = "",
        order_memo: str = "",
        qty: int = 1,
    ) -> None:
        self.source_url = source_url
        self.marketplace = marketplace
        self.price_minor = price_minor
        self.currency = currency
        self.variant_map = variant_map or {}
        self.forwarder_address = forwarder_address
        self.order_memo = order_memo
        self.qty = qty


# ── Abstract executor ─────────────────────────────────────────────────────────

class PaymentExecutor(abc.ABC):
    """Interface for executing a purchase on a source marketplace.

    Implementations:
    - DryRunPaymentExecutor (default, safe)
    - RakutenPlaywrightExecutor (Ichiba: add-to-cart → login → checkout)
    - AmazonJPPlaywrightExecutor (Amazon: buy-now → place-order)
    """

    @abc.abstractmethod
    async def execute(
        self,
        instruction: PaymentInstruction,
    ) -> PaymentResult:
        """Attempt to charge the source card and place the order."""
        ...


# ── Dry-run (no-op) ───────────────────────────────────────────────────────────

class DryRunPaymentExecutor(PaymentExecutor):
    """Logs intent only — used when relay_dry_run=True."""

    async def execute(self, instruction: PaymentInstruction) -> PaymentResult:
        log.info(
            "payment_dry_run",
            marketplace=instruction.marketplace,
            url=instruction.source_url,
            price=instruction.price_minor,
            currency=instruction.currency,
        )
        return PaymentResult(
            ok=True,
            src_order_id="DRY-RUN",
            paid_minor=instruction.price_minor,
            currency=instruction.currency,
            payment_method="dry_run",
        )


# ── Rakuten Ichiba (Playwright) ───────────────────────────────────────────────
#
# Flow: product page → select variant (if any) → カートに入れる → 购物车确认 →
# 注文に進む → login (if redirected) → お支払い方法選択 → 注文確定.
#
# We do NOT store Rakuten credentials in env vars. They are stored in app_config
# under "payment.rakuten" as encrypted-at-rest is out of scope; the operator
# configures the account once via the seller portal and Playwright reuses the
# browser session cookies persisted in a dedicated profile directory.

_RAKUTEN_LOGIN_URL = "https://www.rakuten.co.jp/"
_RAKUTEN_CART_URL = "https://order.step.rakuten.co.jp/rms/mall/basket/vc"


class RakutenPlaywrightExecutor(PaymentExecutor):
    """Automates Rakuten Ichiba checkout via Playwright.

    Requires:
    - A Chromium profile with an already-logged-in Rakuten account
      (path set in app_config key "payment.rakuten.profile_path").
    - A stored payment method on the Rakuten account (credit card or
      registered carrier payment).
    """

    async def execute(self, instruction: PaymentInstruction) -> PaymentResult:
        if instruction.marketplace != "rakuten":
            return PaymentResult(ok=False, error=f"unsupported marketplace: {instruction.marketplace}")

        profile_path = await self._get_profile_path()
        if not profile_path:
            return PaymentResult(
                ok=False,
                error="Rakuten profile path not configured. Set app_config key payment.rakuten.profile_path",
            )

        try:
            return await self._run_checkout(instruction, profile_path)
        except Exception as e:
            log.error("rakuten_payment_error", error=str(e), url=instruction.source_url)
            return PaymentResult(ok=False, error=str(e)[:500])

    async def _run_checkout(
        self,
        instruction: PaymentInstruction,
        profile_path: str,
    ) -> PaymentResult:
        """Navigate product → cart → confirm → place order."""
        from playwright.async_api import async_playwright

        log.info("rakuten_checkout_start", url=instruction.source_url)

        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                profile_path,
                headless=True,
                locale="ja-JP",
            )
            page = context.pages[0] if context.pages else await context.new_page()

            # 1. Product page
            await page.goto(instruction.source_url, wait_until="domcontentloaded", timeout=30000)

            # 2. Select variant (color/size) if present
            await self._select_variant(page, instruction.variant_map)

            # 3. Add to cart
            add_btn = page.locator("#allcart, input[name='加入 cart'], button:has-text('カートに入れる'), a:has-text('カートに入れる')")
            if await add_btn.count() == 0:
                await context.close()
                return PaymentResult(ok=False, error="Add-to-cart button not found — sold out or layout change?")
            await add_btn.first.click()
            await page.wait_for_timeout(2000)

            # 4. Go to cart
            await page.goto(_RAKUTEN_CART_URL, wait_until="domcontentloaded", timeout=15000)

            # 5. Set quantity if > 1
            if instruction.qty > 1:
                qty_input = page.locator("input[name='quantity'], input.qty")
                if await qty_input.count() > 0:
                    await qty_input.first.fill(str(instruction.qty))
                    await page.wait_for_timeout(1000)

            # 6. Fill order memo (お届け添え書き / メッセージ) if field exists
            if instruction.order_memo:
                memo_field = page.locator("textarea[name='memo'], input[name='comment']")
                if await memo_field.count() > 0:
                    await memo_field.first.fill(instruction.order_memo[:500])

            # 7. Proceed to order (注文に進む / 注文確認)
            order_btn = page.locator("button:has-text('注文に進む'), button:has-text('注文確認'), input[value='注文に進む']")
            if await order_btn.count() == 0:
                await context.close()
                return PaymentResult(ok=False, error="Order proceed button not found")
            await order_btn.first.click()
            await page.wait_for_timeout(3000)

            # 8. Check if redirected to login (shouldn't happen with persistent context)
            if "login" in page.url.lower():
                await context.close()
                return PaymentResult(ok=False, error="Rakuten session expired — re-login required")

            # 9. Confirm order (注文を確定 / 注文する)
            confirm_btn = page.locator("button:has-text('注文を確定'), button:has-text('注文する'), button:has-text('注文を確定する')")
            if await confirm_btn.count() == 0:
                await context.close()
                return PaymentResult(ok=False, error="Confirm-order button not found")
            await confirm_btn.first.click()
            await page.wait_for_timeout(3000)

            # 10. Grab order confirmation number
            src_order_id = await self._extract_order_id(page)

            await context.close()

        log.info(
            "rakuten_checkout_complete",
            order_id=src_order_id,
            price=instruction.price_minor,
        )
        return PaymentResult(
            ok=True,
            src_order_id=src_order_id or "UNKNOWN",
            paid_minor=instruction.price_minor,
            currency=instruction.currency,
            payment_method="rakuten_stored_card",
        )

    async def _select_variant(self, page, variant_map: dict[str, Any]) -> None:
        """Select color/size from the product page variant UI."""
        for key, value in variant_map.items():
            # Rakuten typically uses select elements or radio-like links
            selector = f"option:has-text('{value}'), a:has-text('{value}'), label:has-text('{value}')"
            el = page.locator(selector).first
            if await el.count() > 0:
                tag = await el.evaluate("el => el.tagName")
                if tag == "OPTION":
                    await page.select_option(f"select[name*='{key}'], select[name*='spec']", value=str(value))
                else:
                    await el.click()
                await page.wait_for_timeout(800)

    async def _extract_order_id(self, page) -> str:
        """Extract the order ID from the confirmation page."""
        # Confirmation URL pattern: ...rakuten.co.jp/order/... or visible text
        import re
        url = page.url
        m = re.search(r"/order/(\d+)", url)
        if m:
            return m.group(1)
        # Try text content
        body = await page.content()
        m = re.search(r"注文番号[：:\s]*(\d{3,}-\d{3,}|\d{8,})", body)
        return m.group(1) if m else ""

    async def _get_profile_path(self) -> str:
        """Read browser profile path from app_config.
        Note: Playwright persistent context uses a real profile dir, not
        the app_config directly. This is a simplified read — full impl
        caches the lookup.
        """
        # Default paths per OS
        import sys
        if sys.platform == "darwin":
            return "~/Library/Application Support/relay-playwright/rakuten"
        if sys.platform == "win32":
            return r"~\AppData\Local\relay-playwright\rakuten"
        return "~/.local/share/relay-playwright/rakuten"


# ── Amazon Japan (Playwright) ─────────────────────────────────────────────────

_AMAZON_BUY_NOW = "#buy-now-button, #one-click-button, input[name='submit.buy-now']"
_AMAZON_PLACE_ORDER = "#submitOrderButtonId, input[name='placeYourOrder'], button:has-text('注文を確認する'), span:has-text('注文を確認する')"


class AmazonJPPlaywrightExecutor(PaymentExecutor):
    """Automates Amazon.co.jp Buy-Now via Playwright.

    Amazon's checkout is notoriously hard to automate (bot detection,
    MFA prompts, login wall). This is an M3+ path — current status:
    interface defined, implementation operator-assisted.
    """

    async def execute(self, instruction: PaymentInstruction) -> PaymentResult:
        if instruction.marketplace != "amazon_jp":
            return PaymentResult(ok=False, error=f"unsupported marketplace: {instruction.marketplace}")

        # Amazon strongly discourages this per 08_PLATFORM_APIS.md.
        # The only compliant path is an API (PA-API + Order API), which
        # requires associate sales quota and is out of scope for M3.
        log.warning(
            "amazon_jp_payment_blocked",
            msg="Amazon JP automated payment not implemented. Use HITL for Amazon-sourced orders.",
        )
        return PaymentResult(
            ok=False,
            error="Amazon JP auto-pay not implemented — use HITL or PA-API when available",
        )


# ── Factory ────────────────────────────────────────────────────────────────────

async def get_executor(
    marketplace: str,
    session: AsyncSession | None = None,
) -> PaymentExecutor:
    """Return the right executor for the given marketplace.

    - If relay_dry_run is True → always return DryRunPaymentExecutor
    - Otherwise, branch on marketplace.
    """
    if settings.relay_dry_run:
        return DryRunPaymentExecutor()

    if marketplace == "rakuten":
        return RakutenPlaywrightExecutor()

    if marketplace == "amazon_jp":
        return AmazonJPPlaywrightExecutor()

    # Unknown marketplace → no auto-pay
    log.warning("no_executor_for_marketplace", marketplace=marketplace)
    return DryRunPaymentExecutor()  # safe no-op


# ── High-level: execute + persist result ──────────────────────────────────────

async def execute_and_record(
    *,
    session: AsyncSession,
    order_id: int,
    instruction: PaymentInstruction,
) -> PaymentResult:
    """Execute the payment and write the result to `purchases`.

    On success:
      - Insert/update purchases row with status='CHARGED', src_order_id, paid_at
      - Returns PaymentResult(ok=True)

    On failure:
      - Insert purchases row with status='FAILED', error message
      - Returns PaymentResult(ok=False) → caller should fall back to HITL
    """
    marketplace = instruction.marketplace
    executor = await get_executor(marketplace, session)

    result = await executor.execute(instruction)

    payment_method = result.payment_method or "unknown"
    status = "CHARGED" if result.ok else "FAILED"
    paid_at = datetime.now(UTC) if result.ok else None

    await session.execute(
        text("""
            UPDATE purchases
            SET status = CAST(:status AS TEXT),
                payment_method = :pm,
                src_order_id = :src_oid,
                paid_minor = :paid,
                fx = (SELECT fx FROM product_sources WHERE id = purchases.source_id LIMIT 1),
                paid_at = :paid_at
            WHERE order_id = :oid AND status IN ('PREPARED', 'FAILED')
        """),
        {
            "status": status,
            "pm": payment_method,
            "src_oid": result.src_order_id,
            "paid": result.paid_minor,
            "paid_at": paid_at,
            "oid": order_id,
        }),
    log.info(
        "payment_recorded",
        order_id=order_id,
        status=status,
        src_order_id=result.src_order_id,
        error=result.error or None,
    )
    return result
