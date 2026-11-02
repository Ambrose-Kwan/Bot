from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from collections.abc import Sequence
from http.cookiejar import CookieJar
from typing import Any
from urllib.parse import urlparse

try:
    from curl_cffi.requests import AsyncSession
    from curl_cffi.requests.cookies import RequestsCookieJar
except ImportError:
    AsyncSession = Any  # type: ignore
    RequestsCookieJar = Any  # type: ignore

try:
    from playwright.async_api import (
        Browser,
        BrowserContext,
        Page,
        async_playwright,
    )
    from playwright.async_api import (
        Error as PlaywrightError,
    )
except ImportError:
    Browser = Any  # type: ignore
    BrowserContext = Any  # type: ignore
    PlaywrightError = RuntimeError  # type: ignore[misc, assignment]
    Page = Any  # type: ignore
    async_playwright = None  # type: ignore

logger = logging.getLogger("nexus.utils.handoff")


def parse_playwright_proxy(raw_proxy: str | None) -> dict[str, str] | None:
    """
    Normalizes arbitrary proxy formats (HTTP/SOCKS5, user:pass@host:port, host:port:user:pass)
    into Playwright's required dict structure: {"server": "...", "username": "...", "password": "..."}.
    """
    if not raw_proxy:
        return None

    raw = raw_proxy.strip()
    protocol = "http"
    if "://" in raw:
        proto, rest = raw.split("://", 1)
        protocol = "socks5" if "socks5" in proto.lower() else "http"
        raw = rest

    # Format 1: user:pass@host:port
    if "@" in raw:
        auth, host_port = raw.split("@", 1)
        username, password = auth.split(":", 1)
        return {
            "server": f"{protocol}://{host_port}",
            "username": username,
            "password": password,
        }

    tokens = raw.split(":")
    # Format 2: host:port:user:pass
    if len(tokens) == 4:
        host, port, user, pwd = tokens
        return {
            "server": f"{protocol}://{host}:{port}",
            "username": user,
            "password": pwd,
        }

    # Format 3: host:port (no auth)
    if len(tokens) == 2:
        host, port = tokens
        return {"server": f"{protocol}://{host}:{port}"}

    return {"server": f"{protocol}://{raw}"}


def extract_playwright_cookies(
    cookie_source: CookieJar | dict[str, str] | Any,
    target_url: str,
) -> list[dict[str, Any]]:
    """
    Converts curl_cffi / RequestsCookieJar into standard Playwright cookie format.
    Ensures domain, path, secure, and httpOnly attributes are valid.
    """
    parsed_target = urlparse(target_url)
    fallback_domain = parsed_target.hostname or "localhost"

    playwright_cookies: list[dict[str, Any]] = []

    # Case 1: Standard CookieJar iterable
    if hasattr(cookie_source, "__iter__"):
        try:
            for c in cookie_source:
                domain = getattr(c, "domain", "")
                if not domain:
                    domain = fallback_domain

                cookie_dict: dict[str, Any] = {
                    "name": getattr(c, "name", ""),
                    "value": getattr(c, "value", ""),
                    "domain": domain,
                    "path": getattr(c, "path", "/"),
                    "secure": bool(getattr(c, "secure", False)),
                    "httpOnly": bool(getattr(c, "_rest", {}).get("HttpOnly", False)),
                }

                expires = getattr(c, "expires", None)
                if expires and isinstance(expires, (int, float)) and expires > 0:
                    cookie_dict["expires"] = int(expires)

                playwright_cookies.append(cookie_dict)
            return playwright_cookies
        except TypeError:
            pass

    # Case 2: Dictionary format {name: value}
    if isinstance(cookie_source, dict):
        for name, value in cookie_source.items():
            playwright_cookies.append({
                "name": str(name),
                "value": str(value),
                "domain": fallback_domain,
                "path": "/",
                "secure": parsed_target.scheme == "https",
                "httpOnly": False,
            })

    return playwright_cookies


async def _handoff_to_browser(
    curl_session: AsyncSession,
    target_url: str,
    proxy: str | None = None,
    user_agent: str | None = None,
    headless: bool = False,
    timeout_sec: float = 180.0,
    success_url_substrings: Sequence[str] | None = None,
) -> dict[str, Any]:
    """
    Transitions state from curl_cffi AsyncSession to an interactive Playwright browser for 3DS challenge.
    Listens for bank OTP submission, response callbacks, and page redirection.
    """
    start_time = time.monotonic()
    deadline = start_time + timeout_sec
    success_markers = list(success_url_substrings or [
        "order-confirmation",
        "thank-you",
        "orders/",
        "receipt",
        "checkout/success",
        "confirmed",
    ])

    ua = user_agent or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    session_cookies = getattr(curl_session, "cookies", {})
    pw_cookies = extract_playwright_cookies(session_cookies, target_url)
    logger.info(
        f"[Handoff] Extracted {len(pw_cookies)} cookies from curl_cffi session for {target_url}"
    )

    active_proxy_str = proxy
    if not active_proxy_str and hasattr(curl_session, "proxies") and curl_session.proxies:
        active_proxy_str = (
            curl_session.proxies.get("https")
            or curl_session.proxies.get("http")
            or curl_session.proxies.get("all")
        )
    pw_proxy = parse_playwright_proxy(active_proxy_str)

    if pw_proxy:
        logger.info(f"[Handoff] Route through proxy: {pw_proxy['server']}")
    else:
        logger.info("[Handoff] Running without proxy (Direct Connection)")

    chromium_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-features=IsolateOrigins,site-per-process",
        "--window-size=1280,850",
    ]

    logger.info(
        f"[Handoff] Launching Playwright browser (Headless={headless}) for 3D Secure / Checkout..."
    )

    if async_playwright is None:
        raise RuntimeError("Playwright is not installed. Please run: pip install playwright && playwright install chromium")

    captured_order_id: str | None = None

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=headless,
            proxy=pw_proxy,
            args=chromium_args,
        )

        try:
            context: BrowserContext = await browser.new_context(
                user_agent=ua,
                viewport={"width": 1280, "height": 850},
                locale="en-US",
                timezone_id="America/Los_Angeles",
                color_scheme="light",
                device_scale_factor=1,
            )

            await context.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                """
            )

            if pw_cookies:
                await context.add_cookies(pw_cookies)
                logger.info(f"[Handoff] Successfully injected {len(pw_cookies)} cookies into Playwright context.")

            page: Page = await context.new_page()

            # Network response sniffing for background order confirmation payloads
            async def handle_response(response: Any):
                nonlocal captured_order_id
                url_lower = response.url.lower()
                if any(k in url_lower for k in ("order", "checkout", "receipt")) and response.status in (200, 201):
                    try:
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            body = await response.json()
                            if isinstance(body, dict):
                                order_num = body.get("orderNumber") or body.get("orderId") or body.get("id")
                                if order_num:
                                    captured_order_id = str(order_num)
                    except (PlaywrightError, ValueError, TypeError) as exc:
                        logger.debug("Unable to read order details from checkout response: %s", exc)

            page.on("response", handle_response)

            logger.info(f"[Handoff] Navigating to Checkout / ACS URL: {target_url}...")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError("3DS handoff timed out before navigation")
            await page.goto(
                target_url,
                wait_until="domcontentloaded",
                timeout=max(1, min(60000, int(remaining * 1000))),
            )

            logger.info(
                f"[Handoff] Page loaded. Awaiting 3DS confirmation or manual approval (Max {timeout_sec}s)..."
            )
            is_success = False
            last_url = page.url

            while time.monotonic() < deadline:
                current_url = page.url
                last_url = current_url

                for marker in success_markers:
                    if marker in current_url.lower():
                        is_success = True
                        logger.info(
                            f"[Handoff] 🏆 3D Secure Verification PASSED! Order confirmed at: {current_url}"
                        )
                        break

                if is_success:
                    match = re.search(r"orders?/([A-Z0-9_-]+)", current_url, re.IGNORECASE)
                    if match and not captured_order_id:
                        captured_order_id = match.group(1)

                    if not captured_order_id:
                        try:
                            order_elem = await page.query_selector(".order-number, .order-id, [data-order-id]")
                            if order_elem:
                                text_val = await order_elem.inner_text()
                                if text_val and text_val.strip():
                                    captured_order_id = text_val.strip()
                        except PlaywrightError as exc:
                            logger.debug("Unable to read order number from confirmation page: %s", exc)
                    break

                await asyncio.sleep(min(1.0, max(0.0, deadline - time.monotonic())))

            final_browser_cookies = await context.cookies()
            elapsed_sec = round(time.monotonic() - start_time, 2)

            return {
                "success": is_success,
                "order_id": captured_order_id,
                "final_url": last_url,
                "elapsed_seconds": elapsed_sec,
                "cookies_count": len(final_browser_cookies),
            }

        finally:
            await browser.close()
            logger.info(f"[Handoff] Browser session closed gracefully in {round(time.monotonic() - start_time, 2)}s.")


async def handoff_to_browser(
    curl_session: AsyncSession,
    target_url: str,
    proxy: str | None = None,
    user_agent: str | None = None,
    headless: bool = False,
    timeout_sec: float = 180.0,
    success_url_substrings: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Run the complete Playwright handoff under one end-to-end timeout."""
    if not math.isfinite(timeout_sec) or timeout_sec <= 0:
        raise ValueError("timeout_sec must be finite and greater than zero")

    return await asyncio.wait_for(
        _handoff_to_browser(
            curl_session=curl_session,
            target_url=target_url,
            proxy=proxy,
            user_agent=user_agent,
            headless=headless,
            timeout_sec=timeout_sec,
            success_url_substrings=success_url_substrings,
        ),
        timeout=timeout_sec,
    )