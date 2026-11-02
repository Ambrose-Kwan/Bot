"""
Module: core.monitor
Description: Standalone high-speed stock monitoring engine featuring dynamic
query-string cache busting, explicit proxy routing, and coordinated lifecycle reporting.
"""
from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from inspect import isawaitable
from typing import Any

try:
    from .events import StockEventBus, StockEventPayload, global_event_bus
    from .exceptions import RateLimitError, ResourceCleanupError
    from .network import RobustAsyncSession
except ImportError:
    from core.events import StockEventBus, StockEventPayload, global_event_bus
    from core.exceptions import RateLimitError, ResourceCleanupError
    try:
        from core.network import RobustAsyncSession
    except ImportError:
        from network import RobustAsyncSession  # type: ignore[no-redef]

logger = logging.getLogger("nexus.monitor")


class FastStockMonitor:
    """
    Lightweight, decoupled inventory poller designed to penetrate CDN edge caches
    and broadcast instantly to pre-warmed worker queues.
    """

    API_INVENTORY_URL: str = (
        "https://www.vans.com/api/products/v1/styles/{style_code}/inventory?locale=en-us"
    )

    def __init__(
        self,
        sku: str,
        preferred_sizes: list[str],
        poll_interval_ms: int = 1200,
        proxy: str | None = None,
        event_bus: StockEventBus | None = None,
        timeout: float = 6.0,
        max_monitor_duration: float = 900.0,
        max_consecutive_errors: int = 5,
        max_backoff_seconds: float = 60.0,
    ) -> None:
        if poll_interval_ms <= 0:
            raise ValueError("poll_interval_ms must be greater than zero")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if max_monitor_duration <= 0:
            raise ValueError("max_monitor_duration must be greater than zero")
        if max_consecutive_errors < 1:
            raise ValueError("max_consecutive_errors must be at least one")
        if max_backoff_seconds <= 0:
            raise ValueError("max_backoff_seconds must be greater than zero")

        self.sku = sku.strip().upper()
        self.preferred_sizes = preferred_sizes or ["ANY_SIZE"]
        self.poll_interval = poll_interval_ms / 1000.0
        self.event_bus = event_bus or global_event_bus
        self.proxy = proxy
        self.timeout = timeout
        self.max_monitor_duration = max_monitor_duration
        self.max_consecutive_errors = max_consecutive_errors
        self.max_backoff_seconds = max_backoff_seconds
        self._stopped = False
        self.exit_reason: str | None = None
        self.last_error: BaseException | None = None
        self.last_status_code: int | None = None
        self.cleanup_errors: list[BaseException] = []
        self.session: RobustAsyncSession | None = None

    def _rate_limit_delay(self, retry_after: float, consecutive_errors: int) -> float:
        if math.isfinite(retry_after) and retry_after > 0:
            return retry_after
        exponent = min(consecutive_errors - 1, 30)
        return min(self.poll_interval * (2**exponent), self.max_backoff_seconds)

    def _build_cache_busting_url(self, base_url: str) -> str:
        """Append nanosecond timestamp and random token to bypass CDN edge caches."""
        now_ns = time.time_ns()
        rnd_token = random.randint(100000, 999999)
        delimiter = "&" if "?" in base_url else "?"
        return f"{base_url}{delimiter}_nocache={now_ns}&r={rnd_token}"

    def _parse_and_find_variant(self, data: dict[str, Any]) -> dict[str, Any] | None:
        """Parse inventory JSON and match available variants by size priority."""
        product = data.get("product", data)
        variants = product.get("variants", data.get("variants", []))
        if not isinstance(variants, list):
            return None

        available_variants: dict[str, dict[str, Any]] = {}
        for item in variants:
            if not isinstance(item, dict):
                continue
            available_qty = int(item.get("availableQuantity") or 0)
            in_stock = bool(item.get("inStock", False) or available_qty > 0)
            if in_stock:
                size_label = str(item.get("size") or item.get("sizeDescription") or "").strip()
                v_id = str(item.get("productId") or "")
                if v_id:
                    available_variants[size_label] = item
                    numeric_token = (
                        size_label.split("=")[0]
                        .replace("US", "")
                        .replace("Men", "")
                        .strip()
                    )
                    if numeric_token:
                        available_variants[numeric_token] = item

        for pref in self.preferred_sizes:
            if pref == "ANY_SIZE" and available_variants:
                return next(iter(available_variants.values()))
            if pref in available_variants:
                return available_variants[pref]
        return None

    async def start(self) -> None:
        """Run low-overhead inventory polling loop until stock broadcast or deadline."""
        self._stopped = False
        self.exit_reason = None
        self.last_error = None
        self.last_status_code = None
        self.cleanup_errors.clear()
        proxy_list = [self.proxy] if self.proxy else None
        self.session = RobustAsyncSession(
            proxy_pool=proxy_list,
            timeout=self.timeout,
            task_id=f"MONITOR-{self.sku}",
        )

        style_code = self.sku.replace("-", "").upper()
        if not style_code.startswith("VN"):
            style_code = f"VN{style_code}"
        endpoint = self.API_INVENTORY_URL.format(style_code=style_code)

        headers = {
            "Accept": "application/json",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "siteid": "VANS-US",
            "source": "ECOM15",
            "locale": "en_US",
        }

        route_label = self.proxy if self.proxy else "DIRECT"
        logger.info(
            "🚀 [MonitorEngine] Started | SKU: %s | Sizes: %s | Interval: %.2fs | "
            "MaxDuration: %.1fs | Route: %s",
            self.sku,
            self.preferred_sizes,
            self.poll_interval,
            self.max_monitor_duration,
            route_label,
        )

        active_exc: BaseException | None = None
        try:
            await self.session.__aenter__()
            deadline = time.monotonic() + self.max_monitor_duration
            consecutive_errors = 0
            while not self._stopped:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.exit_reason = "duration_limit"
                    logger.warning(
                        "Monitor duration limit reached for SKU %s (%.1fs).",
                        self.sku,
                        self.max_monitor_duration,
                    )
                    break

                t_start = time.monotonic()
                cache_busted_url = self._build_cache_busting_url(endpoint)
                try:
                    resp = await asyncio.wait_for(
                        self.session.execute_with_retry(
                            "GET",
                            cache_busted_url,
                            headers=headers,
                            timeout=min(self.timeout, remaining),
                        ),
                        timeout=remaining,
                    )

                    if resp.status_code == 200:
                        self.last_error = None
                        self.last_status_code = None
                        data = resp.json()
                        if not isinstance(data, dict):
                            raise ValueError("Inventory response must be a JSON object")
                        product = data.get("product", data)
                        if not isinstance(product, dict):
                            raise ValueError("Inventory product must be a JSON object")
                        variants = product.get("variants", data.get("variants", []))
                        if not isinstance(variants, list):
                            raise ValueError("Inventory variants must be a list")

                        matched_variant = self._parse_and_find_variant(data)
                        consecutive_errors = 0
                        if matched_variant:
                            v_id = str(matched_variant.get("productId") or "")
                            s_label = str(matched_variant.get("size") or "")
                            price = float(matched_variant.get("price") or 0.0)
                            logger.info(
                                "🎯 [MonitorEngine] Stock detected! Variant: %s (%s)",
                                v_id,
                                s_label,
                            )

                            payload = StockEventPayload(
                                sku=self.sku,
                                variant_id=v_id,
                                size_label=s_label,
                                price=price,
                                timestamp_ms=int(time.time() * 1000),
                                raw_response=matched_variant,
                            )
                            await self.event_bus.publish(payload)
                            self.exit_reason = "stock_found"
                            break
                    else:
                        self.last_status_code = resp.status_code
                        raise RuntimeError(
                            f"Inventory endpoint returned HTTP {resp.status_code}"
                        )

                    delay = max(0.0, self.poll_interval - (time.monotonic() - t_start))
                except RateLimitError as err:
                    self.last_error = err
                    self.last_status_code = err.status_code
                    consecutive_errors += 1
                    if consecutive_errors >= self.max_consecutive_errors:
                        self.exit_reason = "max_rate_limits"
                        logger.error(
                            "Monitor stopped after %d consecutive rate limits for SKU %s.",
                            consecutive_errors,
                            self.sku,
                        )
                        break
                    delay = self._rate_limit_delay(err.retry_after, consecutive_errors)
                    logger.warning(
                        "Monitor rate limited for SKU %s; waiting %.2fs before next check.",
                        self.sku,
                        delay,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as err:  # noqa: BLE001 - polling retries unexpected parse and transport errors
                    self.last_error = err
                    status_code = getattr(err, "status_code", None)
                    if status_code is not None:
                        self.last_status_code = status_code
                    consecutive_errors += 1
                    logger.warning(
                        "Monitor iteration %d/%d failed for SKU %s: %s",
                        consecutive_errors,
                        self.max_consecutive_errors,
                        self.sku,
                        err,
                    )
                    if consecutive_errors >= self.max_consecutive_errors:
                        self.exit_reason = "max_errors"
                        logger.error(
                            "Monitor stopped after %d consecutive errors for SKU %s.",
                            consecutive_errors,
                            self.sku,
                        )
                        break
                    delay = min(
                        self.poll_interval * (2 ** min(consecutive_errors - 1, 30)),
                        self.max_backoff_seconds,
                    )

                remaining = deadline - time.monotonic()
                if remaining > 0 and delay > 0:
                    await asyncio.sleep(min(delay, remaining))

            if self.exit_reason is None and self._stopped:
                self.exit_reason = "stopped"
        except BaseException as exc:
            active_exc = exc
            raise
        finally:
            self._stopped = True
            session, self.session = self.session, None
            if session is not None:
                close_fn = getattr(session, "close", None)
                if callable(close_fn):
                    try:
                        res = close_fn()
                        if isawaitable(res):
                            await res
                    except asyncio.CancelledError as close_exc:
                        self.cleanup_errors.append(close_exc)
                        if active_exc is None:
                            raise
                    except Exception as close_exc:
                        self.cleanup_errors.append(close_exc)
                        logger.error(
                            "Failed to close monitor session for SKU %s: %s",
                            self.sku,
                            close_exc,
                        )
                        if active_exc is None:
                            raise ResourceCleanupError(
                                f"Monitor session cleanup failed for {self.sku}: {close_exc}",
                                task_id=f"MONITOR-{self.sku}",
                                errors=[("monitor.session.close", close_exc)],
                            ) from close_exc

    def stop(self) -> None:
        self._stopped = True