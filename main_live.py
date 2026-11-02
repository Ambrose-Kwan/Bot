"""
Module: main_live.py
Description: Two-stage decoupled live Vans task dispatcher with:
  1. Explicit proxy lease ownership tracking (no cross-task or BYO pool pollution).
  2. Structured proxy health vs. task outcome classification.
  3. Non-swallowing zero-leak resource cleanup with CancelledError preservation.
  4. Consistent outbound routing across FastStockMonitor and VansSite workers.
  5. Unified Stage 2 deadline coordination across monitor and awaiting workers.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from inspect import isawaitable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from weakref import WeakSet

from pydantic import BaseModel, ConfigDict, Field, ValidationError

try:
    from core.proxy import ProxyPool, ProxyPoolEmptyError
except ImportError:
    try:
        from .core.proxy import ProxyPool, ProxyPoolEmptyError
    except ImportError:
        try:
            from proxy import ProxyPool, ProxyPoolEmptyError  # type: ignore[import-not-found]
        except ImportError:
            ProxyPool = None  # type: ignore[assignment, misc]
            ProxyPoolEmptyError = Exception  # type: ignore[assignment, misc]

try:
    from core.events import StockEventPayload, global_event_bus
    from core.exceptions import (
        CheckoutException,
        NetworkTransportError,
        RateLimitError,
        ResourceCleanupError,
        WAFChallengeError,
    )
    from core.monitor import FastStockMonitor
except ImportError:
    from .core.events import StockEventPayload, global_event_bus
    from .core.exceptions import (
        CheckoutException,
        NetworkTransportError,
        RateLimitError,
        ResourceCleanupError,
        WAFChallengeError,
    )
    from .core.monitor import FastStockMonitor

if TYPE_CHECKING:
    from .models.profile import BuyerProfile, ProxyConfig, TaskConfig, VariantPriorityModel
    from .sites.base_site import CheckoutResult
    from .sites.vans_site import VansSite
else:
    if __package__:
        from .models.profile import BuyerProfile, ProxyConfig, TaskConfig, VariantPriorityModel
        from .sites.base_site import CheckoutResult
        from .sites.vans_site import VansSite
    else:
        from models.profile import BuyerProfile, ProxyConfig, TaskConfig, VariantPriorityModel
        from sites.base_site import CheckoutResult
        from sites.vans_site import VansSite

logger = logging.getLogger("nexus.live_dispatcher")

_HTTP_RESTRICTED_STATUS_RE = re.compile(
    r"(?:\bHTTP[/\s]+|\bstatus(?:\s+code)?\s*[:=]?\s*)(407)\b",
    re.IGNORECASE,
)


class ProxyHealthDisposition(str, Enum):
    """Separates proxy transport health from application/business outcomes."""

    HEALTHY = "healthy"
    RESTRICTED = "restricted"


@dataclass
class ProxyLease:
    """Explicit ownership record for a proxy leased by this dispatcher."""

    task_id: str
    proxy_url: str | None = None
    owned_by_dispatcher: bool = False
    disposition: ProxyHealthDisposition = ProxyHealthDisposition.HEALTHY
    status_code: int = 200
    cooldown: float | None = None
    health_reported: bool = False
    task_released: bool = False
    settled: bool = False


@dataclass(frozen=True)
class CleanupFailureRecord:
    """Structured record of any session or proxy cleanup error."""

    task_id: str
    step: str
    error: BaseException


class LiveTaskInput(BaseModel):
    """Runtime-validated buyer and proxy inputs for a live dispatch."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    site_key: Literal["vans"] = "vans"
    target_url_template: str = "https://www.vans.com/en-us/p/shoes/{sku}"
    buyers: list[BuyerProfile] = Field(default_factory=list)
    proxy_pool: list[str] = Field(
        default_factory=list,
        description="Optional proxy nodes managed dynamically by ProxyPool",
    )
    monitor_proxy: str | None = Field(
        default=None,
        description="Optional dedicated proxy URL reserved for FastStockMonitor",
    )
    allow_direct_monitor: bool = Field(
        default=False,
        description="Explicit opt-in allowing monitor to run direct when proxy_pool is set",
    )


def _classify_exception_for_proxy(
    exc: BaseException,
) -> tuple[ProxyHealthDisposition, int, float | None]:
    """
    Classify whether an exception reflects proxy/network degradation vs. normal
    lifecycle or business-layer errors.
    """
    if isinstance(exc, asyncio.CancelledError):
        return (ProxyHealthDisposition.HEALTHY, 200, None)

    if isinstance(exc, NetworkTransportError):
        return (ProxyHealthDisposition.RESTRICTED, exc.status_code or 503, 15.0)

    if isinstance(exc, ConnectionError):
        return (ProxyHealthDisposition.RESTRICTED, 503, 15.0)

    if isinstance(exc, RateLimitError):
        return (
            ProxyHealthDisposition.RESTRICTED,
            429,
            max(15.0, exc.retry_after),
        )

    if isinstance(exc, WAFChallengeError):
        return (
            ProxyHealthDisposition.RESTRICTED,
            exc.status_code,
            60.0 if exc.status_code == 403 else 15.0,
        )

    if isinstance(exc, CheckoutException):
        if exc.status_code == 407:
            return (ProxyHealthDisposition.RESTRICTED, 407, 15.0)
        return (ProxyHealthDisposition.HEALTHY, exc.status_code or 200, None)

    return (ProxyHealthDisposition.HEALTHY, 200, None)


def _classify_result_for_proxy(
    result: CheckoutResult,
) -> tuple[ProxyHealthDisposition, int, float | None]:
    """Classify a returned CheckoutResult without conflating business failures with proxy faults."""
    if result.success:
        return (ProxyHealthDisposition.HEALTHY, 200, None)

    status_code = getattr(result, "status_code", None) or getattr(result, "http_status", None)
    if status_code == 407:
        return (ProxyHealthDisposition.RESTRICTED, 407, 15.0)

    msg = result.error_message or ""
    match = _HTTP_RESTRICTED_STATUS_RE.search(msg)
    if match:
        parsed_code = int(match.group(1))
        cd = 60.0 if parsed_code in {403, 429} else 15.0
        return (ProxyHealthDisposition.RESTRICTED, parsed_code, cd)

    return (ProxyHealthDisposition.HEALTHY, 200, None)


class TwoStageLiveDispatcher:
    """
    Orchestrates the two-stage execution pipeline with strict proxy lease ownership,
    decoupled health reporting, consistent monitor routing, and unified Stage 2 deadlines.
    """

    def __init__(
        self,
        concurrency_limit: int,
        proxy_pool: Any | None = None,
        *,
        monitor_proxy: str | None = None,
        allow_direct_monitor: bool = False,
    ) -> None:
        if concurrency_limit < 1:
            raise ValueError("concurrency_limit must be at least 1")
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.proxy_pool = proxy_pool
        self.monitor_proxy = (
            ProxyConfig.from_string(monitor_proxy).formatted_url
            if monitor_proxy
            else None
        )
        self.allow_direct_monitor = allow_direct_monitor
        self.active_tasks = 0
        self.max_active_tasks = 0
        self.cleanup_errors: list[CleanupFailureRecord] = []
        self._task_leases: dict[str, ProxyLease] = {}
        self._closed_workers: WeakSet[Any] = WeakSet()
        self._workers_by_task: dict[str, Any] = {}

    def _record_cleanup_failure(
        self,
        task_id: str,
        step: str,
        error: BaseException,
    ) -> CleanupFailureRecord:
        record = CleanupFailureRecord(task_id=task_id, step=step, error=error)
        self.cleanup_errors.append(record)
        logger.error("[%s] Cleanup step '%s' failed: %s", task_id, step, error)
        return record

    async def _settle_proxy_lease(
        self,
        lease: ProxyLease | None,
        *,
        disposition: ProxyHealthDisposition,
        status_code: int = 200,
        cooldown: float | None = None,
    ) -> list[CleanupFailureRecord]:
        """
        Return a lease to self.proxy_pool ONLY if this dispatcher acquired it
        and it has not already been settled.
        """
        failures: list[CleanupFailureRecord] = []
        if (
            self.proxy_pool is None
            or lease is None
            or not lease.owned_by_dispatcher
            or lease.settled
        ):
            return failures

        if disposition == ProxyHealthDisposition.RESTRICTED:
            lease.disposition = disposition
            lease.status_code = status_code
            lease.cooldown = cooldown

        if not lease.proxy_url:
            lease.health_reported = True

        if lease.proxy_url and not lease.health_reported:
            try:
                if lease.disposition == ProxyHealthDisposition.RESTRICTED:
                    await self.proxy_pool.report_restricted(
                        lease.proxy_url,
                        status_code=lease.status_code,
                        cooldown=15.0 if lease.cooldown is None else lease.cooldown,
                        task_id=lease.task_id,
                    )
                else:
                    await self.proxy_pool.report_success(
                        lease.proxy_url,
                        task_id=lease.task_id,
                    )
                lease.health_reported = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - settlement must preserve retry state
                failures.append(
                    self._record_cleanup_failure(lease.task_id, "proxy_pool.report", exc)
                )

        # Keep the lease unavailable and retryable if health reporting fails.
        if (not lease.proxy_url or lease.health_reported) and not lease.task_released:
            try:
                await self.proxy_pool.release_task(lease.task_id)
                lease.task_released = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - cleanup must continue through sibling steps
                failures.append(
                    self._record_cleanup_failure(lease.task_id, "proxy_pool.release_task", exc)
                )

        lease.settled = lease.health_reported and lease.task_released

        return failures

    async def _cleanup_worker(
        self,
        worker: Any | None,
        task_config: TaskConfig,
        *,
        disposition: ProxyHealthDisposition = ProxyHealthDisposition.HEALTHY,
        status_code: int = 200,
        cooldown: float | None = None,
        suppress_raise: bool = False,
    ) -> list[CleanupFailureRecord]:
        """
        Idempotently close worker session and settle dispatcher-owned proxy lease.
        Guarantees every cleanup step runs even if an earlier step raises, logs all
        failures at ERROR level, and raises ResourceCleanupError unless suppressed
        to preserve an in-flight exception/CancelledError.
        """
        task_id = getattr(task_config, "task_id", "UNKNOWN")
        failures: list[CleanupFailureRecord] = []
        cancelled_exc: asyncio.CancelledError | None = None

        if worker is None:
            worker = self._workers_by_task.get(task_id)

        worker_closed = worker is None
        if worker is not None:
            try:
                worker_closed = worker in self._closed_workers
            except TypeError:
                worker_closed = False

        if worker is not None and not worker_closed:
            session = getattr(worker, "session", None)
            close_fn = getattr(session, "close", None) if session is not None else None
            if callable(close_fn):
                try:
                    close_res = close_fn()
                    if isawaitable(close_res):
                        await close_res
                    try:
                        self._closed_workers.add(worker)
                    except TypeError:
                        pass
                    if self._workers_by_task.get(task_id) is worker:
                        self._workers_by_task.pop(task_id, None)
                    logger.debug("[%s] Worker session closed cleanly", task_id)
                except asyncio.CancelledError as exc:
                    cancelled_exc = exc
                except Exception as exc:  # noqa: BLE001 - close failure must not block lease settlement
                    failures.append(
                        self._record_cleanup_failure(task_id, "session.close", exc)
                    )
            else:
                try:
                    self._closed_workers.add(worker)
                except TypeError:
                    pass
                if self._workers_by_task.get(task_id) is worker:
                    self._workers_by_task.pop(task_id, None)

        lease = self._task_leases.get(task_id)
        try:
            lease_failures = await self._settle_proxy_lease(
                lease,
                disposition=disposition,
                status_code=status_code,
                cooldown=cooldown,
            )
            failures.extend(lease_failures)
        except asyncio.CancelledError as exc:
            cancelled_exc = cancelled_exc or exc

        if cancelled_exc is not None:
            raise cancelled_exc

        if failures and not suppress_raise:
            summary = "; ".join(f"{f.step}: {f.error}" for f in failures)
            raise ResourceCleanupError(
                f"Cleanup failed for task {task_id}: {summary}",
                task_id=task_id,
                errors=[(f.step, f.error) for f in failures],
            )

        return failures

    async def _retry_pending_cleanups(
        self,
        tasks: Sequence[tuple[TaskConfig, BuyerProfile]],
    ) -> list[CleanupFailureRecord]:
        cleanup_coroutines = []
        for task_config, _ in tasks:
            task_id = task_config.task_id
            worker = self._workers_by_task.get(task_id)
            lease = self._task_leases.get(task_id)
            if worker is None and (lease is None or lease.settled):
                continue
            cleanup_coroutines.append(
                self._cleanup_worker(
                    worker,
                    task_config,
                    disposition=(
                        lease.disposition
                        if lease is not None
                        else ProxyHealthDisposition.HEALTHY
                    ),
                    status_code=lease.status_code if lease is not None else 200,
                    cooldown=lease.cooldown if lease is not None else None,
                    suppress_raise=True,
                )
            )

        if not cleanup_coroutines:
            return []

        results = await asyncio.gather(*cleanup_coroutines, return_exceptions=True)
        failures: list[CleanupFailureRecord] = []
        for result in results:
            if isinstance(result, list):
                failures.extend(result)
            elif isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                failures.append(
                    self._record_cleanup_failure(
                        "PENDING-CLEANUP",
                        "cleanup_worker",
                        result,
                    )
                )
        return failures

    async def prewarm_task(
        self,
        task_config: TaskConfig,
        buyer: BuyerProfile,
    ) -> tuple[TaskConfig, VansSite]:
        """
        Stage 1: Lease proxy (if unbound) -> bind to TaskConfig -> build VansSite -> prewarm.
        Tracks explicit lease ownership and preserves original failure exceptions.
        """
        async with self.semaphore:
            worker: VansSite | None = None
            task_id = task_config.task_id

            if getattr(task_config, "bound_proxy", None) is not None:
                bound = task_config.bound_proxy
                preset_url = (
                    bound.formatted_url if hasattr(bound, "formatted_url") else str(bound)
                )
                self._task_leases[task_id] = ProxyLease(
                    task_id=task_id,
                    proxy_url=preset_url,
                    owned_by_dispatcher=False,
                )
            else:
                self._task_leases[task_id] = ProxyLease(
                    task_id=task_id,
                    proxy_url=None,
                    owned_by_dispatcher=False,
                )

            try:
                if self.proxy_pool and not getattr(task_config, "bound_proxy", None):
                    assigned_proxy = await self.proxy_pool.get_proxy(
                        timeout=15.0,
                        task_id=task_id,
                    )
                    self._task_leases[task_id] = ProxyLease(
                        task_id=task_id,
                        proxy_url=assigned_proxy,
                        owned_by_dispatcher=True,
                    )
                    task_config.bound_proxy = ProxyConfig.from_string(assigned_proxy)

                worker = VansSite(task_config, buyer)
                self._workers_by_task[task_id] = worker

                proxy_display = (
                    task_config.bound_proxy.formatted_url
                    if getattr(task_config, "bound_proxy", None)
                    else "DIRECT"
                )
                logger.info("🔥 [%s] Pre-warming worker | Route: %s", task_id, proxy_display)

                await worker.prewarm()
                return (task_config, worker)

            except asyncio.CancelledError as cancel_exc:
                logger.warning("⚠️ [%s] Prewarm cancelled; cleaning up resources", task_id)
                cleanup_failures = await self._cleanup_worker(
                    worker,
                    task_config,
                    disposition=ProxyHealthDisposition.HEALTHY,
                    suppress_raise=True,
                )
                if cleanup_failures and hasattr(cancel_exc, "add_note"):
                    for failure in cleanup_failures:
                        cancel_exc.add_note(
                            f"Cleanup failure ({failure.step}): {failure.error}"
                        )
                raise

            except Exception as exc:
                disposition, code, cooldown = _classify_exception_for_proxy(exc)
                logger.error(
                    "❌ [%s] Prewarm failed (%s, proxy_disposition=%s); releasing resources",
                    task_id,
                    exc,
                    disposition.value,
                )
                await self._cleanup_worker(
                    worker,
                    task_config,
                    disposition=disposition,
                    status_code=code,
                    cooldown=cooldown,
                    suppress_raise=True,
                )
                raise

    async def prewarm_worker(
        self,
        worker: Any,
        task_config: TaskConfig,
    ) -> bool:
        """Backward-compatible helper when a worker instance was constructed externally."""
        async with self.semaphore:
            task_id = getattr(task_config, "task_id", "TASK")
            self._workers_by_task[task_id] = worker
            if task_id not in self._task_leases:
                self._task_leases[task_id] = ProxyLease(
                    task_id=task_id,
                    owned_by_dispatcher=False,
                )
            try:
                if self.proxy_pool and not getattr(task_config, "bound_proxy", None):
                    assigned_proxy = await self.proxy_pool.get_proxy(
                        timeout=15.0,
                        task_id=task_id,
                    )
                    self._task_leases[task_id] = ProxyLease(
                        task_id=task_id,
                        proxy_url=assigned_proxy,
                        owned_by_dispatcher=True,
                    )
                    task_config.bound_proxy = ProxyConfig.from_string(assigned_proxy)

                await worker.prewarm()
                return True
            except asyncio.CancelledError:
                await self._cleanup_worker(
                    worker,
                    task_config,
                    disposition=ProxyHealthDisposition.HEALTHY,
                    suppress_raise=True,
                )
                raise
            except Exception as exc:  # noqa: BLE001 - prewarm worker failures become task results
                disposition, code, cooldown = _classify_exception_for_proxy(exc)
                logger.error("❌ [%s] Worker prewarm failed: %s", task_id, exc)
                await self._cleanup_worker(
                    worker,
                    task_config,
                    disposition=disposition,
                    status_code=code,
                    cooldown=cooldown,
                    suppress_raise=True,
                )
                return False

    async def _await_stock_signal(
        self,
        queue: asyncio.Queue[StockEventPayload],
        *,
        wait_timeout: float,
        monitor_abort_event: asyncio.Event | None = None,
        abort_reason_getter: Callable[[], str | None] | None = None,
    ) -> tuple[StockEventPayload | None, str | None]:
        """
        Wait for a stock broadcast up to wait_timeout, or abort early if the monitor
        terminates abnormally without having queued a stock signal.
        """
        if not queue.empty():
            return (queue.get_nowait(), None)

        if wait_timeout <= 0:
            return (None, "Timed out waiting for stock broadcast")

        if monitor_abort_event is None:
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=wait_timeout)
                return (payload, None)
            except asyncio.TimeoutError:
                return (None, "Timed out waiting for stock broadcast")

        get_task: asyncio.Task[StockEventPayload] = asyncio.create_task(queue.get())
        abort_task: asyncio.Task[bool] = asyncio.create_task(monitor_abort_event.wait())
        try:
            done, pending = await asyncio.wait(
                {get_task, abort_task},
                timeout=wait_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for p_task in pending:
                p_task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            if get_task in done:
                return (get_task.result(), None)

            if not queue.empty():
                return (queue.get_nowait(), None)

            if abort_task in done:
                reason = abort_reason_getter() if abort_reason_getter else None
                return (None, reason or "Stock monitor stopped before stock broadcast")

            return (None, "Timed out waiting for stock broadcast")
        except BaseException:
            for helper in (get_task, abort_task):
                if not helper.done():
                    helper.cancel()
            await asyncio.gather(get_task, abort_task, return_exceptions=True)
            raise

    async def wait_and_burst(
        self,
        worker: VansSite,
        task_config: TaskConfig,
        queue: asyncio.Queue[StockEventPayload],
        timeout: float = 120.0,
        *,
        deadline: float | None = None,
        monitor_abort_event: asyncio.Event | None = None,
        abort_reason_getter: Callable[[], str | None] | None = None,
    ) -> CheckoutResult:
        """
        Stage 2: Wait for stock event under unified deadline, then burst ATC under
        semaphore limit with decoupled proxy health reporting.
        """
        disposition = ProxyHealthDisposition.HEALTHY
        status_code = 200
        cooldown: float | None = None
        in_flight_cancel: asyncio.CancelledError | None = None

        try:
            wait_seconds = (
                max(0.0, deadline - time.monotonic()) if deadline is not None else timeout
            )
            logger.info(
                "💤 [%s] Pre-warmed and awaiting stock broadcast (budget: %.1fs)...",
                task_config.task_id,
                wait_seconds,
            )

            payload, wait_error = await self._await_stock_signal(
                queue,
                wait_timeout=wait_seconds,
                monitor_abort_event=monitor_abort_event,
                abort_reason_getter=abort_reason_getter,
            )
            if payload is None:
                msg = wait_error or "Timed out waiting for stock broadcast"
                logger.error("⏰ [%s] %s", task_config.task_id, msg)
                # Stock timeout or monitor stop is NOT a proxy fault; disposition stays HEALTHY.
                return CheckoutResult(success=False, error_message=msg)

            worker.apply_stock_signal(payload)
            logger.info("🚀 [%s] Stock signal received! Bursting checkout...", task_config.task_id)

            if deadline is None:
                async with self.semaphore:
                    result = await worker.execute_fast_checkout()
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return CheckoutResult(
                        success=False,
                        error_message="Stage 2 deadline expired before checkout",
                    )
                try:
                    await asyncio.wait_for(self.semaphore.acquire(), timeout=remaining)
                except asyncio.TimeoutError:
                    return CheckoutResult(
                        success=False,
                        error_message="Stage 2 deadline expired waiting for checkout capacity",
                    )

                try:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return CheckoutResult(
                            success=False,
                            error_message="Stage 2 deadline expired before checkout",
                        )
                    checkout_task = asyncio.create_task(worker.execute_fast_checkout())
                    try:
                        done, _ = await asyncio.wait({checkout_task}, timeout=remaining)
                        if not done:
                            checkout_task.cancel()
                            await asyncio.gather(checkout_task, return_exceptions=True)
                            return CheckoutResult(
                                success=False,
                                error_message="Stage 2 deadline expired during checkout",
                            )
                        result = checkout_task.result()
                    except BaseException:
                        if not checkout_task.done():
                            checkout_task.cancel()
                        await asyncio.gather(checkout_task, return_exceptions=True)
                        raise
                finally:
                    self.semaphore.release()

            disposition, status_code, cooldown = _classify_result_for_proxy(result)
            return result

        except asyncio.CancelledError as cancel_exc:
            in_flight_cancel = cancel_exc
            disposition = ProxyHealthDisposition.HEALTHY
            logger.warning("⚠️ [%s] Burst task cancelled", task_config.task_id)
            raise
        except Exception as exc:  # noqa: BLE001 - worker execution boundary returns structured failures
            disposition, status_code, cooldown = _classify_exception_for_proxy(exc)
            logger.error(
                "💥 [%s] Burst execution error (%s, proxy_disposition=%s)",
                task_config.task_id,
                exc,
                disposition.value,
            )
            return CheckoutResult(
                success=False,
                error_message=f"Burst error: {exc}",
            )
        finally:
            cleanup_failures = await self._cleanup_worker(
                worker,
                task_config,
                disposition=disposition,
                status_code=status_code,
                cooldown=cooldown,
                suppress_raise=in_flight_cancel is not None,
            )
            if in_flight_cancel is not None and cleanup_failures and hasattr(
                in_flight_cancel, "add_note"
            ):
                for failure in cleanup_failures:
                    in_flight_cancel.add_note(
                        f"Cleanup failure ({failure.step}): {failure.error}"
                    )

    async def _acquire_monitor_route(
        self,
        sku: str,
        timeout_budget: float,
        *,
        worker_proxy_urls: Sequence[str | None] = (),
    ) -> tuple[str | None, ProxyLease | None]:
        """
        Resolve outbound proxy for FastStockMonitor consistently with worker routing:
          - Dedicated monitor_proxy -> use directly (not owned by ProxyPool).
          - Active ProxyPool -> lease dedicated MONITOR-{SKU} proxy (never share worker lease).
          - Exhausted ProxyPool -> fail closed unless allow_direct_monitor is explicitly True.
          - Without a pool, a single worker proxy route is reused for monitoring.
          - Mixed worker routes require an explicit monitor route or direct opt-in.
        """
        if self.monitor_proxy:
            return (self.monitor_proxy, None)

        if self.proxy_pool is None:
            routes = set(worker_proxy_urls)
            if len(routes) == 1:
                return (next(iter(routes)), None)
            if len(routes) > 1 and not self.allow_direct_monitor:
                raise ValueError(
                    "Workers use different proxy routes; configure monitor_proxy "
                    "or enable allow_direct_monitor."
                )
            if len(routes) > 1:
                logger.warning(
                    "Worker proxy routes differ; using DIRECT monitor route because "
                    "allow_direct_monitor=True."
                )
            return (None, None)

        monitor_task_id = f"MONITOR-{sku.strip().upper()}"
        try:
            leased_url = await self.proxy_pool.get_proxy(
                timeout=min(10.0, timeout_budget),
                task_id=monitor_task_id,
            )
            lease = ProxyLease(
                task_id=monitor_task_id,
                proxy_url=leased_url,
                owned_by_dispatcher=True,
            )
            logger.info("📡 [%s] Dedicated monitor proxy leased: %s", monitor_task_id, leased_url)
            return (leased_url, lease)
        except ProxyPoolEmptyError as exc:
            if self.allow_direct_monitor:
                logger.warning(
                    "⚠️ [%s] ProxyPool exhausted for monitor; falling back to DIRECT "
                    "(allow_direct_monitor=True)",
                    monitor_task_id,
                )
                return (None, None)
            logger.error(
                "❌ [%s] ProxyPool has no spare proxy for FastStockMonitor and "
                "allow_direct_monitor=False. Aborting to prevent direct IP leak.",
                monitor_task_id,
            )
            raise ProxyPoolEmptyError(
                "No spare proxy available in ProxyPool for FastStockMonitor. "
                "Add an extra proxy to proxy_pool, configure monitor_proxy, "
                "or enable allow_direct_monitor."
            ) from exc

    async def run_pipeline(
        self,
        sku: str,
        preferred_sizes: list[str],
        tasks: list[tuple[TaskConfig, BuyerProfile]],
        monitor_poll_ms: int = 1200,
        max_wait_seconds: float = 900.0,
    ) -> list[CheckoutResult]:
        """
        Execute Stage 1 prewarm and Stage 2 monitor + burst under a unified deadline,
        strict lease ownership, and zero-leak cleanup verification.
        """
        if max_wait_seconds <= 0:
            raise ValueError("max_wait_seconds must be greater than zero")

        cleanup_baseline = len(self.cleanup_errors)
        final_results: list[CheckoutResult | None] = [None] * len(tasks)

        # =========================================================
        # [Stage 1] Concurrent Pre-warm with Cascade Cancellation
        # =========================================================
        logger.info("=== [Stage 1] Starting worker proxy allocation and pre-warm ===")
        prewarm_tasks = [
            asyncio.create_task(
                self.prewarm_task(cfg, buyer),
                name=f"prewarm-{cfg.task_id}",
            )
            for cfg, buyer in tasks
        ]

        try:
            prewarm_raw_results = await asyncio.gather(*prewarm_tasks, return_exceptions=True)
        except asyncio.CancelledError as cancel_exc:
            logger.warning("⚠️ Stage 1 cancelled; cascading cancellation to all workers...")
            for p_task in prewarm_tasks:
                if not p_task.done():
                    p_task.cancel()
            gathered = await asyncio.gather(*prewarm_tasks, return_exceptions=True)
            cleanup_coros = []
            for res in gathered:
                if isinstance(res, tuple):
                    cfg, worker = res
                    cleanup_coros.append(
                        self._cleanup_worker(
                            worker,
                            cfg,
                            disposition=ProxyHealthDisposition.HEALTHY,
                            suppress_raise=True,
                        )
                    )
                elif isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                    logger.error("Error during Stage 1 cancellation drain: %s", res)
            if cleanup_coros:
                drain_results = await asyncio.gather(*cleanup_coros, return_exceptions=True)
                for d_res in drain_results:
                    if isinstance(d_res, BaseException) and not isinstance(
                        d_res, asyncio.CancelledError
                    ):
                        self._record_cleanup_failure("STAGE1-DRAIN", "cleanup_worker", d_res)
            await self._retry_pending_cleanups(tasks)
            if len(self.cleanup_errors) > cleanup_baseline and hasattr(cancel_exc, "add_note"):
                cancel_exc.add_note(
                    f"{len(self.cleanup_errors) - cleanup_baseline} cleanup error(s) recorded"
                )
            raise

        ready_workers: list[tuple[int, TaskConfig, VansSite]] = []
        failed_prewarm_tasks: list[tuple[TaskConfig, BuyerProfile]] = []
        for idx, res in enumerate(prewarm_raw_results):
            if isinstance(res, asyncio.CancelledError):
                final_results[idx] = CheckoutResult(
                    success=False,
                    error_message=f"[{tasks[idx][0].task_id}] Prewarm cancelled",
                )
                failed_prewarm_tasks.append(tasks[idx])
                continue
            if isinstance(res, tuple):
                cfg, worker = res
                ready_workers.append((idx, cfg, worker))
            else:
                err_msg = (
                    str(res) if isinstance(res, BaseException) else "Prewarm initialization failed"
                )
                final_results[idx] = CheckoutResult(
                    success=False,
                    error_message=f"[{tasks[idx][0].task_id}] Prewarm failed: {err_msg}",
                )
                failed_prewarm_tasks.append(tasks[idx])

        if failed_prewarm_tasks:
            await self._retry_pending_cleanups(failed_prewarm_tasks)

        if not ready_workers:
            logger.critical("❌ All tasks failed during Stage 1 prewarm; aborting pipeline.")
            if len(self.cleanup_errors) > cleanup_baseline:
                recent = self.cleanup_errors[cleanup_baseline:]
                raise ResourceCleanupError(
                    "Stage 1 aborted with resource cleanup failures",
                    errors=[(r.step, r.error) for r in recent],
                )
            return [res for res in final_results if res is not None]

        logger.info("✅ Stage 1 complete! Ready workers: %d/%d", len(ready_workers), len(tasks))

        # =========================================================
        # [Stage 2] Coordinated Monitor & Burst with Shared Deadline
        # =========================================================
        stage2_deadline = time.monotonic() + max_wait_seconds
        logger.info(
            "=== [Stage 2] Launching monitor & awaiting workers (shared deadline: %.1fs) ===",
            max_wait_seconds,
        )

        worker_queues: list[tuple[int, TaskConfig, VansSite, asyncio.Queue[StockEventPayload]]] = []
        monitor: FastStockMonitor | None = None
        monitor_task: asyncio.Task[None] | None = None
        monitor_lease: ProxyLease | None = None
        monitor_disposition = ProxyHealthDisposition.HEALTHY
        monitor_status_code = 200
        monitor_cooldown: float | None = None
        monitor_abort_event = asyncio.Event()
        monitor_abort_reasons: list[str] = []
        burst_tasks: list[asyncio.Task[CheckoutResult]] = []
        burst_indices: list[int] = []
        active_exc: BaseException | None = None

        def _get_abort_reason() -> str | None:
            return monitor_abort_reasons[0] if monitor_abort_reasons else None

        try:
            for idx, cfg, worker in ready_workers:
                q = await global_event_bus.subscribe(sku)
                worker_queues.append((idx, cfg, worker, q))

            remaining_budget = stage2_deadline - time.monotonic()
            if remaining_budget <= 0:
                raise asyncio.TimeoutError("Stage 2 deadline expired before monitor startup")
            monitor_proxy_url, monitor_lease = await self._acquire_monitor_route(
                sku,
                remaining_budget,
                worker_proxy_urls=[
                    (
                        cfg.bound_proxy.formatted_url
                        if getattr(cfg, "bound_proxy", None) is not None
                        else None
                    )
                    for _, cfg, _ in ready_workers
                ],
            )

            remaining_budget = stage2_deadline - time.monotonic()
            if remaining_budget <= 0:
                raise asyncio.TimeoutError("Stage 2 deadline expired before monitor startup")
            monitor = FastStockMonitor(
                sku=sku,
                preferred_sizes=preferred_sizes,
                poll_interval_ms=monitor_poll_ms,
                proxy=monitor_proxy_url,
                event_bus=global_event_bus,
                max_monitor_duration=remaining_budget,
            )

            async def _run_monitor_guarded() -> None:
                nonlocal monitor_disposition, monitor_status_code, monitor_cooldown
                assert monitor is not None
                try:
                    await monitor.start()
                except asyncio.CancelledError:
                    raise
                except Exception as m_exc:
                    (
                        monitor_disposition,
                        monitor_status_code,
                        monitor_cooldown,
                    ) = _classify_exception_for_proxy(m_exc)
                    monitor_abort_reasons.append(f"Stock monitor failed: {m_exc}")
                    monitor_abort_event.set()
                    raise
                else:
                    exit_reason = getattr(monitor, "exit_reason", None)
                    monitor_error = getattr(monitor, "last_error", None)
                    if monitor_error is not None:
                        (
                            monitor_disposition,
                            monitor_status_code,
                            monitor_cooldown,
                        ) = _classify_exception_for_proxy(monitor_error)
                        if getattr(monitor, "last_status_code", None) == 407:
                            monitor_disposition = ProxyHealthDisposition.RESTRICTED
                            monitor_status_code = 407
                            monitor_cooldown = 15.0
                    if exit_reason in {"max_rate_limits", "max_errors"}:
                        reason = (
                            "consecutive rate limits"
                            if exit_reason == "max_rate_limits"
                            else "consecutive errors"
                        )
                        monitor_abort_reasons.append(
                            f"Stock monitor stopped after {reason}"
                        )
                        monitor_abort_event.set()
                    elif exit_reason == "duration_limit":
                        monitor_abort_reasons.append("Timed out waiting for stock broadcast")
                        monitor_abort_event.set()

            monitor_task = asyncio.create_task(
                _run_monitor_guarded(),
                name=f"monitor-{sku.strip().upper()}",
            )

            for idx, cfg, worker, queue in worker_queues:
                burst_tasks.append(
                    asyncio.create_task(
                        self.wait_and_burst(
                            worker,
                            cfg,
                            queue,
                            timeout=max_wait_seconds,
                            deadline=stage2_deadline,
                            monitor_abort_event=monitor_abort_event,
                            abort_reason_getter=_get_abort_reason,
                        ),
                        name=f"burst-{cfg.task_id}",
                    )
                )
                burst_indices.append(idx)

            raw_results = await asyncio.gather(*burst_tasks, return_exceptions=True)

            for idx, res in zip(burst_indices, raw_results):
                if isinstance(res, asyncio.CancelledError):
                    raise res
                if isinstance(res, ResourceCleanupError):
                    raise res
                if isinstance(res, Exception):
                    final_results[idx] = CheckoutResult(success=False, error_message=str(res))
                elif isinstance(res, BaseException):
                    raise res
                else:
                    final_results[idx] = res

            return [res for res in final_results if res is not None]

        except BaseException as exc:
            active_exc = exc
            raise

        finally:
            if monitor is not None:
                try:
                    monitor.stop()
                except Exception as stop_exc:  # noqa: BLE001 - shutdown failure is surfaced to cleanup records
                    self._record_cleanup_failure(f"MONITOR-{sku}", "monitor.stop", stop_exc)

            if monitor_task is not None and not monitor_task.done():
                monitor_task.cancel()
            if monitor_task is not None:
                monitor_results = await asyncio.gather(monitor_task, return_exceptions=True)
                for m_res in monitor_results:
                    if isinstance(m_res, ResourceCleanupError):
                        if not getattr(monitor, "cleanup_errors", None):
                            for step_name, err in m_res.errors:
                                self._record_cleanup_failure(
                                    f"MONITOR-{sku}",
                                    step_name,
                                    err,
                                )
                    elif isinstance(m_res, Exception) and not isinstance(
                        m_res, asyncio.CancelledError
                    ):
                        logger.error("Monitor task exited with exception: %s", m_res)
                if monitor is not None:
                    for cleanup_error in getattr(monitor, "cleanup_errors", ()):
                        self._record_cleanup_failure(
                            f"MONITOR-{sku}",
                            "monitor.session.close",
                            cleanup_error,
                        )

            if monitor_lease is not None:
                await self._settle_proxy_lease(
                    monitor_lease,
                    disposition=monitor_disposition,
                    status_code=monitor_status_code,
                    cooldown=monitor_cooldown,
                )

            for b_task in burst_tasks:
                if not b_task.done():
                    b_task.cancel()
            if burst_tasks:
                burst_drain = await asyncio.gather(*burst_tasks, return_exceptions=True)
                for b_res in burst_drain:
                    if isinstance(b_res, Exception) and not isinstance(
                        b_res, (asyncio.CancelledError, ResourceCleanupError)
                    ):
                        logger.error("Burst task shutdown exception: %s", b_res)

            unhandled_workers = [
                self._cleanup_worker(
                    worker,
                    cfg,
                    disposition=ProxyHealthDisposition.HEALTHY,
                    suppress_raise=True,
                )
                for idx, cfg, worker in ready_workers
                if idx not in burst_indices
            ]
            if unhandled_workers:
                unhandled_results = await asyncio.gather(
                    *unhandled_workers, return_exceptions=True
                )
                for u_worker_res in unhandled_results:
                    if isinstance(u_worker_res, BaseException) and not isinstance(
                        u_worker_res, asyncio.CancelledError
                    ):
                        self._record_cleanup_failure(
                            "STAGE2-UNHANDLED", "cleanup_worker", u_worker_res
                        )

            await self._retry_pending_cleanups(tasks)

            if worker_queues:
                unsubscribe_results = await asyncio.gather(
                    *(global_event_bus.unsubscribe(sku, q) for _, _, _, q in worker_queues),
                    return_exceptions=True,
                )
                for u_res in unsubscribe_results:
                    if isinstance(u_res, BaseException) and not isinstance(
                        u_res, asyncio.CancelledError
                    ):
                        self._record_cleanup_failure(
                            f"EVENTBUS-{sku}", "event_bus.unsubscribe", u_res
                        )

            new_cleanup_failures = self.cleanup_errors[cleanup_baseline:]
            if new_cleanup_failures:
                if active_exc is None:
                    summary = "; ".join(
                        f"{rec.task_id}:{rec.step}: {rec.error}" for rec in new_cleanup_failures
                    )
                    raise ResourceCleanupError(
                        f"Pipeline completed with resource cleanup failures: {summary}",
                        errors=[(rec.step, rec.error) for rec in new_cleanup_failures],
                    )
                if hasattr(active_exc, "add_note"):
                    active_exc.add_note(
                        f"{len(new_cleanup_failures)} cleanup failure(s) also occurred"
                    )


def load_task_input(path: Path) -> LiveTaskInput:
    """Load buyer/proxy inputs from JSON and validate them with Pydantic."""
    try:
        with path.open("r", encoding="utf-8") as input_file:
            raw_input = json.load(input_file)
    except OSError as exc:
        raise OSError(f"Unable to read live task input from {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in live task input file {path}: {exc}") from exc
    return LiveTaskInput.model_validate(raw_input)


def build_live_tasks(
    sku: str,
    preferred_sizes: list[str],
    task_input: LiveTaskInput,
) -> list[tuple[TaskConfig, BuyerProfile]]:
    sku = sku.strip()
    if not sku:
        raise ValueError("Target SKU must not be empty")
    if not task_input.buyers:
        raise ValueError("Live task input must contain at least one buyer")

    try:
        target_url = task_input.target_url_template.format(sku=sku)
    except (IndexError, KeyError, ValueError) as exc:
        raise ValueError(f"Invalid target URL template: {exc}") from exc

    tasks: list[tuple[TaskConfig, BuyerProfile]] = []
    for index, buyer in enumerate(task_input.buyers):
        default_proxy = buyer.default_proxy if not task_input.proxy_pool else None
        task_config = TaskConfig(
            task_id=f"LIVE-{index + 1:03d}",
            site_key=task_input.site_key,
            target_url=target_url,
            sku_or_variant_id=sku,
            target_size=preferred_sizes[0] if preferred_sizes else None,
            variant_priority=VariantPriorityModel(preferred_sizes=preferred_sizes),
            bound_proxy=default_proxy,
        )
        tasks.append((task_config, buyer))
    return tasks


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def _non_empty_string(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError("must not be empty")
    return normalized


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dispatch two-stage live Vans tasks with coordinated monitoring & workers"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/tasks.json"),
        help="JSON file containing buyer profiles and optional proxy distribution",
    )
    parser.add_argument("--sku", type=_non_empty_string, required=True, help="Target product SKU")
    parser.add_argument(
        "--sizes",
        nargs="+",
        default=["10.5", "10", "11"],
        help="Preferred size order, with optional ANY_SIZE fallback",
    )
    parser.add_argument(
        "--concurrency",
        type=_positive_integer,
        default=5,
        help="Maximum simultaneous pre-warm & live checkout tasks",
    )
    parser.add_argument(
        "--poll-interval-ms",
        type=_positive_integer,
        default=1200,
        help="MonitorEngine CDN bypass polling interval in milliseconds",
    )
    parser.add_argument(
        "--max-wait-seconds",
        type=_positive_float,
        default=900.0,
        help="Shared Stage 2 deadline in seconds for both FastStockMonitor and awaiting workers",
    )
    parser.add_argument(
        "--monitor-proxy",
        type=_non_empty_string,
        default=None,
        help="Optional dedicated proxy URL reserved specifically for FastStockMonitor",
    )
    parser.add_argument(
        "--allow-direct-monitor",
        action="store_true",
        help="Explicitly allow FastStockMonitor to run direct if ProxyPool has no spare proxy",
    )
    return parser.parse_args(argv)


async def async_main(argv: Sequence[str] | None = None) -> int:
    parser_args = _parse_args(argv)
    try:
        task_input = load_task_input(parser_args.config)
        tasks = build_live_tasks(parser_args.sku, parser_args.sizes, task_input)
    except ValidationError as exc:
        fields = sorted(
            {
                ".".join(str(part) for part in error["loc"])
                for error in exc.errors(include_input=False)
            }
        )
        raise SystemExit(
            "Live task configuration failed Pydantic validation at: " + ", ".join(fields)
        ) from None
    except (OSError, ValueError) as exc:
        raise SystemExit(f"Live task configuration error: {exc}") from exc

    proxy_pool: Any | None = None
    if task_input.proxy_pool and ProxyPool is not None:
        logger.info("Initializing global ProxyPool with %d proxies", len(task_input.proxy_pool))
        proxy_pool = ProxyPool(
            proxies=task_input.proxy_pool,
            default_cooldown=60.0,
            sweep_interval=1.0,
        )
        await proxy_pool.start()

    resolved_monitor_proxy = parser_args.monitor_proxy or task_input.monitor_proxy
    allow_direct = bool(parser_args.allow_direct_monitor or task_input.allow_direct_monitor)

    try:
        dispatcher = TwoStageLiveDispatcher(
            parser_args.concurrency,
            proxy_pool=proxy_pool,
            monitor_proxy=resolved_monitor_proxy,
            allow_direct_monitor=allow_direct,
        )
        results = await dispatcher.run_pipeline(
            sku=parser_args.sku,
            preferred_sizes=parser_args.sizes,
            tasks=tasks,
            monitor_poll_ms=parser_args.poll_interval_ms,
            max_wait_seconds=parser_args.max_wait_seconds,
        )
        succeeded = sum(result.success for result in results)
        logger.info(
            "Live dispatch summary: total=%d succeeded=%d failed=%d concurrency_limit=%d",
            len(results),
            succeeded,
            len(results) - succeeded,
            parser_args.concurrency,
        )
        return 0 if succeeded == len(results) and not dispatcher.cleanup_errors else 1
    finally:
        if proxy_pool:
            await proxy_pool.stop()


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())