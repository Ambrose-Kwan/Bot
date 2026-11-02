from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import main_live
from core.events import StockEventPayload, global_event_bus
from core.exceptions import (
    PaymentDeclinedError,
    RateLimitError,
    ResourceCleanupError,
    WAFChallengeError,
)
from core.proxy import ProxyLeaseOwnershipError, ProxyPool, ProxyPoolEmptyError
from main_live import (
    LiveTaskInput,
    ProxyHealthDisposition,
    TwoStageLiveDispatcher,
    build_live_tasks,
)
from models.profile import BuyerProfile, PaymentMethod, ProxyConfig, ShippingAddress, TaskConfig
from sites.base_site import CheckoutResult
from sites.vans_site import VansSite


def _buyer(profile_id: str) -> BuyerProfile:
    return BuyerProfile(
        profile_id=profile_id,
        profile_name=profile_id,
        shipping=ShippingAddress(
            first_name="Test",
            last_name="Buyer",
            email=f"{profile_id}@example.com",
            phone="4155550100",
            address1="10 Main Street",
            city="San Francisco",
            state_code="CA",
            postal_code="94103",
        ),
        payment=PaymentMethod(
            cardholder_name="Test Buyer",
            card_number="4111111111111111",
            exp_month=12,
            exp_year=2030,
            cvv="123",
        ),
    )


def test_build_live_tasks_uses_current_task_and_buyer_models() -> None:
    task_input = LiveTaskInput(
        buyers=[_buyer("buyer-1"), _buyer("buyer-2"), _buyer("buyer-3")],
    )

    tasks = build_live_tasks("VN000EAABOP", ["10.5", "10", "11"], task_input)

    assert [task.task_id for task, _ in tasks] == ["LIVE-001", "LIVE-002", "LIVE-003"]
    assert all(task.target_url.endswith("/VN000EAABOP") for task, _ in tasks)
    assert all(task.variant_priority.preferred_sizes == ["10.5", "10", "11"] for task, _ in tasks)
    assert [buyer.profile_id for _, buyer in tasks] == ["buyer-1", "buyer-2", "buyer-3"]


@pytest.mark.asyncio
async def test_dispatcher_prewarm_respects_concurrency_limit(
    task_config: TaskConfig,
) -> None:
    active = 0
    maximum_active = 0

    class _FakeWorker:
        async def prewarm(self) -> None:
            nonlocal active, maximum_active
            active += 1
            maximum_active = max(maximum_active, active)
            await asyncio.sleep(0.01)
            active -= 1

    dispatcher = TwoStageLiveDispatcher(concurrency_limit=2)

    results = await asyncio.gather(
        *(dispatcher.prewarm_worker(_FakeWorker(), task_config) for _ in range(4))
    )

    assert results == [True] * 4
    assert maximum_active == 2


@pytest.mark.asyncio
async def test_checkout_timeout_is_not_reported_as_stock_wait_timeout() -> None:
    """Verify TimeoutError inside execute_fast_checkout is not misreported as stock wait timeout."""

    class _TimeoutWorker:
        def apply_stock_signal(self, payload: StockEventPayload) -> None:
            del payload

        async def execute_fast_checkout(self) -> CheckoutResult:
            raise TimeoutError("Checkout gateway connection timed out")

    queue: asyncio.Queue[StockEventPayload] = asyncio.Queue()
    await queue.put(
        StockEventPayload(
            sku="VN000TEST",
            variant_id="V-1",
            size_label="10.5",
        )
    )

    cfg = TaskConfig(
        task_id="TASK-TO-01",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )

    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1)
    result = await dispatcher.wait_and_burst(_TimeoutWorker(), cfg, queue, timeout=10.0)

    assert result.success is False
    assert result.error_message is not None
    assert "Timed out waiting for stock broadcast" not in result.error_message
    assert "Burst error" in result.error_message


@pytest.mark.asyncio
async def test_wait_and_burst_respects_concurrency_limit() -> None:
    """Verify Stage 2 burst checkouts strictly obey the dispatcher semaphore."""
    active_checkout = 0
    max_active_checkout = 0

    class _BurstWorker:
        def apply_stock_signal(self, payload: StockEventPayload) -> None:
            del payload

        async def execute_fast_checkout(self) -> CheckoutResult:
            nonlocal active_checkout, max_active_checkout
            active_checkout += 1
            max_active_checkout = max(max_active_checkout, active_checkout)
            await asyncio.sleep(0.02)
            active_checkout -= 1
            return CheckoutResult(success=True)

    dispatcher = TwoStageLiveDispatcher(concurrency_limit=2)

    queues: list[asyncio.Queue[StockEventPayload]] = [asyncio.Queue() for _ in range(4)]
    for q in queues:
        await q.put(StockEventPayload(sku="VN000TEST", variant_id="V-1", size_label="10.5"))

    tasks = [
        dispatcher.wait_and_burst(
            _BurstWorker(),
            TaskConfig(
                task_id=f"T-{i}",
                site_key="vans",
                target_url="https://example.com",
                sku_or_variant_id="VN000TEST",
            ),
            queues[i],
        )
        for i in range(4)
    ]

    results = await asyncio.gather(*tasks)
    assert all(r.success for r in results)
    assert max_active_checkout == 2


@pytest.mark.asyncio
async def test_prewarm_task_allocates_proxy_before_worker_session_init() -> None:
    allocated_proxy = "http://user:pass@192.168.1.100:8080"
    mock_pool = MagicMock(spec=ProxyPool)
    mock_pool.get_proxy = AsyncMock(return_value=allocated_proxy)
    mock_pool.report_success = AsyncMock()
    mock_pool.release_task = AsyncMock()

    cfg = TaskConfig(
        task_id="TASK-ROUTE-01",
        site_key="vans",
        target_url="https://www.vans.com/en-us/p/shoes/VN000TEST",
        sku_or_variant_id="VN000TEST",
        bound_proxy=None,
    )
    buyer = _buyer("buyer-route")
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=2, proxy_pool=mock_pool)

    with patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock) as mock_prewarm:
        task_cfg, worker = await dispatcher.prewarm_task(cfg, buyer)

        assert task_cfg.bound_proxy is not None
        assert task_cfg.bound_proxy.formatted_url == allocated_proxy
        assert worker.config.bound_proxy is not None
        assert worker.config.bound_proxy.formatted_url == allocated_proxy

        mock_pool.get_proxy.assert_awaited_once_with(timeout=15.0, task_id="TASK-ROUTE-01")
        mock_prewarm.assert_awaited_once()

        await dispatcher._cleanup_worker(worker, task_cfg)


@pytest.mark.asyncio
async def test_prewarm_task_failure_closes_session_and_releases_proxy() -> None:
    allocated_proxy = "http://user:pass@192.168.1.200:8080"
    mock_pool = MagicMock(spec=ProxyPool)
    mock_pool.get_proxy = AsyncMock(return_value=allocated_proxy)
    mock_pool.report_restricted = AsyncMock()
    mock_pool.release_task = AsyncMock()

    cfg = TaskConfig(
        task_id="TASK-FAIL-01",
        site_key="vans",
        target_url="https://www.vans.com/en-us/p/shoes/VN000TEST",
        sku_or_variant_id="VN000TEST",
        bound_proxy=None,
    )
    buyer = _buyer("buyer-fail")
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1, proxy_pool=mock_pool)

    with patch(
        "sites.vans_site.VansSite.prewarm",
        side_effect=ConnectionResetError("Vans 403 blocked"),
    ):
        with pytest.raises(ConnectionResetError, match="Vans 403 blocked"):
            await dispatcher.prewarm_task(cfg, buyer)

        mock_pool.report_restricted.assert_awaited_once_with(
            allocated_proxy,
            status_code=503,
            cooldown=15.0,
            task_id="TASK-FAIL-01",
        )
        mock_pool.release_task.assert_awaited_once_with("TASK-FAIL-01")


# =========================================================================
# Issue 1 Tests: Strict Proxy Lease Ownership & Cross-Task Protection
# =========================================================================
@pytest.mark.asyncio
async def test_external_bound_proxy_is_never_reported_or_released_to_global_pool() -> None:
    """Issue 1: Pre-bound external proxies must never mutate global ProxyPool state."""
    preset_proxy = "http://preset:pass@10.0.0.1:8080"
    real_pool = ProxyPool(proxies=["http://pool:pass@10.0.0.99:8080"])
    await real_pool.start()

    cfg = TaskConfig(
        task_id="TASK-PRESET-01",
        site_key="vans",
        target_url="https://www.vans.com/en-us/p/shoes/VN000TEST",
        sku_or_variant_id="VN000TEST",
        bound_proxy=ProxyConfig.from_string(preset_proxy),
    )
    buyer = _buyer("buyer-preset")
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1, proxy_pool=real_pool)

    try:
        with patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock):
            task_cfg, worker = await dispatcher.prewarm_task(cfg, buyer)
            await dispatcher._cleanup_worker(worker, task_cfg)

        # Ensure preset_proxy was NOT injected into real_pool's available queue
        leased = await real_pool.get_proxy(timeout=1.0, task_id="CHECK-POOL")
        assert leased == "http://pool:pass@10.0.0.99:8080"
        with pytest.raises(ProxyPoolEmptyError):
            await real_pool.get_proxy(timeout=0.05, task_id="CHECK-EMPTY")
        await real_pool.release_task("CHECK-POOL")
    finally:
        await real_pool.stop()


@pytest.mark.asyncio
async def test_proxy_pool_prevents_cross_task_lease_corruption_and_double_settle() -> None:
    """Issue 1: A task cannot release or restrict a proxy currently leased by another task."""
    proxy_url = "http://user:pass@192.168.10.10:8080"
    pool = ProxyPool(proxies=[proxy_url])
    await pool.start()
    try:
        leased = await pool.get_proxy(timeout=1.0, task_id="TASK-OWNER")
        assert leased == proxy_url

        with pytest.raises(ProxyLeaseOwnershipError):
            await pool.report_success(proxy_url, task_id="TASK-INTRUDER")

        with pytest.raises(ProxyLeaseOwnershipError):
            await pool.report_restricted(proxy_url, status_code=429, task_id="TASK-INTRUDER")

        # Proxy must still be exclusively leased by TASK-OWNER
        assert proxy_url in pool._leased
        await pool.release_task("TASK-INTRUDER")
        assert proxy_url in pool._leased

        await pool.release_task("TASK-OWNER")
        assert proxy_url not in pool._leased
    finally:
        await pool.stop()


# =========================================================================
# Issue 2 Tests: Decoupled Task Outcome vs. Proxy Health Classification
# =========================================================================
@pytest.mark.asyncio
async def test_stock_wait_timeout_and_payment_decline_do_not_cool_down_proxy() -> None:
    """Issue 2: Stock broadcast timeouts and payment declines must report proxy as healthy."""
    proxy_url = "http://user:pass@192.168.1.50:8080"
    mock_pool = MagicMock(spec=ProxyPool)
    mock_pool.get_proxy = AsyncMock(return_value=proxy_url)
    mock_pool.report_success = AsyncMock()
    mock_pool.report_restricted = AsyncMock()
    mock_pool.release_task = AsyncMock()

    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1, proxy_pool=mock_pool)

    # Case A: Stock wait timeout
    cfg_timeout = TaskConfig(
        task_id="TASK-STOCK-TO",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    with patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock):
        _, worker_to = await dispatcher.prewarm_task(cfg_timeout, _buyer("b-to"))

    empty_queue: asyncio.Queue[StockEventPayload] = asyncio.Queue()
    res_to = await dispatcher.wait_and_burst(worker_to, cfg_timeout, empty_queue, timeout=0.02)
    assert res_to.success is False
    assert res_to.error_message == "Timed out waiting for stock broadcast"
    mock_pool.report_restricted.assert_not_awaited()
    mock_pool.report_success.assert_awaited_once_with(
        proxy_url,
        task_id="TASK-STOCK-TO",
    )

    # Case B: Payment declined error
    mock_pool.report_success.reset_mock()
    cfg_pay = TaskConfig(
        task_id="TASK-PAY-DECLINE",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    with patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock):
        _, worker_pay = await dispatcher.prewarm_task(cfg_pay, _buyer("b-pay"))

    worker_pay.apply_stock_signal = MagicMock()  # type: ignore[method-assign]
    worker_pay.execute_fast_checkout = AsyncMock(  # type: ignore[method-assign]
        side_effect=PaymentDeclinedError(reason="Card declined", task_id="TASK-PAY-DECLINE")
    )
    signal_queue: asyncio.Queue[StockEventPayload] = asyncio.Queue()
    await signal_queue.put(
        StockEventPayload(sku="VN000TEST", variant_id="V-1", size_label="10.5")
    )
    res_pay = await dispatcher.wait_and_burst(worker_pay, cfg_pay, signal_queue, timeout=1.0)
    assert res_pay.success is False
    mock_pool.report_restricted.assert_not_awaited()
    mock_pool.report_success.assert_awaited_once_with(
        proxy_url,
        task_id="TASK-PAY-DECLINE",
    )


@pytest.mark.asyncio
async def test_structured_rate_limit_and_waf_errors_cool_down_proxy() -> None:
    """Issue 2: Structured RateLimitError (429) and WAFChallengeError (403) cool down proxy."""
    proxy_url = "http://user:pass@192.168.1.51:8080"
    mock_pool = MagicMock(spec=ProxyPool)
    mock_pool.get_proxy = AsyncMock(return_value=proxy_url)
    mock_pool.report_success = AsyncMock()
    mock_pool.report_restricted = AsyncMock()
    mock_pool.release_task = AsyncMock()

    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1, proxy_pool=mock_pool)
    cfg = TaskConfig(
        task_id="TASK-RL-01",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    with patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock):
        _, worker = await dispatcher.prewarm_task(cfg, _buyer("b-rl"))

    worker.apply_stock_signal = MagicMock()  # type: ignore[method-assign]
    worker.execute_fast_checkout = AsyncMock(  # type: ignore[method-assign]
        side_effect=RateLimitError(retry_after=45.0, task_id="TASK-RL-01")
    )
    queue: asyncio.Queue[StockEventPayload] = asyncio.Queue()
    await queue.put(StockEventPayload(sku="VN000TEST", variant_id="V-1", size_label="10.5"))

    res = await dispatcher.wait_and_burst(worker, cfg, queue, timeout=1.0)
    assert res.success is False
    mock_pool.report_success.assert_not_awaited()
    mock_pool.report_restricted.assert_awaited_once_with(
        proxy_url,
        status_code=429,
        cooldown=45.0,
        task_id="TASK-RL-01",
    )

    disp_waf, code_waf, cd_waf = main_live._classify_exception_for_proxy(
        WAFChallengeError(status_code=403, task_id="TASK-RL-01")
    )
    assert (disp_waf, code_waf, cd_waf) == (ProxyHealthDisposition.RESTRICTED, 403, 60.0)


# =========================================================================
# Issue 3 Tests: Non-Swallowing Zero-Leak Cleanup & CancelledError Priority
# =========================================================================
@pytest.mark.asyncio
async def test_cleanup_worker_runs_all_steps_logs_error_and_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue 3: If session.close fails, proxy lease release still executes and raises."""
    proxy_url = "http://user:pass@192.168.1.77:8080"
    mock_pool = MagicMock(spec=ProxyPool)
    mock_pool.get_proxy = AsyncMock(return_value=proxy_url)
    mock_pool.report_success = AsyncMock()
    mock_pool.release_task = AsyncMock()

    cfg = TaskConfig(
        task_id="TASK-CLEAN-ERR",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1, proxy_pool=mock_pool)

    with patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock):
        _, worker = await dispatcher.prewarm_task(cfg, _buyer("b-clean"))

    worker.session.close = AsyncMock(side_effect=RuntimeError("socket close failed"))  # type: ignore[method-assign]

    with (
        caplog.at_level(logging.ERROR, logger="nexus.live_dispatcher"),
        pytest.raises(ResourceCleanupError, match="socket close failed"),
    ):
        await dispatcher._cleanup_worker(worker, cfg)

    # Sibling proxy cleanup steps MUST still have executed
    mock_pool.report_success.assert_awaited_once_with(
        proxy_url,
        task_id="TASK-CLEAN-ERR",
    )
    mock_pool.release_task.assert_awaited_once_with("TASK-CLEAN-ERR")
    assert len(dispatcher.cleanup_errors) == 1
    assert dispatcher.cleanup_errors[0].step == "session.close"
    assert any("socket close failed" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_cancellation_preserves_cancelled_error_while_recording_cleanup_failures() -> None:
    """Issue 3: During cancellation, CancelledError propagates while cleanup errors are recorded."""
    proxy_url = "http://user:pass@192.168.1.78:8080"
    mock_pool = MagicMock(spec=ProxyPool)
    mock_pool.get_proxy = AsyncMock(return_value=proxy_url)
    mock_pool.report_success = AsyncMock()
    mock_pool.release_task = AsyncMock(side_effect=RuntimeError("lease release failed"))

    cfg = TaskConfig(
        task_id="TASK-CANC-CLEAN",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1, proxy_pool=mock_pool)

    with patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock):
        _, worker = await dispatcher.prewarm_task(cfg, _buyer("b-canc"))

    worker.apply_stock_signal = MagicMock()  # type: ignore[method-assign]
    worker.execute_fast_checkout = AsyncMock(side_effect=asyncio.CancelledError)  # type: ignore[method-assign]

    queue: asyncio.Queue[StockEventPayload] = asyncio.Queue()
    await queue.put(StockEventPayload(sku="VN000TEST", variant_id="V-1", size_label="10.5"))

    with pytest.raises(asyncio.CancelledError):
        await dispatcher.wait_and_burst(worker, cfg, queue, timeout=1.0)

    assert len(dispatcher.cleanup_errors) == 1
    assert dispatcher.cleanup_errors[0].step == "proxy_pool.release_task"


# =========================================================================
# Issue 4 Tests: Consistent Monitor & Worker Proxy Routing
# =========================================================================
@pytest.mark.asyncio
async def test_monitor_uses_dedicated_proxy_lease_and_releases_it() -> None:
    """Issue 4: FastStockMonitor leases its own proxy from ProxyPool (separate from workers)."""
    real_pool = ProxyPool(
        proxies=[
            "http://user:pass@10.10.0.1:8080",
            "http://user:pass@10.10.0.2:8080",
        ]
    )
    await real_pool.start()

    cfg = TaskConfig(
        task_id="TASK-MON-01",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=2, proxy_pool=real_pool)
    captured_monitor_kwargs: dict[str, object] = {}

    class _CapturingMonitor:
        def __init__(self, **kwargs: object) -> None:
            captured_monitor_kwargs.update(kwargs)
            self.exit_reason = "stock_found"

        async def start(self) -> None:
            await global_event_bus.publish(
                StockEventPayload(sku="VN000TEST", variant_id="V-1", size_label="10.5")
            )

        def stop(self) -> None:
            return None

    try:
        with (
            patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock),
            patch(
                "sites.vans_site.VansSite.execute_fast_checkout",
                new_callable=AsyncMock,
                return_value=CheckoutResult(success=True, order_id="ORD-1"),
            ),
            patch("main_live.FastStockMonitor", side_effect=lambda **kw: _CapturingMonitor(**kw)),
        ):
            results = await dispatcher.run_pipeline(
                "VN000TEST", ["10.5"], [(cfg, _buyer("b-mon"))], max_wait_seconds=10.0
            )

        assert len(results) == 1 and results[0].success is True
        worker_proxy = cfg.bound_proxy.formatted_url if cfg.bound_proxy else None
        monitor_proxy = captured_monitor_kwargs.get("proxy")
        assert monitor_proxy is not None
        assert monitor_proxy != worker_proxy
        assert len(real_pool._leased) == 0
    finally:
        await real_pool.stop()


@pytest.mark.asyncio
async def test_monitor_strict_routing_fails_closed_when_pool_exhausted() -> None:
    """Issue 4: When ProxyPool has no spare proxy for monitor and allow_direct_monitor=False, fail closed."""
    real_pool = ProxyPool(proxies=["http://user:pass@10.10.0.1:8080"])
    await real_pool.start()

    cfg = TaskConfig(
        task_id="TASK-MON-STRICT",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    dispatcher = TwoStageLiveDispatcher(
        concurrency_limit=1,
        proxy_pool=real_pool,
        allow_direct_monitor=False,
    )

    try:
        with (
            patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock),
            pytest.raises(ProxyPoolEmptyError, match="FastStockMonitor"),
        ):
            await dispatcher.run_pipeline(
                "VN000TEST", ["10.5"], [(cfg, _buyer("b-strict"))], max_wait_seconds=0.2
            )

        # Ready worker's lease MUST have been cleaned up and returned to pool
        assert len(real_pool._leased) == 0
    finally:
        await real_pool.stop()


# =========================================================================
# Issue 5 Tests: Shared Stage 2 Deadline & Early Monitor Abort Coordination
# =========================================================================
@pytest.mark.asyncio
async def test_pipeline_synchronizes_deadline_across_monitor_and_workers() -> None:
    """Issue 5: Monitor max_monitor_duration and worker wait budget share max_wait_seconds."""
    cfg = TaskConfig(
        task_id="TASK-SYNC-DL",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1)
    observed_monitor_durations: list[float] = []

    class _IdleMonitor:
        def __init__(self, **kwargs: object) -> None:
            observed_monitor_durations.append(float(kwargs["max_monitor_duration"]))  # type: ignore[arg-type]
            self.exit_reason = "duration_limit"

        async def start(self) -> None:
            await asyncio.sleep(0.08)

        def stop(self) -> None:
            return None

    with (
        patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock),
        patch("main_live.FastStockMonitor", side_effect=lambda **kw: _IdleMonitor(**kw)),
    ):
        t0 = asyncio.get_running_loop().time()
        results = await dispatcher.run_pipeline(
            "VN000TEST",
            ["10.5"],
            [(cfg, _buyer("b-dl"))],
            max_wait_seconds=0.08,
        )
        elapsed = asyncio.get_running_loop().time() - t0

    assert len(observed_monitor_durations) == 1
    assert observed_monitor_durations[0] <= 0.08
    assert elapsed < 1.0
    assert results[0].success is False
    assert results[0].error_message == "Timed out waiting for stock broadcast"


@pytest.mark.asyncio
async def test_workers_abort_immediately_if_monitor_stops_on_consecutive_errors() -> None:
    """Issue 5: If FastStockMonitor aborts on max_errors, workers wake immediately instead of hanging."""
    cfg = TaskConfig(
        task_id="TASK-MON-ABORT",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1)

    class _AbortingMonitor:
        def __init__(self, **_kwargs: object) -> None:
            self.exit_reason = "max_errors"

        async def start(self) -> None:
            await asyncio.sleep(0.01)

        def stop(self) -> None:
            return None

    with (
        patch("sites.vans_site.VansSite.prewarm", new_callable=AsyncMock),
        patch("main_live.FastStockMonitor", side_effect=lambda **kw: _AbortingMonitor(**kw)),
    ):
        t0 = asyncio.get_running_loop().time()
        results = await dispatcher.run_pipeline(
            "VN000TEST",
            ["10.5"],
            [(cfg, _buyer("b-abort"))],
            max_wait_seconds=60.0,
        )
        elapsed = asyncio.get_running_loop().time() - t0

    assert elapsed < 1.0
    assert results[0].success is False
    assert results[0].error_message is not None
    assert "consecutive errors" in results[0].error_message


# =========================================================================
# Earlier Regression Tests (Routing, Stage 1 Cancellation, Result Alignment)
# =========================================================================
@pytest.mark.asyncio
async def test_base_site_authoritative_bound_proxy_not_overwritten() -> None:
    preallocated_proxy = "http://user:pass@1.2.3.4:8080"
    pool_proxies = ["http://pool_user:pass@9.9.9.9:8080"]

    cfg = TaskConfig(
        task_id="TASK-AUTHORITY-01",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
        bound_proxy=ProxyConfig.from_string(preallocated_proxy),
    )
    cfg.proxy_pool = pool_proxies
    worker = VansSite(cfg, _buyer("buyer-authority"))

    try:
        captured_kwargs: dict[str, object] = {}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"status": "ok"}'
        mock_resp.json = MagicMock(return_value={"status": "ok"})

        underlying_session = await worker.session._get_session()

        async def mock_transport_request(*_args: object, **kwargs: object) -> MagicMock:
            captured_kwargs.update(kwargs)
            return mock_resp

        with patch.object(underlying_session, "request", side_effect=mock_transport_request):
            await worker.session.get("https://example.com/api/ping")

        assert captured_kwargs.get("proxy") == preallocated_proxy
        assert "9.9.9.9" not in str(captured_kwargs)
    finally:
        await worker.session.close()


@pytest.mark.asyncio
async def test_run_pipeline_stage1_cancellation_cleans_up_finished_workers_and_leases() -> None:
    """Real lease lifecycle during Stage 1 cancellation: acquire -> finish prewarm -> cancel -> release."""
    real_pool = ProxyPool(
        proxies=[
            "http://user:pass@192.168.50.1:8080",
            "http://user:pass@192.168.50.2:8080",
        ]
    )
    await real_pool.start()

    cfg1 = TaskConfig(
        task_id="TASK-CANC-01",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    cfg2 = TaskConfig(
        task_id="TASK-CANC-02",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=2, proxy_pool=real_pool)
    first_prewarmed = asyncio.Event()

    async def slow_second_prewarm(self_worker: VansSite) -> None:
        if self_worker.task_id == "TASK-CANC-01":
            first_prewarmed.set()
            return
        await asyncio.sleep(10.0)

    try:
        with patch.object(VansSite, "prewarm", slow_second_prewarm):
            pipeline_task = asyncio.create_task(
                dispatcher.run_pipeline(
                    "VN000TEST", ["10.5"], [(cfg1, _buyer("b1")), (cfg2, _buyer("b2"))]
                )
            )
            await asyncio.wait_for(first_prewarmed.wait(), timeout=2.0)
            await asyncio.sleep(0.02)
            pipeline_task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await pipeline_task

        assert len(real_pool._leased) == 0
        assert not dispatcher.cleanup_errors
    finally:
        await real_pool.stop()


@pytest.mark.asyncio
async def test_run_pipeline_aligns_results_when_some_prewarms_fail() -> None:
    cfg_success = TaskConfig(
        task_id="TASK-OK",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    cfg_fail = TaskConfig(
        task_id="TASK-BAD",
        site_key="vans",
        target_url="https://example.com",
        sku_or_variant_id="VN000TEST",
    )
    tasks = [(cfg_success, _buyer("b1")), (cfg_fail, _buyer("b2"))]

    dispatcher = TwoStageLiveDispatcher(concurrency_limit=2)

    fake_worker = MagicMock()
    fake_worker.session = MagicMock()
    fake_worker.session.close = AsyncMock()
    fake_worker.apply_stock_signal = MagicMock()
    fake_worker.execute_fast_checkout = AsyncMock(return_value=CheckoutResult(success=True))

    async def mock_prewarm_task(cfg: TaskConfig, _buyer_prof: BuyerProfile) -> tuple[TaskConfig, Any]:
        if cfg.task_id == "TASK-OK":
            return (cfg, fake_worker)
        raise ConnectionResetError("Vans WAF 403 blocked")

    with (
        patch.object(dispatcher, "prewarm_task", side_effect=mock_prewarm_task),
        patch("main_live.FastStockMonitor") as mock_monitor_cls,
    ):
        monitor_instance = mock_monitor_cls.return_value
        monitor_instance.exit_reason = "stock_found"

        async def mock_monitor_start() -> None:
            await global_event_bus.publish(
                StockEventPayload(
                    sku="VN000TEST",
                    variant_id="V-1",
                    size_label="10.5",
                )
            )

        monitor_instance.start = mock_monitor_start
        monitor_instance.stop = MagicMock()

        results = await dispatcher.run_pipeline("VN000TEST", ["10.5"], tasks)

        assert len(results) == 2
        assert results[0].success is True
        assert results[1].success is False
        assert results[1].error_message is not None
        assert "Prewarm failed" in results[1].error_message
        assert "Vans WAF 403 blocked" in results[1].error_message


@pytest.mark.asyncio
async def test_pipeline_cleans_ready_workers_if_subscription_fails(
    task_config: TaskConfig,
    buyer_profile: BuyerProfile,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workers: list[object] = []

    class _Worker:
        def __init__(self, *_args: object) -> None:
            self.session = MagicMock()
            self.session.close = AsyncMock()
            workers.append(self)

        async def prewarm(self) -> None:
            return None

    class _FailingBus:
        async def subscribe(self, _sku: str) -> asyncio.Queue[StockEventPayload]:
            raise RuntimeError("subscription failed")

    monkeypatch.setattr(main_live, "VansSite", _Worker)
    monkeypatch.setattr(main_live, "global_event_bus", _FailingBus())
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1)

    with pytest.raises(RuntimeError, match="subscription failed"):
        await dispatcher.run_pipeline(
            task_config.sku_or_variant_id,
            ["10.5"],
            [(task_config, buyer_profile)],
        )

    assert len(workers) == 1
    workers[0].session.close.assert_awaited_once()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_pipeline_propagates_burst_cancellation_and_cleans_up(
    task_config: TaskConfig,
    buyer_profile: BuyerProfile,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workers: list[object] = []
    queues: list[asyncio.Queue[StockEventPayload]] = []

    class _Worker:
        def __init__(self, *_args: object) -> None:
            self.session = MagicMock()
            self.session.close = AsyncMock()
            workers.append(self)

        async def prewarm(self) -> None:
            return None

        def apply_stock_signal(self, _payload: StockEventPayload) -> None:
            return None

        async def execute_fast_checkout(self) -> CheckoutResult:
            raise asyncio.CancelledError

    class _Bus:
        async def subscribe(self, _sku: str) -> asyncio.Queue[StockEventPayload]:
            queue: asyncio.Queue[StockEventPayload] = asyncio.Queue()
            queue.put_nowait(
                StockEventPayload(
                    sku=task_config.sku_or_variant_id,
                    variant_id="V-1",
                    size_label="10.5",
                )
            )
            queues.append(queue)
            return queue

        async def unsubscribe(
            self, _sku: str, _queue: asyncio.Queue[StockEventPayload]
        ) -> None:
            return None

    class _Monitor:
        def __init__(self, **_kwargs: object) -> None:
            self.stopped = False
            self.exit_reason = None

        async def start(self) -> None:
            await asyncio.Event().wait()

        def stop(self) -> None:
            self.stopped = True

    monitor_instances: list[_Monitor] = []

    def monitor_factory(**kwargs: object) -> _Monitor:
        monitor = _Monitor(**kwargs)
        monitor_instances.append(monitor)
        return monitor

    monkeypatch.setattr(main_live, "VansSite", _Worker)
    monkeypatch.setattr(main_live, "global_event_bus", _Bus())
    monkeypatch.setattr(main_live, "FastStockMonitor", monitor_factory)
    dispatcher = TwoStageLiveDispatcher(concurrency_limit=1)

    with pytest.raises(asyncio.CancelledError):
        await dispatcher.run_pipeline(
            task_config.sku_or_variant_id,
            ["10.5"],
            [(task_config, buyer_profile)],
        )

    assert len(workers) == 1
    workers[0].session.close.assert_awaited_once()  # type: ignore[attr-defined]
    assert len(queues) == 1
    assert monitor_instances[0].stopped