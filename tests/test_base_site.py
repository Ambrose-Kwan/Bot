from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from core.exceptions import OutOfStockError, RateLimitError
from core.state_machine import CheckoutState
from models.profile import BuyerProfile, TaskConfig
from sites.base_site import BaseSite, CartResult, CheckoutResult, StockResult


class _TestSite(BaseSite):
    def __init__(
        self,
        task_config: TaskConfig,
        profile: BuyerProfile,
        session: Any,
    ) -> None:
        super().__init__(task_config, profile, session)
        self.steps: list[str] = []
        self.shipping_accepted = True
        self.cart_is_out_of_stock = False
        self.stock_checks = 0

    async def initialize_session(self) -> None:
        self.steps.append("init")

    async def check_stock(self) -> StockResult:
        self.steps.append("stock")
        self.stock_checks += 1
        if self.stock_checks > 1:
            self.stop()
            return StockResult(in_stock=False)
        return StockResult(in_stock=True, variant_id="SKU-1", price=10)

    async def add_to_cart(self) -> CartResult:
        self.steps.append("cart")
        if self.cart_is_out_of_stock:
            raise OutOfStockError("Out of stock", task_id=self.task_id)
        return CartResult(success=True, cart_id="cart-1")

    async def submit_shipping(self) -> bool:
        self.steps.append("shipping")
        return self.shipping_accepted

    async def submit_checkout(self) -> CheckoutResult:
        self.steps.append("checkout")
        return CheckoutResult(success=True, order_id="ORDER-1")


@pytest.fixture
def test_site(task_config: TaskConfig, buyer_profile: BuyerProfile, mock_session: Any) -> _TestSite:
    return _TestSite(task_config, buyer_profile, mock_session)


@pytest.mark.asyncio
async def test_base_site_runs_template_in_order_and_returns_result(test_site: _TestSite) -> None:
    result = await test_site.run()

    assert result == CheckoutResult(success=True, order_id="ORDER-1")
    assert test_site.steps == ["init", "stock", "cart", "shipping", "checkout"]
    assert test_site.fsm.current_state is CheckoutState.SUCCESS


@pytest.mark.asyncio
async def test_base_site_routes_rejected_shipping_to_failed(test_site: _TestSite) -> None:
    test_site.shipping_accepted = False

    result = await test_site.run()

    assert not result.success
    assert result.error_message == "[VANS-TEST] Shipping details were rejected"
    assert test_site.fsm.current_state is CheckoutState.FAILED


@pytest.mark.asyncio
async def test_base_site_returns_to_monitor_after_cart_stock_race(test_site: _TestSite) -> None:
    test_site.cart_is_out_of_stock = True

    result = await test_site.run()

    assert not result.success
    assert test_site.steps == ["init", "stock", "cart", "stock"]
    assert test_site.fsm.current_state is CheckoutState.MONITOR


@pytest.mark.asyncio
async def test_base_site_waits_for_retry_after_before_rate_limit_retry(
    test_site: _TestSite,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("sites.base_site.asyncio.sleep", record_sleep)
    test_site.check_stock = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            RateLimitError(retry_after=3.5, task_id=test_site.task_id),
            StockResult(in_stock=True, variant_id="SKU-1", price=10),
        ]
    )

    result = await test_site.run()

    assert result.success
    assert delays == [3.5]
    assert test_site.fsm.current_state is CheckoutState.SUCCESS


@pytest.mark.asyncio
async def test_base_site_uses_bounded_backoff_for_invalid_retry_after(
    test_site: _TestSite,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("sites.base_site.asyncio.sleep", record_sleep)
    test_site.check_stock = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            RateLimitError(retry_after=float("inf"), task_id=test_site.task_id),
            StockResult(in_stock=True, variant_id="SKU-1", price=10),
        ]
    )

    result = await test_site.run()

    assert result.success
    assert delays == [test_site.RATE_LIMIT_BACKOFF_BASE_SECONDS]
