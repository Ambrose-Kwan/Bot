"""Typed adapter contracts and the shared checkout lifecycle."""
from __future__ import annotations

import asyncio
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

try:
    from ..core.exceptions import (
        CheckoutException,
        OutOfStockError,
        PaymentDeclinedError,
        ProductUnavailableError,
        RateLimitError,
        SessionExpiredError,
    )
    from ..core.network import RobustAsyncSession
    from ..core.state_machine import AsyncStateMachine, CheckoutState
    from ..models.profile import BuyerProfile, TaskConfig
except ImportError:
    from core.exceptions import (
        CheckoutException,
        OutOfStockError,
        PaymentDeclinedError,
        ProductUnavailableError,
        RateLimitError,
        SessionExpiredError,
    )
    from core.network import RobustAsyncSession
    from core.state_machine import AsyncStateMachine, CheckoutState
    from models.profile import BuyerProfile, TaskConfig


class StockResult(BaseModel):
    """Normalized stock status returned by a site adapter."""

    model_config = ConfigDict(extra="forbid")

    in_stock: bool
    variant_id: str | None = None
    price: float | None = None


class StockStatus(StockResult):
    """Backward-compatible stock result with optional display metadata."""

    title: str = ""
    available_sizes: list[str] = Field(default_factory=list)
    raw_response: dict[str, Any] = Field(default_factory=dict)


class CartResult(BaseModel):
    """Normalized result of adding the requested variant to a cart."""

    model_config = ConfigDict(extra="forbid")

    success: bool
    cart_id: str | None = None
    error_message: str | None = None

    @property
    def cart_token(self) -> str | None:
        """Legacy alias for adapters that call a cart ID a token."""
        return self.cart_id


class CheckoutResult(BaseModel):
    """Normalized checkout result returned by adapters and the run template."""

    model_config = ConfigDict(extra="forbid")

    success: bool
    order_id: str | None = None
    error_message: str | None = None

    @property
    def is_success(self) -> bool:
        """Backward-compatible alias for the former result field."""
        return self.success


@dataclass
class ShippingRate:
    """Available shipping tier option retained for existing integrations."""

    rate_id: str
    title: str
    price: float


@dataclass
class ShippingResult:
    """Legacy detailed shipping response; new adapters return a boolean."""

    shipping_rates: list[ShippingRate]
    selected_rate: ShippingRate
    total_price: float
    checkout_step_token: str | None = None


@dataclass
class TaskSummary:
    """Legacy task summary retained for callers that construct one directly."""

    task_id: str
    site_key: str
    is_success: bool
    final_state: CheckoutState
    total_duration_sec: float
    order_id: str | None = None
    step_timings_ms: dict[str, float] = field(default_factory=dict)
    error_reason: str | None = None


class BaseSite(ABC):
    """Abstract site adapter implementing a guarded checkout template."""

    RATE_LIMIT_BACKOFF_BASE_SECONDS = 1.0
    RATE_LIMIT_BACKOFF_MAX_SECONDS = 60.0

    def __init__(
        self,
        task_config: TaskConfig,
        profile: BuyerProfile,
        session: RobustAsyncSession | None = None,
    ) -> None:
        self.config = task_config
        self.profile = profile
        self.task_id = task_config.task_id
        self.site_key = task_config.site_key

        # 核心修復：權威路由解析。
        # bound_proxy 具備最高優先權；唯有在未預先綁定時才降級讀取 proxy_pool
        session_proxy_pool: list[str] | None = None
        if task_config.bound_proxy:
            bound_url = getattr(task_config.bound_proxy, "formatted_url", str(task_config.bound_proxy))
            session_proxy_pool = [bound_url]
        elif task_config.proxy_pool:
            session_proxy_pool = task_config.proxy_pool

        self.session: RobustAsyncSession = session or RobustAsyncSession(
            proxy_pool=session_proxy_pool,
            timeout=task_config.timeout_seconds,
            task_id=self.task_id,
        )
        self.fsm = AsyncStateMachine(
            task_id=self.task_id,
            initial_state=CheckoutState.INIT,
            max_state_retries=task_config.max_retries,
        )
        self.stock_info: StockStatus | None = None
        self.cart_result: CartResult | None = None
        self.checkout_result: CheckoutResult | None = None
        self._is_stopped = False

        def recover_out_of_stock(
            error: Exception,
            state: CheckoutState,
            retries: int,
        ) -> CheckoutState | None:
            del error, retries
            return CheckoutState.MONITOR if state == CheckoutState.ATC else None

        def recover_expired_session(
            error: Exception,
            state: CheckoutState,
            retries: int,
        ) -> CheckoutState | None:
            del error, retries
            if state in (CheckoutState.SHIPPING, CheckoutState.PAYMENT):
                return CheckoutState.ATC
            return None

        self.fsm.register_recovery_handler(
            (OutOfStockError, ProductUnavailableError),
            recover_out_of_stock,
        )
        self.fsm.register_recovery_handler(
            SessionExpiredError,
            recover_expired_session,
        )

    async def initialize_session(self) -> None:
        """Optional adapter hook for session setup before monitoring begins."""

    @abstractmethod
    async def check_stock(self) -> StockStatus:
        """Return normalized stock availability for the configured variant."""

    @abstractmethod
    async def add_to_cart(self) -> CartResult:
        """Attempt to add the configured variant to the cart."""

    @abstractmethod
    async def submit_shipping(self) -> bool:
        """Submit the shipping profile and return whether it was accepted."""

    @abstractmethod
    async def submit_checkout(self) -> CheckoutResult:
        """Submit checkout and return the normalized order result."""

    async def run(self) -> CheckoutResult:
        """Run session setup, stock monitoring, shipping, and checkout in FSM order."""
        try:
            async with self.session:
                while not self.fsm.is_terminal and not self._is_stopped:
                    state = self.fsm.current_state
                    try:
                        if state == CheckoutState.INIT:
                            await self.initialize_session()
                            await self.fsm.transition_to(CheckoutState.MONITOR)
                        elif state == CheckoutState.MONITOR:
                            self.stock_info = await self.check_stock()
                            if self.stock_info.in_stock:
                                await self.fsm.transition_to(CheckoutState.ATC)
                            else:
                                await asyncio.sleep(self.config.monitor_delay_ms / 1000)
                                await self.fsm.transition_to(CheckoutState.MONITOR)
                        elif state == CheckoutState.ATC:
                            self.cart_result = await self.add_to_cart()
                            if not self.cart_result.success:
                                raise CheckoutException(
                                    self.cart_result.error_message or "Unable to add item to cart",
                                    task_id=self.task_id,
                                )
                            await self.fsm.transition_to(CheckoutState.SHIPPING)
                        elif state == CheckoutState.SHIPPING:
                            if not await self.submit_shipping():
                                raise CheckoutException(
                                    "Shipping details were rejected",
                                    task_id=self.task_id,
                                )
                            if self.config.checkout_delay_ms:
                                await asyncio.sleep(self.config.checkout_delay_ms / 1000)
                            await self.fsm.transition_to(CheckoutState.PAYMENT)
                        elif state == CheckoutState.PAYMENT:
                            self.checkout_result = await self.submit_checkout()
                            if not self.checkout_result.success:
                                raise PaymentDeclinedError(
                                    reason=self.checkout_result.error_message or "Checkout failed",
                                    task_id=self.task_id,
                                )
                            await self.fsm.transition_to(CheckoutState.SUCCESS)
                        else:
                            raise CheckoutException(
                                f"Unsupported task state: {state.value}",
                                task_id=self.task_id,
                            )
                    except asyncio.CancelledError:
                        self._is_stopped = True
                        raise
                    except (OutOfStockError, ProductUnavailableError) as exc:
                        next_state = await self.fsm.handle_exception(exc)
                        if next_state == CheckoutState.FAILED:
                            return self._failure_result(str(exc))
                    except RateLimitError as exc:
                        retry_count = self.fsm._state_retry_counts[self.fsm.current_state]
                        if (
                            retry_count < self.fsm.max_state_retries
                            and self.fsm.can_transition_to(self.fsm.current_state)
                        ):
                            retry_after = exc.retry_after
                            if (
                                isinstance(retry_after, (int, float))
                                and math.isfinite(retry_after)
                                and retry_after > 0
                            ):
                                delay = min(retry_after, self.RATE_LIMIT_BACKOFF_MAX_SECONDS)
                            else:
                                delay = min(
                                    self.RATE_LIMIT_BACKOFF_BASE_SECONDS
                                    * (2 ** min(retry_count, 30)),
                                    self.RATE_LIMIT_BACKOFF_MAX_SECONDS,
                                )
                            logger.warning(
                                "[%s] Rate limited; waiting %.2fs before retry %d/%d",
                                self.task_id,
                                delay,
                                retry_count + 1,
                                self.fsm.max_state_retries,
                            )
                            await asyncio.sleep(delay)
                        next_state = await self.fsm.handle_exception(exc)
                        if next_state == CheckoutState.FAILED:
                            return self._failure_result(str(exc))
                    except SessionExpiredError as exc:
                        next_state = await self.fsm.handle_exception(exc)
                        if next_state == CheckoutState.FAILED:
                            return self._failure_result(str(exc))
                    except Exception as exc:  # noqa: BLE001 - site lifecycle boundary converts failures
                        logger.exception("[{}] Checkout task failed", self.task_id)
                        await self.fsm.transition_to(CheckoutState.FAILED)
                        self.checkout_result = self._failure_result(str(exc))
                        return self.checkout_result

            if self.checkout_result is not None and self.checkout_result.success:
                return self.checkout_result
            return self._failure_result("Checkout stopped before completion")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - site lifecycle boundary converts failures
            logger.exception("[{}] Checkout session failed", self.task_id)
            if not self.fsm.is_terminal:
                await self.fsm.transition_to(CheckoutState.FAILED)
            return self._failure_result(str(exc))
        finally:
            await self.session.close()

    def _failure_result(self, message: str) -> CheckoutResult:
        result = CheckoutResult(success=False, error_message=message)
        self.checkout_result = result
        return result

    def stop(self) -> None:
        """Ask a monitoring run to stop at the next completed step."""
        self._is_stopped = True


__all__ = [
    "BaseSite",
    "CartResult",
    "CheckoutResult",
    "ShippingRate",
    "ShippingResult",
    "StockResult",
    "StockStatus",
    "TaskSummary",
]