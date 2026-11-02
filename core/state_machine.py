from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from inspect import isawaitable
from typing import Any, ClassVar

from loguru import logger

from .exceptions import CheckoutException, InvalidStateTransitionError

_active_hook_machine: ContextVar[int | None] = ContextVar(
    "active_fsm_hook_machine",
    default=None,
)


class TaskState(str, Enum):
    """States in a checkout task lifecycle."""

    INIT = "INIT"
    MONITOR = "MONITOR"
    ATC = "ATC"
    SHIPPING = "SHIPPING"
    PAYMENT = "PAYMENT"
    CHALLENGE_SOLVING = "CHALLENGE_SOLVING"
    AWAIT_3DS = "AWAIT_3DS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    ERROR = "FAILED"  # noqa: PIE796 - backward-compatible enum alias


CheckoutState = TaskState


@dataclass
class StateTransitionRecord:
    """Record of a state transition for auditing and performance metrics."""
    from_state: CheckoutState
    to_state: CheckoutState
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_seconds: float = 0.0
    duration_ms: float = 0.0
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


TransitionHook = Callable[[CheckoutState, CheckoutState, dict[str, Any]], None | Awaitable[None]]
ErrorRecoveryHandler = Callable[[Exception, CheckoutState, int], CheckoutState | None | Awaitable[CheckoutState | None]]


class AsyncStateMachine:
    """
    Asynchronous Finite State Machine with transition constraints and challenge recovery.
    """

    ALLOWED_TRANSITIONS: ClassVar[dict[TaskState, set[TaskState]]] = {
        CheckoutState.INIT: {
            CheckoutState.MONITOR,
            CheckoutState.ATC,
            CheckoutState.CHALLENGE_SOLVING,
            CheckoutState.FAILED,
        },
        CheckoutState.MONITOR: {
            CheckoutState.MONITOR,
            CheckoutState.ATC,
            CheckoutState.CHALLENGE_SOLVING,
            CheckoutState.FAILED,
        },
        CheckoutState.ATC: {
            CheckoutState.ATC,
            CheckoutState.SHIPPING,
            CheckoutState.MONITOR,
            CheckoutState.CHALLENGE_SOLVING,
            CheckoutState.FAILED,
        },
        CheckoutState.SHIPPING: {
            CheckoutState.SHIPPING,
            CheckoutState.PAYMENT,
            CheckoutState.ATC,
            CheckoutState.CHALLENGE_SOLVING,
            CheckoutState.FAILED,
        },
        CheckoutState.PAYMENT: {
            CheckoutState.PAYMENT,
            CheckoutState.AWAIT_3DS,
            CheckoutState.SUCCESS,
            CheckoutState.SHIPPING,
            CheckoutState.ATC,
            CheckoutState.CHALLENGE_SOLVING,
            CheckoutState.FAILED,
        },
        CheckoutState.CHALLENGE_SOLVING: {
            CheckoutState.INIT,
            CheckoutState.MONITOR,
            CheckoutState.ATC,
            CheckoutState.SHIPPING,
            CheckoutState.PAYMENT,
            CheckoutState.CHALLENGE_SOLVING,
            CheckoutState.FAILED,
        },
        CheckoutState.AWAIT_3DS: {
            CheckoutState.SUCCESS,
            CheckoutState.PAYMENT,
            CheckoutState.AWAIT_3DS,
            CheckoutState.FAILED,
        },
        CheckoutState.SUCCESS: set(),
        CheckoutState.FAILED: set(),
    }
    DEFAULT_TRANSITION_GRAPH = ALLOWED_TRANSITIONS

    def __init__(
        self,
        task_id: str,
        initial_state: CheckoutState = CheckoutState.INIT,
        transition_graph: dict[CheckoutState, set[CheckoutState]] | None = None,
        max_state_retries: int = 3,
    ) -> None:
        self.task_id = task_id
        self._current_state = initial_state
        self._previous_state: CheckoutState | None = None
        self._state_before_challenge: CheckoutState | None = None
        self._state_entered_at: float = time.monotonic()
        self._transitions_history: list[StateTransitionRecord] = []

        self.transition_graph = (
            transition_graph if transition_graph is not None else self.DEFAULT_TRANSITION_GRAPH
        )
        self.max_state_retries = max_state_retries
        self._state_retry_counts: dict[CheckoutState, int] = {s: 0 for s in CheckoutState}

        self._before_hooks: list[TransitionHook] = []
        self._after_hooks: list[TransitionHook] = []
        self._recovery_handlers: dict[type, ErrorRecoveryHandler] = {}
        self._lock = asyncio.Lock()

    @property
    def current_state(self) -> CheckoutState:
        return self._current_state

    @property
    def previous_state(self) -> CheckoutState | None:
        return self._previous_state

    @property
    def state_before_challenge(self) -> CheckoutState | None:
        return self._state_before_challenge

    @property
    def is_terminal(self) -> bool:
        return self._current_state in (CheckoutState.SUCCESS, CheckoutState.FAILED)

    @property
    def history(self) -> list[StateTransitionRecord]:
        return list(self._transitions_history)

    def register_before_hook(self, hook: TransitionHook) -> None:
        self._before_hooks.append(hook)

    def register_after_hook(self, hook: TransitionHook) -> None:
        self._after_hooks.append(hook)

    def register_recovery_handler(
        self, exception_cls: type, handler: ErrorRecoveryHandler
    ) -> None:
        self._recovery_handlers[exception_cls] = handler

    def can_transition_to(self, target_state: CheckoutState) -> bool:
        allowed = self.transition_graph.get(self._current_state, set())
        return target_state in allowed

    async def transition(
        self,
        target_state: CheckoutState,
        reason: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if _active_hook_machine.get() == id(self):
            raise InvalidStateTransitionError(
                from_state=self._current_state.value,
                to_state=str(target_state),
                task_id=self.task_id,
            )

        try:
            target_state = TaskState(target_state)
        except (TypeError, ValueError) as exc:
            raise CheckoutException(
                message=f"Unknown task state: {target_state!r}",
                task_id=self.task_id,
            ) from exc

        async with self._lock:
            from_state = self._current_state

            if not self.can_transition_to(target_state):
                logger.error(
                    f"[{self.task_id}] Invalid state transition: {from_state.value} -> {target_state.value}"
                )
                raise InvalidStateTransitionError(
                    from_state=from_state.value,
                    to_state=target_state.value,
                    task_id=self.task_id,
                )

            ctx = context or {}

            for hook in self._before_hooks:
                token = _active_hook_machine.set(id(self))
                try:
                    res = hook(from_state, target_state, ctx)
                    if isawaitable(res):
                        await res
                finally:
                    _active_hook_machine.reset(token)

            now = time.monotonic()
            duration = now - self._state_entered_at
            duration_ms = duration * 1000

            record = StateTransitionRecord(
                from_state=from_state,
                to_state=target_state,
                duration_seconds=round(duration, 3),
                duration_ms=round(duration_ms, 1),
                reason=reason,
                metadata=dict(ctx),
            )
            self._transitions_history.append(record)

            if target_state == CheckoutState.CHALLENGE_SOLVING and from_state != CheckoutState.CHALLENGE_SOLVING:
                self._state_before_challenge = from_state

            self._previous_state = from_state
            self._current_state = target_state
            self._state_entered_at = now

            if target_state == from_state:
                self._state_retry_counts[target_state] += 1
            else:
                self._state_retry_counts[from_state] = 0

            log_msg = (
                f"[{self.task_id}] STATE: {from_state.value} -> {target_state.value} "
                f"({round(duration_ms, 1)}ms)"
            )
            if reason:
                log_msg += f" | Reason: {reason}"
            logger.info(log_msg)

            for hook in self._after_hooks:
                token = _active_hook_machine.set(id(self))
                try:
                    res = hook(from_state, target_state, ctx)
                    if isawaitable(res):
                        await res
                finally:
                    _active_hook_machine.reset(token)

    async def transition_to(self, new_state: TaskState) -> None:
        await self.transition(new_state)

    async def handle_exception(
        self,
        error: Exception,
        context: dict[str, Any] | None = None,
    ) -> CheckoutState:
        current = self._current_state
        retries = self._state_retry_counts[current]

        logger.warning(
            f"[{self.task_id}] Exception in state {current.value}: {type(error).__name__}: {error} "
            f"(Retries: {retries}/{self.max_state_retries})"
        )

        for exc_cls, handler in self._recovery_handlers.items():
            if isinstance(error, exc_cls):
                suggested = handler(error, current, retries)
                if isawaitable(suggested):
                    suggested = await suggested
                if suggested and self.can_transition_to(suggested):
                    await self.transition(
                        suggested,
                        reason=f"Recovery via {type(error).__name__}: {error}",
                        context=context,
                    )
                    return suggested

        if retries < self.max_state_retries and self.can_transition_to(current):
            await self.transition(
                current,
                reason=f"Retry attempt {retries + 1} after {type(error).__name__}",
                context=context,
            )
            return current

        await self.transition(
            CheckoutState.FAILED,
            reason=f"Fatal exception: {type(error).__name__}: {error!s}",
            context=context,
        )
        return CheckoutState.FAILED