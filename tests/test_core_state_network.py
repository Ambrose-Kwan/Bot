from __future__ import annotations

import asyncio

import pytest

from core import network
from core.exceptions import (
    CheckoutException,
    InvalidStateTransitionError,
    NetworkTransportError,
    RateLimitError,
    WafChallengeError,
)
from core.state_machine import AsyncStateMachine, TaskState


class _Response:
    def __init__(self, status_code: int, text: str = "", headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class _Session:
    def __init__(
        self,
        responses: list[_Response] | None = None,
        errors: list[Exception] | None = None,
    ):
        self.responses = responses or []
        self.errors = errors or []
        self.request_count = 0

    async def request(self, **kwargs: object) -> _Response:
        self.request_count += 1
        if self.errors:
            raise self.errors.pop(0)
        return self.responses.pop(0)

    async def close(self) -> None:
        return None


class _MonitorSession:
    def __init__(self, *, data: dict | None = None, errors: list[Exception] | None = None):
        self.data = data or {"product": {"variants": []}}
        self.errors = errors or []
        self.request_count = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute_with_retry(self, *_: object, **__: object):
        self.request_count += 1
        if self.errors:
            raise self.errors.pop(0)
        return _MonitorResponse(self.data)


class _MonitorResponse:
    status_code = 200

    def __init__(self, data: dict):
        self.data = data

    def json(self) -> dict:
        return self.data


@pytest.mark.asyncio
async def test_state_machine_tracks_duration_and_rejects_illegal_edges() -> None:
    machine = AsyncStateMachine(task_id="TASK-1")

    await machine.transition_to(TaskState.MONITOR)
    assert machine.current_state is TaskState.MONITOR
    assert machine.history[0].duration_ms >= 0

    with pytest.raises(CheckoutException):
        await machine.transition_to(TaskState.SUCCESS)


@pytest.mark.asyncio
async def test_network_stops_on_rate_limit_and_preserves_retry_after(monkeypatch) -> None:
    fake_session = _Session([
        _Response(429, headers={"Retry-After": "3"}),
    ])
    monkeypatch.setattr(network, "AsyncSession", lambda **kwargs: fake_session)
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(network.asyncio, "sleep", sleep)
    client = network.RobustAsyncSession(max_retries=1, base_delay=0.01)

    with pytest.raises(RateLimitError) as error:
        await client.execute_with_retry("GET", "https://example.test")

    assert error.value.retry_after == 3
    assert not delays
    assert len(fake_session.responses) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_after", "expected"),
    [("999999", 300.0), ("inf", 0.0), ("nan", 0.0), ("invalid", 0.0)],
)
async def test_network_bounds_and_validates_retry_after(
    monkeypatch,
    retry_after: str,
    expected: float,
) -> None:
    fake_session = _Session([_Response(429, headers={"Retry-After": retry_after})])
    monkeypatch.setattr(network, "AsyncSession", lambda **kwargs: fake_session)
    client = network.RobustAsyncSession(max_retries=0)

    with pytest.raises(RateLimitError) as error:
        await client.get("https://example.test")

    assert error.value.retry_after == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", ["init.js", "PX", "PerimeterX"])
async def test_network_fails_closed_on_waf_challenge(monkeypatch, marker: str) -> None:
    fake_session = _Session([_Response(403, text=f"blocked by {marker}")])
    monkeypatch.setattr(network, "AsyncSession", lambda **kwargs: fake_session)
    client = network.RobustAsyncSession()

    with pytest.raises(WafChallengeError) as error:
        await client.get("https://example.test")

    assert error.value.block_type == "PX"


@pytest.mark.asyncio
async def test_network_stops_on_unclassified_403_without_retry(monkeypatch) -> None:
    fake_session = _Session([_Response(403, text="Forbidden")])
    monkeypatch.setattr(network, "AsyncSession", lambda **kwargs: fake_session)
    client = network.RobustAsyncSession(max_retries=3)

    with pytest.raises(WafChallengeError) as error:
        await client.get("https://example.test")

    assert error.value.block_type == "HTTP_403"
    assert fake_session.request_count == 1


@pytest.mark.asyncio
async def test_network_does_not_retry_non_idempotent_post(monkeypatch) -> None:
    fake_session = _Session(errors=[OSError("connection lost")])
    monkeypatch.setattr(network, "AsyncSession", lambda **kwargs: fake_session)
    client = network.RobustAsyncSession(max_retries=3, base_delay=0)

    with pytest.raises(NetworkTransportError):
        await client.post("https://example.test/orders", json={"item": "sample"})

    assert fake_session.request_count == 1


@pytest.mark.asyncio
async def test_state_machine_rejects_nested_transition_from_async_hook() -> None:
    machine = AsyncStateMachine(task_id="TASK-HOOK-ASYNC")

    async def reenter(*_args: object) -> None:
        await machine.transition(TaskState.ATC)

    machine.register_before_hook(reenter)

    with pytest.raises(InvalidStateTransitionError):
        await asyncio.wait_for(machine.transition(TaskState.MONITOR), timeout=0.1)

    assert machine.current_state is TaskState.INIT
    assert not machine.history


@pytest.mark.asyncio
async def test_state_machine_rejects_transition_spawned_by_hook() -> None:
    machine = AsyncStateMachine(task_id="TASK-HOOK-TASK")

    def spawn_reentry(*_args: object):
        return asyncio.create_task(machine.transition(TaskState.ATC))

    machine.register_before_hook(spawn_reentry)

    with pytest.raises(InvalidStateTransitionError):
        await asyncio.wait_for(machine.transition(TaskState.MONITOR), timeout=0.1)

    assert machine.current_state is TaskState.INIT
    assert not machine.history


@pytest.mark.parametrize("poll_interval_ms", [0, -1])
def test_monitor_rejects_non_positive_poll_interval(poll_interval_ms: int) -> None:
    from core.monitor import FastStockMonitor

    with pytest.raises(ValueError, match="poll_interval_ms"):
        FastStockMonitor(
            sku="VN000TEST",
            preferred_sizes=[],
            poll_interval_ms=poll_interval_ms,
        )


@pytest.mark.asyncio
async def test_monitor_stops_after_consecutive_errors(monkeypatch) -> None:
    from core import monitor

    fake_session = _MonitorSession(errors=[OSError("network down")] * 5)
    monkeypatch.setattr(monitor, "RobustAsyncSession", lambda **kwargs: fake_session)
    stock_monitor = monitor.FastStockMonitor(
        sku="VN000TEST",
        preferred_sizes=[],
        poll_interval_ms=1,
        max_consecutive_errors=2,
    )

    await stock_monitor.start()

    assert fake_session.request_count == 2


@pytest.mark.asyncio
async def test_monitor_stops_at_total_duration_limit(monkeypatch) -> None:
    from core import monitor

    fake_session = _MonitorSession()
    monkeypatch.setattr(monitor, "RobustAsyncSession", lambda **kwargs: fake_session)
    stock_monitor = monitor.FastStockMonitor(
        sku="VN000TEST",
        preferred_sizes=[],
        poll_interval_ms=1,
        max_monitor_duration=0.02,
    )

    await stock_monitor.start()

    assert fake_session.request_count > 0
    assert fake_session.request_count < 100


def test_monitor_uses_bounded_backoff_for_invalid_retry_after() -> None:
    from core.monitor import FastStockMonitor

    stock_monitor = FastStockMonitor(
        sku="VN000TEST",
        preferred_sizes=[],
        poll_interval_ms=500,
        max_backoff_seconds=2,
    )

    assert stock_monitor._rate_limit_delay(0, consecutive_errors=3) == 2
    assert stock_monitor._rate_limit_delay(float("nan"), consecutive_errors=1) == 0.5
