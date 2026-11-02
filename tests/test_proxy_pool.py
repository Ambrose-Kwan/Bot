from __future__ import annotations

import pytest

from core.proxy import ProxyPool, ProxyPoolEmptyError


@pytest.mark.asyncio
async def test_static_proxy_is_returned_after_successful_lease() -> None:
    pool = ProxyPool(["http://proxy.example:8080"])

    leased = await pool.get_proxy(timeout=0.05)
    await pool.report_success(leased)
    leased_again = await pool.get_proxy(timeout=0.05)

    assert leased_again == leased
    await pool.report_success(leased_again)


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [403, 429])
async def test_restricted_proxy_enters_cooldown_and_is_released(status_code: int) -> None:
    proxy = "http://proxy.example:8080"
    pool = ProxyPool([proxy], default_cooldown=0.04, sweep_interval=0.005)

    leased = await pool.get_proxy(timeout=0.05)
    await pool.report_restricted(leased, status_code=status_code)

    with pytest.raises(ProxyPoolEmptyError):
        await pool.get_proxy(timeout=0.01)

    assert await pool.get_proxy(timeout=0.2) == proxy
    await pool.report_success(proxy)
    await pool.stop()


@pytest.mark.asyncio
async def test_sticky_task_reuses_its_residential_proxy() -> None:
    generated: list[str] = []

    def proxy_factory(token: str) -> str:
        proxy = f"http://{token}@proxy.example:8080"
        generated.append(proxy)
        return proxy

    pool = ProxyPool(proxy_factory=proxy_factory)
    first = await pool.get_proxy(task_id="TASK-1")
    await pool.report_success(first)
    second = await pool.get_proxy(task_id="TASK-1")

    assert second == first
    assert generated == [first]

    await pool.report_success(second)
    await pool.release_task("TASK-1")
    await pool.stop()
