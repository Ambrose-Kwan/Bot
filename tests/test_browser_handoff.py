from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from utils import browser_handoff


def _playwright_mocks(goto: AsyncMock):
    page = SimpleNamespace(
        on=MagicMock(),
        goto=goto,
        url="https://acs.example.test/challenge",
        query_selector=AsyncMock(return_value=None),
    )
    context = SimpleNamespace(
        add_init_script=AsyncMock(),
        add_cookies=AsyncMock(),
        new_page=AsyncMock(return_value=page),
        cookies=AsyncMock(return_value=[]),
    )
    browser = SimpleNamespace(
        new_context=AsyncMock(return_value=context),
        close=AsyncMock(),
    )
    playwright = SimpleNamespace(
        chromium=SimpleNamespace(launch=AsyncMock(return_value=browser))
    )
    manager = MagicMock()
    manager.__aenter__ = AsyncMock(return_value=playwright)
    manager.__aexit__ = AsyncMock(return_value=False)
    return manager, browser


@pytest.mark.asyncio
async def test_handoff_timeout_closes_browser_and_playwright(monkeypatch: pytest.MonkeyPatch) -> None:
    async def slow_navigation(*_args: object, **_kwargs: object) -> None:
        await asyncio.sleep(10)

    manager, browser = _playwright_mocks(AsyncMock(side_effect=slow_navigation))
    monkeypatch.setattr(browser_handoff, "async_playwright", lambda: manager)
    curl_session = SimpleNamespace(cookies={})

    with pytest.raises(asyncio.TimeoutError):
        await browser_handoff.handoff_to_browser(
            curl_session,
            "https://acs.example.test/challenge",
            timeout_sec=0.02,
        )

    browser.close.assert_awaited_once()
    manager.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_handoff_cancellation_closes_browser_and_playwright(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    navigation_started = asyncio.Event()

    async def wait_for_navigation_cancel(*_args: object, **_kwargs: object) -> None:
        navigation_started.set()
        await asyncio.Event().wait()

    manager, browser = _playwright_mocks(AsyncMock(side_effect=wait_for_navigation_cancel))
    monkeypatch.setattr(browser_handoff, "async_playwright", lambda: manager)
    curl_session = SimpleNamespace(cookies={})

    handoff_task = asyncio.create_task(
        browser_handoff.handoff_to_browser(
            curl_session,
            "https://acs.example.test/challenge",
            timeout_sec=10,
        )
    )
    await asyncio.wait_for(navigation_started.wait(), timeout=1)
    handoff_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await handoff_task

    browser.close.assert_awaited_once()
    manager.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("timeout_sec", [0, -1, float("inf"), float("nan")])
@pytest.mark.asyncio
async def test_handoff_rejects_invalid_timeout(timeout_sec: float) -> None:
    with pytest.raises(ValueError, match="timeout_sec"):
        await browser_handoff.handoff_to_browser(
            SimpleNamespace(cookies={}),
            "https://acs.example.test/challenge",
            timeout_sec=timeout_sec,
        )
