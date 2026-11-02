"""
Module: utils.token_harvester
Description: Hybrid Token Harvester Pool combining headless browsers with CapSolver / 2Captcha API
for generating clean PerimeterX (_px3) credentials.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

try:
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None  # type: ignore
    PlaywrightError = RuntimeError  # type: ignore[misc, assignment]

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore

logger = logging.getLogger("nexus.harvester")


@dataclass
class HarvesterToken:
    """Represents a cached WAF credential token."""
    cookies: dict[str, str]
    created_at: float
    ttl: float = 120.0

    @property
    def is_expired(self) -> bool:
        return (time.monotonic() - self.created_at) > (self.ttl - 15.0)


class HybridTokenHarvesterPool:
    """
    Maintains a pool of headless browser workers evaluating WAF challenge JS
    and solves interactive challenges via external solving services (CapSolver / 2Captcha).
    """

    def __init__(
        self,
        target_url: str = "https://www.vans.com",
        pool_size: int = 2,
        capsolver_api_key: str | None = None,
        twocaptcha_api_key: str | None = None,
        proxy: str | None = None,
    ) -> None:
        self.target_url = target_url
        self.pool_size = pool_size
        self.capsolver_api_key = capsolver_api_key
        self.twocaptcha_api_key = twocaptcha_api_key
        self.proxy = proxy

        self._queue: asyncio.Queue[HarvesterToken] = asyncio.Queue(maxsize=pool_size * 2)
        self._running: bool = False
        self._workers: list[asyncio.Task[None]] = []
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start token harvesting workers."""
        if self._running:
            return
        self._running = True
        for i in range(self.pool_size):
            task = asyncio.create_task(self._harvester_worker(worker_id=i))
            self._workers.append(task)
        logger.info(f"HybridTokenHarvesterPool started with {self.pool_size} workers.")

    async def stop(self) -> None:
        """Gracefully shutdown workers."""
        self._running = False
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("HybridTokenHarvesterPool stopped.")

    async def get_valid_token(self, timeout: float = 30.0) -> dict[str, str]:
        """Fetch a fresh _px3 token from the harvested pool or trigger an immediate harvest."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                token = self._queue.get_nowait()
                if not token.is_expired and "_px3" in token.cookies:
                    return token.cookies
            except asyncio.QueueEmpty:
                pass

            harvested = await self._harvest_once()
            if harvested and "_px3" in harvested:
                return harvested
            await asyncio.sleep(1.0)

        raise TimeoutError("Failed to acquire valid WAF token within timeout window")

    async def _harvester_worker(self, worker_id: int) -> None:
        """Background continuous worker generating and replenishing token buffer."""
        while self._running:
            try:
                cookies = await self._harvest_once()
                if cookies and "_px3" in cookies:
                    token = HarvesterToken(cookies=cookies, created_at=time.monotonic())
                    await self._queue.put(token)
                    logger.debug(f"[Worker-{worker_id}] Fresh _px3 token cached into pool.")
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 - worker loop logs failures and remains available
                logger.error(f"[Worker-{worker_id}] Token harvest error: {exc}")
                await asyncio.sleep(5.0)

    async def _harvest_once(self) -> dict[str, str]:
        """Spawn a browser instance to navigate target domain and harvest _px3 cookie."""
        if async_playwright is None:
            logger.warning("Playwright is not installed. Falling back to external API solvers directly.")
            return await self._dispatch_external_solvers(self.target_url)

        cookies_dict: dict[str, str] = {}
        async with async_playwright() as p:
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
            browser = await p.chromium.launch(headless=True, args=launch_args)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                proxy={"server": self.proxy} if self.proxy else None,
            )

            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )

            page = await context.new_page()
            try:
                await page.goto(self.target_url, wait_until="domcontentloaded", timeout=25000)
                await asyncio.sleep(3.0)

                captcha_frame = page.locator("iframe[title*='PerimeterX'], div#px-captcha")
                if await captcha_frame.count() > 0:
                    logger.warning("Interactive PX challenge detected in harvester. Forwarding to API solvers...")
                    solved = await self._dispatch_external_solvers(page.url)
                    if solved and "_px3" in solved:
                        return solved

                all_cookies = await context.cookies()
                for c in all_cookies:
                    if c["name"] in ("_px3", "_pxvid", "_pxhd", "_px2"):
                        cookies_dict[c["name"]] = c["value"]

            except PlaywrightError as exc:
                logger.debug(f"Harvester navigation note: {exc}")
            finally:
                await context.close()
                await browser.close()

        return cookies_dict

    async def _dispatch_external_solvers(self, page_url: str) -> dict[str, str]:
        """Attempt solving with CapSolver first, falling back to 2Captcha."""
        if self.capsolver_api_key:
            token = await self.solve_perimeterx_via_capsolver(page_url)
            if token and "_px3" in token:
                return token

        if self.twocaptcha_api_key:
            token = await self.solve_perimeterx_via_2captcha(page_url)
            if token and "_px3" in token:
                return token

        return {}

    async def solve_perimeterx_via_capsolver(self, page_url: str) -> dict[str, str]:
        """Solve PerimeterX challenge using CapSolver API."""
        if not self.capsolver_api_key or httpx is None:
            return {}

        api_url = "https://api.capsolver.com/createTask"
        payload = {
            "clientKey": self.capsolver_api_key,
            "task": {
                "type": "AntiPerimeterXTask",
                "websiteURL": page_url,
                "proxy": self.proxy or "",
            },
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                res = await client.post(api_url, json=payload)
                data = res.json()
                task_id = data.get("taskId")
                if not task_id:
                    logger.error(f"CapSolver failed to create task: {data}")
                    return {}

                result_url = "https://api.capsolver.com/getTaskResult"
                for _ in range(30):
                    await asyncio.sleep(2.0)
                    poll_res = await client.post(
                        result_url,
                        json={"clientKey": self.capsolver_api_key, "taskId": task_id},
                    )
                    poll_data = poll_res.json()
                    if poll_data.get("status") == "ready":
                        solution = poll_data.get("solution", {})
                        token = solution.get("token") or solution.get("cookie")
                        logger.info("CapSolver successfully resolved PerimeterX challenge.")
                        return {"_px3": str(token)}
                    if poll_data.get("status") == "failed":
                        logger.error(f"CapSolver failed: {poll_data}")
                        break
        except (httpx.HTTPError, ValueError, TypeError, AttributeError) as exc:
            logger.error(f"CapSolver request error: {exc}")
        return {}

    async def solve_perimeterx_via_2captcha(self, page_url: str) -> dict[str, str]:
        """Solve PerimeterX challenge using 2Captcha API."""
        if not self.twocaptcha_api_key or httpx is None:
            return {}

        api_url = "https://api.2captcha.com/createTask"
        payload = {
            "clientKey": self.twocaptcha_api_key,
            "task": {
                "type": "PerimeterxTask",
                "websiteURL": page_url,
                "proxy": self.proxy or "",
            },
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                res = await client.post(api_url, json=payload)
                data = res.json()
                task_id = data.get("taskId")
                if not task_id:
                    logger.error(f"2Captcha failed to create task: {data}")
                    return {}

                result_url = "https://api.2captcha.com/getTaskResult"
                for _ in range(30):
                    await asyncio.sleep(2.0)
                    poll_res = await client.post(
                        result_url,
                        json={"clientKey": self.twocaptcha_api_key, "taskId": task_id},
                    )
                    poll_data = poll_res.json()
                    if poll_data.get("status") == "ready":
                        solution = poll_data.get("solution", {})
                        token = solution.get("token")
                        logger.info("2Captcha successfully resolved PerimeterX challenge.")
                        return {"_px3": str(token)}
                    if poll_data.get("status") == "failed":
                        logger.error(f"2Captcha failed: {poll_data}")
                        break
        except (httpx.HTTPError, ValueError, TypeError, AttributeError) as exc:
            logger.error(f"2Captcha request error: {exc}")
        return {}