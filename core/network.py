from __future__ import annotations

import asyncio
import math
import random
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, ClassVar

from curl_cffi.requests import AsyncSession, Response
from curl_cffi.requests.errors import RequestsError
from loguru import logger
from typing_extensions import Self

try:
    from .proxy import ProxyPool
except ImportError:
    from proxy import ProxyPool

try:
    from .exceptions import (
        NetworkTransportError,
        RateLimitError,
        ServiceUnavailableError,
        WAFChallengeError,
    )
except ImportError:
    from exceptions import (
        NetworkTransportError,
        RateLimitError,
        ServiceUnavailableError,
        WAFChallengeError,
    )


# Compatibility name; both APIs use the same pool and cooldown state.
ProxyManager = ProxyPool


class RobustAsyncSession:
    """HTTP client with Chrome 124 impersonation, task-sticky proxies, cooldowns, and backoff retries."""

    DEFAULT_IMPERSONATE: str = "chrome124"
    RETRY_STATUS_CODES = frozenset({502, 503, 504})
    RETRY_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

    DEFAULT_CHROME124_HEADERS: ClassVar[dict[str, str]] = {
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "sec-fetch-site": "none",
        "sec-fetch-mode": "navigate",
        "sec-fetch-user": "?1",
        "sec-fetch-dest": "document",
        "accept-language": "en-US,en;q=0.9",
    }

    def __init__(
        self,
        proxy: str | None = None,
        *,
        impersonate: str | None = None,
        proxy_pool: Sequence[str] | ProxyPool | None = None,
        proxy_manager: ProxyPool | None = None,
        sticky: bool = True,
        sticky_id: str | None = None,
        rotate_on_retry: bool = True,
        timeout: float = 15.0,
        max_retries: int = 4,
        base_delay: float = 1.0,
        max_delay: float = 20.0,
        max_retry_after: float = 300.0,
        task_id: str | None = None,
        default_headers: Mapping[str, str] | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if base_delay < 0 or max_delay < 0:
            raise ValueError("retry delays must be non-negative")
        if not math.isfinite(max_retry_after) or max_retry_after <= 0:
            raise ValueError("max_retry_after must be finite and greater than zero")

        self.task_id = task_id or "TASK-UNSET"
        self.impersonate = impersonate or self.DEFAULT_IMPERSONATE
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retry_after = max_retry_after

        # 預設套用 Chrome 124 瀏覽器標頭，消滅指紋與標頭特徵矛盾
        self.default_headers = dict(default_headers) if default_headers is not None else dict(self.DEFAULT_CHROME124_HEADERS)
        self.dynamic_cookies: dict[str, str] = {}
        self.dynamic_headers: dict[str, str] = {}

        self.sticky = sticky
        self.sticky_id = sticky_id or self.task_id
        self.rotate_on_retry = rotate_on_retry

        if proxy_manager is not None:
            self.proxy_pool = proxy_manager
            self._owns_proxy_pool = False
        elif isinstance(proxy_pool, ProxyPool):
            self.proxy_pool = proxy_pool
            self._owns_proxy_pool = False
        else:
            configured_proxies = list(proxy_pool or ())
            if proxy and proxy not in configured_proxies:
                configured_proxies.insert(0, proxy)
            self.proxy_pool = ProxyPool(configured_proxies) if configured_proxies else None
            self._owns_proxy_pool = self.proxy_pool is not None

        self.proxy_manager = self.proxy_pool
        self._direct_proxy = proxy if self.proxy_pool is None else None
        self._current_proxy: str | None = None
        self._session: AsyncSession | None = None

    @property
    def cookies(self) -> Any:
        """提供對底層 Session Cookie Jar 的存取。"""
        if self._session is not None:
            return self._session.cookies
        return self.dynamic_cookies

    def inject_cookies(self, cookies: Mapping[str, str]) -> None:
        """動態注入 Cookie（例如 TokenHarvester 求解之 _px3 憑證）。"""
        self.dynamic_cookies.update(cookies)
        if self._session is not None:
            for k, v in cookies.items():
                self._session.cookies.set(k, v)

    def inject_headers(self, headers: Mapping[str, str]) -> None:
        """動態注入持久化 Header。"""
        self.dynamic_headers.update(headers)

    async def _ensure_proxy(self) -> str | None:
        if self._current_proxy is None and self.proxy_pool is not None:
            sticky_id = self.sticky_id if self.sticky else None
            self._current_proxy = await self.proxy_pool.get_proxy(task_id=sticky_id)
        return self._current_proxy or self._direct_proxy

    async def _cool_current_proxy(self, status_code: int, cooldown: float) -> None:
        if self.proxy_pool is not None and self._current_proxy is not None:
            await self.proxy_pool.report_restricted(
                self._current_proxy,
                status_code=status_code,
                cooldown=cooldown,
            )
            self._current_proxy = None

    async def rotate_proxy(self) -> str | None:
        """Cool down the current proxy and lease a replacement."""
        await self._cool_current_proxy(status_code=500, cooldown=15.0)
        return await self._ensure_proxy()

    async def _get_session(self) -> AsyncSession:
        if self._session is None:
            merged_headers = dict(self.default_headers)
            merged_headers.update(self.dynamic_headers)

            # 啟用 Chrome 124 impersonate 模擬真實二進位 TLS / HTTP2 指紋
            self._session = AsyncSession(
                impersonate=self.impersonate,
                timeout=self.timeout,
                headers=merged_headers,
            )
            for k, v in self.dynamic_cookies.items():
                self._session.cookies.set(k, v)
        return self._session

    @staticmethod
    def _retry_after_seconds(header: str | None) -> float | None:
        if not header:
            return None
        try:
            seconds = float(header.strip())
            return max(0.0, seconds) if math.isfinite(seconds) else None
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(header)
            except (TypeError, ValueError, OverflowError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
            return max(0.0, seconds) if math.isfinite(seconds) else None

    def _backoff_delay(self, attempt: int) -> float:
        ceiling = min(self.max_delay, self.base_delay * (2**attempt))
        return random.uniform(0.0, ceiling)

    def _inspect_waf_patterns(self, response: Response) -> None:
        """Stop on explicit WAF challenges and rate limits without rotating proxies."""
        status = response.status_code
        body = getattr(response, "text", "") or ""
        lowered_body = body.casefold()

        if status == 403 and (
            "_px" in lowered_body
            or "init.js" in lowered_body
            or "perimeterx" in lowered_body
            or "captcha" in lowered_body
            or "access denied" in lowered_body
            or re.search(r"\bpx\b", lowered_body)
        ):
            raise WAFChallengeError(
                message="HTTP 403 WAF challenge detected; request stopped",
                status_code=status,
                block_type="PX",
                task_id=self.task_id,
                raw_html=body[:1000],
            )

        if status == 403:
            raise WAFChallengeError(
                message="HTTP 403 access denial; request stopped",
                status_code=status,
                block_type="HTTP_403",
                task_id=self.task_id,
            )

        if status == 429:
            retry_after_header = (
                response.headers.get("Retry-After")
                or response.headers.get("retry-after")
            )
            retry_after = self._retry_after_seconds(retry_after_header)
            if retry_after is not None:
                retry_after = min(retry_after, self.max_retry_after)
            raise RateLimitError(
                message="HTTP 429 rate limit encountered; request stopped",
                retry_after=retry_after or 0.0,
                task_id=self.task_id,
            )

    async def execute_with_retry(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        max_retries: int | None = None,
        **kwargs: Any,
    ) -> Response:
        retry_limit = self.max_retries if max_retries is None else max_retries
        if retry_limit < 0:
            raise ValueError("max_retries must be non-negative")
        method = method.upper()
        if method not in self.RETRY_SAFE_METHODS:
            retry_limit = 0

        session = await self._get_session()
        request_timeout = kwargs.pop("timeout", self.timeout)
        has_proxy_override = "proxy" in kwargs
        proxy_override = kwargs.pop("proxy", None)

        # 合併即時請求標頭與動態標頭
        merged_req_headers = dict(self.dynamic_headers)
        if headers:
            merged_req_headers.update(headers)

        for attempt in range(retry_limit + 1):
            request_proxy = (
                proxy_override
                if has_proxy_override
                else await self._ensure_proxy()
            )

            try:
                response = await session.request(
                    method=method,
                    url=url,
                    headers=merged_req_headers if merged_req_headers else None,
                    timeout=request_timeout,
                    proxy=request_proxy,
                    **kwargs,
                )
            except (RequestsError, OSError, asyncio.TimeoutError) as exc:
                if self.rotate_on_retry and attempt < retry_limit:
                    await self._cool_current_proxy(status_code=500, cooldown=15.0)
                    await self._ensure_proxy()

                if attempt >= retry_limit:
                    raise NetworkTransportError(
                        f"Request failed after {attempt} retries: {exc}",
                        task_id=self.task_id,
                    ) from exc

                delay = self._backoff_delay(attempt)
                logger.warning(
                    "[{}] Transport error on {} {}; retrying in {:.2f}s ({}/{})",
                    self.task_id,
                    method.upper(),
                    url,
                    delay,
                    attempt + 1,
                    retry_limit,
                )
                await asyncio.sleep(delay)
                continue

            self._inspect_waf_patterns(response)

            if response.status_code not in self.RETRY_STATUS_CODES:
                return response

            retry_after = self._retry_after_seconds(
                response.headers.get("Retry-After") or response.headers.get("retry-after")
            )
            if retry_after is not None:
                retry_after = min(retry_after, self.max_retry_after)
            if self.rotate_on_retry and attempt < retry_limit:
                cooldown = retry_after if retry_after is not None else 60.0
                await self._cool_current_proxy(
                    status_code=response.status_code,
                    cooldown=cooldown,
                )
                await self._ensure_proxy()

            if response.status_code == 429:
                if attempt >= retry_limit:
                    raise RateLimitError(
                        message=f"HTTP 429 after {attempt} retries",
                        retry_after=retry_after or 0.0,
                        task_id=self.task_id,
                    )
            elif attempt >= retry_limit:
                raise ServiceUnavailableError(
                    message=f"HTTP {response.status_code} after {attempt} retries",
                    status_code=response.status_code,
                    task_id=self.task_id,
                )

            delay = self._backoff_delay(attempt)
            if response.status_code == 429 and retry_after is not None:
                delay = max(delay, retry_after)
            logger.warning(
                "[{}] HTTP {} on {}; retrying in {:.2f}s ({}/{})",
                self.task_id,
                response.status_code,
                url,
                delay,
                attempt + 1,
                retry_limit,
            )
            await asyncio.sleep(delay)

        raise AssertionError("unreachable retry loop")

    async def request(self, method: str, url: str, **kwargs: Any) -> Response:
        return await self.execute_with_retry(method, url, **kwargs)

    async def get(self, url: str, **kwargs: Any) -> Response:
        return await self.execute_with_retry("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Response:
        return await self.execute_with_retry("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> Response:
        return await self.execute_with_retry("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any) -> Response:
        return await self.execute_with_retry("DELETE", url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> Response:
        return await self.execute_with_retry("PATCH", url, **kwargs)

    async def close(self) -> None:
        session, self._session = self._session, None
        try:
            if session is not None:
                await session.close()
        finally:
            try:
                if self.proxy_pool is not None and self._current_proxy is not None:
                    proxy, self._current_proxy = self._current_proxy, None
                    await self.proxy_pool.report_success(proxy)
            finally:
                try:
                    if self.proxy_pool is not None and self.sticky:
                        await self.proxy_pool.release_task(self.sticky_id)
                finally:
                    if self._owns_proxy_pool and self.proxy_pool is not None:
                        await self.proxy_pool.stop()

    async def __aenter__(self) -> Self:
        await self._get_session()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: object,
    ) -> None:
        await self.close()