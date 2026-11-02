"""Proxy leasing, sticky residential sessions, and cooldown management."""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from uuid import uuid4

logger = logging.getLogger("ProxyPool")


class ProxyPoolEmptyError(Exception):
    """Raised when no proxy is available before the acquisition timeout."""


class ProxyLeaseOwnershipError(RuntimeError):
    """Raised when a task attempts to mutate or release another task's proxy lease."""


class ResidentialProxyProvider:
    """Factory helpers for common commercial residential proxy URL formats."""

    @staticmethod
    def create_factory(
        provider: str,
        username: str,
        password: str,
        host: str,
        port: int,
        session_duration_minutes: int = 10,
        extra_zone: str | None = None,
    ) -> Callable[[str], str]:
        provider = provider.lower()

        def factory(session_token: str) -> str:
            if provider == "oxylabs":
                user_str = (
                    f"customer-{username}-sessid-{session_token}"
                    f"-sesstime-{session_duration_minutes}"
                )
            elif provider == "brightdata":
                zone_str = f"-zone-{extra_zone}" if extra_zone else ""
                user_str = f"lum-customer-{username}{zone_str}-session-{session_token}"
            elif provider == "smartproxy":
                user_str = (
                    f"user-{username}-session-{session_token}"
                    f"-sessionduration-{session_duration_minutes}"
                )
            else:
                user_str = f"{username}-session-{session_token}"

            return f"http://{user_str}:{password}@{host}:{port}"

        return factory


class ProxyPool:
    """Single proxy manager for static nodes and task-scoped residential sessions."""

    def __init__(
        self,
        proxies: Sequence[str] | None = None,
        default_cooldown: float = 60.0,
        sweep_interval: float = 1.0,
        *,
        proxy_factory: Callable[[str], str] | None = None,
    ) -> None:
        if default_cooldown < 0 or sweep_interval <= 0:
            raise ValueError("Cooldown must be non-negative and sweep interval positive")

        self._all_proxies = set(proxies or ())
        self._proxy_factory = proxy_factory
        self.default_cooldown = default_cooldown
        self.sweep_interval = sweep_interval

        self._available_queue: asyncio.Queue[str] = asyncio.Queue()
        self._available: set[str] = set()
        self._leased: set[str] = set()
        self._lease_owners: dict[str, str] = {}
        self._factory_proxies: set[str] = set()
        self._cooldown_dict: dict[str, float] = {}
        self._sticky_sessions: dict[str, str] = {}
        self._proxy_tasks: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._sweeper_task: asyncio.Task[None] | None = None

        for proxy in self._all_proxies:
            self._enqueue(proxy)

    def _enqueue(self, proxy: str) -> None:
        if proxy not in self._available:
            self._available.add(proxy)
            self._available_queue.put_nowait(proxy)

    def _is_managed_proxy(self, proxy: str) -> bool:
        return proxy in self._all_proxies or proxy in self._factory_proxies

    def _new_sticky_proxy(self, task_id: str | None) -> str:
        token = f"{task_id}-{uuid4().hex}" if task_id else uuid4().hex
        if self._proxy_factory is None:
            raise RuntimeError("A proxy factory is required to create a residential proxy.")
        proxy = self._proxy_factory(token)
        self._factory_proxies.add(proxy)
        if task_id:
            self._sticky_sessions[task_id] = proxy
            self._proxy_tasks[proxy] = task_id
        return proxy

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._sweeper_task = asyncio.create_task(self._cooldown_sweeper())
        logger.info("ProxyPool started with %d static proxies.", len(self._all_proxies))

    async def stop(self) -> None:
        self._running = False
        if self._sweeper_task:
            self._sweeper_task.cancel()
            try:
                await self._sweeper_task
            except asyncio.CancelledError:
                pass
            self._sweeper_task = None
        logger.info("ProxyPool stopped.")

    async def get_proxy(
        self,
        timeout: float | None = 10.0,
        *,
        task_id: str | None = None,
    ) -> str:
        if self._cooldown_dict and not self._running:
            await self.start()

        if self._proxy_factory:
            async with self._lock:
                if task_id:
                    proxy = self._sticky_sessions.get(task_id)
                    if proxy is None:
                        proxy = self._new_sticky_proxy(task_id)
                    if proxy in self._cooldown_dict:
                        self._sticky_sessions.pop(task_id, None)
                        proxy = self._new_sticky_proxy(task_id)
                    if proxy in self._leased:
                        raise ProxyPoolEmptyError(
                            f"Proxy for task {task_id!r} is already leased."
                        )
                else:
                    proxy = self._new_sticky_proxy(None)
                self._leased.add(proxy)
                if task_id:
                    self._lease_owners[proxy] = task_id
                return proxy

        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
        while True:
            try:
                if deadline is None:
                    proxy = await self._available_queue.get()
                else:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError
                    proxy = await asyncio.wait_for(
                        self._available_queue.get(),
                        timeout=remaining,
                    )
            except asyncio.TimeoutError as exc:
                raise ProxyPoolEmptyError(
                    "All proxies are exhausted or cooling down."
                ) from exc

            async with self._lock:
                self._available.discard(proxy)
                if proxy in self._cooldown_dict or proxy in self._leased:
                    continue
                self._leased.add(proxy)
                if task_id:
                    self._lease_owners[proxy] = task_id
                return proxy

    async def report_success(
        self,
        proxy: str,
        *,
        task_id: str | None = None,
    ) -> None:
        async with self._lock:
            if not self._is_managed_proxy(proxy):
                logger.warning(
                    "Ignoring report_success for unmanaged external proxy %r.",
                    proxy,
                )
                return

            owner = self._lease_owners.get(proxy)
            if task_id is not None and owner is not None and owner != task_id:
                raise ProxyLeaseOwnershipError(
                    f"Task {task_id!r} cannot release proxy owned by {owner!r}."
                )
            if task_id is not None and proxy not in self._leased:
                logger.debug(
                    "Ignoring duplicate report_success from task %r for unleased proxy.",
                    task_id,
                )
                return

            self._lease_owners.pop(proxy, None)
            self._leased.discard(proxy)
            if proxy not in self._cooldown_dict and proxy not in self._factory_proxies:
                self._enqueue(proxy)

    async def report_restricted(
        self,
        proxy: str,
        status_code: int,
        cooldown: float | None = None,
        *,
        task_id: str | None = None,
    ) -> None:
        duration = self.default_cooldown if cooldown is None else max(0.0, cooldown)
        release_at = time.monotonic() + duration
        async with self._lock:
            if not self._is_managed_proxy(proxy):
                logger.warning(
                    "Ignoring report_restricted for unmanaged external proxy %r.",
                    proxy,
                )
                return

            owner = self._lease_owners.get(proxy)
            if task_id is not None and owner is not None and owner != task_id:
                raise ProxyLeaseOwnershipError(
                    f"Task {task_id!r} cannot restrict proxy owned by {owner!r}."
                )

            self._lease_owners.pop(proxy, None)
            self._leased.discard(proxy)
            self._available.discard(proxy)
            self._cooldown_dict[proxy] = release_at
            mapped_task = self._proxy_tasks.get(proxy)
            if mapped_task and self._sticky_sessions.get(mapped_task) == proxy:
                self._sticky_sessions.pop(mapped_task, None)
            logger.warning(
                "Proxy received HTTP %d; cooling down for %.1fs "
                "(available: %d, cooling: %d).",
                status_code,
                duration,
                self._available_queue.qsize(),
                len(self._cooldown_dict),
            )

    async def release_task(self, task_id: str) -> None:
        """Release sticky mappings and any static proxy leases still owned by task_id."""
        async with self._lock:
            sticky_proxy = self._sticky_sessions.pop(task_id, None)
            if sticky_proxy is not None:
                self._proxy_tasks.pop(sticky_proxy, None)
                if self._lease_owners.get(sticky_proxy) in (None, task_id):
                    self._lease_owners.pop(sticky_proxy, None)
                    self._leased.discard(sticky_proxy)
                if sticky_proxy not in self._cooldown_dict:
                    self._factory_proxies.discard(sticky_proxy)

            owned_proxies = [
                proxy for proxy, owner in self._lease_owners.items() if owner == task_id
            ]
            for proxy in owned_proxies:
                self._lease_owners.pop(proxy, None)
                self._leased.discard(proxy)
                if proxy not in self._cooldown_dict and proxy not in self._factory_proxies:
                    self._enqueue(proxy)

    async def _cooldown_sweeper(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.sweep_interval)
                now = time.monotonic()
                async with self._lock:
                    for proxy, release_at in list(self._cooldown_dict.items()):
                        if now >= release_at:
                            del self._cooldown_dict[proxy]
                            if proxy not in self._factory_proxies:
                                self._enqueue(proxy)
                            else:
                                self._factory_proxies.discard(proxy)
                                self._proxy_tasks.pop(proxy, None)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Unexpected error in proxy cooldown sweeper.")


# Compatibility name; both APIs use the same pool and state.
ProxyManager = ProxyPool