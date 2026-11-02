"""Runnable example for retrying requests with a rotating proxy session."""

from __future__ import annotations

import asyncio

try:
    from .network import RobustAsyncSession
except ImportError:
    from core.network import RobustAsyncSession


async def main() -> None:
    async with RobustAsyncSession(
        proxy_pool=[
            "http://proxy1.internal:8080",
            "http://proxy2.internal:8080",
            "http://proxy3.internal:8080",
        ],
        task_id="SCRAPER-TASK-02",
        sticky=True,           # 成功時固定使用同一個 IP
        rotate_on_retry=True,  # 遇 429 或連線中斷自動換下一個 IP 重試
    ) as client:
        resp = await client.get("https://api.target.com/data")
        _ = resp


if __name__ == "__main__":
    asyncio.run(main())