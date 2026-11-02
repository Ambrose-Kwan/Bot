"""
Checkout Framework Package.
Provides asynchronous e-commerce checkout and stock monitoring engines.
"""
import asyncio
from contextlib import asynccontextmanager

# Python 3.10 backward-compatibility polyfill for asyncio.timeout
if not hasattr(asyncio, "timeout"):
    @asynccontextmanager
    async def _poly_timeout(delay):
        if delay is None:
            yield
            return
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        handle = loop.call_later(delay, task.cancel) if task else None
        try:
            yield
        except asyncio.CancelledError:
            raise asyncio.TimeoutError()
        finally:
            if handle:
                handle.cancel()
    asyncio.timeout = _poly_timeout
