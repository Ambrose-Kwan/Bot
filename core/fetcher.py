# fetcher.py
import asyncio
import logging

import httpx

try:
    from .proxy import ProxyPool
except ImportError:
    from proxy import ProxyPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Fetcher")

async def fetch_with_retry(
    pool: ProxyPool,
    url: str,
    max_retries: int = 5,
    client_timeout: float = 10.0
) -> httpx.Response | None:
    retries = 0

    while retries < max_retries:
        proxy = await pool.get_proxy()
        proxy_url = f"http://{proxy}" if not proxy.startswith("http") else proxy
        proxy_returned = False

        try:
            async with httpx.AsyncClient(proxy=proxy_url, timeout=client_timeout) as client:
                response = await client.get(url)

                if response.status_code in (429, 403):
                    retry_after = response.headers.get("Retry-After")
                    cooldown = float(retry_after) if retry_after and retry_after.isdigit() else None
                    await pool.report_restricted(proxy, status_code=response.status_code, cooldown=cooldown)
                    proxy_returned = True
                    retries += 1
                    continue

                await pool.report_success(proxy)
                proxy_returned = True
                return response

        except (httpx.RequestError, asyncio.TimeoutError) as err:
            logger.warning(f"Connection failed on {proxy}: {err}")
            await pool.report_restricted(proxy, status_code=500, cooldown=15.0)
            proxy_returned = True
            retries += 1
        finally:
            if not proxy_returned:
                await pool.report_restricted(proxy, status_code=500, cooldown=15.0)

    raise RuntimeError(f"Exceeded max retries ({max_retries}) for URL: {url}")

async def main():
    test_proxies = [
        "127.0.0.1:8080",
        "127.0.0.1:8081",
        "127.0.0.1:8082"
    ]
    pool = ProxyPool(proxies=test_proxies, default_cooldown=30.0)
    await pool.start()

    try:
        resp = await fetch_with_retry(pool, "https://httpbin.org/ip")
        print("Success:", resp.json())
    finally:
        await pool.stop()

if __name__ == "__main__":
    asyncio.run(main())