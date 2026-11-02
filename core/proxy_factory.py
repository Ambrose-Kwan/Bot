"""Example wiring for task-sticky residential proxies."""

try:
    from .network import RobustAsyncSession
    from .proxy import ProxyPool
except ImportError:
    from network import RobustAsyncSession
    from proxy import ProxyPool


def residential_proxy_factory(session_token: str) -> str:
    return (
        "http://customer-example-session-"
        f"{session_token}:placeholder@proxy.example.com:7777"
    )


proxy_pool = ProxyPool(proxy_factory=residential_proxy_factory)


async def example_requests() -> None:
    async with RobustAsyncSession(
        proxy_pool=proxy_pool,
        task_id="CHECKOUT-USER-001",
        sticky=True,
        rotate_on_retry=True,
    ) as client:
        await client.get("https://example.com/step1")
        await client.post("https://example.com/step2")
