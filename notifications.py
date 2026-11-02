import logging
import time
from typing import Any

try:
    from curl_cffi.requests import AsyncSession
except ImportError:
    AsyncSession = Any

logger = logging.getLogger("Notifications")

class DiscordNotifier:
    """Discord Webhook 推播器"""

    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url
        self._session: AsyncSession | None = None

    async def _get_session(self) -> AsyncSession:
        if self._session is None:
            self._session = AsyncSession(impersonate="chrome131", timeout=8.0)
        return self._session

    async def send_cart_success(
        self,
        task_id: str,
        product_name: str,
        size: str,
        cart_id: str,
        proxy_host: str,
        latency_ms: float,
        product_url: str | None = None,
        image_url: str | None = None,
    ) -> bool:
        """發送加購成功高優先級推播通知"""
        if not self.webhook_url:
            logger.info(f"📢 [Discord 模擬推播] Task {task_id} 加購成功! {product_name} (Size: {size}) | CartID: {cart_id[:12]}...")
            return True

        embed = {
            "title": "🎉 [SUCCESS] Vans 搶購成功加入購物車！",
            "description": f"**任務編號**: `{task_id}` 已成功鎖定庫存並加入購物車。",
            "url": product_url or "https://www.vans.com",
            "color": 0x10B981,  # 綠色
            "fields": [
                {"name": "👟 商品名稱", "value": product_name, "inline": False},
                {"name": "📏 尺碼 (Size)", "value": f"`{size}`", "inline": True},
                {"name": "⚡ 反應延遲", "value": f"`{latency_ms:.1f} ms`", "inline": True},
                {"name": "🌐 綁定代理", "value": f"`{proxy_host}`", "inline": True},
                {"name": "🛒 Cart ID", "value": f"`{cart_id}`", "inline": False},
            ],
            "footer": {
                "text": "Vans Async Task Pool Engine • Powered by curl_cffi",
                "icon_url": "https://assets.vans.com/images/favicon.ico"
            },
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        if image_url:
            embed["thumbnail"] = {"url": image_url}

        payload = {
            "username": "Vans Snatcher Bot",
            "avatar_url": "https://assets.vans.com/images/favicon.ico",
            "content": f"🚨 **搶購成功警報** | Task `{task_id}` 成功捕獲尺碼 `{size}`！",
            "embeds": [embed]
        }

        return await self._dispatch_webhook(payload)

    async def send_rate_limit_alert(self, task_id: str, proxy: str, cooldown_sec: float) -> bool:
        """發送 429 熔斷警告"""
        if not self.webhook_url:
            return True

        payload = {
            "username": "Vans Snatcher Sentinel",
            "embeds": [{
                "title": "⚠️ [CIRCUIT BREAKER] 遭遇 Cloudflare 429 風控",
                "description": f"任務 `{task_id}` 綁定之代理 IP 遭暫時限制，已自動觸發熔斷並切換備用節點。",
                "color": 0xF59E0B,  # 黃色
                "fields": [
                    {"name": "受限 IP", "value": f"`{proxy}`", "inline": True},
                    {"name": "隔離冷卻", "value": f"`{cooldown_sec:.1f} 秒`", "inline": True},
                ],
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }]
        }
        return await self._dispatch_webhook(payload)

    async def _dispatch_webhook(self, payload: dict[str, Any]) -> bool:
        try:
            session = await self._get_session()
            headers = {"Content-Type": "application/json"}
            resp = await session.post(self.webhook_url, json=payload, headers=headers)
            if resp.status_code in (200, 204):
                logger.info("✅ Discord Webhook 推播成功")
                return True
            else:
                logger.warning(f"⚠️ Discord Webhook 回應異常 HTTP {resp.status_code}: {resp.text[:100]}")
                return False
        except Exception as e:  # noqa: BLE001 - webhook boundary reports failures without interrupting checkout
            logger.error(f"❌ Discord Webhook 發送失敗: {e}")
            return False

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None
