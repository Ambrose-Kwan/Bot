from __future__ import annotations

import asyncio
import logging
import math
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any

from pydantic import Field

# 標準規範匯入，支援套件內相對匯入與獨立執行之絕對匯入
try:
    from ..core.events import StockEventPayload
    from ..core.exceptions import (
        CartCheckoutError,
        CheckoutException,
        InvalidStateTransitionError,
        OutOfStockError,
        PaymentDeclinedError,
        ProductUnavailableError,
        RateLimitError,
        ServiceUnavailableError,
        WAFChallengeError,
    )
    from ..core.network import RobustAsyncSession
    from ..core.state_machine import AsyncStateMachine, CheckoutState
    from ..models.profile import BuyerProfile, ShippingAddress, TaskConfig
    from .base_site import BaseSite, CartResult, CheckoutResult, StockStatus
except (ImportError, ValueError):
    from core.events import StockEventPayload
    from core.exceptions import (
        CartCheckoutError,
        CheckoutException,
        InvalidStateTransitionError,
        OutOfStockError,
        PaymentDeclinedError,
        ProductUnavailableError,
        RateLimitError,
        ServiceUnavailableError,
        WAFChallengeError,
    )
    from core.network import RobustAsyncSession
    from core.state_machine import AsyncStateMachine, CheckoutState
    from models.profile import BuyerProfile, ShippingAddress, TaskConfig
    from sites.base_site import BaseSite, CartResult, CheckoutResult, StockStatus

# 輔助模組載入（若未安裝對應函式庫則優雅降級）
try:
    from ..utils.browser_handoff import handoff_to_browser
except (ImportError, ValueError):
    try:
        from utils.browser_handoff import handoff_to_browser
    except ImportError:
        handoff_to_browser = None

try:
    from ..utils.token_harvester import HybridTokenHarvesterPool
except (ImportError, ValueError):
    try:
        from utils.token_harvester import HybridTokenHarvesterPool
    except ImportError:
        HybridTokenHarvesterPool = None  # type: ignore

logger = logging.getLogger("nexus.sites.vans")


def _size_aliases(size_label: str) -> list[str]:
    """將 Vans 官網標註的複合尺碼標籤解析為精確的匹配 Alias 清單。"""
    aliases: list[str] = []
    for segment in re.split(r"\s*=\s*", size_label):
        segment = segment.strip()
        if not segment:
            continue
        aliases.append(segment)
        aliases.append(re.sub(r"^US\s+", "", segment, flags=re.IGNORECASE))
        size_token = re.match(r"^(?:US\s+)?(\d+(?:\.\d+)?)\b", segment, re.IGNORECASE)
        if size_token:
            numeric_size = size_token.group(1)
            aliases.extend((numeric_size, f"US {numeric_size}"))
    return list(dict.fromkeys(alias for alias in aliases if alias))


@dataclass
class VansVariant:
    """Vans 特定商品尺碼規格與變體模型。"""
    product_id: str
    upc: str = ""
    size_label: str = ""
    in_stock: bool = True
    available_qty: int = 10
    max_qty: int = 10
    price: float | None = None
    image_url: str | None = None


class VansStockResult(StockStatus):
    """結構化 Vans 庫存響應，相容 BaseSite.StockStatus。"""
    variants: list[VansVariant] = Field(default_factory=list)
    product_title: str = ""
    style_code: str = ""


def rank_available_variants(
    preferred_sizes: list[str],
    variants_by_alias: dict[str, VansVariant],
) -> list[VansVariant]:
    """
    根據使用者設定的偏好尺碼陣列，將現有有庫存的變體按照優先級嚴格排序。
    若包含 'ANY_SIZE'，則將其餘尚未加入的現貨變體接續追加。
    """
    ordered: list[VansVariant] = []
    seen_ids: set[str] = set()

    for pref in preferred_sizes:
        if pref == "ANY_SIZE":
            for v in variants_by_alias.values():
                if v.in_stock and v.product_id not in seen_ids:
                    ordered.append(v)
                    seen_ids.add(v.product_id)
        elif pref in variants_by_alias:
            v = variants_by_alias[pref]
            if v.in_stock and v.product_id not in seen_ids:
                ordered.append(v)
                seen_ids.add(v.product_id)

    return ordered


def select_best_variant(
    preferred_sizes: list[str],
    stock_matrix: dict[str, str],
) -> str | None:
    """Return the first available variant ID in the requested size priority order."""
    for size in preferred_sizes:
        if size == "ANY_SIZE" and stock_matrix:
            return next(iter(stock_matrix.values()))
        if size in stock_matrix:
            return stock_matrix[size]
    return None


class VansSite(BaseSite):
    """
    生產級 Vans (VF Corporation) 專屬電商站點適配器。
    全面繼承 BaseSite，整合非同步連線池、FSM 狀態機與反爬動態對抗。
    """

    BASE_URL: str = "https://www.vans.com"
    API_CART_URL: str = "https://www.vans.com/api/cart/v1/item?locale=en-us"
    API_INVENTORY_URL: str = (
        "https://www.vans.com/api/products/v1/sku/{style_code}?locale=en-us"
    )
    API_CHECKOUT_URL: str = "https://www.vans.com/api/checkout/v1/orders?locale=en-us"

    DEFAULT_GATEWAY_PUBLIC_KEY: str = (
        "-----BEGIN PUBLIC KEY-----\n"
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAr3NGroczLRRLim/ObKeX\n"
        "B60q2xHcE3h8wPLucIv9aupqkQbclWaKJ50KWavTgMFIhgmgxYMlYXd0ecSVRiHJ\n"
        "L+vIIMXpOTxXCjmn9c5uU964cr7ErO3TZnZiA6hYyddmghBqkjVwIoz1dv2BYBDi\n"
        "JNgGeJAeuj+mhIFZcw6jVhq3qC0GH+SaFislcUjyJAK7s4NHuPm5UdUdC4aVOsPA\n"
        "RgPKL26Ce6ZsP0eNYFPHinHSqXZ+bmAq4AIaxUcEQ5VltLPG80tf9PbiN9aTISmS\n"
        "GdFPjqg/oU5wrxQ3a6RtCsAOI9zfr6V2mAAXic163gEN38THggevqWOg6FYt4hwI\n"
        "2QIDAQAB\n"
        "-----END PUBLIC KEY-----\n"
    )

    def __init__(
        self,
        task_config: TaskConfig,
        profile: BuyerProfile,
        session: RobustAsyncSession | None = None,
        state_machine: AsyncStateMachine | None = None,
        token_harvester: Any | None = None,
        event_queue: asyncio.Queue[StockEventPayload] | None = None,
        request_timeout: float = 15.0,
        max_waf_retries: int = 1,
    ) -> None:
        super().__init__(task_config, profile, session)
        self.request_timeout = request_timeout
        # 若未單獨傳入 state_machine，自動綁定 BaseSite 原生初始化的 self.fsm
        self.state_machine = state_machine or getattr(self, "fsm", None)
        self.token_harvester = token_harvester
        self.event_queue = event_queue
        self.max_waf_retries = max_waf_retries

        # 運行狀態快取
        self.csrf_token: str | None = None
        self.cart_id: str | None = None
        self.active_variant: VansVariant | None = None
        self._ranked_variants: list[VansVariant] = []  # 具備優先級順序的候選尺碼清單
        self.is_prewarmed: bool = False
        self._staged_shipping: dict[str, Any] | None = None

        self._static_headers: dict[str, str] = {
            "siteid": "VANS-US",
            "source": "ECOM15",
            "channel": "ECOMM",
            "locale": "en_US",
            "brand": "VANS",
            "region": "NORA",
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
        }

    def _get_gateway_public_key(self) -> str:
        """獲取金流閘道公鑰，支援從 config 注入或使用預設憑證。"""
        key = getattr(self.config, "gateway_public_key", None)
        if key and isinstance(key, str) and "BEGIN PUBLIC KEY" in key:
            return key
        return self.DEFAULT_GATEWAY_PUBLIC_KEY

    def _build_headers(
        self,
        referer: str | None = None,
        custom_correlation_id: str | None = None,
    ) -> dict[str, str]:
        """組裝包含動態 UUIDv4 交易追蹤號與 CSRF Token 的標準 HTTP 標頭。"""
        corr_id = custom_correlation_id or str(uuid.uuid4())
        headers = dict(self._static_headers)
        headers["x-transaction-id"] = corr_id
        headers["x-correlation-id"] = corr_id

        if referer:
            headers["Referer"] = referer
        elif getattr(self.config, "target_url", None):
            headers["Referer"] = self.config.target_url

        if self.csrf_token:
            headers["x-csrf-token"] = self.csrf_token

        return headers

    @staticmethod
    def _response_json(response: Any, action: str) -> dict[str, Any]:
        """安全解析 JSON 回應，精準攔截 WAF HTML 挑戰封包以防解析崩潰。"""
        try:
            data = response.json()
        except (ValueError, TypeError, RuntimeError) as exc:
            snippet = getattr(response, "text", "")[:200]
            if "<html" in snippet.lower():
                raise WAFChallengeError(
                    f"Vans {action} 遭 WAF 攔截並回傳 HTML 挑戰頁面",
                    status_code=getattr(response, "status_code", 403),
                ) from exc
            raise ServiceUnavailableError(
                f"Vans {action} 回傳無效 JSON 格式",
                status_code=getattr(response, "status_code", 502),
            ) from exc

        if not isinstance(data, dict):
            raise ServiceUnavailableError(
                f"非預期的 Vans {action} 回應結構",
                status_code=getattr(response, "status_code", 502),
            )
        return data

    async def _transition_fsm(self, state: CheckoutState, reason: str | None = None) -> None:
        """
        非同步推動狀態機狀態變更。
        保證狀態轉移嚴格性：具備冪等性與終端保護，且不再吞掉非法轉移例外。
        """
        sm = self.state_machine or getattr(self, "fsm", None)
        if not sm or not hasattr(sm, "transition"):
            return

        current = getattr(sm, "current_state", None)
        # 1. 冪等防護：已處於目標狀態則無需重複轉移
        if current == state:
            return

        # 2. 終端防護：已是終端狀態則不再允許狀態變更（例如已 SUCCESS 則不能再轉 FAILED）
        if getattr(sm, "is_terminal", False):
            raise InvalidStateTransitionError(
                from_state=getattr(current, "value", str(current)),
                to_state=state.value,
                task_id=self.task_id,
            )

        try:
            await sm.transition(state, reason=reason)
        except Exception as exc:
            logger.error(f"[{self.task_id}] ❌ FSM 狀態轉移非法 ({current} -> {state}): {exc}")
            raise

    async def _handle_waf_challenge(self, origin_state: CheckoutState) -> bool:
        """
        Treat HTTP 403 / WAF responses as a stop signal; do not solve or retry them.
        """
        logger.warning(
            "[%s] HTTP 403/WAF response at %s; stopping task without challenge solving.",
            self.task_id,
            origin_state.value,
        )
        await self._transition_fsm(
            CheckoutState.FAILED,
            reason=f"HTTP 403/WAF stop at {origin_state.value}",
        )
        return False

    async def prewarm(self) -> None:
        """預熱連線並抓取初始 CSRF/Cart 憑證。"""
        await self.initialize_session()
        self.is_prewarmed = True

    async def initialize_session(self) -> None:
        """造訪官網首頁提取 Meta CSRF Token 並向微服務索取 Cart ID。"""
        for attempt in range(self.max_waf_retries + 1):
            start_time = time.monotonic()
            try:
                target_url = getattr(self.config, "target_url", None) or f"{self.BASE_URL}/en-us"
                response = await self.session.execute_with_retry(
                    "GET",
                    target_url,
                    headers={"Accept": "text/html"},
                    timeout=self.request_timeout,
                )
                if response.status_code == 403:
                    raise WAFChallengeError("首頁連線遭 WAF 阻擋 (HTTP 403)", task_id=self.task_id)
                if response.status_code < 200 or response.status_code >= 300:
                    raise ServiceUnavailableError(
                        f"Vans 首頁連線回傳 HTTP {response.status_code}",
                        status_code=response.status_code,
                        task_id=self.task_id,
                    )

                # 提取 CSRF Token (支援 Header 與 HTML Meta 標籤)
                csrf = response.headers.get("x-csrf-token") or response.headers.get("x-xsrf-token")
                if not csrf and response.text:
                    match = re.search(
                        r'<meta\s+[^>]*name=["\'](?:csrf-token|_csrf|csrf)["\'][^>]*'
                        r'content=["\']([^"\']+)["\']',
                        response.text,
                        re.IGNORECASE,
                    )
                    if not match:
                        match = re.search(
                            r'<meta\s+[^>]*content=["\']([^"\']+)["\'][^>]*'
                            r'name=["\'](?:csrf-token|_csrf|csrf)["\']',
                            response.text,
                            re.IGNORECASE,
                        )
                    if match:
                        csrf = match.group(1)

                if csrf:
                    self.csrf_token = csrf

                # 初始化購物車以取得動態 cartId
                cart_response = await self.session.execute_with_retry(
                    "GET",
                    self.API_CART_URL,
                    headers=self._build_headers(referer=self.BASE_URL),
                    timeout=self.request_timeout,
                )
                if cart_response.status_code == 403:
                    raise WAFChallengeError("購物車端點遭 WAF 阻擋 (HTTP 403)", task_id=self.task_id)
                if cart_response.status_code not in (200, 201):
                    raise CartCheckoutError(
                        f"購物車初始化失敗 (HTTP {cart_response.status_code})",
                        task_id=self.task_id,
                    )
                cart_data = self._response_json(cart_response, "cart initialization")
                cart_id = cart_data.get("cartId") or cart_data.get("id")
                if not isinstance(cart_id, str) or not cart_id.strip():
                    raise CartCheckoutError("伺服器未回傳有效 Cart ID", task_id=self.task_id)

                self.cart_id = cart_id
                latency = round((time.monotonic() - start_time) * 1000, 1)
                logger.info(f"[{self.task_id}] Vans 會話與 Cart ID 初始化完成 ({latency}ms) | ID: {self.cart_id}")
                return
            except WAFChallengeError:
                if attempt < self.max_waf_retries and await self._handle_waf_challenge(CheckoutState.INIT):
                    continue
                raise
            except asyncio.CancelledError:
                raise
            except CheckoutException:
                raise
            except Exception as exc:
                raise ServiceUnavailableError(f"會話初始化異常: {exc}", task_id=self.task_id) from exc

    def apply_stock_signal(self, payload: StockEventPayload) -> None:
        """供獨立監控節點廣播時直接注入現貨變體資訊，達到零延遲喚醒。"""
        raw = getattr(payload, "raw_response", None) or {}
        upc = str(raw.get("upc") or "")
        max_qty = int(raw.get("maxQty") or 10)
        image_url = raw.get("productImageURL") or ""

        variant = VansVariant(
            product_id=payload.variant_id,
            upc=upc,
            size_label=payload.size_label,
            in_stock=True,
            price=payload.price,
            max_qty=max_qty,
            image_url=image_url,
        )
        self.active_variant = variant
        self._ranked_variants = [variant]
        logger.info(
            f"⚡ [{self.task_id}] 收到外部庫存廣播信號！鎖定變體: {payload.variant_id} ({payload.size_label})"
        )

    async def check_stock(self) -> StockStatus:
        """
        檢查商品庫存。支援三種獲取途徑：
        1. 外部信號佇列非同步等待 (event_queue)
        2. 已由 apply_stock_signal 注入之快取
        3. 透過電商端點主動查詢庫存並計算優先級矩陣
        """
        # 途徑 1: 訂閱佇列等待通知
        if self.event_queue is not None:
            logger.info(f"[{self.task_id}] 會話預熱完成，非阻塞等待監控端發布庫存廣播...")
            payload = await self.event_queue.get()
            self.apply_stock_signal(payload)

        # 途徑 2: 已鎖定變體
        if self.active_variant and self.active_variant.in_stock:
            return StockStatus(
                in_stock=True,
                variant_id=self.active_variant.product_id,
                price=self.active_variant.price,
                title="",
                available_sizes=[self.active_variant.size_label],
                raw_response={"source": "external_signal"},
            )

        # 途徑 3: 主動向官網端點發送請求
        sku_id = getattr(self.config, "sku_or_variant_id", "")
        style_code = sku_id.replace("-", "").upper()
        if not style_code.startswith("VN"):
            style_code = f"VN{style_code}"
        endpoint = self.API_INVENTORY_URL.format(style_code=style_code)
        headers = self._build_headers(referer=getattr(self.config, "target_url", None))

        for attempt in range(self.max_waf_retries + 1):
            try:
                response = await self.session.execute_with_retry(
                    "GET",
                    endpoint,
                    headers=headers,
                    timeout=self.request_timeout,
                )
                if response.status_code == 403:
                    raise WAFChallengeError("庫存查詢遭 WAF 阻擋 (HTTP 403)", task_id=self.task_id)
                if response.status_code != 200:
                    raise ProductUnavailableError(
                        f"庫存查詢端點回傳 HTTP {response.status_code}",
                        task_id=self.task_id,
                    )

                data = self._response_json(response, "inventory")
                product = data.get("product", data)
                variants = product.get("variants", data.get("variants", []))
                if not isinstance(variants, list):
                    raise TypeError("商品變體列表格式錯誤")

                variants_by_alias: dict[str, VansVariant] = {}
                for item in variants:
                    if not isinstance(item, dict):
                        continue
                    size_label = str(item.get("size") or item.get("sizeDescription") or "").strip()
                    available_qty = int(item.get("availableQuantity") or 0)
                    in_stock = bool(item.get("inStock", False) or available_qty > 0)
                    raw_price = item.get("price", product.get("price"))
                    price = float(raw_price) if raw_price is not None else 0.0
                    variant = VansVariant(
                        product_id=str(item.get("productId") or ""),
                        upc=str(item.get("upc") or ""),
                        size_label=size_label,
                        in_stock=in_stock,
                        available_qty=available_qty,
                        max_qty=int(item.get("maxQty") or 10),
                        price=price,
                        image_url=item.get("productImageURL"),
                    )
                    if not variant.product_id:
                        continue
                    if in_stock:
                        for alias in _size_aliases(size_label) or [variant.product_id]:
                            variants_by_alias.setdefault(alias, variant)

                # 提取優先尺碼陣列
                priority = getattr(self.config, "variant_priority", None)
                preferred_sizes = getattr(priority, "preferred_sizes", []) if priority else []
                if not preferred_sizes and getattr(self.config, "target_size", None):
                    preferred_sizes = [self.config.target_size]
                if not preferred_sizes:
                    preferred_sizes = ["ANY_SIZE"]

                # 建立所有可用變體的降級排序清單
                self._ranked_variants = rank_available_variants(preferred_sizes, variants_by_alias)
                self.active_variant = self._ranked_variants[0] if self._ranked_variants else None

                product_price = product.get("price")
                selected_price = (
                    self.active_variant.price if self.active_variant is not None else
                    (float(product_price) if product_price is not None else None)
                )
                return StockStatus(
                    in_stock=self.active_variant is not None,
                    variant_id=self.active_variant.product_id if self.active_variant else None,
                    price=selected_price,
                    title=str(product.get("title") or ""),
                    available_sizes=[
                        str(item.get("size") or item.get("sizeDescription") or "")
                        for item in variants
                        if isinstance(item, dict)
                        and (bool(item.get("inStock", False)) or int(item.get("availableQuantity") or 0) > 0)
                    ],
                    raw_response=data,
                )
            except WAFChallengeError:
                if attempt < self.max_waf_retries and await self._handle_waf_challenge(CheckoutState.MONITOR):
                    continue
                raise
            except asyncio.CancelledError:
                raise
            except CheckoutException:
                raise
            except Exception as exc:
                raise ProductUnavailableError(f"庫存查詢失敗: {exc}", task_id=self.task_id) from exc

        raise ProductUnavailableError("耗盡 WAF 重試次數，無法獲取庫存", task_id=self.task_id)

    async def add_to_cart(self) -> CartResult:
        """
        提交加購請求。
        關鍵改進：若第一首選尺碼加購時已被搶空，自動依序嘗試候選陣列中的下一個尺碼。
        """
        if not self._ranked_variants and self.active_variant:
            self._ranked_variants = [self.active_variant]

        if not self._ranked_variants:
            raise OutOfStockError("無任何可用的有庫存變體可供加購", task_id=self.task_id)
        if not self.cart_id:
            raise CartCheckoutError("購物車 ID 尚未初始化", task_id=self.task_id)

        quantity = getattr(self.config, "quantity", 1)
        headers = self._build_headers(referer=getattr(self.config, "target_url", None))

        # 遍歷所有候選變體，實現加購級別的尺碼自動降級
        for candidate in list(self._ranked_variants):
            self.active_variant = candidate
            payload = [
                {
                    "productId": candidate.product_id,
                    "qty": quantity,
                    "upc": candidate.upc,
                    "maxQty": candidate.max_qty,
                    "productImageURL": candidate.image_url or "",
                    "cartId": self.cart_id,
                }
            ]
            logger.info(
                f"[{self.task_id}] 正在嘗試加購尺碼: {candidate.size_label} (ID: {candidate.product_id})..."
            )

            for attempt in range(self.max_waf_retries + 1):
                try:
                    response = await self.session.execute_with_retry(
                        "POST",
                        self.API_CART_URL,
                        json=payload,
                        headers=headers,
                        timeout=self.request_timeout,
                    )
                    if response.status_code == 403:
                        raise WAFChallengeError("加購端點遭 WAF 阻擋 (HTTP 403)", task_id=self.task_id)

                    # 偵測售罄拒絕訊號
                    if response.status_code not in (200, 201):
                        details = getattr(response, "text", "")[:200]
                        if response.status_code in (400, 409) and any(
                            marker in details.casefold()
                            for marker in ("out of stock", "sold out", "unavailable", "inventory")
                        ):
                            logger.warning(
                                f"[{self.task_id}] ⚠️ 尺碼 {candidate.size_label} 剛剛售罄，自動切換至下一個次選尺碼..."
                            )
                            break  # 跳出 WAF 重試，嘗試下一個候選尺碼

                        raise CartCheckoutError(
                            f"加購被拒 (HTTP {response.status_code}): {details}",
                            task_id=self.task_id,
                        )

                    data = self._response_json(response, "add-to-cart")
                    server_cart_id = data.get("cartId") or data.get("id") or self.cart_id
                    self.cart_id = str(server_cart_id)
                    logger.info(
                        f"🛒 [{self.task_id}] 加購成功！已鎖定尺寸: {candidate.size_label} | Cart ID: {self.cart_id}"
                    )
                    return CartResult(success=True, cart_id=self.cart_id)

                except WAFChallengeError:
                    if attempt < self.max_waf_retries and await self._handle_waf_challenge(CheckoutState.ATC):
                        continue
                    raise

        raise OutOfStockError("偏好清單中的所有尺碼均已售罄", task_id=self.task_id)

    async def submit_shipping(self) -> bool:
        """
        提交並暫存配送資料，套用防砍單地址擾動變形演算法 (Address Jigging)。
        """
        if not self.cart_id:
            raise CartCheckoutError("購物車尚未建立，無法提交配送資訊", task_id=self.task_id)

        shipping = self.profile.shipping
        if not shipping.address1 or not shipping.city or not shipping.postal_code:
            logger.error(f"[{self.task_id}] 配送地址資料不完整")
            return False

        if hasattr(shipping, "generate_jigged_address"):
            final_shipping: ShippingAddress = shipping.generate_jigged_address()
            logger.info(
                f"[{self.task_id}] 啟動防砍單地址變形: {shipping.address1} -> {final_shipping.address1}"
            )
        else:
            final_shipping = shipping

        self._staged_shipping = {
            "customer": {
                "email": final_shipping.email,
                "firstName": final_shipping.first_name,
                "lastName": final_shipping.last_name,
                "phone": final_shipping.phone,
            },
            "shippingAddress": {
                "firstName": final_shipping.first_name,
                "lastName": final_shipping.last_name,
                "address1": final_shipping.address1,
                "address2": final_shipping.address2 or "",
                "city": final_shipping.city,
                "state": final_shipping.state_code,
                "postalCode": final_shipping.postal_code,
                "country": getattr(final_shipping, "country_code", "US"),
                "phone": final_shipping.phone,
            },
            "billingAddressSameAsShipping": True,
            "shippingMethod": {
                "code": "STANDARD_GROUND",
                "carrier": "FEDEX",
                "rate": 0.0,
            },
        }
        logger.info(f"[{self.task_id}] 配送資料暫存就緒: {final_shipping.city}, {final_shipping.state_code}")
        return True

    async def submit_checkout(self) -> CheckoutResult:
        """
        執行結帳付款：
        1. 信用卡敏感欄位透過 RSA-OAEP + AES-GCM (CSE) 前端公鑰加密。
        2. 辨識發卡銀行 3D Secure 跳轉挑戰。
        3. 調用 Playwright 注入連線 Cookies 彈出視窗完成 OTP 授權並捕捉訂單號。
        """
        if not self.cart_id:
            raise CheckoutException("缺少購物車資訊", task_id=self.task_id)
        if not self._staged_shipping and not await self.submit_shipping():
            raise CartCheckoutError("配送資料不完整，結帳中止", task_id=self.task_id)

        payment = self.profile.payment
        masked_card = payment.masked_card_number

        logger.info(f"[{self.task_id}] 正在對信用卡資訊執行客戶端公鑰加密 (CSE)...")
        gateway_key = self._get_gateway_public_key()
        cse_payload = payment.encrypt_cse(gateway_key)

        payload: dict[str, Any] = {
            "cartId": self.cart_id,
            **self._staged_shipping,
            "payment": {
                "type": "CREDIT_CARD",
                "cardHolderName": payment.cardholder_name,
                "encryptedCardNumber": cse_payload["encryptedCardNumber"],
                "encryptedExpiryMonth": cse_payload["encryptedExpiryMonth"],
                "encryptedExpiryYear": cse_payload["encryptedExpiryYear"],
                "encryptedSecurityCode": cse_payload["encryptedSecurityCode"],
                "generationtime": cse_payload["generationtime"],
            },
            "browserInfo": {
                "acceptHeader": "*/*",
                "colorDepth": 24,
                "language": "en-US",
                "screenHeight": 1080,
                "screenWidth": 1920,
                "timeZoneOffset": 0,
                "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            },
            "metadata": {
                "source": "ECOM15",
                "clientTimestamp": int(time.time() * 1000),
            },
        }

        headers = self._build_headers(referer=f"{self.BASE_URL}/en-us/checkout")

        for attempt in range(self.max_waf_retries + 1):
            start_time = time.monotonic()
            logger.info(f"[{self.task_id}] 發送結帳 Payload 至 {self.API_CHECKOUT_URL} | 卡號: {masked_card}")

            try:
                response = await self.session.execute_with_retry(
                    "POST",
                    self.API_CHECKOUT_URL,
                    json=payload,
                    headers=headers,
                    timeout=self.request_timeout,
                    allow_redirects=False,
                )
                latency = round((time.monotonic() - start_time) * 1000, 2)
                status_code = response.status_code

                if status_code == 429:
                    retry_after_header = (
                        response.headers.get("Retry-After")
                        or response.headers.get("retry-after")
                    )
                    retry_after = RobustAsyncSession._retry_after_seconds(
                        retry_after_header
                    )
                    max_retry_after = getattr(
                        self.session, "max_retry_after", 300.0
                    )
                    if (
                        not isinstance(max_retry_after, (int, float))
                        or not math.isfinite(max_retry_after)
                        or max_retry_after <= 0
                    ):
                        max_retry_after = 300.0

                    if retry_after is None or retry_after <= 0:
                        retry_after = min(2.0, max_retry_after)
                    else:
                        retry_after = min(retry_after, max_retry_after)
                    raise RateLimitError("觸發結帳端點限流", retry_after=retry_after, task_id=self.task_id)

                if status_code == 403:
                    raise WAFChallengeError("結帳請求遭 WAF 阻擋 (HTTP 403)", task_id=self.task_id)

                # -------------------------------------------------------------
                # 判斷結果類型 1: 3D Secure / OTP 銀行核身挑戰 (3DS Challenge)
                # -------------------------------------------------------------
                is_3ds = False
                acs_target_url: str | None = None

                # 情況 A: HTTP 30x Location 跳轉
                if status_code in (301, 302, 303, 307, 308):
                    location = response.headers.get("Location")
                    if location:
                        is_3ds = True
                        acs_target_url = location

                # 情況 B: JSON 宣告 3DS 挑戰
                raw_data: dict[str, Any] = {}
                if not is_3ds:
                    raw_data = self._response_json(response, "checkout")
                    status_str = str(raw_data.get("status") or "").upper()
                    three_ds_data = raw_data.get("threeDSecure") if isinstance(raw_data.get("threeDSecure"), dict) else {}
                    explicit_acs = raw_data.get("redirectUrl") or three_ds_data.get("acsUrl")

                    if explicit_acs and (
                        status_str in ("3DS_REQUIRED", "CHALLENGE_REQUIRED", "PENDING_AUTHENTICATION")
                        or status_code == 202
                    ):
                        is_3ds = True
                        acs_target_url = explicit_acs

                if is_3ds and acs_target_url:
                    logger.warning(f"[{self.task_id}] ⚡ 偵測到 3D Secure 驗證挑戰！目標 ACS: {acs_target_url}")
                    await self._transition_fsm(CheckoutState.AWAIT_3DS, reason="ACS 3DS Challenge triggered")

                    if handoff_to_browser is None:
                        raise PaymentDeclinedError(
                            reason="遭遇 3DS 挑戰，但 Playwright 模組未安裝",
                            task_id=self.task_id,
                        )

                    proxy_obj = getattr(self.config, "bound_proxy", None)
                    proxy_url = getattr(proxy_obj, "formatted_url", None)

                    # 喚起有頭瀏覽器，注入會話並供持卡人輸入驗證碼
                    handoff_res = await handoff_to_browser(
                        curl_session=self.session,
                        target_url=acs_target_url,
                        proxy=proxy_url,
                        headless=False,
                        timeout_sec=180.0,
                        success_url_substrings=["order-confirmation", "orders/", "thank-you", "receipt"],
                    )

                    if handoff_res.get("success", False):
                        order_id = handoff_res.get("order_id")
                        if not order_id or not str(order_id).strip():
                            logger.error(f"[{self.task_id}] ❌ 3DS 完成但未捕獲到訂單號")
                            await self._transition_fsm(CheckoutState.FAILED, reason="3DS missing order ID")
                            return CheckoutResult(success=False, error_message="3DS missing order ID")

                        logger.info(f"[{self.task_id}] 🏆 3DS 驗證通過！最終訂單成立: {order_id}")
                        await self._transition_fsm(CheckoutState.SUCCESS, reason="3DS Verification Passed")
                        return CheckoutResult(success=True, order_id=str(order_id))
                    else:
                        await self._transition_fsm(CheckoutState.FAILED, reason="3DS Verification Timed Out")
                        raise PaymentDeclinedError(reason="3DS 驗證超時或使用者關閉視窗", task_id=self.task_id)

                # -------------------------------------------------------------
                # 判斷結果類型 2: 直接扣款成功 (Direct Success)
                # -------------------------------------------------------------
                status_str = str(raw_data.get("status") or "").upper()
                if status_code in (200, 201) and status_str in ("ORDER_PLACED", "SUCCESS", "COMPLETED"):
                    order_id = raw_data.get("orderNumber") or raw_data.get("orderId")
                    if order_id and str(order_id).strip():
                        logger.info(f"[{self.task_id}] 🎉 結帳成功！訂單號: {order_id} ({latency}ms)")
                        await self._transition_fsm(CheckoutState.SUCCESS, reason="Order Placed Directly")
                        return CheckoutResult(success=True, order_id=str(order_id))

                # -------------------------------------------------------------
                # 判斷結果類型 3: 付款失敗 / 風控拒付 (Declined)
                # -------------------------------------------------------------
                err_msg = (
                    raw_data.get("message")
                    or raw_data.get("errorMessage")
                    or raw_data.get("error")
                    or f"HTTP {status_code}"
                )
                logger.error(f"[{self.task_id}] ❌ 結帳被拒: {err_msg}")
                await self._transition_fsm(CheckoutState.FAILED, reason=f"Checkout declined: {err_msg}")
                return CheckoutResult(success=False, error_message=str(err_msg))

            except WAFChallengeError:
                if attempt < self.max_waf_retries and await self._handle_waf_challenge(CheckoutState.PAYMENT):
                    continue
                raise
            except asyncio.CancelledError:
                raise
            except CheckoutException:
                raise
            except Exception as exc:  # noqa: BLE001 - checkout adapter returns a structured failure
                logger.error(f"[{self.task_id}] 結帳提交發生非預期異常: {exc}")
                await self._transition_fsm(CheckoutState.FAILED, reason=f"Unexpected error: {exc}")
                return CheckoutResult(success=False, error_message=str(exc))

        raise CheckoutException("耗盡 WAF 重試次數，結帳失敗", task_id=self.task_id)

    async def execute_fast_checkout(self) -> CheckoutResult:
        """
        便捷方法：跳過單獨輪詢，直接並發執行加購、配送與結帳閉環。
        嚴格將 FSM 依序推進：MONITOR -> ATC -> SHIPPING -> PAYMENT -> SUCCESS/FAILED。
        """
        sm = self.state_machine or getattr(self, "fsm", None)
        current = getattr(sm, "current_state", None)

        try:
            # 1. 推進至 ATC (若仍停留在 INIT，先轉入 MONITOR 保持狀態機圖路徑合法)
            if current == CheckoutState.INIT:
                await self._transition_fsm(CheckoutState.MONITOR, reason="Stock signal received")
            await self._transition_fsm(CheckoutState.ATC, reason="Burst ATC started")

            cart_res = await self.add_to_cart()
            if not cart_res.success:
                await self._transition_fsm(CheckoutState.FAILED, reason="ATC returned failure")
                return CheckoutResult(success=False, error_message="ATC failed")

            # 2. 推進至 SHIPPING
            await self._transition_fsm(CheckoutState.SHIPPING, reason="Submitting shipping address")
            if not await self.submit_shipping():
                await self._transition_fsm(CheckoutState.FAILED, reason="Shipping address rejected")
                return CheckoutResult(
                    success=False,
                    error_message="Shipping details are incomplete",
                )

            # 3. 推進至 PAYMENT，並呼叫結帳端點
            await self._transition_fsm(CheckoutState.PAYMENT, reason="Submitting payment payload")
            return await self.submit_checkout()

        except CheckoutException as exc:
            if sm and not getattr(sm, "is_terminal", False):
                await self._transition_fsm(CheckoutState.FAILED, reason=str(exc))
            return CheckoutResult(success=False, error_message=str(exc))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - checkout adapter returns a structured failure
            if sm and not getattr(sm, "is_terminal", False):
                await self._transition_fsm(CheckoutState.FAILED, reason=str(exc))
            return CheckoutResult(success=False, error_message=f"Checkout error: {exc}")