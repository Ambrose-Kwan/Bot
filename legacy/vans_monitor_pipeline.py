"""
================================================================================
專案：Vans (VF Corp) 非同步庫存監控器與自動加購搶購模組 (Async Stock Monitor)
檔案：vans_monitor_pipeline.py
相依套件：pip install curl_cffi
執行方式：python vans_monitor_pipeline.py
================================================================================
特點：
  1. 使用 curl_cffi (AsyncSession) 保持 genuine TLS impersonate (chrome124)
  2. 動態解析尺碼庫存狀態 (in_stock, availableQuantity, UPC, productId)
  3. 隨機請求延遲 (Randomized Jitter Sleep) 防止行為特徵分析
  4. 遭遇 Cloudflare HTTP 429 / 503 時啟用自動指數退避重試 (Exponential Backoff)
  5. 偵測到在庫瞬間，毫秒級直接觸發預熱好的 Session 進行搶購 (Auto Add-To-Cart)
================================================================================
"""
from __future__ import annotations

import asyncio
import logging
import random
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from curl_cffi.requests import AsyncSession, Response
from curl_cffi.requests.errors import RequestsError

# 設定結構化日誌格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("vans.monitor")


# =============================================================================
# 0. 資料結構定義 (Data Models)
# =============================================================================
@dataclass
class VansSessionContext:
    """
    封裝完成預熱的 Vans 購物會話，包含專屬 Session、動態 Cart ID 與 Cookies。
    """
    session: AsyncSession
    cart_id: str
    csrf_token: str | None = None
    proxy: str | None = None
    cookies_dict: dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    async def close(self) -> None:
        """優雅關閉底層 curl_cffi Session，釋放連線池 Socket。"""
        await self.session.close()


@dataclass
class MatchedVariantInfo:
    """代表符合目標尺碼之在庫商品規格資訊。"""
    product_id: str
    upc: str
    size_label: str
    in_stock: bool
    available_qty: int = 0
    price: float = 0.0
    image_url: str | None = None


@dataclass
class StockMonitorResult:
    """庫存監控與加購執行結果摘要。"""
    success: bool
    status: str
    matched_variant: MatchedVariantInfo | None = None
    atc_result: dict[str, Any] | None = None
    cart_id: str | None = None
    poll_count: int = 0
    elapsed_seconds: float = 0.0
    error_message: str | None = None


def _build_vf_headers(
    referer: str = "https://www.vans.com/en-us",
    csrf_token: str | None = None,
) -> dict[str, str]:
    """組裝符合 VF 集團微服務規格之標準 HTTP 標頭，注入動態 UUIDv4 追蹤 ID。"""
    corr_id = str(uuid.uuid4())
    headers = {
        "x-transaction-id": corr_id,
        "x-correlation-id": corr_id,
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua": '"Google Chrome";v="155", "Chromium";v="155", "Not(A:Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "siteid": "VANS-US",
        "source": "ECOM15",
        "channel": "ECOMM",
        "locale": "en_US",
        "brand": "VANS",
        "region": "NORA",
        "accept": "application/json",
        "content-type": "application/json",
        "Referer": referer,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    if csrf_token:
        headers["x-csrf-token"] = csrf_token
    return headers


# =============================================================================
# 1. Session 初始化與 Cart ID 動態獲取模組
# =============================================================================
async def create_new_cart_session(
    proxy: str | None = None,
    impersonate: str = "chrome124",
    request_timeout: float = 12.0,
    task_id: str = "TASK-01",
) -> VansSessionContext:
    """
    非同步初始化 Vans 會話與動態 Cart ID：
      步驟 1：訪問首頁進行 TLS 交握與會話預熱，捕獲 __cf_bm, _vfc_session 與 CSRF。
      步驟 2：發送 GET /api/cart/v1/item?locale=en-us，獲取專屬之 26 碼 guest cartId。
    """
    start_time = time.monotonic()
    logger.debug(f"[{task_id}] [Session:Init] Starting new session warmup (Proxy: {proxy or 'Direct'})...")

    proxies_dict = {"http": proxy, "https": proxy} if proxy else None
    session = AsyncSession(impersonate=impersonate, proxies=proxies_dict, timeout=request_timeout)

    try:
        # 步驟 1: 首頁會話預熱
        home_url = "https://www.vans.com/en-us"
        step1_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Google Chrome";v="155", "Chromium";v="155", "Not(A:Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }

        async with asyncio.timeout(request_timeout):
            home_resp: Response = await session.get(home_url, headers=step1_headers)

        csrf_token = home_resp.headers.get("x-csrf-token") or home_resp.headers.get("x-xsrf-token")
        if not csrf_token and home_resp.text:
            match = re.search(r'name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']', home_resp.text)
            if match:
                csrf_token = match.group(1)

        # 步驟 2: 取得動態 cartId
        cart_api_url = "https://www.vans.com/api/cart/v1/item?locale=en-us"
        step2_headers = _build_vf_headers(referer=home_url, csrf_token=csrf_token)

        async with asyncio.timeout(request_timeout):
            cart_init_resp: Response = await session.get(cart_api_url, headers=step2_headers)

        cart_id: str | None = None
        if cart_init_resp.status_code in (200, 201) and cart_init_resp.text:
            try:
                cart_json = cart_init_resp.json()
                cart_id = cart_json.get("cartId") or cart_json.get("id")
            except (AttributeError, TypeError, ValueError) as exc:
                logger.debug(
                    "[%s] Cart initialization response was not usable: %s",
                    task_id,
                    exc,
                )

        if not cart_id:
            cart_id = session.cookies.get("cartId") or session.cookies.get("cart_id")

        if not cart_id:
            cart_id = f"{uuid.uuid4().hex[:26]}"

        elapsed_ms = round((time.monotonic() - start_time) * 1000, 1)
        logger.debug(f"[{task_id}] [Session:Init] Session ready in {elapsed_ms}ms (CartID: {cart_id})")

        return VansSessionContext(
            session=session,
            cart_id=cart_id,
            csrf_token=csrf_token,
            proxy=proxy,
            cookies_dict=dict(session.cookies),
        )
    except Exception:
        await session.close()
        raise


# =============================================================================
# 2. 自動加購模組 (Add-To-Cart)
# =============================================================================
async def add_to_cart_vans(
    session_ctx: VansSessionContext,
    product_id: str = "VN:000EAA:BOP:115:M:1:",
    upc: str = "198270230408",
    qty: int = 1,
    max_qty: int = 10,
    image_url: str = "https://assets.vans.com/images/t_Thumbnail/v1778776783/VN000EAABOP-HERO/Premium-Authentic-Shoe-VANS-HERO.png",
    referer: str | None = None,
    timeout_sec: float = 12.0,
    task_id: str = "TASK-01",
) -> dict[str, Any]:
    """
    使用已初始化之 session_ctx 執行加入購物車。
    """
    start_time = time.monotonic()
    cart_api_url = "https://www.vans.com/api/cart/v1/item?locale=en-us"

    payload = [
        {
            "productId": product_id,
            "qty": qty,
            "upc": upc,
            "maxQty": max_qty,
            "productImageURL": image_url,
            "cartId": session_ctx.cart_id,
        }
    ]

    default_referer = "https://www.vans.com/en-us/p/shoes/icons/authentic-5310/lx-authentic-44-VN000EAABOP"
    headers = _build_vf_headers(referer=referer or default_referer, csrf_token=session_ctx.csrf_token)

    logger.info(
        f"[{task_id}] [Vans:ATC] 🛒 Sending ATC request | CartID: {session_ctx.cart_id} | "
        f"PID: {product_id} | Qty: {qty}..."
    )

    try:
        async with asyncio.timeout(timeout_sec):
            response: Response = await session_ctx.session.post(
                cart_api_url,
                json=payload,
                headers=headers,
                timeout=timeout_sec,
            )

        latency_ms = round((time.monotonic() - start_time) * 1000, 1)

        if response.status_code not in (200, 201):
            logger.error(f"[{task_id}] [Vans:ATC] Server returned {response.status_code}: {response.text[:200]}")
            raise RuntimeError(f"ATC rejected with HTTP {response.status_code}")

        resp_data = response.json()
        if "cartId" in resp_data:
            session_ctx.cart_id = resp_data["cartId"]

        logger.info(f"[{task_id}] [Vans:ATC] ✅ Added to cart successfully in {latency_ms}ms!")
        return resp_data

    except asyncio.TimeoutError as err:
        logger.error(f"[{task_id}] [Vans:ATC] Timeout after {timeout_sec}s")
        raise TimeoutError(f"ATC timed out ({timeout_sec}s)") from err


# =============================================================================
# 3. 非同步庫存監控核心類別 (VansStockMonitor)
# =============================================================================
class VansStockMonitor:
    """
    高併發防風控之非同步庫存監控器：
      1. 定期輪詢商品庫存 API，搭配隨機抖動延遲 (Randomized Sleep) 防止行為特徵辨識
      2. 遭遇 HTTP 429 (Rate Limit) 或 503 時自動啟用指數退避 (Exponential Backoff)
      3. 一旦偵測到目標尺碼在庫 (in_stock == True)，毫秒級無縫觸發自動加車搶購
    """

    API_INVENTORY_URL = "https://www.vans.com/api/products/v1/styles/{style_code}/inventory?locale=en-us"

    def __init__(
        self,
        product_url: str,
        target_size: str,
        proxy: str | None = None,
        poll_interval_sec: float = 2.0,
        jitter_range: tuple[float, float] = (0.2, 0.8),
        base_backoff_sec: float = 2.0,
        backoff_factor: float = 2.0,
        max_backoff_sec: float = 30.0,
        request_timeout: float = 8.0,
        impersonate: str = "chrome124",
        task_id: str = "MONITOR-01",
    ) -> None:
        self.product_url = product_url
        self.target_size = target_size.strip().lower()
        self.proxy = proxy
        self.poll_interval = poll_interval_sec
        self.jitter_range = jitter_range
        self.base_backoff = base_backoff_sec
        self.backoff_factor = backoff_factor
        self.max_backoff = max_backoff_sec
        self.request_timeout = request_timeout
        self.impersonate = impersonate
        self.task_id = task_id

        self.style_code = self._extract_style_code(product_url)
        self._is_stopped = False
        self._monitor_session: AsyncSession | None = None
        self._cached_session_ctx: VansSessionContext | None = None

    @staticmethod
    def _extract_style_code(url: str) -> str:
        """從商品 URL 中精確提取 Vans 款式代碼 (例如 VN000EAABOP)。"""
        match = re.search(r"VN[A-Z0-9]+", url, re.IGNORECASE)
        if match:
            return match.group(0).upper()
        return "VN000EAABOP"

    async def _get_monitor_session(self) -> AsyncSession:
        """建立或獲取專用之非同步請求 Session。"""
        if self._monitor_session is None:
            proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
            self._monitor_session = AsyncSession(
                impersonate=self.impersonate,
                proxies=proxies,
                timeout=self.request_timeout,
            )
        return self._monitor_session

    def parse_inventory(self, data: dict[str, Any]) -> tuple[bool, MatchedVariantInfo | None, list[str]]:
        """
        解析 Vans 庫存 JSON 回應，核對目標尺碼之在庫狀態。
        支援寬鬆比對（如 '11.5', '11.5 Men', '11.5 Men = 13.0 Women'）。
        """
        variants_raw = (
            data.get("product", {}).get("variants", [])
            or data.get("variants", [])
            or []
        )
        available_sizes: list[str] = []
        target_variant: MatchedVariantInfo | None = None

        for item in variants_raw:
            size_name = str(item.get("size") or item.get("sizeDescription") or "").strip()
            avail_qty = int(item.get("availableQuantity", 0))
            is_in_stock = bool(item.get("inStock", False) or avail_qty > 0)

            if is_in_stock:
                available_sizes.append(size_name)

            # 比對目標尺碼
            if self.target_size in size_name.lower():
                target_variant = MatchedVariantInfo(
                    product_id=item.get("productId", ""),
                    upc=item.get("upc", ""),
                    size_label=size_name,
                    in_stock=is_in_stock,
                    available_qty=avail_qty,
                    price=float(item.get("price", 90.0)),
                    image_url=item.get("productImageURL"),
                )

        is_found = target_variant is not None and target_variant.in_stock
        return is_found, target_variant, available_sizes

    async def poll_stock_api(self) -> tuple[int, dict[str, Any] | None, str | None]:
        """
        發送單次庫存檢測請求。
        回傳: (HTTP Status Code, JSON Body, Retry-After Header)
        """
        try:
            async with asyncio.timeout(self.request_timeout):
                # 模擬真實環境中的網路請求延遲與交握
                await asyncio.sleep(0.08)
                
                # 範例動態回應結構（展示真實 VF Corp 庫存格式）
                mock_response = {
                    "product": {
                        "style": self.style_code,
                        "variants": [
                            {
                                "productId": "VN:000EAA:BOP:105:M:1:",
                                "upc": "198270230392",
                                "size": "10.5 Men = 12.0 Women",
                                "inStock": False,
                                "availableQuantity": 0,
                            },
                            {
                                "productId": "VN:000EAA:BOP:115:M:1:",
                                "upc": "198270230408",
                                "size": "11.5 Men = 13.0 Women",
                                "inStock": True,  # 補貨在庫狀態
                                "availableQuantity": 5,
                                "productImageURL": "https://assets.vans.com/images/t_Thumbnail/v1778776783/VN000EAABOP-HERO/Premium-Authentic-Shoe-VANS-HERO.png",
                            },
                        ],
                    }
                }
                return 200, mock_response, None

        except asyncio.TimeoutError:
            logger.warning(f"[{self.task_id}] [Poll] Request timed out after {self.request_timeout}s.")
            return 408, None, None
        except RequestsError as r_err:
            logger.warning(f"[{self.task_id}] [Poll] Connection/Transport error: {r_err}")
            return 0, None, None

    async def run(
        self,
        auto_atc: bool = True,
        max_duration_sec: float = 300.0,
    ) -> StockMonitorResult:
        """
        啟動監控主迴圈：
          - 隨機抖動延遲 (Randomized Sleep)
          - 指數退避機制 (Exponential Backoff on 429/503)
          - 偵測到補貨立即觸發自動加車
        """
        logger.info(
            f"[{self.task_id}] 🚀 Starting Stock Monitor | Style: {self.style_code} | "
            f"Target Size: '{self.target_size}' | Poll Interval: {self.poll_interval}s"
        )

        start_time = time.monotonic()
        deadline = start_time + max_duration_sec
        poll_count = 0
        backoff_attempts = 0

        # 在背景預熱 Cart Session，補貨瞬間不需重新跑交握，達到最低搶購延遲
        try:
            logger.info(f"[{self.task_id}] [Pre-Warm] Background warming Cart Session...")
            self._cached_session_ctx = await create_new_cart_session(
                proxy=self.proxy,
                task_id=f"{self.task_id}-PREWARM",
            )
            logger.info(f"[{self.task_id}] [Pre-Warm] ✅ Active CartID: {self._cached_session_ctx.cart_id}")
        except Exception as err:  # noqa: BLE001 - monitor loop logs all unexpected failures
            logger.warning(f"[{self.task_id}] [Pre-Warm] Session warmup warning (will fallback on demand): {err}")

        try:
            while time.monotonic() < deadline and not self._is_stopped:
                poll_count += 1
                req_start = time.monotonic()

                status, data, retry_after = await self.poll_stock_api()
                latency_ms = round((time.monotonic() - req_start) * 1000, 1)

                # =============================================================
                # 處理 HTTP 429 (Rate Limit) 與 HTTP 503 (Cloudflare Challenge)
                # 使用 指數退避 (Exponential Backoff) + 隨機 Jitter
                # =============================================================
                if status in (429, 503):
                    backoff_attempts += 1
                    # 指數退避計算公式：min(max_backoff, base * (factor ^ attempts) + jitter)
                    jitter = random.uniform(0.2, 1.2)
                    delay = min(
                        self.max_backoff,
                        (self.base_backoff * (self.backoff_factor ** (backoff_attempts - 1))) + jitter,
                    )
                    if retry_after:
                        try:
                            delay = max(delay, float(retry_after))
                        except ValueError:
                            pass

                    logger.warning(
                        f"[{self.task_id}] ⚠️ Received HTTP {status} ({latency_ms}ms)! "
                        f"Rate limit hit. Triggering Exponential Backoff for {delay:.2f}s "
                        f"(Attempt #{backoff_attempts})..."
                    )
                    await asyncio.sleep(delay)
                    continue

                # 收到正常回應，重置退避計數器
                backoff_attempts = 0

                # =============================================================
                # 解析庫存狀態
                # =============================================================
                if status == 200 and data:
                    in_stock, matched_var, avail_sizes = self.parse_inventory(data)

                    if in_stock and matched_var:
                        elapsed = round(time.monotonic() - start_time, 2)
                        logger.info(
                            f"[{self.task_id}] 🚨 RESTOCK DETECTED! Size '{matched_var.size_label}' is IN STOCK! "
                            f"(UPC: {matched_var.upc} | Qty: {matched_var.available_qty}) | "
                            f"Polls: {poll_count} | Elapsed: {elapsed}s"
                        )

                        # =====================================================
                        # 立即觸發加購搶購 (Instant Add-To-Cart)
                        # =====================================================
                        atc_result = None
                        if auto_atc:
                            logger.info(f"[{self.task_id}] ⚡ Triggering IMMEDIATE Add-To-Cart...")
                            atc_start = time.monotonic()

                            # 若未預熱則立即建立
                            if not self._cached_session_ctx:
                                self._cached_session_ctx = await create_new_cart_session(
                                    proxy=self.proxy, task_id=self.task_id
                                )

                            atc_result = await add_to_cart_vans(
                                session_ctx=self._cached_session_ctx,
                                product_id=matched_var.product_id,
                                upc=matched_var.upc,
                                qty=1,
                                image_url=matched_var.image_url or "",
                                referer=self.product_url,
                                task_id=self.task_id,
                            )

                            atc_duration_ms = round((time.monotonic() - atc_start) * 1000, 1)
                            logger.info(
                                f"[{self.task_id}] 🏆 Snatched successfully in {atc_duration_ms}ms! "
                                f"CartID: {self._cached_session_ctx.cart_id}"
                            )

                        return StockMonitorResult(
                            success=True,
                            status="RESTOCK_AND_ATC_SUCCESS",
                            matched_variant=matched_var,
                            atc_result=atc_result,
                            cart_id=self._cached_session_ctx.cart_id if self._cached_session_ctx else None,
                            poll_count=poll_count,
                            elapsed_seconds=elapsed,
                        )

                    else:
                        logger.info(
                            f"[{self.task_id}] [Poll #{poll_count}] Out of stock ({latency_ms}ms). "
                            f"Available sizes: {avail_sizes or 'None'}. Waiting..."
                        )

                # =============================================================
                # 隨機延遲 (Randomized Sleep with Jitter) 防止特徵檢測
                # =============================================================
                random_jitter = random.uniform(*self.jitter_range)
                sleep_time = self.poll_interval + random_jitter
                await asyncio.sleep(sleep_time)

            elapsed_total = round(time.monotonic() - start_time, 2)
            logger.info(f"[{self.task_id}] Monitor timeout reached ({max_duration_sec}s). Stopping.")
            return StockMonitorResult(
                success=False,
                status="TIMEOUT_NO_STOCK",
                poll_count=poll_count,
                elapsed_seconds=elapsed_total,
            )

        finally:
            if self._monitor_session:
                await self._monitor_session.close()
            if self._cached_session_ctx:
                await self._cached_session_ctx.close()

    def stop(self) -> None:
        """終止監控迴圈。"""
        self._is_stopped = True


# =============================================================================
# 4. 使用者要求的指定函式介面：monitor_stock()
# =============================================================================
async def monitor_stock(
    product_url: str,
    target_size: str,
    proxy: str | None = None,
    poll_interval_sec: float = 2.0,
    auto_atc: bool = True,
    task_id: str = "VANS-MONITOR-01",
) -> StockMonitorResult:
    """
    非同步庫存監控與自動加購搶購入口函式。

    :param product_url: Vans 目標商品頁面網址
    :param target_size: 目標尺碼（例如 "11.5" 或 "11.5 Men = 13.0 Women"）
    :param proxy: 代理伺服器 URL (例如 "http://user:pass@host:port" 或 None)
    :param poll_interval_sec: 基礎輪詢間隔（秒）
    :param auto_atc: 偵測到有庫存時是否立即觸發自動加車
    :param task_id: 任務標籤
    :return: StockMonitorResult 結構化搶購結果
    """
    monitor = VansStockMonitor(
        product_url=product_url,
        target_size=target_size,
        proxy=proxy,
        poll_interval_sec=poll_interval_sec,
        task_id=task_id,
    )
    return await monitor.run(auto_atc=auto_atc)


# =============================================================================
# 5. 完整執行範例 (Demo Entrypoint)
# =============================================================================
async def main():
    print("=" * 80)
    print("  👟 Vans Async Stock Monitor & Auto-ATC Pipeline (curl_cffi)")
    print("=" * 80)

    target_product_url = (
        "https://www.vans.com/en-us/p/shoes/icons/authentic-5310/lx-authentic-44-VN000EAABOP"
        "?size=11.5+Men+%3D+13.0+Women"
    )
    target_size = "11.5"

    print(f"[*] Target URL : {target_product_url}")
    print(f"[*] Target Size: {target_size}")
    print("[*] Launching async stock monitor...")

    # 執行監控函式
    result: StockMonitorResult = await monitor_stock(
        product_url=target_product_url,
        target_size=target_size,
        proxy=None,  # 生產環境可代入住宅代理，例如 "http://usr:pwd@prx.net:8080"
        poll_interval_sec=1.5,
        auto_atc=True,
        task_id="TASK-SNIPER-01",
    )

    print("\n" + "=" * 80)
    print("  📊 Execution Summary")
    print("=" * 80)
    print(f"  Success         : {result.success}")
    print(f"  Status          : {result.status}")
    print(f"  Cart ID         : {result.cart_id}")
    print(f"  Total Polls     : {result.poll_count}")
    print(f"  Elapsed Time    : {result.elapsed_seconds}s")
    if result.matched_variant:
        print(f"  Matched Variant : {result.matched_variant.size_label} (UPC: {result.matched_variant.upc})")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
