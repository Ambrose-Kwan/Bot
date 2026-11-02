"""
================================================================================
專案：Vans (VF Corp) Session 初始化與 Cart ID 動態獲取 + 加車串接完整範例
檔案：vans_cart_pipeline.py
特性：整合 RobustAsyncSession（ProxyPool 代理池、Sticky 黏性會話、Rotate-on-Retry 自動輪轉）
執行方式：python vans_cart_pipeline.py
================================================================================
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

# 1. 匯入專案的強健網路連線層 RobustAsyncSession
try:
    from core.network import RobustAsyncSession
except ImportError:
    import importlib

    try:
        parent_package = __package__.rpartition(".")[0] if __package__ else ""
        module_name = f"{parent_package}.core.network" if parent_package else "core.network"
        RobustAsyncSession = importlib.import_module(module_name).RobustAsyncSession
    except ImportError:
        try:
            RobustAsyncSession = importlib.import_module("network").RobustAsyncSession
        except ImportError:
            # 備用降級 Mock（若於獨立環境執行）
            RobustAsyncSession = None  # type: ignore[assignment, misc]

# 2. 匯入 curl_cffi 相關型別與錯誤
try:
    from curl_cffi.requests import AsyncSession, Response
    from curl_cffi.requests.errors import RequestsError
except ImportError:
    class RequestsError(Exception):
        pass
    class Response:  # type: ignore[no-redef]
        def __init__(self, status_code=200, headers=None, text="", json_data=None):
            self.status_code = status_code
            self.headers = headers or {}
            self.text = text
            self._json = json_data or {}
        def json(self):
            return self._json
    AsyncSession = Any

# 設定結構化日誌格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("vans.pipeline")


# =============================================================================
# 0. Session Context 資料結構
# =============================================================================
@dataclass
class VansSessionContext:
    """
    封裝完成預熱的 Vans 購物會話，包含專屬 RobustAsyncSession、動態 Cart ID 與 Cookies。
    """
    session: Any
    cart_id: str
    csrf_token: str | None = None
    proxy: str | None = None
    proxy_pool: list[str] | None = None
    cookies_dict: dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    async def close(self) -> None:
        """優雅關閉底層連線池，釋放 Socket 與代理資源。"""
        if self.session and hasattr(self.session, "close") and callable(self.session.close):
            await self.session.close()


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
# 1. Session 初始化與 Cart ID 動態獲取函式 (整合 RobustAsyncSession)
# =============================================================================
async def create_new_cart_session(
    proxy: str | None = None,
    proxy_pool: list[str] | None = None,
    impersonate: str = "chrome124",
    request_timeout: float = 12.0,
    task_id: str = "TASK-01",
    sticky: bool = True,
    rotate_on_retry: bool = True,
) -> VansSessionContext:
    """
    非同步初始化 Vans 會話與動態 Cart ID：
      - 整合 RobustAsyncSession 支援 Sticky IP 與 429 自動輪轉
      - 步驟 1：訪問首頁進行 TLS 交握與會話預熱，捕獲 __cf_bm, _vfc_session 與 CSRF
      - 步驟 2：發送 GET /api/cart/v1/item?locale=en-us，獲取專屬之 26 碼 guest cartId
    """
    start_time = time.monotonic()
    
    # 決定代理清單 (相容傳入單一代理字串或完整代理清單)
    resolved_pool: list[str] | None = None
    if proxy_pool:
        resolved_pool = list(proxy_pool)
    elif proxy:
        resolved_pool = [proxy]

    display_proxy = resolved_pool[0] if resolved_pool else "Direct Connection"
    logger.info(
        f"[{task_id}] [Session:Init] 啟動連線預熱 | 代理模式: {display_proxy} "
        f"| Sticky: {sticky} | Rotate-on-Retry: {rotate_on_retry}"
    )

    # 實例化 RobustAsyncSession 或降級的原生 AsyncSession
    if RobustAsyncSession is not None:
        session = RobustAsyncSession(
            proxy_pool=resolved_pool,
            task_id=task_id,
            impersonate=impersonate,
            timeout=request_timeout,
            sticky=sticky,
            rotate_on_retry=rotate_on_retry,
        )
    else:
        proxies_dict = {"http": proxy, "https": proxy} if proxy else None
        session = AsyncSession(impersonate=impersonate, proxies=proxies_dict, timeout=request_timeout)

    try:
        # ---------------------------------------------------------------------
        # 步驟 1: 首頁會話預熱 (Session Warm-up & Cookie Harvesting)
        # ---------------------------------------------------------------------
        home_url = "https://www.vans.com/en-us"
        logger.debug(f"[{task_id}] [Session:Init] Step 1: 訪問首頁 {home_url} 建立連線與指紋...")

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

        home_resp: Response = await session.get(home_url, headers=step1_headers, timeout=request_timeout)

        # 提取 CSRF Token
        csrf_token = home_resp.headers.get("x-csrf-token") or home_resp.headers.get("x-xsrf-token")
        if not csrf_token and getattr(home_resp, "text", None):
            match = re.search(r'name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']', home_resp.text)
            if match:
                csrf_token = match.group(1)

        # ---------------------------------------------------------------------
        # 步驟 2: 呼叫購物車端點動態提取 cartId
        # ---------------------------------------------------------------------
        cart_api_url = "https://www.vans.com/api/cart/v1/item?locale=en-us"
        logger.debug(f"[{task_id}] [Session:Init] Step 2: 獲取專屬購物車 ID {cart_api_url}...")

        step2_headers = _build_vf_headers(referer=home_url, csrf_token=csrf_token)
        cart_init_resp: Response = await session.get(cart_api_url, headers=step2_headers, timeout=request_timeout)

        cart_id: str | None = None

        if cart_init_resp.status_code in (200, 201) and getattr(cart_init_resp, "text", None):
            try:
                cart_json = cart_init_resp.json()
                cart_id = cart_json.get("cartId") or cart_json.get("id")
            except (AttributeError, TypeError, ValueError) as exc:
                logger.debug("[%s] Cart initialization response was not usable: %s", task_id, exc)

        # 若未直接於 Body 回傳，自 Cookie 提取
        session_cookies = getattr(session, "cookies", {}) or {}
        if not cart_id:
            cart_id = session_cookies.get("cartId") or session_cookies.get("cart_id")

        # 若伺服器採用首次加車才派發機制，則本地生成標準 VF 格式之 26 碼 cartId
        if not cart_id:
            cart_id = f"{uuid.uuid4().hex[:26]}"
            logger.info(f"[{task_id}] [Session:Init] 生成本地 Guest CartID: {cart_id}")
        else:
            logger.info(f"[{task_id}] [Session:Init] 伺服器成功派發 CartID: {cart_id}")

        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
        cookies_captured = dict(session_cookies)

        logger.info(
            f"[{task_id}] [Session:Init] ✅ 連線與購物車預熱完成 ({elapsed_ms}ms)! "
            f"CartID: {cart_id} | 捕獲 Cookies: {len(cookies_captured)} 個"
        )

        return VansSessionContext(
            session=session,
            cart_id=cart_id,
            csrf_token=csrf_token,
            proxy=proxy,
            proxy_pool=resolved_pool,
            cookies_dict=cookies_captured,
        )

    except (asyncio.TimeoutError, TimeoutError) as err:
        logger.error(f"[{task_id}] [Session:Init] 連線逾時 ({request_timeout}s)")
        if hasattr(session, "close") and callable(session.close):
            await session.close()
        raise TimeoutError(f"Vans session creation timed out ({request_timeout}s)") from err
    except Exception as exc:
        logger.error(f"[{task_id}] [Session:Init] 預熱過程失敗: {exc}")
        if hasattr(session, "close") and callable(session.close):
            await session.close()
        raise


# =============================================================================
# 2. 串接加購函式 (Add To Cart with Context Injection)
# =============================================================================
async def add_to_cart_vans(
    session_ctx: VansSessionContext,
    product_id: str | list[dict[str, Any]] | dict[str, Any] = "VN:000EAA:BOP:115:M:1:",
    upc: str = "198270230408",
    qty: int = 1,
    max_qty: int = 10,
    image_url: str = "https://assets.vans.com/images/t_Thumbnail/v1778776783/VN000EAABOP-HERO/Premium-Authentic-Shoe-VANS-HERO.png",
    referer: str | None = None,
    timeout_sec: float = 12.0,
    task_id: str = "TASK-01",
    **kwargs: Any,
) -> dict[str, Any]:
    """
    使用已初始化之 session_ctx 執行高速加車：
      - 沿用 session_ctx 內帶有 Sticky 狀態的 RobustAsyncSession
      - 注入動態生成的 session_ctx.cart_id
    """
    start_time = time.monotonic()
    cart_api_url = "https://www.vans.com/api/cart/v1/item?locale=en-us"

    if isinstance(product_id, list):
        payload = product_id
        for entry in payload:
            if isinstance(entry, dict) and not entry.get("cartId"):
                entry["cartId"] = session_ctx.cart_id
        pid_display = payload[0].get("productId", "BATCH") if payload else "EMPTY"
        qty_display = sum(e.get("qty", 1) for e in payload if isinstance(e, dict))
    elif isinstance(product_id, dict):
        payload = [product_id]
        if not payload[0].get("cartId"):
            payload[0]["cartId"] = session_ctx.cart_id
        pid_display = payload[0].get("productId", "SINGLE")
        qty_display = payload[0].get("qty", 1)
    else:
        pid_display = str(product_id)
        qty_display = qty
        payload = [
            {
                "productId": str(product_id),
                "qty": qty,
                "upc": upc,
                "maxQty": max_qty,
                "productImageURL": image_url,
                "cartId": session_ctx.cart_id,
            }
        ]

    default_referer = (
        "https://www.vans.com/en-us/p/shoes/icons/authentic-5310/lx-authentic-44-VN000EAABOP"
        "?size=11.5+Men+%3D+13.0+Women"
    )
    headers = _build_vf_headers(referer=referer or default_referer, csrf_token=session_ctx.csrf_token)

    logger.info(
        f"[{task_id}] [Vans:ATC] 發動快速加購 | CartID: {session_ctx.cart_id} | "
        f"PID: {pid_display} | 數量: {qty_display} | CorrelationID: {headers['x-correlation-id'][:8]}..."
    )

    try:
        response: Response = await session_ctx.session.post(
            cart_api_url,
            json=payload,
            headers=headers,
            timeout=timeout_sec,
        )

        latency_ms = round((time.monotonic() - start_time) * 1000, 2)

        if response.status_code not in (200, 201):
            logger.error(
                f"[{task_id}] [Vans:ATC] 伺服器拒絕 HTTP {response.status_code} ({latency_ms}ms) | "
                f"內容: {getattr(response, 'text', '')[:200]}"
            )
            raise RuntimeError(f"Vans ATC rejected with status {response.status_code}")

        resp_data = response.json()

        # 同步後端可能回傳的最新 cartId
        if "cartId" in resp_data:
            session_ctx.cart_id = resp_data["cartId"]

        logger.info(
            f"[{task_id}] [Vans:ATC] ✅ 加購成功 ({latency_ms}ms)! "
            f"現行 CartID: {session_ctx.cart_id}"
        )
        return resp_data

    except (asyncio.TimeoutError, TimeoutError) as err:
        logger.error(f"[{task_id}] [Vans:ATC] 加購請求連線逾時 ({timeout_sec}s)")
        raise TimeoutError(f"Vans ATC timeout ({timeout_sec}s)") from err
    except RequestsError as req_err:
        logger.error(f"[{task_id}] [Vans:ATC] 傳輸層錯誤: {req_err}")
        raise


# =============================================================================
# 3. 單一 Worker 任務流水線
# =============================================================================
async def run_single_checkout_task(
    task_id: str,
    proxy_pool: list[str] | None = None,
    product_id: str = "VN:000EAA:BOP:115:M:1:",
    upc: str = "198270230408",
    qty: int = 1,
) -> dict[str, Any]:
    logger.info(f"[{task_id}] >>> 啟動搶購 Worker...")
    
    # 初始化：注入 ProxyPool、啟用 Sticky IP 與自動重試換 IP (Rotate-on-Retry)
    session_ctx = await create_new_cart_session(
        proxy_pool=proxy_pool,
        task_id=task_id,
        sticky=True,
        rotate_on_retry=True,
    )

    try:
        atc_result = await add_to_cart_vans(
            session_ctx=session_ctx,
            product_id=product_id,
            upc=upc,
            qty=qty,
            task_id=task_id,
        )
        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "cart_id": session_ctx.cart_id,
            "atc_result": atc_result,
        }
    finally:
        await session_ctx.close()


# =============================================================================
# 4. 多任務高併發執行器 (Main Runner)
# =============================================================================
async def main() -> None:
    logger.info("=================================================================")
    logger.info("  🚀 啟動 Vans 高併發加購流水線 (RobustAsyncSession 整合版)")
    logger.info("=================================================================")

    # 模擬輪轉代理池
    proxies = [
        "http://proxy1.internal:8080",
        "http://proxy2.internal:8080",
        "http://proxy3.internal:8080",
    ]

    concurrency_limit = 3
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def bounded_worker(i: int):
        async with semaphore:
            task_id = f"WORKER-{i+1:02d}"
            return await run_single_checkout_task(
                task_id=task_id,
                proxy_pool=proxies,
                product_id="VN:000EAA:BOP:115:M:1:",
                upc="198270230408",
                qty=1,
            )

    tasks = [bounded_worker(i) for i in range(3)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("=================================================================")
    logger.info("  📊 任務執行統計結果")
    logger.info("=================================================================")
    for res in results:
        logger.info(f"  執行結果: {res}")
    logger.info("=================================================================")


if __name__ == "__main__":
    asyncio.run(main())