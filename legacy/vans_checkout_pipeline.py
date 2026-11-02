"""================================================================================
專案：Vans (VF Corp) 自動結帳與 3D Secure 混合接力流水線 (Hybrid Checkout Pipeline)
檔案：vans_checkout_pipeline.py
相依套件：pip install curl_cffi playwright
         playwright install chromium
執行方式：python vans_checkout_pipeline.py (或從外層 python checkout_framework/vans_checkout_pipeline.py)
================================================================================
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

# 動態路徑解析，確保在 checkout_framework 內部或專案根目錄下直接執行均不會拋出 ModuleNotFoundError
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in (current_dir, parent_dir):
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from sites.vans_checkout import CheckoutOutcome, VansCheckoutClient
    from sites.vans_session_manager import (
        VansSessionContext,
        add_to_cart_vans,
        create_new_cart_session,
    )
except ImportError:
    from checkout_framework.sites.vans_checkout import CheckoutOutcome, VansCheckoutClient
    from checkout_framework.sites.vans_session_manager import (
        VansSessionContext,
        add_to_cart_vans,
        create_new_cart_session,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("vans.hybrid_checkout")


async def run_hybrid_checkout_pipeline(
    proxy: str | None = None,
    target_size: str = "11.5",
    trigger_3ds_demo: bool = True,
    headless_browser: bool = False,
) -> CheckoutOutcome:
    """執行端到端完整搶購結帳流程：
    1. 初始化 Session 與動態 Cart ID
    2. 執行高效加購 (Add to Cart)
    3. 套用防砍單地址變形 (Address Jigging)
    4. 發送結帳 POST 請求
    5. 若遭遇 3DS / OTP 挑戰，自動移交 Playwright 瀏覽器彈出視窗由使用者驗證
    """
    logger.info("=" * 70)
    logger.info("👟 開始執行 Vans Hybrid Checkout 自動結帳與 3DS 接力任務")
    logger.info(f"   目標尺寸: {target_size} | Proxy: {proxy or 'Direct Connection'}")
    logger.info("=" * 70)

    # 步驟 1: 建立動態會話
    logger.info("[Step 1/4] 🚀 正在獲取全新 Vans 會話與動態 Cart ID...")
    ctx: VansSessionContext = await create_new_cart_session(proxy=proxy)
    logger.info(f"✅ 會話建立完成！Cart ID: {ctx.cart_id} | CSRF: {ctx.csrf_token or 'None'}")

    try:
        # 步驟 2: 加購目標商品 (Authentic 44 DX - 11.5)
        logger.info("[Step 2/4] 🛒 正在發送 Add-To-Cart (ATC) 請求...")
        cart_res = await add_to_cart_vans(
            session_ctx=ctx,
            product_id="VN:000EAA:BOP:115:M:1:",
            upc="198270230408",
            qty=1,
            image_url="https://assets.vans.com/images/t_Thumbnail/v1778776783/VN000EAABOP-HERO/Premium-Authentic-Shoe-VANS-HERO.png",
            task_id="TEST-ATC",
        )
        if not cart_res or cart_res.get("error") or "cartId" not in cart_res:
            raise RuntimeError(f"加購失敗: {cart_res}")

        subtotal = float(cart_res.get("subtotal", 90.0)) if isinstance(cart_res, dict) else 90.0
        logger.info(f"✅ 加購成功！購物車總金額: ${subtotal:.2f}")

        # 步驟 3: 準備買家寄送資料 (支援 Address Jigging)
        logger.info("[Step 3/4] 📦 準備收件資料 (Address Jigging 啟動)...")
        raw_shipping = {
            "firstName": "Alex",
            "lastName": "Chen",
            "address1": "1588 South Harbor Blvd",
            "address2": "",
            "city": "Anaheim",
            "stateCode": "CA",
            "postalCode": "92802",
            "phone": "7145550199",
            "email": "buyer.alex@example.com",
        }

        # 準備信用卡 (若開啟 trigger_3ds_demo，使用觸發 3DS 測試卡號)
        card_number = "4000123456789010" if trigger_3ds_demo else "4111111111111111"
        raw_payment = {
            "cardHolder": "Alex Chen",
            "cardNumber": card_number,
            "expMonth": "10",
            "expYear": "2028",
            "cvv": "789",
        }

        # 步驟 4: 執行結帳請求與 3DS 接力
        logger.info("[Step 4/4] 💳 發送結帳 POST 請求並監控 3DS 轉接點...")
        checkout_client = VansCheckoutClient(
            session=ctx.session,
            cart_id=ctx.cart_id,
            csrf_token=ctx.csrf_token,
            proxy=proxy,
        )
        outcome: CheckoutOutcome = await checkout_client.execute_checkout(
            shipping_raw=raw_shipping,
            payment_raw=raw_payment,
            enable_jig=True,
            auto_browser_handoff=True,
            browser_headless=headless_browser,
        )

        if outcome.is_success:
            logger.info("=" * 70)
            logger.info("🎉🎉🎉 恭喜！訂單已成功成立！")
            logger.info(f"    訂單編號: {outcome.order_id}")
            logger.info(f"    是否經過 3DS 接力: {outcome.requires_3ds}")
            logger.info(f"    總耗時: {outcome.latency_ms} ms")
            logger.info("=" * 70)
        else:
            logger.error(f"❌ 結帳流程未完成: {outcome.error_message}")

        return outcome
    finally:
        await ctx.close()


async def main():
    proxy_arg = sys.argv[1] if len(sys.argv) > 1 else None
    await run_hybrid_checkout_pipeline(
        proxy=proxy_arg,
        target_size="11.5",
        trigger_3ds_demo=True,
        headless_browser=False,
    )


if __name__ == "__main__":
    asyncio.run(main())
