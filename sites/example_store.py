"""
Module: sites.example_store
Description: Concrete demonstration of an e-commerce adapter subclassing BaseSite.
Demonstrates:
  - Session warmup and CSRF header extraction
  - Stock polling with JSON API
  - Cart generation
  - Shipping address submission and rate selection
  - Final checkout submission with tokenized card data
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from .base_site import (
    BaseSite,
    CartResult,
    CheckoutResult,
    StockStatus,
)

logger = logging.getLogger("nexus.sites.example")


class ExampleEcomSite(BaseSite):
    """
    Production-style implementation for a JSON/REST-based e-commerce platform.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.csrf_token: str = ""
        self.checkout_token: str = ""

    async def initialize_session(self) -> None:
        """Fetch anti-bot session cookies and CSRF token."""
        logger.info(f"[{self.task_id}] Warming up session cookies from {self.config.target_url}...")
        # Simulated session handshake (in real sites, performs GET to store root)
        # res = await self.session.get(self.config.target_url)
        # self.csrf_token = res.headers.get("x-csrf-token", "tok_example_csrf_9921")
        await asyncio.sleep(0.1)  # Network handshake simulation
        self.csrf_token = f"csrf_{uuid.uuid4().hex[:12]}"
        logger.info(f"[{self.task_id}] Session initialized successfully. CSRF: {self.csrf_token}")

    async def check_stock(self) -> StockStatus:
        """Poll the product availability API."""
        sku = self.config.sku_or_variant_id
        target_size = self.config.target_size or "Universal"
        logger.debug(f"[{self.task_id}] Checking stock for SKU: {sku} (Size: {target_size})")

        # In production, query endpoint:
        # res = await self.session.get(f"https://api.store.com/v1/products/{sku}/inventory")
        await asyncio.sleep(0.15)

        # Return structured StockStatus
        return StockStatus(
            in_stock=True,
            title="Limited Edition Runner Sneakers",
            price=180.00,
            available_sizes=["US 9", "US 9.5", "US 10", "US 10.5", "US 11"],
            variant_id=sku,
            raw_response={"inventory_quantity": 4, "sku": sku},
        )

    async def add_to_cart(self) -> CartResult:
        """Add the product variant to the cart."""
        logger.info(f"[{self.task_id}] Submitting Add-To-Cart payload for SKU: {self.config.sku_or_variant_id}")

        # In production:
        # payload = {"items": [{"id": self.config.sku_or_variant_id, "quantity": self.config.quantity}]}
        # res = await self.session.post("https://api.store.com/cart/add.js", json=payload)
        await asyncio.sleep(0.12)

        cart_id = f"cart_{uuid.uuid4().hex[:16]}"
        return CartResult(
            success=True,
            cart_id=cart_id,
        )

    async def submit_shipping(self) -> bool:
        """Submit buyer address and choose express delivery."""
        addr = self.profile.shipping
        logger.info(f"[{self.task_id}] Submitting shipping to {addr.city}, {addr.state_code} {addr.postal_code}")

        # In production:
        # payload = {"address": addr.model_dump()}
        # res = await self.session.post(f"https://api.store.com/checkouts/{self.cart_result.cart_token}/shipping", json=payload)
        await asyncio.sleep(0.18)

        return True

    async def submit_checkout(self) -> CheckoutResult:
        """Submit payment credentials."""
        card = self.profile.payment
        logger.info(
            f"[{self.task_id}] Submitting payment | Brand: {card.card_brand} | Number: {card.masked_card_number}"
        )

        # In production, encrypt payment via Stripe/Adyen/Shopify Pay SDK:
        # payload = {"credit_card": {"number": card.card_number.get_secret_value(), ...}}
        # res = await self.session.post(f"https://api.store.com/checkouts/complete", json=payload)
        await asyncio.sleep(0.2)

        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        return CheckoutResult(
            success=True,
            order_id=order_id,
        )
