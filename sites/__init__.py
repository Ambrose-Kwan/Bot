"""Canonical site adapters and shared checkout result models."""

from .base_site import (
    BaseSite,
    CartResult,
    CheckoutResult,
    ShippingRate,
    ShippingResult,
    StockStatus,
    TaskSummary,
)
from .base_site import (
    StockResult as SiteStockResult,
)
from .vans_site import VansSite, VansStockResult, VansVariant

__all__ = [
    "BaseSite",
    "CartResult",
    "CheckoutResult",
    "ShippingRate",
    "ShippingResult",
    "SiteStockResult",
    "StockStatus",
    "TaskSummary",
    "VansSite",
    "VansStockResult",
    "VansVariant",
]
