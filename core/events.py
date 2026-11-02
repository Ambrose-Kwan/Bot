from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("nexus.events")


@dataclass
class StockEventPayload:
    """Carries discovered stock data broadcast to awaiting checkout workers."""
    sku: str
    variant_id: str
    size_label: str
    price: float | None = None
    timestamp_ms: int = 0
    raw_response: dict[str, Any] = field(default_factory=dict)


class StockEventBus:
    """
    High-speed in-memory Event Bus using asyncio.Queue and Events for zero-latency
    broadcast signaling across decoupled tasks.
    """

    def __init__(self) -> None:
        self._listeners: dict[str, list[asyncio.Queue[StockEventPayload]]] = {}
        self._latest_payload: dict[str, StockEventPayload] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, sku: str) -> asyncio.Queue[StockEventPayload]:
        """Subscribe a worker to instant stock release notifications for a specific SKU."""
        normalized_sku = sku.strip().upper()
        queue: asyncio.Queue[StockEventPayload] = asyncio.Queue()
        async with self._lock:
            if normalized_sku not in self._listeners:
                self._listeners[normalized_sku] = []
            self._listeners[normalized_sku].append(queue)
        logger.debug("Subscribed worker queue to SKU '%s'", normalized_sku)
        return queue

    async def unsubscribe(self, sku: str, queue: asyncio.Queue[StockEventPayload]) -> None:
        """Safely detach worker queue from listener registry."""
        normalized_sku = sku.strip().upper()
        async with self._lock:
            if normalized_sku in self._listeners:
                try:
                    self._listeners[normalized_sku].remove(queue)
                    if not self._listeners[normalized_sku]:
                        del self._listeners[normalized_sku]
                except ValueError:
                    pass

    async def publish(self, payload: StockEventPayload) -> int:
        """
        Broadcast stock availability to all awaiting workers concurrently.
        Returns the number of awakened listeners.
        """
        normalized_sku = payload.sku.strip().upper()
        async with self._lock:
            self._latest_payload[normalized_sku] = payload
            queues = list(self._listeners.get(normalized_sku, []))

        count = len(queues)
        if count > 0:
            for q in queues:
                q.put_nowait(payload)
            logger.info(
                "⚡ [EventBus] Broadcast stock alert for SKU '%s' (%s) to %d awaiting workers!",
                normalized_sku,
                payload.size_label,
                count,
            )
        return count

    def get_latest(self, sku: str) -> StockEventPayload | None:
        """Retrieve cached inventory payload if already detected."""
        return self._latest_payload.get(sku.strip().upper())


# 全域單例事件匯流排
global_event_bus = StockEventBus()