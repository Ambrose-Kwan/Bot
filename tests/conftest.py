from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest
from typing_extensions import Self

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from models.profile import BuyerProfile, PaymentMethod, ShippingAddress, TaskConfig


class MockResponse:
    def __init__(
        self,
        status_code: int = 200,
        *,
        headers: dict[str, str] | None = None,
        text: str = "",
        data: dict[str, Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self._data = data or {}

    def json(self) -> dict[str, Any]:
        return self._data


class MockSession:
    def __init__(self) -> None:
        self.responses: list[MockResponse] = []
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def execute_with_retry(self, method: str, url: str, **kwargs: Any) -> MockResponse:
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError(f"Unexpected request: {method} {url}")
        return self.responses.pop(0)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def close(self) -> None:
        return None


@pytest.fixture
def mock_session() -> MockSession:
    return MockSession()


@pytest.fixture
def mock_response() -> type[MockResponse]:
    return MockResponse


@pytest.fixture
def task_config() -> TaskConfig:
    return TaskConfig(
        task_id="VANS-TEST",
        site_key="vans",
        target_url="https://www.vans.com/en-us/p/shoes/VN000EAABOP",
        sku_or_variant_id="VN000EAABOP",
        target_size="10.5",
        monitor_delay_ms=100,
        checkout_delay_ms=0,
    )


@pytest.fixture
def buyer_profile() -> BuyerProfile:
    return BuyerProfile(
        profile_id="test-profile",
        profile_name="Test Profile",
        shipping=ShippingAddress(
            first_name="Test",
            last_name="Buyer",
            email="test@example.com",
            phone="4155550100",
            address1="10 Main Street",
            address2=None,
            city="San Francisco",
            state_code="CA",
            postal_code="94103",
            country_code="US",
        ),
        payment=PaymentMethod(
            cardholder_name="Test Buyer",
            card_number="4111111111111111",
            exp_month=12,
            exp_year=2030,
            cvv="123",
        ),
    )
