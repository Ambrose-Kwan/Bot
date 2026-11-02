from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from typing import Any

import pytest

from core.exceptions import InvalidStateTransitionError, RateLimitError
from core.state_machine import CheckoutState
from models.profile import BuyerProfile, PaymentMethod, TaskConfig
from sites.vans_site import (
    VansSite,
    VansVariant,
    _size_aliases,
    rank_available_variants,
)


def test_size_aliases_cover_equivalent_us_size_labels() -> None:
    aliases = _size_aliases("US 10.5 Men = 12 Women")

    assert "US 10.5 Men" in aliases
    assert "10.5 Men" in aliases
    assert "10.5" in aliases
    assert "US 10.5" in aliases
    assert "12" in aliases


def test_rank_available_variants_preserves_preference_order_and_deduplicates() -> None:
    first = VansVariant(product_id="VAR-105", size_label="10.5 Men")
    second = VansVariant(product_id="VAR-110", size_label="11 Men")
    unavailable = VansVariant(product_id="VAR-115", size_label="11.5 Men", in_stock=False)
    by_alias = {
        "10.5": first,
        "US 10.5": first,
        "11": second,
        "11.5": unavailable,
    }

    ranked = rank_available_variants(["US 10.5", "10.5", "11", "11.5"], by_alias)

    assert [variant.product_id for variant in ranked] == ["VAR-105", "VAR-110"]


@pytest.mark.asyncio
async def test_check_stock_uses_next_available_preferred_size(
    task_config: TaskConfig,
    buyer_profile: BuyerProfile,
    mock_session: Any,
    mock_response: Any,
) -> None:
    task_config.variant_priority.preferred_sizes = ["10.5", "11"]
    mock_session.responses.append(
        mock_response(
            data={
                "product": {
                    "price": 85,
                    "variants": [
                        {
                            "productId": "VAR-105",
                            "size": "10.5 Men",
                            "inStock": False,
                            "availableQuantity": 0,
                        },
                        {
                            "productId": "VAR-110",
                            "size": "11 Men",
                            "inStock": True,
                            "availableQuantity": 2,
                            "price": 85,
                        },
                    ],
                }
            }
        )
    )
    site = VansSite(task_config, buyer_profile, session=mock_session)

    result = await site.check_stock()

    assert result.in_stock
    assert result.variant_id == "VAR-110"
    assert site.active_variant is not None
    assert site.active_variant.size_label == "11 Men"
    assert mock_session.calls[0][0] == "GET"


@pytest.mark.asyncio
async def test_submit_shipping_serializes_configured_address(
    task_config: TaskConfig,
    buyer_profile: BuyerProfile,
    mock_session: Any,
) -> None:
    site = VansSite(task_config, buyer_profile, session=mock_session)
    site.cart_id = "cart-test"

    accepted = await site.submit_shipping()

    assert accepted
    assert site._staged_shipping is not None
    assert site._staged_shipping["customer"] == {
        "email": "test@example.com",
        "firstName": "Test",
        "lastName": "Buyer",
        "phone": "4155550100",
    }
    assert site._staged_shipping["shippingAddress"] == {
        "firstName": "Test",
        "lastName": "Buyer",
        "address1": "10 Main Street",
        "address2": "",
        "city": "San Francisco",
        "state": "CA",
        "postalCode": "94103",
        "country": "US",
        "phone": "4155550100",
    }


@pytest.mark.asyncio
async def test_transition_fsm_rejects_transition_out_of_terminal_state(
    task_config: TaskConfig,
    buyer_profile: BuyerProfile,
    mock_session: Any,
) -> None:
    class _TerminalStateMachine:
        current_state = CheckoutState.SUCCESS
        is_terminal = True

        async def transition(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("terminal transition must be rejected before dispatch")

    site = VansSite(task_config, buyer_profile, session=mock_session)
    site.state_machine = _TerminalStateMachine()

    with pytest.raises(InvalidStateTransitionError):
        await site._transition_fsm(CheckoutState.FAILED, reason="unexpected failure")


@pytest.mark.asyncio
async def test_execute_fast_checkout_propagates_cancellation(
    task_config: TaskConfig,
    buyer_profile: BuyerProfile,
    mock_session: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    site = VansSite(task_config, buyer_profile, session=mock_session)

    async def cancel_during_atc() -> Any:
        raise asyncio.CancelledError

    monkeypatch.setattr(site, "add_to_cart", cancel_during_atc)

    with pytest.raises(asyncio.CancelledError):
        await site.execute_fast_checkout()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_after", "expected"),
    [
        ("5.5", 5.5),
        ("1200", 10.0),
        ("invalid", 2.0),
        ("-1", 2.0),
        ("", 2.0),
    ],
)
async def test_checkout_429_parses_and_bounds_retry_after(
    task_config: TaskConfig,
    buyer_profile: BuyerProfile,
    mock_session: Any,
    mock_response: Any,
    monkeypatch: pytest.MonkeyPatch,
    retry_after: str,
    expected: float,
) -> None:
    monkeypatch.setattr(
        PaymentMethod,
        "encrypt_cse",
        lambda *_args, **_kwargs: {
            "encryptedCardNumber": "test",
            "encryptedExpiryMonth": "test",
            "encryptedExpiryYear": "test",
            "encryptedSecurityCode": "test",
            "generationtime": "test",
        },
    )
    mock_session.max_retry_after = 10.0
    mock_session.responses.append(
        mock_response(status_code=429, headers={"Retry-After": retry_after})
    )
    site = VansSite(task_config, buyer_profile, session=mock_session)
    site.cart_id = "cart-test"
    site._staged_shipping = {"shippingAddress": {}}

    with pytest.raises(RateLimitError) as exc_info:
        await site.submit_checkout()

    assert exc_info.value.retry_after == expected


@pytest.mark.asyncio
async def test_checkout_429_accepts_http_date_retry_after(
    task_config: TaskConfig,
    buyer_profile: BuyerProfile,
    mock_session: Any,
    mock_response: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        PaymentMethod,
        "encrypt_cse",
        lambda *_args, **_kwargs: {
            "encryptedCardNumber": "test",
            "encryptedExpiryMonth": "test",
            "encryptedExpiryYear": "test",
            "encryptedSecurityCode": "test",
            "generationtime": "test",
        },
    )
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=20)
    mock_session.responses.append(
        mock_response(
            status_code=429,
            headers={"Retry-After": format_datetime(retry_at, usegmt=True)},
        )
    )
    site = VansSite(task_config, buyer_profile, session=mock_session)
    site.cart_id = "cart-test"
    site._staged_shipping = {"shippingAddress": {}}

    with pytest.raises(RateLimitError) as exc_info:
        await site.submit_checkout()

    assert 0 < exc_info.value.retry_after <= 20