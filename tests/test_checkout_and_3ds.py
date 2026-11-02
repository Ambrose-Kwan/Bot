"""
Module: tests.test_checkout_and_3ds
Description: 針對 Vans 結帳端點、CSE 金流公鑰加密、3DS 跳轉辨識與 Playwright 瀏覽器接管的完整 pytest 測試套件。
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from core.exceptions import (
    PaymentDeclinedError,
    RateLimitError,
)
from core.state_machine import AsyncStateMachine, CheckoutState
from models.profile import (
    BuyerProfile,
    PaymentMethod,
    ShippingAddress,
    TaskConfig,
    VariantPriorityModel,
)
from sites.base_site import CheckoutResult
from sites.vans_site import VansSite


@pytest.fixture(scope="session")
def valid_rsa_public_key_pem() -> str:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    pem_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return pem_bytes.decode("utf-8")


@pytest.fixture
def mock_buyer_profile() -> BuyerProfile:
    return BuyerProfile(
        profile_id="PROF-TEST-001",
        profile_name="Production Test Profile",
        shipping=ShippingAddress(
            first_name="Alex",
            last_name="Chen",
            email="alex.chen@example.com",
            phone="7145550199",
            address1="123 Main St",
            address2="Suite 401",
            city="Costa Mesa",
            state_code="CA",
            postal_code="92626",
            country_code="US",
        ),
        payment=PaymentMethod(
            cardholder_name="Alex Chen",
            card_number="4111111111111111",
            exp_month=10,
            exp_year=2028,
            cvv="789",
        ),
    )


@pytest.fixture
def mock_task_config(valid_rsa_public_key_pem) -> TaskConfig:
    return TaskConfig(
        task_id="TASK-VANS-E2E-01",
        site_key="vans",
        target_url="https://www.vans.com/en-us/p/shoes/VN000EE3BLK",
        sku_or_variant_id="VN000EE3BLK",
        target_size="10.5",
        variant_priority=VariantPriorityModel(preferred_sizes=["10.5", "10", "ANY_SIZE"]),
        quantity=1,
    )


@pytest.fixture
def mock_fsm() -> AsyncStateMachine:
    return AsyncStateMachine(
        task_id="TASK-VANS-E2E-01",
        initial_state=CheckoutState.PAYMENT,
    )


@pytest.mark.asyncio
async def test_checkout_direct_success(mock_task_config, mock_buyer_profile, mock_fsm):
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.json.return_value = {
        "status": "ORDER_PLACED",
        "orderNumber": "VN-US-20261025-001",
        "total": 95.00,
        "currency": "USD",
    }
    mock_session.execute_with_retry = AsyncMock(return_value=mock_response)

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=mock_fsm,
    )
    site.cart_id = "CART-VF-DIRECT-888"
    
    await site.submit_shipping()
    result: CheckoutResult = await site.submit_checkout()

    assert result.success is True
    assert result.order_id == "VN-US-20261025-001"
    assert mock_fsm.current_state == CheckoutState.SUCCESS

    call_kwargs = mock_session.execute_with_retry.await_args.kwargs
    sent_payload = call_kwargs["json"]
    
    assert sent_payload["cartId"] == "CART-VF-DIRECT-888"
    assert "cardNumber" not in sent_payload["payment"]
    assert "encryptedCardNumber" in sent_payload["payment"]
    assert sent_payload["payment"]["encryptedCardNumber"].startswith("cse_")


@pytest.mark.asyncio
async def test_checkout_3ds_challenge_handoff_success(
    mock_task_config, mock_buyer_profile, mock_fsm
):
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.json.return_value = {
        "status": "3DS_REQUIRED",
        "action": "CHALLENGE_REDIRECT",
        "redirectUrl": "https://cardsecurity.bank.com/acs/challenge?tx=3ds_99214a",
        "threeDSecure": {
            "acsUrl": "https://cardsecurity.bank.com/acs/challenge?tx=3ds_99214a",
            "transactionId": "tx-3ds-99214a",
        },
    }
    mock_session.execute_with_retry = AsyncMock(return_value=mock_response)

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=mock_fsm,
    )
    site.cart_id = "CART-VF-3DS-777"
    await site.submit_shipping()

    mock_handoff = AsyncMock(
        return_value={
            "success": True,
            "order_id": "VN-3DS-CONFIRMED-8899",
            "final_url": "https://www.vans.com/en-us/checkout/order-confirmation",
            "elapsed_seconds": 18.5,
        }
    )

    with patch("sites.vans_site.handoff_to_browser", mock_handoff):
        result: CheckoutResult = await site.submit_checkout()

    assert result.success is True
    assert result.order_id == "VN-3DS-CONFIRMED-8899"
    assert mock_fsm.current_state == CheckoutState.SUCCESS

    mock_handoff.assert_awaited_once()
    handoff_args = mock_handoff.await_args.kwargs
    assert handoff_args["target_url"] == "https://cardsecurity.bank.com/acs/challenge?tx=3ds_99214a"
    assert handoff_args["headless"] is False


@pytest.mark.asyncio
async def test_checkout_3ds_timeout_or_closed(
    mock_task_config, mock_buyer_profile, mock_fsm
):
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.json.return_value = {
        "status": "CHALLENGE_REQUIRED",
        "redirectUrl": "https://acs.issuer.com/otp",
    }
    mock_session.execute_with_retry = AsyncMock(return_value=mock_response)

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=mock_fsm,
    )
    site.cart_id = "CART-VF-3DS-TIMEOUT"
    await site.submit_shipping()

    mock_handoff = AsyncMock(
        return_value={
            "success": False,
            "order_id": None,
            "final_url": "https://acs.issuer.com/otp",
            "elapsed_seconds": 180.0,
        }
    )

    with (
        patch("sites.vans_site.handoff_to_browser", mock_handoff),
        pytest.raises(PaymentDeclinedError) as exc_info,
    ):
        await site.submit_checkout()

    # 配合 PaymentDeclinedError 實際回傳的訊息進行斷言
    assert "Payment" in str(exc_info.value) or "declined" in str(exc_info.value)
    assert mock_fsm.current_state == CheckoutState.FAILED


@pytest.mark.asyncio
async def test_checkout_card_declined(mock_task_config, mock_buyer_profile, mock_fsm):
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.headers = {}
    mock_response.json.return_value = {
        "status": "DECLINED",
        "errorMessage": "Insufficient Funds / Do Not Honor",
    }
    mock_session.execute_with_retry = AsyncMock(return_value=mock_response)

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=mock_fsm,
    )
    site.cart_id = "CART-DECLINED"
    await site.submit_shipping()

    result: CheckoutResult = await site.submit_checkout()

    assert result.success is False
    assert "Insufficient Funds" in str(result.error_message)
    assert mock_fsm.current_state == CheckoutState.FAILED


@pytest.mark.asyncio
async def test_checkout_rate_limited(mock_task_config, mock_buyer_profile, mock_fsm):
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.headers = {"Retry-After": "3.5"}
    mock_response.json.return_value = {"error": "Too Many Requests"}
    mock_session.execute_with_retry = AsyncMock(return_value=mock_response)

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=mock_fsm,
    )
    site.cart_id = "CART-RATE-LIMIT"
    await site.submit_shipping()

    with pytest.raises(RateLimitError) as exc_info:
        await site.submit_checkout()

    assert exc_info.value.status_code == 429
    assert exc_info.value.retry_after == 3.5