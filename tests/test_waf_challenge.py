"""Verify that WAF/403 responses stop a task without challenge solving or retries."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.exceptions import WAFChallengeError
from core.state_machine import AsyncStateMachine, CheckoutState
from models.profile import (
    BuyerProfile,
    PaymentMethod,
    ShippingAddress,
    TaskConfig,
    VariantPriorityModel,
)
from sites.vans_site import VansSite, VansVariant

# =====================================================================
# Fixtures: 買家資料、任務設定與狀態機
# =====================================================================

@pytest.fixture
def mock_buyer_profile() -> BuyerProfile:
    return BuyerProfile(
        profile_id="PROF-WAF-001",
        profile_name="WAF Test Profile",
        shipping=ShippingAddress(
            first_name="John",
            last_name="Doe",
            email="waf_test@example.com",
            phone="7145550199",
            address1="123 Security Blvd",
            city="Costa Mesa",
            state_code="CA",
            postal_code="92626",
            country_code="US",
        ),
        payment=PaymentMethod(
            cardholder_name="John Doe",
            card_number="4111111111111111",
            exp_month=12,
            exp_year=2028,
            cvv="123",
        ),
    )


@pytest.fixture
def mock_task_config() -> TaskConfig:
    return TaskConfig(
        task_id="TASK-WAF-01",
        site_key="vans",
        target_url="https://www.vans.com/en-us/p/shoes/VN000EE3BLK",
        sku_or_variant_id="VN000EE3BLK",
        target_size="10.5",
        variant_priority=VariantPriorityModel(preferred_sizes=["10.5", "ANY_SIZE"]),
        quantity=1,
    )


# =====================================================================
# 1. WAF/403 stops the add-to-cart task without invoking a token solver.
# =====================================================================

@pytest.mark.asyncio
async def test_add_to_cart_waf_challenge_stops(mock_task_config, mock_buyer_profile):
    """A configured token solver must not cause a 403 request to be retried."""
    fsm = AsyncStateMachine(task_id="TASK-WAF-01", initial_state=CheckoutState.ATC)

    # 模擬連線層 Session
    mock_session = MagicMock()
    mock_session.inject_cookies = MagicMock()

    # 第一次請求回應 403 (PerimeterX block)
    resp_waf_block = MagicMock()
    resp_waf_block.status_code = 403
    resp_waf_block.text = "<html><title>Access Denied</title><body>_px captcha required</body></html>"

    mock_session.execute_with_retry = AsyncMock(return_value=resp_waf_block)

    # 模擬 TokenHarvester
    mock_harvester = MagicMock()
    mock_harvester.get_valid_token = AsyncMock(return_value={"_px3": "fresh_valid_px3_token_xyz"})

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=fsm,
        token_harvester=mock_harvester,
        max_waf_retries=1,
    )
    site.cart_id = "CART-INIT-001"
    site.active_variant = VansVariant(
        product_id="VN:000EAA:BOP:105:M:1:",
        upc="198270230408",
        size_label="10.5",
        in_stock=True,
    )

    with pytest.raises(WAFChallengeError):
        await site.add_to_cart()

    mock_harvester.get_valid_token.assert_not_awaited()
    mock_session.inject_cookies.assert_not_called()
    assert mock_session.execute_with_retry.await_count == 1
    assert fsm.current_state == CheckoutState.FAILED


# =====================================================================
# 2. WAF/403 stops checkout without invoking a token solver.
# =====================================================================

@pytest.mark.asyncio
async def test_submit_checkout_waf_challenge_stops(mock_task_config, mock_buyer_profile):
    """A checkout 403 is terminal and must not trigger a retry."""
    fsm = AsyncStateMachine(task_id="TASK-WAF-01", initial_state=CheckoutState.PAYMENT)

    mock_session = MagicMock()
    mock_session.inject_cookies = MagicMock()

    resp_waf_block = MagicMock()
    resp_waf_block.status_code = 403
    resp_waf_block.text = "<html>perimeterx blocked</html>"

    mock_session.execute_with_retry = AsyncMock(return_value=resp_waf_block)

    mock_harvester = MagicMock()
    mock_harvester.get_valid_token = AsyncMock(return_value={"_px3": "px3_token_pass"})

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=fsm,
        token_harvester=mock_harvester,
        max_waf_retries=1,
    )
    site.cart_id = "CART-CHECKOUT-WAF"
    await site.submit_shipping()

    with pytest.raises(WAFChallengeError):
        await site.submit_checkout()

    mock_harvester.get_valid_token.assert_not_awaited()
    mock_session.inject_cookies.assert_not_called()
    assert mock_session.execute_with_retry.await_count == 1
    assert fsm.current_state == CheckoutState.FAILED


# =====================================================================
# 3. WAF stops when no token solver is configured.
# =====================================================================

@pytest.mark.asyncio
async def test_waf_challenge_fails_without_harvester(mock_task_config, mock_buyer_profile):
    """Verify a WAF response transitions the task to FAILED and is re-raised."""
    fsm = AsyncStateMachine(task_id="TASK-WAF-01", initial_state=CheckoutState.ATC)

    mock_session = MagicMock()
    resp_waf_block = MagicMock()
    resp_waf_block.status_code = 403
    resp_waf_block.text = "<html>Access Denied By PerimeterX</html>"
    mock_session.execute_with_retry = AsyncMock(return_value=resp_waf_block)

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=fsm,
        token_harvester=None,  # 故意不配置 harvester
        max_waf_retries=1,
    )
    site.cart_id = "CART-NO-HARVESTER"
    site.active_variant = VansVariant(
        product_id="VN:VAR:01",
        size_label="10.5",
        in_stock=True,
    )

    with pytest.raises(WAFChallengeError):
        await site.add_to_cart()

    assert fsm.current_state == CheckoutState.FAILED


# =====================================================================
# 4. A configured TokenHarvester is not invoked for WAF responses.
# =====================================================================

@pytest.mark.asyncio
async def test_waf_challenge_stops_even_when_harvester_is_configured(
    mock_task_config, mock_buyer_profile
):
    """Configured token solving must not run after a 403/WAF response."""
    fsm = AsyncStateMachine(task_id="TASK-WAF-01", initial_state=CheckoutState.ATC)

    mock_session = MagicMock()
    resp_waf_block = MagicMock()
    resp_waf_block.status_code = 403
    resp_waf_block.text = "<html>captcha</html>"
    mock_session.execute_with_retry = AsyncMock(return_value=resp_waf_block)

    mock_harvester = MagicMock()
    mock_harvester.get_valid_token = AsyncMock(return_value={"_px3": "must_not_be_used"})

    site = VansSite(
        task_config=mock_task_config,
        profile=mock_buyer_profile,
        session=mock_session,
        state_machine=fsm,
        token_harvester=mock_harvester,
        max_waf_retries=1,
    )
    site.cart_id = "CART-TIMEOUT"
    site.active_variant = VansVariant(
        product_id="VN:VAR:01",
        size_label="10.5",
        in_stock=True,
    )

    with pytest.raises(WAFChallengeError):
        await site.add_to_cart()

    mock_harvester.get_valid_token.assert_not_awaited()
    assert mock_session.execute_with_retry.await_count == 1
    assert fsm.current_state == CheckoutState.FAILED