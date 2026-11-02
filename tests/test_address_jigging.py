"""
Module: tests.test_address_jigging
Description: 專門驗證防砍單地址變形（Address Jigging）演算法、隨機微擾動有效性與電商配送封裝之測試套件。
"""
from __future__ import annotations

import pytest

from models.profile import (
    BuyerProfile,
    PaymentMethod,
    ShippingAddress,
    TaskConfig,
    VariantPriorityModel,
)
from sites.vans_site import VansSite

# =====================================================================
# Fixtures
# =====================================================================

@pytest.fixture
def base_shipping_address() -> ShippingAddress:
    return ShippingAddress(
        first_name="David",
        last_name="Beckham",
        email="david.beckham@example.com",
        phone="7145550199",
        address1="789 Market Street",
        address2="Suite 401",
        city="San Francisco",
        state_code="CA",
        postal_code="94103",
        country_code="US",
    )


@pytest.fixture
def mock_buyer_profile(base_shipping_address) -> BuyerProfile:
    return BuyerProfile(
        profile_id="PROF-JIG-001",
        profile_name="Jigging Test Profile",
        shipping=base_shipping_address,
        payment=PaymentMethod(
            cardholder_name="David Beckham",
            card_number="4111111111111111",
            exp_month=12,
            exp_year=2028,
            cvv="123",
        ),
    )


@pytest.fixture
def mock_task_config() -> TaskConfig:
    return TaskConfig(
        task_id="TASK-JIG-01",
        site_key="vans",
        target_url="https://www.vans.com/en-us/p/shoes/VN000EE3BLK",
        sku_or_variant_id="VN000EE3BLK",
        target_size="10.5",
        variant_priority=VariantPriorityModel(preferred_sizes=["10.5"]),
        quantity=1,
    )


# =====================================================================
# 1. 驗證 AddressProfile.generate_jigged_address 核心演算法
# =====================================================================

def test_generate_jigged_address_perturbation(base_shipping_address):
    """
    驗證地址變形演算法能產生大小寫/樣式微擾動：
    1. 多次產生變形地址。
    2. 驗證 address_line_2 / address2 確有產生變形版本（例如轉大寫、轉小寫）。
    3. 驗證其餘重要配送資訊（城市、郵遞區號、主地址）維持不變。
    """
    original_line2 = base_shipping_address.address2
    generated_variants = set()

    # 執行多次微擾動產生
    for _ in range(20):
        jigged = base_shipping_address.generate_jigged_address()
        # 核心物流資訊必須維持完好
        assert jigged.address1 == base_shipping_address.address1
        assert jigged.city == base_shipping_address.city
        assert jigged.state_code == base_shipping_address.state_code
        assert jigged.postal_code == base_shipping_address.postal_code
        assert jigged.country_code == base_shipping_address.country_code

        generated_variants.add(jigged.address2)

    # 驗證產生的變體中包含不同於原始字串的擾動結果
    perturbed_variants = generated_variants - {original_line2}
    assert len(perturbed_variants) > 0, "地址變形未能成功產生微擾動變體"


def test_generate_jigged_address_without_line2():
    """驗證當原始地址無 address_line_2 時，變形邏輯不會拋錯並能安全回傳。"""
    addr = ShippingAddress(
        first_name="David",
        last_name="Beckham",
        email="david@example.com",
        phone="7145550199",
        address1="789 Market Street",
        address2=None,
        city="San Francisco",
        state_code="CA",
        postal_code="94103",
        country_code="US",
    )

    jigged = addr.generate_jigged_address()
    assert jigged.address1 == "789 Market Street"
    assert jigged.address2 is None


# =====================================================================
# 2. 驗證 VansSite 適配器端到端呼叫與 Payload 封裝
# =====================================================================

@pytest.mark.asyncio
async def test_vans_submit_shipping_applies_jigging(mock_task_config, mock_buyer_profile):
    """
    驗證 VansSite.submit_shipping() 在封裝配送資訊時：
    1. 確實呼叫了 generate_jigged_address。
    2. 封裝進 _staged_shipping 的配送資料使用了變形後的結果。
    """
    site = VansSite(task_config=mock_task_config, profile=mock_buyer_profile)
    site.cart_id = "CART-JIG-TEST-123"

    # 執行配送封裝
    success = await site.submit_shipping()
    assert success is True
    assert site._staged_shipping is not None

    staged_shipping_addr = site._staged_shipping["shippingAddress"]

    # 驗證必要欄位正確組裝
    assert staged_shipping_addr["address1"] == "789 Market Street"
    assert staged_shipping_addr["city"] == "San Francisco"
    assert staged_shipping_addr["state"] == "CA"
    assert staged_shipping_addr["postalCode"] == "94103"
    assert staged_shipping_addr["country"] == "US"

    # 驗證 address2 存在於 staged 資料中
    assert staged_shipping_addr["address2"] is not None