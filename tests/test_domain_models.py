import pytest
from pydantic import SecretStr, ValidationError

from core.exceptions import (
    CheckoutException,
    OutOfStockError,
    RateLimitError,
    SessionExpiredError,
    WAFChallengeError,
    WafChallengeError,
)
from models.profile import AddressProfile, PaymentMethod, ShippingAddress


def _address(**overrides: object) -> AddressProfile:
    data: dict[str, object] = {
        "first_name": "Taylor",
        "last_name": "Sample",
        "email": "taylor@example.com",
        "phone": "4155550100",
        "address_line_1": "10 Main Street",
        "address_line_2": "Apt 4",
        "city": "San Francisco",
        "state_code": "ca",
        "postal_code": "94103",
        "country_code": "us",
    }
    data.update(overrides)
    return AddressProfile(**data)


def test_checkout_exceptions_include_task_context() -> None:
    error = OutOfStockError("No stock", task_id="TASK-007")

    assert isinstance(error, CheckoutException)
    assert error.message == "No stock"
    assert error.task_id == "TASK-007"
    assert str(error) == "[TASK-007] No stock"
    assert RateLimitError(retry_after=1.5).retry_after == 1.5
    assert WAFChallengeError(block_type="CF").block_type == "CF"
    assert isinstance(WafChallengeError(status_code=403), WAFChallengeError)
    assert isinstance(SessionExpiredError(), CheckoutException)


def test_address_profile_accepts_legacy_names_and_jigs_only_line_two() -> None:
    legacy = ShippingAddress(
        first_name="Taylor",
        last_name="Sample",
        email="taylor@example.com",
        phone="4155550100",
        address1="10 Main Street",
        address2="Apt 4",
        city="San Francisco",
        state_code="ca",
        postal_code="94103",
    )
    jigged = _address().generate_jigged_address()

    assert legacy.address_line_1 == "10 Main Street"
    assert legacy.address1 == "10 Main Street"
    assert jigged.address_line_1 == "10 Main Street"
    assert jigged.address_line_2 is not None
    assert jigged.address_line_2.casefold() == "apt 4"
    assert jigged.address_line_2 != "Apt 4"


def test_payment_secrets_are_masked_and_card_number_uses_luhn() -> None:
    payment = PaymentMethod(
        cardholder_name="Taylor Sample",
        card_number="4111 1111 1111 1111",
        exp_month=12,
        exp_year=2030,
        cvv="123",
    )

    assert isinstance(payment.card_number, SecretStr)
    assert isinstance(payment.cvv, SecretStr)
    assert "4111111111111111" not in repr(payment)
    assert "123" not in repr(payment)
    assert payment.card_number.get_secret_value() == "4111111111111111"

    with pytest.raises(ValidationError):
        PaymentMethod(
            cardholder_name="Taylor Sample",
            card_number="4111 1111 1111 1112",
            exp_month=12,
            exp_year=2030,
            cvv="123",
        )
