from __future__ import annotations

import base64
import json
import os
import random
import re
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

try:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.serialization import (
        load_der_public_key,
        load_pem_public_key,
    )
    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False


def _luhn_checksum(card_number_digits: str) -> bool:
    """Verifies credit card number via standard ISO/IEC 7812 Luhn Algorithm."""
    digits = [int(d) for d in card_number_digits if d.isdigit()]
    if len(digits) < 12:
        return False
    checksum = 0
    reverse_digits = digits[::-1]
    for i, d in enumerate(reverse_digits):
        if i % 2 == 1:
            doubled = d * 2
            checksum += (doubled - 9) if doubled > 9 else doubled
        else:
            checksum += d
    return checksum % 10 == 0


class CSEEngine:
    """
    Client-Side Encryption (CSE) Engine.
    Simulates Adyen / WebCrypto standard RSA-OAEP and AES-256-GCM hybrid encryption:
    1. Generates 256-bit ephemeral AES key and 96-bit random IV/nonce.
    2. Encrypts card field payload (with ISO 8601 timestamp) using AES-256-GCM.
    3. Encrypts ephemeral AES key using gateway RSA Public Key via RSA-OAEP (SHA-256).
    4. Bundles and Base64-encodes payload with 'cse_' prefix.
    """

    @staticmethod
    def _parse_public_key(public_key_str: str) -> Any:
        raw = public_key_str.strip()
        if "BEGIN PUBLIC KEY" in raw:
            return load_pem_public_key(raw.encode("utf-8"))
        try:
            der_bytes = base64.b64decode(raw)
            return load_der_public_key(der_bytes)
        except ValueError:
            pem_wrapped = f"-----BEGIN PUBLIC KEY-----\n{raw}\n-----END PUBLIC KEY-----"
            return load_pem_public_key(pem_wrapped.encode("utf-8"))

    @classmethod
    def encrypt_field(cls, field_value: str, public_key_str: str, gen_time: datetime | None = None) -> str:
        gen_time = gen_time or datetime.now(timezone.utc)
        payload = {
            "value": field_value,
            "generationtime": gen_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }
        plaintext = json.dumps(payload, separators=(",", ":")).encode("utf-8")

        # 1. 256-bit AES key and 12-byte nonce
        aes_key = AESGCM.generate_key(bit_length=256)
        nonce = os.urandom(12)
        aesgcm = AESGCM(aes_key)
        ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext, None)

        # 2. RSA-OAEP encryption of AES key
        rsa_key = cls._parse_public_key(public_key_str)
        encrypted_aes_key = rsa_key.encrypt(
            aes_key,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )

        # 3. Base64 payload bundling
        bundle = {
            "version": "1_0_0",
            "key": base64.b64encode(encrypted_aes_key).decode("ascii"),
            "iv": base64.b64encode(nonce).decode("ascii"),
            "data": base64.b64encode(ciphertext_with_tag).decode("ascii"),
        }
        encoded_bundle = base64.b64encode(json.dumps(bundle, separators=(",", ":")).encode("utf-8")).decode("ascii")
        return f"cse_{encoded_bundle}"


class AddressProfile(BaseModel):
    """Structured postal address with compatibility aliases for the legacy field names."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    first_name: str = Field(..., min_length=1, max_length=50, description="Recipient first name")
    last_name: str = Field(..., min_length=1, max_length=50, description="Recipient last name")
    email: str = Field(..., description="Order confirmation email")
    phone: str = Field(..., min_length=7, max_length=20, description="Contact phone number")
    address_line_1: str = Field(
        ...,
        min_length=3,
        max_length=100,
        validation_alias=AliasChoices("address_line_1", "address1"),
        serialization_alias="address1",
        description="Street address line 1",
    )
    address_line_2: str | None = Field(
        None,
        max_length=100,
        validation_alias=AliasChoices("address_line_2", "address2"),
        serialization_alias="address2",
        description="Apartment, suite, unit",
    )
    city: str = Field(..., min_length=1, max_length=60, description="City")
    state_code: str = Field(..., min_length=2, max_length=20, description="State, province, or region code")
    postal_code: str = Field(..., min_length=2, max_length=15, description="ZIP or postal code")
    country_code: str = Field("US", min_length=2, max_length=2, description="ISO 3166-1 alpha-2 country code")

    @field_validator("country_code", mode="before")
    @classmethod
    def normalize_country(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("state_code", mode="before")
    @classmethod
    def normalize_state(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        if not re.match(pattern, v):
            raise ValueError(f"Invalid email format: {v}")
        return v.strip().lower()

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    @property
    def address1(self) -> str:
        """Legacy access to the first address line."""
        return self.address_line_1

    @property
    def address2(self) -> str | None:
        """Legacy access to the second address line."""
        return self.address_line_2

    def generate_jigged_address(self) -> AddressProfile:
        """Return an equivalent address with a randomized casing of line 2."""
        line_2 = self.address_line_2
        if not line_2:
            return self.model_copy()

        variants = sorted({line_2.lower(), line_2.upper(), line_2.title()} - {line_2})
        if not variants:
            return self.model_copy()
        return self.model_copy(update={"address_line_2": random.choice(variants)})


class ShippingAddress(AddressProfile):
    """Backward-compatible name for an address used for shipping."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
        validate_assignment=True,
    )


class PaymentMethod(BaseModel):
    """Credit card or payment token specification with CSE support."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cardholder_name: str = Field(..., min_length=2, max_length=100)
    card_number: SecretStr = Field(..., description="Credit card number (15-19 digits)")
    exp_month: int = Field(..., ge=1, le=12, description="Expiration month (1-12)")
    exp_year: int = Field(..., ge=2024, le=2040, description="4-digit expiration year")
    cvv: SecretStr = Field(..., description="Card CVV / CVC code (3 or 4 digits)")

    @field_validator("card_number")
    @classmethod
    def validate_card_number(cls, v: SecretStr) -> SecretStr:
        raw = re.sub(r"[\s\-]", "", v.get_secret_value())
        if not (13 <= len(raw) <= 19 and raw.isascii() and raw.isdigit()):
            raise ValueError("Card number must be 13-19 digits")
        if not _luhn_checksum(raw):
            raise ValueError("Invalid credit card number (failed Luhn checksum)")
        return SecretStr(raw)

    @field_validator("cvv")
    @classmethod
    def validate_cvv(cls, v: SecretStr) -> SecretStr:
        val = v.get_secret_value().strip()
        if not (val.isascii() and val.isdigit() and 3 <= len(val) <= 4):
            raise ValueError("CVV must be 3 or 4 digits")
        return SecretStr(val)

    @property
    def masked_card_number(self) -> str:
        raw = self.card_number.get_secret_value()
        if len(raw) < 4:
            return "****"
        return f"**** **** **** {raw[-4:]}"

    @property
    def card_brand(self) -> str:
        raw = self.card_number.get_secret_value()
        if raw.startswith("4"):
            return "VISA"
        elif raw.startswith(("51", "52", "53", "54", "55")) or (
            2221 <= int(raw[:4]) <= 2720 if len(raw) >= 4 and raw[:4].isdigit() else False
        ):
            return "MASTERCARD"
        elif raw.startswith(("34", "37")):
            return "AMEX"
        elif raw.startswith(("6011", "65")):
            return "DISCOVER"
        return "GENERIC"

    def encrypt_cse(self, gateway_public_key: str) -> dict[str, str]:
        """Encrypts sensitive payment card attributes into PCI-DSS compliant CSE payload."""
        gen_time = datetime.now(timezone.utc)
        raw_card = self.card_number.get_secret_value()
        raw_cvv = self.cvv.get_secret_value()

        return {
            "encryptedCardNumber": CSEEngine.encrypt_field(raw_card, gateway_public_key, gen_time),
            "encryptedExpiryMonth": CSEEngine.encrypt_field(f"{self.exp_month:02d}", gateway_public_key, gen_time),
            "encryptedExpiryYear": CSEEngine.encrypt_field(str(self.exp_year), gateway_public_key, gen_time),
            "encryptedSecurityCode": CSEEngine.encrypt_field(raw_cvv, gateway_public_key, gen_time),
            "generationtime": gen_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }


class ProxyConfig(BaseModel):
    """Proxy definition with helper formatters."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    protocol: Literal["http", "https", "socks5"] = "http"
    host: str
    port: int = Field(..., ge=1, le=65535)
    username: str | None = None
    password: SecretStr | None = None

    @property
    def formatted_url(self) -> str:
        if self.username and self.password:
            pwd = self.password.get_secret_value()
            return f"{self.protocol}://{self.username}:{pwd}@{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"

    @classmethod
    def from_string(cls, raw: str) -> ProxyConfig:
        raw = raw.strip()
        protocol: Literal["http", "https", "socks5"] = "http"
        if "://" in raw:
            proto, rest = raw.split("://", 1)
            normalized_protocol = proto.lower()
            if normalized_protocol == "http":
                protocol = "http"
            elif normalized_protocol == "https":
                protocol = "https"
            elif normalized_protocol in ("socks5", "socks5h"):
                protocol = "socks5"
            else:
                raise ValueError("Unsupported proxy protocol")
            raw = rest

        parts = raw.split(":")
        if len(parts) == 2:
            return cls(protocol=protocol, host=parts[0], port=int(parts[1]))
        elif len(parts) == 4:
            if "@" in parts[1]:
                host = parts[1].split("@")[1]
                user, pwd = parts[0], parts[1].split("@")[0]
                port = int(parts[2])
                return cls(protocol=protocol, host=host, port=port, username=user, password=SecretStr(pwd))
            return cls(
                protocol=protocol,
                host=parts[0],
                port=int(parts[1]),
                username=parts[2],
                password=SecretStr(parts[3]),
            )
        elif "@" in raw:
            auth, host_port = raw.split("@", 1)
            user, pwd = auth.split(":", 1)
            host, port = host_port.split(":", 1)
            return cls(
                protocol=protocol,
                host=host,
                port=int(port),
                username=user,
                password=SecretStr(pwd),
            )
        raise ValueError(f"Unable to parse proxy format: {raw}")


class VariantPriorityModel(BaseModel):
    """Ordered size preferences used to select an available product variant."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    preferred_sizes: list[str] = Field(
        default_factory=list,
        description="Size labels ordered from highest to lowest preference",
    )

    @field_validator("preferred_sizes")
    @classmethod
    def validate_preferred_sizes(cls, sizes: list[str]) -> list[str]:
        normalized = [size.strip() for size in sizes]
        if any(not size for size in normalized):
            raise ValueError("Preferred sizes must not contain empty values")
        if len(normalized) != len(set(normalized)):
            raise ValueError("Preferred sizes must be unique")
        return normalized


class TaskConfig(BaseModel):
    """Specification for an automated checkout task instance."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    task_id: str = Field(..., description="Unique alphanumeric task identifier")
    site_key: str = Field(..., description="Target site identifier (e.g. 'shopify', 'nike', 'vans')")
    target_url: str = Field(..., description="Product page or direct checkout URL")
    sku_or_variant_id: str = Field(..., description="Target SKU, Variant ID, or PID")
    target_size: str | None = Field(None, description="Preferred size / variant label, e.g. '10.5' or 'M'")
    variant_priority: VariantPriorityModel = Field(default_factory=VariantPriorityModel)
    quantity: int = Field(1, ge=1, le=10, description="Desired checkout quantity")
    max_price: float | None = Field(None, ge=0.0, description="Price safety ceiling in USD/Local currency")

    monitor_delay_ms: int = Field(2500, ge=100, description="Delay between restock checks in MONITOR state")
    checkout_delay_ms: int = Field(500, ge=0, description="Delay before submitting payment in PAYMENT state")
    max_retries: int = Field(5, ge=1, le=20, description="Maximum retry count per step")
    timeout_seconds: float = Field(15.0, ge=3.0, le=60.0, description="Per-request socket timeout")

    impersonate_profile: str = Field(
        "chrome124",
        description="Browser TLS fingerprint impersonation profile (chrome124, safari17_0, edge101)",
    )
    bound_proxy: ProxyConfig | None = None
    proxy_pool: list[str] = Field(default_factory=list, description="Shared pool of proxy URIs")


class BuyerProfile(BaseModel):
    """Aggregated buyer identity grouping address, payment, and default proxy preferences."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    profile_id: str = Field(..., description="Unique profile reference ID")
    profile_name: str = Field(..., description="Human-friendly label (e.g. 'Personal Amex US')")
    shipping: ShippingAddress
    billing: ShippingAddress | None = Field(
        None, description="Billing address. If None, inherits directly from shipping address."
    )
    payment: PaymentMethod
    default_proxy: ProxyConfig | None = None

    @model_validator(mode="after")
    def populate_billing_default(self) -> BuyerProfile:
        if self.billing is None:
            self.billing = self.shipping.model_copy()
        return self

    def get_billing_address(self) -> ShippingAddress:
        return self.billing or self.shipping