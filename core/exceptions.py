from __future__ import annotations

from typing import Any


class CheckoutException(Exception):
    """Base exception for all checkout lifecycle operations."""

    def __init__(
        self,
        message: str,
        task_id: str | None = None,
        status_code: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.task_id = task_id
        self.status_code = status_code
        self.extra = kwargs

    def __str__(self) -> str:
        prefix = f"[{self.task_id}] " if self.task_id else ""
        code = f" (HTTP {self.status_code})" if self.status_code else ""
        return f"{prefix}{self.message}{code}"


BotException = CheckoutException
CheckoutFrameworkError = CheckoutException


class ResourceCleanupError(CheckoutException):
    """Raised when closing sessions or releasing proxy leases fails during cleanup."""

    def __init__(
        self,
        message: str = "Resource cleanup failed",
        task_id: str | None = None,
        errors: list[tuple[str, BaseException]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, task_id=task_id, **kwargs)
        self.errors: list[tuple[str, BaseException]] = list(errors or ())


class ProductUnavailableError(CheckoutException):
    """Raised when target product or SKU is not found."""


class OutOfStockError(CheckoutException):
    """Raised when target variant inventory is depleted."""


class CartCheckoutError(CheckoutException):
    """Generic error encountered during cart manipulations."""


class ServiceUnavailableError(CheckoutException):
    """Raised on upstream 5xx or server timeout."""


class NetworkTransportError(ServiceUnavailableError):
    """Raised when an HTTP request fails before receiving a response."""


class SessionExpiredError(CheckoutException):
    """Raised when a site session is no longer valid."""

    def __init__(
        self,
        message: str = "Session expired",
        task_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, task_id=task_id, **kwargs)


class PaymentDeclinedError(CheckoutException):
    """Raised when card authorization or gateway returns rejection."""

    def __init__(
        self,
        message: str = "Payment authorization declined",
        reason: str | None = None,
        task_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, task_id=task_id, **kwargs)
        self.reason = reason


class RateLimitError(CheckoutException):
    """Raised on HTTP 429 or excessive request pacing."""

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: float = 2.0,
        task_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, task_id=task_id, status_code=429, **kwargs)
        self.retry_after = retry_after


class InvalidStateTransitionError(CheckoutException):
    """Raised when an illegal FSM edge transition is requested."""

    def __init__(
        self,
        from_state: str | None = None,
        to_state: str | None = None,
        task_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        msg = f"Invalid state transition: {from_state} -> {to_state}"
        super().__init__(msg, task_id=task_id, **kwargs)
        self.from_state = from_state
        self.to_state = to_state


StateMachineError = InvalidStateTransitionError


class WAFChallengeError(CheckoutException):
    """Raised when HTTP responses show PerimeterX / Cloudflare / Datadome block patterns."""

    def __init__(
        self,
        message: str = "WAF Challenge or Block encountered",
        task_id: str | None = None,
        status_code: int = 403,
        waf_type: str = "PerimeterX",
        block_type: str | None = None,
        block_script: str | None = None,
        raw_html: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, task_id=task_id, status_code=status_code, **kwargs)
        self.waf_type = waf_type
        self.block_type = block_type or waf_type
        self.block_script = block_script
        self.raw_html = raw_html


# Backward-compatible spelling used by existing callers.
WafChallengeError = WAFChallengeError


class CaptchaRequiredError(WAFChallengeError):
    """Raised when WAF demands an interactive captcha challenge (e.g. PX Captcha, Turnstile)."""

    def __init__(
        self,
        message: str = "Interactive CAPTCHA challenge required",
        site_key: str | None = None,
        page_url: str | None = None,
        captcha_type: str = "perimeterx",
        task_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, task_id=task_id, status_code=403, **kwargs)
        self.site_key = site_key
        self.page_url = page_url
        self.captcha_type = captcha_type