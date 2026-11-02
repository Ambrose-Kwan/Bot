"""
Utils package for checkout automation.
"""
from .browser_handoff import (
    extract_playwright_cookies,
    handoff_to_browser,
    parse_playwright_proxy,
)

__all__ = [
    "extract_playwright_cookies",
    "handoff_to_browser",
    "parse_playwright_proxy",
]
