"""
tokens.py — Cryptographic Token Utilities for Newsletter Security

Generates and validates HMAC-signed tokens for secure, 1-click email
unsubscribe links. Prevents unauthenticated mass-unsubscribe attacks
by requiring a valid signed token in the URL.

Usage:
    from tokens import generate_unsubscribe_token, validate_unsubscribe_token

    token = generate_unsubscribe_token("user@example.com")
    # => "InVzZXJAZXhhbXBsZS5jb20i.ZxN..."

    email = validate_unsubscribe_token(token, max_age_hours=168)
    # => "user@example.com" or None if invalid/expired
"""

from __future__ import annotations

import logging
import os

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

log = logging.getLogger(__name__)

# The secret key used to sign tokens. Falls back to API_SECRET_KEY.
_SECRET_KEY = os.getenv("TOKEN_SECRET_KEY") or os.getenv("API_SECRET_KEY") or ""
_SALT = "newsletter-unsubscribe"

if not _SECRET_KEY:
    log.warning(
        "TOKEN_SECRET_KEY and API_SECRET_KEY are both unset — "
        "signed unsubscribe tokens will be insecure."
    )

_serializer = URLSafeTimedSerializer(_SECRET_KEY) if _SECRET_KEY else None


def generate_unsubscribe_token(email: str) -> str:
    """
    Generate a time-limited, signed unsubscribe token for the given email.

    The token is URL-safe and contains:
    - The email address (encrypted payload)
    - A creation timestamp
    - An HMAC signature
    """
    if not _serializer:
        raise RuntimeError(
            "Cannot generate tokens: TOKEN_SECRET_KEY or API_SECRET_KEY must be set."
        )
    return _serializer.dumps(email, salt=_SALT)


def validate_unsubscribe_token(
    token: str,
    max_age_hours: int = 168,  # 7 days default
) -> str | None:
    """
    Validate a signed unsubscribe token and return the email address.

    Args:
        token: The URL-safe signed token string.
        max_age_hours: Maximum age of the token in hours (default: 7 days).
                       Set to 0 to disable expiry.

    Returns:
        The email address if the token is valid and not expired, or None.
    """
    if not _serializer:
        log.error("Cannot validate tokens: no secret key configured.")
        return None

    max_age_seconds = max_age_hours * 3600 if max_age_hours > 0 else None

    try:
        email: str = _serializer.loads(token, salt=_SALT, max_age=max_age_seconds)
        return email
    except SignatureExpired:
        log.warning("Unsubscribe token expired.")
        return None
    except BadSignature:
        log.warning("Invalid unsubscribe token signature.")
        return None
    except Exception as e:
        log.error("Unexpected token validation error: %s", e)
        return None


def generate_unsubscribe_url(email: str, base_url: str = "") -> str:
    """
    Generate a complete 1-click unsubscribe URL.

    Args:
        email: The subscriber email address.
        base_url: The API base URL (e.g., "https://api.yourdomain.com").
                  If empty, reads from UNSUBSCRIBE_BASE_URL env var.

    Returns:
        Full unsubscribe URL with signed token.
    """
    if not base_url:
        base_url = os.getenv("UNSUBSCRIBE_BASE_URL", "").rstrip("/")

    token = generate_unsubscribe_token(email)
    return f"{base_url}/api/newsletter/unsubscribe?token={token}"
