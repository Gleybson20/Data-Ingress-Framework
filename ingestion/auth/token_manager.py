"""
ingestion/auth/token_manager.py
--------------------------------
OAuth2 client-credentials token manager with automatic renewal.

Handles:
  - Initial token acquisition via client_credentials grant
  - In-memory caching with expiry buffer
  - Thread-safe renewal using a lock
  - Structured logging for every token lifecycle event

Usage
-----
    manager = TokenManager(
        token_url="https://auth.example.com/oauth/token",
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
        scope="read:data",
    )
    headers = {"Authorization": f"Bearer {manager.get_token()}"}

Note: The public connectors (Open-Meteo, Frankfurter) do not require OAuth2.
This module is used for any future connector that targets a protected API.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Seconds before actual expiry to trigger proactive renewal.
# Avoids race conditions where a token expires mid-request.
_EXPIRY_BUFFER_SECONDS: int = 60


@dataclass
class _CachedToken:
    """In-memory representation of an active access token."""

    access_token: str
    expires_at: float  # Unix timestamp (UTC)
    token_type: str = "Bearer"
    scope: str = ""

    def is_expired(self, buffer: int = _EXPIRY_BUFFER_SECONDS) -> bool:
        """Return True if the token will expire within *buffer* seconds."""
        return time.time() >= (self.expires_at - buffer)


class TokenManager:
    """Thread-safe OAuth2 client-credentials token manager.

    Attributes
    ----------
    token_url     : Full URL of the OAuth2 token endpoint.
    client_id     : OAuth2 application client ID.
    client_secret : OAuth2 application client secret (never logged).
    scope         : Space-separated OAuth2 scope string (optional).
    extra_params  : Additional form fields sent with every token request.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str = "",
        extra_params: dict[str, str] | None = None,
        timeout: int = 15,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._extra_params = extra_params or {}
        self._timeout = timeout

        self._cached: _CachedToken | None = None
        self._lock = threading.Lock()
        
    def get_token(self) -> str:
        """Return a valid access token, refreshing it if necessary.

        This method is thread-safe: concurrent callers will block on the lock
        rather than triggering duplicate token requests.

        Returns
        -------
        str
            Raw access token string (without the "Bearer" prefix).

        Raises
        ------
        TokenError
            If the token endpoint returns an error or is unreachable.
        """
        with self._lock:
            if self._cached is None or self._cached.is_expired():
                self._refresh()
            return self._cached.access_token  # type: ignore[union-attr]

    def invalidate(self) -> None:
        """Force the next get_token() call to acquire a fresh token.

        Useful when a downstream API returns 401, indicating the cached
        token was revoked or rejected before its scheduled expiry.
        """
        with self._lock:
            logger.info("[TokenManager] Token invalidated manually — will renew on next call.")
            self._cached = None

    def _refresh(self) -> None:
        """Acquire a new token from the OAuth2 endpoint and cache it.

        Called only from within the lock; must not be called directly.

        Raises
        ------
        TokenError
            On HTTP error, network failure, or malformed response.
        """
        logger.info(
            "[TokenManager] Acquiring token from %s (client_id=%s).",
            self._token_url,
            self._client_id,
        )

        payload: dict[str, Any] = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            **self._extra_params,
        }
        if self._scope:
            payload["scope"] = self._scope

        try:
            response = requests.post(
                self._token_url,
                data=payload,
                timeout=self._timeout,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise TokenError(
                f"Token endpoint timed out after {self._timeout}s: {exc}"
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            raise TokenError(f"Could not connect to token endpoint: {exc}") from exc
        except requests.exceptions.HTTPError as exc:
            raise TokenError(
                f"Token endpoint returned HTTP {exc.response.status_code}: "
                f"{exc.response.text[:200]}"
            ) from exc

        try:
            data = response.json()
        except ValueError as exc:
            raise TokenError(f"Token endpoint returned non-JSON response: {exc}") from exc

        self._cached = self._parse_token_response(data)
        logger.info(
            "[TokenManager] Token acquired. Expires at %.0f (in ~%ds).",
            self._cached.expires_at,
            max(0, int(self._cached.expires_at - time.time())),
        )

    def _parse_token_response(self, data: dict[str, Any]) -> _CachedToken:
        """Validate and parse the token endpoint JSON response.

        Args
        ----
        data : Parsed JSON dict from the token endpoint.

        Returns
        -------
        _CachedToken

        Raises
        ------
        TokenError
            If required fields are missing or have unexpected types.
        """
        access_token = data.get("access_token")
        if not access_token or not isinstance(access_token, str):
            raise TokenError(
                f"Token response missing 'access_token'. Keys received: {list(data.keys())}"
            )

        expires_in = data.get("expires_in")
        if expires_in is None:
            logger.warning(
                "[TokenManager] 'expires_in' not present in token response. "
                "Defaulting to 3600 seconds."
            )
            expires_in = 3600

        try:
            expires_in = int(expires_in)
        except (TypeError, ValueError) as exc:
            raise TokenError(
                f"'expires_in' is not a valid integer: {expires_in!r}"
            ) from exc

        return _CachedToken(
            access_token=access_token,
            expires_at=time.time() + expires_in,
            token_type=data.get("token_type", "Bearer"),
            scope=data.get("scope", ""),
        )

class TokenError(Exception):
    """Raised when token acquisition or renewal fails."""