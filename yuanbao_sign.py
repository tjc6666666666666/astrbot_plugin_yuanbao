"""
Yuanbao token signing — HMAC-SHA256 based authentication.

Replicates the sign-token flow from openclaw-plugin-yuanbao:
  POST https://{apiDomain}/api/v5/robotLogic/sign-token
  Body: { app_key, nonce, signature, timestamp }
  Signature: HMAC-SHA256(nonce + timestamp + appKey + appSecret, appSecret)
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

# Beijing timezone (UTC+8)
_BJ_TZ = timezone(timedelta(hours=8))

# Default Yuanbao API domain
DEFAULT_API_DOMAIN = "bot.yuanbao.tencent.com"

SIGN_TOKEN_PATH = "/api/v5/robotLogic/sign-token"
SIGN_MAX_RETRIES = 3
SIGN_RETRY_DELAY_MS = 1000
CACHE_REFRESH_MARGIN_S = 5 * 60

# Friendly retryable sign-token code
RETRYABLE_SIGN_CODE = 10099


@dataclass
class SignTokenData:
    bot_id: str
    duration: int  # seconds
    product: str
    source: str
    token: str


@dataclass
class TokenCacheEntry:
    data: SignTokenData
    expires_at: float  # monotonic timestamp (ms)


_token_cache: dict[str, TokenCacheEntry] = {}


def clear_token_cache(account_id: str = "") -> None:
    """Clear cached sign tokens.  Pass empty string to clear all."""
    global _token_cache
    if account_id:
        _token_cache.pop(account_id, None)
    else:
        _token_cache.clear()


def compute_signature(
    nonce: str, timestamp: str, app_key: str, app_secret: str
) -> str:
    """Compute HMAC-SHA256 signature for sign-token request."""
    plain = nonce + timestamp + app_key + app_secret
    return hmac.new(
        app_secret.encode("utf-8"),
        plain.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _make_timestamp() -> str:
    """Generate Beijing-time ISO-8601 timestamp with +08:00 offset."""
    now = datetime.now(_BJ_TZ)
    return now.strftime("%Y-%m-%dT%H:%M:%S+08:00")


async def sign_token(
    app_key: str,
    app_secret: str,
    api_domain: str = DEFAULT_API_DOMAIN,
    route_env: str | None = None,
    session=None,
) -> SignTokenData:
    """
    Request a signed token from the Yuanbao sign-token API.

    Attempts up to SIGN_MAX_RETRIES times if the server returns a
    retryable error code (10099).  Uses HMAC-SHA256 for the signature.
    """
    import aiohttp

    url = f"https://{api_domain}{SIGN_TOKEN_PATH}"
    close_session = session is None

    if session is None:
        session = aiohttp.ClientSession()

    try:
        for attempt in range(SIGN_MAX_RETRIES + 1):
            nonce = os.urandom(16).hex()
            timestamp = _make_timestamp()
            signature = compute_signature(nonce, timestamp, app_key, app_secret)

            body = {
                "app_key": app_key,
                "nonce": nonce,
                "signature": signature,
                "timestamp": timestamp,
            }
            headers = {
                "Content-Type": "application/json",
                "X-AppVersion": "0.0.0",
                "X-OperationSystem": _get_os(),
                "X-Instance-Id": "16",
                "X-Bot-Version": "0.0.0",
            }
            if route_env:
                headers["x-route-env"] = route_env

            async with session.post(url, json=body, headers=headers) as resp:
                if not resp.ok:
                    raise SignTokenError(
                        f"sign-token HTTP {resp.status} {resp.reason}"
                    )
                result = await resp.json()

            code = result.get("code", -1)
            if code == 0:
                data = result.get("data", {})
                st = SignTokenData(
                    bot_id=str(data.get("bot_id", "")),
                    duration=int(data.get("duration", 0)),
                    product=str(data.get("product", "yuanbao")),
                    source=str(data.get("source", "web")),
                    token=str(data.get("token", "")),
                )
                # Cache the token
                if st.duration > 0:
                    _token_cache[app_key] = TokenCacheEntry(
                        data=st,
                        expires_at=time.monotonic() + st.duration - CACHE_REFRESH_MARGIN_S,
                    )
                return st

            if code == RETRYABLE_SIGN_CODE and attempt < SIGN_MAX_RETRIES:
                await _sleep_s(SIGN_RETRY_DELAY_MS / 1000.0)
                continue

            raise SignTokenError(
                f"sign-token business error: code={code}, msg={result.get('msg', '')}"
            )

        raise SignTokenError("sign-token failed: max retries exceeded")
    finally:
        if close_session:
            await session.close()


async def get_or_refresh_token(
    app_key: str,
    app_secret: str,
    api_domain: str = DEFAULT_API_DOMAIN,
    route_env: str | None = None,
    session=None,
) -> SignTokenData:
    """Return cached token if valid, otherwise sign a new one."""
    cached = _token_cache.get(app_key)
    if cached and time.monotonic() < cached.expires_at:
        return cached.data
    return await sign_token(app_key, app_secret, api_domain, route_env, session)


def _get_os() -> str:
    import platform
    return platform.system() or "Linux"


async def _sleep_s(seconds: float) -> None:
    import asyncio
    await asyncio.sleep(seconds)


class SignTokenError(Exception):
    """Raised when token signing fails."""
