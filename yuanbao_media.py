"""
Yuanbao media upload — COS (Cloud Object Storage) pipeline.

Replicates the openclaw-plugin-yuanbao media flow:
  1. Download image from URL / read local file
  2. Call ``POST /api/resource/genUploadInfo`` (authenticated) for COS credentials
  3. PUT the file to COS (S3‑compatible API, HMAC‑SHA1 signed)
  4. Return the CDN ``resourceUrl`` for use in ``TIMImageElem.image_info_array``
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time as _time
import urllib.parse
from io import BytesIO
from typing import Any

import aiohttp


# ── helpers  ─────────────────────────────────────

def parse_image_size(data: bytes) -> dict | None:
    """Parse image dimensions from raw bytes.  Supports PNG / JPEG / GIF / WebP."""
    try:
        w = h = 0
        if len(data) >= 24 and data[0] == 0x89 and data[1:4] == b"PNG":
            w = (data[16] << 24) | (data[17] << 16) | (data[18] << 8) | data[19]
            h = (data[20] << 24) | (data[21] << 16) | (data[22] << 8) | data[23]
        elif len(data) >= 4 and data[0] == 0xFF and data[1] == 0xD8:
            i = 2
            while i < len(data) - 9:
                if data[i] == 0xFF:
                    marker = data[i + 1]
                    if marker in (0xC0, 0xC2):
                        h = (data[i + 5] << 8) | data[i + 6]
                        w = (data[i + 7] << 8) | data[i + 8]
                        break
                    if i + 3 < len(data):
                        i += 2 + ((data[i + 2] << 8) | data[i + 3])
                    else:
                        break
                i += 1
        elif len(data) >= 10 and data[:6] in (b"GIF87a", b"GIF89a"):
            w = data[7] << 8 | data[6]
            h = data[9] << 8 | data[8]
        elif len(data) >= 30 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            if data[12:16] == b"VP8 " and data[23] == 0x9D and data[24] == 0x01 and data[25] == 0x2A:
                w = (data[26] | (data[27] << 8)) & 0x3FFF
                h = (data[28] | (data[29] << 8)) & 0x3FFF
            elif data[12:16] == b"VP8L" and data[20] == 0x2F:
                bits = data[21] | (data[22] << 8) | (data[23] << 16) | (data[24] << 24)
                w = (bits & 0x3FFF) + 1
                h = ((bits >> 14) & 0x3FFF) + 1
            elif data[12:16] == b"VP8X":
                w = (data[24] | (data[25] << 8) | (data[26] << 16)) + 1
                h = (data[27] | (data[28] << 8) | (data[29] << 16)) + 1
        if w and h:
            return {"width": w, "height": h}
    except Exception:
        pass
    return None


def _sha1_hex(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def _hmac_sha1(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha1).digest()


def _hmac_sha1_hex(key: bytes, msg: bytes) -> str:
    return _hmac_sha1(key, msg).hex()


# ── COS PUT object (S3‑compatible)  ──────────────

async def _cos_put_object(
    *,
    bucket: str,
    region: str,
    key: str,
    body: bytes,
    content_type: str,
    tmp_secret_id: str,
    tmp_secret_key: str,
    security_token: str,
    start_time: int,
    expired_time: int,
    session: aiohttp.ClientSession,
    is_image: bool = False,
    pic_fileid: str = "",
) -> str:
    """Raw HTTP PUT to COS using HMAC‑SHA1 V4 signing.

    Matches the original ``cos-nodejs-sdk-v5`` behaviour:
    accelerate endpoint + Pic‑Operations header for images.
    """

    # Standard COS endpoint — accelerate is optional per-bucket
    host = f"{bucket}.cos.{region}.myqcloud.com"
    # Normalise key: strip leading "/", then percent-encode.
    # The identical path MUST be used for both the HTTP URL and the signature.
    key_clean = key.lstrip("/")
    url_path = "/" + urllib.parse.quote(key_clean, safe='/')
    url = f"https://{host}{url_path}"

    # Signing parameters
    now = int(_time.time())
    key_time = f"{now};{now + 3600}"
    sign_time = f"{now};{now + 3600}"

    headers_to_sign: dict[str, str] = {
        "Host": host,
        "Content-Type": content_type,
        "x-cos-security-token": security_token,
    }
    # Pic-Operations → same as original for image uploads
    if is_image and pic_fileid:
        headers_to_sign["Pic-Operations"] = (
            '{"is_pic_info":1,"rules":[{"fileid":"'
            + pic_fileid
            + '","rule":"imageMogr2/format/jpg"}]}'
        )

    # Only sign headers that the cos-nodejs-sdk-v5 signs:
    #   - x-cos-* and x-ci-* headers
    #   - specific standard headers: host, pic-operations, etc.
    #   (content-type is NOT in the signed header list)
    _cos_signed_std = {"host", "pic-operations", "content-disposition",
                       "content-encoding", "content-length", "content-md5",
                       "expect", "expires", "if-match", "if-modified-since",
                       "if-none-match", "if-unmodified-since", "origin",
                       "range", "transfer-encoding"}
    signed = {}
    for k, v in headers_to_sign.items():
        kl = k.lower()
        if kl.startswith("x-cos-") or kl.startswith("x-ci-") or kl in _cos_signed_std:
            signed[k] = v

    # Sort by lowercased key (matching getObjectKeys behaviour)
    sorted_keys = sorted(signed, key=lambda k: k.lower())

    # Signed header list (for authorization q-header-list)
    hl = ";".join(k.lower() for k in sorted_keys)

    # Header string: obj2str format — key=val&key=val with camSafeUrlEncode
    # camSafeUrlEncode = encodeURIComponent + %21 %27 %28 %29 %2A
    # Python equivalent: quote with safe='-_.~' then replace ! ' ( ) *
    _safe = "-_.~"
    def _cam_safe(s: str) -> str:
        encoded = urllib.parse.quote(s, safe=_safe)
        for ch, repl in (("!", "%21"), ("'", "%27"), ("(", "%28"),
                         (")", "%29"), ("*", "%2A")):
            encoded = encoded.replace(ch, repl)
        return encoded

    header_pairs = []
    for k in sorted_keys:
        kl = k.lower()
        kv = str(signed[k])
        header_pairs.append(f"{_cam_safe(kl)}={_cam_safe(kv)}")
    headers_str = "&".join(header_pairs)

    # FormatString = method + \n + pathname + \n + queryString + \n + headersStr + \n
    # (SDK: [method, pathname, obj2str(query,true), obj2str(headers,true), ''].join('\n'))
    http_method = "put"
    uri_path = url_path
    http_string = f"{http_method}\n{uri_path}\n\n{headers_str}\n"

    # StringToSign
    sha1_http_string = _sha1_hex(http_string.encode("utf-8"))
    string_to_sign = f"sha1\n{sign_time}\n{sha1_http_string}\n"

    # SignKey = HMAC-SHA1(tmpSecretKey, q-key-time) → hex string
    # (SDK: .digest('hex') returns hex string; that string's UTF-8 bytes
    #  become the HMAC key for the second step.)
    sign_key_raw = _hmac_sha1(tmp_secret_key.encode("utf-8"), key_time.encode("utf-8"))
    sign_key_hex = sign_key_raw.hex()  # SDK's `.digest('hex')` output

    # Signature = HMAC-SHA1(signKey_hex_as_utf8, stringToSign)
    signature = _hmac_sha1_hex(sign_key_hex.encode("utf-8"), string_to_sign.encode("utf-8"))

    authorization = (
        f"q-sign-algorithm=sha1"
        f"&q-ak={tmp_secret_id}"
        f"&q-sign-time={sign_time}"
        f"&q-key-time={key_time}"
        f"&q-header-list={hl}"
        f"&q-url-param-list="
        f"&q-signature={signature}"
    )

    async with session.put(
        url,
        data=body,
        headers={
            **headers_to_sign,
            "Authorization": authorization,
        },
    ) as resp:
        if resp.status not in (200, 204):
            text = await resp.text()
            raise RuntimeError(
                f"COS PUT failed: HTTP {resp.status} — {text[:300]}"
            )

    return url


# ── public API  ──────────────────────────────────

async def api_get_upload_info(
    *,
    token: str,
    bot_id: str,
    api_domain: str,
    file_name: str,
    session: aiohttp.ClientSession,
    route_env: str | None = None,
    force_refresh_token: Any = None,  # async callable() -> SignTokenData | None
) -> dict[str, Any]:
    """POST /api/resource/genUploadInfo — get COS pre‑signed config.

    If the server returns 401 the *force_refresh_token* callback (when
    provided) is invoked once, and the request is retried with the new
    token.  This matches the ``yuanbaoPost`` retry behaviour in the
    original openclaw-plugin-yuanbao.
    """

    import platform as _platform

    url = f"https://{api_domain}/api/resource/genUploadInfo"

    file_id = secrets.token_hex(16)
    body = {
        "fileName": file_name,
        "fileId": file_id,
        "docFrom": "localDoc",
        "docOpenId": "",
    }

    for attempt in range(2):  # initial + one retry
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "X-ID": bot_id,
            "X-Token": token,
            "X-Source": "web",
            "X-Instance-Id": "16",
            "X-AppVersion": "0.0.0",
            "X-OperationSystem": _platform.system() or "Linux",
            "X-Bot-Version": "0.0.0",
        }
        if route_env:
            headers["X-Route-Env"] = route_env

        async with session.post(url, json=body, headers=headers) as resp:
            if resp.status == 401 and attempt == 0 and force_refresh_token is not None:
                # Token expired — refresh and retry once
                try:
                    new_token_data = await force_refresh_token()
                    if new_token_data and getattr(new_token_data, "token", None):
                        token = new_token_data.token
                        bot_id = new_token_data.bot_id or bot_id
                        continue  # retry
                except Exception:
                    pass  # fall through to raise below

            if not resp.ok:
                text = await resp.text()
                raise RuntimeError(
                    f"genUploadInfo HTTP {resp.status}: {text[:300]}"
                )
            result = await resp.json()

        # Match original yuanbaoPost: code=0 or missing → success
        if "code" in result and result["code"] != 0:
            raise RuntimeError(
                f"genUploadInfo error: code={result['code']}, msg={result.get('msg', '')[:200]}"
            )

        # Original returns json.data ?? json (data field or whole response)
        data = result.get("data", result)

        _logger = __import__("astrbot", fromlist=["logger"]).logger
        _logger.info(f"[yuanbao][genUploadInfo] response keys={list(data.keys())[:10]}")
        required = ("bucketName", "region", "location",
                    "encryptTmpSecretId", "encryptTmpSecretKey",
                    "encryptToken", "startTime", "expiredTime", "resourceUrl")
        for k in required:
            if k not in data:
                raise RuntimeError(f"genUploadInfo response missing '{k}': {data}")
        return data

    raise RuntimeError("genUploadInfo: 401 retry exhausted")


async def upload_to_cos(
    *,
    upload_config: dict[str, Any],
    data: bytes,
    content_type: str,
    session: aiohttp.ClientSession,
    is_image: bool = False,
) -> str:
    """Upload bytes to COS using the pre‑signed config."""

    return await _cos_put_object(
        bucket=upload_config["bucketName"],
        region=upload_config["region"],
        key=upload_config["location"],
        body=data,
        content_type=content_type,
        tmp_secret_id=upload_config["encryptTmpSecretId"],
        tmp_secret_key=upload_config["encryptTmpSecretKey"],
        security_token=upload_config["encryptToken"],
        start_time=int(upload_config["startTime"]),
        expired_time=int(upload_config["expiredTime"]),
        session=session,
        is_image=is_image,
        pic_fileid=upload_config.get("location", ""),
    )


async def upload_raw(
    *,
    data: bytes,
    filename: str,
    content_type: str,
    token: str,
    bot_id: str,
    api_domain: str,
    route_env: str | None = None,
    force_refresh_token: Any = None,
) -> dict[str, Any]:
    """Upload raw image bytes to Yuanbao COS, return CDN metadata dict."""
    async with aiohttp.ClientSession() as session:
        upload_config = await api_get_upload_info(
            token=token, bot_id=bot_id, api_domain=api_domain,
            file_name=filename, session=session, route_env=route_env,
            force_refresh_token=force_refresh_token,
        )
        is_img = content_type.startswith("image/")
        cos_url = await upload_to_cos(
            upload_config=upload_config, data=data, content_type=content_type,
            session=session, is_image=is_img,
        )
        size = len(data)
        img_uuid = hashlib.md5(data).hexdigest()
        img_size = parse_image_size(data) if is_img else None
        return {
            "url": upload_config.get("resourceUrl", cos_url),
            "filename": filename, "size": size, "uuid": img_uuid,
            "width": img_size["width"] if img_size else 0,
            "height": img_size["height"] if img_size else 0,
        }


async def download_and_upload(
    *,
    image_url: str,
    token: str,
    bot_id: str,
    api_domain: str,
    media_max_mb: int = 20,
    route_env: str | None = None,
    force_refresh_token: Any = None,
) -> dict[str, Any]:
    """
    Download image from URL, upload to Yuanbao COS,
    return ``{url, filename, size, uuid, width, height}``.
    """
    max_bytes = media_max_mb * 1024 * 1024

    async with aiohttp.ClientSession() as session:
        async with session.get(image_url) as resp:
            if not resp.ok:
                raise RuntimeError(f"download image failed: HTTP {resp.status} — {image_url[:120]}")
            img_data = await resp.read()

        if len(img_data) > max_bytes:
            raise RuntimeError(f"image too large: {(len(img_data) / 1024 / 1024):.1f} MB > {media_max_mb} MB")

        content_type = resp.headers.get("Content-Type", "image/png").split(";")[0].strip()
        parsed = urllib.parse.urlparse(image_url)
        filename = os.path.basename(parsed.path) or "image.png"
        if "." not in filename.rsplit("/", 1)[-1]:
            ext_map = {"image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp"}
            filename = filename + ext_map.get(content_type, ".png")

    # Reuse the core upload
    return await upload_raw(
        data=img_data, filename=filename, content_type=content_type,
        token=token, bot_id=bot_id, api_domain=api_domain,
        route_env=route_env, force_refresh_token=force_refresh_token,
    )
