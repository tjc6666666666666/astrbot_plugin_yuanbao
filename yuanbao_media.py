"""
Yuanbao media upload — COS (Cloud Object Storage) pipeline.

Replicates the openclaw-plugin-yuanbao media flow:
  1. Download image from URL / read local file
  2. Call ``POST /api/resource/genUploadInfo`` (authenticated) for COS credentials
  3. PUT the file to COS (S3‑compatible API, HMAC‑SHA1 signed)
  4. Return the CDN ``resourceUrl`` for use in ``TIMImageElem.image_info_array``
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import secrets
import time as _time
import urllib.parse
from io import BytesIO
from typing import Any

import aiohttp

from astrbot.api import logger

# ── HTTP request defaults ────────────────────────
# Hard timeouts prevent indefinite hangs when the remote server is slow
# or unresponsive.  DOWNLOAD_CHUNK_SIZE limits per-chunk memory pressure.
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(
    total=30,        # whole operation (connect + read)
    connect=10,      # TCP connect
    sock_read=20,    # between chunks
)
DOWNLOAD_MAX_CHUNK_BYTES = 65536  # 64 KiB per chunk
STREAM_RATE_LIMIT = 5 * 1024 * 1024  # 5 MB/s per-stream throttle hint

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

        async with session.post(url, json=body, headers=headers, timeout=DOWNLOAD_TIMEOUT) as resp:
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

        logger.info(f"[yuanbao][genUploadInfo] response keys={list(data.keys())[:10]}")
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
        # ── Download image with chunked streaming + early size abort ──
        async with session.get(image_url, timeout=DOWNLOAD_TIMEOUT) as resp:
            if not resp.ok:
                raise RuntimeError(f"download image failed: HTTP {resp.status} — {image_url[:120]}")

            content_type = resp.headers.get("Content-Type", "image/png").split(";")[0].strip()

            # Quick reject via Content-Length header
            content_length = resp.content_length
            if content_length is not None and content_length > max_bytes:
                raise RuntimeError(
                    f"image too large: {(content_length / 1024 / 1024):.1f} MB > {media_max_mb} MB"
                )

            # Stream chunk-by-chunk, abort early if exceeding limit
            img_data = bytearray()
            async for chunk in resp.content.iter_chunked(DOWNLOAD_MAX_CHUNK_BYTES):
                img_data.extend(chunk)
                if len(img_data) > max_bytes:
                    raise RuntimeError(
                        f"image too large during stream: {(len(img_data) / 1024 / 1024):.1f} MB > {media_max_mb} MB"
                    )
            img_data = bytes(img_data)

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


# ── Download media (for receiving images/files) ─────

_MIME_EXT_MAP = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
    "application/pdf": ".pdf",
    "text/plain": ".txt",
}


def _guess_mime_type(filename: str) -> str:
    """Guess MIME type from filename extension."""
    ext = os.path.splitext(filename)[1].lower()
    return {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".bmp": "image/bmp",
        ".pdf": "application/pdf",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".xls": "application/vnd.ms-excel",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".ppt": "application/vnd.ms-powerpoint",
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".txt": "text/plain",
        ".zip": "application/zip",
        ".tar": "application/x-tar",
        ".gz": "application/gzip",
        ".mp3": "audio/mpeg",
        ".mp4": "video/mp4",
        ".wav": "audio/wav",
        ".ogg": "audio/ogg",
        ".webm": "video/webm",
    }.get(ext, "application/octet-stream")


def _infer_filename(resp, url: str, content_type: str) -> str:
    """Infer filename from HTTP response headers or URL path."""
    # 1. content-disposition header
    cd = resp.headers.get("Content-Disposition", "")
    if cd:
        import re as _re
        m = _re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^;"\'\r\n]+)', cd, _re.I)
        if m:
            try:
                return urllib.parse.unquote(m.group(1).replace('"', "").strip())
            except Exception:
                pass
    # 2. URL path last segment
    parsed = urllib.parse.urlparse(url)
    from_path = os.path.basename(parsed.path).strip()
    if from_path:
        if os.path.splitext(from_path)[1]:
            return from_path
        ext = _MIME_EXT_MAP.get(content_type, "")
        if not ext and content_type.startswith("image/"):
            ext = f".{content_type.split('/')[1]}"
        return from_path + ext
    # 3. random fallback
    ext = _MIME_EXT_MAP.get(content_type, "")
    return f"{secrets.token_hex(8)}{ext}"


async def api_get_download_url(
    *,
    token: str,
    bot_id: str,
    api_domain: str,
    resource_id: str,
    session: aiohttp.ClientSession,
    route_env: str | None = None,
    force_refresh_token: Any = None,
) -> str:
    """GET /api/resource/v1/download?resourceId=... — exchange resourceId for real download URL.

    If the server returns 401 the *force_refresh_token* callback is invoked once,
    and the request is retried with the new token.
    """
    import platform as _platform

    url = f"https://{api_domain}/api/resource/v1/download"
    params = {"resourceId": resource_id}

    for attempt in range(2):
        headers: dict[str, str] = {
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

        async with session.get(url, params=params, headers=headers, timeout=DOWNLOAD_TIMEOUT) as resp:
            if resp.status == 401 and attempt == 0 and force_refresh_token is not None:
                try:
                    new_token_data = await force_refresh_token()
                    if new_token_data and getattr(new_token_data, "token", None):
                        token = new_token_data.token
                        bot_id = new_token_data.bot_id or bot_id
                        continue
                except Exception:
                    pass

            if not resp.ok:
                text = await resp.text()
                raise RuntimeError(
                    f"apiGetDownloadUrl HTTP {resp.status}: {text[:300]}"
                )
            result = await resp.json()

        if "code" in result and result["code"] != 0:
            raise RuntimeError(
                f"apiGetDownloadUrl error: code={result['code']}, msg={result.get('msg', '')[:200]}"
            )

        download_url = result.get("url") or result.get("realUrl") or result.get("data", {}).get("url")
        if not download_url:
            raise RuntimeError(f"apiGetDownloadUrl returned no valid URL: {result}")
        return download_url

    raise RuntimeError("apiGetDownloadUrl: 401 retry exhausted")


async def download_media(
    *,
    url: str,
    token: str = "",
    bot_id: str = "",
    api_domain: str = "bot.yuanbao.tencent.com",
    media_max_mb: int = 20,
    route_env: str | None = None,
    force_refresh_token: Any = None,
) -> dict[str, Any]:
    """Download media from a yuanbao URL (or any HTTP URL / local path).

    Handles:
      - Direct HTTP/HTTPS URLs
      - Yuanbao resourceId-based URLs (URL contains ?resourceId=xxx) → resolves via api_get_download_url()
      - Local file paths (file://, absolute paths)

    Returns: { "filename": str, "data": bytes, "mime_type": str }
    """
    max_bytes = media_max_mb * 1024 * 1024

    # ── Local file path ──
    if not url or (not url.startswith("http://") and not url.startswith("https://")):
        filepath = url
        if filepath.startswith("file:///"):
            filepath = filepath[8:]
        elif filepath.startswith("file://"):
            filepath = filepath[7:]

        if os.path.exists(filepath) and os.path.isfile(filepath):
            file_size = os.path.getsize(filepath)
            if file_size > max_bytes:
                raise RuntimeError(
                    f"file too large: {(file_size / 1024 / 1024):.1f} MB > {media_max_mb} MB"
                )
            with open(filepath, "rb") as f:
                data = f.read()
            filename = os.path.basename(filepath)
            mime_type = _guess_mime_type(filename)
            return {"filename": filename, "data": data, "mime_type": mime_type}
        raise RuntimeError(f"local file not found: {url}")

    async with aiohttp.ClientSession() as session:
        # ── Resolve resourceId if present ──
        fetch_url = url
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        resource_id = qs.get("resourceId", [None])[0]
        if resource_id and token and bot_id:
            try:
                fetch_url = await api_get_download_url(
                    token=token, bot_id=bot_id, api_domain=api_domain,
                    resource_id=resource_id, session=session,
                    route_env=route_env, force_refresh_token=force_refresh_token,
                )
            except Exception as exc:
                # Fall back to original URL if resourceId resolution fails
                logger.warning(f"[yuanbao] resourceId download resolution failed, using raw URL: {exc}")

        # ── Download with chunked streaming + early size abort ──
        async with session.get(fetch_url, timeout=DOWNLOAD_TIMEOUT) as resp:
            if not resp.ok:
                raise RuntimeError(f"download media failed: HTTP {resp.status} — {fetch_url[:120]}")

            content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
            filename = _infer_filename(resp, fetch_url, content_type)
            mime_type = content_type or _guess_mime_type(filename)

            # Quick reject via Content-Length header
            content_length = resp.content_length
            if content_length is not None and content_length > max_bytes:
                raise RuntimeError(
                    f"media too large: {(content_length / 1024 / 1024):.1f} MB > {media_max_mb} MB"
                )

            # Stream chunk-by-chunk and abort early if the limit is exceeded.
            # Prevents OOM when a malicious/slow server omits Content-Length
            # or sends an unexpectedly large body.
            data = bytearray()
            async for chunk in resp.content.iter_chunked(DOWNLOAD_MAX_CHUNK_BYTES):
                data.extend(chunk)
                if len(data) > max_bytes:
                    raise RuntimeError(
                        f"media too large during stream: {(len(data) / 1024 / 1024):.1f} MB > {media_max_mb} MB"
                    )

    return {"filename": filename, "data": bytes(data), "mime_type": mime_type}


async def download_medias_to_local(
    *,
    medias: list[dict],
    token: str = "",
    bot_id: str = "",
    api_domain: str = "bot.yuanbao.tencent.com",
    media_max_mb: int = 20,
    route_env: str | None = None,
    force_refresh_token: Any = None,
    cache_dir: str | None = None,
) -> list[dict]:
    """Download multiple media items and save to local cache directory.

    Returns a list of: { "local_path": str, "mime_type": str, "url": str, "media_type": str }
    Individual download failures do not block others.
    """
    import tempfile as _tempfile

    if not cache_dir:
        cache_dir = os.path.join(_tempfile.gettempdir(), "yuanbao-media")
    os.makedirs(cache_dir, exist_ok=True)

    async def _download_one(item: dict) -> dict | None:
        try:
            url = item.get("url", "")
            if not url:
                return None

            result = await download_media(
                url=url,
                token=token,
                bot_id=bot_id,
                api_domain=api_domain,
                media_max_mb=media_max_mb,
                route_env=route_env,
                force_refresh_token=force_refresh_token,
            )

            # Content-based deduplication: save as <md5>.<ext>
            md5 = hashlib.md5(result["data"]).hexdigest()
            ext = os.path.splitext(result["filename"])[1].lower() or (
                _MIME_EXT_MAP.get(result["mime_type"], "")
            )
            md5_filename = f"{md5}{ext}" if ext else md5
            cached_path = os.path.join(cache_dir, md5_filename)

            if not os.path.exists(cached_path):
                with open(cached_path, "wb") as f:
                    f.write(result["data"])

            return {
                "local_path": cached_path,
                "mime_type": result["mime_type"],
                "url": url,
                "media_type": item.get("type", ""),
                "file_name": item.get("file_name", result["filename"]),
            }
        except Exception as exc:
            logger.warning(f"[yuanbao] download media failed: {url[:80] if 'url' in dir() else '?'} — {exc}")
            return None

    # Download up to 20 concurrently
    tasks = [asyncio.ensure_future(_download_one(item)) for item in medias[:20]]
    results_raw = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = []
    for r in results_raw:
        if isinstance(r, Exception):
            continue
        if r is not None:
            results.append(r)
    return results
