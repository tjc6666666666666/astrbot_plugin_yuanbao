"""
Yuanbao platform event — AstrMessageEvent subclass for the Yuanbao adapter.

Translates MessageChain components into Yuanbao msg_body format, encodes them
as protobuf SendC2CMessageReq / SendGroupMessageReq, and dispatches through
the platform WebSocket client directly.  Images / files are uploaded to
Yuanbao COS before being referenced in TIMImageElem / TIMFileElem.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import TYPE_CHECKING

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.platform import AstrBotMessage, PlatformMetadata
from astrbot.api.message_components import Plain, Image, File, Record, Video
from astrbot.api import logger

if TYPE_CHECKING:
    from .yuanbao_client import YuanbaoWsClient

try:
    from astrbot.api.message_components import At
except ImportError:
    At = None


# ── Defaults  ────────────────────────────────────

DEFAULT_API_DOMAIN = "bot.yuanbao.tencent.com"


class YuanbaoPlatformEvent(AstrMessageEvent):
    """AstrBot event class specialised for the Yuanbao IM platform."""

    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        ws_client: "YuanbaoWsClient",
        from_account: str | None = None,
        *,
        token: str = "",
        app_key: str = "",
        app_secret: str = "",
        api_domain: str = DEFAULT_API_DOMAIN,
        route_env: str | None = None,
    ) -> None:
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.ws_client = ws_client
        self.from_account = from_account
        self._token = token
        self._app_key = app_key
        self._app_secret = app_secret
        self._api_domain = api_domain
        self._route_env = route_env

    # ── send  ─────────────────────────────────────

    async def send(self, message: MessageChain) -> None:
        msg_body = await self._build_msg_body(message)
        if not msg_body:
            msg_body = [{
                "msg_type": "TIMTextElem",
                "msg_content": {"text": message.get_plain_text() or "(empty)"},
            }]

        envelope = self._build_envelope(msg_body, message)

        try:
            await self._send_envelope(envelope)
        except Exception as exc:
            logger.error(f"[yuanbao] send failed: {exc}")
            try:
                fallback = {
                    **envelope,
                    "msg_body": [{
                        "msg_type": "TIMTextElem",
                        "msg_content": {"text": message.get_plain_text() or "..."},
                    }],
                }
                await self._send_envelope(fallback)
            except Exception as exc2:
                logger.error(f"[yuanbao] fallback send also failed: {exc2}")

        await super().send(message)

    # ── builders  ─────────────────────────────────

    async def _build_msg_body(self, message: MessageChain) -> list[dict]:
        body: list[dict] = []
        for comp in message.chain:
            if isinstance(comp, Plain):
                text = (comp.text or "").strip()
                if text:
                    body.append({"msg_type": "TIMTextElem", "msg_content": {"text": text}})

            elif isinstance(comp, Image):
                img_url = self._extract_url(comp)
                if img_url:
                    uploaded = await self._upload_image(img_url)
                    if uploaded:
                        body.append(uploaded)
                        continue
                # Try base64 inline data
                b64_data = self._extract_base64(comp)
                if b64_data:
                    uploaded = await self._upload_image_data(b64_data["data"], b64_data["content_type"])
                    if uploaded:
                        body.append(uploaded)
                        continue
                # Try local file path
                local_fp = self._extract_local_path(comp)
                if local_fp:
                    uploaded = await self._upload_image_file(local_fp)
                    if uploaded:
                        body.append(uploaded)
                        continue
                body.append({"msg_type": "TIMTextElem", "msg_content": {"text": "[图片]"}})

            elif isinstance(comp, File):
                # File.file_ = local path; File.url = remote URL (File.file is a @property — avoid)
                file_url = comp.file_ or comp.url or ""
                file_name = comp.name or os.path.basename(file_url) or "file"
                if file_url and (file_url.startswith("http://") or file_url.startswith("https://")):
                    uploaded = await self._upload_file(file_url, file_name)
                    if uploaded:
                        body.append(uploaded)
                    else:
                        body.append({"msg_type": "TIMTextElem", "msg_content": {"text": file_url}})
                else:
                    # Try local file path (like Image already supports)
                    local_fp = self._extract_local_path_for_file(comp)
                    if local_fp:
                        uploaded = await self._upload_file_file(local_fp, file_name)
                        if uploaded:
                            body.append(uploaded)
                        else:
                            body.append({"msg_type": "TIMTextElem", "msg_content": {"text": f"[文件: {file_name}]"}})
                    else:
                        body.append({"msg_type": "TIMTextElem", "msg_content": {"text": f"[文件: {file_name}]"}})

            elif isinstance(comp, Record):
                body.append({"msg_type": "TIMSoundElem", "msg_content": {"sound": comp.file or ""}})

            elif isinstance(comp, Video):
                body.append({"msg_type": "TIMVideoFileElem", "msg_content": {"data": comp.file or ""}})

            elif At is not None and isinstance(comp, At):
                qq = comp.qq
                if str(qq) == "all":
                    body.append({"msg_type": "TIMTextElem", "msg_content": {"text": "@全体成员"}})
                else:
                    name = comp.name or ""
                    text_at = f"@{name}" if name else f"@{qq}"
                    body.append({
                        "msg_type": "TIMCustomElem",
                        "msg_content": {
                            "data": json.dumps({
                                "elem_type": 1002,
                                "text": text_at,
                                "user_id": str(qq),
                            }),
                        },
                    })

            else:
                raw = getattr(comp, "text", "") or str(comp)
                if raw:
                    body.append({"msg_type": "TIMTextElem", "msg_content": {"text": raw}})
        return body

    # ── COS upload helpers  ───────────────────────

    async def _upload_image(self, image_url: str) -> dict | None:
        """Download → COS upload → TIMImageElem msg_body dict.  Returns None on failure."""
        try:
            from .yuanbao_media import download_and_upload

            refresh_cb = self._make_token_refresh_cb()
            result = await download_and_upload(
                image_url=image_url,
                token=self._token,
                bot_id=self.from_account or "",
                api_domain=self._api_domain,
                route_env=self._route_env,
                force_refresh_token=refresh_cb,
            )
            logger.debug(f"[yuanbao] COS upload OK: {result['url'][:80]}")
            return {
                "msg_type": "TIMImageElem",
                "msg_content": {
                    "uuid": result["uuid"],
                    "image_format": 255,
                    "image_info_array": [{
                        "type": 1,
                        "size": result["size"],
                        "width": result.get("width", 0),
                        "height": result.get("height", 0),
                        "url": result["url"],
                    }],
                },
            }
        except Exception as exc:
            logger.warning(f"[yuanbao] COS upload failed, falling back to text: {exc}")
            return None

    async def _upload_file(self, file_url: str, file_name: str) -> dict | None:
        try:
            from .yuanbao_media import download_and_upload

            refresh_cb = self._make_token_refresh_cb()
            result = await download_and_upload(
                image_url=file_url,
                token=self._token,
                bot_id=self.from_account or "",
                api_domain=self._api_domain,
                route_env=self._route_env,
                force_refresh_token=refresh_cb,
            )
            return {
                "msg_type": "TIMFileElem",
                "msg_content": {
                    "uuid": result["uuid"],
                    "file_name": result["filename"] or file_name,
                    "file_size": result["size"],
                    "url": result["url"],
                },
            }
        except Exception as exc:
            logger.warning(f"[yuanbao] COS upload (file) failed: {exc}")
            return None

    def _make_token_refresh_cb(self):
        """Return an async callable that re‑signs the token using app_key/app_secret."""
        app_key = self._app_key
        app_secret = self._app_secret
        api_domain = self._api_domain
        route_env = self._route_env

        async def _refresh() -> object | None:
            if not app_key or not app_secret:
                logger.error("[yuanbao] cannot refresh token: missing app_key/app_secret")
                return None
            from .yuanbao_sign import sign_token
            try:
                data = await sign_token(
                    app_key=app_key,
                    app_secret=app_secret,
                    api_domain=api_domain,
                    route_env=route_env,
                )
                # Update the event's cached values so subsequent calls use the fresh token
                self._token = data.token
                if data.bot_id:
                    self.from_account = data.bot_id
                logger.info(f"[yuanbao] token refreshed during upload, new bot_id={data.bot_id}")
                return data
            except Exception as exc:
                logger.error(f"[yuanbao] token refresh during upload failed: {exc}")
                return None

        return _refresh

    # ── envelope / dispatch  ──────────────────────

    def _build_envelope(self, msg_body: list[dict], message: MessageChain | None = None) -> dict:
        from astrbot.core.platform.message_type import MessageType

        msg_obj = self.message_obj
        is_group = msg_obj.type == MessageType.GROUP_MESSAGE
        group_code = ""
        sender_id = msg_obj.sender.user_id if msg_obj.sender else ""
        if is_group:
            # Get group_code from group object or fall back to session_id
            if msg_obj.group and msg_obj.group.group_id:
                group_code = msg_obj.group.group_id
            else:
                group_code = (msg_obj.session_id or "")

        to_account = sender_id if is_group else (msg_obj.session_id or "")
        env: dict = {
            "to_account": to_account,
            "msg_body": msg_body,
            "from_account": self.from_account or "",
            "msg_random": uuid.uuid4().int % (2**32 - 1),
        }
        if group_code:
            env["group_code"] = group_code
        env["_is_group"] = is_group or bool(group_code)

        # Attach ref_msg_id for group reply quoting ONLY when AstrBot's
        # ResultDecorateStage has inserted a Reply component into the chain
        # (controlled by the reply_with_quote config).  This mirrors how the
        # Telegram / aiocqhttp adapters handle outbound quoting.
        if is_group and group_code and message is not None:
            ref_msg_id = self._extract_ref_msg_id(message)
            if ref_msg_id:
                env["ref_msg_id"] = str(ref_msg_id)

        return env

    @staticmethod
    def _extract_ref_msg_id(message: MessageChain) -> str | None:
        """Look for a Reply component in the outgoing MessageChain.

        The Reply component is injected by AstrBot's ``ResultDecorateStage``
        when ``reply_with_quote`` is enabled.  If present, its ``id`` is used
        as ``refMsgId`` in the protobuf SendGroupMessageReq.
        """
        from astrbot.api.message_components import Reply as ReplyComp

        for comp in getattr(message, "chain", []) or []:
            if isinstance(comp, ReplyComp) and comp.id:
                return str(comp.id)
        return None

    async def _send_envelope(self, envelope: dict) -> None:
        import random as _random
        from . import yuanbao_codec as codec
        from .yuanbao_client import ClientState

        if self.ws_client is None:
            raise RuntimeError("[yuanbao] WS client not available")
        if self.ws_client.state is not ClientState.CONNECTED:
            raise RuntimeError(
                f"[yuanbao] WS not connected (state={self.ws_client.state}), cannot send"
            )

        msg_body = envelope.get("msg_body", [])
        to_account = envelope.get("to_account", "")
        from_account = envelope.get("from_account", self.from_account)
        group_code = envelope.get("group_code", "")
        is_group = envelope.get("_is_group", False)
        msg_random = envelope.get("msg_random", _random.randint(0, 2**32 - 1))
        ref_msg_id = envelope.get("ref_msg_id", "")

        if is_group or group_code:
            biz_data = codec.encode_send_group_message_req(
                group_code=group_code or "",
                msg_body=msg_body,
                from_account=from_account,
                to_account=to_account,
                msg_random=msg_random,
                ref_msg_id=str(ref_msg_id) if ref_msg_id else "",
            )
            cmd = codec.BIZ_CMD_SEND_GROUP
        else:
            biz_data = codec.encode_send_c2c_message_req(
                to_account=to_account,
                msg_body=msg_body,
                from_account=from_account,
                group_code=group_code or "",
                msg_random=msg_random,
            )
            cmd = codec.BIZ_CMD_SEND_C2C

        ok = await self.ws_client.send_biz_frame(cmd, codec.BIZ_MODULE, biz_data)
        if not ok:
            raise RuntimeError("[yuanbao] send_biz_frame returned False")

        logger.debug(
            f"[yuanbao] send OK: cmd={cmd}, to={to_account}, "
            f"group={group_code}, body_len={len(msg_body)}"
        )

    # ── helpers  ──────────────────────────────────

    async def _upload_image_file(self, filepath: str) -> dict | None:
        """Read local file → upload to COS → TIMImageElem.  Returns None on failure."""
        import mimetypes
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            content_type, _ = mimetypes.guess_type(filepath)
            if not content_type or not content_type.startswith("image/"):
                content_type = "image/png"
            return await self._upload_image_data(data, content_type)
        except Exception as exc:
            logger.warning(f"[yuanbao] local file upload failed ({filepath}): {exc}")
            return None

    async def _upload_file_file(self, filepath: str, file_name: str = "") -> dict | None:
        """Read local file → upload to COS → TIMFileElem.  Returns None on failure."""
        import mimetypes
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            content_type, _ = mimetypes.guess_type(filepath)
            if not content_type:
                content_type = "application/octet-stream"
            return await self._upload_file_data(data, content_type, file_name or os.path.basename(filepath))
        except Exception as exc:
            logger.warning(f"[yuanbao] local file upload failed ({filepath}): {exc}")
            return None

    async def _upload_file_data(self, data: bytes, content_type: str, file_name: str = "file") -> dict | None:
        """Upload raw file bytes to COS → TIMFileElem.  Returns None on failure."""
        try:
            from .yuanbao_media import upload_raw

            refresh_cb = self._make_token_refresh_cb()
            result = await upload_raw(
                data=data, filename=file_name, content_type=content_type,
                token=self._token, bot_id=self.from_account or "",
                api_domain=self._api_domain, route_env=self._route_env,
                force_refresh_token=refresh_cb,
            )
            return {
                "msg_type": "TIMFileElem",
                "msg_content": {
                    "uuid": result["uuid"],
                    "file_name": result["filename"] or file_name,
                    "file_size": result["size"],
                    "url": result["url"],
                },
            }
        except Exception as exc:
            logger.warning(f"[yuanbao] file data upload failed: {exc}")
            return None

    @staticmethod
    def _extract_local_path_for_file(comp: File) -> str | None:
        """Extract a local file path from a File component (file:// or bare path).

        Note: File.file is a @property that may trigger sync download — avoid it.
        Use file_ (local path) and url (remote URL) instead.
        """
        candidates = [
            getattr(comp, "file_", None),
            getattr(comp, "url", None),
            getattr(comp, "path", None),
        ]
        for c in candidates:
            if not c:
                continue
            c = str(c).strip()
            if c.startswith("file:///"):
                p = c[8:]
                if os.path.exists(p):
                    return p
            elif c.startswith("file://"):
                p = c[7:]
                if os.path.exists(p):
                    return p
            elif not c.startswith("http://") and not c.startswith("https://"):
                if os.path.exists(c):
                    return c
        return None

    async def _upload_image_data(self, data: bytes, content_type: str) -> dict | None:
        """Upload raw image bytes to COS → TIMImageElem.  Returns None on failure."""
        try:
            from .yuanbao_media import upload_raw

            refresh_cb = self._make_token_refresh_cb()
            ext_map = {"image/jpeg": ".jpg", "image/png": ".png",
                       "image/gif": ".gif", "image/webp": ".webp"}
            ext = ext_map.get(content_type, ".png")
            filename = f"img_{uuid.uuid4().hex[:8]}{ext}"

            result = await upload_raw(
                data=data, filename=filename, content_type=content_type,
                token=self._token, bot_id=self.from_account or "",
                api_domain=self._api_domain, route_env=self._route_env,
                force_refresh_token=refresh_cb,
            )
            return {
                "msg_type": "TIMImageElem",
                "msg_content": {
                    "uuid": result["uuid"], "image_format": 255,
                    "image_info_array": [{
                        "type": 1, "size": result["size"],
                        "width": result.get("width", 0),
                        "height": result.get("height", 0),
                        "url": result["url"],
                    }],
                },
            }
        except Exception as exc:
            logger.warning(f"[yuanbao] base64 image upload failed: {exc}")
            return None

    @staticmethod
    def _extract_base64(comp: Image) -> dict | None:
        """Decode a base64 image from Image.file (data: or base64://)."""
        raw = getattr(comp, "file", "") or getattr(comp, "url", "") or ""
        raw = str(raw).strip()
        import base64 as _b64
        try:
            if raw.startswith("data:"):
                # data:image/png;base64,iVBORw0...
                header, _, b64 = raw.partition(",")
                content_type = header.split(";")[0].replace("data:", "", 1) or "image/png"
                return {"data": _b64.b64decode(b64), "content_type": content_type}
            if raw.startswith("base64://"):
                b64 = raw[len("base64://"):]
                # Guess type from mime hints or default to png
                content_type = "image/png"
                return {"data": _b64.b64decode(b64), "content_type": content_type}
        except Exception:
            pass
        return None

    @staticmethod
    def _extract_local_path(comp: Image) -> str | None:
        """Extract a local file path from an Image component (file:// or bare path)."""
        candidates = [
            getattr(comp, "file", None),
            getattr(comp, "url", None),
            getattr(comp, "path", None),
            getattr(comp, "file_", None),
        ]
        for c in candidates:
            if not c:
                continue
            c = str(c).strip()
            if c.startswith("file:///"):
                p = c[8:]
                if os.path.exists(p):
                    return p
            elif c.startswith("file://"):
                p = c[7:]
                if os.path.exists(p):
                    return p
            elif not c.startswith("http://") and not c.startswith("https://"):
                if os.path.exists(c):
                    return c
        return None

    @staticmethod
    def _extract_url(comp: Image) -> str | None:
        for attr in ("file", "url", "path"):
            val = getattr(comp, attr, None)
            if val:
                val = str(val).strip()
                if val.startswith("http://") or val.startswith("https://"):
                    return val
        return None
