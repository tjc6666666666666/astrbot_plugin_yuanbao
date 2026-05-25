"""
Yuanbao platform adapter — bridges the Yuanbao IM platform to AstrBot.

This adapter:
1. Signs a security token via the Yuanbao sign-token API (HMAC-SHA256).
2. Establishes a persistent WebSocket connection to the Yuanbao backend.
3. Converts Yuanbao inbound push events into AstrBotMessage instances.
4. Submits them as AstrMessageEvent objects into the AstrBot event pipeline.
5. Sends outbound replies back through the same WebSocket.

Configuration (stored in AstrBot's platform config):
  token  – colon-separated "appKey:appSecret" string (preferred), OR
  app_key  + app_secret – individual credentials
  ws_url  – optional, defaults to wss://bot-wss.yuanbao.tencent.com/wss/connection
  api_domain – optional, defaults to bot.yuanbao.tencent.com
  require_mention – whether group chat requires @mention (default True)
  enabled – boolean flag
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any

from astrbot.api.platform import (
    Platform,
    AstrBotMessage,
    MessageMember,
    PlatformMetadata,
    MessageType,
    register_platform_adapter,
)
from astrbot.api.event import MessageChain
from astrbot.api.message_components import Plain, Image, File, Record, Reply, Video

try:
    from astrbot.api.message_components import At
except ImportError:
    At = None
from astrbot.core.platform.astr_message_event import MessageSesion
from astrbot.core.utils.astrbot_path import get_astrbot_temp_path
from astrbot.api import logger

from .yuanbao_client import YuanbaoWsClient, ClientState
from .yuanbao_sign import sign_token, SignTokenError
from .yuanbao_codec import (
    parse_push_content_to_msg_body,
    extract_text_from_msg_body,
    extract_media_from_msg_body,
    extract_mentions_from_msg_body,
)
from .yuanbao_platform_event import YuanbaoPlatformEvent
from . import yuanbao_codec as codec


# ── Defaults ─────────────────────────────────────────────
DEFAULT_WS_URL = "wss://bot-wss.yuanbao.tencent.com/wss/connection"
DEFAULT_API_DOMAIN = "bot.yuanbao.tencent.com"

# Registry of active adapter instances (used by the event class for outbound dispatch)
_active_adapters: dict[str, "YuanbaoPlatformAdapter"] = {}


# ── Decorator registration ──────────────────────────────
@register_platform_adapter(
    "yuanbao",
    "腾讯元宝 (Yuanbao) 适配器 — 基于 WebSocket 协议",
    default_config_tmpl={
        "token": "",
        "app_key": "",
        "app_secret": "",
        "ws_url": DEFAULT_WS_URL,
        "api_domain": DEFAULT_API_DOMAIN,
        "require_mention": True,
        "enable": False,
        "id": "yuanbao",
    },
)
class YuanbaoPlatformAdapter(Platform):
    """AstrBot platform adapter for Tencent Yuanbao IM."""

    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)
        self.config = platform_config
        self.settings = platform_settings
        self.client: YuanbaoWsClient | None = None
        self._stop_event = asyncio.Event()
        self._app_key: str = ""
        self._app_secret: str = ""
        self._from_account: str = ""
        self._token: str = ""
        self._route_env: str | None = None

        # Media timeline cache: mirrors openclaw-plugin-yuanbao's chatMediaHistories
        # key = group_code, value = list of {msg_id, medias: list[media_item]}
        self._media_timeline: dict[str, list[dict[str, Any]]] = {}
        self._MAX_MEDIA_TIMELINE = 50  # max entries per group (matches JS LRU cap)

        # Register self
        adapter_id = platform_config.get("id", "yuanbao")
        _active_adapters[adapter_id] = self

    # ── Platform abstract methods ──────────────────────

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(
            name="yuanbao",
            description="腾讯元宝 (Yuanbao) 适配器",
            id=self.config.get("id", "yuanbao"),
            default_config_tmpl=None,
        )

    async def run(self) -> None:
        """
        Main adapter loop.

        1. Parse credentials from config.
        2. Sign a security token via HTTP API.
        3. Connect to the Yuanbao WebSocket gateway.
        4. Process inbound push events until stopped.
        """
        await self._resolve_credentials()

        if not self._app_key or not self._app_secret:
            logger.error(
                "[yuanbao] 缺少 app_key / app_secret，请在配置中设置 token "
                '(格式 "appKey:appSecret") 或分别填写 app_key, app_secret'
            )
            self._stop_event.set()
            return

        # Sign token
        try:
            token_data = await sign_token(
                app_key=self._app_key,
                app_secret=self._app_secret,
                api_domain=self.config.get("api_domain", DEFAULT_API_DOMAIN),
                route_env=self.config.get("route_env"),
            )
            logger.info(f"[yuanbao] 令牌签名成功, bot_id={token_data.bot_id}")
            self._from_account = token_data.bot_id
            self._token = token_data.token
        except SignTokenError as exc:
            logger.error(f"[yuanbao] 令牌签名失败: {exc}")
            self._stop_event.set()
            return

        # Build auth meta
        auth: dict[str, str] = {
            "bizId": "ybBot",
            "uid": token_data.bot_id,
            "source": token_data.source or "bot",
            "token": token_data.token,
        }
        route_env = self.config.get("route_env")
        if route_env:
            auth["routeEnv"] = route_env
            self._route_env = route_env
        else:
            self._route_env = None

        ws_url = self.config.get("ws_url", DEFAULT_WS_URL)

        # Create WS client
        self.client = YuanbaoWsClient(
            gateway_url=ws_url,
            auth=auth,
            on_ready=self._on_ws_ready,
            on_dispatch=self._on_ws_dispatch,
            on_error=self._on_ws_error,
            on_close=self._on_ws_close,
            on_auth_failed=self._on_auth_failed,
        )

        # Connect (blocking until aborted)
        try:
            await self.client.connect()
            # Wait for stop signal
            await self._stop_event.wait()
        except asyncio.CancelledError:
            logger.info("[yuanbao] 适配器被取消")
        except Exception as exc:
            logger.error(f"[yuanbao] 适配器运行异常: {exc}")
        finally:
            if self.client:
                await self.client.disconnect()

    async def send_by_session(
        self, session: MessageSesion, message_chain: MessageChain
    ) -> None:
        """
        Send a message via a persistent session.

        For Yuanbao this converts the MessageChain into a msg_body and
        dispatches it through the WebSocket client.
        """
        await super().send_by_session(session, message_chain)

    async def terminate(self) -> None:
        """Graceful shutdown."""
        logger.info("[yuanbao] 正在关闭适配器...")
        self._stop_event.set()
        if _active_adapters:
            _active_adapters.pop(self.config.get("id", "yuanbao"), None)
        if self.client:
            await self.client.disconnect()

    # ── WS callbacks ──────────────────────────────────

    async def _on_ws_ready(self, data: dict) -> None:
        logger.info(
            f"[yuanbao] WebSocket 就绪: connectId={data.get('connectId')} ✅"
        )

    async def _on_ws_dispatch(self, push_event: dict) -> None:
        """Convert a WS push event into an AstrBotMessage and commit it."""
        try:
            msg = await self._convert_push_to_message(push_event)
            if msg is None:
                return

            # Build AstrMessageEvent and commit
            event = YuanbaoPlatformEvent(
                message_str=msg.message_str,
                message_obj=msg,
                platform_meta=self.meta(),
                session_id=msg.session_id,
                ws_client=self.client,
                from_account=self._from_account,
                token=self._token,
                app_key=self._app_key,
                app_secret=self._app_secret,
                api_domain=self.config.get("api_domain", DEFAULT_API_DOMAIN),
                route_env=self._route_env,
            )
            self.commit_event(event)
            logger.debug(
                f"[yuanbao] 消息已提交: type={msg.type}, "
                f"session={msg.session_id}, text={msg.message_str[:50]}"
            )
        except Exception as exc:
            logger.error(f"[yuanbao] 处理推送事件异常: {exc}", exc_info=True)

    async def _on_ws_error(self, message: str) -> None:
        logger.error(f"[yuanbao] WebSocket 错误: {message}")

    async def _on_ws_close(self, code: int, reason: str) -> None:
        logger.info(f"[yuanbao] WebSocket 关闭: code={code}, reason={reason}")

    async def _on_auth_failed(self, error_code: int) -> dict | None:
        """Refresh token on auth failure."""
        logger.warning(f"[yuanbao] 认证失败 (code={error_code}), 正在刷新令牌...")
        try:
            token_data = await sign_token(
                app_key=self._app_key,
                app_secret=self._app_secret,
                api_domain=self.config.get("api_domain", DEFAULT_API_DOMAIN),
                route_env=self.config.get("route_env"),
            )
            self._from_account = token_data.bot_id
            self._token = token_data.token
            return {
                "bizId": "ybBot",
                "uid": token_data.bot_id,
                "source": token_data.source or "bot",
                "token": token_data.token,
            }
        except SignTokenError as exc:
            logger.error(f"[yuanbao] 令牌刷新失败: {exc}")
            return None

    # ── Inbound message conversion ────────────────────

    async def _convert_push_to_message(self, push: dict) -> AstrBotMessage | None:
        """
        Convert a raw WS push event to an AstrBotMessage.

        The push event may arrive in several shapes:
        - DirectedPush / PushMsg with JSON content
        - Raw protobuf data (decoded in gateway, passed as connData / rawData)
        - Plain text from the 'content' field
        """
        msg_body: list[dict] | None = None
        extra: dict[str, Any] = {}

        # 1. Try connData / rawData as JSON
        for key in ("connData", "rawData"):
            data = push.get(key)
            if isinstance(data, bytes) and len(data) > 0:
                try:
                    obj = json.loads(data.decode("utf-8"))
                    if isinstance(obj, dict):
                        extra.update(obj)
                        if "msg_body" in obj:
                            msg_body = obj.get("msg_body")
                except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                    pass

        # 2. Try content field
        content = push.get("content")
        if isinstance(content, str) and content.strip():
            parsed = parse_push_content_to_msg_body(content)
            if parsed:
                msg_body = parsed
                try:
                    obj = json.loads(content)
                    if isinstance(obj, dict):
                        extra.update(obj)
                except (json.JSONDecodeError, TypeError):
                    pass

        if not msg_body:
            logger.debug("[yuanbao] 无法解析消息体, 跳过")
            return None

        # ── Parse quoted / reply message from cloud_custom_data ──
        # Mirrors the openclaw-plugin-yuanbao resolve-quote middleware:
        #   cloud_custom_data is a JSON string embedded in the push payload
        #   that carries a ``quote`` object with the quoted message metadata.
        #
        #   quote.type:  1=text  2=image  3=file  4=video  5=audio
        #   quote.desc:  text description (filename for files, empty for images)
        quote_info: dict | None = None
        cloud_custom_str = extra.get("cloud_custom_data") or extra.get("cloudCustomData")
        if isinstance(cloud_custom_str, str) and cloud_custom_str.strip():
            try:
                ccd = json.loads(cloud_custom_str)
                if isinstance(ccd, dict) and "quote" in ccd:
                    q = ccd["quote"]
                    if isinstance(q, dict):
                        raw_desc = str(q.get("desc", "")).strip()
                        qtype = int(q.get("type", 0) or 0)
                        # Enrich desc for media types — mirrors JS parseQuoteFromCloudCustomData
                        if qtype == 2:   # image
                            raw_desc = raw_desc or "[图片]"
                        elif qtype == 3: # file
                            raw_desc = raw_desc or "[文件]"
                        elif qtype == 4: # video
                            raw_desc = raw_desc or "[视频]"
                        elif qtype == 5: # audio
                            raw_desc = raw_desc or "[语音]"
                        if raw_desc:
                            q["desc"] = raw_desc
                            quote_info = q
            except (json.JSONDecodeError, TypeError):
                pass

        # Determine chat type
        group_code = extra.get("group_code") or extra.get("groupCode") or ""
        is_group = bool(group_code) or push.get("cmd", "").startswith("Group.")

        # Extract @mentions (for At components — AstrBot's WakingCheckStage handles wake)
        mentions = extract_mentions_from_msg_body(msg_body)

        # Build AstrBotMessage
        abm = AstrBotMessage()

        if is_group:
            abm.type = MessageType.GROUP_MESSAGE
            abm.group_id = group_code
        else:
            abm.type = MessageType.FRIEND_MESSAGE

        # Sender
        from_uid = extra.get("from_account") or extra.get("fromAccount") or "unknown"
        nickname = extra.get("sender_nickname") or extra.get("senderNickname") or ""
        abm.sender = MessageMember(user_id=from_uid, nickname=nickname)

        # Message content — extract text + media
        abm.message_str = extract_text_from_msg_body(msg_body)
        abm.message = [Plain(text=abm.message_str)]

        # Add At components for @mentions (so AstrBot's WakingCheckStage detects them)
        if At is not None and mentions:
            at_components = []
            for m in mentions:
                uid = m["user_id"]
                text = m.get("text", "")
                # text format is "@Nickname" — strip the "@" for the name field
                name = text.lstrip("@") if text.startswith("@") else text
                at_components.append(At(qq=uid, name=name))
            # Insert At components at the front of the chain (before Plain)
            abm.message = at_components + abm.message

        # Insert Reply component at the front of the chain (before At and Plain)
        # so AstrBot's WakingCheckStage can detect it and LLM can process the quoted context.
        #
        # We populate *both* message_str AND chain:
        #   - ``chain``  is checked first by extract_quoted_message_text() / _process_quote_message()
        #   - ``message_str`` is the fallback (and also used for log display)
        if quote_info:
            desc = str(quote_info.get("desc", ""))
            sender_nick = str(quote_info.get("sender_nickname", ""))
            sender_id = str(quote_info.get("sender_id", ""))

            # Build a human-readable summary for the chain Plain component
            # e.g. "[引用文件 from 张三]: Google-Play-Store_49.7.20-29_0_PR_854325918.apk.js"
            sender_label = sender_nick or sender_id or "unknown"
            reply_plain_text = f"[引用消息 from {sender_label}]: {desc}"

            reply_comp = Reply(
                id=str(quote_info.get("id") or quote_info.get("seq", "")),
                chain=[Plain(text=reply_plain_text)],
                message_str=desc,
                sender_id=str(quote_info.get("sender_id", "")),
                sender_nickname=str(quote_info.get("sender_nickname", "")),
                time=int(quote_info.get("time", 0) or 0),
            )
            abm.message = [reply_comp] + abm.message

        # Extract media elements from msg_body and populate Image/File/Record/Video components
        media_items = extract_media_from_msg_body(msg_body)
        for mi in media_items:
            media_type = mi.get("type", "")
            if media_type == "image":
                abm.message.append(Image(file=mi.get("url", "")))
            elif media_type == "file":
                abm.message.append(File(
                    name=mi.get("file_name", "file"),
                    url=mi.get("url", ""),
                ))
            elif media_type == "record":
                abm.message.append(Record(file=mi.get("url", "")))
            elif media_type == "video":
                abm.message.append(Video(file=mi.get("url", "")))

        # ── Record current media to timeline cache (mirrors recordMediaHistory) ──
        #  This lets future quote lookups recover the media URLs by message ID.
        #  Deferred: abm.message_id is set below (after the download block).
        #  We record in the tail section after abm.message_id is assigned.

        # ── Resolve quoted-message media from the timeline cache  ──
        #  Mirrors getHistoryMedias() in download-media.js: look up the
        #  quoted message's media by quote_info.id in the local LRU cache.
        quote_media_items: list[dict] = []
        if quote_info and is_group and group_code:
            quote_msg_id = str(quote_info.get("id") or "")
            if quote_msg_id:
                quote_media_items = self._lookup_quote_media(quote_msg_id, group_code)

        # Combine current + quoted media for a single download pass
        all_media = list(media_items)
        seen_urls = {m.get("url") for m in all_media if m.get("url")}
        for qm in quote_media_items:
            if qm.get("url") and qm["url"] not in seen_urls:
                all_media.append(qm)
                seen_urls.add(qm["url"])

        # Download all media to local temp paths so AI agents can access them
        quote_local_map: dict[str, dict] = {}  # url → download result (for quote media)
        current_local_map: dict[str, dict] = {}  # url → download result (for current media)
        current_urls = {m.get("url") for m in media_items if m.get("url")}
        if all_media:
            try:
                from .yuanbao_media import download_medias_to_local

                cache_dir = os.path.join(get_astrbot_temp_path(), "yuanbao-media")
                refresh_cb = self._make_media_token_refresh_cb()
                local_results = await download_medias_to_local(
                    medias=all_media,
                    token=self._token,
                    bot_id=self._from_account,
                    api_domain=self.config.get("api_domain", DEFAULT_API_DOMAIN),
                    route_env=self._route_env,
                    force_refresh_token=refresh_cb,
                    cache_dir=cache_dir,
                )
                result_by_url = {r["url"]: r for r in local_results}
                # Separate current vs quote results
                for url, r in result_by_url.items():
                    if url in current_urls:
                        current_local_map[url] = r
                    else:
                        quote_local_map[url] = r

                # Replace current-media components with local‑path versions in the main chain
                new_chain: list = list(abm.message[:1])  # keep Reply (or first Plain if no Reply)
                for comp in abm.message[1:]:
                    comp_url = getattr(comp, "url", "") or getattr(comp, "file_", "") or getattr(comp, "file", "")
                    if comp_url and comp_url in current_local_map:
                        r = current_local_map[comp_url]
                        local_path = r["local_path"]
                        if isinstance(comp, Image):
                            new_chain.append(Image(file=local_path))
                        elif isinstance(comp, File):
                            new_chain.append(File(
                                name=getattr(comp, "name", None) or r.get("file_name", "file"),
                                file=local_path,
                                url=comp_url,
                            ))
                        elif isinstance(comp, Record):
                            new_chain.append(Record(file=local_path))
                        elif isinstance(comp, Video):
                            new_chain.append(Video(file=local_path))
                        else:
                            new_chain.append(comp)
                    else:
                        new_chain.append(comp)
                abm.message = new_chain

                # Attach quoted-media components into Reply.chain so
                # AstrBot's _process_quote_message() / _build_llm_request()
                # exposes them to the LLM as quoted attachments.
                if quote_media_items and quote_local_map and isinstance(abm.message[0], Reply):
                    reply_comp: Reply = abm.message[0]
                    for qm in quote_media_items:
                        url = qm.get("url", "")
                        if url and url in quote_local_map:
                            r = quote_local_map[url]
                            local_path = r["local_path"]
                            mtype = qm.get("type", "")
                            if mtype == "image":
                                reply_comp.chain.append(Image(file=local_path))
                            elif mtype == "file":
                                reply_comp.chain.append(File(
                                    name=qm.get("file_name") or r.get("file_name", "file"),
                                    file=local_path,
                                    url=url,
                                ))
            except Exception as exc:
                logger.warning(f"[yuanbao] media download failed, using remote URLs: {exc}")

        abm.raw_message = push
        abm.self_id = self._from_account
        abm.session_id = group_code if is_group else from_uid
        abm.message_id = extra.get("msg_id") or extra.get("msgId") or push.get("msgId", uuid.uuid4().hex)
        abm.timestamp = int(extra.get("msg_time", 0) or extra.get("msgTime", 0) or 0)

        # Record current media to timeline cache (mirrors recordMediaHistory)
        if is_group and group_code and media_items:
            self._record_media(group_code, abm.message_id, media_items)

        # Group info
        if is_group and group_code:
            from astrbot.api.platform import Group
            abm.group = Group(
                group_id=group_code,
                group_name=extra.get("group_name") or extra.get("groupName") or "",
            )

        return abm

    # ── Credential resolution ────────────────────────

    async def _resolve_credentials(self) -> None:
        """
        Resolve app_key and app_secret from config.

        Supports the colon-separated token format ("appKey:appSecret") as well
        as explicit app_key / app_secret fields.
        """
        cfg = self.config

        # Already set?
        if self._app_key and self._app_secret:
            return

        token_str = (cfg.get("token") or "").strip()
        if token_str:
            colon = token_str.index(":") if ":" in token_str else -1
            if colon > 0:
                self._app_key = token_str[:colon].strip()
                self._app_secret = token_str[colon + 1 :].strip()
            else:
                # Could be a pre-signed static token — still needs app_key/app_secret
                logger.warning(
                    "[yuanbao] token 格式不包含冒号，请同时提供 app_key / app_secret"
                )

        # Explicit fields take precedence
        explicit_key = (cfg.get("app_key") or "").strip()
        explicit_secret = (cfg.get("app_secret") or "").strip()
        if explicit_key:
            self._app_key = explicit_key
        if explicit_secret:
            self._app_secret = explicit_secret

    # ── Media timeline cache (mirrors JS chatMediaHistories) ──

    def _record_media(self, group_code: str, msg_id: str, medias: list[dict]) -> None:
        """Record a message's media to the timeline cache for future quote lookups.

        Mirrors ``recordMediaHistory`` from download-media.js.
        Limited to ``_MAX_MEDIA_TIMELINE`` entries per group (LRU-eviction by oldest).
        """
        if not group_code or not msg_id or not medias:
            return
        tl = self._media_timeline.setdefault(group_code, [])
        # Remove duplicate entry for same msg_id if any
        tl = [e for e in tl if e.get("msg_id") != msg_id]
        tl.append({"msg_id": msg_id, "medias": medias, "time": int(asyncio.get_running_loop().time())})
        if len(tl) > self._MAX_MEDIA_TIMELINE:
            tl = tl[-self._MAX_MEDIA_TIMELINE:]
        self._media_timeline[group_code] = tl

    def _lookup_quote_media(self, quote_msg_id: str, group_code: str) -> list[dict]:
        """Look up a quoted message's media items from the timeline cache.

        Mirrors ``getHistoryMedias`` from download-media.js.
        Returns the cached medias list, or [].

        The returned items have the same shape as ``extract_media_from_msg_body``:
            { "type": "image"|"file", "url": str, "file_name": str, "msg_content": dict }
        """
        if not quote_msg_id or not group_code:
            return []
        tl = self._media_timeline.get(group_code, [])
        for entry in reversed(tl):
            if str(entry.get("msg_id", "")) == quote_msg_id:
                return entry.get("medias", [])
        return []

    def _make_media_token_refresh_cb(self):
        """Return an async callable that re‑signs the token for media download retries."""
        app_key = self._app_key
        app_secret = self._app_secret
        api_domain = self.config.get("api_domain", DEFAULT_API_DOMAIN)
        route_env = self._route_env
        adapter = self

        async def _refresh() -> object | None:
            if not app_key or not app_secret:
                logger.error("[yuanbao] cannot refresh token for media: missing app_key/app_secret")
                return None
            try:
                data = await sign_token(
                    app_key=app_key,
                    app_secret=app_secret,
                    api_domain=api_domain,
                    route_env=route_env,
                )
                adapter._token = data.token
                adapter._from_account = data.bot_id
                logger.info(f"[yuanbao] token refreshed during media download, bot_id={data.bot_id}")
                return data
            except SignTokenError as exc:
                logger.error(f"[yuanbao] token refresh during media download failed: {exc}")
                return None

        return _refresh

    # ── Outbound send (called by event class) ────────

    async def send_raw(self, envelope: dict, *, is_group: bool = False) -> None:
        """Encode and send a raw msg_body envelope over the WebSocket."""
        import random as _random

        if self.client is None or self.client.state is not ClientState.CONNECTED:
            raise RuntimeError("[yuanbao] WebSocket not connected, cannot send")

        msg_body = envelope.get("msg_body", [])
        to_account = envelope.get("to_account", "")
        from_account = envelope.get("from_account", self._from_account)
        group_code = envelope.get("group_code", "")
        msg_random = envelope.get("msg_random", _random.randint(0, 2**32 - 1))
        ref_msg_id = envelope.get("ref_msg_id", "")

        if is_group or group_code:
            biz_data = codec.encode_send_group_message_req(
                group_code=group_code,
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
                group_code=group_code,
                msg_random=msg_random,
            )
            cmd = codec.BIZ_CMD_SEND_C2C

        ok = await self.client.send_biz_frame(cmd, codec.BIZ_MODULE, biz_data)
        if not ok:
            raise RuntimeError("[yuanbao] Failed to send business frame")

        logger.debug(
            f"[yuanbao] send_raw OK: cmd={cmd}, to={to_account}, "
            f"group={group_code}, body_len={len(msg_body)}"
        )
