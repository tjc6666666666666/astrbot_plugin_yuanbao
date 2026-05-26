"""
Yuanbao WebSocket client — connection lifecycle, auth, heartbeat, auto-reconnect.

A Python reimplementation of the `YuanbaoWsClient` from
openclaw-plugin-yuanbao.  Communicates over binary WebSocket frames
using the conn-layer protobuf protocol (ConnMsg).

Usage::

    client = YuanbaoWsClient(
        gateway_url="wss://...",
        auth={"bizId":"ybBot", "uid":"...", "source":"bot", "token":"..."},
        on_dispatch=my_handler,
        on_ready=my_ready_cb,
    )
    await client.connect()
"""

from __future__ import annotations

import asyncio
import platform
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from . import yuanbao_codec as codec

# ─────────────────────────────────────────────
#  Constants (mirror openclaw-plugin-yuanbao)
# ─────────────────────────────────────────────

DEFAULT_RECONNECT_DELAYS: list[float] = [1, 2, 5, 10, 30, 60]
NO_RECONNECT_CLOSE_CODES: set[int] = {4012, 4013, 4014, 4018, 4019, 4021}
DEFAULT_MAX_RECONNECT_ATTEMPTS = 100
DEFAULT_HEARTBEAT_INTERVAL_S = 5
HEARTBEAT_TIMEOUT_THRESHOLD = 2
AUTH_FAILED_CODES: set[int] = {41103, 41104, 41108}
AUTH_ALREADY_CODE = 41101
AUTH_RETRYABLE_CODES: set[int] = {50400, 50503, 90001, 90003}


class ClientState(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTHENTICATING = "authenticating"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"


def _generate_msg_id() -> str:
    return uuid.uuid4().hex


# ─────────────────────────────────────────────
#  Callback type aliases
# ─────────────────────────────────────────────

OnReady = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
OnDispatch = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
OnStateChange = Callable[[ClientState], Coroutine[Any, Any, None]]
OnError = Callable[[str], Coroutine[Any, Any, None]]
OnClose = Callable[[int, str], Coroutine[Any, Any, None]]
OnAuthFailed = Callable[[int], Coroutine[Any, Any, dict | None]]


# ─────────────────────────────────────────────
#  Client
# ─────────────────────────────────────────────

@dataclass
class YuanbaoWsClient:
    gateway_url: str
    auth: dict[str, str]  # bizId, uid, source, token, routeEnv?
    on_ready: OnReady | None = None
    on_dispatch: OnDispatch | None = None
    on_state_change: OnStateChange | None = None
    on_error: OnError | None = None
    on_close: OnClose | None = None
    on_auth_failed: OnAuthFailed | None = None

    max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS
    reconnect_delays: list[float] = field(
        default_factory=lambda: list(DEFAULT_RECONNECT_DELAYS)
    )

    # -- private state --
    _ws: ClientConnection | None = field(default=None, init=False)
    _state: ClientState = field(default=ClientState.DISCONNECTED, init=False)
    _connect_id: str | None = field(default=None, init=False)
    _heartbeat_interval_s: int = field(default=DEFAULT_HEARTBEAT_INTERVAL_S, init=False)
    _heartbeat_ack_received: bool = field(default=True, init=False)
    _heartbeat_timeout_count: int = field(default=0, init=False)
    _last_heartbeat_at: float = field(default=0.0, init=False)
    _reconnect_attempts: int = field(default=0, init=False)
    _disposed: bool = field(default=False, init=False)
    _heartbeat_task: asyncio.Task | None = field(default=None, init=False)
    _recv_task: asyncio.Task | None = field(default=None, init=False)
    _connection_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    # ── public API ──────────────────────────

    def update_auth(self, auth: dict[str, str]) -> None:
        self.auth = {**self.auth, **auth}

    async def connect(self) -> None:
        """Initiate connection and start the receive loop."""
        if self._disposed:
            raise RuntimeError("Client has been disposed")
        await self._do_connect()

    async def disconnect(self) -> None:
        self._disposed = True
        await self._cleanup()

    @property
    def state(self) -> ClientState:
        return self._state

    @property
    def connect_id(self) -> str | None:
        return self._connect_id

    # ── internal ────────────────────────────

    async def _set_state(self, new: ClientState) -> None:
        if self._state is new:
            return
        self._state = new
        if self.on_state_change:
            try:
                await self.on_state_change(new)
            except Exception:
                pass

    async def _do_connect(self, *, is_reconnect: bool = False) -> None:
        if self._disposed:
            return

        async with self._connection_lock:
            await self._set_state(
                ClientState.RECONNECTING if is_reconnect else ClientState.CONNECTING
            )
            try:
                # Use websockets to connect
                self._ws = await websockets.connect(
                    self.gateway_url,
                    max_size=2**24,
                    ping_interval=None,  # we handle heartbeat ourselves
                )
            except Exception as exc:
                _connect_err = str(exc)
            else:
                _connect_err = None

        # ── Handle connection error OUTSIDE the lock ──
        # Calling _on_ws_error inside the lock would trigger _schedule_reconnect
        # → _do_connect again, causing an asyncio.Lock reentry deadlock.
        if _connect_err is not None:
            await self._on_ws_error(_connect_err)
            return

        # Send auth-bind (still protected by CONNECTING state)
        await self._set_state(ClientState.AUTHENTICATING)
        await self._send_auth_bind()

        # Start receive loop
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def _send_auth_bind(self) -> None:
        auth = self.auth
        msg_id = _generate_msg_id()

        encoded = codec.encode_auth_bind_req(
            biz_id=auth.get("bizId", "ybBot"),
            uid=auth.get("uid", ""),
            source=auth.get("source", "bot"),
            token=auth.get("token", ""),
            app_version="0.0.0",   # placeholder; real version injected by plugin
            os_name=platform.system(),
            bot_version="0.0.0",
            instance_id="16",
            route_env=auth.get("routeEnv"),
        )
        head = codec.encode_head(
            cmd=codec.CMD_AUTH_BIND,
            module=codec.MODULE_CONN_ACCESS,
            msg_id=msg_id,
            cmd_type=codec.CMD_TYPE_REQUEST,
            seq_no=codec.next_seq_no(),
        )
        frame = codec.encode_conn_msg(head, encoded)
        if frame is None:
            raise RuntimeError("Failed to encode auth-bind frame")

        await self._send_binary(frame)

    async def _send_ping(self) -> None:
        if not self._heartbeat_ack_received:
            self._heartbeat_timeout_count += 1
            if self._heartbeat_timeout_count >= HEARTBEAT_TIMEOUT_THRESHOLD:
                await self._trigger_reconnect(
                    f"heartbeat timeout ({self._heartbeat_timeout_count} consecutive)"
                )
                return
        else:
            self._heartbeat_timeout_count = 0

        self._heartbeat_ack_received = False
        self._last_heartbeat_at = asyncio.get_running_loop().time()

        msg_id = _generate_msg_id()
        head = codec.encode_head(
            cmd=codec.CMD_PING,
            module=codec.MODULE_CONN_ACCESS,
            msg_id=msg_id,
            cmd_type=codec.CMD_TYPE_REQUEST,
            seq_no=codec.next_seq_no(),
        )
        frame = codec.encode_conn_msg(head, codec.encode_ping_req())
        if frame is None:
            return
        await self._send_binary(frame)

    async def send_biz_frame(self, cmd: str, module: str, biz_data: bytes) -> bool:
        """Encode and send a business request ConnMsg (fire-and-forget)."""
        frame = codec.build_business_conn_msg(cmd, module, biz_data)
        if frame is None:
            return False
        return await self._send_binary(frame)

    async def _send_binary(self, data: bytes) -> bool:
        if self._ws is None:
            return False
        try:
            await self._ws.send(data)
            return True
        except Exception:
            return False

    async def _recv_loop(self) -> None:
        """Continuously receive and dispatch WebSocket messages."""
        assert self._ws is not None
        try:
            async for raw in self._ws:
                try:
                    if isinstance(raw, bytes):
                        await self._on_message(raw)
                    elif isinstance(raw, str):
                        await self._on_message(raw.encode("utf-8"))
                except Exception:
                    import traceback
                    traceback.print_exc()
        except ConnectionClosed as exc:
            code = exc.code or 1000
            reason = exc.reason or ""
            await self._on_close(code, reason)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            try:
                await self._on_close(1006, f"_recv_loop unexpected error: {exc}")
            except Exception:
                pass

    async def _on_message(self, data: bytes) -> None:
        conn_msg = codec.decode_conn_msg(data)
        if conn_msg is None:
            return

        head = conn_msg.get("head", {})
        if not isinstance(head, dict):
            return

        cmd_type: int = head.get("cmdType", 0)

        if cmd_type == codec.CMD_TYPE_RESPONSE:
            await self._on_response(conn_msg)
        elif cmd_type == codec.CMD_TYPE_PUSH:
            await self._on_push(conn_msg)

    async def _on_response(self, conn_msg: dict) -> None:
        head = conn_msg.get("head", {})
        body = conn_msg.get("data", b"")
        if isinstance(body, str):
            body = body.encode("utf-8")
        cmd: str = head.get("cmd", "")

        if cmd == codec.CMD_AUTH_BIND:
            await self._on_auth_bind_response(head, body)
        elif cmd == codec.CMD_PING:
            await self._on_ping_response(head, body)

    async def _on_auth_bind_response(self, head: dict, data: bytes) -> None:
        rsp = codec.decode_auth_bind_rsp(data) or {}
        status: int = head.get("status", 0)
        code: int = rsp.get("code", 0)

        if status != 0 and code != AUTH_ALREADY_CODE:
            if code in AUTH_FAILED_CODES:
                await self._try_auth_failed_refresh(code)
                return
            if code in AUTH_RETRYABLE_CODES:
                await self._close_ws()
                await self._schedule_reconnect()
                return
            await self._close_ws()
            await self._set_state(ClientState.DISCONNECTED)
            if self.on_error:
                await self.on_error(f"Auth-bind failed: status={status}")
            return

        if code != 0 and code != AUTH_ALREADY_CODE:
            if code in AUTH_FAILED_CODES:
                await self._try_auth_failed_refresh(code)
                return
            if code in AUTH_RETRYABLE_CODES:
                await self._close_ws()
                await self._schedule_reconnect()
                return
            await self._close_ws()
            await self._set_state(ClientState.DISCONNECTED)
            if self.on_error:
                await self.on_error(f"Auth-bind response error: code={code}")
            return

        self._connect_id = rsp.get("connectId") or None
        self._reconnect_attempts = 0
        await self._set_state(ClientState.CONNECTED)
        self._start_heartbeat()

        if self.on_ready:
            await self.on_ready({
                "connectId": self._connect_id or "",
                "timestamp": rsp.get("timestamp", 0),
                "clientIp": rsp.get("clientIp", ""),
            })

    async def _on_ping_response(self, head: dict, data: bytes) -> None:
        self._heartbeat_ack_received = True
        self._heartbeat_timeout_count = 0
        rsp = codec.decode_ping_rsp(data) or {}
        interval = rsp.get("heartInterval", 0)
        if isinstance(interval, int) and interval > 1:
            self._heartbeat_interval_s = interval

    async def _on_push(self, conn_msg: dict) -> None:
        head = conn_msg.get("head", {})
        data = conn_msg.get("data", b"")
        if isinstance(data, str):
            data = data.encode("utf-8")

        cmd: str = head.get("cmd", "")

        # Kickout
        if cmd == codec.CMD_KICKOUT:
            ko = codec.decode_kickout_msg(data) or {}
            # Just log & disconnect
            await self._close_ws()
            await self._set_state(ClientState.DISCONNECTED)
            return

        # Try PushMsg
        push = codec.decode_push_msg(data)
        if push and (push.get("cmd") or push.get("module")):
            event: dict = {
                "cmd": push.get("cmd") or head.get("cmd", ""),
                "module": push.get("module") or head.get("module", ""),
                "msgId": push.get("msgId") or head.get("msgId", ""),
                "rawData": push.get("data"),
                "connData": data,
            }
            if self.on_dispatch:
                try:
                    await self.on_dispatch(event)
                except Exception:
                    pass
            return

        # Try DirectedPush
        directed = codec.decode_directed_push(data)
        if directed and (directed.get("type") is not None or directed.get("content")):
            event = {
                "type": directed.get("type"),
                "content": directed.get("content", ""),
                "cmd": head.get("cmd", ""),
                "module": head.get("module", ""),
                "msgId": head.get("msgId", ""),
            }
            if self.on_dispatch:
                try:
                    await self.on_dispatch(event)
                except Exception:
                    pass
            return

        # Unrecognised push — pass raw data
        if self.on_dispatch:
            try:
                await self.on_dispatch({
                    "cmd": head.get("cmd", ""),
                    "module": head.get("module", ""),
                    "msgId": head.get("msgId", ""),
                    "rawData": data,
                })
            except Exception:
                pass

    async def _on_close(self, code: int, reason: str) -> None:
        self._stop_heartbeat()
        if self.on_close:
            try:
                await self.on_close(code, reason)
            except Exception:
                import traceback
                traceback.print_exc()
        if self._disposed:
            return
        if code in NO_RECONNECT_CLOSE_CODES:
            await self._set_state(ClientState.DISCONNECTED)
            if self.on_error:
                await self.on_error(
                    f"Connection closed with non-retryable code={code}: {reason}"
                )
            return
        await self._schedule_reconnect()

    async def _on_ws_error(self, message: str) -> None:
        if self.on_error:
            await self.on_error(message)
        if not self._disposed:
            await self._schedule_reconnect()

    # ── heartbeat ──────────────────────────

    def _start_heartbeat(self) -> None:
        self._stop_heartbeat()
        self._heartbeat_ack_received = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        # First ping after 5s
        await asyncio.sleep(5)
        while True:
            if self._disposed:
                break
            try:
                await self._send_ping()
            except Exception:
                pass
            await asyncio.sleep(self._heartbeat_interval_s - 1)

    # ── reconnect ──────────────────────────

    async def _trigger_reconnect(self, reason: str) -> None:
        self._heartbeat_timeout_count = 0
        await self._close_ws()
        await self._schedule_reconnect()

    async def _schedule_reconnect(self, delay: float | None = None) -> None:
        if self._disposed:
            return
        if self._reconnect_attempts >= self.max_reconnect_attempts:
            await self._set_state(ClientState.DISCONNECTED)
            if self.on_error:
                await self.on_error(
                    f"Max reconnect attempts ({self.max_reconnect_attempts}) exceeded"
                )
            return
        if delay is None:
            idx = min(self._reconnect_attempts, len(self.reconnect_delays) - 1)
            delay = self.reconnect_delays[idx]
        self._reconnect_attempts += 1
        await self._set_state(ClientState.RECONNECTING)
        await asyncio.sleep(delay)
        if not self._disposed:
            await self._do_connect(is_reconnect=True)

    async def _try_auth_failed_refresh(self, error_code: int) -> None:
        """Call on_auth_failed callback to refresh token, then reconnect."""
        if not self.on_auth_failed:
            await self._close_ws()
            await self._set_state(ClientState.DISCONNECTED)
            return
        await self._close_ws()
        try:
            new_auth = await self.on_auth_failed(error_code)
            if new_auth and not self._disposed:
                self.update_auth(new_auth)
                await self._schedule_reconnect()
            else:
                await self._set_state(ClientState.DISCONNECTED)
        except Exception:
            if not self._disposed:
                await self._schedule_reconnect()

    async def _close_ws(self) -> None:
        self._stop_heartbeat()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _cleanup(self) -> None:
        self._stop_heartbeat()
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
        self._recv_task = None
        await self._close_ws()
        await self._set_state(ClientState.DISCONNECTED)
