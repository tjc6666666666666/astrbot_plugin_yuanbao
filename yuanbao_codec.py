"""
Yuanbao ConnMsg / BizMsg Protobuf codec.

Implements minimal protobuf wire-format encoding/decoding based on the
conn.json and biz.json proto descriptors from openclaw-plugin-yuanbao.
This avoids a dependency on the protobuf Python package.

Supported message types:
  Connection layer:
    - Head, ConnMsg, AuthBindReq, AuthBindRsp, PingReq, PingRsp,
      KickoutMsg, PushMsg, DirectedPush
  Business layer:
    - InboundMessagePush, SendC2CMessageReq, SendGroupMessageReq,
      SendC2CMessageRsp, SendGroupMessageRsp, MsgContent,
      MsgBodyElement
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

# ─────────────────────────────────────────────
#  Protobuf wire-format helpers
# ─────────────────────────────────────────────

_WIRE_VARINT = 0
_WIRE_I64 = 1
_WIRE_LEN = 2
_WIRE_I32 = 5


def _encode_varint(value: int) -> bytes:
    """Encode an unsigned integer as a protobuf varint."""
    if value < 0:
        value = value & 0xFFFF_FFFF_FFFF_FFFF  # unsigned 64-bit
    buf = bytearray()
    while value > 0x7F:
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf.append(value & 0x7F)
    return bytes(buf)


def _decode_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Decode a protobuf varint.  Returns (value, new_offset)."""
    result = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        result |= (byte & 0x7F) << shift
        offset += 1
        if not (byte & 0x80):
            break
        shift += 7
    return result, offset


def _tag(field_number: int, wire_type: int) -> int:
    return (field_number << 3) | wire_type


def _field_varint(field_number: int, value: int) -> bytes:
    return _encode_varint(_tag(field_number, _WIRE_VARINT)) + _encode_varint(value)


def _field_string(field_number: int, value: str) -> bytes:
    raw = value.encode("utf-8")
    return (
        _encode_varint(_tag(field_number, _WIRE_LEN))
        + _encode_varint(len(raw))
        + raw
    )


def _field_bytes(field_number: int, data: bytes) -> bytes:
    return (
        _encode_varint(_tag(field_number, _WIRE_LEN))
        + _encode_varint(len(data))
        + data
    )


def _field_bool(field_number: int, value: bool) -> bytes:
    return _encode_varint(_tag(field_number, _WIRE_VARINT)) + (
        b"\x01" if value else b"\x00"
    )


def _field_message(field_number: int, msg: bytes) -> bytes:
    return _field_bytes(field_number, msg)


# ─────────────────────────────────────────────
#  Proto field descriptors (extracted from conn.json / biz.json)
# ─────────────────────────────────────────────

@dataclass
class ProtoField:
    number: int
    name: str
    type: str  # "uint32"|"int32"|"uint64"|"string"|"bool"|"bytes"|"message"
    message_type: str | None = None
    repeated: bool = False

    @property
    def wire_type(self) -> int:
        if self.type in ("uint32", "int32", "uint64", "bool"):
            return _WIRE_VARINT
        if self.type in ("string", "bytes", "message"):
            return _WIRE_LEN
        return _WIRE_LEN  # fallback


# Fields for each message type
FIELDS_HEAD: list[ProtoField] = [
    ProtoField(1,  "cmdType", "uint32"),
    ProtoField(2,  "cmd",     "string"),
    ProtoField(3,  "seqNo",   "uint32"),
    ProtoField(4,  "msgId",   "string"),
    ProtoField(5,  "module",  "string"),
    ProtoField(6,  "needAck", "bool"),
    ProtoField(10, "status",  "int32"),
]

FIELDS_CONN_MSG: list[ProtoField] = [
    ProtoField(1, "head", "message", "Head"),
    ProtoField(2, "data", "bytes"),
]

FIELDS_AUTH_INFO: list[ProtoField] = [
    ProtoField(1, "uid",    "string"),
    ProtoField(2, "source", "string"),
    ProtoField(3, "token",  "string"),
]

FIELDS_DEVICE_INFO: list[ProtoField] = [
    ProtoField(1, "appVersion",       "string"),
    ProtoField(2, "appOperationSystem","string"),
    ProtoField(3, "botVersion",       "string"),
    ProtoField(4, "instanceId",       "string"),
]

FIELDS_AUTH_BIND_REQ: list[ProtoField] = [
    ProtoField(1, "bizId",         "string"),
    ProtoField(2, "authInfo",      "message", "AuthInfo"),
    ProtoField(3, "deviceInfo",    "message", "DeviceInfo"),
    ProtoField(5, "envName",       "string"),
    ProtoField(6, "bindMode",      "uint32"),
    ProtoField(7, "forceToken",    "string"),
]

FIELDS_AUTH_BIND_RSP: list[ProtoField] = [
    ProtoField(1, "code",       "int32"),
    ProtoField(2, "message",    "string"),
    ProtoField(3, "connectId",  "string"),
    ProtoField(4, "timestamp",  "uint64"),
    ProtoField(5, "clientIp",   "string"),
]

FIELDS_PING_RSP: list[ProtoField] = [
    ProtoField(1, "heartInterval", "uint32"),
    ProtoField(2, "timestamp",     "uint64"),
]

FIELDS_KICKOUT_MSG: list[ProtoField] = [
    ProtoField(1, "status",          "int32"),
    ProtoField(2, "reason",          "string"),
    ProtoField(3, "otherDeviceName", "string"),
]

FIELDS_PUSH_MSG: list[ProtoField] = [
    ProtoField(1, "cmd",    "string"),
    ProtoField(2, "module", "string"),
    ProtoField(3, "msgId",  "string"),
    ProtoField(4, "data",   "bytes"),
]

FIELDS_DIRECTED_PUSH: list[ProtoField] = [
    ProtoField(1, "type",    "uint32"),
    ProtoField(2, "content", "string"),
]

# Business-layer fields (simplified)
FIELDS_IM_IMAGE_INFO: list[ProtoField] = [
    ProtoField(1, "type",   "uint32"),
    ProtoField(2, "size",   "uint32"),
    ProtoField(3, "width",  "uint32"),
    ProtoField(4, "height", "uint32"),
    ProtoField(5, "url",    "string"),
]

FIELDS_MSG_CONTENT: list[ProtoField] = [
    ProtoField(1,  "text",           "string"),
    ProtoField(2,  "uuid",           "string"),
    ProtoField(3,  "imageFormat",    "uint32"),
    ProtoField(4,  "data",           "string"),
    ProtoField(5,  "desc",           "string"),
    ProtoField(6,  "ext",            "string"),
    ProtoField(7,  "sound",          "string"),
    ProtoField(8,  "imageInfoArray", "message", "ImImageInfoArray", repeated=True),
    ProtoField(9,  "index",          "uint32"),
    ProtoField(10, "url",            "string"),
    ProtoField(11, "fileSize",       "uint32"),
    ProtoField(12, "fileName",       "string"),
]

# MsgBodyElement fields (from biz.json)
FIELDS_MSG_BODY_ELEMENT: list[ProtoField] = [
    ProtoField(1, "msgType",    "string"),
    ProtoField(2, "msgContent", "message", "MsgContent"),
]

# ImMsgSeq — sequence info for recalled messages
FIELDS_IM_MSG_SEQ: list[ProtoField] = [
    ProtoField(1, "msgSeq",  "uint64"),
    ProtoField(2, "msgId",   "string"),
]

# LogInfoExt — trace context embedded in business messages
FIELDS_LOG_INFO_EXT: list[ProtoField] = [
    ProtoField(1, "traceId", "string"),
]

# InboundMessagePush — the primary inbound message push proto
FIELDS_INBOUND_MSG_PUSH: list[ProtoField] = [
    ProtoField(1,  "callbackCommand",      "string"),
    ProtoField(2,  "fromAccount",          "string"),
    ProtoField(3,  "toAccount",            "string"),
    ProtoField(4,  "senderNickname",       "string"),
    ProtoField(5,  "groupId",              "string"),
    ProtoField(6,  "groupCode",            "string"),
    ProtoField(7,  "groupName",            "string"),
    ProtoField(8,  "msgSeq",               "uint32"),
    ProtoField(9,  "msgRandom",            "uint32"),
    ProtoField(10, "msgTime",              "uint32"),
    ProtoField(11, "msgKey",               "string"),
    ProtoField(12, "msgId",                "string"),
    ProtoField(13, "msgBody",              "message", "MsgBodyElement", repeated=True),
    ProtoField(14, "cloudCustomData",      "string"),
    ProtoField(15, "eventTime",            "uint32"),
    ProtoField(16, "botOwnerId",           "string"),
    ProtoField(17, "recallMsgSeqList",     "message", "ImMsgSeq",      repeated=True),
    ProtoField(18, "clawMsgType",          "uint32"),   # EnumCLawMsgType as varint
    ProtoField(19, "privateFromGroupCode", "string"),
    ProtoField(20, "logExt",              "message", "LogInfoExt"),
]


# ─────────────────────────────────────────────
#  Message builders (encoding)
# ─────────────────────────────────────────────

def _encode_submsg(fields: list[ProtoField], values: dict) -> bytes:
    """Encode a protobuf sub-message from field descriptors and a value dict.

    Repeated message fields (lists of dicts) are encoded as repeated
    length-delimited entries with the same field number.
    """
    buf = bytearray()
    for f in fields:
        key = f.name
        if key not in values or values[key] is None:
            continue
        v = values[key]

        # ── repeated message field (list of dicts) ──
        if isinstance(v, list) and f.type == "message":
            sub_fields = _get_fields(f.message_type) if f.message_type else []
            for item in v:
                if isinstance(item, dict):
                    sub = _encode_submsg(sub_fields, item)
                    buf += _field_message(f.number, sub)
                elif isinstance(item, bytes):
                    buf += _field_message(f.number, item)
            continue

        if f.type in ("uint32", "uint64"):
            buf += _field_varint(f.number, int(v))
        elif f.type == "int32":
            buf += _field_varint(f.number, int(v))
        elif f.type == "bool":
            buf += _field_bool(f.number, bool(v))
        elif f.type == "string":
            buf += _field_string(f.number, str(v))
        elif f.type == "bytes":
            buf += _field_bytes(f.number, v if isinstance(v, bytes) else bytes(v))
        elif f.type == "message":
            # v must be the pre-encoded bytes of the sub-message
            if isinstance(v, dict):
                sub_fields = _get_fields(f.message_type) if f.message_type else []
                sub = _encode_submsg(sub_fields, v)
            else:
                sub = v if isinstance(v, bytes) else bytes(v)
            buf += _field_message(f.number, sub)
    return bytes(buf)


def _get_fields(name: str) -> list[ProtoField]:
    _map = {
        "Head":              FIELDS_HEAD,
        "ConnMsg":           FIELDS_CONN_MSG,
        "AuthInfo":          FIELDS_AUTH_INFO,
        "DeviceInfo":        FIELDS_DEVICE_INFO,
        "AuthBindReq":       FIELDS_AUTH_BIND_REQ,
        "AuthBindRsp":       FIELDS_AUTH_BIND_RSP,
        "PingReq":           [],
        "PingRsp":           FIELDS_PING_RSP,
        "KickoutMsg":        FIELDS_KICKOUT_MSG,
        "PushMsg":           FIELDS_PUSH_MSG,
        "DirectedPush":      FIELDS_DIRECTED_PUSH,
        "ImImageInfoArray":  FIELDS_IM_IMAGE_INFO,
        "MsgContent":        FIELDS_MSG_CONTENT,
        "MsgBodyElement":    FIELDS_MSG_BODY_ELEMENT,
        "ImMsgSeq":          FIELDS_IM_MSG_SEQ,
        "LogInfoExt":        FIELDS_LOG_INFO_EXT,
        "InboundMessagePush":FIELDS_INBOUND_MSG_PUSH,
    }
    return _map.get(name, [])


def encode_head(
    cmd: str,
    module: str,
    msg_id: str,
    cmd_type: int = 0,
    seq_no: int = 0,
    need_ack: bool = False,
) -> bytes:
    return _encode_submsg(FIELDS_HEAD, {
        "cmdType": cmd_type,
        "cmd": cmd,
        "seqNo": seq_no,
        "msgId": msg_id,
        "module": module,
        "needAck": need_ack,
    })


def encode_conn_msg(head_data: bytes, body_data: bytes) -> bytes:
    """Encode a full ConnMsg frame."""
    return _encode_submsg(FIELDS_CONN_MSG, {
        "head": head_data,
        "data": body_data,
    })


def encode_auth_bind_req(
    biz_id: str,
    uid: str,
    source: str,
    token: str,
    app_version: str,
    os_name: str,
    bot_version: str,
    instance_id: str = "16",
    route_env: str | None = None,
) -> bytes:
    auth_info = _encode_submsg(FIELDS_AUTH_INFO, {
        "uid": uid, "source": source, "token": token,
    })
    device_info = _encode_submsg(FIELDS_DEVICE_INFO, {
        "appVersion": app_version,
        "appOperationSystem": os_name,
        "botVersion": bot_version,
        "instanceId": instance_id,
    })
    vals: dict = {
        "bizId": biz_id,
        "authInfo": auth_info,
        "deviceInfo": device_info,
    }
    if route_env:
        vals["envName"] = route_env
    return _encode_submsg(FIELDS_AUTH_BIND_REQ, vals)


def encode_ping_req() -> bytes:
    return b""


# ─────────────────────────────────────────────
#  Message decoders
# ─────────────────────────────────────────────

def _decode_msg(
    fields: list[ProtoField], data: bytes, offset: int = 0
) -> tuple[dict, int]:
    """Decode a protobuf message.  Returns (decoded_dict, new_offset).

    Repeated fields (``ProtoField.repeated == True``) are accumulated into
    lists.  Other fields are assigned as scalar values.
    """

    def _assign(result: dict, fname: str, value: Any, f: ProtoField | None) -> None:
        """Assign a value to a field, accumulating into a list for repeated fields."""
        if f and f.repeated:
            if fname in result:
                result[fname].append(value)
            else:
                result[fname] = [value]
        else:
            result[fname] = value

    result: dict[str, Any] = {}
    field_by_number = {f.number: f for f in fields}
    end = len(data)

    while offset < end:
        tag, offset = _decode_varint(data, offset)
        if tag == 0:
            continue
        field_number = tag >> 3
        wire_type = tag & 0x07
        f = field_by_number.get(field_number)
        fname = f.name if f else f"field_{field_number}"
        ftype = f.type if f else "bytes"

        if wire_type == _WIRE_VARINT:
            value, offset = _decode_varint(data, offset)
            if ftype == "bool":
                value = bool(value)
            _assign(result, fname, value, f)
        elif wire_type == _WIRE_LEN:
            length, offset = _decode_varint(data, offset)
            chunk = data[offset : offset + length]
            offset += length
            if ftype == "string":
                _assign(result, fname, chunk.decode("utf-8", errors="replace"), f)
            elif ftype == "bytes":
                _assign(result, fname, chunk, f)
            elif ftype == "message":
                sub_fields = _get_fields(f.message_type) if f and f.message_type else []
                if sub_fields:
                    sub_result, _ = _decode_msg(sub_fields, chunk)
                    _assign(result, fname, sub_result, f)
                else:
                    _assign(result, fname, chunk, f)
            else:
                _assign(result, fname, chunk, f)
        elif wire_type == _WIRE_I64:
            _assign(
                result,
                fname,
                int.from_bytes(data[offset : offset + 8], "little", signed=True),
                f,
            )
            offset += 8
        elif wire_type == _WIRE_I32:
            _assign(
                result,
                fname,
                int.from_bytes(data[offset : offset + 4], "little", signed=True),
                f,
            )
            offset += 4
        else:
            # Skip unknown
            break

    return result, offset


def decode_conn_msg(data: bytes) -> dict | None:
    """Decode a full ConnMsg binary frame."""
    try:
        conn, _ = _decode_msg(FIELDS_CONN_MSG, data)
        return conn
    except Exception:
        return None


def decode_auth_bind_rsp(data: bytes) -> dict | None:
    try:
        rsp, _ = _decode_msg(FIELDS_AUTH_BIND_RSP, data)
        return rsp
    except Exception:
        return None


def decode_ping_rsp(data: bytes) -> dict | None:
    try:
        rsp, _ = _decode_msg(FIELDS_PING_RSP, data)
        return rsp
    except Exception:
        return None


def decode_kickout_msg(data: bytes) -> dict | None:
    try:
        rsp, _ = _decode_msg(FIELDS_KICKOUT_MSG, data)
        return rsp
    except Exception:
        return None


def decode_push_msg(data: bytes) -> dict | None:
    try:
        msg, _ = _decode_msg(FIELDS_PUSH_MSG, data)
        return msg
    except Exception:
        return None


def decode_directed_push(data: bytes) -> dict | None:
    try:
        msg, _ = _decode_msg(FIELDS_DIRECTED_PUSH, data)
        return msg
    except Exception:
        return None


# ── InboundMessagePush protobuf decoder ──────────────────────

def _from_proto_msg_content(mc: dict) -> dict:
    """Convert proto-decoded MsgContent (camelCase keys) → snake_case."""
    if not mc:
        return {}
    _map = {
        "imageFormat":     "image_format",
        "imageInfoArray":  "image_info_array",
        "fileName":        "file_name",
        "fileSize":        "file_size",
    }
    out: dict[str, Any] = {}
    for k, v in mc.items():
        out[_map.get(k, k)] = v
    return out


def _decode_proto_msg_body_elements(msg_body_raw: list) -> list[dict]:
    """Convert proto-decoded msgBody elements to the snake_case format expected
    by extract_text_from_msg_body / extract_media_from_msg_body / etc.

    Proto-decoded MsgBodyElement shape:
        { "msgType": "TIMTextElem", "msgContent": { "text": "...", "imageFormat": 0, ... }}

    Output shape:
        { "msg_type": "TIMTextElem", "msg_content": { "text": "...", "image_format": 0, ... }}
    """
    result: list[dict] = []
    for el in msg_body_raw:
        if not isinstance(el, dict):
            continue
        mc = el.get("msgContent")
        if isinstance(mc, dict):
            mc = _from_proto_msg_content(mc)
        result.append({
            "msg_type": el.get("msgType", ""),
            "msg_content": mc if isinstance(mc, dict) else {},
        })
    return result


def decode_inbound_message(data: bytes) -> dict | None:
    """Decode an InboundMessagePush protobuf message from raw bytes.

    Returns a dict with the same shape as the JS ``decodeInboundMessage``
    output, or ``None`` on decode failure.

    Top-level keys are kept in camelCase (matching the extra dict usage in
    ``_convert_push_to_message``).  The ``msg_body`` list is converted to
    snake_case ``{msg_type, msg_content}`` format.
    """
    try:
        decoded, _ = _decode_msg(FIELDS_INBOUND_MSG_PUSH, data)
    except Exception:
        return None
    if not isinstance(decoded, dict):
        return None
    if not (decoded.get("callbackCommand") or decoded.get("fromAccount")
            or decoded.get("msgBody") or decoded.get("msgId")):
        return None

    msg_body_raw = decoded.get("msgBody")
    msg_body = _decode_proto_msg_body_elements(
        msg_body_raw if isinstance(msg_body_raw, list) else []
    ) if msg_body_raw else None

    return {
        "callback_command":      decoded.get("callbackCommand"),
        "from_account":          decoded.get("fromAccount"),
        "to_account":            decoded.get("toAccount"),
        "sender_nickname":       decoded.get("senderNickname"),
        "group_id":              decoded.get("groupId"),
        "group_code":            decoded.get("groupCode"),
        "group_name":            decoded.get("groupName"),
        "msg_seq":               decoded.get("msgSeq"),
        "msg_random":            decoded.get("msgRandom"),
        "msg_time":              decoded.get("msgTime"),
        "msg_key":               decoded.get("msgKey"),
        "msg_id":                decoded.get("msgId"),
        "msg_body":              msg_body,
        "cloud_custom_data":     decoded.get("cloudCustomData"),
        "event_time":            decoded.get("eventTime"),
        "bot_owner_id":          decoded.get("botOwnerId"),
        "recall_msg_seq_list":   _clean_recall_list(decoded.get("recallMsgSeqList")),
        "claw_msg_type":         decoded.get("clawMsgType"),
        "private_from_group_code": decoded.get("privateFromGroupCode"),
        "trace_id":              _extract_trace_id(decoded.get("logExt")),
    }


def _clean_recall_list(raw: Any) -> list[dict] | None:
    """Normalise recallMsgSeqList to a list of dicts or None."""
    if not raw or not isinstance(raw, list):
        return None
    cleaned = []
    for item in raw:
        if isinstance(item, dict):
            cleaned.append(item)
    return cleaned if cleaned else None


def _extract_trace_id(log_ext: Any) -> str | None:
    """Extract traceId from logExt sub-message."""
    if isinstance(log_ext, dict):
        tid = log_ext.get("traceId")
        if isinstance(tid, str) and tid.strip():
            return tid.strip()
    return None


# ─────────────────────────────────────────────
#  JSON helper for msg_body/content fallbacks
# ─────────────────────────────────────────────

def parse_push_content_to_msg_body(content: str) -> list[dict] | None:
    """Try parsing push content string into a msg_body list."""
    if not content or not content.strip():
        return None
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            if "msg_body" in parsed and isinstance(parsed["msg_body"], list):
                return parsed["msg_body"]
            if "text" in parsed:
                return [{"msg_type": "TIMTextElem", "msg_content": {"text": parsed["text"]}}]
    except (json.JSONDecodeError, TypeError):
        pass
    # Plain text
    return [{"msg_type": "TIMTextElem", "msg_content": {"text": content}}]


def sanitize_msg_body_elements(elements: list[dict]) -> list[dict]:
    """Ensure each element has msg_type and msg_content keys."""
    result = []
    for el in elements:
        if not isinstance(el, dict):
            continue
        e = dict(el)
        if "msg_type" not in e:
            e["msg_type"] = "TIMTextElem"
        if "msg_content" not in e:
            e["msg_content"] = {}
        result.append(e)
    return result


def extract_text_from_msg_body(msg_body: list[dict]) -> str:
    """Extract plain text from msg_body elements.

    Skips TIMCustomElem @mention elements (elem_type=1002) — those are
    handled separately by extract_mentions_from_msg_body().
    """
    parts = []
    for el in msg_body or []:
        msg_type = el.get("msg_type", "")
        mc = el.get("msg_content", {})
        # Skip custom @mention elements — they are processed as At components
        if msg_type == "TIMCustomElem":
            data_str = mc.get("data", "")
            if isinstance(data_str, str):
                try:
                    custom = json.loads(data_str)
                    if custom.get("elem_type") == 1002:
                        # Don't include @text in the plain text extraction
                        continue
                except (json.JSONDecodeError, TypeError):
                    pass
        t = mc.get("text", "") or mc.get("data", "")
        if t:
            parts.append(str(t))
    return "".join(parts)


def extract_mentions_from_msg_body(msg_body: list[dict]) -> list[dict]:
    """Extract @mention elements (TIMCustomElem elem_type=1002) from msg_body.

    Mirrors the openclaw-plugin-yuanbao customHandler logic:
    TIMCustomElem with elem_type=1002 encodes an @mention, where
    ``user_id`` is the mentioned user and ``text`` is the display text
    (e.g. \"@张三\").

    Returns a list of mention dicts:
        { "user_id": str, "text": str }
    """
    mentions = []
    for el in msg_body or []:
        if el.get("msg_type") != "TIMCustomElem":
            continue
        mc = el.get("msg_content", {}) or {}
        data_str = mc.get("data", "")
        if not data_str or not isinstance(data_str, str):
            continue
        try:
            custom = json.loads(data_str)
        except (json.JSONDecodeError, TypeError):
            continue
        if custom.get("elem_type") != 1002:
            continue
        user_id = custom.get("user_id")
        if user_id:
            mentions.append({
                "user_id": str(user_id),
                "text": custom.get("text") or f"@{user_id}",
            })
    return mentions


def is_bot_mentioned(msg_body: list[dict], bot_id: str) -> bool:
    """Check whether the bot is @mentioned in a msg_body list."""
    for m in extract_mentions_from_msg_body(msg_body):
        if str(m["user_id"]) == str(bot_id):
            return True
    return False


def extract_media_from_msg_body(msg_body: list[dict]) -> list[dict]:
    """Extract media elements (Image, File, Record, Video) from msg_body.

    Returns a list of media item dicts:
        { "type": "image"|"file"|"record"|"video",
          "url": str | None,
          "file_name": str | None,
          "msg_content": dict (raw msg_content) }

    Text elements are skipped.
    """
    media_items: list[dict] = []
    for el in msg_body or []:
        msg_type = el.get("msg_type", "")
        mc = el.get("msg_content", {}) or {}

        if msg_type == "TIMImageElem":
            # image_info_array may have multiple sizes; prefer index 1 (medium), fallback to 0
            info_arr = mc.get("image_info_array") or mc.get("imageInfoArray") or []
            img_info = (info_arr[1] if len(info_arr) > 1 else None) or (
                info_arr[0] if len(info_arr) > 0 else None
            )
            url = img_info.get("url") if isinstance(img_info, dict) else None
            media_items.append({
                "type": "image",
                "url": url,
                "file_name": mc.get("uuid") or mc.get("file_name") or "image",
                "msg_content": mc,
            })

        elif msg_type == "TIMFileElem":
            url = mc.get("url") or ""
            file_name = mc.get("file_name") or mc.get("fileName") or "file"
            media_items.append({
                "type": "file",
                "url": url,
                "file_name": file_name,
                "msg_content": mc,
            })

        elif msg_type == "TIMSoundElem":
            url = mc.get("sound") or mc.get("url") or ""
            media_items.append({
                "type": "record",
                "url": url,
                "file_name": mc.get("file_name") or mc.get("fileName") or "audio",
                "msg_content": mc,
            })

        elif msg_type == "TIMVideoFileElem":
            url = mc.get("data") or mc.get("url") or ""
            media_items.append({
                "type": "video",
                "url": url,
                "file_name": mc.get("file_name") or mc.get("fileName") or "video",
                "msg_content": mc,
            })

    return media_items


# ─────────────────────────────────────────────
#  Sequence number generator
# ─────────────────────────────────────────────

_seq_counter = 0
_MAX_SAFE = 2**53 - 1


def next_seq_no() -> int:
    global _seq_counter
    n = _seq_counter
    _seq_counter = (_seq_counter + 1) % _MAX_SAFE
    return n


# ─────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────

CMD_TYPE_REQUEST = 0
CMD_TYPE_RESPONSE = 1
CMD_TYPE_PUSH = 2
CMD_TYPE_PUSH_ACK = 3

CMD_AUTH_BIND = "auth-bind"
CMD_PING = "ping"
CMD_KICKOUT = "kickout"
CMD_UPDATE_META = "update-meta"

MODULE_CONN_ACCESS = "conn_access"

# ─────────────────────────────────────────────
#  Business-layer encoding
# ─────────────────────────────────────────────

# Business command constants (mirror openclaw-plugin-yuanbao BIZ_CMD)
BIZ_CMD_SEND_C2C = "send_c2c_message"
BIZ_CMD_SEND_GROUP = "send_group_message"
BIZ_MODULE = "yuanbao_openclaw_proxy"

# SendC2CMessageReq fields (verified against biz.json)
FIELDS_SEND_C2C_REQ: list[ProtoField] = [
    ProtoField(1, "msgId",       "string"),
    ProtoField(2, "toAccount",   "string"),
    ProtoField(3, "fromAccount", "string"),
    ProtoField(4, "msgRandom",   "uint32"),
    # msgBody  is field 5 (repeated) — encoded manually
    ProtoField(6, "groupCode",   "string"),
    ProtoField(7, "msgSeq",      "uint64"),
]

# SendGroupMessageReq fields (verified against biz.json)
FIELDS_SEND_GROUP_REQ: list[ProtoField] = [
    ProtoField(1, "msgId",       "string"),
    ProtoField(2, "groupCode",   "string"),
    ProtoField(3, "fromAccount", "string"),
    ProtoField(4, "toAccount",   "string"),
    ProtoField(5, "random",      "string"),
    # msgBody  is field 6 (repeated) — encoded manually
    ProtoField(7, "refMsgId",    "string"),
    ProtoField(8, "msgSeq",      "uint64"),
]


def _to_proto_msg_content(snake: dict) -> dict:
    """Map snake_case msg_content keys → camelCase proto field names.

    Mirrors the original openclaw-plugin-yuanbao ``toProtoMsgBody`` logic.
    Keys NOT listed here are passed through unchanged.
    """
    _map = {
        "image_format":     "imageFormat",
        "image_info_array": "imageInfoArray",
        "file_name":        "fileName",
        "file_size":        "fileSize",
    }
    out = {}
    for k, v in snake.items():
        out[_map.get(k, k)] = v
    return out


def encode_msg_content(values: dict) -> bytes:
    """Encode a single MsgContent sub-message."""
    return _encode_submsg(FIELDS_MSG_CONTENT, _to_proto_msg_content(values))


def encode_msg_body_element(msg_type: str, msg_content: dict) -> bytes:
    """Encode a single MsgBodyElement."""
    content_bytes = encode_msg_content(msg_content)
    return _encode_submsg(FIELDS_MSG_BODY_ELEMENT, {
        "msgType": msg_type,
        "msgContent": content_bytes,
    })


def encode_repeated_msg_body(elements: list[dict]) -> bytes:
    """Encode a list of msg_body elements as repeated MsgBodyElement fields.

    The ``msgBody`` field in SendC2CMessageReq is field #5 (repeated).
    Each element is encoded as its own length-delimited field.
    """
    return b"".join(
        _field_message(5, encode_msg_body_element(el["msg_type"], el.get("msg_content", {})))
        for el in elements
    )


def encode_repeated_msg_body_group(elements: list[dict]) -> bytes:
    """Like encode_repeated_msg_body but uses field #6 for group messages."""
    return b"".join(
        _field_message(6, encode_msg_body_element(el["msg_type"], el.get("msg_content", {})))
        for el in elements
    )


def encode_send_c2c_message_req(
    to_account: str,
    msg_body: list[dict],
    from_account: str = "",
    group_code: str = "",
    msg_random: int = 0,
    msg_seq: int = 0,
) -> bytes:
    """Encode a SendC2CMessageReq protobuf message."""
    vals: dict[str, Any] = {
        "msgId": uuid.uuid4().hex,
        "toAccount": to_account,
        "fromAccount": from_account,
        "msgRandom": msg_random,
    }
    if group_code:
        vals["groupCode"] = group_code
    if msg_seq:
        vals["msgSeq"] = msg_seq

    base = _encode_submsg(FIELDS_SEND_C2C_REQ, vals)
    body_bytes = encode_repeated_msg_body(msg_body)
    return base + body_bytes


def encode_send_group_message_req(
    group_code: str,
    msg_body: list[dict],
    from_account: str = "",
    to_account: str = "",
    msg_random: int = 0,
    ref_msg_id: str = "",
    msg_seq: int = 0,
) -> bytes:
    """Encode a SendGroupMessageReq protobuf message."""
    vals: dict[str, Any] = {
        "msgId": uuid.uuid4().hex,
        "groupCode": group_code,
        "fromAccount": from_account,
        "random": str(msg_random),
    }
    if to_account:
        vals["toAccount"] = to_account
    if ref_msg_id:
        vals["refMsgId"] = ref_msg_id
    if msg_seq:
        vals["msgSeq"] = msg_seq

    base = _encode_submsg(FIELDS_SEND_GROUP_REQ, vals)
    body_bytes = encode_repeated_msg_body_group(msg_body)
    return base + body_bytes


def build_business_conn_msg(
    cmd: str,
    module: str,
    biz_data: bytes,
    msg_id: str | None = None,
) -> bytes | None:
    """Encode a full business ConnMsg (head + body)."""
    mid = msg_id or uuid.uuid4().hex
    head_bytes = encode_head(
        cmd=cmd,
        module=module,
        msg_id=mid,
        cmd_type=CMD_TYPE_REQUEST,
        seq_no=next_seq_no(),
    )
    return encode_conn_msg(head_bytes, biz_data)
