from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from django.conf import settings
from django.core.cache import cache


DEFAULT_CONVERSATION_TTL_SECONDS = 24 * 60 * 60
DEFAULT_MAX_TURNS = 6
CACHE_KEY_PREFIX = "energy-agent-conversation"


def _cache_key(conversation_id: str) -> str:
    return f"{CACHE_KEY_PREFIX}:{conversation_id}"


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ttl() -> int:
    return int(
        getattr(
            settings,
            "CLAUDE_CHAT_CONVERSATION_TTL",
            DEFAULT_CONVERSATION_TTL_SECONDS,
        )
    )


def _max_turns() -> int:
    return max(1, int(getattr(settings, "CLAUDE_CHAT_MAX_STORED_TURNS", DEFAULT_MAX_TURNS)))


def generate_conversation_id() -> str:
    return str(uuid.uuid4())


def _is_user_text_message(msg: dict) -> bool:
    if not isinstance(msg, dict) or msg.get("role") != "user":
        return False
    content = msg.get("content")
    if isinstance(content, str):
        return True
    if isinstance(content, list):
        return not any(
            isinstance(block, dict) and block.get("type") == "tool_result"
            for block in content
        )
    return False


def _trim_to_recent_turns(messages: list[dict], max_turns: int) -> list[dict]:
    turn_starts = [index for index, message in enumerate(messages) if _is_user_text_message(message)]
    if len(turn_starts) <= max_turns:
        return messages
    return messages[turn_starts[-max_turns]:]


def _sanitize(messages: Any) -> list[dict]:
    if not isinstance(messages, list):
        return []

    clean: list[dict] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        content = message.get("content")
        if role not in {"user", "assistant"}:
            continue
        if not (isinstance(content, str) and content.strip()) and not (
            isinstance(content, list) and content
        ):
            continue
        clean.append({"role": role, "content": content})

    while clean and clean[0]["role"] != "user":
        clean.pop(0)

    while clean and clean[-1]["role"] == "assistant" and any(
        isinstance(block, dict) and block.get("type") == "tool_use"
        for block in (clean[-1]["content"] if isinstance(clean[-1]["content"], list) else [])
    ):
        clean.pop()

    return clean


def load_history(conversation_id: str | None) -> list[dict]:
    if not conversation_id:
        return []
    cached = cache.get(_cache_key(conversation_id))
    if not isinstance(cached, dict):
        return []
    return _sanitize(cached.get("messages"))


def append_turn(conversation_id: str, new_messages: list[dict]) -> None:
    history = load_history(conversation_id)
    combined = _sanitize(history + list(new_messages or []))
    combined = _trim_to_recent_turns(combined, _max_turns())
    cache.set(
        _cache_key(conversation_id),
        {
            "conversation_id": conversation_id,
            "messages": combined,
            "updated_at": _utc_now_iso(),
        },
        timeout=_ttl(),
    )
