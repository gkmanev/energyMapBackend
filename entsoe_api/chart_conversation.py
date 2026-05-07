from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from django.conf import settings
from django.core.cache import cache


DEFAULT_CONVERSATION_TTL_SECONDS = 24 * 60 * 60
DEFAULT_MAX_MESSAGES = 12
DEFAULT_CONTEXT_MESSAGES = 6
CACHE_KEY_PREFIX = "chart-query-conversation"


def _cache_key(conversation_id: str) -> str:
    return f"{CACHE_KEY_PREFIX}:{conversation_id}"


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _conversation_ttl_seconds() -> int:
    return int(getattr(settings, "CLAUDE_CHAT_CONVERSATION_TTL", DEFAULT_CONVERSATION_TTL_SECONDS))


def _max_messages() -> int:
    return int(getattr(settings, "CLAUDE_CHAT_MAX_STORED_MESSAGES", DEFAULT_MAX_MESSAGES))


def _context_messages() -> int:
    return int(getattr(settings, "CLAUDE_CHAT_MAX_CONTEXT_MESSAGES", DEFAULT_CONTEXT_MESSAGES))


def generate_conversation_id() -> str:
    return str(uuid.uuid4())


def load_chart_conversation(conversation_id: str | None) -> dict[str, Any] | None:
    if not conversation_id:
        return None
    cached = cache.get(_cache_key(conversation_id))
    if not isinstance(cached, dict):
        return None
    messages = cached.get("messages")
    if not isinstance(messages, list):
        cached["messages"] = []
    return cached


def _trim_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    max_messages = max(1, _max_messages())
    return messages[-max_messages:]


def save_chart_conversation(
    conversation_id: str,
    *,
    messages: list[dict[str, Any]],
    previous_query: dict[str, Any] | None,
    pending_clarification: dict[str, Any] | None,
) -> None:
    payload = {
        "conversation_id": conversation_id,
        "messages": _trim_messages(messages),
        "previous_query": previous_query,
        "pending_clarification": pending_clarification,
        "updated_at": _utc_now_iso(),
    }
    cache.set(_cache_key(conversation_id), payload, timeout=_conversation_ttl_seconds())


def append_chart_conversation_turn(
    conversation_id: str,
    *,
    user_message: str,
    assistant_message: str,
    status: str,
    previous_query: dict[str, Any] | None,
    pending_clarification: dict[str, Any] | None,
) -> None:
    conversation = load_chart_conversation(conversation_id) or {"messages": []}
    messages = list(conversation.get("messages", []))
    timestamp = _utc_now_iso()
    messages.append({"role": "user", "content": user_message, "timestamp": timestamp})
    messages.append(
        {
            "role": "assistant",
            "content": assistant_message,
            "timestamp": timestamp,
            "status": status,
        }
    )
    save_chart_conversation(
        conversation_id,
        messages=messages,
        previous_query=previous_query,
        pending_clarification=pending_clarification,
    )


def conversation_messages_for_model(conversation: dict[str, Any] | None) -> list[dict[str, str]]:
    if not isinstance(conversation, dict):
        return []
    raw_messages = conversation.get("messages")
    if not isinstance(raw_messages, list):
        return []
    recent_messages = raw_messages[-max(1, _context_messages()):]
    prepared_messages: list[dict[str, str]] = []
    for item in recent_messages:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = str(item.get("content", "")).strip()
        if not content:
            continue
        prepared_messages.append({"role": role, "content": content})
    return prepared_messages
