from __future__ import annotations

from typing import Any

from .conversation import append_turn, generate_conversation_id, load_history


def load_chart_conversation(conversation_id: str | None) -> dict[str, Any] | None:
    messages = load_history(conversation_id)
    if not messages:
        return None
    return {
        "conversation_id": conversation_id,
        "messages": messages,
    }


def append_chart_conversation_turn(
    conversation_id: str,
    *,
    user_message: str,
    assistant_message: str,
    status: str,
    previous_query: dict[str, Any] | None,
    pending_clarification: dict[str, Any] | None,
) -> None:
    del status, previous_query, pending_clarification
    append_turn(
        conversation_id,
        [
            {"role": "user", "content": user_message},
            {"role": "assistant", "content": assistant_message},
        ],
    )


def conversation_messages_for_model(conversation: dict[str, Any] | None) -> list[dict[str, str]]:
    if not isinstance(conversation, dict):
        return []
    prepared_messages: list[dict[str, str]] = []
    for item in conversation.get("messages", []):
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip().lower()
        content = item.get("content")
        if role not in {"user", "assistant"} or not isinstance(content, str) or not content.strip():
            continue
        prepared_messages.append({"role": role, "content": content.strip()})
    return prepared_messages
