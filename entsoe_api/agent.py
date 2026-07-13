from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

import anthropic
from django.conf import settings

from .tools import TOOL_SCHEMAS, execute_tool


MAX_AGENT_TURNS = 8


@dataclass(frozen=True)
class AgentResult:
    text: str
    charts: list[dict] = field(default_factory=list)
    new_messages: list[dict] = field(default_factory=list)
    stop_reason: str = "end_turn"

    @property
    def status(self) -> str:
        return "chart" if self.charts else "text"


def _supported_country_codes() -> set[str]:
    codes: set[str] = set()
    for name in ("ENTSOE_COUNTRY_TO_EICS", "ENTSOE_PRICE_COUNTRY_TO_EICS"):
        mapping = getattr(settings, name, None) or {}
        if isinstance(mapping, dict):
            codes.update(str(code).strip().upper() for code in mapping if len(str(code).strip()) == 2)
    return {code for code in codes if code.isalpha()}


def _build_system_prompt(now_utc: dt.datetime) -> str:
    now = now_utc.astimezone(dt.timezone.utc)
    supported = ", ".join(sorted(_supported_country_codes())) or "European ISO-2 codes"
    return f"""You are the assistant for visualize.energy, a European electricity market data platform.

Current UTC datetime: {now.strftime("%Y-%m-%dT%H:%M:%SZ")} ({now.strftime("%A")}).
Resolve all relative timeframes ("today", "yesterday", "last 7 days", "April") against this
datetime and pass explicit UTC ISO ranges to tools. "Today" = {now.strftime("%Y-%m-%d")}T00:00:00Z
to tomorrow 00:00:00Z. A bare month name without a year means the most recent occurrence.

Datasets (ENTSO-E): A69 RES generation (solar/wind), A75 generation mix, A44 day-ahead prices,
A68 installed capacity, A11 cross-border flows.
Supported countries: {supported}.

Behavior:
- When the user wants to SEE data (show, plot, chart, compare over time), call render_chart.
  Do not fetch raw rows first unless you also need to answer an analytical question.
- When the user asks for a statistic (average, max, total, "how many days..."), fetch the data
  with the appropriate get_* tool at a sensible resolution, then answer with specific numbers.
- If a required detail is missing (metric, country, or timeframe) and cannot be inferred from
  the conversation, ask ONE concise clarifying question instead of guessing.
- Reuse the metric, countries, timeframe, and resolution from earlier in the conversation for
  follow-ups ("make it a bar chart", "same for FR", "and prices too") unless the user changes them.
- For windows longer than ~28 days prefer daily resolution.
- If a tool returns an error, fix the input and retry, or tell the user what's unsupported.
- Answer general questions (what data exists, what RES means, supported countries) directly
  in text without tools.
- Keep replies concise. Never invent numbers not present in tool results."""


def _serialize_content(blocks: list[Any]) -> list[dict]:
    out: list[dict] = []
    for block in blocks:
        if block.type == "text":
            out.append({"type": "text", "text": block.text})
        elif block.type == "tool_use":
            out.append(
                {
                    "type": "tool_use",
                    "id": block.id,
                    "name": block.name,
                    "input": block.input,
                }
            )
    return out


def _extract_text(content: list[dict]) -> str:
    return "\n".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    ).strip()


def run_energy_agent(
    message: str,
    *,
    history: list[dict] | None = None,
    now_utc: dt.datetime | None = None,
) -> AgentResult:
    user_message = (message or "").strip()
    if not user_message:
        raise ValueError("message is required.")

    api_key = getattr(settings, "ANTHROPIC_API_KEY", "") or ""
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY is not configured.")

    model = getattr(settings, "CLAUDE_CHAT_MODEL", "claude-sonnet-4-6")
    timeout_seconds = float(getattr(settings, "CLAUDE_CHAT_TIMEOUT", 60))

    now = (now_utc or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    system = _build_system_prompt(now)

    messages: list[dict] = list(history or [])
    messages.append({"role": "user", "content": user_message})
    new_messages: list[dict] = [messages[-1]]

    client = anthropic.Anthropic(api_key=api_key, timeout=timeout_seconds)
    ctx: dict[str, Any] = {"now_utc": now}

    stop_reason = "max_turns"
    last_content: list[dict] = []

    for _ in range(MAX_AGENT_TURNS):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=2048,
                system=system,
                messages=messages,
                tools=TOOL_SCHEMAS,
            )
        except anthropic.APIConnectionError as exc:
            raise ValueError(f"Claude request failed (connection error): {exc}") from exc
        except anthropic.RateLimitError as exc:
            raise ValueError(f"Claude request failed (rate limit): {exc}") from exc
        except anthropic.APIStatusError as exc:
            raise ValueError(f"Claude request failed ({exc.status_code}): {exc.message}") from exc

        last_content = _serialize_content(response.content)
        assistant_message = {"role": "assistant", "content": last_content}
        messages.append(assistant_message)
        new_messages.append(assistant_message)

        if response.stop_reason != "tool_use":
            stop_reason = response.stop_reason or "end_turn"
            break

        tool_results: list[dict] = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": execute_tool(block.name, block.input, ctx),
                }
            )

        result_message = {"role": "user", "content": tool_results}
        messages.append(result_message)
        new_messages.append(result_message)

    text = _extract_text(last_content)
    charts = list(ctx.get("charts", []))

    if not text and charts:
        text = "Here's your chart."
    if not text and not charts:
        text = (
            "I can help you explore European electricity data including RES generation, solar, wind, "
            "day-ahead prices, installed capacity, and cross-border flows. "
            "Try asking: 'Show RES generation for DE for the last 7 days'."
        )

    return AgentResult(
        text=text,
        charts=charts,
        new_messages=new_messages,
        stop_reason=stop_reason,
    )
