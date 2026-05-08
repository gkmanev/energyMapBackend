from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass, field

import anthropic
from django.conf import settings
from django.utils.dateparse import parse_date, parse_datetime


# ──────────────────────────── Tool definition ────────────────────────────────

ANALYZE_QUERY_TOOL: dict = {
    "name": "analyze_energy_query",
    "description": (
        "Analyze a user message about European electricity data and return structured "
        "parameters needed to answer the request — either by querying the database or "
        "responding directly with text."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "intent": {
                "type": "string",
                "enum": ["chart", "data", "text", "needs_clarification"],
                "description": (
                    "chart: user wants a time-series visualization. "
                    "data: user wants statistics or aggregated values (max, avg, total…). "
                    "text: general question answerable without a DB query (capabilities, definitions, country list). "
                    "needs_clarification: request too vague — metric, country, or timeframe is missing."
                ),
            },
            "text_answer": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "description": "Complete direct answer when intent is 'text'.",
            },
            "clarifying_question": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "description": "One concise question when intent is 'needs_clarification'.",
            },
            "missing_fields": {
                "type": "array",
                "items": {"type": "string", "enum": ["metric", "country", "timeframe"]},
                "description": "Fields missing for a complete chart/data query.",
            },
            "data_type": {
                "anyOf": [
                    {
                        "type": "string",
                        "enum": ["generation_res", "generation", "prices", "capacity", "flows"],
                    },
                    {"type": "null"},
                ],
                "description": (
                    "generation_res: RES (solar+wind) actual generation from A69. "
                    "generation: all production types actual generation from A75. "
                    "prices: day-ahead electricity prices EUR/MWh from A44. "
                    "capacity: installed generation capacity snapshots from A68. "
                    "flows: cross-border physical power flows from A11."
                ),
            },
            "country": {
                "type": "string",
                "description": "Primary 2-letter ISO country code (first in the list).",
            },
            "countries": {
                "type": "array",
                "items": {"type": "string"},
                "description": "All requested 2-letter ISO country codes in the order mentioned.",
            },
            "country_from": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "description": "Source country ISO code for flows queries.",
            },
            "country_to": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "description": "Destination country ISO code for flows queries.",
            },
            "resolution": {
                "type": "string",
                "enum": ["native", "d", "m", "y"],
                "description": "Aggregation: native = raw/hourly, d = daily, m = monthly, y = yearly.",
            },
            "generation_series": {
                "type": "array",
                "items": {"type": "string", "enum": ["res", "solar", "wind"]},
                "description": "Generation series to include. res = solar+wind combined.",
            },
            "include_prices": {
                "type": "boolean",
                "description": "Whether to include day-ahead prices alongside generation.",
            },
            "chart_type": {
                "type": "string",
                "enum": ["line", "bar"],
                "description": "Visualization type — line or bar chart.",
            },
            "timeframe": {
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": [
                            "today",
                            "yesterday",
                            "last_n_days",
                            "last_n_weeks",
                            "explicit_utc_range",
                            "unknown",
                        ],
                    },
                    "amount": {
                        "anyOf": [
                            {"type": "integer", "minimum": 1},
                            {"type": "null"},
                        ],
                    },
                    "start_utc": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    "end_utc": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                },
                "required": ["kind", "amount", "start_utc", "end_utc"],
            },
        },
        "required": [
            "intent",
            "text_answer",
            "clarifying_question",
            "missing_fields",
            "data_type",
            "country",
            "countries",
            "country_from",
            "country_to",
            "resolution",
            "generation_series",
            "include_prices",
            "chart_type",
            "timeframe",
        ],
    },
}


# ──────────────────────────── System prompt ──────────────────────────────────

SYSTEM_PROMPT = """You are an energy data assistant for visualize.energy, a European electricity market data platform.

## Available datasets

- **generation_res** – Actual RES (renewable energy sources) generation per country from the ENTSO-E A69 dataset.
  Supported generation_series: "res" (solar + wind combined), "solar" (B16), "wind" (B18/B19).
- **generation** – Actual generation by all production types from the A75 dataset.
- **prices** – Day-ahead electricity prices in EUR/MWh from the A44 dataset.
- **capacity** – Annual installed generation capacity snapshots by production type from the A68 dataset.
- **flows** – Cross-border physical power flows between country pairs from the A11 dataset.

## Supported countries
European countries identified by 2-letter ISO codes:
AT, BE, BG, CH, CZ, DE, DK, EE, ES, FI, FR, GB, GR, HR, HU, IE, IT,
LT, LU, LV, ME, MK, MT, NL, NO, PL, PT, RO, RS, SE, SI, SK, TR, UA, XK, GE and others.

## Intent classification

**chart** – User wants a time-series visualization. Triggers on: "show", "plot", "display", "chart",
  "graph", "compare over time", "how did X change". Do NOT use "chart" when the user asks for a
  single aggregate value. Extract: data_type, countries, timeframe, resolution, generation_series /
  include_prices, chart_type.
**data** – User wants a computed statistic, not a chart. Triggers on keywords like: average, avg,
  mean, max, maximum, min, minimum, highest, lowest, total, sum, how many, count, number of,
  "what was the", "what is the", "how much did", "days with", "hours above/below".
  Also use "data" when user asks about installed capacity (data_type: capacity) — the backend
  will look up the capacity snapshot for the requested country and year.
  The backend will fetch the raw data and pass it back to Claude for analysis, so there is no
  need to specify an aggregate function — just classify as "data" and extract the query parameters.
  - Supported for data_type "prices", "generation_res", and "capacity".
    For data_type "flows" or "generation" use intent "chart" instead.
  - For capacity: extract country and timeframe (year). generation_series and include_prices are unused.
  - Still extract countries, timeframe, generation_series / include_prices for other types.
  - Set resolution to "native" and chart_type to "line" (ignored for data intent).
**text** – User asks a general question you can answer without hitting the database:
  - "What data do you have?" → describe available datasets
  - "Which countries are supported?" → list key European countries
  - "What is RES?" → explain renewable energy sources
  - "How fresh is the data?" → describe update frequency (hourly/daily)
  Set text_answer to a helpful, concise response.
**needs_clarification** – The request is missing metric, country, or timeframe. Ask for the most critical piece only.

## Mapping rules

- "renewables", "RES", "renewable generation/energy" → generation_series: ["res"], data_type: generation_res
- "solar" → generation_series: ["solar"], data_type: generation_res
- "wind" → generation_series: ["wind"], data_type: generation_res
- "prices", "electricity price", "spot price", "day-ahead" → include_prices: true, data_type: prices
- User can request generation AND prices simultaneously → fill generation_series AND set include_prices: true
- "flows", "cross-border", "import from X to Y", "export from X to Y" → data_type: flows, fill country_from and country_to
- "capacity", "installed capacity" → data_type: capacity, intent: data

## Timeframe rules

- "today" → kind: today
- "yesterday" → kind: yesterday
- "last N days" → kind: last_n_days, amount: N
- "last N weeks" → kind: last_n_weeks, amount: N
- Specific date, month name ("April 2025"), or ISO range → kind: explicit_utc_range, fill start_utc and end_utc
- Unknown or missing → kind: unknown → prefer needs_clarification

## Resolution rules

- Not specified and range > 28 days → use "d" (daily)
- Not specified and range ≤ 28 days → use "native"
- "hourly", "raw", "native" → native
- "daily" / "day by day" → d
- "monthly" → m
- "yearly" / "annual" → y

## Missing fields

- generation_series is empty AND include_prices is false AND data_type is null or empty → add "metric" to missing_fields
- If data_type is set to any non-null value ("capacity", "prices", "generation_res", "flows", "generation"), metric is NOT missing
- countries is empty → add "country" to missing_fields
- timeframe kind is "unknown" → add "timeframe" to missing_fields

## Follow-up handling

If the user's message is only a chart-style change (e.g. "make it a bar chart", "switch to line") without prior context,
return intent: needs_clarification with a question asking what data they want to see.

Always call analyze_energy_query with all required fields. Set unused fields to null or empty arrays."""


# ────────────────────────────── Dataclasses ──────────────────────────────────

@dataclass(frozen=True)
class ParsedChartQuery:
    original_message: str
    country: str
    countries: list[str]
    start_utc: dt.datetime
    end_utc: dt.datetime
    resolution: str
    time_phrase: str
    generation_series: list[str] = field(default_factory=list)
    include_prices: bool = False
    chart_type: str = "line"


@dataclass(frozen=True)
class ParsedDataQuery:
    original_message: str
    country: str
    countries: list[str]
    start_utc: dt.datetime
    end_utc: dt.datetime
    time_phrase: str
    data_type: str          # "prices" | "generation_res"
    generation_series: list[str] = field(default_factory=list)
    include_prices: bool = False


@dataclass(frozen=True)
class ChartQueryClarification:
    original_message: str
    question: str
    missing_fields: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ChartQueryAgentResult:
    status: str  # "ready" | "data" | "needs_clarification" | "text"
    query: ParsedChartQuery | ParsedDataQuery | None = None
    clarification: ChartQueryClarification | None = None
    text_answer: str | None = None


class ChartQueryNeedsClarification(ValueError):
    def __init__(self, missing_fields: list[str], message: str = "The request needs clarification."):
        super().__init__(message)
        self.missing_fields = _ordered_unique([f for f in missing_fields if f])


# ─────────────────────────── Utility helpers ─────────────────────────────────

def _ensure_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _parse_utc_value(raw_value: str) -> dt.datetime:
    parsed = parse_datetime(raw_value)
    if parsed is None and raw_value.endswith("Z"):
        parsed = parse_datetime(raw_value.replace("Z", "+00:00"))
    if parsed is None:
        parsed_date = parse_date(raw_value)
        if parsed_date is not None:
            parsed = dt.datetime.combine(parsed_date, dt.time.min, tzinfo=dt.timezone.utc)
    if parsed is None:
        raise ValueError(f"Invalid explicit UTC datetime '{raw_value}'.")
    return _ensure_utc(parsed)


def _ordered_unique(items: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


def _message_implies_res_generation(message: str) -> bool:
    lowered = message.lower()
    return bool(re.search(r"\bres\b|\brenewable(?:s)?\b", lowered))


def _message_mentions_supported_metric(message: str) -> bool:
    lowered = message.lower()
    return bool(
        re.search(
            r"\bprice(?:s)?\b|\bres\b|\brenewable(?:s)?\b|\bsolar\b|\bwind\b"
            r"|\bgeneration\b|\bcapacity\b|\bflow(?:s)?\b",
            lowered,
        )
    )


def _message_mentions_timeframe(message: str) -> bool:
    lowered = message.lower()
    return bool(
        re.search(
            r"\b(today|yesterday|last|week|weeks|month|months|year|years"
            r"|from|to|between|daily|monthly|yearly|annual)\b",
            lowered,
        )
        or re.search(r"\d{4}-\d{2}-\d{2}", lowered)
    )


def _message_mentions_resolution(message: str) -> bool:
    lowered = message.lower()
    return bool(
        re.search(
            r"\b(hour|hours|hourly|daily|day-by-day|monthly|yearly|annual|native|raw)\b",
            lowered,
        )
    )


def _extract_chart_type(message: str) -> str | None:
    lowered = message.lower()
    if re.search(r"\b(bar|bars|column|columns)(?:\s+chart)?\b", lowered):
        return "bar"
    if re.search(r"\bline(?:\s+chart)?\b", lowered):
        return "line"
    return None


def _looks_like_visualization_follow_up(message: str) -> bool:
    return (
        _extract_chart_type(message) is not None
        and not _message_mentions_supported_metric(message)
        and not _message_mentions_timeframe(message)
    )


def _configured_country_codes() -> set[str]:
    codes: set[str] = set()
    for setting_name in ("ENTSOE_COUNTRY_TO_EICS", "ENTSOE_PRICE_COUNTRY_TO_EICS"):
        raw_mapping = getattr(settings, setting_name, None) or {}
        if isinstance(raw_mapping, dict):
            codes.update(
                str(code).strip().upper()
                for code in raw_mapping.keys()
                if len(str(code).strip()) == 2
            )
    raw_coords = getattr(settings, "COUNTRY_COORDS", None) or []
    if isinstance(raw_coords, list):
        codes.update(
            str(item.get("code", "")).strip().upper()
            for item in raw_coords
            if isinstance(item, dict) and len(str(item.get("code", "")).strip()) == 2
        )
    return {code for code in codes if code.isalpha()}


def _extract_countries_from_message(message: str) -> list[str]:
    configured_codes = _configured_country_codes()
    countries: list[str] = []
    for match in re.finditer(r"\b[A-Z]{2}\b", message):
        code = match.group(0).upper()
        if configured_codes and code not in configured_codes:
            continue
        countries.append(code)
    return _ordered_unique(countries)


def _extract_resolution_from_message(message: str) -> str | None:
    lowered = message.lower()
    if re.search(r"\b(daily|day-by-day)\b", lowered):
        return "d"
    if re.search(r"\b(monthly)\b", lowered):
        return "m"
    if re.search(r"\b(yearly|annual)\b", lowered):
        return "y"
    if re.search(r"\b(native|raw|hourly|hour|hours)\b", lowered):
        return ""
    return None


def _extract_metrics_from_message(message: str) -> tuple[list[str], bool] | None:
    lowered = message.lower()
    include_prices = bool(re.search(r"\bprice(?:s)?\b", lowered))
    series_matches: list[tuple[int, str]] = []
    for series_key, pattern in (("wind", r"\bwind\b"), ("solar", r"\bsolar\b")):
        match = re.search(pattern, lowered)
        if match:
            series_matches.append((match.start(), series_key))
    generation_series = [series_key for _, series_key in sorted(series_matches)]
    if not generation_series and _message_implies_res_generation(message):
        generation_series = ["res"]
    if generation_series or include_prices:
        return generation_series, include_prices
    return None


def _extract_calendar_month_window(
    message: str,
    now_utc: dt.datetime,
) -> tuple[dt.datetime, dt.datetime, str] | None:
    month_match = re.search(
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
        r"nov(?:ember)?|dec(?:ember)?"
        r")\b(?:\s+(\d{4}))?",
        message.lower(),
    )
    if month_match is None:
        return None

    month_token = month_match.group(1).lower()
    month_number = {
        "jan": 1, "january": 1, "feb": 2, "february": 2,
        "mar": 3, "march": 3, "apr": 4, "april": 4,
        "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }[month_token]

    today_utc = _ensure_utc(now_utc).replace(hour=0, minute=0, second=0, microsecond=0)
    year = int(month_match.group(2)) if month_match.group(2) else today_utc.year
    start_utc = dt.datetime(year, month_number, 1, tzinfo=dt.timezone.utc)
    next_month_start = (
        dt.datetime(year + 1, 1, 1, tzinfo=dt.timezone.utc)
        if month_number == 12
        else dt.datetime(year, month_number + 1, 1, tzinfo=dt.timezone.utc)
    )
    end_utc = (
        min(next_month_start, today_utc)
        if year == today_utc.year and month_number == today_utc.month
        else next_month_start
    )
    if start_utc >= end_utc:
        return None

    month_label = start_utc.strftime("%B")
    if month_match.group(2):
        month_label = f"{month_label} {year}"
    return start_utc, end_utc, month_label


def _infer_default_resolution(
    requested_resolution: str,
    message: str,
    start_utc: dt.datetime,
    end_utc: dt.datetime,
) -> str:
    if requested_resolution:
        return requested_resolution
    if _message_mentions_resolution(message):
        return requested_resolution
    if end_utc - start_utc >= dt.timedelta(days=28):
        return "d"
    return requested_resolution


def _format_utc_z(value: dt.datetime) -> str:
    return _ensure_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


# ────────────────────────── Claude API call ──────────────────────────────────

def _call_claude_for_intent(
    message: str,
    *,
    previous_query: dict | None = None,
    conversation_messages: list[dict[str, str]] | None = None,
    pending_clarification: dict | None = None,
) -> dict:
    api_key = getattr(settings, "ANTHROPIC_API_KEY", "") or ""
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY is not configured.")

    model = getattr(settings, "CLAUDE_CHAT_MODEL", "claude-sonnet-4-6")
    timeout_seconds = float(getattr(settings, "CLAUDE_CHAT_TIMEOUT", 30))

    # Build system prompt additions for stateful context
    system_parts = [SYSTEM_PROMPT]
    if previous_query:
        system_parts.append(
            "PREVIOUS QUERY CONTEXT — reuse metric, countries, timeframe, and resolution "
            "unless the user explicitly changes them:\n"
            + json.dumps(previous_query, sort_keys=True, default=str)
        )
    if pending_clarification:
        system_parts.append(
            "PENDING CLARIFICATION — you previously asked the user a question. "
            "Treat their next message primarily as the answer:\n"
            + json.dumps(pending_clarification, sort_keys=True)
        )
    full_system = "\n\n".join(system_parts)

    # Build message list from conversation history
    messages: list[dict] = []
    if conversation_messages:
        for msg in conversation_messages:
            role = str(msg.get("role", "")).strip().lower()
            content = str(msg.get("content", "")).strip()
            if role in {"user", "assistant"} and content:
                messages.append({"role": role, "content": content})
        # Anthropic requires messages to start with "user" and not end with "user"
        # before appending the new user turn — trim trailing user messages
        while messages and messages[0]["role"] == "assistant":
            messages.pop(0)
        while messages and messages[-1]["role"] == "user":
            messages.pop()

    messages.append({"role": "user", "content": message})

    client = anthropic.Anthropic(api_key=api_key, timeout=timeout_seconds)
    try:
        response = client.messages.create(
            model=model,
            max_tokens=1024,
            system=full_system,
            messages=messages,
            tools=[ANALYZE_QUERY_TOOL],
            tool_choice={"type": "tool", "name": "analyze_energy_query"},
        )
    except anthropic.APIConnectionError as exc:
        raise ValueError(f"Claude request failed (connection error): {exc}") from exc
    except anthropic.RateLimitError as exc:
        raise ValueError(f"Claude request failed (rate limit): {exc}") from exc
    except anthropic.APIStatusError as exc:
        raise ValueError(f"Claude request failed ({exc.status_code}): {exc.message}") from exc

    for block in response.content:
        if block.type == "tool_use" and block.name == "analyze_energy_query":
            return block.input

    raise ValueError("Claude did not return a structured analysis.")


# ──────────────────────── Query normalization helpers ────────────────────────

def _normalize_previous_query(previous_query: dict | None) -> dict | None:
    if not isinstance(previous_query, dict):
        return None

    raw_countries = previous_query.get("countries")
    countries: list[str] = []
    if isinstance(raw_countries, list):
        countries = _ordered_unique(
            [
                str(item).strip().upper()
                for item in raw_countries
                if str(item).strip() and len(str(item).strip()) == 2
            ]
        )

    country = str(previous_query.get("country", "")).strip().upper()
    if not countries and len(country) == 2:
        countries = [country]
    if not countries:
        return None

    generation_series = _ordered_unique(
        [
            str(item).strip().lower()
            for item in previous_query.get("generation_series", [])
            if str(item).strip().lower() in {"res", "solar", "wind"}
        ]
    )
    include_prices = bool(previous_query.get("include_prices", False))
    if not generation_series and not include_prices:
        return None

    start_raw = previous_query.get("start_utc")
    end_raw = previous_query.get("end_utc")
    if not start_raw or not end_raw:
        return None

    start_utc = _parse_utc_value(str(start_raw))
    end_utc = _parse_utc_value(str(end_raw))
    if start_utc >= end_utc:
        return None

    resolution = str(previous_query.get("resolution", "")).strip().lower()
    if resolution not in {"", "d", "m", "y"}:
        resolution = ""

    chart_type = str(previous_query.get("chart_type", "line")).strip().lower()
    if chart_type not in {"line", "bar"}:
        chart_type = "line"

    resolved_country = country if country in countries else countries[0]
    return {
        "country": resolved_country,
        "countries": countries,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "resolution": resolution,
        "generation_series": generation_series,
        "include_prices": include_prices,
        "chart_type": chart_type,
    }


def _merge_with_previous_query(intent: dict, message: str, previous_query: dict | None) -> dict:
    normalized_previous = _normalize_previous_query(previous_query)
    if normalized_previous is None:
        return intent

    merged_intent = dict(intent)
    requested_chart_type = _extract_chart_type(message)
    if requested_chart_type:
        merged_intent["chart_type"] = requested_chart_type

    if _looks_like_visualization_follow_up(message):
        return {
            "intent": merged_intent.get("intent", "chart"),
            "country": normalized_previous["country"],
            "countries": normalized_previous["countries"],
            "resolution": normalized_previous["resolution"] or "native",
            "generation_series": normalized_previous["generation_series"],
            "include_prices": normalized_previous["include_prices"],
            "chart_type": merged_intent.get("chart_type", normalized_previous["chart_type"]),
            "timeframe": {
                "kind": "explicit_utc_range",
                "amount": None,
                "start_utc": _format_utc_z(normalized_previous["start_utc"]),
                "end_utc": _format_utc_z(normalized_previous["end_utc"]),
            },
        }

    if not merged_intent.get("generation_series") and not merged_intent.get("include_prices", False):
        merged_intent["generation_series"] = normalized_previous["generation_series"]
        merged_intent["include_prices"] = normalized_previous["include_prices"]

    raw_countries = merged_intent.get("countries")
    if not isinstance(raw_countries, list) or not any(str(item).strip() for item in raw_countries):
        merged_intent["countries"] = normalized_previous["countries"]
        merged_intent["country"] = normalized_previous["country"]

    return merged_intent


def _compute_window_from_intent(timeframe: dict, now_utc: dt.datetime) -> tuple[dt.datetime, dt.datetime, str]:
    today_utc = _ensure_utc(now_utc).replace(hour=0, minute=0, second=0, microsecond=0)
    kind = timeframe["kind"]

    if kind == "today":
        return today_utc, today_utc + dt.timedelta(days=1), "today"
    if kind == "yesterday":
        return today_utc - dt.timedelta(days=1), today_utc, "yesterday"
    if kind == "last_n_days":
        amount = timeframe["amount"]
        if not isinstance(amount, int) or amount < 1:
            raise ValueError("last_n_days requires a positive integer amount.")
        return today_utc - dt.timedelta(days=amount), today_utc, f"last {amount} days"
    if kind == "last_n_weeks":
        amount = timeframe["amount"]
        if not isinstance(amount, int) or amount < 1:
            raise ValueError("last_n_weeks requires a positive integer amount.")
        return today_utc - dt.timedelta(days=amount * 7), today_utc, f"last {amount} weeks"
    if kind == "explicit_utc_range":
        start_raw = timeframe["start_utc"]
        end_raw = timeframe["end_utc"]
        if not start_raw or not end_raw:
            raise ValueError("explicit_utc_range requires start_utc and end_utc.")
        return _parse_utc_value(start_raw), _parse_utc_value(end_raw), "custom range"
    if kind == "unknown":
        raise ChartQueryNeedsClarification(["timeframe"])

    raise ValueError(f"Unsupported timeframe kind '{kind}'.")


def _fallback_clarifying_question(missing_fields: list[str]) -> str:
    missing = set(missing_fields)
    if missing == {"metric"}:
        return (
            "Which data would you like to see? Options: RES generation, solar, wind, "
            "day-ahead prices, installed capacity, or cross-border flows."
        )
    if missing == {"country"}:
        return "Which country (or countries) should I use?"
    if missing == {"timeframe"}:
        return "What time range should I use? For example: 'last 7 days', 'April 2025', or 'from 2025-01-01 to 2025-03-31'."
    if missing == {"metric", "country"}:
        return "Which data and for which country would you like to see?"
    if missing == {"metric", "timeframe"}:
        return "Which data would you like and for what time range?"
    if missing == {"country", "timeframe"}:
        return "Which country and what time range should I use?"
    return (
        "Which data would you like to see and for which country? "
        "For example: 'RES generation for DE for the last 7 days' or 'day-ahead prices for FR in April 2025'."
    )


def _build_clarification_result(
    message: str,
    analysis: dict,
    missing_fields: list[str],
) -> ChartQueryAgentResult:
    normalized_missing = _ordered_unique(
        [
            str(f).strip().lower()
            for f in missing_fields
            if str(f).strip().lower() in {"metric", "country", "timeframe"}
        ]
    )
    question = (
        str(analysis.get("clarifying_question") or "").strip()
        or _fallback_clarifying_question(normalized_missing)
    )
    return ChartQueryAgentResult(
        status="needs_clarification",
        clarification=ChartQueryClarification(
            original_message=message,
            question=question,
            missing_fields=normalized_missing,
        ),
    )


def _apply_explicit_message_overrides(intent: dict, message: str) -> dict:
    overridden = dict(intent)

    explicit_countries = _extract_countries_from_message(message)
    if explicit_countries:
        overridden["countries"] = explicit_countries
        overridden["country"] = explicit_countries[0]

    explicit_resolution = _extract_resolution_from_message(message)
    if explicit_resolution is not None:
        overridden["resolution"] = explicit_resolution or "native"

    explicit_metrics = _extract_metrics_from_message(message)
    if explicit_metrics is not None:
        gen_series, incl_prices = explicit_metrics
        overridden["generation_series"] = gen_series
        overridden["include_prices"] = incl_prices

    return overridden


def _infer_missing_fields(intent: dict, *, message: str, now_utc: dt.datetime) -> list[str]:
    missing_fields: list[str] = []

    raw_countries = intent.get("countries")
    countries: list[str] = []
    if isinstance(raw_countries, list):
        countries = [str(item).strip().upper() for item in raw_countries if str(item).strip()]
    compat_country = str(intent.get("country", "")).strip().upper()
    if not countries and compat_country:
        countries = [compat_country]
    if not countries:
        missing_fields.append("country")

    generation_series = [
        str(item).strip().lower()
        for item in intent.get("generation_series", [])
        if str(item).strip()
    ]
    include_prices = bool(intent.get("include_prices", False))
    data_type = str(intent.get("data_type") or "").strip().lower()
    _known_standalone_types = {"capacity", "flows", "generation", "generation_res", "prices"}
    if (
        not generation_series
        and not include_prices
        and not _message_implies_res_generation(message)
        and data_type not in _known_standalone_types
    ):
        missing_fields.append("metric")

    explicit_month_window = _extract_calendar_month_window(message, now_utc)
    if explicit_month_window is None:
        timeframe = intent.get("timeframe")
        if not isinstance(timeframe, dict):
            missing_fields.append("timeframe")
        else:
            kind = str(timeframe.get("kind", "")).strip().lower()
            if kind == "unknown":
                missing_fields.append("timeframe")
            elif kind in {"last_n_days", "last_n_weeks"} and not isinstance(timeframe.get("amount"), int):
                missing_fields.append("timeframe")
            elif kind == "explicit_utc_range" and (not timeframe.get("start_utc") or not timeframe.get("end_utc")):
                missing_fields.append("timeframe")
            elif kind not in {"today", "yesterday", "last_n_days", "last_n_weeks", "explicit_utc_range"}:
                missing_fields.append("timeframe")

    return _ordered_unique(missing_fields)


_SUPPORTED_DATA_TYPES = {"prices", "generation_res", "capacity"}


def _parse_data_query(
    intent: dict,
    *,
    message: str,
    now_utc: dt.datetime,
) -> "ParsedDataQuery | None":
    """Returns None if data_type is unsupported (caller falls back to chart intent)."""
    data_type = str(intent.get("data_type") or "").strip().lower()
    if data_type not in _SUPPORTED_DATA_TYPES:
        return None

    raw_countries = intent.get("countries")
    countries: list[str] = []
    if isinstance(raw_countries, list):
        countries = _ordered_unique(
            [str(c).strip().upper() for c in raw_countries if str(c).strip()]
        )
    compat_country = str(intent.get("country", "")).strip().upper()
    if not countries and compat_country:
        countries = [compat_country]
    if not countries:
        raise ChartQueryNeedsClarification(["country"])
    if any(len(c) != 2 or not c.isalpha() for c in countries):
        raise ValueError("Claude returned invalid country codes.")
    country = compat_country if compat_country in countries else countries[0]

    explicit_month_window = _extract_calendar_month_window(message, now_utc)
    if explicit_month_window is not None:
        start_utc, end_utc, time_phrase = explicit_month_window
    else:
        timeframe = intent.get("timeframe")
        if not isinstance(timeframe, dict):
            raise ChartQueryNeedsClarification(["timeframe"])
        start_utc, end_utc, time_phrase = _compute_window_from_intent(timeframe, now_utc)
    if start_utc >= end_utc:
        raise ValueError("Parsed start time must be earlier than end time.")

    generation_series: list[str] = []
    include_prices = False
    if data_type == "prices":
        include_prices = True
    elif data_type == "generation_res":
        generation_series = _ordered_unique(
            [str(s).strip().lower() for s in intent.get("generation_series", []) if str(s).strip()]
        )
        if not generation_series and _message_implies_res_generation(message):
            generation_series = ["res"]
        if not generation_series:
            raise ChartQueryNeedsClarification(["metric"])
    # data_type == "capacity": no generation_series or include_prices needed

    return ParsedDataQuery(
        original_message=message,
        country=country,
        countries=countries,
        start_utc=start_utc,
        end_utc=end_utc,
        time_phrase=time_phrase,
        data_type=data_type,
        generation_series=generation_series,
        include_prices=include_prices,
    )


def _parse_ready_chart_query(
    intent: dict,
    *,
    message: str,
    now_utc: dt.datetime,
    previous_query: dict | None = None,
) -> ParsedChartQuery:
    raw_countries = intent.get("countries")
    countries: list[str] = []
    if isinstance(raw_countries, list):
        countries = _ordered_unique(
            [str(item).strip().upper() for item in raw_countries if str(item).strip()]
        )

    compat_country = str(intent.get("country", "")).strip().upper()
    if not countries and compat_country:
        countries = [compat_country]

    if not countries:
        raise ChartQueryNeedsClarification(["country"])
    if any(len(c) != 2 or not c.isalpha() for c in countries):
        raise ValueError("Claude returned invalid country codes.")

    country = compat_country if compat_country in countries else countries[0]

    resolution = intent.get("resolution", "native")
    if resolution == "native":
        resolution = ""
    if resolution not in {"", "d", "m", "y"}:
        raise ValueError("Claude returned an unsupported resolution.")

    generation_series = _ordered_unique(
        [str(item).strip().lower() for item in intent.get("generation_series", []) if str(item).strip()]
    )
    if any(item not in {"res", "solar", "wind"} for item in generation_series):
        raise ValueError("Claude returned unsupported generation series.")

    include_prices = bool(intent.get("include_prices", False))
    if not generation_series and _message_implies_res_generation(message):
        generation_series = ["res"]
    data_type_for_chart = str(intent.get("data_type") or "").strip().lower()
    _known_standalone_chart_types = {"capacity", "flows", "generation", "generation_res", "prices"}
    if not generation_series and not include_prices and data_type_for_chart not in _known_standalone_chart_types:
        raise ChartQueryNeedsClarification(["metric"])

    chart_type = str(intent.get("chart_type", "line")).strip().lower()
    if chart_type not in {"line", "bar"}:
        chart_type = _extract_chart_type(message) or "line"

    timeframe = intent.get("timeframe")
    explicit_month_window = _extract_calendar_month_window(message, now_utc)
    if explicit_month_window is not None:
        start_utc, end_utc, time_phrase = explicit_month_window
    else:
        if not isinstance(timeframe, dict):
            raise ChartQueryNeedsClarification(["timeframe"])
        start_utc, end_utc, time_phrase = _compute_window_from_intent(timeframe, now_utc)

    if start_utc >= end_utc:
        raise ValueError("Parsed start time must be earlier than end time.")

    resolution = _infer_default_resolution(resolution, message, start_utc, end_utc)

    resolution_label = {"": "native", "d": "daily", "m": "monthly", "y": "yearly"}[resolution]
    time_phrase_override = None
    if _looks_like_visualization_follow_up(message) and isinstance(previous_query, dict):
        raw_phrase = previous_query.get("time_phrase")
        if isinstance(raw_phrase, str) and raw_phrase.strip():
            time_phrase_override = raw_phrase.strip()

    return ParsedChartQuery(
        original_message=message,
        country=country,
        countries=countries,
        start_utc=start_utc,
        end_utc=end_utc,
        resolution=resolution,
        time_phrase=time_phrase_override or (
            f"{time_phrase} at {resolution_label} resolution" if resolution else time_phrase
        ),
        generation_series=generation_series,
        include_prices=include_prices,
        chart_type=chart_type,
    )


# ─────────────────────── Data analysis (second Claude call) ──────────────────

def build_data_description(query: ParsedDataQuery) -> str:
    countries_str = ", ".join(query.countries)
    if query.data_type == "prices":
        return (
            f"Day-ahead electricity prices (EUR/MWh) for {countries_str} — {query.time_phrase}. "
            "Columns: date, avg_eur_mwh, max_eur_mwh, min_eur_mwh, hour_count."
        )
    if query.data_type == "capacity":
        year = query.start_utc.year
        return (
            f"Installed generation capacity (MW) for {countries_str} — year {year}. "
            "Columns: psr_type, psr_name, installed_capacity_mw."
        )
    series_str = " + ".join(query.generation_series) if query.generation_series else "RES"
    return (
        f"RES generation ({series_str}, MW) for {countries_str} — {query.time_phrase}. "
        "Columns: date, avg_mw, max_mw, min_mw, hour_count."
    )


def call_claude_for_data_analysis(
    question: str,
    data_rows: list[dict],
    *,
    data_description: str,
) -> str:
    """Second Claude call: receives fetched data rows and answers the user's question."""
    api_key = getattr(settings, "ANTHROPIC_API_KEY", "") or ""
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY is not configured.")

    model = getattr(settings, "CLAUDE_CHAT_MODEL", "claude-sonnet-4-6")
    timeout_seconds = float(getattr(settings, "CLAUDE_CHAT_TIMEOUT", 30))

    system = (
        "You are an energy data analyst. "
        "Answer the user's question using only the dataset provided. "
        "Be concise and always include specific numbers. "
        "If the data is insufficient to answer precisely, say so clearly."
    )

    data_json = json.dumps(data_rows, default=str)
    user_content = (
        f"Dataset: {data_description}\n\n"
        f"Data:\n{data_json}\n\n"
        f"Question: {question}"
    )

    client = anthropic.Anthropic(api_key=api_key, timeout=timeout_seconds)
    try:
        response = client.messages.create(
            model=model,
            max_tokens=512,
            system=system,
            messages=[{"role": "user", "content": user_content}],
        )
    except anthropic.APIConnectionError as exc:
        raise ValueError(f"Claude analysis request failed (connection error): {exc}") from exc
    except anthropic.RateLimitError as exc:
        raise ValueError(f"Claude analysis request failed (rate limit): {exc}") from exc
    except anthropic.APIStatusError as exc:
        raise ValueError(f"Claude analysis request failed ({exc.status_code}): {exc.message}") from exc

    for block in response.content:
        if hasattr(block, "text"):
            return block.text.strip()

    raise ValueError("Claude did not return a text analysis.")


# ───────────────────────────── Main entry point ──────────────────────────────

def parse_chart_query(
    message: str,
    *,
    now_utc: dt.datetime,
    previous_query: dict | None = None,
    conversation_messages: list[dict[str, str]] | None = None,
    pending_clarification: dict | None = None,
) -> ChartQueryAgentResult:
    normalized_message = (message or "").strip()
    if not normalized_message:
        raise ValueError("message is required.")

    normalized_previous = _normalize_previous_query(previous_query)

    analysis = _call_claude_for_intent(
        normalized_message,
        previous_query=previous_query,
        conversation_messages=conversation_messages,
        pending_clarification=pending_clarification,
    )

    # Direct text answer — no DB query needed
    if analysis.get("intent") == "text":
        text_answer = str(analysis.get("text_answer") or "").strip()
        if not text_answer:
            text_answer = (
                "I can help you explore European electricity data including RES generation, "
                "solar, wind, day-ahead prices, installed capacity, and cross-border flows. "
                "Try asking: 'Show RES generation for DE for the last 7 days'."
            )
        return ChartQueryAgentResult(status="text", text_answer=text_answer)

    # Visualization-only follow-up with no prior context
    if _looks_like_visualization_follow_up(normalized_message) and normalized_previous is None:
        missing_fields = ["metric", "country", "timeframe"]
        model_missing = analysis.get("missing_fields")
        if isinstance(model_missing, list):
            missing_fields = _ordered_unique(
                missing_fields
                + [
                    str(f).strip().lower()
                    for f in model_missing
                    if str(f).strip().lower() in {"metric", "country", "timeframe"}
                ]
            )
        return _build_clarification_result(normalized_message, analysis, missing_fields)

    # Explicit clarification from Claude
    if analysis.get("intent") == "needs_clarification":
        missing_fields = [
            str(f).strip().lower()
            for f in (analysis.get("missing_fields") or [])
            if str(f).strip().lower() in {"metric", "country", "timeframe"}
        ]
        return _build_clarification_result(normalized_message, analysis, missing_fields or ["metric"])

    # chart or data — build query object
    intent_merged = _merge_with_previous_query(analysis, normalized_message, normalized_previous)
    intent_merged = _apply_explicit_message_overrides(intent_merged, normalized_message)

    if str(analysis.get("intent", "")).strip().lower() == "data":
        try:
            data_query = _parse_data_query(
                intent_merged,
                message=normalized_message,
                now_utc=now_utc,
            )
        except ChartQueryNeedsClarification as exc:
            missing_fields = _ordered_unique(
                exc.missing_fields
                + _infer_missing_fields(intent_merged, message=normalized_message, now_utc=now_utc)
            )
            return _build_clarification_result(normalized_message, analysis, missing_fields or ["metric"])
        if data_query is not None:
            return ChartQueryAgentResult(status="data", query=data_query)
        # unsupported data_type — fall through to chart

    try:
        query = _parse_ready_chart_query(
            intent_merged,
            message=normalized_message,
            now_utc=now_utc,
            previous_query=previous_query,
        )
    except ChartQueryNeedsClarification as exc:
        missing_fields = _ordered_unique(
            exc.missing_fields
            + _infer_missing_fields(intent_merged, message=normalized_message, now_utc=now_utc)
        )
        model_missing = analysis.get("missing_fields")
        if isinstance(model_missing, list):
            missing_fields = _ordered_unique(
                missing_fields
                + [
                    str(f).strip().lower()
                    for f in model_missing
                    if str(f).strip().lower() in {"metric", "country", "timeframe"}
                ]
            )
        return _build_clarification_result(normalized_message, analysis, missing_fields)

    return ChartQueryAgentResult(status="ready", query=query)
