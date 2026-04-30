from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass, field

import requests
from django.conf import settings
from django.utils.dateparse import parse_date, parse_datetime


OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

INTENT_JSON_SCHEMA = {
    "name": "chart_query_intent",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "country": {"type": "string"},
            "countries": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
            },
            "resolution": {
                "type": "string",
                "enum": ["native", "d", "m", "y"],
            },
            "generation_series": {
                "type": "array",
                "items": {"type": "string", "enum": ["res", "solar", "wind"]},
            },
            "include_prices": {"type": "boolean"},
            "chart_type": {
                "type": "string",
                "enum": ["line", "bar"],
            },
            "timeframe": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": [
                            "today",
                            "yesterday",
                            "last_n_days",
                            "last_n_weeks",
                            "explicit_utc_range",
                        ],
                    },
                    "amount": {
                        "anyOf": [
                            {"type": "integer", "minimum": 1},
                            {"type": "null"},
                        ]
                    },
                    "start_utc": {
                        "anyOf": [
                            {"type": "string"},
                            {"type": "null"},
                        ]
                    },
                    "end_utc": {
                        "anyOf": [
                            {"type": "string"},
                            {"type": "null"},
                        ]
                    },
                },
                "required": ["kind", "amount", "start_utc", "end_utc"],
            },
        },
        "required": [
            "country",
            "countries",
            "resolution",
            "generation_series",
            "include_prices",
            "chart_type",
            "timeframe",
        ],
    },
}

SYSTEM_PROMPT = """You convert user chart requests into a strict JSON intent.

Rules:
- Return only data matching the provided JSON schema.
- Supported generation series are only: res, solar, wind.
- Map requests for "RES", "renewables", or "renewable generation" to res unless the user explicitly asks for specific types.
- In this API, res means the available A69 renewable set: solar + wind.
- Supported non-generation metric is prices, mapped to include_prices=true.
- Supported chart types are line and bar.
- Countries must be 2-letter ISO codes.
- Always fill countries as an array in the same order as the request.
- If country is included, set it to the first requested country for compatibility.
- Resolution must be one of:
  - native: no aggregation requested
  - d: daily
  - m: monthly
  - y: yearly
- For "last two weeks" style requests, return timeframe.kind=last_n_weeks and amount=2.
- For explicit UTC ranges, use timeframe.kind=explicit_utc_range and fill start_utc/end_utc.
- If the request asks for unsupported metrics, omit them rather than inventing new ones.
- If the user asks for a bar or line chart, set chart_type accordingly. Otherwise use line.
- If no supported metric is requested, return empty generation_series and include_prices=false.
"""


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
    return bool(re.search(r"\bprice(?:s)?\b|\bres\b|\brenewable(?:s)?\b|\bsolar\b|\bwind\b", lowered))


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


def _message_mentions_timeframe(message: str) -> bool:
    lowered = message.lower()
    return bool(
        re.search(
            r"\b(today|yesterday|last|week|weeks|month|months|year|years|from|to|between|daily|monthly|yearly|annual)\b",
            lowered,
        )
        or re.search(r"\d{4}-\d{2}-\d{2}", lowered)
    )


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
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }[month_token]

    today_utc = _ensure_utc(now_utc).replace(hour=0, minute=0, second=0, microsecond=0)
    year = int(month_match.group(2)) if month_match.group(2) else today_utc.year
    start_utc = dt.datetime(year, month_number, 1, tzinfo=dt.timezone.utc)
    if month_number == 12:
        next_month_start = dt.datetime(year + 1, 1, 1, tzinfo=dt.timezone.utc)
    else:
        next_month_start = dt.datetime(year, month_number + 1, 1, tzinfo=dt.timezone.utc)

    end_utc = min(next_month_start, today_utc) if year == today_utc.year and month_number == today_utc.month else next_month_start
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

    window = end_utc - start_utc
    if window >= dt.timedelta(days=28):
        return "d"
    return requested_resolution


def _extract_output_text(response_json: dict) -> str:
    output_text = response_json.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    for item in response_json.get("output", []):
        if item.get("type") != "message":
            continue
        for content_item in item.get("content", []):
            text_value = content_item.get("text")
            if isinstance(text_value, str) and text_value.strip():
                return text_value

    raise ValueError("OpenAI response did not contain structured output text.")


def _call_openai_for_intent(message: str, *, previous_query: dict | None = None) -> dict:
    api_key = getattr(settings, "OPENAI_API_KEY", "") or ""
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not configured.")

    model = getattr(settings, "OPENAI_CHART_QUERY_MODEL", "gpt-4o-mini")
    timeout_seconds = getattr(settings, "OPENAI_CHART_QUERY_TIMEOUT", 30)

    input_items = [{"role": "system", "content": SYSTEM_PROMPT}]
    if previous_query:
        input_items.append(
            {
                "role": "system",
                "content": (
                    "Previous chart query context is provided as JSON below. "
                    "If the latest user message is a follow-up such as a chart-style change, "
                    "reuse the previous metric, countries, resolution, and timeframe unless the user explicitly changes them.\n"
                    f"{json.dumps(previous_query, sort_keys=True)}"
                ),
            }
        )
    input_items.append({"role": "user", "content": message})

    payload = {
        "model": model,
        "input": input_items,
        "text": {
            "format": {
                "type": "json_schema",
                "name": INTENT_JSON_SCHEMA["name"],
                "strict": INTENT_JSON_SCHEMA["strict"],
                "schema": INTENT_JSON_SCHEMA["schema"],
            }
        },
    }

    try:
        response = requests.post(
            OPENAI_RESPONSES_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
    except requests.HTTPError as exc:
        response_body = ""
        if exc.response is not None:
            try:
                response_body = exc.response.text.strip()
            except Exception:
                response_body = ""
        suffix = f" Response body: {response_body}" if response_body else ""
        raise ValueError(f"OpenAI request failed: {exc}.{suffix}") from exc
    except requests.RequestException as exc:
        raise ValueError(f"OpenAI request failed: {exc}") from exc

    response_json = response.json()
    if response_json.get("status") == "incomplete":
        raise ValueError("OpenAI response was incomplete.")

    return json.loads(_extract_output_text(response_json))


def _format_utc_z(value: dt.datetime) -> str:
    return _ensure_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


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

    raise ValueError(f"Unsupported timeframe kind '{kind}'.")


def parse_chart_query(message: str, *, now_utc: dt.datetime, previous_query: dict | None = None) -> ParsedChartQuery:
    normalized_message = (message or "").strip()
    if not normalized_message:
        raise ValueError("message is required.")

    normalized_previous = _normalize_previous_query(previous_query)
    if _looks_like_visualization_follow_up(normalized_message) and normalized_previous is None:
        raise ValueError(
            "No supported metric was found in the query. For follow-ups like 'make it a bar chart', "
            "send previous_query from the prior response or repeat the metric and date range."
        )

    intent = _call_openai_for_intent(normalized_message, previous_query=previous_query)
    intent = _merge_with_previous_query(intent, normalized_message, normalized_previous)

    explicit_countries = _extract_countries_from_message(normalized_message)
    if explicit_countries:
        intent["countries"] = explicit_countries
        intent["country"] = explicit_countries[0]

    explicit_resolution = _extract_resolution_from_message(normalized_message)
    if explicit_resolution is not None:
        intent["resolution"] = explicit_resolution or "native"

    explicit_metrics = _extract_metrics_from_message(normalized_message)
    if explicit_metrics is not None:
        generation_series_override, include_prices_override = explicit_metrics
        intent["generation_series"] = generation_series_override
        intent["include_prices"] = include_prices_override

    raw_countries = intent.get("countries")
    countries: list[str] = []
    if isinstance(raw_countries, list):
        countries = _ordered_unique(
            [
                str(item).strip().upper()
                for item in raw_countries
                if str(item).strip()
            ]
        )

    compatibility_country = str(intent.get("country", "")).strip().upper()
    if not countries and compatibility_country:
        countries = [compatibility_country]

    if not countries or any(len(country_code) != 2 or not country_code.isalpha() for country_code in countries):
        raise ValueError("The model did not return valid 2-letter country codes.")

    country = compatibility_country if compatibility_country in countries else countries[0]

    resolution = intent.get("resolution", "native")
    if resolution == "native":
        resolution = ""
    if resolution not in {"", "d", "m", "y"}:
        raise ValueError("The model returned an unsupported resolution.")

    generation_series = _ordered_unique(
        [
            str(item).strip().lower()
            for item in intent.get("generation_series", [])
            if str(item).strip()
        ]
    )
    if any(item not in {"res", "solar", "wind"} for item in generation_series):
        raise ValueError("The model returned unsupported generation series.")

    include_prices = bool(intent.get("include_prices", False))
    if not generation_series and _message_implies_res_generation(normalized_message):
        generation_series = ["res"]
    if not generation_series and not include_prices:
        if _extract_chart_type(normalized_message):
            raise ValueError(
                "No supported metric was found in the query. For follow-ups like 'make it a bar chart', "
                "send previous_query from the prior response or repeat the metric and date range."
            )
        raise ValueError("No supported metric was found in the query.")

    chart_type = str(intent.get("chart_type", "line")).strip().lower()
    if chart_type not in {"line", "bar"}:
        chart_type = _extract_chart_type(normalized_message) or "line"

    timeframe = intent.get("timeframe")
    explicit_month_window = _extract_calendar_month_window(normalized_message, now_utc)
    if explicit_month_window is not None:
        start_utc, end_utc, time_phrase = explicit_month_window
    else:
        if not isinstance(timeframe, dict):
            raise ValueError("The model did not return a valid timeframe object.")
        start_utc, end_utc, time_phrase = _compute_window_from_intent(timeframe, now_utc)
    if start_utc >= end_utc:
        raise ValueError("The parsed start time must be earlier than end time.")

    resolution = _infer_default_resolution(
        resolution,
        normalized_message,
        start_utc,
        end_utc,
    )

    resolution_label = {
        "": "native",
        "d": "daily",
        "m": "monthly",
        "y": "yearly",
    }[resolution]
    time_phrase_override = None
    if _looks_like_visualization_follow_up(normalized_message) and isinstance(previous_query, dict):
        raw_time_phrase = previous_query.get("time_phrase")
        if isinstance(raw_time_phrase, str) and raw_time_phrase.strip():
            time_phrase_override = raw_time_phrase.strip()

    return ParsedChartQuery(
        original_message=normalized_message,
        country=country,
        countries=countries,
        start_utc=start_utc,
        end_utc=end_utc,
        resolution=resolution,
        time_phrase=time_phrase_override or (f"{time_phrase} at {resolution_label} resolution" if resolution else time_phrase),
        generation_series=generation_series,
        include_prices=include_prices,
        chart_type=chart_type,
    )
