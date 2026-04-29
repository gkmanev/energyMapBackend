from __future__ import annotations

import datetime as dt
import json
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
                "items": {"type": "string", "enum": ["solar", "wind"]},
            },
            "include_prices": {"type": "boolean"},
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
            "timeframe",
        ],
    },
}

SYSTEM_PROMPT = """You convert user chart requests into a strict JSON intent.

Rules:
- Return only data matching the provided JSON schema.
- Supported generation series are only: solar, wind.
- Supported non-generation metric is prices, mapped to include_prices=true.
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


def _call_openai_for_intent(message: str) -> dict:
    api_key = getattr(settings, "OPENAI_API_KEY", "") or ""
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not configured.")

    model = getattr(settings, "OPENAI_CHART_QUERY_MODEL", "gpt-4o-mini")
    timeout_seconds = getattr(settings, "OPENAI_CHART_QUERY_TIMEOUT", 30)

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": message},
        ],
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


def parse_chart_query(message: str, *, now_utc: dt.datetime) -> ParsedChartQuery:
    normalized_message = (message or "").strip()
    if not normalized_message:
        raise ValueError("message is required.")

    intent = _call_openai_for_intent(normalized_message)

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

    generation_series = _ordered_unique(list(intent.get("generation_series", [])))
    if any(item not in {"solar", "wind"} for item in generation_series):
        raise ValueError("The model returned unsupported generation series.")

    include_prices = bool(intent.get("include_prices", False))
    if not generation_series and not include_prices:
        raise ValueError("No supported metric was found in the query.")

    timeframe = intent.get("timeframe")
    if not isinstance(timeframe, dict):
        raise ValueError("The model did not return a valid timeframe object.")

    start_utc, end_utc, time_phrase = _compute_window_from_intent(timeframe, now_utc)
    if start_utc >= end_utc:
        raise ValueError("The parsed start time must be earlier than end time.")

    resolution_label = {
        "": "native",
        "d": "daily",
        "m": "monthly",
        "y": "yearly",
    }[resolution]

    return ParsedChartQuery(
        original_message=normalized_message,
        country=country,
        countries=countries,
        start_utc=start_utc,
        end_utc=end_utc,
        resolution=resolution,
        time_phrase=f"{time_phrase} at {resolution_label} resolution" if resolution else time_phrase,
        generation_series=generation_series,
        include_prices=include_prices,
    )
