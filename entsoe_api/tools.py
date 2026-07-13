from __future__ import annotations

import datetime as dt
import json
from collections import defaultdict
from typing import Any

from django.conf import settings
from django.db.models import Avg
from django.db.models.functions import TruncDay, TruncMonth, TruncYear
from django.utils.dateparse import parse_date, parse_datetime

from .models import (
    ContractType,
    CountryCapacitySnapshot,
    CountryGenerationByType,
    CountryPricePoint,
    CountryResGenerationByType,
    PhysicalFlow,
)


MAX_ROWS_TO_MODEL = 400

CHART_GENERATION_SERIES = {
    "res": {"label": "RES (solar + wind)", "psr_types": {"B16", "B18", "B19"}},
    "solar": {"label": "Solar", "psr_types": {"B16"}},
    "wind": {"label": "Wind", "psr_types": {"B18", "B19"}},
}


def _ensure_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _fmt_z(value: dt.datetime) -> str:
    return _ensure_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _bucket_start(timestamp: dt.datetime, resolution: str) -> dt.datetime:
    timestamp = _ensure_utc(timestamp)
    if resolution == "d":
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    if resolution == "m":
        return timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if resolution == "y":
        return timestamp.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return timestamp


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 3)


def _configured_country_codes() -> set[str]:
    codes: set[str] = set()
    for name in ("ENTSOE_COUNTRY_TO_EICS", "ENTSOE_PRICE_COUNTRY_TO_EICS"):
        mapping = getattr(settings, name, None) or {}
        if isinstance(mapping, dict):
            codes.update(str(code).strip().upper() for code in mapping if len(str(code).strip()) == 2)
    return {code for code in codes if code.isalpha()}


def _validate_countries(raw: Any) -> tuple[list[str] | None, str | None]:
    if not isinstance(raw, list) or not raw:
        return None, "countries must be a non-empty list of 2-letter ISO codes."
    codes = [str(code).strip().upper() for code in raw if str(code).strip()]
    bad = [code for code in codes if len(code) != 2 or not code.isalpha()]
    if bad:
        return None, f"Invalid country codes: {bad}. Use 2-letter ISO codes like DE, FR, BG."
    configured = _configured_country_codes()
    unsupported = [code for code in codes if configured and code not in configured]
    if unsupported:
        return None, (
            f"Unsupported countries: {unsupported}. "
            f"Supported: {', '.join(sorted(configured))}."
        )
    seen: set[str] = set()
    return [code for code in codes if not (code in seen or seen.add(code))], None


def _parse_utc(raw: Any, field: str) -> tuple[dt.datetime | None, str | None]:
    value = str(raw or "").strip()
    if not value:
        return None, f"{field} is required (ISO 8601 UTC, e.g. 2026-07-01T00:00:00Z)."
    parsed = parse_datetime(value)
    if parsed is None and value.endswith("Z"):
        parsed = parse_datetime(value.replace("Z", "+00:00"))
    if parsed is None:
        parsed_date = parse_date(value)
        if parsed_date is not None:
            parsed = dt.datetime.combine(parsed_date, dt.time.min, tzinfo=dt.timezone.utc)
    if parsed is None:
        return None, f"Could not parse {field}='{value}' as an ISO datetime."
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc), None


def _validate_window(inputs: dict) -> tuple[tuple[dt.datetime, dt.datetime] | None, str | None]:
    start, error = _parse_utc(inputs.get("start_utc"), "start_utc")
    if error:
        return None, error
    end, error = _parse_utc(inputs.get("end_utc"), "end_utc")
    if error:
        return None, error
    if start >= end:
        return None, f"start_utc ({start}) must be earlier than end_utc ({end})."
    return (start, end), None


def _cap_rows(rows: list[dict]) -> dict:
    if len(rows) <= MAX_ROWS_TO_MODEL:
        return {"rows": rows, "row_count": len(rows), "truncated": False}
    return {
        "rows": rows[:MAX_ROWS_TO_MODEL],
        "row_count": len(rows),
        "truncated": True,
        "note": (
            f"Returned first {MAX_ROWS_TO_MODEL} of {len(rows)} rows. "
            "Re-call with a coarser resolution (d/m/y) or a narrower window if you need the full picture."
        ),
    }


def _resolution_error(resolution: str) -> str | None:
    if resolution not in {"native", "d", "m", "y"}:
        return "resolution must be one of: native, d, m, y."
    return None


def _exec_get_res_generation(inputs: dict, ctx: dict) -> dict:
    countries, error = _validate_countries(inputs.get("countries"))
    if error:
        return {"error": error}
    window, error = _validate_window(inputs)
    if error:
        return {"error": error}

    series = [item for item in (inputs.get("series") or ["res"]) if item in CHART_GENERATION_SERIES]
    if not series:
        return {"error": "series must contain at least one of: res, solar, wind."}

    resolution = str(inputs.get("resolution") or "native")
    error = _resolution_error(resolution)
    if error:
        return {"error": error}

    requested_psr_types = sorted(
        {
            psr_type
            for series_key in series
            for psr_type in CHART_GENERATION_SERIES[series_key]["psr_types"]
        }
    )
    rows = (
        CountryResGenerationByType.objects
        .filter(
            country_id__in=countries,
            datetime_utc__gte=window[0],
            datetime_utc__lt=window[1],
            psr_type__in=requested_psr_types,
        )
        .order_by("country_id", "datetime_utc", "psr_type")
        .values("country_id", "datetime_utc", "psr_type", "generation_mw")
    )

    timestamp_totals: dict[tuple[str, str, dt.datetime], float] = defaultdict(float)
    for row in rows:
        generation_value = row["generation_mw"]
        if generation_value is None:
            continue
        timestamp = _ensure_utc(row["datetime_utc"])
        for series_key in series:
            if row["psr_type"] not in CHART_GENERATION_SERIES[series_key]["psr_types"]:
                continue
            timestamp_totals[(row["country_id"], series_key, timestamp)] += float(generation_value)

    grouped_values: dict[tuple[str, str, dt.datetime], list[float]] = defaultdict(list)
    for (country_code, series_key, timestamp), total_value in timestamp_totals.items():
        grouped_values[(country_code, series_key, _bucket_start(timestamp, resolution))].append(total_value)

    country_order = {country: index for index, country in enumerate(countries)}
    series_order = {series_key: index for index, series_key in enumerate(series)}
    result_rows = [
        {
            "country": country_code,
            "series": series_key,
            "datetime_utc": _fmt_z(bucket),
            "value_mw": _average(values),
        }
        for (country_code, series_key, bucket), values in sorted(
            grouped_values.items(),
            key=lambda item: (
                country_order.get(item[0][0], 999),
                series_order.get(item[0][1], 999),
                item[0][2],
            ),
        )
    ]

    return _cap_rows(result_rows) | {
        "unit": "MW",
        "dataset": "A69 actual RES generation",
        "countries": countries,
        "series": series,
        "resolution": resolution,
    }


def _exec_get_prices(inputs: dict, ctx: dict) -> dict:
    countries, error = _validate_countries(inputs.get("countries"))
    if error:
        return {"error": error}
    window, error = _validate_window(inputs)
    if error:
        return {"error": error}

    resolution = str(inputs.get("resolution") or "native")
    error = _resolution_error(resolution)
    if error:
        return {"error": error}

    if resolution == "native":
        rows = (
            CountryPricePoint.objects
            .filter(
                country_id__in=countries,
                contract_type=ContractType.A01,
                datetime_utc__gte=window[0],
                datetime_utc__lt=window[1],
                price__isnull=False,
            )
            .order_by("country_id", "datetime_utc")
            .values("country_id", "datetime_utc", "price")
        )
        result_rows = [
            {
                "country": row["country_id"],
                "datetime_utc": _fmt_z(row["datetime_utc"]),
                "price_eur_mwh": round(float(row["price"]), 3),
            }
            for row in rows
        ]
    else:
        trunc_fn = {"d": TruncDay, "m": TruncMonth, "y": TruncYear}[resolution]
        rows = (
            CountryPricePoint.objects
            .filter(
                country_id__in=countries,
                contract_type=ContractType.A01,
                datetime_utc__gte=window[0],
                datetime_utc__lt=window[1],
                price__isnull=False,
            )
            .annotate(bucket=trunc_fn("datetime_utc", tzinfo=dt.timezone.utc))
            .values("country_id", "bucket")
            .annotate(avg_price=Avg("price"))
            .order_by("country_id", "bucket")
        )
        result_rows = [
            {
                "country": row["country_id"],
                "datetime_utc": _fmt_z(row["bucket"]),
                "price_eur_mwh": round(float(row["avg_price"]), 3),
            }
            for row in rows
            if row["avg_price"] is not None
        ]

    return _cap_rows(result_rows) | {
        "unit": "EUR/MWh",
        "dataset": "A44 day-ahead prices",
        "countries": countries,
        "resolution": resolution,
    }


def _exec_get_generation_mix(inputs: dict, ctx: dict) -> dict:
    countries, error = _validate_countries(inputs.get("countries"))
    if error:
        return {"error": error}
    window, error = _validate_window(inputs)
    if error:
        return {"error": error}

    resolution = str(inputs.get("resolution") or "native")
    error = _resolution_error(resolution)
    if error:
        return {"error": error}

    if resolution == "native":
        rows = (
            CountryGenerationByType.objects
            .filter(
                country_id__in=countries,
                datetime_utc__gte=window[0],
                datetime_utc__lt=window[1],
                generation_mw__isnull=False,
            )
            .order_by("country_id", "datetime_utc", "psr_type")
            .values("country_id", "datetime_utc", "psr_type", "psr_name", "generation_mw")
        )
        result_rows = [
            {
                "country": row["country_id"],
                "datetime_utc": _fmt_z(row["datetime_utc"]),
                "psr_type": row["psr_type"],
                "psr_name": row["psr_name"] or row["psr_type"],
                "generation_mw": round(float(row["generation_mw"]), 3),
            }
            for row in rows
        ]
    else:
        trunc_fn = {"d": TruncDay, "m": TruncMonth, "y": TruncYear}[resolution]
        rows = (
            CountryGenerationByType.objects
            .filter(
                country_id__in=countries,
                datetime_utc__gte=window[0],
                datetime_utc__lt=window[1],
                generation_mw__isnull=False,
            )
            .annotate(bucket=trunc_fn("datetime_utc", tzinfo=dt.timezone.utc))
            .values("country_id", "bucket", "psr_type")
            .annotate(avg_generation=Avg("generation_mw"))
            .order_by("country_id", "bucket", "psr_type")
        )
        result_rows = [
            {
                "country": row["country_id"],
                "datetime_utc": _fmt_z(row["bucket"]),
                "psr_type": row["psr_type"],
                "generation_mw": round(float(row["avg_generation"]), 3),
            }
            for row in rows
            if row["avg_generation"] is not None
        ]

    return _cap_rows(result_rows) | {
        "unit": "MW",
        "dataset": "A75 generation by production type",
        "countries": countries,
        "resolution": resolution,
    }


def _exec_get_capacity(inputs: dict, ctx: dict) -> dict:
    countries, error = _validate_countries(inputs.get("countries"))
    if error:
        return {"error": error}
    year = inputs.get("year")
    if not isinstance(year, int) or year < 2015:
        return {"error": "year must be an integer >= 2015."}

    rows = (
        CountryCapacitySnapshot.objects
        .filter(country_id__in=countries, year=year)
        .order_by("country_id", "psr_name", "psr_type")
        .values("country_id", "psr_type", "psr_name", "installed_capacity_mw")
    )
    result_rows = [
        {
            "country": row["country_id"],
            "psr_type": row["psr_type"],
            "psr_name": row["psr_name"] or row["psr_type"],
            "installed_capacity_mw": (
                round(float(row["installed_capacity_mw"]), 3)
                if row["installed_capacity_mw"] is not None
                else None
            ),
        }
        for row in rows
    ]

    return _cap_rows(result_rows) | {
        "unit": "MW",
        "dataset": "A68 installed capacity",
        "countries": countries,
        "year": year,
    }


def _exec_get_flows(inputs: dict, ctx: dict) -> dict:
    countries, error = _validate_countries([inputs.get("country_from"), inputs.get("country_to")])
    if error:
        return {"error": error}
    window, error = _validate_window(inputs)
    if error:
        return {"error": error}

    resolution = str(inputs.get("resolution") or "native")
    error = _resolution_error(resolution)
    if error:
        return {"error": error}

    if resolution == "native":
        rows = (
            PhysicalFlow.objects
            .filter(
                country_from_id=countries[0],
                country_to_id=countries[1],
                datetime_utc__gte=window[0],
                datetime_utc__lt=window[1],
            )
            .order_by("datetime_utc")
            .values("datetime_utc", "quantity_mw")
        )
        result_rows = [
            {
                "country_from": countries[0],
                "country_to": countries[1],
                "datetime_utc": _fmt_z(row["datetime_utc"]),
                "quantity_mw": round(float(row["quantity_mw"]), 3),
            }
            for row in rows
        ]
    else:
        trunc_fn = {"d": TruncDay, "m": TruncMonth, "y": TruncYear}[resolution]
        rows = (
            PhysicalFlow.objects
            .filter(
                country_from_id=countries[0],
                country_to_id=countries[1],
                datetime_utc__gte=window[0],
                datetime_utc__lt=window[1],
            )
            .annotate(bucket=trunc_fn("datetime_utc", tzinfo=dt.timezone.utc))
            .values("bucket")
            .annotate(avg_quantity=Avg("quantity_mw"))
            .order_by("bucket")
        )
        result_rows = [
            {
                "country_from": countries[0],
                "country_to": countries[1],
                "datetime_utc": _fmt_z(row["bucket"]),
                "quantity_mw": round(float(row["avg_quantity"]), 3),
            }
            for row in rows
            if row["avg_quantity"] is not None
        ]

    return _cap_rows(result_rows) | {
        "unit": "MW",
        "dataset": "A11 cross-border physical flows",
        "country_from": countries[0],
        "country_to": countries[1],
        "resolution": resolution,
    }


def _exec_render_chart(inputs: dict, ctx: dict) -> dict:
    data_type = inputs.get("data_type")
    if data_type not in {"generation_res", "generation", "prices", "capacity", "flows"}:
        return {
            "error": "data_type must be one of: generation_res, generation, prices, capacity, flows."
        }

    chart_type = str(inputs.get("chart_type") or "line")
    if chart_type not in {"line", "bar"}:
        return {"error": "chart_type must be 'line' or 'bar'."}

    resolution = str(inputs.get("resolution") or "native")
    error = _resolution_error(resolution)
    if error:
        return {"error": error}

    countries_input = inputs.get("countries")
    if data_type == "flows" and not countries_input:
        countries_input = [inputs.get("country_from"), inputs.get("country_to")]
    countries, error = _validate_countries(countries_input)
    if error:
        return {"error": error}

    window, error = _validate_window(inputs)
    if error:
        return {"error": error}

    spec = {
        "title": str(inputs.get("title") or "").strip() or None,
        "data_type": data_type,
        "countries": countries,
        "series": [
            series_key
            for series_key in (inputs.get("series") or [])
            if series_key in CHART_GENERATION_SERIES
        ],
        "include_prices": bool(inputs.get("include_prices", False)),
        "start_utc": _fmt_z(window[0]),
        "end_utc": _fmt_z(window[1]),
        "resolution": resolution,
        "chart_type": chart_type,
    }
    if data_type == "flows":
        spec["country_from"] = str(inputs.get("country_from") or countries[0]).strip().upper()
        spec["country_to"] = str(inputs.get("country_to") or countries[-1]).strip().upper()

    ctx.setdefault("charts", []).append(spec)
    return {"status": "chart_queued", "spec": spec}


_WINDOW_PROPS = {
    "start_utc": {"type": "string", "description": "ISO 8601 UTC start, e.g. 2026-07-01T00:00:00Z"},
    "end_utc": {"type": "string", "description": "ISO 8601 UTC end (exclusive)"},
}
_COUNTRIES_PROP = {
    "countries": {
        "type": "array",
        "items": {"type": "string"},
        "description": "2-letter ISO country codes, e.g. ['DE', 'FR']",
    }
}
_RESOLUTION_PROP = {
    "resolution": {
        "type": "string",
        "enum": ["native", "d", "m", "y"],
        "description": (
            "native = hourly/raw, d = daily, m = monthly, y = yearly. "
            "Use d or coarser for windows longer than ~28 days."
        ),
    }
}

TOOLS: dict[str, dict] = {
    "get_res_generation": {
        "schema": {
            "name": "get_res_generation",
            "description": (
                "Actual renewable (solar/wind) generation in MW from ENTSO-E A69. "
                "Returns aggregated rows for analysis. For a visualization, "
                "use render_chart instead of reading raw rows."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    **_COUNTRIES_PROP,
                    **_WINDOW_PROPS,
                    **_RESOLUTION_PROP,
                    "series": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["res", "solar", "wind"]},
                        "description": "res = solar + wind combined.",
                    },
                },
                "required": ["countries", "start_utc", "end_utc"],
            },
        },
        "handler": _exec_get_res_generation,
    },
    "get_day_ahead_prices": {
        "schema": {
            "name": "get_day_ahead_prices",
            "description": "Day-ahead electricity prices in EUR/MWh from ENTSO-E A44.",
            "input_schema": {
                "type": "object",
                "properties": {**_COUNTRIES_PROP, **_WINDOW_PROPS, **_RESOLUTION_PROP},
                "required": ["countries", "start_utc", "end_utc"],
            },
        },
        "handler": _exec_get_prices,
    },
    "get_generation_mix": {
        "schema": {
            "name": "get_generation_mix",
            "description": (
                "Actual generation by all production types (nuclear, gas, hydro, ...) "
                "in MW from ENTSO-E A75."
            ),
            "input_schema": {
                "type": "object",
                "properties": {**_COUNTRIES_PROP, **_WINDOW_PROPS, **_RESOLUTION_PROP},
                "required": ["countries", "start_utc", "end_utc"],
            },
        },
        "handler": _exec_get_generation_mix,
    },
    "get_installed_capacity": {
        "schema": {
            "name": "get_installed_capacity",
            "description": (
                "Annual installed generation capacity snapshot by production type in MW "
                "from ENTSO-E A68."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    **_COUNTRIES_PROP,
                    "year": {"type": "integer", "description": "Snapshot year, e.g. 2025"},
                },
                "required": ["countries", "year"],
            },
        },
        "handler": _exec_get_capacity,
    },
    "get_cross_border_flows": {
        "schema": {
            "name": "get_cross_border_flows",
            "description": (
                "Cross-border physical power flows in MW between two countries from ENTSO-E A11."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "country_from": {"type": "string", "description": "Source ISO code"},
                    "country_to": {"type": "string", "description": "Destination ISO code"},
                    **_WINDOW_PROPS,
                    **_RESOLUTION_PROP,
                },
                "required": ["country_from", "country_to", "start_utc", "end_utc"],
            },
        },
        "handler": _exec_get_flows,
    },
    "render_chart": {
        "schema": {
            "name": "render_chart",
            "description": (
                "Queue a time-series chart for the user. The backend fetches the full-resolution "
                "data server-side; you only provide the spec. Call this whenever the user wants "
                "to see data (show/plot/compare). You may queue multiple charts in one turn."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "data_type": {
                        "type": "string",
                        "enum": ["generation_res", "generation", "prices", "capacity", "flows"],
                    },
                    **_COUNTRIES_PROP,
                    "series": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["res", "solar", "wind"]},
                    },
                    "include_prices": {"type": "boolean"},
                    **_WINDOW_PROPS,
                    **_RESOLUTION_PROP,
                    "chart_type": {"type": "string", "enum": ["line", "bar"]},
                    "country_from": {"type": "string"},
                    "country_to": {"type": "string"},
                },
                "required": ["data_type", "countries", "start_utc", "end_utc"],
            },
        },
        "handler": _exec_render_chart,
    },
}

TOOL_SCHEMAS: list[dict] = [entry["schema"] for entry in TOOLS.values()]


def execute_tool(name: str, inputs: dict, ctx: dict) -> str:
    entry = TOOLS.get(name)
    if entry is None:
        return json.dumps({"error": f"Unknown tool '{name}'."})

    try:
        result = entry["handler"](inputs if isinstance(inputs, dict) else {}, ctx)
    except Exception as exc:
        result = {"error": f"Tool '{name}' failed: {exc}"}

    return json.dumps(result, default=str)
