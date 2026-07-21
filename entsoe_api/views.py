# entsoe_api/views_readonly.py
from __future__ import annotations

import datetime as dt
import logging
import time
from collections import defaultdict
from typing import Iterable, Tuple, Dict, List
from urllib.parse import urlencode

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.utils.encoding import force_str
from django.utils.http import urlsafe_base64_decode
from django.utils.dateparse import parse_date, parse_datetime
from django.utils.decorators import method_decorator
from django.utils.timezone import get_default_timezone
from django.views.decorators.cache import cache_page
from django.shortcuts import redirect
from django.db.models import Avg, Count, Max, Min, Q
from django.db.models.functions import TruncDay, TruncMonth, TruncYear
from drf_spectacular.utils import OpenApiExample, extend_schema
from rest_framework.views import APIView
from rest_framework.decorators import api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from rest_framework.reverse import reverse
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenRefreshView

from .models import (
    Country,
    CountryCapacitySnapshot,
    CountryGenerationByType,
    CountryResGenerationByType,
    CountryGenerationForecastByType,
    CountryTiltedIrradiancePoint,
    CountryWindSpeedPoint,
    CountryPricePoint,
    PhysicalFlow,
)
from entsoe_api.serializers import (
    AuthUserSerializer,
    CountryGenerationForecastByTypeSerializer,
    CountryResGenerationByTypeSerializer,
    CountryTiltedIrradiancePointSerializer,
    LoginSerializer,
    CountryWindSpeedPointSerializer,
    PhysicalFlowSerializer,
    RegisterSerializer,
)
from .api_docs import (
    AccessTokenResponseSerializer,
    ActivationPendingResponseSerializer,
    AuthSuccessResponseSerializer,
    AuthUserResponseSerializer,
    AZIMUTH_PARAMETER,
    ApiRootResponseSerializer,
    BAD_REQUEST_RESPONSE,
    CapacityBulkResponseSerializer,
    CapacityLatestResponseSerializer,
    ChartQueryRequestSerializer,
    ChartQueryResponseSerializer,
    CONTRACT_PARAMETER,
    countries_parameter,
    country_parameter,
    END_PARAMETER,
    FLOW_FROM_PARAMETER,
    FLOW_TO_PARAMETER,
    GenerationBulkResponseSerializer,
    GenerationForecastResponseSerializer,
    GenerationRangeResponseSerializer,
    INVALID_COUNTRY_EXAMPLE,
    INVALID_RANGE_EXAMPLE,
    LoginRequestSerializer,
    LOCAL_PARAMETER,
    MONTHLY_FLAG_PARAMETER,
    NEIGHBORS_PARAMETER,
    period_parameter,
    PhysicalFlowsLatestResponseSerializer,
    PhysicalFlowsRangeResponseSerializer,
    PriceBulkResponseSerializer,
    PriceRangeResponseSerializer,
    psr_parameter,
    RefreshRequestSerializer,
    RegisterRequestSerializer,
    RESOLUTION_PARAMETER,
    ResGenerationResponseSerializer,
    START_PARAMETER,
    TILT_PARAMETER,
    TiltedIrradianceBulkResponseSerializer,
    TiltedIrradianceResponseSerializer,
    WindSpeedBulkResponseSerializer,
    WindSpeedResponseSerializer,
)
from .agent import run_energy_agent
from .conversation import append_turn, generate_conversation_id, load_history
from .email_activation import activation_token_generator, send_activation_email

logger = logging.getLogger(__name__)
User = get_user_model()


# ──────────────────────────────── Utils / Helpers ───────────────────────────────

def _ensure_utc(d: dt.datetime) -> dt.datetime:
    """Return datetime with tzinfo=UTC and converted to UTC."""
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc)

def _fmt_z(d: dt.datetime) -> str:
    """ISO8601 with trailing 'Z'."""
    return _ensure_utc(d).strftime("%Y-%m-%dT%H:%M:%SZ")

def _utc_floor_hour(d: dt.datetime) -> dt.datetime:
    d = _ensure_utc(d)
    return d.replace(minute=0, second=0, microsecond=0)

def _floor_15min(d: dt.datetime) -> dt.datetime:
    d = _ensure_utc(d).replace(second=0, microsecond=0)
    return d - dt.timedelta(minutes=d.minute % 15)

def _parse_iso_utc_floor_hour(s: str) -> dt.datetime:
    """Parse ISO datetime/date and floor to hour in UTC."""
    raw_value = (s or "").strip()

    if not raw_value:
        raise ValueError("Datetime value cannot be empty.")

    d = parse_datetime(raw_value)
    if d is None and raw_value.endswith("Z"):
        # Keep compatibility with clients that send UTC values with trailing Z.
        d = parse_datetime(raw_value.replace("Z", "+00:00"))

    if d is None:
        parsed_date = parse_date(raw_value)
        if parsed_date is not None:
            d = dt.datetime.combine(parsed_date, dt.time.min, tzinfo=dt.timezone.utc)

    if d is None:
        raise ValueError(
            f"Invalid datetime '{raw_value}'. Use ISO datetime (e.g. 2026-02-11T22:00:00Z) or date (YYYY-MM-DD)."
        )

    return _utc_floor_hour(d)

def _bool_param(request, key: str) -> bool:
    return (request.query_params.get(key, "") or "").lower() in ("1", "y", "yes", "true")

def _split_codes(s: str) -> List[str]:
    return [c.strip().upper() for c in (s or "").split(",") if c.strip()]

def _all_country_codes() -> set:
    """Return the set of all known country ISO codes, cached for 1 hour."""
    codes = cache.get("all_country_codes")
    if codes is None:
        codes = set(Country.objects.values_list("pk", flat=True))
        cache.set("all_country_codes", codes, 3600)
    return codes

def _get_country_or_400(country_iso: str) -> Country:
    iso = (country_iso or "").upper()
    if iso not in _all_country_codes():
        raise ValueError(f"Unknown country '{iso}'. Make sure it's loaded in the DB.")
    return Country(pk=iso)

def _partition_country_codes(codes: Iterable[str]) -> Tuple[List[str], List[str]]:
    requested_codes = sorted({c.upper() for c in codes if c})
    known = _all_country_codes()
    valid = sorted(c for c in requested_codes if c in known)
    missing = sorted(set(requested_codes) - known)
    return valid, missing


def _validate_countries_or_400(codes: Iterable[str]) -> List[str]:
    valid, missing = _partition_country_codes(codes)
    if missing:
        raise ValueError(f"Unknown countries: {', '.join(missing)}")
    return valid


def _configured_country_coords_codes() -> List[str]:
    raw_coords = getattr(settings, "COUNTRY_COORDS", None)
    if not isinstance(raw_coords, list):
        return []

    return sorted({
        str(item.get("code") or "").upper().strip()
        for item in raw_coords
        if isinstance(item, dict) and str(item.get("code") or "").strip()
    })


def _optional_float_param(request, key: str) -> float | None:
    raw_value = request.query_params.get(key)
    if raw_value is None or str(raw_value).strip() == "":
        return None
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a number.")

def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)


CHART_GENERATION_SERIES = {
    "res": {"label": "RES (solar + wind)", "psr_types": {"B16", "B18", "B19"}},
    "solar": {"label": "Solar", "psr_types": {"B16"}},
    "wind": {"label": "Wind", "psr_types": {"B18", "B19"}},
}


def _chart_bucket_start(timestamp: dt.datetime, resolution: str) -> dt.datetime:
    timestamp = _ensure_utc(timestamp)
    if resolution == "d":
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    if resolution == "m":
        return timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if resolution == "y":
        return timestamp.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return timestamp


def _chart_average(values: List[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _build_generation_chart_panel(query: ParsedChartQuery) -> dict:
    requested_psr_types = sorted(
        {
            psr_type
            for series_key in query.generation_series
            for psr_type in CHART_GENERATION_SERIES[series_key]["psr_types"]
        }
    )
    rows = (
        CountryResGenerationByType.objects
        .filter(
            country_id__in=query.countries,
            datetime_utc__gte=query.start_utc,
            datetime_utc__lt=query.end_utc,
            psr_type__in=requested_psr_types,
        )
        .order_by("datetime_utc", "psr_type")
        .values("country_id", "datetime_utc", "psr_type", "generation_mw")
    )

    timestamp_totals: dict[tuple[str, str, dt.datetime], float] = defaultdict(float)
    for row in rows:
        generation_value = row["generation_mw"]
        if generation_value is None:
            continue
        timestamp = _ensure_utc(row["datetime_utc"])
        for series_key in query.generation_series:
            if row["psr_type"] not in CHART_GENERATION_SERIES[series_key]["psr_types"]:
                continue
            timestamp_totals[(row["country_id"], series_key, timestamp)] += float(generation_value)

    grouped_values: dict[tuple[str, str, dt.datetime], List[float]] = defaultdict(list)
    for (country_code, series_key, timestamp), total_value in timestamp_totals.items():
        bucket = _chart_bucket_start(timestamp, query.resolution)
        grouped_values[(country_code, series_key, bucket)].append(total_value)

    if len(query.countries) == 1:
        series_points: dict[str, List[dict]] = {series_key: [] for series_key in query.generation_series}
        series_order = {series_key: index for index, series_key in enumerate(query.generation_series)}
        for (_, series_key, bucket), values in sorted(grouped_values.items(), key=lambda item: (series_order[item[0][1]], item[0][2])):
            series_points[series_key].append({
                "datetime_utc": _fmt_z(bucket),
                "value": _chart_average(values),
            })

        return {
            "id": "generation",
            "title": f"{query.country} renewable generation",
            "type": query.chart_type,
            "x_key": "datetime_utc",
            "unit": "MW",
            "series": [
                {
                    "id": series_key,
                    "name": CHART_GENERATION_SERIES[series_key]["label"],
                    "unit": "MW",
                    "data": series_points[series_key],
                }
                for series_key in query.generation_series
            ],
        }

    series_points_multi: dict[tuple[str, str], List[dict]] = {
        (country_code, series_key): []
        for country_code in query.countries
        for series_key in query.generation_series
    }
    country_order = {country_code: index for index, country_code in enumerate(query.countries)}
    series_order = {series_key: index for index, series_key in enumerate(query.generation_series)}
    for (country_code, series_key, bucket), values in sorted(
        grouped_values.items(),
        key=lambda item: (country_order[item[0][0]], series_order[item[0][1]], item[0][2]),
    ):
        series_points_multi[(country_code, series_key)].append({
            "datetime_utc": _fmt_z(bucket),
            "value": _chart_average(values),
        })

    return {
        "id": "generation",
        "title": f"{' vs '.join(query.countries)} renewable generation",
        "type": query.chart_type,
        "x_key": "datetime_utc",
        "unit": "MW",
        "series": [
            {
                "id": f"{country_code.lower()}_{series_key}",
                "name": f"{country_code} {CHART_GENERATION_SERIES[series_key]['label']}",
                "unit": "MW",
                "data": series_points_multi[(country_code, series_key)],
            }
            for country_code in query.countries
            for series_key in query.generation_series
        ],
    }


def _build_price_chart_panel(query: ParsedChartQuery) -> dict:
    rows = (
        CountryPricePoint.objects
        .filter(
            country_id__in=query.countries,
            contract_type="A01",
            datetime_utc__gte=query.start_utc,
            datetime_utc__lt=query.end_utc,
        )
        .order_by("datetime_utc")
        .values("country_id", "datetime_utc", "price")
    )

    grouped_values: dict[tuple[str, dt.datetime], List[float]] = defaultdict(list)
    for row in rows:
        price = row["price"]
        if price is None:
            continue
        bucket = _chart_bucket_start(row["datetime_utc"], query.resolution)
        grouped_values[(row["country_id"], bucket)].append(float(price))

    if len(query.countries) == 1:
        data = [
            {
                "datetime_utc": _fmt_z(bucket),
                "value": _chart_average(values),
            }
            for (_, bucket), values in sorted(grouped_values.items(), key=lambda item: item[0][1])
        ]

        return {
            "id": "prices",
            "title": f"{query.country} day-ahead prices",
            "type": query.chart_type,
            "x_key": "datetime_utc",
            "unit": "EUR/MWh",
            "series": [
                {
                    "id": "price",
                    "name": "Day-ahead price",
                    "unit": "EUR/MWh",
                    "data": data,
                }
            ],
        }

    series_points: dict[str, List[dict]] = {country_code: [] for country_code in query.countries}
    country_order = {country_code: index for index, country_code in enumerate(query.countries)}
    for (country_code, bucket), values in sorted(grouped_values.items(), key=lambda item: (country_order[item[0][0]], item[0][1])):
        series_points[country_code].append({
            "datetime_utc": _fmt_z(bucket),
            "value": _chart_average(values),
        })

    return {
        "id": "prices",
        "title": f"{' vs '.join(query.countries)} day-ahead prices",
        "type": query.chart_type,
        "x_key": "datetime_utc",
        "unit": "EUR/MWh",
        "series": [
            {
                "id": country_code.lower(),
                "name": country_code,
                "unit": "EUR/MWh",
                "data": series_points[country_code],
            }
            for country_code in query.countries
        ],
    }


def _fetch_price_data_for_analysis(query: ParsedDataQuery) -> list[dict]:
    """Daily-aggregated price rows passed to Claude for open-ended analysis."""
    rows = (
        CountryPricePoint.objects
        .filter(
            country_id__in=query.countries,
            contract_type="A01",
            datetime_utc__gte=query.start_utc,
            datetime_utc__lt=query.end_utc,
            price__isnull=False,
        )
        .annotate(day=TruncDay("datetime_utc"))
        .values("country_id", "day")
        .annotate(
            avg_price=Avg("price"),
            max_price=Max("price"),
            min_price=Min("price"),
            hour_count=Count("price"),
        )
        .order_by("country_id", "day")
    )
    return [
        {
            "country": r["country_id"],
            "date": r["day"].strftime("%Y-%m-%d"),
            "avg_eur_mwh": round(float(r["avg_price"]), 2) if r["avg_price"] is not None else None,
            "max_eur_mwh": round(float(r["max_price"]), 2) if r["max_price"] is not None else None,
            "min_eur_mwh": round(float(r["min_price"]), 2) if r["min_price"] is not None else None,
            "hour_count": r["hour_count"],
        }
        for r in rows
    ]


def _fetch_generation_data_for_analysis(query: ParsedDataQuery) -> list[dict]:
    """Daily-aggregated RES generation rows passed to Claude for open-ended analysis."""
    requested_psr_types = sorted(
        {pt for s in query.generation_series for pt in CHART_GENERATION_SERIES[s]["psr_types"]}
    )
    rows = (
        CountryResGenerationByType.objects
        .filter(
            country_id__in=query.countries,
            datetime_utc__gte=query.start_utc,
            datetime_utc__lt=query.end_utc,
            psr_type__in=requested_psr_types,
            generation_mw__isnull=False,
        )
        .values("country_id", "datetime_utc", "psr_type", "generation_mw")
        .order_by("country_id", "datetime_utc")
    )
    # Sum psr_types per timestamp (same approach as chart panel builder)
    ts_by_country: dict[str, dict] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        ts_by_country[row["country_id"]][row["datetime_utc"]] += float(row["generation_mw"])

    result = []
    for country_code in query.countries:
        day_buckets: dict[str, list[float]] = defaultdict(list)
        for ts, val in ts_by_country.get(country_code, {}).items():
            day_buckets[ts.strftime("%Y-%m-%d")].append(val)
        for day in sorted(day_buckets):
            vals = day_buckets[day]
            result.append({
                "country": country_code,
                "date": day,
                "avg_mw": round(sum(vals) / len(vals), 2),
                "max_mw": round(max(vals), 2),
                "min_mw": round(min(vals), 2),
                "hour_count": len(vals),
            })
    return result


def _fetch_capacity_data_for_analysis(data_query: "ParsedDataQuery") -> list[dict]:
    """Installed capacity snapshot rows passed to Claude for analysis."""
    year = data_query.start_utc.year
    rows = (
        CountryCapacitySnapshot.objects
        .filter(country_id__in=data_query.countries, year=year)
        .order_by("country_id", "psr_name")
        .values("country_id", "psr_type", "psr_name", "installed_capacity_mw")
    )
    return [
        {
            "country": r["country_id"],
            "psr_type": r["psr_type"],
            "psr_name": r["psr_name"],
            "installed_capacity_mw": float(r["installed_capacity_mw"]) if r["installed_capacity_mw"] is not None else None,
        }
        for r in rows
    ]


def _describe_chart_query(query: ParsedChartQuery) -> str:
    if query.include_prices and query.generation_series:
        metric_text = "generation and prices"
    elif query.include_prices:
        metric_text = "day-ahead prices"
    elif query.generation_series == ["res"]:
        metric_text = "RES generation"
    else:
        metric_labels = {
            "solar": "solar generation",
            "wind": "wind generation",
            "res": "RES generation",
        }
        metric_text = " and ".join(metric_labels.get(series_key, series_key) for series_key in query.generation_series)

    country_text = " and ".join(query.countries)
    chart_label = "bar chart" if query.chart_type == "bar" else "line chart"
    return f"Showing {metric_text} for {country_text} for {query.time_phrase} as a {chart_label}."

def _compute_window_utc(
    period: str | None,
    start_s: str | None,
    end_s: str | None,
    *,
    allow_yesterday: bool = True,
    use_local_for_yesterday: bool = False,
    align_to_15min: bool = False,
) -> Tuple[dt.datetime, dt.datetime, str]:
    """
    Compute [start_utc, end_utc) and a label.
    Supported period values:
      - 'today'     -> [00:00, 24:00) today (UTC)
      - 'dayahead'  -> [00:00 tomorrow, 24:00 tomorrow) (UTC)
      - 'yesterday' -> [00:00 yesterday, 24:00 yesterday) (UTC or local if requested)
    Else requires explicit start & end (ISO). Floors to hour unless align_to_15min=True.
    """
    now = _now_utc()
    today_utc = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if period:
        p = period.lower()
        if p == "today":
            start, end, label = today_utc, today_utc + dt.timedelta(days=1), "today (UTC)"
        elif p == "dayahead":
            start = today_utc + dt.timedelta(days=1)
            end = start + dt.timedelta(days=1)
            label = "dayahead (UTC)"
        elif allow_yesterday and p == "yesterday":
            if use_local_for_yesterday:
                tz = get_default_timezone()
                today_local = dt.datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
                start_local = today_local - dt.timedelta(days=1)
                end_local = today_local
                start = _ensure_utc(start_local)
                end = _ensure_utc(end_local)
                label = f"yesterday ({settings.TIME_ZONE})"
            else:
                start = today_utc - dt.timedelta(days=1)
                end = today_utc
                label = "yesterday (UTC)"
        else:
            # Unknown keyword → fall through to explicit handling
            start = end = None
            label = f"{period} (UTC)"
        if start and end:
            if align_to_15min:
                start = _floor_15min(start)
                end = _floor_15min(end)
            else:
                start = _utc_floor_hour(start)
                end = _utc_floor_hour(end)
            return start, end, label

    # Explicit range
    if not start_s or not end_s:
        raise ValueError(
            "Provide start & end (ISO UTC, e.g. 2025-09-18T00:00:00Z) "
            "or use period=today|yesterday|dayahead."
        )
    start = _parse_iso_utc_floor_hour(start_s)
    end = _parse_iso_utc_floor_hour(end_s)
    if align_to_15min:
        start = _floor_15min(start)
        end = _floor_15min(end)
    return start, end, "custom range"


# ── PhysicalFlow field mapping (matches your model) ─────────────────────────────
PHYSICAL_FLOW_SRC_FIELD = "country_from_id"     # sending domain
PHYSICAL_FLOW_DST_FIELD = "country_to_id"      # receiving domain
PHYSICAL_FLOW_TS_FIELD  = "datetime_utc"       # timestamp of the flow
PHYSICAL_FLOW_MW_FIELD  = "quantity_mw"        # MW value

def _flow_field_names() -> Tuple[str, str, str]:
    return (PHYSICAL_FLOW_SRC_FIELD, PHYSICAL_FLOW_DST_FIELD, PHYSICAL_FLOW_TS_FIELD)


# ───────────────────────────────── API Root ────────────────────────────────────

@extend_schema(
    tags=["Meta"],
    summary="List API entrypoints",
    description="Returns the main data endpoints together with links to the OpenAPI schema, Swagger UI, and ReDoc.",
    responses={200: ApiRootResponseSerializer},
    examples=[
        OpenApiExample(
            "API root response",
            response_only=True,
            value={
                "auth_register": "https://api.example.com/api/auth/register/",
                "auth_login": "https://api.example.com/api/auth/login/",
                "auth_refresh": "https://api.example.com/api/auth/refresh/",
                "auth_me": "https://api.example.com/api/auth/me/",
                "capacity_latest": "https://api.example.com/api/capacity/latest/",
                "capacity_bulk_latest": "https://api.example.com/api/capacity/bulk-latest/",
                "generation_yesterday": "https://api.example.com/api/generation/yesterday/",
                "chart_query": "https://api.example.com/api/chart-query/",
                "prices_range": "https://api.example.com/api/prices/range/",
                "price_bulk": "https://api.example.com/api/prices/bulk-range/",
                "generation_range": "https://api.example.com/api/generation/range/",
                "generation_res_range": "https://api.example.com/api/generation-res/range/",
                "generation_bulk_range": "https://api.example.com/api/generation/bulk-range/",
                "generation_forecast_range": "https://api.example.com/api/generation-forecast/range/",
                "generation_irradiance_range": "https://api.example.com/api/generation-irradiance/range/",
                "generation_irradiance_bulk_range": "https://api.example.com/api/generation-irradiance/bulk-range/",
                "generation_wind_speed_range": "https://api.example.com/api/generation-wind-speed/range/",
                "generation_wind_speed_bulk_range": "https://api.example.com/api/generation-wind-speed/bulk-range/",
                "flows_range": "https://api.example.com/api/flows/range/",
                "flows_latest": "https://api.example.com/api/flows/latest/",
                "schema": "https://api.example.com/api/schema/",
                "swagger_ui": "https://api.example.com/api/docs/swagger/",
                "redoc": "https://api.example.com/api/docs/redoc/",
            },
        )
    ],
)
@api_view(["GET"])
def api_root(request, format=None):
    return Response({
        "auth_register": request.build_absolute_uri(reverse("auth-register")),
        "auth_login": request.build_absolute_uri(reverse("auth-login")),
        "auth_refresh": request.build_absolute_uri(reverse("token-refresh")),
        "auth_me": request.build_absolute_uri(reverse("auth-me")),
        "capacity_latest": request.build_absolute_uri(reverse("capacity-latest")),
        "capacity_bulk_latest": request.build_absolute_uri(reverse("capacity-bulk-latest")),
        "generation_yesterday": request.build_absolute_uri(reverse("generation-yesterday")),
        "chart_query": request.build_absolute_uri(reverse("chart-query")),
        "prices_range": request.build_absolute_uri(reverse("prices-range")),
        "price_bulk": request.build_absolute_uri(reverse("country-prices-bulk-range")),
        "generation_range": request.build_absolute_uri(reverse("generation-range")),
        "generation_res_range": request.build_absolute_uri(reverse("generation-res-range")),
        "generation_bulk_range": request.build_absolute_uri(reverse("generation-bulk-range")),
        "generation_forecast_range": request.build_absolute_uri(reverse("generation-forecast-range")),
        "generation_irradiance_range": request.build_absolute_uri(reverse("generation-irradiance-range")),
        "generation_irradiance_bulk_range": request.build_absolute_uri(reverse("generation-irradiance-bulk-range")),
        "generation_wind_speed_range": request.build_absolute_uri(reverse("generation-wind-speed-range")),
        "generation_wind_speed_bulk_range": request.build_absolute_uri(reverse("generation-wind-speed-bulk-range")),
        "flows_range": request.build_absolute_uri(reverse("flows-range")),
        "flows_latest": request.build_absolute_uri(reverse("flows-latest")),
        "schema": request.build_absolute_uri(reverse("schema")),
        "swagger_ui": request.build_absolute_uri(reverse("swagger-ui")),
        "redoc": request.build_absolute_uri(reverse("redoc")),
    })


def _build_auth_response(user: User) -> dict:
    refresh = RefreshToken.for_user(user)
    return {
        "user": AuthUserSerializer(user).data,
        "access": str(refresh.access_token),
        "refresh": str(refresh),
    }


class RegisterView(APIView):
    @extend_schema(
        tags=["Meta"],
        summary="Register a user account",
        description="Creates an inactive user and sends a one-time activation link by email.",
        request=RegisterRequestSerializer,
        responses={
            201: ActivationPendingResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
            503: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Register request",
                request_only=True,
                value={
                    "email": "user@example.com",
                    "password": "StrongPassword123!",
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                },
            ),
        ],
    )
    def post(self, request):
        serializer = RegisterSerializer(data=request.data)
        serializer.is_valid(raise_exception=False)
        if serializer.errors:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        user = serializer.save()
        try:
            send_activation_email(user)
        except Exception:
            logger.exception("Failed to send activation email for user %s", user.pk)
            user.delete()
            return Response(
                {"detail": "We could not send the activation email. Please try again."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return Response(
            {"detail": "Check your email to activate your account."},
            status=status.HTTP_201_CREATED,
        )


class LoginView(APIView):
    @extend_schema(
        tags=["Meta"],
        summary="Log in with email and password",
        description="Authenticates a user and returns a JWT access/refresh token pair.",
        request=LoginRequestSerializer,
        responses={
            200: AuthSuccessResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Login request",
                request_only=True,
                value={
                    "email": "user@example.com",
                    "password": "StrongPassword123!",
                },
            ),
        ],
    )
    def post(self, request):
        serializer = LoginSerializer(data=request.data, context={"request": request})
        serializer.is_valid(raise_exception=False)
        if serializer.errors:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        return Response(_build_auth_response(serializer.validated_data["user"]))


class ActivateAccountView(APIView):
    """Activate an account from the one-time URL sent by email."""

    def get(self, request, uidb64, token):
        try:
            user = User.objects.get(pk=force_str(urlsafe_base64_decode(uidb64)))
        except (User.DoesNotExist, ValueError, TypeError, OverflowError):
            user = None

        if user is None or not activation_token_generator.check_token(user, token):
            return Response({"detail": "This activation link is invalid or has expired."}, status=status.HTTP_400_BAD_REQUEST)

        user.is_active = True
        user.save(update_fields=["is_active"])
        frontend_url = settings.FRONTEND_PUBLIC_URL.rstrip("/")
        return redirect(f"{frontend_url}/login?{urlencode({'activated': '1'})}")


class MeView(APIView):
    permission_classes = [IsAuthenticated]

    @extend_schema(
        tags=["Meta"],
        summary="Return the current authenticated user",
        description="Requires a Bearer access token.",
        responses={
            200: AuthUserResponseSerializer,
            401: BAD_REQUEST_RESPONSE,
        },
    )
    def get(self, request):
        return Response(AuthUserSerializer(request.user).data)


class AuthTokenRefreshView(TokenRefreshView):
    @extend_schema(
        tags=["Meta"],
        summary="Refresh an access token",
        description="Accepts a refresh token and returns a new access token.",
        request=RefreshRequestSerializer,
        responses={
            200: AccessTokenResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class EnergyAgentChatView(APIView):
    @extend_schema(
        tags=["Meta"],
        summary="Chat with the energy agent",
        description=(
            "Accepts a natural-language message, runs the multi-step energy agent, "
            "and returns either plain text or one or more chart specs."
        ),
        request=ChartQueryRequestSerializer,
        responses={
            200: ChartQueryResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
            502: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Chat request",
                value={
                    "message": "Compare RES generation for BG and RO for April. Daily resolution",
                },
                request_only=True,
            ),
            OpenApiExample(
                "Chart response",
                response_only=True,
                value={
                    "conversation_id": "7d4d9300-4a6f-44be-b59f-7c648cc44062",
                    "status": "chart",
                    "text": "Showing RES generation for BG and RO for April as a line chart.",
                    "charts": [
                        {
                            "title": "BG vs RO renewable generation",
                            "data_type": "generation_res",
                            "countries": ["BG", "RO"],
                            "series": ["res"],
                            "include_prices": False,
                            "start_utc": "2026-04-01T00:00:00Z",
                            "end_utc": "2026-04-30T00:00:00Z",
                            "resolution": "d",
                            "chart_type": "line",
                        }
                    ],
                },
            ),
            OpenApiExample(
                "Text response",
                response_only=True,
                value={
                    "conversation_id": "7d4d9300-4a6f-44be-b59f-7c648cc44062",
                    "status": "text",
                    "text": "Which country and what time range should I use?",
                    "charts": [],
                },
            ),
        ],
    )
    def post(self, request):
        serializer = ChartQueryRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=False)
        if serializer.errors:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        message = str(serializer.validated_data.get("message") or "").strip()
        if not message:
            return Response(
                {"error": "message is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        conversation_id = (
            str(serializer.validated_data.get("conversation_id") or "").strip()
            or generate_conversation_id()
        )
        history = load_history(conversation_id)

        try:
            result = run_energy_agent(
                message,
                history=history,
                now_utc=dt.datetime.now(dt.timezone.utc),
            )
        except ValueError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_502_BAD_GATEWAY)

        append_turn(conversation_id, result.new_messages)

        return Response(
            {
                "conversation_id": conversation_id,
                "status": result.status,
                "text": result.text,
                "charts": result.charts,
            }
        )


ChartQueryView = EnergyAgentChatView


# ─────────────────────────── Capacity (latest year) ────────────────────────────

@method_decorator(cache_page(3600), name="get")
class CountryCapacityLatestView(APIView):
    """
    GET /api/capacity/latest/?country=CZ[&psr=B16]
    """

    @extend_schema(
        tags=["Capacity"],
        summary="Latest capacity snapshot for one country",
        description="Returns the most recent installed-capacity snapshot available for a single country, optionally filtered by ENTSO-E production type.",
        parameters=[
            country_parameter(),
            psr_parameter(),
        ],
        responses={
            200: CapacityLatestResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Capacity snapshot",
                response_only=True,
                value={
                    "country": "BG",
                    "year": 2025,
                    "items": [
                        {"psr_type": "B16", "psr_name": "Solar", "installed_capacity_mw": "3021.000"},
                        {"psr_type": "B18", "psr_name": "Wind Offshore", "installed_capacity_mw": "245.500"},
                    ],
                },
            ),
            INVALID_COUNTRY_EXAMPLE,
        ],
    )
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr = request.query_params.get("psr")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        latest = (
            CountryCapacitySnapshot.objects
            .filter(country=country)
            .aggregate(max_year=Max("year"))
        )["max_year"]

        if latest is None:
            return Response({"country": country.pk, "year": None, "items": []}, status=200)

        qs = CountryCapacitySnapshot.objects.filter(country=country, year=latest)
        if psr:
            qs = qs.filter(psr_type=psr)

        items = list(
            qs.order_by("psr_type").values("psr_type", "psr_name", "installed_capacity_mw")
        )
        return Response({"country": country.pk, "year": int(latest), "items": items}, status=200)


@method_decorator(cache_page(3600), name="get")
class CountryCapacityBulkLatestView(APIView):
    """
    GET /api/capacity/bulk-latest/?countries=AT,DE,FR[&psr=B16]

    Returns the latest-year installed capacity snapshot for multiple countries
    in a single request.
    """
    MAX_COUNTRIES = 40

    @extend_schema(
        tags=["Capacity"],
        summary="Latest capacity snapshot for multiple countries",
        description="Returns the latest installed-capacity snapshot for each requested country. The `data` object is keyed by country code.",
        parameters=[
            countries_parameter(max_items=40),
            psr_parameter(),
        ],
        responses={
            200: CapacityBulkResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Bulk capacity snapshot",
                response_only=True,
                value={
                    "request_info": {
                        "countries_requested": ["BG", "RO", "XX"],
                        "countries_found": ["BG", "RO"],
                        "countries_ignored": ["XX"],
                        "psr": "B16",
                        "total_countries": 2,
                        "total_records": 2,
                        "server_elapsed_ms": 8.47,
                    },
                    "data": {
                        "BG": {
                            "country": "BG",
                            "year": 2025,
                            "items": [{"psr_type": "B16", "psr_name": "Solar", "installed_capacity_mw": "3021.000"}],
                        },
                        "RO": {
                            "country": "RO",
                            "year": 2025,
                            "items": [{"psr_type": "B16", "psr_name": "Solar", "installed_capacity_mw": "1885.400"}],
                        },
                    },
                },
            ),
            OpenApiExample(
                "Missing countries parameter",
                response_only=True,
                status_codes=["400"],
                value={"detail": "countries parameter is required"},
            ),
        ],
    )
    def get(self, request):
        start_perf = time.perf_counter()
        countries_param = request.query_params.get("countries", "")
        psr = request.query_params.get("psr")

        country_codes = _split_codes(countries_param)
        if not country_codes:
            return Response({"detail": "countries parameter is required"}, status=400)
        if len(country_codes) > self.MAX_COUNTRIES:
            return Response({"detail": f"Maximum {self.MAX_COUNTRIES} countries per request"}, status=400)

        valid_countries, missing_countries = _partition_country_codes(country_codes)
        if not valid_countries:
            return Response({"detail": f"Unknown countries: {', '.join(missing_countries)}"}, status=400)

        # Find the latest year per country in one query
        latest_years = (
            CountryCapacitySnapshot.objects
            .filter(country_id__in=valid_countries)
            .values("country_id")
            .annotate(max_year=Max("year"))
        )
        year_map = {row["country_id"]: row["max_year"] for row in latest_years}

        # Build a single query filtering each country by its latest year
        combined_q = Q()
        for cid, yr in year_map.items():
            combined_q |= Q(country_id=cid, year=yr)

        results: Dict[str, dict] = {
            code: {"country": code, "year": year_map.get(code), "items": []}
            for code in valid_countries
        }

        if combined_q:
            qs = CountryCapacitySnapshot.objects.filter(combined_q)
            if psr:
                qs = qs.filter(psr_type=psr)
            qs = qs.order_by("country_id", "psr_type")

            for row in qs.values("country_id", "psr_type", "psr_name", "installed_capacity_mw", "year"):
                cid = row["country_id"]
                results[cid]["year"] = row["year"]
                results[cid]["items"].append({
                    "psr_type": row["psr_type"],
                    "psr_name": row["psr_name"],
                    "installed_capacity_mw": row["installed_capacity_mw"],
                })

        elapsed_ms = (time.perf_counter() - start_perf) * 1000
        total_records = sum(len(v["items"]) for v in results.values())

        return Response({
            "request_info": {
                "countries_requested": country_codes,
                "countries_found": valid_countries,
                "countries_ignored": missing_countries,
                "psr": psr,
                "total_countries": len(results),
                "total_records": total_records,
                "server_elapsed_ms": round(elapsed_ms, 2),
            },
            "data": results,
        }, status=200)


# ───────────────────────── Generation (yesterday quick) ────────────────────────

class CountryGenerationYesterdayView(APIView):
    """
    GET /api/generation/yesterday/?country=CZ[&psr=B16][&local=1]
    """

    @extend_schema(
        tags=["Generation"],
        summary="Yesterday generation for one country",
        description="Returns actual generation points for the previous day. Set `local=true` to interpret `yesterday` in the configured Django timezone instead of UTC.",
        parameters=[
            country_parameter(),
            psr_parameter(),
            LOCAL_PARAMETER,
        ],
        responses={
            200: GenerationRangeResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Yesterday generation",
                response_only=True,
                value={
                    "country": "CZ",
                    "date_label": "yesterday (UTC)",
                    "start_utc": "2026-03-09T00:00:00Z",
                    "end_utc": "2026-03-10T00:00:00Z",
                    "items": [
                        {
                            "datetime_utc": "2026-03-09T00:00:00Z",
                            "psr_type": "B16",
                            "psr_name": "Solar",
                            "generation_mw": "0.000",
                        },
                        {
                            "datetime_utc": "2026-03-09T00:15:00Z",
                            "psr_type": "B18",
                            "psr_name": "Wind Offshore",
                            "generation_mw": "412.600",
                        },
                    ],
                },
            ),
            INVALID_COUNTRY_EXAMPLE,
        ],
    )
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr = request.query_params.get("psr")
        use_local = _bool_param(request, "local")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        start_utc, end_utc, label = _compute_window_utc(
            "yesterday", None, None,
            allow_yesterday=True,
            use_local_for_yesterday=use_local,
            align_to_15min=True,
        )

        qs = CountryGenerationByType.objects.filter(
            country=country,
            datetime_utc__gte=start_utc,
            datetime_utc__lt=end_utc,
        )
        if psr:
            qs = qs.filter(psr_type=psr)

        rows = qs.order_by("datetime_utc", "psr_type").values(
            "datetime_utc", "psr_type", "psr_name", "generation_mw"
        )

        items = [{
            "datetime_utc": _fmt_z(r["datetime_utc"]),
            "psr_type": r["psr_type"],
            "psr_name": r["psr_name"] or r["psr_type"],
            "generation_mw": r["generation_mw"],
        } for r in rows]

        return Response({
            "country": country.pk,
            "date_label": label,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "items": items,
        }, status=200)


# ─────────────────────────────── Prices (range) ────────────────────────────────

@method_decorator(cache_page(600), name="get")
class CountryPricesRangeView(APIView):
    """
    GET /api/prices/range/?country=AT&contract=A01&period=today
    GET /api/prices/range/?country=AT&contract=A01&period=dayahead
    GET /api/prices/range/?country=AT&contract=A01&start=...&end=...
    GET /api/prices/range/?country=AT&contract=A01&start=...&end=...&resolution=d
    GET /api/prices/range/?country=AT&contract=A01&start=...&end=...&resolution=m
    """

    @extend_schema(
        tags=["Prices"],
        summary="Price series for one country",
        description="Returns country price points for a UTC date window or shortcut period. Results can be aggregated by day, month, or year.",
        parameters=[
            country_parameter(),
            CONTRACT_PARAMETER,
            period_parameter(allow_yesterday=True, allow_dayahead=True),
            START_PARAMETER,
            END_PARAMETER,
            RESOLUTION_PARAMETER,
            MONTHLY_FLAG_PARAMETER,
        ],
        responses={
            200: PriceRangeResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Daily price aggregation",
                response_only=True,
                value={
                    "country": "AT",
                    "contract_type": "A01",
                    "start_utc": "2026-03-01T00:00:00Z",
                    "end_utc": "2026-03-03T00:00:00Z",
                    "items": [
                        {
                            "datetime_utc": "2026-03-01T00:00:00Z",
                            "price": "83.250000",
                            "currency": "EUR",
                            "unit": "MWH",
                            "resolution": "P1D",
                        }
                    ],
                },
            ),
            INVALID_RANGE_EXAMPLE,
        ],
    )
    def get(self, request):
        iso = request.query_params.get("country", "")
        contract = (request.query_params.get("contract") or "A01").upper()
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")
        resolution = request.query_params.get("resolution")
        normalized_resolution = (resolution or "").lower()
        aggregate_daily = normalized_resolution == "d"
        aggregate_monthly = normalized_resolution == "m" or _bool_param(request, "m")
        aggregate_yearly = normalized_resolution == "y"

        try:
            country = _get_country_or_400(iso)
            start_utc, end_utc, _ = _compute_window_utc(period, start_s, end_s)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if period and period.lower() == "dayahead" and contract != "A01":
            return Response({"detail": "period=dayahead is only valid with contract=A01 (Day-ahead)."}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        base_qs = CountryPricePoint.objects.filter(
            country=country,
            contract_type=contract,
            datetime_utc__gte=start_utc,
            datetime_utc__lt=end_utc,
        )

        if aggregate_yearly:
            qs = (base_qs
                  .annotate(year=TruncYear("datetime_utc", tzinfo=dt.timezone.utc))
                  .values("year")
                  .annotate(
                      price=Avg("price"),
                      currency=Max("currency"),
                      unit=Max("unit"),
                  )
                  .order_by("year"))

            items = [{
                "datetime_utc": _fmt_z(r["year"]),
                "price": r["price"],
                "currency": r["currency"],
                "unit": r["unit"],
                "resolution": "P1Y",
            } for r in qs]
        elif aggregate_monthly:
            qs = (base_qs
                  .annotate(month=TruncMonth("datetime_utc", tzinfo=dt.timezone.utc))
                  .values("month")
                  .annotate(
                      price=Avg("price"),
                      currency=Max("currency"),
                      unit=Max("unit"),
                  )
                  .order_by("month"))

            items = [{
                "datetime_utc": _fmt_z(r["month"]),
                "price": r["price"],
                "currency": r["currency"],
                "unit": r["unit"],
                "resolution": "P1M",
            } for r in qs]
        elif aggregate_daily:
            qs = (base_qs
                  .annotate(day=TruncDay("datetime_utc", tzinfo=dt.timezone.utc))
                  .values("day")
                  .annotate(
                      price=Avg("price"),
                      currency=Max("currency"),
                      unit=Max("unit"),
                  )
                  .order_by("day"))

            items = [{
                "datetime_utc": _fmt_z(r["day"]),
                "price": r["price"],
                "currency": r["currency"],
                "unit": r["unit"],
                "resolution": "P1D",
            } for r in qs]
        else:
            qs = base_qs.order_by("datetime_utc")
            items = [{
                "datetime_utc": _fmt_z(r.datetime_utc),
                "price": r.price,
                "currency": r.currency,
                "unit": r.unit,
                "resolution": r.resolution,
            } for r in qs]

        return Response({
            "country": country.pk,
            "contract_type": contract,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "items": items,
        }, status=200)


@method_decorator(cache_page(600), name="get")
class CountryPricesBulkRangeView(APIView):
    """
    GET /api/prices/bulk-range/?countries=AT,DE,FR&contract=A01&period=today
    GET /api/prices/bulk-range/?countries=AT,DE,FR&contract=A01&start=...&end=...
    GET /api/prices/bulk-range/?countries=AT,DE,FR&contract=A01&start=...&end=...&resolution=d
    GET /api/prices/bulk-range/?countries=AT,DE,FR&contract=A01&start=...&end=...&m=1
    """
    MAX_COUNTRIES = 20

    @extend_schema(
        tags=["Prices"],
        summary="Price series for multiple countries",
        description="Returns price time series for several countries in one request. The `data` object is keyed by country code.",
        parameters=[
            countries_parameter(max_items=20),
            CONTRACT_PARAMETER,
            period_parameter(allow_yesterday=True, allow_dayahead=True),
            START_PARAMETER,
            END_PARAMETER,
            RESOLUTION_PARAMETER,
            MONTHLY_FLAG_PARAMETER,
        ],
        responses={
            200: PriceBulkResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Bulk price response",
                response_only=True,
                value={
                    "request_info": {
                        "countries_requested": ["AT", "DE"],
                        "countries_found": ["AT", "DE"],
                        "countries_ignored": [],
                        "contract_type": "A01",
                        "resolution": "d",
                        "start_utc": "2026-03-01T00:00:00Z",
                        "end_utc": "2026-03-03T00:00:00Z",
                        "total_countries": 2,
                        "total_records": 4,
                        "server_elapsed_ms": 12.11,
                    },
                    "data": {
                        "AT": {
                            "country": "AT",
                            "contract_type": "A01",
                            "start_utc": "2026-03-01T00:00:00Z",
                            "end_utc": "2026-03-03T00:00:00Z",
                            "items": [
                                {
                                    "datetime_utc": "2026-03-01T00:00:00Z",
                                    "price": "83.250000",
                                    "currency": "EUR",
                                    "unit": "MWH",
                                    "resolution": "P1D",
                                }
                            ],
                        }
                    },
                },
            ),
            OpenApiExample(
                "Too many countries",
                response_only=True,
                status_codes=["400"],
                value={"detail": "Maximum 20 countries per request"},
            ),
        ],
    )
    def get(self, request):
        start_perf = time.perf_counter()
        countries_param = request.query_params.get("countries", "")
        contract = (request.query_params.get("contract") or "A01").upper()
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")
        resolution = request.query_params.get("resolution")
        normalized_resolution = (resolution or "").lower()
        aggregate_daily = normalized_resolution == "d"
        aggregate_monthly = normalized_resolution == "m" or _bool_param(request, "m")
        aggregate_yearly = normalized_resolution == "y"

        country_codes = _split_codes(countries_param)
        if not country_codes:
            return Response({"detail": "countries parameter is required"}, status=400)
        if len(country_codes) > self.MAX_COUNTRIES:
            return Response({"detail": f"Maximum {self.MAX_COUNTRIES} countries per request"}, status=400)

        try:
            start_utc, end_utc, _ = _compute_window_utc(period, start_s, end_s)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if period and period.lower() == "dayahead" and contract != "A01":
            return Response({"detail": "period=dayahead is only valid with contract=A01 (Day-ahead)."}, status=400)
        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        valid_countries, missing_countries = _partition_country_codes(country_codes)
        if not valid_countries:
            return Response({"detail": f"Unknown countries: {', '.join(missing_countries)}"}, status=400)

        base_qs = CountryPricePoint.objects.filter(
            country_id__in=valid_countries,
            contract_type=contract,
            datetime_utc__gte=start_utc,
            datetime_utc__lt=end_utc,
        )

        results: Dict[str, dict] = {}
        if aggregate_yearly:
            qs = (base_qs
                  .annotate(year=TruncYear("datetime_utc", tzinfo=dt.timezone.utc))
                  .values("country_id", "year")
                  .annotate(
                      price=Avg("price"),
                      currency=Max("currency"),
                      unit=Max("unit"),
                  )
                  .order_by("country_id", "year"))

            for rec in qs:
                cid = rec["country_id"]
                bucket = results.setdefault(cid, {
                    "country": cid,
                    "contract_type": contract,
                    "start_utc": _fmt_z(start_utc),
                    "end_utc": _fmt_z(end_utc),
                    "items": [],
                })
                bucket["items"].append({
                    "datetime_utc": _fmt_z(rec["year"]),
                    "price": rec["price"],
                    "currency": rec["currency"],
                    "unit": rec["unit"],
                    "resolution": "P1Y",
                })
        elif aggregate_monthly:
            qs = (base_qs
                  .annotate(month=TruncMonth("datetime_utc", tzinfo=dt.timezone.utc))
                  .values("country_id", "month")
                  .annotate(
                      price=Avg("price"),
                      currency=Max("currency"),
                      unit=Max("unit"),
                  )
                  .order_by("country_id", "month"))

            for rec in qs:
                cid = rec["country_id"]
                bucket = results.setdefault(cid, {
                    "country": cid,
                    "contract_type": contract,
                    "start_utc": _fmt_z(start_utc),
                    "end_utc": _fmt_z(end_utc),
                    "items": [],
                })
                bucket["items"].append({
                    "datetime_utc": _fmt_z(rec["month"]),
                    "price": rec["price"],
                    "currency": rec["currency"],
                    "unit": rec["unit"],
                    "resolution": "P1M",
                })
        elif aggregate_daily:
            qs = (base_qs
                  .annotate(day=TruncDay("datetime_utc", tzinfo=dt.timezone.utc))
                  .values("country_id", "day")
                  .annotate(
                      price=Avg("price"),
                      currency=Max("currency"),
                      unit=Max("unit"),
                  )
                  .order_by("country_id", "day"))

            for rec in qs:
                cid = rec["country_id"]
                bucket = results.setdefault(cid, {
                    "country": cid,
                    "contract_type": contract,
                    "start_utc": _fmt_z(start_utc),
                    "end_utc": _fmt_z(end_utc),
                    "items": [],
                })
                bucket["items"].append({
                    "datetime_utc": _fmt_z(rec["day"]),
                    "price": rec["price"],
                    "currency": rec["currency"],
                    "unit": rec["unit"],
                    "resolution": "P1D",
                })
        else:
            qs = base_qs.select_related('country').order_by("country_id", "datetime_utc")
            for rec in qs:
                cid = rec.country.pk
                bucket = results.setdefault(cid, {
                    "country": cid,
                    "contract_type": contract,
                    "start_utc": _fmt_z(start_utc),
                    "end_utc": _fmt_z(end_utc),
                    "items": [],
                })
                bucket["items"].append({
                    "datetime_utc": _fmt_z(rec.datetime_utc),
                    "price": rec.price,
                    "currency": rec.currency,
                    "unit": rec.unit,
                    "resolution": rec.resolution,
                })

        # Ensure empty buckets for countries with no data
        for cid in valid_countries:
            results.setdefault(cid, {
                "country": cid,
                "contract_type": contract,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "items": [],
            })

        elapsed_ms = (time.perf_counter() - start_perf) * 1000
        total_records = sum(len(v["items"]) for v in results.values())
        logger.info(
            "bulk price range request complete",
            extra={
                "countries": country_codes,
                "contract": contract,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "resolution": "m" if aggregate_monthly else resolution,
                "total_records": total_records,
                "elapsed_ms": round(elapsed_ms, 2),
            },
        )

        return Response({
            "request_info": {
                "countries_requested": country_codes,
                "countries_found": valid_countries,
                "countries_ignored": missing_countries,
                "contract_type": contract,
                "resolution": "m" if aggregate_monthly else resolution,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "total_countries": len(results),
                "total_records": total_records,
                "server_elapsed_ms": round(elapsed_ms, 2),
            },
            "data": results
        }, status=200)


# ───────────────────────────── Generation (range) ──────────────────────────────

@method_decorator(cache_page(600), name="get")
class CountryGenerationRangeView(APIView):
    """
    GET /api/generation/range/?country=CZ[&psr=B16][&local=1]
    GET /api/generation/range/?country=CZ&period=today|yesterday[&psr=B16][&local=1]
    GET /api/generation/range/?country=CZ&start=...&end=...[&psr=B16]
    GET /api/generation/range/?country=CZ&start=...&end=...[&psr=B16]&resolution=d
    GET /api/generation/range/?country=CZ&start=...&end=...[&psr=B16]&resolution=m
    """

    @extend_schema(
        tags=["Generation"],
        summary="Actual generation range for one country",
        description="Returns actual generation values for one country. Supports shortcut periods or an explicit range, plus optional PSR filtering and daily/monthly/yearly aggregation.",
        parameters=[
            country_parameter(),
            psr_parameter(),
            LOCAL_PARAMETER,
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
            RESOLUTION_PARAMETER,
            MONTHLY_FLAG_PARAMETER,
        ],
        responses={
            200: GenerationRangeResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Monthly generation aggregation",
                response_only=True,
                value={
                    "country": "CZ",
                    "date_label": "custom range",
                    "start_utc": "2026-01-01T00:00:00Z",
                    "end_utc": "2026-03-01T00:00:00Z",
                    "items": [
                        {
                            "datetime_utc": "2026-01-01T00:00:00Z",
                            "psr_type": "B16",
                            "psr_name": "Solar",
                            "generation_mw": "114.320",
                            "resolution": "P1M",
                        }
                    ],
                },
            ),
            INVALID_RANGE_EXAMPLE,
        ],
    )
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr = request.query_params.get("psr")
        use_local = _bool_param(request, "local")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")
        resolution = request.query_params.get("resolution")
        normalized_resolution = (resolution or "").lower()
        aggregate_daily = normalized_resolution == "d"
        aggregate_monthly = normalized_resolution == "m" or _bool_param(request, "m")
        aggregate_yearly = normalized_resolution == "y"

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        try:
            start_utc, end_utc, label = _compute_window_utc(
                period, start_s, end_s,
                allow_yesterday=True,
                use_local_for_yesterday=use_local,
                align_to_15min=True,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        qs = CountryGenerationByType.objects.filter(
            country=country,
            datetime_utc__gte=start_utc,
            datetime_utc__lt=end_utc,
        )
        if psr:
            qs = qs.filter(psr_type=psr)

        if aggregate_yearly:
            rows = (
                qs.annotate(year=TruncYear("datetime_utc", tzinfo=dt.timezone.utc))
                .values("year", "psr_type")
                .annotate(
                    psr_name=Max("psr_name"),
                    generation_mw=Avg("generation_mw"),
                )
                .order_by("year", "psr_type")
            )

            items = [{
                "datetime_utc": _fmt_z(r["year"]),
                "psr_type": r["psr_type"],
                "psr_name": r["psr_name"] or r["psr_type"],
                "generation_mw": r["generation_mw"],
                "resolution": "P1Y",
            } for r in rows]
        elif aggregate_monthly:
            rows = (
                qs.annotate(month=TruncMonth("datetime_utc", tzinfo=dt.timezone.utc))
                .values("month", "psr_type")
                .annotate(
                    psr_name=Max("psr_name"),
                    generation_mw=Avg("generation_mw"),
                )
                .order_by("month", "psr_type")
            )

            items = [{
                "datetime_utc": _fmt_z(r["month"]),
                "psr_type": r["psr_type"],
                "psr_name": r["psr_name"] or r["psr_type"],
                "generation_mw": r["generation_mw"],
                "resolution": "P1M",
            } for r in rows]
        elif aggregate_daily:
            rows = (
                qs.annotate(day=TruncDay("datetime_utc", tzinfo=dt.timezone.utc))
                .values("day", "psr_type")
                .annotate(
                    psr_name=Max("psr_name"),
                    generation_mw=Avg("generation_mw"),
                )
                .order_by("day", "psr_type")
            )

            items = [{
                "datetime_utc": _fmt_z(r["day"]),
                "psr_type": r["psr_type"],
                "psr_name": r["psr_name"] or r["psr_type"],
                "generation_mw": r["generation_mw"],
                "resolution": "P1D",
            } for r in rows]
        else:
            rows = qs.order_by("datetime_utc", "psr_type").values(
                "datetime_utc", "psr_type", "psr_name", "generation_mw", "resolution"
            )

            items = [{
                "datetime_utc": _fmt_z(r["datetime_utc"]),
                "psr_type": r["psr_type"],
                "psr_name": r["psr_name"] or r["psr_type"],
                "generation_mw": r["generation_mw"],
                "resolution": r["resolution"],
            } for r in rows]

        return Response({
            "country": country.pk,
            "date_label": label,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "items": items,
        }, status=200)


@method_decorator(cache_page(600), name="get")
class CountryGenerationResRangeView(APIView):
    """
    GET /api/generation-res/range/?country=CZ[&psr=B16][&local=1]
    GET /api/generation-res/range/?country=CZ&period=today|yesterday[&psr=B16][&local=1]
    GET /api/generation-res/range/?country=CZ&start=...&end=...[&psr=B16]
    """

    @extend_schema(
        tags=["Generation"],
        summary="Renewable generation range for one country",
        description="Returns renewable generation by production type. The `psr` parameter may contain one code or a comma-separated list.",
        parameters=[
            country_parameter(),
            psr_parameter(allow_multiple=True),
            LOCAL_PARAMETER,
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
        ],
        responses={
            200: ResGenerationResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Renewable generation response",
                response_only=True,
                value={
                    "country": "BG",
                    "date_label": "today (UTC)",
                    "start_utc": "2026-03-10T00:00:00Z",
                    "end_utc": "2026-03-11T00:00:00Z",
                    "items": [
                        {
                            "country": "BG",
                            "datetime_utc": "2026-03-10T09:00:00Z",
                            "psr_type": "B16",
                            "psr_name": "Solar",
                            "generation_mw": "812.120",
                            "unit": "MW",
                            "resolution": "PT60M",
                        }
                    ],
                },
            ),
            INVALID_COUNTRY_EXAMPLE,
        ],
    )
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr_param = request.query_params.get("psr")
        use_local = _bool_param(request, "local")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        try:
            start_utc, end_utc, label = _compute_window_utc(
                period, start_s, end_s,
                allow_yesterday=True,
                use_local_for_yesterday=use_local,
                align_to_15min=True,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        qs = CountryResGenerationByType.objects.filter(
            country=country,
            datetime_utc__gte=start_utc,
            datetime_utc__lt=end_utc,
        )

        if psr_param:
            psr_codes = _split_codes(psr_param)
            if len(psr_codes) == 1:
                qs = qs.filter(psr_type=psr_codes[0])
            elif psr_codes:
                qs = qs.filter(psr_type__in=psr_codes)

        qs = qs.order_by("datetime_utc", "psr_type")
        serializer = CountryResGenerationByTypeSerializer(qs, many=True)

        return Response({
            "country": country.pk,
            "date_label": label,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "items": serializer.data,
        }, status=200)


@method_decorator(cache_page(600), name="get")
class CountryGenerationForecastRangeView(APIView):
    """GET /api/generation-forecast/range/?country=CZ&period=today|..."""

    @extend_schema(
        tags=["Generation"],
        summary="Generation forecast range for one country",
        description="Returns forecast generation by production type for a country, using shortcut periods or an explicit UTC range.",
        parameters=[
            country_parameter(),
            psr_parameter(),
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
        ],
        responses={
            200: GenerationForecastResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Generation forecast response",
                response_only=True,
                value={
                    "country": "CZ",
                    "date_label": "today (UTC)",
                    "start_utc": "2026-03-10T00:00:00Z",
                    "end_utc": "2026-03-11T00:00:00Z",
                    "items": [
                        {
                            "country": "CZ",
                            "datetime_utc": "2026-03-10T10:00:00Z",
                            "psr_type": "B16",
                            "psr_name": "Solar",
                            "forecast_mw": "920.440",
                            "resolution": "PT60M",
                        }
                    ],
                },
            ),
            INVALID_RANGE_EXAMPLE,
        ],
    )
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr = request.query_params.get("psr")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        try:
            start_utc, end_utc, label = _compute_window_utc(
                period,
                start_s,
                end_s,
                allow_yesterday=True,
                align_to_15min=True,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        qs = CountryGenerationForecastByType.objects.filter(
            country=country,
            datetime_utc__gte=start_utc,
            datetime_utc__lt=end_utc,
        )
        if psr:
            qs = qs.filter(psr_type=psr)

        qs = qs.order_by("datetime_utc", "psr_type")
        serializer = CountryGenerationForecastByTypeSerializer(qs, many=True)

        return Response({
            "country": country.pk,
            "date_label": label,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "items": serializer.data,
        }, status=200)


@method_decorator(cache_page(600), name="get")
class CountryTiltedIrradianceRangeView(APIView):
    """GET /api/generation-irradiance/range/?country=BG&period=today[&tilt=30][&azimuth=0]"""

    @extend_schema(
        tags=["Generation"],
        summary="Global tilted irradiance range for one country",
        description=(
            "Returns hourly Open-Meteo global tilted irradiance points for one country. "
            "The query can optionally be filtered by panel geometry using `tilt` and `azimuth`. "
            "If omitted, all stored tilt and azimuth combinations are returned."
        ),
        parameters=[
            country_parameter(),
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
            TILT_PARAMETER,
            AZIMUTH_PARAMETER,
        ],
        responses={
            200: TiltedIrradianceResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Tilted irradiance response",
                response_only=True,
                value={
                    "country": "BG",
                    "date_label": "today (UTC)",
                    "start_utc": "2026-03-19T00:00:00Z",
                    "end_utc": "2026-03-20T00:00:00Z",
                    "tilt_degrees": None,
                    "azimuth_degrees": None,
                    "items": [
                        {
                            "country": "BG",
                            "datetime_utc": "2026-03-19T10:00:00Z",
                            "tilt_degrees": "30.00",
                            "azimuth_degrees": "0.00",
                            "irradiance_wm2": 612.4,
                            "resolution": "PT1H",
                        }
                    ],
                },
            ),
            INVALID_COUNTRY_EXAMPLE,
            INVALID_RANGE_EXAMPLE,
        ],
    )
    def get(self, request):
        country_q = request.query_params.get("country", "")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        try:
            tilt = _optional_float_param(request, "tilt")
            azimuth = _optional_float_param(request, "azimuth")
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        try:
            start_utc, end_utc, label = _compute_window_utc(
                period,
                start_s,
                end_s,
                allow_yesterday=True,
                align_to_15min=False,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        filters = {
            "country": country,
            "datetime_utc__gte": start_utc,
            "datetime_utc__lt": end_utc,
        }
        if tilt is not None:
            filters["tilt_degrees"] = tilt
        if azimuth is not None:
            filters["azimuth_degrees"] = azimuth

        qs = (
            CountryTiltedIrradiancePoint.objects
            .filter(**filters)
            .order_by("tilt_degrees", "azimuth_degrees", "datetime_utc")
        )
        serializer = CountryTiltedIrradiancePointSerializer(qs, many=True)

        return Response({
            "country": country.pk,
            "date_label": label,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "tilt_degrees": tilt,
            "azimuth_degrees": azimuth,
            "items": serializer.data,
        }, status=200)


@method_decorator(cache_page(600), name="get")
class CountryTiltedIrradianceBulkRangeView(APIView):
    """GET /api/generation-irradiance/bulk-range/?countries=ALL&period=today[&tilt=30][&azimuth=0]"""

    MAX_COUNTRIES = 60

    @extend_schema(
        tags=["Generation"],
        summary="Global tilted irradiance range for multiple countries",
        description=(
            "Returns hourly Open-Meteo global tilted irradiance points for multiple countries. "
            "Use `countries=ALL` or omit the parameter to include all configured countries from `COUNTRY_COORDS`. "
            "Geometry filters are optional; if omitted, all stored tilt and azimuth combinations are returned."
        ),
        parameters=[
            countries_parameter(required=False, max_items=60),
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
            TILT_PARAMETER,
            AZIMUTH_PARAMETER,
        ],
        responses={
            200: TiltedIrradianceBulkResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Bulk tilted irradiance response",
                response_only=True,
                value={
                    "request_info": {
                        "countries_requested": ["BG", "RO"],
                        "countries_found": ["BG", "RO"],
                        "countries_ignored": [],
                        "tilt_degrees": None,
                        "azimuth_degrees": None,
                        "start_utc": "2026-03-19T00:00:00Z",
                        "end_utc": "2026-03-20T00:00:00Z",
                        "date_label": "today (UTC)",
                        "total_countries": 2,
                        "total_records": 48,
                    },
                    "data": {
                        "BG": {
                            "country": "BG",
                            "date_label": "today (UTC)",
                            "start_utc": "2026-03-19T00:00:00Z",
                            "end_utc": "2026-03-20T00:00:00Z",
                            "tilt_degrees": None,
                            "azimuth_degrees": None,
                            "items": [
                                {
                                    "country": "BG",
                                    "datetime_utc": "2026-03-19T10:00:00Z",
                                    "tilt_degrees": "30.00",
                                    "azimuth_degrees": "0.00",
                                    "irradiance_wm2": 612.4,
                                    "resolution": "PT1H",
                                }
                            ],
                        }
                    },
                },
            ),
            OpenApiExample(
                "Too many countries",
                response_only=True,
                status_codes=["400"],
                value={"detail": "Maximum 60 countries per request"},
            ),
        ],
    )
    def get(self, request):
        countries_param = request.query_params.get("countries", "")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        try:
            tilt = _optional_float_param(request, "tilt")
            azimuth = _optional_float_param(request, "azimuth")
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        requested_all = not countries_param or countries_param.strip().upper() == "ALL"
        country_codes = _configured_country_coords_codes() if requested_all else _split_codes(countries_param)

        if not country_codes:
            return Response({"detail": "countries parameter is required or COUNTRY_COORDS must be configured"}, status=400)
        if len(country_codes) > self.MAX_COUNTRIES:
            return Response({"detail": f"Maximum {self.MAX_COUNTRIES} countries per request"}, status=400)

        try:
            start_utc, end_utc, label = _compute_window_utc(
                period,
                start_s,
                end_s,
                allow_yesterday=True,
                align_to_15min=False,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        valid_countries, missing_countries = _partition_country_codes(country_codes)
        if not valid_countries and not requested_all:
            return Response({"detail": f"Unknown countries: {', '.join(missing_countries)}"}, status=400)

        results: Dict[str, dict] = {
            code: {
                "country": code,
                "date_label": label,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "tilt_degrees": tilt,
                "azimuth_degrees": azimuth,
                "items": [],
            }
            for code in valid_countries
        }

        total_records = 0
        if valid_countries:
            filters = {
                "country_id__in": valid_countries,
                "datetime_utc__gte": start_utc,
                "datetime_utc__lt": end_utc,
            }
            if tilt is not None:
                filters["tilt_degrees"] = tilt
            if azimuth is not None:
                filters["azimuth_degrees"] = azimuth

            qs = (
                CountryTiltedIrradiancePoint.objects
                .filter(**filters)
                .order_by("country_id", "tilt_degrees", "azimuth_degrees", "datetime_utc")
            )

            for row in qs.values(
                "country_id",
                "datetime_utc",
                "tilt_degrees",
                "azimuth_degrees",
                "irradiance_wm2",
                "resolution",
            ):
                results[row["country_id"]]["items"].append({
                    "country": row["country_id"],
                    "datetime_utc": _fmt_z(row["datetime_utc"]),
                    "tilt_degrees": row["tilt_degrees"],
                    "azimuth_degrees": row["azimuth_degrees"],
                    "irradiance_wm2": row["irradiance_wm2"],
                    "resolution": row["resolution"],
                })
                total_records += 1

        return Response({
            "request_info": {
                "countries_requested": country_codes,
                "countries_found": valid_countries,
                "countries_ignored": missing_countries,
                "tilt_degrees": tilt,
                "azimuth_degrees": azimuth,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "date_label": label,
                "total_countries": len(results),
                "total_records": total_records,
            },
            "data": results,
        }, status=200)


@method_decorator(cache_page(600), name="get")
class CountryWindSpeedRangeView(APIView):
    """GET /api/generation-wind-speed/range/?country=BG&period=today"""

    @extend_schema(
        tags=["Generation"],
        summary="120m wind speed range for one country",
        description="Returns hourly Open-Meteo `wind_speed_120m` points for one country.",
        parameters=[
            country_parameter(),
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
        ],
        responses={
            200: WindSpeedResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Wind speed response",
                response_only=True,
                value={
                    "country": "BG",
                    "date_label": "today (UTC)",
                    "start_utc": "2026-03-19T00:00:00Z",
                    "end_utc": "2026-03-20T00:00:00Z",
                    "items": [
                        {
                            "country": "BG",
                            "datetime_utc": "2026-03-19T10:00:00Z",
                            "wind_speed_120m": 8.7,
                            "resolution": "PT1H",
                        }
                    ],
                },
            ),
            INVALID_COUNTRY_EXAMPLE,
            INVALID_RANGE_EXAMPLE,
        ],
    )
    def get(self, request):
        country_q = request.query_params.get("country", "")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        try:
            start_utc, end_utc, label = _compute_window_utc(
                period,
                start_s,
                end_s,
                allow_yesterday=True,
                align_to_15min=False,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        qs = (
            CountryWindSpeedPoint.objects
            .filter(
                country=country,
                datetime_utc__gte=start_utc,
                datetime_utc__lt=end_utc,
            )
            .order_by("datetime_utc")
        )
        serializer = CountryWindSpeedPointSerializer(qs, many=True)

        return Response({
            "country": country.pk,
            "date_label": label,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "items": serializer.data,
        }, status=200)


@method_decorator(cache_page(600), name="get")
class CountryWindSpeedBulkRangeView(APIView):
    """GET /api/generation-wind-speed/bulk-range/?countries=ALL&period=today"""

    MAX_COUNTRIES = 60

    @extend_schema(
        tags=["Generation"],
        summary="120m wind speed range for multiple countries",
        description=(
            "Returns hourly Open-Meteo `wind_speed_120m` points for multiple countries. "
            "Use `countries=ALL` or omit the parameter to include all configured countries from `COUNTRY_COORDS`."
        ),
        parameters=[
            countries_parameter(required=False, max_items=60),
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
        ],
        responses={
            200: WindSpeedBulkResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Bulk wind speed response",
                response_only=True,
                value={
                    "request_info": {
                        "countries_requested": ["BG", "RO"],
                        "countries_found": ["BG", "RO"],
                        "countries_ignored": [],
                        "start_utc": "2026-03-19T00:00:00Z",
                        "end_utc": "2026-03-20T00:00:00Z",
                        "date_label": "today (UTC)",
                        "total_countries": 2,
                        "total_records": 48,
                    },
                    "data": {
                        "BG": {
                            "country": "BG",
                            "date_label": "today (UTC)",
                            "start_utc": "2026-03-19T00:00:00Z",
                            "end_utc": "2026-03-20T00:00:00Z",
                            "items": [
                                {
                                    "country": "BG",
                                    "datetime_utc": "2026-03-19T10:00:00Z",
                                    "wind_speed_120m": 8.7,
                                    "resolution": "PT1H",
                                }
                            ],
                        }
                    },
                },
            ),
            OpenApiExample(
                "Too many countries",
                response_only=True,
                status_codes=["400"],
                value={"detail": "Maximum 60 countries per request"},
            ),
        ],
    )
    def get(self, request):
        countries_param = request.query_params.get("countries", "")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        requested_all = not countries_param or countries_param.strip().upper() == "ALL"
        country_codes = _configured_country_coords_codes() if requested_all else _split_codes(countries_param)

        if not country_codes:
            return Response({"detail": "countries parameter is required or COUNTRY_COORDS must be configured"}, status=400)
        if len(country_codes) > self.MAX_COUNTRIES:
            return Response({"detail": f"Maximum {self.MAX_COUNTRIES} countries per request"}, status=400)

        try:
            start_utc, end_utc, label = _compute_window_utc(
                period,
                start_s,
                end_s,
                allow_yesterday=True,
                align_to_15min=False,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        valid_countries, missing_countries = _partition_country_codes(country_codes)
        if not valid_countries and not requested_all:
            return Response({"detail": f"Unknown countries: {', '.join(missing_countries)}"}, status=400)

        results: Dict[str, dict] = {
            code: {
                "country": code,
                "date_label": label,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "items": [],
            }
            for code in valid_countries
        }

        total_records = 0
        if valid_countries:
            qs = (
                CountryWindSpeedPoint.objects
                .filter(
                    country_id__in=valid_countries,
                    datetime_utc__gte=start_utc,
                    datetime_utc__lt=end_utc,
                )
                .order_by("country_id", "datetime_utc")
            )

            for row in qs.values(
                "country_id",
                "datetime_utc",
                "wind_speed_120m",
                "resolution",
            ):
                results[row["country_id"]]["items"].append({
                    "country": row["country_id"],
                    "datetime_utc": _fmt_z(row["datetime_utc"]),
                    "wind_speed_120m": row["wind_speed_120m"],
                    "resolution": row["resolution"],
                })
                total_records += 1

        return Response({
            "request_info": {
                "countries_requested": country_codes,
                "countries_found": valid_countries,
                "countries_ignored": missing_countries,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "date_label": label,
                "total_countries": len(valid_countries),
                "total_records": total_records,
            },
            "data": results,
        }, status=200)


@method_decorator(cache_page(600), name="get")
class CountryGenerationBulkRangeView(APIView):
    """
    GET /api/generation/bulk-range/?countries=AT,DE,FR[&psr=B16][&local=1]
    GET /api/generation/bulk-range/?countries=AT,DE,FR&period=today|yesterday[&psr=B16][&local=1]
    GET /api/generation/bulk-range/?countries=AT,DE,FR&start=...&end=...[&psr=B16][&local=1]
    GET /api/generation/bulk-range/?countries=AT,DE,FR&start=...&end=...[&psr=B16]&resolution=d
    GET /api/generation/bulk-range/?countries=AT,DE,FR&start=...&end=...[&psr=B16]&resolution=m
    """
    MAX_COUNTRIES = 20

    @extend_schema(
        tags=["Generation"],
        summary="Actual generation range for multiple countries",
        description="Returns actual generation for several countries in one request. The `data` object is keyed by country code.",
        parameters=[
            countries_parameter(max_items=20),
            psr_parameter(),
            LOCAL_PARAMETER,
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
            RESOLUTION_PARAMETER,
            MONTHLY_FLAG_PARAMETER,
        ],
        responses={
            200: GenerationBulkResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Bulk generation response",
                response_only=True,
                value={
                    "request_info": {
                        "countries_requested": ["BG", "RO"],
                        "countries_found": ["BG", "RO"],
                        "countries_ignored": [],
                        "psr": "B16",
                        "resolution": "d",
                        "start_utc": "2026-03-01T00:00:00Z",
                        "end_utc": "2026-03-03T00:00:00Z",
                        "date_label": "custom range",
                        "total_countries": 2,
                        "total_records": 4,
                    },
                    "data": {
                        "BG": {
                            "country": "BG",
                            "date_label": "custom range",
                            "start_utc": "2026-03-01T00:00:00Z",
                            "end_utc": "2026-03-03T00:00:00Z",
                            "items": [
                                {
                                    "datetime_utc": "2026-03-01T00:00:00Z",
                                    "psr_type": "B16",
                                    "psr_name": "Solar",
                                    "generation_mw": "614.210",
                                    "resolution": "P1D",
                                }
                            ],
                        }
                    },
                },
            ),
            OpenApiExample(
                "Too many countries",
                response_only=True,
                status_codes=["400"],
                value={"detail": "Maximum 20 countries per request"},
            ),
        ],
    )
    def get(self, request):
        countries_param = request.query_params.get("countries", "")
        psr = request.query_params.get("psr")
        use_local = _bool_param(request, "local")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")
        resolution = request.query_params.get("resolution")
        normalized_resolution = (resolution or "").lower()
        aggregate_daily = normalized_resolution == "d"
        aggregate_monthly = normalized_resolution == "m" or _bool_param(request, "m")
        aggregate_yearly = normalized_resolution == "y"

        country_codes = _split_codes(countries_param)
        if not country_codes:
            return Response({"detail": "countries parameter is required"}, status=400)
        if len(country_codes) > self.MAX_COUNTRIES:
            return Response({"detail": f"Maximum {self.MAX_COUNTRIES} countries per request"}, status=400)

        try:
            start_utc, end_utc, label = _compute_window_utc(
                period, start_s, end_s,
                allow_yesterday=True,
                use_local_for_yesterday=use_local,
                align_to_15min=True,
            )
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        valid_countries, missing_countries = _partition_country_codes(country_codes)
        if not valid_countries:
            return Response({"detail": f"Unknown countries: {', '.join(missing_countries)}"}, status=400)

        base_qs = (
            CountryGenerationByType.objects
            .filter(
                country_id__in=valid_countries,
                datetime_utc__gte=start_utc,
                datetime_utc__lt=end_utc,
            )
        )
        if psr:
            base_qs = base_qs.filter(psr_type=psr)

        results: Dict[str, dict] = {
            code: {
                "country": code,
                "date_label": label,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "items": [],
            }
            for code in valid_countries
        }

        total_records = 0
        if aggregate_yearly:
            qs = (
                base_qs
                .annotate(year=TruncYear("datetime_utc", tzinfo=dt.timezone.utc))
                .values("country_id", "year", "psr_type")
                .annotate(
                    psr_name=Max("psr_name"),
                    generation_mw=Avg("generation_mw"),
                )
                .order_by("country_id", "year", "psr_type")
            )
            for r in qs:
                results[r["country_id"]]["items"].append({
                    "datetime_utc": _fmt_z(r["year"]),
                    "psr_type": r["psr_type"],
                    "psr_name": r["psr_name"] or r["psr_type"],
                    "generation_mw": r["generation_mw"],
                    "resolution": "P1Y",
                })
                total_records += 1
        elif aggregate_monthly:
            qs = (
                base_qs
                .annotate(month=TruncMonth("datetime_utc", tzinfo=dt.timezone.utc))
                .values("country_id", "month", "psr_type")
                .annotate(
                    psr_name=Max("psr_name"),
                    generation_mw=Avg("generation_mw"),
                )
                .order_by("country_id", "month", "psr_type")
            )
            for r in qs:
                results[r["country_id"]]["items"].append({
                    "datetime_utc": _fmt_z(r["month"]),
                    "psr_type": r["psr_type"],
                    "psr_name": r["psr_name"] or r["psr_type"],
                    "generation_mw": r["generation_mw"],
                    "resolution": "P1M",
                })
                total_records += 1
        elif aggregate_daily:
            qs = (
                base_qs
                .annotate(day=TruncDay("datetime_utc", tzinfo=dt.timezone.utc))
                .values("country_id", "day", "psr_type")
                .annotate(
                    psr_name=Max("psr_name"),
                    generation_mw=Avg("generation_mw"),
                )
                .order_by("country_id", "day", "psr_type")
            )
            for r in qs:
                results[r["country_id"]]["items"].append({
                    "datetime_utc": _fmt_z(r["day"]),
                    "psr_type": r["psr_type"],
                    "psr_name": r["psr_name"] or r["psr_type"],
                    "generation_mw": r["generation_mw"],
                    "resolution": "P1D",
                })
                total_records += 1
        else:
            qs = base_qs.order_by("country_id", "datetime_utc", "psr_type").values(
                "country_id", "datetime_utc", "psr_type", "psr_name", "generation_mw", "resolution"
            )
            for r in qs:
                results[r["country_id"]]["items"].append({
                    "datetime_utc": _fmt_z(r["datetime_utc"]),
                    "psr_type": r["psr_type"],
                    "psr_name": r["psr_name"] or r["psr_type"],
                    "generation_mw": r["generation_mw"],
                    "resolution": r["resolution"],
                })
                total_records += 1

        return Response(
            {
                "request_info": {
                    "countries_requested": country_codes,
                    "countries_found": valid_countries,
                "countries_ignored": missing_countries,
                    "psr": psr,
                    "resolution": "m" if aggregate_monthly else resolution,
                    "start_utc": _fmt_z(start_utc),
                    "end_utc": _fmt_z(end_utc),
                    "date_label": label,
                    "total_countries": len(results),
                    "total_records": total_records,
                },
                "data": results,
            },
            status=200,
        )


# ─────────────────────────────── Physical Flows ────────────────────────────────

class PhysicalFlowsRangeView(APIView):
    """
    GET /api/flows/range/?start=YYYY-MM-DDTHH:MM:SSZ&end=YYYY-MM-DDTHH:MM:SSZ
    GET /api/flows/range/?period=today
    Optional filters:
      - from=BG  (source country)
      - to=RO    (target country)
      - countries=BG,RO (both directions among listed)
    """

    @extend_schema(
        tags=["Flows"],
        summary="Physical flow range",
        description="Returns cross-border physical flows for a period or explicit UTC window. Results can be filtered by source country, destination country, or a country set.",
        parameters=[
            period_parameter(),
            START_PARAMETER,
            END_PARAMETER,
            FLOW_FROM_PARAMETER,
            FLOW_TO_PARAMETER,
            countries_parameter(required=False),
        ],
        responses={
            200: PhysicalFlowsRangeResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Flow range response",
                response_only=True,
                value={
                    "start_utc": "2026-03-10T00:00:00Z",
                    "end_utc": "2026-03-10T03:00:00Z",
                    "count": 2,
                    "items": [
                        {
                            "datetime_utc": "2026-03-10T01:00:00Z",
                            "country_from": "BG",
                            "country_to": "RO",
                            "out_domain_eic": "10YCA-BULGARIA-R",
                            "in_domain_eic": "10YRO-TEL------P",
                            "resolution": "PT60M",
                            "quantity_mw": 421.5,
                            "created_at": "2026-03-10T01:05:00Z",
                        }
                    ],
                },
            ),
            INVALID_RANGE_EXAMPLE,
        ],
    )
    def get(self, request):
        period   = request.query_params.get("period")
        start_s  = request.query_params.get("start")
        end_s    = request.query_params.get("end")
        src_iso  = (request.query_params.get("from") or "").upper().strip()
        dst_iso  = (request.query_params.get("to") or "").upper().strip()
        multi    = request.query_params.get("countries")

        try:
            start_utc, end_utc, _ = _compute_window_utc(period, start_s, end_s)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        # When using period shortcuts (e.g. period=today) clamp the upper bound
        # to "now" so we do not surface future/forecast rows that may already
        # exist in the DB (e.g. after a manual backfill of a full day).  Align
        # the cutoff to 15-minute buckets to match the PhysicalFlow resolution.
        explicit_range = bool(start_s or end_s) and not period
        if period and not explicit_range:
            now_utc = _floor_15min(_now_utc())
            if end_utc > now_utc:
                end_utc = now_utc

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        src_field, dst_field, ts_field = _flow_field_names()

        time_filters = {f"{ts_field}__gte": start_utc}
        if explicit_range:
            # When the caller provides an explicit start/end window we treat the
            # upper bound as inclusive.  The ENTSO-E physical flow payloads are
            # timestamped at the *end* of the interval (e.g. the 13:00→14:00
            # flow is stored at 14:00).  Using ``__lt`` therefore drops the
            # record that users expect to see in that custom window.  The
            # period-based helpers (today/yesterday/…) still use the original
            # half-open interval semantics to avoid double counting across
            # adjacent ranges.
            time_filters[f"{ts_field}__lte"] = end_utc
        else:
            time_filters[f"{ts_field}__lt"] = end_utc

        qs = PhysicalFlow.objects.filter(**time_filters)

        if src_iso:
            try:
                _get_country_or_400(src_iso)
            except ValueError as e:
                return Response({"detail": str(e)}, status=400)
            qs = qs.filter(country_from_id=src_iso)

        if dst_iso:
            try:
                _get_country_or_400(dst_iso)
            except ValueError as e:
                return Response({"detail": str(e)}, status=400)
            qs = qs.filter(country_to_id=dst_iso)

        if multi:
            codes = _split_codes(multi)
            if not codes:
                return Response({"detail": "No valid country codes in 'countries'."}, status=400)
            try:
                _validate_countries_or_400(codes)
            except ValueError as e:
                return Response({"detail": str(e)}, status=400)
            qs = qs.filter(Q(**{f"{src_field}__in": codes, f"{dst_field}__in": codes}))

        qs = qs.order_by(ts_field, src_field, dst_field)
        serializer = PhysicalFlowSerializer(qs, many=True)
        items = serializer.data

        return Response({
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "count": len(items),
            "items": items,
        }, status=200)


@method_decorator(cache_page(300), name="get")
class PhysicalFlowsLatestView(APIView):
    """
    GET /api/flows/latest/?country=BG[&neighbors=1]
      -> returns latest hour window [t-1h, t) flows touching BG,
         with simple in/out totals; optionally grouped by neighbor.
    """

    @extend_schema(
        tags=["Flows"],
        summary="Latest physical flow window for one country",
        description="Returns the latest available hourly flow window touching the requested country, plus inbound/outbound totals and optional neighbor breakdowns.",
        parameters=[
            country_parameter(),
            NEIGHBORS_PARAMETER,
        ],
        responses={
            200: PhysicalFlowsLatestResponseSerializer,
            400: BAD_REQUEST_RESPONSE,
        },
        examples=[
            OpenApiExample(
                "Latest flow response",
                response_only=True,
                value={
                    "country": "BG",
                    "start_utc": "2026-03-10T09:00:00Z",
                    "end_utc": "2026-03-10T10:00:00Z",
                    "count": 2,
                    "items": [
                        {
                            "datetime_utc": "2026-03-10T09:00:00Z",
                            "country_from": "BG",
                            "country_to": "RO",
                            "out_domain_eic": "10YCA-BULGARIA-R",
                            "in_domain_eic": "10YRO-TEL------P",
                            "resolution": "PT60M",
                            "quantity_mw": 320.0,
                            "created_at": "2026-03-10T09:03:00Z",
                        }
                    ],
                    "totals": {"in_mw": 180.0, "out_mw": 320.0, "net_mw": -140.0},
                    "neighbors": [{"neighbor": "RO", "in_mw": 180.0, "out_mw": 320.0, "net_mw": -140.0}],
                },
            ),
            INVALID_COUNTRY_EXAMPLE,
        ],
    )
    def get(self, request):
        country_q = (request.query_params.get("country") or "").upper().strip()
        with_neighbors = _bool_param(request, "neighbors")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        src_field, dst_field, ts_field = _flow_field_names()

        latest_ts = (
            PhysicalFlow.objects
            .filter(Q(**{src_field: country.pk}) | Q(**{dst_field: country.pk}))
            .aggregate(mx=Max(ts_field))["mx"]
        )
        if not latest_ts:
            return Response({
                "country": country.pk,
                "items": [],
                "totals": {"in_mw": 0.0, "out_mw": 0.0, "net_mw": 0.0},
            }, status=200)

        latest_ts = _utc_floor_hour(_ensure_utc(latest_ts))
        start_utc = latest_ts
        end_utc   = latest_ts + dt.timedelta(hours=1)

        base = (PhysicalFlow.objects
                .filter(**{f"{ts_field}__gte": start_utc, f"{ts_field}__lt": end_utc})
                .filter(Q(**{src_field: country.pk}) | Q(**{dst_field: country.pk}))
                .order_by(ts_field, src_field, dst_field))

        serializer = PhysicalFlowSerializer(base, many=True)
        items = serializer.data

        in_total = 0.0
        out_total = 0.0
        mw_key = PHYSICAL_FLOW_MW_FIELD

        for row in items:
            val = float(row.get(mw_key, 0) or 0)
            src = row.get(src_field) or row.get("country_from") or row.get("source_country") or row.get("from_country")
            dst = row.get(dst_field) or row.get("country_to") or row.get("target_country") or row.get("to_country")
            if src == country.pk:
                out_total += val
            elif dst == country.pk:
                in_total += val

        payload = {
            "country": country.pk,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "count": len(items),
            "items": items,
            "totals": {"in_mw": in_total, "out_mw": out_total, "net_mw": in_total - out_total},
        }

        if with_neighbors:
            by_neighbor: Dict[str, Dict[str, float]] = {}
            for row in items:
                val = float(row.get(mw_key, 0) or 0)
                src = row.get(src_field) or row.get("country_from") or row.get("source_country") or row.get("from_country")
                dst = row.get(dst_field) or row.get("country_to") or row.get("target_country") or row.get("to_country")
                if src == country.pk:
                    neighbor = dst
                    by_neighbor.setdefault(neighbor, {"in_mw": 0.0, "out_mw": 0.0})
                    by_neighbor[neighbor]["out_mw"] += val
                elif dst == country.pk:
                    neighbor = src
                    by_neighbor.setdefault(neighbor, {"in_mw": 0.0, "out_mw": 0.0})
                    by_neighbor[neighbor]["in_mw"] += val

            payload["neighbors"] = [
                {"neighbor": k, "in_mw": v["in_mw"], "out_mw": v["out_mw"], "net_mw": v["in_mw"] - v["out_mw"]}
                for k, v in sorted(by_neighbor.items())
            ]

        return Response(payload, status=200)
logger = logging.getLogger(__name__)
