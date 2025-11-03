# entsoe_api/views_readonly.py
from __future__ import annotations

import datetime as dt
from typing import Iterable, Tuple, Dict, List

from django.conf import settings
from django.utils.timezone import get_default_timezone
from django.db.models import Max, Q
from rest_framework.views import APIView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from rest_framework.reverse import reverse

from .models import (
    Country,
    CountryCapacitySnapshot,
    CountryGenerationByType,
    CountryPricePoint,
    PhysicalFlow,
)
from entsoe_api.serializers import PhysicalFlowSerializer


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
    """Parse 'YYYY-MM-DDTHH:MM:SSZ' / ISO and floor to hour in UTC."""
    s2 = (s or "").rstrip("Z")
    d = dt.datetime.fromisoformat(s2)
    return _utc_floor_hour(d)

def _bool_param(request, key: str) -> bool:
    return (request.query_params.get(key, "") or "").lower() in ("1", "y", "yes", "true")

def _split_codes(s: str) -> List[str]:
    return [c.strip().upper() for c in (s or "").split(",") if c.strip()]

def _get_country_or_400(country_iso: str) -> Country:
    iso = (country_iso or "").upper()
    try:
        return Country.objects.get(pk=iso)
    except Country.DoesNotExist:
        raise ValueError(f"Unknown country '{iso}'. Make sure it's loaded in the DB.")

def _validate_countries_or_400(codes: Iterable[str]) -> List[str]:
    codes = list({c.upper() for c in codes if c})
    valid = list(Country.objects.filter(pk__in=codes).values_list("pk", flat=True))
    missing = sorted(set(codes) - set(valid))
    if missing:
        raise ValueError(f"Unknown countries: {', '.join(missing)}")
    return valid

def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

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
PHYSICAL_FLOW_SRC_FIELD = "source_country"     # sending domain
PHYSICAL_FLOW_DST_FIELD = "target_country"      # receiving domain
PHYSICAL_FLOW_TS_FIELD  = "datetime_utc"       # timestamp of the flow
PHYSICAL_FLOW_MW_FIELD  = "quantity_mw"        # MW value

def _flow_field_names() -> Tuple[str, str, str]:
    return (PHYSICAL_FLOW_SRC_FIELD, PHYSICAL_FLOW_DST_FIELD, PHYSICAL_FLOW_TS_FIELD)


# ───────────────────────────────── API Root ────────────────────────────────────

@api_view(["GET"])
def api_root(request, format=None):
    return Response({
        "capacity_latest": request.build_absolute_uri(reverse("capacity-latest")),
        "generation_yesterday": request.build_absolute_uri(reverse("generation-yesterday")),
        "prices_range": request.build_absolute_uri(reverse("prices-range")),
        "price_bulk": request.build_absolute_uri(reverse("country-prices-bulk-range")),
        "generation_range": request.build_absolute_uri(reverse("generation-range")),
        "generation_bulk_range": request.build_absolute_uri(reverse("generation-bulk-range")),
        "flows_range": request.build_absolute_uri(reverse("flows-range")),
        "flows_latest": request.build_absolute_uri(reverse("flows-latest")),
    })


# ─────────────────────────── Capacity (latest year) ────────────────────────────

class CountryCapacityLatestView(APIView):
    """
    GET /api/capacity/latest/?country=CZ[&psr=B16]
    """
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


# ───────────────────────── Generation (yesterday quick) ────────────────────────

class CountryGenerationYesterdayView(APIView):
    """
    GET /api/generation/yesterday/?country=CZ[&psr=B16][&local=1]
    """
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

class CountryPricesRangeView(APIView):
    """
    GET /api/prices/range/?country=AT&contract=A01&period=today
    GET /api/prices/range/?country=AT&contract=A01&period=dayahead
    GET /api/prices/range/?country=AT&contract=A01&start=...&end=...
    """
    def get(self, request):
        iso = request.query_params.get("country", "")
        contract = (request.query_params.get("contract") or "A01").upper()
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        try:
            country = _get_country_or_400(iso)
            start_utc, end_utc, _ = _compute_window_utc(period, start_s, end_s)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if period and period.lower() == "dayahead" and contract != "A01":
            return Response({"detail": "period=dayahead is only valid with contract=A01 (Day-ahead)."}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        qs = (CountryPricePoint.objects
              .filter(country=country,
                      contract_type=contract,
                      datetime_utc__gte=start_utc,
                      datetime_utc__lt=end_utc)
              .order_by("datetime_utc"))

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


class CountryPricesBulkRangeView(APIView):
    """
    GET /api/prices/bulk-range/?countries=AT,DE,FR&contract=A01&period=today
    GET /api/prices/bulk-range/?countries=AT,DE,FR&contract=A01&start=...&end=...
    """
    MAX_COUNTRIES = 20

    def get(self, request):
        countries_param = request.query_params.get("countries", "")
        contract = (request.query_params.get("contract") or "A01").upper()
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

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

        try:
            valid_countries = _validate_countries_or_400(country_codes)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        qs = (CountryPricePoint.objects
              .filter(
                  country_id__in=valid_countries,
                  contract_type=contract,
                  datetime_utc__gte=start_utc,
                  datetime_utc__lt=end_utc,
              )
              .select_related('country')
              .order_by("country_id", "datetime_utc"))

        results: Dict[str, dict] = {}
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

        return Response({
            "request_info": {
                "countries_requested": country_codes,
                "countries_found": valid_countries,
                "contract_type": contract,
                "start_utc": _fmt_z(start_utc),
                "end_utc": _fmt_z(end_utc),
                "total_countries": len(results),
                "total_records": sum(len(v["items"]) for v in results.values()),
            },
            "data": results
        }, status=200)


# ───────────────────────────── Generation (range) ──────────────────────────────

class CountryGenerationRangeView(APIView):
    """
    GET /api/generation/range/?country=CZ[&psr=B16][&local=1]
    GET /api/generation/range/?country=CZ&period=today|yesterday[&psr=B16][&local=1]
    GET /api/generation/range/?country=CZ&start=...&end=...[&psr=B16]
    """
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr = request.query_params.get("psr")
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


class CountryGenerationBulkRangeView(APIView):
    """
    GET /api/generation/bulk-range/?countries=AT,DE,FR[&psr=B16][&local=1]
    GET /api/generation/bulk-range/?countries=AT,DE,FR&period=today|yesterday[&psr=B16][&local=1]
    GET /api/generation/bulk-range/?countries=AT,DE,FR&start=...&end=...[&psr=B16][&local=1]
    """
    MAX_COUNTRIES = 20

    def get(self, request):
        countries_param = request.query_params.get("countries", "")
        psr = request.query_params.get("psr")
        use_local = _bool_param(request, "local")
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

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

        try:
            valid_countries = _validate_countries_or_400(country_codes)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        qs = (
            CountryGenerationByType.objects
            .filter(
                country_id__in=valid_countries,
                datetime_utc__gte=start_utc,
                datetime_utc__lt=end_utc,
            )
            .select_related("country")
        )
        if psr:
            qs = qs.filter(psr_type=psr)

        qs = qs.order_by("country_id", "datetime_utc", "psr_type").values(
            "country_id", "datetime_utc", "psr_type", "psr_name", "generation_mw"
        )

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
        for r in qs:
            results[r["country_id"]]["items"].append({
                "datetime_utc": _fmt_z(r["datetime_utc"]),
                "psr_type": r["psr_type"],
                "psr_name": r["psr_name"] or r["psr_type"],
                "generation_mw": r["generation_mw"],
            })
            total_records += 1

        return Response(
            {
                "request_info": {
                    "countries_requested": country_codes,
                    "countries_found": valid_countries,
                    "psr": psr,
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
        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        src_field, dst_field, ts_field = _flow_field_names()

        qs = PhysicalFlow.objects.filter(**{
            f"{ts_field}__gte": start_utc,
            f"{ts_field}__lt":  end_utc,
        })

        if src_iso:
            try:
                _get_country_or_400(src_iso)
            except ValueError as e:
                return Response({"detail": str(e)}, status=400)
            qs = qs.filter(**{src_field: src_iso})

        if dst_iso:
            try:
                _get_country_or_400(dst_iso)
            except ValueError as e:
                return Response({"detail": str(e)}, status=400)
            qs = qs.filter(**{dst_field: dst_iso})

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

        return Response({
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "count": qs.count(),
            "items": serializer.data
        }, status=200)


class PhysicalFlowsLatestView(APIView):
    """
    GET /api/flows/latest/?country=BG[&neighbors=1]
      -> returns latest hour window [t-1h, t) flows touching BG,
         with simple in/out totals; optionally grouped by neighbor.
    """
    def get(self, request):
        country_q = (request.query_params.get("country") or "").upper().strip()
        with_neighbors = _bool_param(request, "neighbors")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        src_field, dst_field, ts_field = _flow_field_names()

        latest_ts = PhysicalFlow.objects.aggregate(mx=Max(ts_field))["mx"]
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

        in_total = 0.0
        out_total = 0.0
        mw_key = PHYSICAL_FLOW_MW_FIELD

        for row in serializer.data:
            val = float(row.get(mw_key, 0) or 0)
            src = row.get(src_field) or row.get("source_country") or row.get("from_country") or row.get("country_from")
            dst = row.get(dst_field) or row.get("target_country") or row.get("to_country")   or row.get("country_to")
            if src == country.pk:
                out_total += val
            elif dst == country.pk:
                in_total += val

        payload = {
            "country": country.pk,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "count": base.count(),
            "items": serializer.data,
            "totals": {"in_mw": in_total, "out_mw": out_total, "net_mw": in_total - out_total},
        }

        if with_neighbors:
            by_neighbor: Dict[str, Dict[str, float]] = {}
            for row in serializer.data:
                val = float(row.get(mw_key, 0) or 0)
                src = row.get(src_field) or row.get("source_country") or row.get("from_country") or row.get("country_from")
                dst = row.get(dst_field) or row.get("target_country") or row.get("to_country")   or row.get("country_to")
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
