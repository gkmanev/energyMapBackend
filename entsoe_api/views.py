# entsoe_api/views_readonly.py
import datetime as dt

from django.conf import settings
from django.utils.timezone import get_default_timezone
from django.db.models import Max
from rest_framework.views import APIView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from rest_framework.reverse import reverse

from .models import (
    Country,
    CountryCapacitySnapshot,
    CountryGenerationByType,
    CountryPricePoint
)

@api_view(["GET"])
def api_root(request, format=None):
    return Response({
        "capacity_latest": request.build_absolute_uri(reverse("capacity-latest")),
        "generation_yesterday": request.build_absolute_uri(reverse("generation-yesterday")),
        "prices_range": request.build_absolute_uri(reverse("prices-range")),
        "price_bulk": request.build_absolute_uri(reverse("country-prices-bulk-range")),
        "generation_range": request.build_absolute_uri(reverse("generation-range")),
    })

def _get_country_or_400(country_iso: str):
    iso = (country_iso or "").upper()
    try:
        return Country.objects.get(pk=iso)
    except Country.DoesNotExist:
        raise ValueError(f"Unknown country '{iso}'. Make sure it's loaded in the DB.")

def _utc_floor_hour(d: dt.datetime) -> dt.datetime:
    d = d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)

def _parse_iso_utc_floor_hour(s: str) -> dt.datetime:
    s2 = (s or "").rstrip("Z")
    d = dt.datetime.fromisoformat(s2)
    return _utc_floor_hour(d)

def _compute_window_utc(period: str | None, start_s: str | None, end_s: str | None) -> tuple[dt.datetime, dt.datetime]:
    """
    Returns [start_utc, end_utc) in UTC (hour-aligned).

    Supported 'period':
      - 'today'    -> [today 00:00Z, tomorrow 00:00Z)
      - 'dayahead' -> [tomorrow 00:00Z, day+2 00:00Z)
    Otherwise requires explicit start & end (ISO UTC).
    """
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if period:
        p = period.lower()
        if p == "today":
            return today, today + dt.timedelta(days=1)
        if p == "dayahead":
            start = today + dt.timedelta(days=1)
            return start, start + dt.timedelta(days=1)
        # any other value falls through to explicit range

    if not start_s or not end_s:
        raise ValueError(
            "Provide start & end (ISO UTC, e.g. 2025-09-18T00:00:00Z) "
            "or use period=today|dayahead."
        )

    start = _parse_iso_utc_floor_hour(start_s)
    end = _parse_iso_utc_floor_hour(end_s)
    return start, end

def _yesterday_window_utc(use_local: bool = False) -> tuple[dt.datetime, dt.datetime, str]:
    """
    Return the UTC window for 'yesterday' as [start_utc, end_utc) and a label.
    If use_local=True, compute 'yesterday' in settings.TIME_ZONE, then convert to UTC.
    Generation data can be 15-min, so we align to 15-minute boundaries.
    """
    def floor_15(d: dt.datetime) -> dt.datetime:
        d = d.astimezone(dt.timezone.utc).replace(second=0, microsecond=0)
        return d - dt.timedelta(minutes=d.minute % 15)

    if use_local:
        tz = get_default_timezone()  # from settings.TIME_ZONE
        now_local = dt.datetime.now(tz)
        today_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        start_local = today_local - dt.timedelta(days=1)
        end_local = today_local
        start_utc = start_local.astimezone(dt.timezone.utc)
        end_utc = end_local.astimezone(dt.timezone.utc)
        label = f"yesterday ({settings.TIME_ZONE})"
    else:
        now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        today_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        start_utc = today_utc - dt.timedelta(days=1)
        end_utc = today_utc
        label = "yesterday (UTC)"

    return floor_15(start_utc), floor_15(end_utc), label

class CountryCapacityLatestView(APIView):
    """
    GET /api/capacity/latest/?country=CZ[&psr=B16]

    Response:
    {
      "country": "CZ",
      "year": 2024,
      "items": [{"psr_type":"B14","psr_name":"Nuclear","installed_capacity_mw":"2000.000"}, ...]
    }
    """
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr = request.query_params.get("psr")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        # find latest year that exists for this country
        latest = (
            CountryCapacitySnapshot.objects
            .filter(country=country)
            .aggregate(max_year=Max("year"))
        )["max_year"]

        if latest is None:
            return Response(
                {"country": country.pk, "year": None, "items": []},
                status=status.HTTP_200_OK,
            )

        qs = CountryCapacitySnapshot.objects.filter(country=country, year=latest)
        if psr:
            qs = qs.filter(psr_type=psr)

        items = list(
            qs.order_by("psr_type").values(
                "psr_type", "psr_name", "installed_capacity_mw"
            )
        )

        return Response(
            {"country": country.pk, "year": int(latest), "items": items},
            status=status.HTTP_200_OK,
        )


class CountryGenerationYesterdayView(APIView):
    """
    GET /api/generation/yesterday/?country=CZ[&psr=B16][&local=1]

    Response:
    {
      "country": "CZ",
      "date_label": "yesterday (UTC)",
      "start_utc": "2025-09-17T00:00:00Z",
      "end_utc": "2025-09-18T00:00:00Z",
      "items": [
        {"datetime_utc":"2025-09-17T00:00:00Z","psr_type":"B16","psr_name":"Solar","generation_mw":"123.000"},
        ...
      ]
    }
    """
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr = request.query_params.get("psr")
        use_local = request.query_params.get("local", "").lower() in ("1", "y", "yes", "true")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        start_utc, end_utc, label = _yesterday_window_utc(use_local=use_local)

        qs = CountryGenerationByType.objects.filter(
            country=country,
            datetime_utc__gte=start_utc,
            datetime_utc__lt=end_utc,
        )
        if psr:
            qs = qs.filter(psr_type=psr)

        # order by time then psr
        rows = qs.order_by("datetime_utc", "psr_type").values(
            "datetime_utc", "psr_type", "psr_name", "generation_mw"
        )

        # format datetimes as Z strings for the frontend
        items = []
        for r in rows:
            ts = r["datetime_utc"]
            ts = (ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)).astimezone(dt.timezone.utc)
            items.append({
                "datetime_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "psr_type": r["psr_type"],
                "psr_name": r["psr_name"] or r["psr_type"],
                "generation_mw": r["generation_mw"],
            })

        payload = {
            "country": country.pk,
            "date_label": label,
            "start_utc": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_utc": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "items": items,
        }
        return Response(payload, status=status.HTTP_200_OK)




class CountryPricesRangeView(APIView):
    """
    GET /api/prices/range/?country=AT&contract=A01&period=today
    GET /api/prices/range/?country=AT&contract=A01&period=dayahead
    GET /api/prices/range/?country=AT&contract=A01&start=YYYY-MM-DDTHH:MM:SSZ&end=YYYY-MM-DDTHH:MM:SSZ
    """
    def get(self, request):
        iso = request.query_params.get("country", "")
        contract = (request.query_params.get("contract") or "A01").upper()  # A01=Day-ahead, A07=Intraday
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        try:
            country = _get_country_or_400(iso)
            start_utc, end_utc = _compute_window_utc(period, start_s, end_s)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        # day-ahead window only makes sense for A01
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

        def _fmt_z(dt_utc):
            dt_utc = dt_utc if dt_utc.tzinfo else dt_utc.replace(tzinfo=dt.timezone.utc)
            return dt_utc.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        items = [{
            "datetime_utc": _fmt_z(r.datetime_utc),  # no flooring
            "price": r.price,
            "currency": r.currency,
            "unit": r.unit,
            "resolution": r.resolution,  # e.g. PT15M / PT60M
        } for r in qs]

        fmt = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")
        return Response({
            "country": country.pk,
            "contract_type": contract,
            "start_utc": fmt(start_utc),
            "end_utc": fmt(end_utc),
            "items": items,
        }, status=200)


class CountryPricesBulkRangeView(APIView):
    """
    GET /api/prices/bulk-range/?countries=AT,DE,FR&contract=A01&start=YYYY-MM-DD&end=YYYY-MM-DD
    GET /api/prices/bulk-range/?countries=AT,DE,FR&contract=A01&period=today
    
    Returns data for multiple countries in a single request.
    Max 20 countries per request to prevent timeout.
    """
    def get(self, request):
        countries_param = request.query_params.get("countries", "")
        contract = (request.query_params.get("contract") or "A01").upper()
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        # Parse and validate countries
        if not countries_param:
            return Response({"detail": "countries parameter is required"}, status=400)
        
        country_codes = [code.strip().upper() for code in countries_param.split(",") if code.strip()]
        
        if len(country_codes) > 20:  # Limit to prevent timeout
            return Response({"detail": "Maximum 20 countries per request"}, status=400)
        
        if not country_codes:
            return Response({"detail": "No valid country codes provided"}, status=400)

        try:
            start_utc, end_utc = _compute_window_utc(period, start_s, end_s)
        except ValueError as e:
            return Response({"detail": str(e)}, status=400)

        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=400)

        # Validate contract type for period
        if period and period.lower() == "dayahead" and contract != "A01":
            return Response({"detail": "period=dayahead is only valid with contract=A01 (Day-ahead)."}, status=400)

        # Get valid countries that exist in database
        from .models import Country  # Adjust import path as needed
        valid_countries = Country.objects.filter(pk__in=country_codes).values_list('pk', flat=True)
        
        if not valid_countries:
            return Response({"detail": "No valid countries found"}, status=400)

        # Optimized bulk query - single database hit for all countries
        qs = (CountryPricePoint.objects
              .filter(
                  country_id__in=valid_countries,
                  contract_type=contract,
                  datetime_utc__gte=start_utc,
                  datetime_utc__lt=end_utc
              )
              .select_related('country')  # Join country data to avoid N+1 queries
              .order_by("country_id", "datetime_utc"))

        # Group results by country
        results = {}
        
        def _fmt_z(dt_utc):
            dt_utc = dt_utc if dt_utc.tzinfo else dt_utc.replace(tzinfo=dt.timezone.utc)
            return dt_utc.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Process results efficiently
        for record in qs:
            country_code = record.country.pk
            
            if country_code not in results:
                results[country_code] = {
                    "country": country_code,
                    "contract_type": contract,
                    "start_utc": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end_utc": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "items": []
                }
            
            results[country_code]["items"].append({
                "datetime_utc": _fmt_z(record.datetime_utc),
                "price": record.price,
                "currency": record.currency,
                "unit": record.unit,
                "resolution": record.resolution,
            })

        # Add empty results for countries with no data
        for country_code in valid_countries:
            if country_code not in results:
                results[country_code] = {
                    "country": country_code,
                    "contract_type": contract,
                    "start_utc": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end_utc": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "items": []
                }

        return Response({
            "request_info": {
                "countries_requested": country_codes,
                "countries_found": list(valid_countries),
                "contract_type": contract,
                "start_utc": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_utc": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "total_countries": len(results),
                "total_records": sum(len(data["items"]) for data in results.values())
            },
            "data": results
        }, status=200)



class CountryGenerationRangeView(APIView):
    """
    GET /api/generation/range/?country=CZ[&psr=B16][&local=1]
    GET /api/generation/range/?country=CZ&period=today[&psr=B16]
    GET /api/generation/range/?country=CZ&period=yesterday[&psr=B16][&local=1] 
    GET /api/generation/range/?country=CZ&start=YYYY-MM-DDTHH:MM:SSZ&end=YYYY-MM-DDTHH:MM:SSZ[&psr=B16]

    Response:
    {
      "country": "CZ",
      "date_label": "yesterday (UTC)" | "today (UTC)" | "custom range",
      "start_utc": "2025-09-17T00:00:00Z",
      "end_utc": "2025-09-18T00:00:00Z",
      "items": [
        {"datetime_utc":"2025-09-17T00:00:00Z","psr_type":"B16","psr_name":"Solar","generation_mw":"123.000"},
        ...
      ]
    }
    """
    def get(self, request):
        country_q = request.query_params.get("country", "")
        psr = request.query_params.get("psr")
        use_local = request.query_params.get("local", "").lower() in ("1", "y", "yes", "true")
        
        # New flexible date parameters
        period = request.query_params.get("period")
        start_s = request.query_params.get("start")
        end_s = request.query_params.get("end")

        try:
            country = _get_country_or_400(country_q)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        # Handle date range logic
        try:
            if period or start_s or end_s:
                # Use flexible date range logic
                start_utc, end_utc = _compute_window_utc(period, start_s, end_s)
                
                # Generate appropriate label
                if period:
                    if period.lower() == "yesterday":
                        label = "yesterday (UTC)" if not use_local else "yesterday (local)"
                    elif period.lower() == "today":
                        label = "today (UTC)" if not use_local else "today (local)"
                    else:
                        label = f"{period} (UTC)"
                else:
                    label = "custom range"
            else:
                # Default to yesterday for backward compatibility
                start_utc, end_utc, label = _yesterday_window_utc(use_local=use_local)
                
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        # Validate date range
        if start_utc >= end_utc:
            return Response({"detail": "start must be earlier than end."}, status=status.HTTP_400_BAD_REQUEST)

        # Query the database
        qs = CountryGenerationByType.objects.filter(
            country=country,
            datetime_utc__gte=start_utc,
            datetime_utc__lt=end_utc,
        )
        if psr:
            qs = qs.filter(psr_type=psr)

        # Order by time then psr
        rows = qs.order_by("datetime_utc", "psr_type").values(
            "datetime_utc", "psr_type", "psr_name", "generation_mw"
        )

        # Format datetimes as Z strings for the frontend
        def _fmt_z(dt_utc):
            dt_utc = dt_utc if dt_utc.tzinfo else dt_utc.replace(tzinfo=dt.timezone.utc)
            return dt_utc.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        items = []
        for r in rows:
            items.append({
                "datetime_utc": _fmt_z(r["datetime_utc"]),
                "psr_type": r["psr_type"],
                "psr_name": r["psr_name"] or r["psr_type"],
                "generation_mw": r["generation_mw"],
            })

        payload = {
            "country": country.pk,
            "date_label": label,
            "start_utc": _fmt_z(start_utc),
            "end_utc": _fmt_z(end_utc),
            "items": items,
        }
        return Response(payload, status=status.HTTP_200_OK)
