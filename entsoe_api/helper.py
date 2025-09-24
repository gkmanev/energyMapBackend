# yourapp/ingest_country.py
from typing import List
from django.db import transaction
from .models import Country, CountryCapacitySnapshot, CountryGenerationByType, CountryPricePoint
from datetime import timezone
import pandas as pd
def _get_or_create_country(iso: str) -> Country:
    obj, _ = Country.objects.get_or_create(pk=iso)
    return obj

@transaction.atomic
def save_capacity_df(df) -> int:
    """
    Expected columns:
      ['country','psr_type','psr_name','installed_capacity_MW','year']
      optional: 'valid_from_utc' (if present, saved)
    """
    if df.empty:
        return 0
    required = {"country","psr_type","installed_capacity_MW","year"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {missing} for CountryCapacitySnapshot")

    n = 0
    for r in df.to_dict(orient="records"):
        c = _get_or_create_country(r["country"])
        defaults = {
            "psr_name": r.get("psr_name") or r["psr_type"],
            "installed_capacity_mw": r.get("installed_capacity_MW"),
        }
        if "valid_from_utc" in r and r["valid_from_utc"]:
            defaults["valid_from_utc"] = r["valid_from_utc"]
        else:
            # if missing, infer as Jan 1 of year (00:00Z) or leave a sentinel; here we infer
            from datetime import datetime, timezone
            defaults["valid_from_utc"] = datetime(int(r["year"]), 1, 1, tzinfo=timezone.utc)

        obj, _created = CountryCapacitySnapshot.objects.update_or_create(
            country=c,
            psr_type=r["psr_type"],
            year=int(r["year"]),
            defaults=defaults,
        )
        n += 1
    return n


@transaction.atomic
def save_generation_df(df) -> int:
    """
    Expected columns (aggregate_by_country=True):
      ['country','datetime_utc','psr_type','psr_name','generation_MW']
      optional: 'resolution'
    """
    if df.empty:
        return 0
    required = {"country","datetime_utc","psr_type","generation_MW"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {missing} for CountryGenerationByType")

    n = 0
    for r in df.to_dict(orient="records"):
        c = _get_or_create_country(r["country"])
        defaults = {
            "psr_name": r.get("psr_name") or r["psr_type"],
            "generation_mw": r.get("generation_MW"),
            "resolution": r.get("resolution") or "",
        }
        obj, _created = CountryGenerationByType.objects.update_or_create(
            country=c,
            psr_type=r["psr_type"],
            datetime_utc=r["datetime_utc"],
            defaults=defaults,
        )
        n += 1
    return n

def _ensure_aware_utc(x):
    if isinstance(x, str):
        x = pd.to_datetime(x, utc=True).to_pydatetime()
    if getattr(x, "tzinfo", None) is None:
        return x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

@transaction.atomic
def save_country_prices_df(df: pd.DataFrame) -> int:
    """
    Upsert country prices:
      - If (country, contract_type, datetime_utc) exists -> update price/currency/unit/resolution
      - Else -> create
    Expected df columns (aggregated-by-country): 
      ['country','datetime_utc','price','currency','unit','contract_type'].
    """
    if df is None or df.empty:
        return 0

    required = {"country","datetime_utc","price","contract_type"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {missing}. Did you fetch with aggregate_by_country=True?")

    # Normalize datetimes to aware UTC
    df = df.copy()
    df["datetime_utc"] = df["datetime_utc"].apply(_ensure_aware_utc)

    # Build key tuples for lookup
    keys: list[tuple[str,str,object]] = [
        (r["country"], r["contract_type"], r["datetime_utc"]) for r in df.to_dict("records")
    ]

    # Map (country, contract_type, dt) -> existing id
    countries = {c.iso_code: c for c in Country.objects.filter(iso_code__in=df["country"].unique())}
    # Create missing Country rows on the fly
    for iso in df["country"].unique():
        if iso not in countries:
            countries[iso], _ = Country.objects.get_or_create(pk=iso, defaults={"name": iso})

    existing_qs = CountryPricePoint.objects.filter(
        country__in=countries.values(),
        contract_type__in=df["contract_type"].unique(),
        datetime_utc__in=list(df["datetime_utc"].unique()),
    ).select_related("country")

    existing_map = {
        (obj.country.iso_code, obj.contract_type, obj.datetime_utc): obj
        for obj in existing_qs
    }

    to_create: list[CountryPricePoint] = []
    to_update: list[CountryPricePoint] = []

    for r in df.to_dict("records"):
        iso = r["country"]
        ct  = r.get("contract_type") or "A01"
        ts  = r["datetime_utc"]
        price = r.get("price")
        currency = r.get("currency") or "EUR"
        unit = r.get("unit") or "MWH"
        res = r.get("resolution") or ""

        key = (iso, ct, ts)
        if key in existing_map:
            obj = existing_map[key]
            obj.price = price
            obj.currency = currency
            obj.unit = unit
            obj.resolution = res
            to_update.append(obj)
        else:
            to_create.append(CountryPricePoint(
                country=countries[iso],
                contract_type=ct,
                datetime_utc=ts,
                price=price,
                currency=currency,
                unit=unit,
                resolution=res,
            ))

    # Bulk ops
    if to_create:
        CountryPricePoint.objects.bulk_create(to_create, ignore_conflicts=True)
    if to_update:
        CountryPricePoint.objects.bulk_update(to_update, ["price","currency","unit","resolution"])

    return len(to_create) + len(to_update)