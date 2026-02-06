# yourapp/ingest_country.py
from __future__ import annotations
from typing import List, Dict, Optional
from django.db import transaction
from .models import (
    Country,
    CountryCapacitySnapshot,
    CountryGenerationByType,
    CountryResGenerationByType,
    CountryGenerationForecastByType,
    CountryPricePoint,
    PhysicalFlow,
)
from functools import lru_cache
from typing import Dict, Iterable, Optional, Union

from datetime import timezone
from django.conf import settings


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


@transaction.atomic
def save_generation_forecast_df(df) -> int:
    """Persist country-level generation forecast rows."""

    if df.empty:
        return 0

    required = {"country", "datetime_utc", "psr_type", "forecast_MW"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {missing} for CountryGenerationForecastByType")

    n = 0
    for r in df.to_dict(orient="records"):
        c = _get_or_create_country(r["country"])
        defaults = {
            "psr_name": r.get("psr_name") or r["psr_type"],
            "forecast_mw": r.get("forecast_MW"),
            "resolution": r.get("resolution") or "",
        }
        CountryGenerationForecastByType.objects.update_or_create(
            country=c,
            psr_type=r["psr_type"],
            datetime_utc=r["datetime_utc"],
            defaults=defaults,
        )
        n += 1
    return n

@transaction.atomic
def save_generation_res_df(df) -> int:
    """
    Persist RES generation rows (A69).

    Expected columns:
      - datetime_utc
      - psr_type
      - generation_mw or generation_MW
    Optional:
      - psr_name, unit, resolution, country (ISO), zone (EIC)
    """
    if df is None or df.empty:
        return 0

    df = df.copy()
    if "generation_mw" not in df.columns and "generation_MW" in df.columns:
        df.rename(columns={"generation_MW": "generation_mw"}, inplace=True)

    required = {"datetime_utc", "psr_type", "generation_mw"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {missing} for CountryResGenerationByType")

    if "country" not in df.columns:
        if "zone" not in df.columns:
            raise ValueError("Missing 'country' or 'zone' column for CountryResGenerationByType")
        eic_to_iso = _eic_to_country_iso_map()
        df["country"] = df["zone"].map(lambda z: eic_to_iso.get(str(z).strip()) if z else None)

    df = df[df["country"].notna()]
    if df.empty:
        return 0

    df["datetime_utc"] = df["datetime_utc"].apply(_ensure_aware_utc)

    n = 0
    for r in df.to_dict(orient="records"):
        c = _get_or_create_country(r["country"])
        defaults = {
            "psr_name": r.get("psr_name") or r["psr_type"],
            "generation_mw": r.get("generation_mw"),
            "unit": r.get("unit") or "",
            "resolution": r.get("resolution") or "",
        }
        CountryResGenerationByType.objects.update_or_create(
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




def _normalize_eic(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    s = str(x).strip()
    return s if s else None

def _iter_eics(val: Union[str, Iterable[str]]) -> Iterable[str]:
    """
    Yield normalized EIC strings from either a single string or an iterable of strings.
    Skips falsy/empty values.
    """
    if isinstance(val, (list, tuple, set)):
        for item in val:
            s = _normalize_eic(item)
            if s:
                yield s
    else:
        s = _normalize_eic(val)
        if s:
            yield s

@lru_cache(maxsize=1)
def _eic_to_country_iso_map() -> Dict[str, str]:
    """
    Build mapping: EIC domain -> ISO2 country code.

    Supports:
      - settings.ENTSOE_COUNTRY_TO_EICS: { "BG": "10Y...", "DE": ["10Y..", "10Y.."] }
      - settings.ENTSOE_PRICE_COUNTRY_TO_EICS: legacy key with same shape
      - settings.ENTSOE_DOMAIN_EIC_TO_COUNTRY: direct { "10Y...": "BG" } overrides
    """
    mapping: Dict[str, str] = {}

    # Prefer ENTSOE_COUNTRY_TO_EICS, else fallback to ENTSOE_PRICE_COUNTRY_TO_EICS
    country_to_eics = getattr(settings, "ENTSOE_COUNTRY_TO_EICS", None)
    if not isinstance(country_to_eics, dict) or not country_to_eics:
        country_to_eics = getattr(settings, "ENTSOE_PRICE_COUNTRY_TO_EICS", {}) or {}

    # Reverse map: (may be str or list[str])
    for iso, eics in country_to_eics.items():
        if not iso:
            continue
        iso2 = str(iso).upper().strip()
        for eic in _iter_eics(eics):
            mapping[eic] = iso2

    # Optional explicit overrides/extensions
    explicit = getattr(settings, "ENTSOE_DOMAIN_EIC_TO_COUNTRY", None)
    if isinstance(explicit, dict):
        for eic, iso in explicit.items():
            seic = _normalize_eic(eic)
            siso = str(iso).upper().strip() if iso else None
            if seic and siso:
                mapping[seic] = siso

    return mapping

@lru_cache(maxsize=None)
def _country_by_iso(iso: str) -> Optional[Country]:
    try:
        return Country.objects.get(pk=str(iso).upper())
    except Country.DoesNotExist:
        return None

def _map_eic_to_country(eic: Optional[str]) -> Optional[Country]:
    if not eic:
        return None
    iso = _eic_to_country_iso_map().get(eic)
    if not iso:
        return None
    return _country_by_iso(iso)

def save_flows_df(df: pd.DataFrame) -> int:
    """
    Save parsed A11 flows DataFrame into PhysicalFlow table.

    Expected columns after normalization:
      - 'datetime_utc' (tz-aware or naive UTC timestamp)
      - 'out_domain_eic' (str)
      - 'in_domain_eic'  (str)
      - 'quantity_mw' or 'quantity_MW' (numeric)
      - optional 'resolution' (str)

    Behavior:
      - Keeps original EIC pair (for uniqueness).
      - Also populates country_from / country_to using EIC->Country mapping.
      - Upserts per (datetime_utc, out_domain_eic, in_domain_eic).
    """
    if df is None or df.empty:
        return 0

    df = df.copy()

    # Normalize quantity column name
    if "quantity_mw" not in df.columns and "quantity_MW" in df.columns:
        df = df.rename(columns={"quantity_MW": "quantity_mw"})

    required = {"datetime_utc", "out_domain_eic", "in_domain_eic", "quantity_mw"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"save_flows_df missing columns: {missing}")

    # Normalize datetimes (force UTC, pandas handles tz-naive as UTC if utc=True)
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)

    # Coerce quantity to float (preserve None where appropriate)
    def _to_float(x):
        if pd.isna(x):
            return None
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    df["quantity_mw"] = df["quantity_mw"].map(_to_float)

    # Iterate as records and upsert
    written = 0
    with transaction.atomic():
        for rec in df.to_dict(orient="records"):
            dt = pd.to_datetime(rec["datetime_utc"], utc=True).to_pydatetime()
            out_eic = rec.get("out_domain_eic")
            in_eic = rec.get("in_domain_eic")
            qty = rec.get("quantity_mw")
            res = rec.get("resolution")

            # Map to countries (best-effort)
            c_from = _map_eic_to_country(out_eic)
            c_to = _map_eic_to_country(in_eic)

            # Upsert by original EIC pair uniqueness
            _, _created = PhysicalFlow.objects.update_or_create(
                datetime_utc=dt,
                out_domain_eic=out_eic,
                in_domain_eic=in_eic,
                defaults={
                    "quantity_mw": qty,
                    "resolution": res,
                    "country_from": c_from,
                    "country_to": c_to,
                },
            )
            written += 1

    return written
