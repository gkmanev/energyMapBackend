# entsoe_api/management/commands/fetch_prices_tr.py
"""
Fetch Turkish day-ahead market prices (PTF / MCP) from the EPİAŞ Transparency
Platform and store them in CountryPricePoint (country=TR, contract_type=A01).

Credentials (settings or .env):
    EPIAS_USERNAME=you@example.com
    EPIAS_PASSWORD=your-password

Examples:
    python manage.py fetch_prices_tr --period dayahead
    python manage.py fetch_prices_tr --start 2026-11-01 --end 2026-11-10
    python manage.py fetch_prices_tr --period today --currency TRY --dry-run
"""
from __future__ import annotations

import datetime as dt
import os
import time
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from dotenv import load_dotenv

from entsoe_api.helper import save_country_prices_df

load_dotenv()

TGT_URL = "https://giris.epias.com.tr/cas/v1/tickets"
MCP_URL = "https://seffaflik.epias.com.tr/electricity-service/v1/markets/dam/data/mcp"
TR_TZ = ZoneInfo("Europe/Istanbul")

# EPİAŞ response field per currency
CURRENCY_FIELDS = {"EUR": "priceEur", "USD": "priceUsd", "TRY": "price"}


def get_tgt(username: str, password: str) -> str:
    """Log in and return a TGT (valid ~2 hours)."""
    r = requests.post(
        TGT_URL,
        data={"username": username, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "text/plain"},
        timeout=30,
    )
    r.raise_for_status()  # expect 201 Created
    return r.text.strip()


def get_mcp(tgt: str, start: dt.date, end: dt.date) -> list[dict]:
    """Hourly PTF items for Turkish dates [start, end] (inclusive)."""
    body = {
        "startDate": f"{start.isoformat()}T00:00:00+03:00",
        "endDate": f"{end.isoformat()}T00:00:00+03:00",
    }
    r = requests.post(
        MCP_URL,
        json=body,
        headers={"TGT": tgt, "Content-Type": "application/json", "Accept": "application/json"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json().get("items", []) or []


def _compute_dates(period: str | None, start_s: str | None, end_s: str | None) -> tuple[dt.date, dt.date]:
    """Inclusive [start, end] range of Turkish calendar dates."""
    today_tr = dt.datetime.now(TR_TZ).date()
    if period == "today":
        return today_tr, today_tr
    if period == "dayahead":
        d = today_tr + dt.timedelta(days=1)
        return d, d
    if not start_s:
        raise CommandError("Provide --period today|dayahead or --start YYYY-MM-DD [--end YYYY-MM-DD].")
    try:
        start = dt.date.fromisoformat(start_s)
        end = dt.date.fromisoformat(end_s) if end_s else start
    except ValueError as e:
        raise CommandError(f"Invalid date: {e}")
    if start > end:
        raise CommandError("--start must not be after --end.")
    return start, end


def items_to_df(items: list[dict], currency: str) -> pd.DataFrame:
    """Convert EPİAŞ MCP items to the frame expected by save_country_prices_df."""
    if not items:
        return pd.DataFrame()
    field = CURRENCY_FIELDS[currency]
    df = pd.DataFrame(items)
    if "date" not in df.columns or field not in df.columns:
        raise CommandError(
            f"Unexpected EPİAŞ response; columns: {list(df.columns)} (need 'date' and '{field}')."
        )
    out = pd.DataFrame({
        "country": "TR",
        "contract_type": "A01",
        "datetime_utc": pd.to_datetime(df["date"], utc=True),
        "price": pd.to_numeric(df[field], errors="coerce"),
        "currency": currency,
        "unit": "MWH",
        "resolution": "PT60M",
    })
    out = out.dropna(subset=["datetime_utc"]).drop_duplicates(subset=["datetime_utc"], keep="last")
    # Decimal-friendly: NaN -> None
    out["price"] = out["price"].astype(object).where(out["price"].notna(), None)
    return out.sort_values("datetime_utc").reset_index(drop=True)


class Command(BaseCommand):
    help = (
        "Fetch Turkish day-ahead prices (PTF/MCP) from EPİAŞ Transparency Platform "
        "and store into CountryPricePoint (country=TR, contract A01)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--period", choices=["today", "dayahead"],
                            help="Turkish-local day shortcut. Use this OR --start/--end.")
        parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD (Turkish time, inclusive).")
        parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (Turkish time, inclusive). Defaults to --start.")
        parser.add_argument("--currency", choices=sorted(CURRENCY_FIELDS), default="EUR",
                            help="Which EPİAŞ price column to store (default: EUR, matches ENTSO-E prices).")
        parser.add_argument("--chunk-days", type=int, default=31,
                            help="Max days per API request (default: 31).")
        parser.add_argument("--max-retries", type=int, default=3, help="Retries per request (default: 3).")
        parser.add_argument("--dry-run", action="store_true", help="Fetch and show rows but do NOT write to DB.")

    def _fetch_with_retry(self, tgt: str, start: dt.date, end: dt.date, max_retries: int) -> list[dict]:
        for attempt in range(max_retries):
            try:
                return get_mcp(tgt, start, end)
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    raise CommandError(f"EPİAŞ request failed for {start}..{end}: {e}")
                wait = 2 ** attempt
                self.stdout.write(self.style.WARNING(
                    f"  ⚠️  Attempt {attempt + 1}/{max_retries} failed: {str(e)[:100]} — retrying in {wait}s"
                ))
                time.sleep(wait)
        return []

    def handle(self, *args, **opts):
        username = getattr(settings, "EPIAS_USERNAME", "") or os.getenv("EPIAS_USERNAME")
        password = getattr(settings, "EPIAS_PASSWORD", "") or os.getenv("EPIAS_PASSWORD")
        if not username or not password:
            raise CommandError("Missing EPIAS_USERNAME / EPIAS_PASSWORD (settings or .env).")

        start, end = _compute_dates(opts.get("period"), opts.get("start"), opts.get("end"))
        currency = opts["currency"]
        chunk_days = max(1, opts["chunk_days"])
        dry_run = opts["dry_run"]

        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("EPİAŞ Turkey Day-Ahead Price (PTF) Fetch"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"📅 Dates (TR local, inclusive): {start} → {end}")
        self.stdout.write(f"💱 Currency: {currency}")
        if dry_run:
            self.stdout.write(self.style.WARNING("🔍 DRY RUN MODE: Data will NOT be saved to database"))

        try:
            tgt = get_tgt(username, password)
        except requests.RequestException as e:
            raise CommandError(f"EPİAŞ login failed: {e}")

        frames = []
        cur = start
        while cur <= end:
            chunk_end = min(end, cur + dt.timedelta(days=chunk_days - 1))
            self.stdout.write(f"  ⬇️  Fetching {cur} → {chunk_end} ...")
            items = self._fetch_with_retry(tgt, cur, chunk_end, opts["max_retries"])
            df = items_to_df(items, currency)
            self.stdout.write(f"     {len(df)} rows")
            if not df.empty:
                frames.append(df)
            cur = chunk_end + dt.timedelta(days=1)

        if not frames:
            self.stdout.write(self.style.WARNING("⚠️  No data returned (day-ahead is published ~14:00 TR time)."))
            return

        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["datetime_utc"], keep="last")
        self.stdout.write(
            f"📊 {len(df)} rows: {df['datetime_utc'].min():%Y-%m-%d %H:%MZ} → {df['datetime_utc'].max():%Y-%m-%d %H:%MZ}"
        )

        if dry_run:
            self.stdout.write(df.head(24).to_string(index=False))
            self.stdout.write(self.style.SUCCESS(f"🔍 Dry-run: {len(df)} rows would have been saved"))
            return

        written = save_country_prices_df(df)
        self.stdout.write(self.style.SUCCESS(f"💾 Saved {written} rows to CountryPricePoint (TR, A01, {currency})"))
