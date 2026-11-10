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

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from dotenv import load_dotenv

from entsoe_api import epias
from entsoe_api.helper import save_country_prices_df

load_dotenv()

MCP_PATH = "/v1/markets/dam/data/mcp"

# EPİAŞ response field per currency
CURRENCY_FIELDS = {"EUR": "priceEur", "USD": "priceUsd", "TRY": "price"}


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

    def handle(self, *args, **opts):
        username, password = epias.get_credentials()
        start, end = epias.compute_dates(opts.get("period"), opts.get("start"), opts.get("end"))
        currency = opts["currency"]
        dry_run = opts["dry_run"]

        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("EPİAŞ Turkey Day-Ahead Price (PTF) Fetch"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"📅 Dates (TR local, inclusive): {start} → {end}")
        self.stdout.write(f"💱 Currency: {currency}")
        if dry_run:
            self.stdout.write(self.style.WARNING("🔍 DRY RUN MODE: Data will NOT be saved to database"))

        tgt = epias.get_tgt(username, password)

        frames = []
        for cur, chunk_end in epias.iter_chunks(start, end, opts["chunk_days"]):
            self.stdout.write(f"  ⬇️  Fetching {cur} → {chunk_end} ...")
            js = epias.post_with_retry(
                tgt, MCP_PATH, epias.date_range_body(cur, chunk_end),
                max_retries=opts["max_retries"], log=lambda m: self.stdout.write(self.style.WARNING(m)),
            )
            df = items_to_df(js.get("items") or [], currency)
            self.stdout.write(f"     {len(df)} rows")
            if not df.empty:
                frames.append(df)

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
