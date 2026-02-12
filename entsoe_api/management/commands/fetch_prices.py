# entsoe_api/management/commands/fetch_prices.py

from __future__ import annotations

import os
import time
import datetime as dt
import warnings
from typing import Dict, List, Union, Optional

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

import pandas as pd

from entsoe_api.entsoe_data import EntsoePrices
from entsoe_api.helper import save_country_prices_df
from dotenv import load_dotenv

load_dotenv()

# Suppress Django timezone warnings since we handle timezone conversion
warnings.filterwarnings('ignore', category=RuntimeWarning, module='django.db.models.fields')


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

    if not start_s or not end_s:
        raise ValueError(
            "Provide start & end (ISO UTC, e.g. 2025-09-18T00:00:00Z) "
            "or use period=today|dayahead."
        )

    start = _parse_iso_utc_floor_hour(start_s)
    end = _parse_iso_utc_floor_hour(end_s)
    return start, end


def _ensure_timezone_aware(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all datetime columns in the DataFrame are timezone-aware (UTC).
    This prevents Django warnings about naive datetimes.
    """
    if df.empty:
        return df
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Find all datetime columns
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    
    for col in datetime_cols:
        # Check if the column has timezone info
        if df[col].dt.tz is None:
            # Convert to UTC timezone
            df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC')
        else:
            # If already timezone-aware, convert to UTC
            df[col] = df[col].dt.tz_convert('UTC')
    
    return df


class Command(BaseCommand):
    help = (
        "Fetch ENTSO-E A44 prices (Day-ahead/Intraday) for one country (--country BG) "
        "or ALL (default), aggregate by country, and store into CountryPricePoint. "
        "Includes rate limiting to prevent API flooding."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--country",
            type=str,
            help="ISO code (e.g. BG, DE). If omitted or 'ALL', fetch all countries from settings.ENTSOE_PRICE_COUNTRY_TO_EICS.",
        )
        parser.add_argument(
            "--contract",
            type=str,
            choices=["A01", "A07"],
            default="A01",
            help="Contract type: A01=Day-ahead, A07=Intraday (default: A01).",
        )
        parser.add_argument(
            "--period",
            type=str,
            choices=["today", "dayahead"],
            help="Shortcut window in UTC. Use this OR --start/--end.",
        )
        parser.add_argument("--start", type=str, help="UTC start ISO, e.g. 2025-09-18T00:00:00Z")
        parser.add_argument("--end", type=str, help="UTC end ISO (exclusive), e.g. 2025-09-19T00:00:00Z")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and show row count but do NOT write to DB.",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=1.0,
            help="Delay in seconds between country requests (default: 1.0). Set to 0 to disable.",
        )
        parser.add_argument(
            "--max-retries",
            type=int,
            default=3,
            help="Maximum number of retries per country on failure (default: 3).",
        )
        parser.add_argument(
            "--continue-on-error",
            action="store_true",
            help="Continue processing other countries even if one fails.",
        )

    def fetch_country_with_retry(
        self,
        country: str,
        eics: Union[str, List[str]],
        api_key: str,
        start: dt.datetime,
        end: dt.datetime,
        contract_type: str,
        max_retries: int,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch data for a single country with exponential backoff retry logic.
        """
        for attempt in range(max_retries):
            try:
                df = EntsoePrices.query_all_countries(
                    api_key=api_key,
                    country_to_eics={country: eics},
                    start=start,
                    end=end,
                    contract_type=contract_type,
                    aggregate_by_country=True,
                    skip_errors=True,
                )
                return df
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    self.stdout.write(
                        self.style.WARNING(
                            f"  ⚠️  Attempt {attempt + 1}/{max_retries} failed for {country}: {str(e)[:100]}"
                        )
                    )
                    self.stdout.write(f"  ⏳ Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self.stdout.write(
                        self.style.ERROR(
                            f"  ❌ All {max_retries} attempts failed for {country}: {str(e)[:100]}"
                        )
                    )
                    return None
        return None

    def handle(self, *args, **opts):
        # --- API key ---
        api_key = getattr(settings, "ENTSOE_API_KEY", "") or os.getenv("ENTSOE_API_KEY") or os.getenv("ENTSOE_TOKEN")
        if not api_key:
            raise CommandError("Missing ENTSOE_API_KEY or ENTSOE_TOKEN (settings or .env).")

        # --- mapping (prices use bidding zones) ---
        mapping = getattr(settings, "ENTSOE_PRICE_COUNTRY_TO_EICS", None)
        if not isinstance(mapping, dict) or not mapping:
            raise CommandError("Define ENTSOE_PRICE_COUNTRY_TO_EICS in settings.py")

        # --- country selection ---
        country_arg = (opts.get("country") or "ALL").upper()
        if country_arg != "ALL":
            if country_arg not in mapping:
                known = ", ".join(sorted(mapping.keys()))
                raise CommandError(f"Unknown country '{country_arg}'. Known: {known}")
            country_to_eics: Dict[str, Union[str, List[str]]] = {country_arg: mapping[country_arg]}
        else:
            country_to_eics = mapping

        # --- window ---
        try:
            start_utc, end_utc = _compute_window_utc(opts.get("period"), opts.get("start"), opts.get("end"))
        except ValueError as e:
            raise CommandError(str(e))
        if start_utc >= end_utc:
            raise CommandError("--start must be earlier than --end.")

        contract = (opts.get("contract") or "A01").upper()
        if opts.get("period") == "dayahead" and contract != "A01":
            raise CommandError("period=dayahead is only valid with --contract=A01 (Day-ahead).")

        delay = opts.get("delay", 1.0)
        max_retries = opts.get("max_retries", 3)
        continue_on_error = opts.get("continue_on_error", False)
        dry_run = opts.get("dry_run", False)

        # --- Display configuration ---
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("ENTSO-E Price Data Fetch"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"📅 Time Range: {start_utc:%Y-%m-%d %H:%MZ} to {end_utc:%Y-%m-%d %H:%MZ}")
        self.stdout.write(f"🌍 Countries: {len(country_to_eics)} ({', '.join(sorted(country_to_eics.keys()))})")
        self.stdout.write(f"📊 Contract Type: {contract} ({'Day-ahead' if contract == 'A01' else 'Intraday'})")
        self.stdout.write(f"⏱️  Delay between requests: {delay}s")
        self.stdout.write(f"🔄 Max retries: {max_retries}")
        if dry_run:
            self.stdout.write(self.style.WARNING("🔍 DRY RUN MODE: Data will NOT be saved to database"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write("")

        # --- fetch country by country with rate limiting ---
        all_dfs = []  # Only used for dry-run or if we need to track all data
        total_countries = len(country_to_eics)
        successful = 0
        failed = 0
        total_rows_retrieved = 0
        total_rows_saved = 0
        countries_no_data = []  # Countries that returned empty data
        countries_failed = []   # Countries that failed to fetch
        countries_save_failed = []  # Countries that fetched but failed to save
        start_time = time.time()

        for idx, (country, eics) in enumerate(sorted(country_to_eics.items()), 1):
            self.stdout.write(f"[{idx}/{total_countries}] Fetching {country}...")
            
            df = self.fetch_country_with_retry(
                country=country,
                eics=eics,
                api_key=api_key,
                start=start_utc,
                end=end_utc,
                contract_type=contract,
                max_retries=max_retries,
            )
            
            if df is not None and not df.empty:
                rows_retrieved = len(df)
                total_rows_retrieved += rows_retrieved
                
                # Ensure timezone-aware datetimes before saving
                df = _ensure_timezone_aware(df)
                
                if dry_run:
                    # In dry-run mode, just count rows
                    successful += 1
                    self.stdout.write(
                        self.style.SUCCESS(f"  ✅ {country}: Retrieved {rows_retrieved} rows (dry-run, not saved)")
                    )
                    all_dfs.append(df)
                else:
                    # Save to database immediately (batch save per country)
                    try:
                        written = save_country_prices_df(df)
                        total_rows_saved += written
                        successful += 1
                        self.stdout.write(
                            self.style.SUCCESS(f"  ✅ {country}: Retrieved {rows_retrieved} rows, saved {written} to DB")
                        )
                    except Exception as e:
                        failed += 1
                        countries_save_failed.append(country)
                        self.stdout.write(
                            self.style.ERROR(f"  ❌ {country}: Retrieved {rows_retrieved} rows but failed to save: {str(e)[:100]}")
                        )
                        if not continue_on_error:
                            raise CommandError(f"Failed to save data for {country}: {e}")
                    
            elif df is not None and df.empty:
                countries_no_data.append(country)
                self.stdout.write(
                    self.style.WARNING(f"  ⚠️  {country}: No data available")
                )
            else:
                failed += 1
                countries_failed.append(country)
                if not continue_on_error:
                    raise CommandError(f"Failed to fetch data for {country}. Use --continue-on-error to skip failed countries.")
                self.stdout.write(
                    self.style.ERROR(f"  ❌ {country}: Failed (continuing...)")
                )
            
            # Rate limiting: wait between countries (except after the last one)
            if idx < total_countries and delay > 0:
                time.sleep(delay)

        elapsed_time = time.time() - start_time

        # --- Summary ---
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("FETCH SUMMARY"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"✅ Successful: {successful}/{total_countries}")
        self.stdout.write(f"❌ Failed: {failed}/{total_countries}")
        self.stdout.write(f"📊 Total rows retrieved: {total_rows_retrieved}")
        if not dry_run:
            self.stdout.write(f"💾 Total rows saved to DB: {total_rows_saved}")
        else:
            self.stdout.write(f"🔍 Dry-run: {total_rows_retrieved} rows would have been saved")
        self.stdout.write(f"⏱️  Total time: {elapsed_time:.2f}s")
        self.stdout.write(self.style.SUCCESS("=" * 70))
        
        # --- Detailed breakdown of issues ---
        if countries_no_data or countries_failed or countries_save_failed:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("⚠️  ISSUES DETECTED"))
            self.stdout.write(self.style.WARNING("=" * 70))
            
            if countries_no_data:
                self.stdout.write(
                    self.style.WARNING(
                        f"📭 No data available ({len(countries_no_data)}): {', '.join(sorted(countries_no_data))}"
                    )
                )
            
            if countries_failed:
                self.stdout.write(
                    self.style.ERROR(
                        f"❌ Failed to fetch ({len(countries_failed)}): {', '.join(sorted(countries_failed))}"
                    )
                )
            
            if countries_save_failed:
                self.stdout.write(
                    self.style.ERROR(
                        f"💥 Failed to save ({len(countries_save_failed)}): {', '.join(sorted(countries_save_failed))}"
                    )
                )
            
            self.stdout.write(self.style.WARNING("=" * 70))
        
        self.stdout.write("")