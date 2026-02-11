# entsoe_api/management/commands/fetch_generation.py

import os
import json
import time
import datetime as dt
import warnings
from typing import Dict, List, Union, Optional

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

import pandas as pd

# ✅ Adjust this import to where your class lives.
# If your class is in entsoe_api/entsoe_data.py use the following:
from entsoe_api.entsoe_data import EntsoeGenerationByType
# If you saved it elsewhere, e.g. entsoe_api/entsoe_generation_by_type.py:
# from entsoe_api.entsoe_generation_by_type import EntsoeGenerationByType

from dotenv import load_dotenv
from entsoe_api.helper import save_generation_df
load_dotenv()

# Suppress Django timezone warnings since we handle timezone conversion
warnings.filterwarnings('ignore', category=RuntimeWarning, module='django.db.models.fields')


def _parse_iso_utc(s: str) -> dt.datetime:
    """
    Accepts 'YYYY-MM-DDTHH:MM[:SS][Z]' or without 'Z'.
    Returns tz-aware UTC datetime.
    """
    if not s:
        raise ValueError("empty datetime string")
    s2 = s.rstrip("Z")
    d = dt.datetime.fromisoformat(s2)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    else:
        d = d.astimezone(dt.timezone.utc)
    return d


def _floor_to_step(d: dt.datetime, minutes: int = 60) -> dt.datetime:
    d = d.astimezone(dt.timezone.utc).replace(second=0, microsecond=0)
    return d - dt.timedelta(minutes=d.minute % minutes)


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
        "Fetch ENTSO-E Actual Generation per Production Type (A75/A16) "
        "for one country (--country ISO) or all countries (default). "
        "Auto-aligns time window to 15-minute MTU boundaries. "
        "Includes rate limiting to prevent API flooding."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--country",
            type=str,
            help="ISO code (e.g. 'CZ'). If omitted or 'ALL', uses all from settings.ENTSOE_COUNTRY_TO_EICS.",
        )
        parser.add_argument(
            "--psr-type",
            type=str,
            help="Optional PSR code filter (e.g. 'B16' for Solar).",
        )
        # Time window: either last N hours, or explicit start/end
        parser.add_argument(
            "--hours",
            type=int,
            default=24,
            help="Lookback window in hours (default: 24). Ignored if --start/--end provided.",
        )
        parser.add_argument(
            "--start",
            type=str,
            help="UTC start (ISO). Example: '2025-09-10T00:00:00Z'.",
        )
        parser.add_argument(
            "--end",
            type=str,
            help="UTC end (ISO, exclusive). Example: '2025-09-17T00:00:00Z'.",
        )
        parser.add_argument(
            "--no-aggregate",
            action="store_true",
            help="Do not aggregate across zones; keep one row per country×zone×PSR×timestamp.",
        )
        parser.add_argument(
            "--format",
            choices=["json", "csv"],
            default="json",
            help="Output format (default: json).",
        )
        parser.add_argument(
            "--output",
            type=str,
            help="If provided, write output to this file; otherwise print to stdout.",
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
        psr_type: Optional[str],
        aggregate: bool,
        max_retries: int,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch data for a single country with exponential backoff retry logic.
        """
        for attempt in range(max_retries):
            try:
                df = EntsoeGenerationByType.query_all_countries(
                    api_key=api_key,
                    country_to_eics={country: eics},
                    start=start,
                    end=end,
                    psr_type=psr_type,
                    aggregate_by_country=aggregate,
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

    def handle(self, *args, **options):
        # --- API key ---
        api_key = os.getenv("ENTSOE_TOKEN")
        if not api_key:
            raise CommandError("Missing ENTSOE_TOKEN. Put it in .env file or export ENTSOE_TOKEN=your_key")

        # --- mapping ---
        mapping = getattr(settings, "ENTSOE_COUNTRY_TO_EICS", None)
        if not isinstance(mapping, dict) or not mapping:
            raise CommandError("settings.ENTSOE_COUNTRY_TO_EICS is missing or empty.")

        # --- one country or all ---
        country_arg = (options.get("country") or "ALL").upper()
        if country_arg != "ALL":
            if country_arg not in mapping:
                known = ", ".join(sorted(mapping.keys()))
                raise CommandError(f"Unknown country '{country_arg}'. Known: {known}")
            country_to_eics: Dict[str, Union[str, List[str]]] = {country_arg: mapping[country_arg]}
        else:
            country_to_eics = mapping

        psr_type = options.get("psr_type")
        aggregate = not options.get("no_aggregate")
        delay = options.get("delay", 1.0)
        max_retries = options.get("max_retries", 3)
        continue_on_error = options.get("continue_on_error", False)

        # --- time window (auto-align to 15-min MTU) ---
        start_opt = options.get("start")
        end_opt = options.get("end")

        if start_opt or end_opt:
            if not (start_opt and end_opt):
                raise CommandError("Provide BOTH --start and --end, or neither.")
            try:
                start = _parse_iso_utc(start_opt)
                end = _parse_iso_utc(end_opt)
            except Exception:
                raise CommandError("Invalid --start/--end format. Use ISO e.g. 2025-09-10T00:00:00Z")
            if start >= end:
                raise CommandError("--start must be earlier than --end.")
            # floor both to 15-min boundaries
            start = _floor_to_step(start, 15)
            end = _floor_to_step(end, 15)
        else:
            hours = options["hours"]
            if hours <= 0:
                raise CommandError("--hours must be > 0.")
            now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
            end = now_utc.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)  # exclusive
            start = end - dt.timedelta(hours=hours)

        # --- Display configuration ---
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("ENTSO-E Generation Data Fetch"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"📅 Time Range: {start} to {end}")
        self.stdout.write(f"🌍 Countries: {len(country_to_eics)} ({', '.join(sorted(country_to_eics.keys()))})")
        if psr_type:
            self.stdout.write(f"⚡ PSR Type Filter: {psr_type}")
        self.stdout.write(f"📊 Aggregate by country: {aggregate}")
        self.stdout.write(f"⏱️  Delay between requests: {delay}s")
        self.stdout.write(f"🔄 Max retries: {max_retries}")
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write("")

        # --- fetch country by country with rate limiting ---
        all_dfs = []  # Only used if output file is requested
        total_countries = len(country_to_eics)
        successful = 0
        failed = 0
        total_rows_retrieved = 0
        total_rows_saved = 0
        start_time = time.time()

        for idx, (country, eics) in enumerate(sorted(country_to_eics.items()), 1):
            self.stdout.write(f"[{idx}/{total_countries}] Fetching {country}...")
            
            df = self.fetch_country_with_retry(
                country=country,
                eics=eics,
                api_key=api_key,
                start=start,
                end=end,
                psr_type=psr_type,
                aggregate=aggregate,
                max_retries=max_retries,
            )
            
            if df is not None and not df.empty:
                rows_retrieved = len(df)
                total_rows_retrieved += rows_retrieved
                
                # Ensure timezone-aware datetimes before saving
                df = _ensure_timezone_aware(df)
                
                # Save to database immediately (batch save per country)
                try:
                    written = save_generation_df(df)
                    total_rows_saved += written
                    successful += 1
                    self.stdout.write(
                        self.style.SUCCESS(f"  ✅ {country}: Retrieved {rows_retrieved} rows, saved {written} to DB")
                    )
                except Exception as e:
                    failed += 1
                    self.stdout.write(
                        self.style.ERROR(f"  ❌ {country}: Retrieved {rows_retrieved} rows but failed to save: {str(e)[:100]}")
                    )
                    if not continue_on_error:
                        raise CommandError(f"Failed to save data for {country}: {e}")
                
                # Keep in memory only if we need to output to file
                if options.get("output"):
                    all_dfs.append(df)
                    
            elif df is not None and df.empty:
                self.stdout.write(
                    self.style.WARNING(f"  ⚠️  {country}: No data available")
                )
            else:
                failed += 1
                if not continue_on_error:
                    raise CommandError(f"Failed to fetch data for {country}. Use --continue-on-error to skip failed countries.")
                self.stdout.write(
                    self.style.ERROR(f"  ❌ {country}: Failed (continuing...)")
                )
            
            # Rate limiting: wait between countries (except after the last one)
            if idx < total_countries and delay > 0:
                time.sleep(delay)

        elapsed_time = time.time() - start_time

        # --- Combine all dataframes (only needed for file output) ---
        if all_dfs:
            df = pd.concat(all_dfs, ignore_index=True)
        else:
            df = pd.DataFrame()

        # --- Summary ---
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("FETCH SUMMARY"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"✅ Successful: {successful}/{total_countries}")
        self.stdout.write(f"❌ Failed: {failed}/{total_countries}")
        self.stdout.write(f"📊 Total rows retrieved: {total_rows_retrieved}")
        self.stdout.write(f"💾 Total rows saved to DB: {total_rows_saved}")
        self.stdout.write(f"⏱️  Total time: {elapsed_time:.2f}s")
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write("")

        # --- output to file/stdout ---
        out_fmt = options["format"]
        out_path = options.get("output")

        if not df.empty and out_path:
            try:
                if out_fmt == "json":
                    records = EntsoeGenerationByType.to_records(df, datetime_cols=["datetime_utc"])
                    payload = json.dumps(records, ensure_ascii=False, indent=2)
                    with open(out_path, "w", encoding="utf-8") as f:
                        f.write(payload)
                    self.stdout.write(self.style.SUCCESS(f"📄 Wrote JSON to {out_path}"))
                else:  # csv
                    df.to_csv(out_path, index=False)
                    self.stdout.write(self.style.SUCCESS(f"📄 Wrote CSV to {out_path}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Failed to write output file: {e}"))
        elif not df.empty and not out_path:
            # Print to stdout only if no output file specified
            if out_fmt == "json":
                records = EntsoeGenerationByType.to_records(df, datetime_cols=["datetime_utc"])
                payload = json.dumps(records, ensure_ascii=False, indent=2)
                self.stdout.write(payload)
            else:  # csv
                self.stdout.write(df.to_csv(index=False))