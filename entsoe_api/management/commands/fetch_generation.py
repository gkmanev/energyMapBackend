# entsoe_api/management/commands/fetch_generation.py

import os
import json
import datetime as dt
from typing import Dict, List, Union, Optional

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

import pandas as pd

# ✅ Adjust this import to where your class lives.
# If your class is in entsoe_api/entsoe_data.py use the following:
from entsoe_api.entsoe_data import EntsoeGenerationByType
# If you saved it elsewhere, e.g. entsoe_api/entsoe_generation_by_type.py:
# from entsoe_api.entsoe_generation_by_type import EntsoeGenerationByType

from dotenv import load_dotenv
from entsoe_api.helper import save_generation_df
load_dotenv()


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


def _floor_to_step(d: dt.datetime, minutes: int = 15) -> dt.datetime:
    """
    Floor a datetime to the previous multiple of `minutes` (UTC), zeroing seconds/micros.
    ENTSO-E periodStart/End must land on an MTU boundary (e.g., 00, 15, 30, 45).
    """
    d = d.astimezone(dt.timezone.utc).replace(second=0, microsecond=0)
    return d - dt.timedelta(minutes=d.minute % minutes)


class Command(BaseCommand):
    help = (
        "Fetch ENTSO-E Actual Generation per Production Type (A75/A16) "
        "for one country (--country ISO) or all countries (default). "
        "Auto-aligns time window to 15-minute MTU boundaries."
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

    def handle(self, *args, **options):
        # --- API key ---
        api_key = os.getenv("ENTSOE_TOKEN")
        if not api_key:
            raise CommandError("Missing ENTSOE_API_KEY. Put it in settings.ENTSOE_API_KEY or env ENTSOE_API_KEY.")

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
            # Align end to the top of the hour (safe for 15-min MTU), start = end - hours
            end = now_utc.replace(minute=0, second=0, microsecond=0)
            start = end - dt.timedelta(hours=hours)

        # --- fetch ---
        df = EntsoeGenerationByType.query_all_countries(
            api_key=api_key,
            country_to_eics=country_to_eics,
            start=start,
            end=end,
            psr_type=psr_type,
            aggregate_by_country=aggregate,
            skip_errors=True,
        )
        written = save_generation_df(df)
        self.stdout.write(self.style.SUCCESS(f"Saved {written} generation rows."))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data returned."))
            return

        # --- output ---
        out_fmt = options["format"]
        out_path = options.get("output")

        if out_fmt == "json":
            records = EntsoeGenerationByType.to_records(df, datetime_cols=["datetime_utc"])
            payload = json.dumps(records, ensure_ascii=False, indent=2)
            if out_path:
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(payload)
                self.stdout.write(self.style.SUCCESS(f"Wrote JSON to {out_path}"))
            else:
                self.stdout.write(payload)
        else:  # csv
            if out_path:
                df.to_csv(out_path, index=False)
                self.stdout.write(self.style.SUCCESS(f"Wrote CSV to {out_path}"))
            else:
                self.stdout.write(df.to_csv(index=False))
