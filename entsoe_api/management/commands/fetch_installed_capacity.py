import os
import json
import datetime as dt
from typing import Dict, List, Union, Optional

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

import pandas as pd

# ✅ adjust import to where you saved the class
from entsoe_api.entsoe_data import EntsoeInstalledCapacity
from entsoe_api.helper import save_capacity_df
from dotenv import load_dotenv

load_dotenv()

class Command(BaseCommand):
    help = (
        "Fetch the latest ENTSO-E Installed Capacity per Production Type (A68/A33) "
        "for one country (--country ISO) or all countries (default)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--country",
            type=str,
            help="ISO country code (e.g. 'CZ'). If omitted or 'ALL', fetches all countries from settings.ENTSOE_COUNTRY_TO_EICS.",
        )
        parser.add_argument(
            "--psr-type",
            type=str,
            help="Optional PSR code filter (e.g. 'B16' for Solar). Omit to fetch all types.",
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
            help="If provided, write output to this file path; otherwise print to stdout.",
        )
        parser.add_argument(
            "--now-utc",
            type=str,
            help="Advanced: ISO timestamp to anchor 'current year' (e.g. '2025-01-10T00:00:00Z'). Normally omit.",
        )

    def handle(self, *args, **options):
        # --- API key ---
        api_key = os.getenv("ENTSOE_TOKEN")
        if not api_key:
            raise CommandError("Missing ENTSOE_API_KEY. Put it in settings.ENTSOE_API_KEY or env ENTSOE_API_KEY.")

        # --- mapping (required) ---
        mapping = getattr(settings, "ENTSOE_COUNTRY_TO_EICS", None)
        if not isinstance(mapping, dict) or not mapping:
            raise CommandError("settings.ENTSOE_COUNTRY_TO_EICS is missing or empty.")

        # --- choose country/all ---
        country_arg = (options.get("country") or "ALL").upper()
        if country_arg != "ALL":
            if country_arg not in mapping:
                known = ", ".join(sorted(mapping.keys()))
                raise CommandError(f"Unknown country '{country_arg}'. Known: {known}")
            country_to_eics = {country_arg: mapping[country_arg]}
        else:
            country_to_eics = mapping

        # --- optional filters/anchors ---
        psr_type = options.get("psr_type")
        now_utc_opt = options.get("now_utc")
        now_utc: Optional[dt.datetime] = None
        if now_utc_opt:
            try:
                ts = now_utc_opt.rstrip("Z")
                now_utc = dt.datetime.fromisoformat(ts)
                if now_utc.tzinfo is None:
                    now_utc = now_utc.replace(tzinfo=dt.timezone.utc)
                else:
                    now_utc = now_utc.astimezone(dt.timezone.utc)
            except Exception:
                raise CommandError(f"Invalid --now-utc value: {now_utc_opt}")

        # --- fetch (always aggregated per country) ---
        df = EntsoeInstalledCapacity.query_all_countries(
            api_key=api_key,
            country_to_eics=country_to_eics,
            psr_type=psr_type,
            aggregate_by_country=True,  # keep it simple
            now_utc=now_utc,
            skip_errors=True,
        )
        written = save_capacity_df(df)
        self.stdout.write(self.style.SUCCESS(f"Saved {written} capacity rows."))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data returned."))
            return

        # --- output ---
        out_format = options["format"]
        out_path = options.get("output")

        if out_format == "json":
            records = EntsoeInstalledCapacity.to_records(df, datetime_cols=[])
            payload = json.dumps(records, ensure_ascii=False, indent=2)
            if out_path:
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(payload)
                self.stdout.write(self.style.SUCCESS(f"Wrote JSON to {out_path}"))
            else:
                self.stdout.write(payload)
        else:
            if out_path:
                df.to_csv(out_path, index=False)
                self.stdout.write(self.style.SUCCESS(f"Wrote CSV to {out_path}"))
            else:
                self.stdout.write(df.to_csv(index=False))
