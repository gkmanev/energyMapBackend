"""Fetch generation forecasts (A71/A01) and persist in DB."""

import os
import json
import datetime as dt
from typing import Dict, List, Union, Optional

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from dotenv import load_dotenv

from entsoe_api.entsoe_data import EntsoeGenerationForecastByType
from entsoe_api.helper import save_generation_forecast_df

load_dotenv()


def _parse_iso_utc(s: str) -> dt.datetime:
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


class Command(BaseCommand):
    help = (
        "Fetch ENTSO-E Generation Forecast per Production Type (A71/A01) "
        "for one country (--country ISO) or all countries (default)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--country",
            type=str,
            help=(
                "ISO code (e.g. 'CZ'). If omitted or 'ALL', uses the mapping "
                "from settings.ENTSOE_COUNTRY_TO_EICS."
            ),
        )
        parser.add_argument(
            "--psr-type",
            type=str,
            help="Optional PSR code filter (e.g. 'B16' for Solar).",
        )
        parser.add_argument(
            "--hours",
            type=int,
            default=24,
            help="Lookback window in hours (default: 24). Ignored if --start/--end provided.",
        )
        parser.add_argument(
            "--start",
            type=str,
            help="UTC start (ISO). Example: '2025-11-19T00:00:00Z'.",
        )
        parser.add_argument(
            "--end",
            type=str,
            help="UTC end (ISO, exclusive). Example: '2025-11-20T00:00:00Z'.",
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
            help="Optional stdout dump (default json).",
        )
        parser.add_argument(
            "--output",
            type=str,
            help="If provided, write the dump to this path instead of stdout.",
        )

    def handle(self, *args, **options):
        api_key = os.getenv("ENTSOE_TOKEN")
        if not api_key:
            raise CommandError("Missing ENTSOE_TOKEN env variable.")

        mapping = getattr(settings, "ENTSOE_COUNTRY_TO_EICS", None)
        if not isinstance(mapping, dict) or not mapping:
            raise CommandError("settings.ENTSOE_COUNTRY_TO_EICS is missing or empty.")

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

        start_opt = options.get("start")
        end_opt = options.get("end")

        if start_opt or end_opt:
            if not (start_opt and end_opt):
                raise CommandError("Provide BOTH --start and --end, or neither.")
            try:
                start = _parse_iso_utc(start_opt)
                end = _parse_iso_utc(end_opt)
            except Exception:
                raise CommandError("Invalid --start/--end format. Use ISO like 2025-11-19T00:00:00Z")
            if start >= end:
                raise CommandError("--start must be earlier than --end.")
            start = _floor_to_step(start, 15)
            end = _floor_to_step(end, 15)
        else:
            hours = options["hours"]
            if hours <= 0:
                raise CommandError("--hours must be > 0.")
            now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
            end = now_utc.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
            start = end - dt.timedelta(hours=hours)

        df = EntsoeGenerationForecastByType.query_all_countries(
            api_key=api_key,
            country_to_eics=country_to_eics,
            start=start,
            end=end,
            psr_type=psr_type,
            aggregate_by_country=aggregate,
            skip_errors=True,
        )

        written = save_generation_forecast_df(df)
        self.stdout.write(self.style.SUCCESS(f"Saved {written} generation forecast rows."))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data returned."))
            return

        out_fmt = options["format"]
        out_path = options.get("output")

        export_df = df.copy()
        if "forecast_MW" in export_df.columns:
            export_df.rename(columns={"forecast_MW": "forecast_mw"}, inplace=True)

        if out_fmt == "json":
            payload = export_df.to_dict(orient="records")
            serialized = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
            if out_path:
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(serialized)
                self.stdout.write(self.style.SUCCESS(f"Wrote JSON to {out_path}"))
            else:
                self.stdout.write(serialized)
        else:
            if out_path:
                export_df.to_csv(out_path, index=False)
                self.stdout.write(self.style.SUCCESS(f"Wrote CSV to {out_path}"))
            else:
                self.stdout.write(export_df.to_csv(index=False))
