"""Fetch and export BG solar generation as an hourly, fixed-UTC+2 CSV."""

from __future__ import annotations

import datetime as dt
import os
import time
from pathlib import Path

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from entsoe_api.entsoe_data import EntsoeGenerationByType


UTC = dt.timezone.utc
UTC_PLUS_2 = dt.timezone(dt.timedelta(hours=2))


def _parse_datetime(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("a timezone offset is required")
    return parsed.astimezone(UTC)


def _month_ranges(start: dt.datetime, end: dt.datetime):
    """Yield UTC ranges bounded by fixed-UTC+2 calendar-month starts.

    ENTSO-E can reject very large A75 requests, so monthly requests are used
    even though the requested export spans two years.
    """
    current = start
    while current < end:
        local_current = current.astimezone(UTC_PLUS_2)
        next_year = local_current.year + (local_current.month == 12)
        next_month = 1 if local_current.month == 12 else local_current.month + 1
        next_boundary = min(
            dt.datetime(next_year, next_month, 1, tzinfo=UTC_PLUS_2).astimezone(UTC),
            end,
        )
        yield current, next_boundary
        current = next_boundary


class Command(BaseCommand):
    help = (
        "Fetch BG solar generation (B16) in monthly ENTSO-E requests and export "
        "hourly mean MW values with fixed UTC+2 timestamps."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--start",
            default="2024-01-01T00:00:00+02:00",
            help="First timestamp in UTC+2 (default: 2024-01-01T00:00:00+02:00).",
        )
        parser.add_argument(
            "--end",
            default="2026-01-01T00:00:00+02:00",
            help="Exclusive end timestamp in UTC+2 (default: 2026-01-01T00:00:00+02:00).",
        )
        parser.add_argument(
            "--output",
            type=Path,
            default=Path(settings.BASE_DIR) / "data" / "solar_generation_BG_2024_2025_hourly_utc_plus_2.xlsx",
            help="Destination .xlsx (default) or .csv path.",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=1.0,
            help="Seconds to wait between monthly API requests (default: 1).",
        )

    def handle(self, *args, **options):
        try:
            start = _parse_datetime(options["start"])
            end = _parse_datetime(options["end"])
        except ValueError as exc:
            raise CommandError(f"Invalid --start/--end: {exc}") from exc
        if start >= end:
            raise CommandError("--start must be earlier than --end.")
        if options["delay"] < 0:
            raise CommandError("--delay must be zero or greater.")

        api_key = getattr(settings, "ENTSOE_API_KEY", "") or os.getenv("ENTSOE_TOKEN")
        if not api_key:
            raise CommandError("Missing ENTSOE_API_KEY setting or ENTSOE_TOKEN environment variable.")

        country_mapping = getattr(settings, "ENTSOE_COUNTRY_TO_EICS", {})
        bg_eics = country_mapping.get("BG")
        if not bg_eics:
            raise CommandError("BG is missing from settings.ENTSOE_COUNTRY_TO_EICS.")

        frames = []
        ranges = list(_month_ranges(start, end))
        for index, (range_start, range_end) in enumerate(ranges, start=1):
            self.stdout.write(
                f"[{index}/{len(ranges)}] Fetching BG solar "
                f"{range_start:%Y-%m-%d} to {range_end:%Y-%m-%d} UTC"
            )
            try:
                frame = EntsoeGenerationByType.query_all_countries(
                    api_key=api_key,
                    country_to_eics={"BG": bg_eics},
                    start=range_start,
                    end=range_end,
                    psr_type="B16",
                    aggregate_by_country=True,
                    skip_errors=False,
                )
            except Exception as exc:
                raise CommandError(
                    f"ENTSO-E request failed for {range_start:%Y-%m-%d} through "
                    f"{range_end:%Y-%m-%d}: {exc}"
                ) from exc
            if not frame.empty:
                frames.append(frame)
            if index < len(ranges) and options["delay"]:
                time.sleep(options["delay"])

        if not frames:
            raise CommandError("ENTSO-E returned no BG solar generation points.")

        source = pd.concat(frames, ignore_index=True)
        source["datetime_utc"] = pd.to_datetime(source["datetime_utc"], utc=True)
        source = source.drop_duplicates(subset=["datetime_utc", "psr_type"])
        source["hour_utc"] = source["datetime_utc"].dt.floor("h")

        hourly = (
            source.groupby("hour_utc", as_index=False)["generation_MW"]
            .agg(["mean", "count"])
            .reset_index()
            .rename(columns={"mean": "generation_mw", "count": "source_samples"})
        )
        hourly["timestamp_utc_plus_2"] = (
            hourly.pop("hour_utc") + pd.Timedelta(hours=2)
        ).dt.strftime("%Y-%m-%dT%H:%M:%S+02:00")
        hourly.insert(0, "country", "BG")
        hourly.insert(1, "psr_type", "B16")
        hourly.insert(2, "psr_name", "Solar")
        hourly.insert(4, "resolution", "PT60M")
        hourly = hourly[
            [
                "country",
                "psr_type",
                "psr_name",
                "timestamp_utc_plus_2",
                "resolution",
                "generation_mw",
                "source_samples",
            ]
        ]

        output_path = options["output"].expanduser()
        if output_path.suffix.lower() not in {".csv", ".xlsx"}:
            raise CommandError("Output must use the .xlsx or .csv extension.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.suffix.lower() == ".xlsx":
            hourly.to_excel(output_path, index=False, sheet_name="BG solar generation")
        else:
            hourly.to_csv(output_path, index=False)
        self.stdout.write(
            self.style.SUCCESS(
                f"Exported {len(hourly)} hourly BG solar rows to {output_path}"
            )
        )
