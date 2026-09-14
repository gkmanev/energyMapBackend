"""Repair missing hourly BG solar values in an exported workbook."""

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


def _parse_utc_plus_2(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC_PLUS_2)
    return parsed.astimezone(UTC)


def _request_ranges(start: dt.datetime, end: dt.datetime, days: int = 7):
    current = start
    while current < end:
        next_boundary = min(current + dt.timedelta(days=days), end)
        yield current, next_boundary
        current = next_boundary


def _contiguous_ranges(hours: list[pd.Timestamp]):
    """Turn sorted, naive UTC+2 hourly timestamps into [start, end) ranges."""
    if not hours:
        return []
    ranges = []
    start = previous = hours[0]
    for timestamp in hours[1:]:
        if timestamp - previous != pd.Timedelta(hours=1):
            ranges.append((start, previous + pd.Timedelta(hours=1)))
            start = timestamp
        previous = timestamp
    ranges.append((start, previous + pd.Timedelta(hours=1)))
    return ranges


class Command(BaseCommand):
    help = (
        "Check an hourly BG solar XLSX for missing or blank UTC+2 hours, fetch "
        "only those ranges from ENTSO-E, and write a repaired workbook plus audit."
    )

    def add_arguments(self, parser):
        parser.add_argument("--input", type=Path, required=True, help="Existing BG solar .xlsx file.")
        parser.add_argument("--output", type=Path, required=True, help="Repaired .xlsx output path.")
        parser.add_argument(
            "--start",
            default="2024-01-01T00:00:00+02:00",
            help="First expected timestamp in UTC+2 (inclusive).",
        )
        parser.add_argument(
            "--end",
            default="2026-01-01T00:00:00+02:00",
            help="End of expected timestamps in UTC+2 (exclusive).",
        )
        parser.add_argument("--delay", type=float, default=1.0, help="Seconds between API calls.")

    def handle(self, *args, **options):
        input_path = options["input"].expanduser()
        output_path = options["output"].expanduser()
        if not input_path.is_file() or input_path.suffix.lower() != ".xlsx":
            raise CommandError(f"Input workbook not found or not .xlsx: {input_path}")
        if output_path.suffix.lower() != ".xlsx":
            raise CommandError("--output must use the .xlsx extension.")
        if options["delay"] < 0:
            raise CommandError("--delay must be zero or greater.")
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise CommandError(f"Cannot create {output_path.parent}: {exc}") from exc

        try:
            start_utc = _parse_utc_plus_2(options["start"])
            end_utc = _parse_utc_plus_2(options["end"])
        except ValueError as exc:
            raise CommandError(f"Invalid --start/--end: {exc}") from exc
        if start_utc >= end_utc:
            raise CommandError("--start must be earlier than --end.")

        existing = pd.read_excel(input_path)
        required = {"timestamp_utc_plus_2", "generation_mw"}
        missing_columns = required - set(existing.columns)
        if missing_columns:
            raise CommandError(f"Input workbook is missing columns: {', '.join(sorted(missing_columns))}")

        existing["timestamp_utc_plus_2"] = (
            pd.to_datetime(existing["timestamp_utc_plus_2"], utc=True) + pd.Timedelta(hours=2)
        ).dt.tz_localize(None).dt.floor("h")
        existing["generation_mw"] = pd.to_numeric(existing["generation_mw"], errors="coerce")
        existing = existing.sort_values("timestamp_utc_plus_2")
        existing = existing.drop_duplicates("timestamp_utc_plus_2", keep="last")
        existing_by_hour = existing.set_index("timestamp_utc_plus_2")

        expected_utc = pd.date_range(start_utc, end_utc, freq="h", inclusive="left")
        expected_local = (expected_utc + pd.Timedelta(hours=2)).tz_localize(None)
        result = pd.DataFrame(index=expected_local)
        result.index.name = "timestamp_utc_plus_2"
        result["generation_mw"] = existing_by_hour.reindex(expected_local)["generation_mw"].to_numpy()
        if "source_samples" in existing_by_hour:
            result["source_samples"] = existing_by_hour.reindex(expected_local)["source_samples"].to_numpy()
        else:
            result["source_samples"] = pd.NA

        missing_hours = list(result.index[result["generation_mw"].isna()])
        audit_rows = []
        for hour in missing_hours:
            reason = "blank_generation_mw" if hour in existing_by_hour.index else "missing_timestamp"
            audit_rows.append({
                "timestamp_utc_plus_2": hour.strftime("%Y-%m-%dT%H:%M:%S+02:00"),
                "initial_issue": reason,
                "fetch_status": "not_attempted",
                "detail": "",
            })

        if missing_hours:
            api_key = getattr(settings, "ENTSOE_API_KEY", "") or os.getenv("ENTSOE_TOKEN")
            bg_eics = getattr(settings, "ENTSOE_COUNTRY_TO_EICS", {}).get("BG")
            if not api_key or not bg_eics:
                raise CommandError("Missing ENTSO-E credentials or BG EIC mapping needed to repair gaps.")

            recovered = {}
            request_errors = []
            local_ranges = _contiguous_ranges(missing_hours)
            requests = [
                (chunk_start, chunk_end)
                for local_start, local_end in local_ranges
                for chunk_start, chunk_end in _request_ranges(
                    local_start.replace(tzinfo=UTC_PLUS_2).astimezone(UTC),
                    local_end.replace(tzinfo=UTC_PLUS_2).astimezone(UTC),
                )
            ]
            for index, (range_start, range_end) in enumerate(requests, start=1):
                self.stdout.write(
                    f"[{index}/{len(requests)}] Fetching BG solar "
                    f"{range_start:%Y-%m-%d} to {range_end:%Y-%m-%d} UTC"
                )
                try:
                    fetched = EntsoeGenerationByType.query_all_countries(
                        api_key=api_key,
                        country_to_eics={"BG": bg_eics},
                        start=range_start,
                        end=range_end,
                        psr_type="B16",
                        aggregate_by_country=True,
                        skip_errors=False,
                    )
                    if not fetched.empty:
                        fetched["datetime_utc"] = pd.to_datetime(fetched["datetime_utc"], utc=True)
                        fetched["hour"] = fetched["datetime_utc"].dt.floor("h")
                        hourly = fetched.groupby("hour")["generation_MW"].agg(["mean", "count"])
                        for hour, values in hourly.iterrows():
                            local_hour = (hour + pd.Timedelta(hours=2)).tz_localize(None)
                            recovered[local_hour] = (float(values["mean"]), int(values["count"]))
                except Exception as exc:
                    request_errors.append((range_start, range_end, str(exc)))
                if index < len(requests) and options["delay"]:
                    time.sleep(options["delay"])

            for hour, (value, samples) in recovered.items():
                if hour in result.index:
                    result.loc[hour, "generation_mw"] = value
                    result.loc[hour, "source_samples"] = samples
            for audit in audit_rows:
                hour = pd.Timestamp(audit["timestamp_utc_plus_2"].replace("+02:00", ""))
                if pd.notna(result.loc[hour, "generation_mw"]):
                    audit["fetch_status"] = "recovered"
                else:
                    audit["fetch_status"] = "unresolved"
                    matching_errors = [
                        message for begin, finish, message in request_errors
                        if begin <= hour.replace(tzinfo=UTC_PLUS_2).astimezone(UTC) < finish
                    ]
                    audit["detail"] = matching_errors[0] if matching_errors else "No solar value returned by ENTSO-E"

        repaired = result.reset_index()
        repaired["timestamp_utc_plus_2"] = repaired["timestamp_utc_plus_2"].dt.strftime("%Y-%m-%dT%H:%M:%S+02:00")
        repaired.insert(0, "country", "BG")
        repaired.insert(1, "psr_type", "B16")
        repaired.insert(2, "psr_name", "Solar")
        repaired.insert(4, "resolution", "PT60M")
        repaired = repaired[["country", "psr_type", "psr_name", "timestamp_utc_plus_2", "resolution", "generation_mw", "source_samples"]]
        audit = pd.DataFrame(audit_rows, columns=["timestamp_utc_plus_2", "initial_issue", "fetch_status", "detail"])

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            repaired.to_excel(writer, index=False, sheet_name="BG solar generation")
            audit.to_excel(writer, index=False, sheet_name="Missing-data audit")
        unresolved = int(repaired["generation_mw"].isna().sum())
        self.stdout.write(self.style.SUCCESS(
            f"Wrote {output_path}; repaired {len(audit_rows) - unresolved} gaps, "
            f"with {unresolved} unresolved hours."
        ))
