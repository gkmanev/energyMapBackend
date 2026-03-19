from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from entsoe_api.helper import save_country_tilted_irradiance_df

try:
    import openmeteo_requests
    import requests_cache
    from retry_requests import retry
except ImportError:  # pragma: no cover - exercised through command runtime
    openmeteo_requests = None
    requests_cache = None
    retry = None


OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLE = "global_tilted_irradiance_instant"


def _parse_iso_date(value: str) -> dt.date:
    if not value:
        raise ValueError("empty date value")
    return dt.date.fromisoformat(value)


def _compute_date_window(
    start_date_value: str | None,
    end_date_value: str | None,
    past_days: int,
    forecast_days: int,
) -> tuple[dt.date, dt.date]:
    if start_date_value or end_date_value:
        if not (start_date_value and end_date_value):
            raise ValueError("Provide both --start-date and --end-date, or neither.")
        start_date = _parse_iso_date(start_date_value)
        end_date = _parse_iso_date(end_date_value)
    else:
        if past_days < 0 or forecast_days < 0:
            raise ValueError("--past-days and --forecast-days must be >= 0.")
        today = dt.datetime.utcnow().date()
        start_date = today - dt.timedelta(days=past_days)
        end_date = today + dt.timedelta(days=max(forecast_days - 1, 0))

    if start_date > end_date:
        raise ValueError("--start-date must be earlier than or equal to --end-date.")

    return start_date, end_date


def _iter_date_chunks(
    start_date: dt.date,
    end_date: dt.date,
    chunk_days: int,
) -> Iterable[tuple[dt.date, dt.date]]:
    if chunk_days <= 0:
        raise ValueError("chunk_days must be > 0")

    current = start_date
    while current <= end_date:
        chunk_end = min(current + dt.timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + dt.timedelta(days=1)


def _chunked(items: Sequence[dict], size: int) -> Iterable[list[dict]]:
    if size <= 0:
        raise ValueError("size must be > 0")
    for idx in range(0, len(items), size):
        yield list(items[idx:idx + size])


def _interval_seconds_to_resolution(interval_seconds: int) -> str:
    if interval_seconds <= 0:
        return ""
    if interval_seconds % 3600 == 0:
        return f"PT{interval_seconds // 3600}H"
    if interval_seconds % 60 == 0:
        return f"PT{interval_seconds // 60}M"
    return f"PT{interval_seconds}S"


def _load_country_coords(country_code: str | None) -> list[dict]:
    raw_coords = getattr(settings, "COUNTRY_COORDS", None)
    if not isinstance(raw_coords, list) or not raw_coords:
        raise CommandError("settings.COUNTRY_COORDS is missing or empty.")

    normalized: list[dict] = []
    for entry in raw_coords:
        if not isinstance(entry, dict):
            continue
        code = str(entry.get("code") or "").upper().strip()
        lat = entry.get("lat")
        lng = entry.get("lng")
        if not code or lat is None or lng is None:
            continue
        normalized.append(
            {
                "code": code,
                "lat": float(lat),
                "lng": float(lng),
            }
        )

    if not normalized:
        raise CommandError("settings.COUNTRY_COORDS does not contain valid country coordinate entries.")

    selected_country = (country_code or "ALL").upper().strip()
    if selected_country == "ALL":
        return sorted(normalized, key=lambda item: item["code"])

    for item in normalized:
        if item["code"] == selected_country:
            return [item]

    known = ", ".join(sorted(item["code"] for item in normalized))
    raise CommandError(f"Unknown country '{selected_country}'. Known: {known}")


def _build_openmeteo_client(cache_seconds: int, request_retries: int):
    if openmeteo_requests is None or requests_cache is None or retry is None:
        raise CommandError(
            "Missing Open-Meteo dependencies. Install openmeteo-requests, requests-cache, and retry-requests."
        )

    cache_dir = Path(settings.BASE_DIR) / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_session = requests_cache.CachedSession(
        str(cache_dir / "openmeteo_irradiance"),
        expire_after=cache_seconds,
    )
    retry_session = retry(cache_session, retries=request_retries, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def _responses_to_dataframe(
    countries: Sequence[dict],
    responses,
    tilt: float,
    azimuth: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    if len(responses) != len(countries):
        raise ValueError(f"Expected {len(countries)} responses, got {len(responses)}")

    for country, response in zip(countries, responses):
        hourly = response.Hourly()
        timestamps = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
        values = hourly.Variables(0).ValuesAsNumpy()

        if len(timestamps) != len(values):
            raise ValueError(
                f"{country['code']} returned {len(values)} values for {len(timestamps)} timestamps."
            )

        frames.append(
            pd.DataFrame(
                {
                    "country": country["code"],
                    "datetime_utc": timestamps,
                    "tilt_degrees": float(tilt),
                    "azimuth_degrees": float(azimuth),
                    "irradiance_wm2": values,
                    "resolution": _interval_seconds_to_resolution(hourly.Interval()),
                    "requested_latitude": country["lat"],
                    "requested_longitude": country["lng"],
                    "response_latitude": response.Latitude(),
                    "response_longitude": response.Longitude(),
                    "elevation_m_asl": response.Elevation(),
                    "utc_offset_seconds": response.UtcOffsetSeconds(),
                }
            )
        )

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


class Command(BaseCommand):
    help = (
        "Fetch Open-Meteo hourly global tilted irradiance for one country or all "
        "countries in settings.COUNTRY_COORDS, saving country-level points to the database."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--country",
            type=str,
            help="ISO code (for example BG). Omit or use ALL to fetch every country in settings.COUNTRY_COORDS.",
        )
        parser.add_argument(
            "--tilt",
            type=float,
            default=30.0,
            help="Panel tilt in degrees (default: 30).",
        )
        parser.add_argument(
            "--azimuth",
            type=float,
            help="Panel azimuth in degrees. Defaults to 0 (south-facing) when omitted.",
        )
        parser.add_argument(
            "--past-days",
            type=int,
            default=0,
            help="Days before today to include when --start-date/--end-date are not provided (default: 0).",
        )
        parser.add_argument(
            "--forecast-days",
            type=int,
            default=7,
            help="Days from today to include when --start-date/--end-date are not provided (default: 7).",
        )
        parser.add_argument(
            "--start-date",
            type=str,
            help="Inclusive UTC date in YYYY-MM-DD format.",
        )
        parser.add_argument(
            "--end-date",
            type=str,
            help="Inclusive UTC date in YYYY-MM-DD format.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=8,
            help="How many countries to request in a single Open-Meteo call (default: 8).",
        )
        parser.add_argument(
            "--chunk-days",
            type=int,
            default=3,
            help="How many days to request per API call (default: 3).",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=0.3,
            help="Delay in seconds between API calls (default: 0.3).",
        )
        parser.add_argument(
            "--max-retries",
            type=int,
            default=3,
            help="Maximum command-level retries per API call (default: 3).",
        )
        parser.add_argument(
            "--cache-seconds",
            type=int,
            default=3600,
            help="Open-Meteo response cache TTL in seconds (default: 3600).",
        )
        parser.add_argument(
            "--request-retries",
            type=int,
            default=5,
            help="HTTP retries configured on the Open-Meteo client session (default: 5).",
        )
        parser.add_argument(
            "--output",
            type=str,
            help="Optional path to export the combined result set.",
        )
        parser.add_argument(
            "--format",
            choices=["json", "csv"],
            default="json",
            help="Export format when --output is used (default: json).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and report rows without writing to the database.",
        )
        parser.add_argument(
            "--continue-on-error",
            action="store_true",
            help="Continue processing remaining batches if a batch fails.",
        )

    def fetch_batch_with_retry(
        self,
        client,
        countries: Sequence[dict],
        start_date: dt.date,
        end_date: dt.date,
        tilt: float,
        azimuth: float,
        max_retries: int,
    ) -> pd.DataFrame | None:
        codes = ",".join(country["code"] for country in countries)
        params = {
            "latitude": ",".join(f"{country['lat']:.6f}" for country in countries),
            "longitude": ",".join(f"{country['lng']:.6f}" for country in countries),
            "hourly": HOURLY_VARIABLE,
            "tilt": tilt,
            "timezone": "GMT",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        params["azimuth"] = azimuth

        for attempt in range(max_retries):
            try:
                responses = client.weather_api(OPENMETEO_URL, params=params)
                return _responses_to_dataframe(countries, responses, tilt=tilt, azimuth=azimuth)
            except Exception as exc:
                if attempt >= max_retries - 1:
                    self.stdout.write(
                        self.style.ERROR(
                            f"  Failed batch {codes} for {start_date}..{end_date}: {str(exc)[:160]}"
                        )
                    )
                    return None

                wait_seconds = 2 ** attempt
                self.stdout.write(
                    self.style.WARNING(
                        f"  Retry {attempt + 1}/{max_retries - 1} for batch {codes} after error: {str(exc)[:160]}"
                    )
                )
                time.sleep(wait_seconds)

        return None

    def handle(self, *args, **options):
        countries = _load_country_coords(options.get("country"))
        batch_size = options["batch_size"]
        chunk_days = options["chunk_days"]
        delay = options["delay"]
        tilt = options["tilt"]
        azimuth = 0.0 if options.get("azimuth") is None else options["azimuth"]
        dry_run = options["dry_run"]
        continue_on_error = options["continue_on_error"]

        if batch_size <= 0:
            raise CommandError("--batch-size must be > 0.")
        if chunk_days <= 0:
            raise CommandError("--chunk-days must be > 0.")
        if options["max_retries"] <= 0:
            raise CommandError("--max-retries must be > 0.")
        if options["request_retries"] < 0:
            raise CommandError("--request-retries must be >= 0.")
        if options["cache_seconds"] < 0:
            raise CommandError("--cache-seconds must be >= 0.")

        try:
            start_date, end_date = _compute_date_window(
                options.get("start_date"),
                options.get("end_date"),
                past_days=options["past_days"],
                forecast_days=options["forecast_days"],
            )
        except ValueError as exc:
            raise CommandError(str(exc))

        date_chunks = list(_iter_date_chunks(start_date, end_date, chunk_days))
        country_batches = list(_chunked(countries, batch_size))
        total_calls = len(date_chunks) * len(country_batches)

        client = _build_openmeteo_client(
            cache_seconds=options["cache_seconds"],
            request_retries=options["request_retries"],
        )

        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("Open-Meteo Global Tilted Irradiance Fetch"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"Countries: {len(countries)} ({', '.join(country['code'] for country in countries)})")
        self.stdout.write(f"Date window: {start_date.isoformat()} to {end_date.isoformat()} (inclusive)")
        self.stdout.write(f"Tilt: {tilt}")
        self.stdout.write(f"Azimuth: {azimuth}")
        self.stdout.write(f"Country batch size: {batch_size}")
        self.stdout.write(f"Date chunk size: {chunk_days} day(s)")
        self.stdout.write(f"Planned API calls: {total_calls}")
        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run mode: fetched rows will not be saved."))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write("")

        total_rows_retrieved = 0
        total_rows_saved = 0
        successful_calls = 0
        failed_calls = 0
        failed_labels: list[str] = []
        collected_frames: list[pd.DataFrame] = []

        call_number = 0
        started_at = time.time()

        for chunk_start, chunk_end in date_chunks:
            for batch in country_batches:
                call_number += 1
                codes = ",".join(country["code"] for country in batch)
                self.stdout.write(
                    f"[{call_number}/{total_calls}] Fetching {codes} for {chunk_start.isoformat()}..{chunk_end.isoformat()}"
                )

                df = self.fetch_batch_with_retry(
                    client=client,
                    countries=batch,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    tilt=tilt,
                    azimuth=azimuth,
                    max_retries=options["max_retries"],
                )

                if df is None:
                    failed_calls += 1
                    failed_labels.append(f"{codes} [{chunk_start.isoformat()}..{chunk_end.isoformat()}]")
                    if not continue_on_error:
                        raise CommandError(
                            f"Failed batch {codes} for {chunk_start.isoformat()}..{chunk_end.isoformat()}."
                        )
                    continue

                rows_retrieved = len(df)
                total_rows_retrieved += rows_retrieved
                successful_calls += 1

                if dry_run:
                    self.stdout.write(self.style.SUCCESS(f"  Retrieved {rows_retrieved} rows (dry-run)."))
                else:
                    written = save_country_tilted_irradiance_df(df)
                    total_rows_saved += written
                    self.stdout.write(
                        self.style.SUCCESS(f"  Retrieved {rows_retrieved} rows, saved {written} rows.")
                    )

                if options.get("output"):
                    collected_frames.append(df)

                if delay > 0 and call_number < total_calls:
                    time.sleep(delay)

        elapsed_seconds = time.time() - started_at

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("FETCH SUMMARY"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"Successful API calls: {successful_calls}/{total_calls}")
        self.stdout.write(f"Failed API calls: {failed_calls}/{total_calls}")
        self.stdout.write(f"Rows retrieved: {total_rows_retrieved}")
        if dry_run:
            self.stdout.write("Rows saved: dry-run")
        else:
            self.stdout.write(f"Rows saved: {total_rows_saved}")
        self.stdout.write(f"Elapsed time: {elapsed_seconds:.2f}s")
        if failed_labels:
            self.stdout.write("Failed batches: " + "; ".join(failed_labels))
        self.stdout.write(self.style.SUCCESS("=" * 70))

        output_path = options.get("output")
        if output_path and collected_frames:
            export_df = pd.concat(collected_frames, ignore_index=True)
            if options["format"] == "csv":
                export_df.to_csv(output_path, index=False)
            else:
                payload = json.dumps(
                    export_df.to_dict(orient="records"),
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
                with open(output_path, "w", encoding="utf-8") as handle:
                    handle.write(payload)

            self.stdout.write(self.style.SUCCESS(f"Wrote {len(export_df)} rows to {output_path}"))
