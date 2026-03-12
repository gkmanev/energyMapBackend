import json
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from django.core.management.base import BaseCommand, CommandError
from dotenv import load_dotenv

from entsoe_api.entsoe_data import PSRTYPE_MAPPINGS

load_dotenv()

DEFAULT_BG_GENERATION_URL = "http://85.14.6.37:17999/api/generation/"
DEFAULT_TIMEOUT_SECONDS = 30


def _normalize_psr_name(psr_type: str, psr_name: Optional[str]) -> str:
    psr_type = (psr_type or "").strip().upper()
    raw_name = (psr_name or "").strip()
    mapped_name = PSRTYPE_MAPPINGS.get(psr_type, "")

    if raw_name and raw_name.upper() != psr_type:
        return raw_name
    if mapped_name:
        return mapped_name
    return raw_name or psr_type


def _normalize_generation_record(record: Dict[str, Any]) -> Dict[str, Any]:
    country_block = record.get("country") or {}
    country_iso = (
        record.get("country_iso_code")
        or country_block.get("iso_code")
        or "BG"
    )
    country_iso = str(country_iso).strip().upper()
    if not country_iso:
        raise ValueError("Missing country ISO code")

    psr_type = str(record.get("psr_type") or "").strip().upper()
    if not psr_type:
        raise ValueError("Missing psr_type")

    datetime_raw = record.get("datetime_utc")
    if not datetime_raw:
        raise ValueError("Missing datetime_utc")

    generation_raw = record.get("generation_mw")
    if generation_raw in (None, ""):
        generation_raw = record.get("generation_MW")
    if generation_raw in (None, ""):
        raise ValueError("Missing generation_mw")

    generation_value = pd.to_numeric(generation_raw, errors="raise")
    datetime_utc = pd.to_datetime(datetime_raw, utc=True, errors="raise").to_pydatetime()

    return {
        "country": country_iso,
        "datetime_utc": datetime_utc,
        "psr_type": psr_type,
        "psr_name": _normalize_psr_name(psr_type, record.get("psr_name")),
        "generation_MW": generation_value,
        "resolution": str(record.get("resolution") or "snapshot").strip() or "snapshot",
    }


def _extract_results(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            return results
    raise ValueError("Unexpected response payload: expected a list or a paginated object with 'results'")


class Command(BaseCommand):
    help = (
        "Fetch BG generation snapshots from the ESO BG endpoint and upsert them "
        "into CountryGenerationByType."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--url",
            type=str,
            default=os.getenv("ESO_BG_GENERATION_URL", DEFAULT_BG_GENERATION_URL),
            help="Generation endpoint URL.",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=DEFAULT_TIMEOUT_SECONDS,
            help="HTTP timeout in seconds (default: 30).",
        )
        parser.add_argument(
            "--output",
            type=str,
            help="Optional path to dump the normalized JSON payload.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and normalize records, but do not write to the database.",
        )

    def _fetch_pages(self, url: str, timeout: int) -> List[Dict[str, Any]]:
        session = requests.Session()
        next_url = url
        collected: List[Dict[str, Any]] = []
        page = 0

        while next_url:
            page += 1
            self.stdout.write(f"Fetching ESO BG generation page {page}: {next_url}")
            try:
                response = session.get(next_url, timeout=timeout)
                response.raise_for_status()
            except requests.RequestException as exc:
                raise CommandError(f"Failed to fetch ESO BG generation data: {exc}") from exc

            try:
                payload = response.json()
            except ValueError as exc:
                raise CommandError("ESO BG generation endpoint did not return valid JSON.") from exc

            page_results = _extract_results(payload)
            collected.extend(page_results)

            if isinstance(payload, dict) and payload.get("next"):
                next_url = urljoin(response.url, str(payload["next"]))
            else:
                next_url = None

        return collected

    def handle(self, *args, **options):
        from entsoe_api.helper import save_generation_df

        url = str(options["url"]).strip()
        timeout = int(options["timeout"])

        if not url:
            raise CommandError("A non-empty --url is required.")
        if timeout <= 0:
            raise CommandError("--timeout must be > 0.")

        raw_records = self._fetch_pages(url=url, timeout=timeout)
        if not raw_records:
            self.stdout.write(self.style.WARNING("No generation records returned."))
            return

        normalized_records: List[Dict[str, Any]] = []
        skipped_records = 0

        for record in raw_records:
            try:
                normalized_records.append(_normalize_generation_record(record))
            except Exception as exc:
                skipped_records += 1
                self.stdout.write(self.style.WARNING(f"Skipping invalid record: {exc}"))

        if not normalized_records:
            raise CommandError("The endpoint returned records, but none could be normalized.")

        df = pd.DataFrame.from_records(normalized_records)

        if options.get("dry_run"):
            written = 0
            self.stdout.write(self.style.WARNING("Dry run enabled; database write skipped."))
        else:
            written = save_generation_df(df)

        self.stdout.write(
            self.style.SUCCESS(
                f"Normalized {len(normalized_records)} records, skipped {skipped_records}, saved {written} rows."
            )
        )

        out_path = options.get("output")
        if out_path:
            payload = json.dumps(normalized_records, ensure_ascii=False, indent=2, default=str)
            with open(out_path, "w", encoding="utf-8") as handle:
                handle.write(payload)
            self.stdout.write(self.style.SUCCESS(f"Wrote normalized payload to {out_path}"))
