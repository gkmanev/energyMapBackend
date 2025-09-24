# entsoe_api/management/commands/fetch_prices.py

from __future__ import annotations

import os
import datetime as dt
from typing import Dict, List, Union

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from entsoe_api.entsoe_data import EntsoePrices
from entsoe_api.helper import save_country_prices_df


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


class Command(BaseCommand):
    help = (
        "Fetch ENTSO-E A44 prices (Day-ahead/Intraday) for one country (--country BG) "
        "or ALL (default), aggregate by country, and store into CountryPricePoint."
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

    def handle(self, *args, **opts):
        # --- API key ---
        api_key = getattr(settings, "ENTSOE_API_KEY", "") or os.getenv("ENTSOE_API_KEY") or os.getenv("ENTSOE_TOKEN")
        if not api_key:
            raise CommandError("Missing ENTSOE_API_KEY (settings or env).")

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

        self.stdout.write(
            f"Fetching prices {contract} for {country_arg} in [{start_utc:%Y-%m-%d %H:%MZ}, {end_utc:%Y-%m-%d %H:%MZ}) UTC..."
        )

        # --- fetch (country-aggregated, because your frontend is country-based) ---
        df = EntsoePrices.query_all_countries(
            api_key=api_key,
            country_to_eics=country_to_eics,
            start=start_utc,
            end=end_utc,
            contract_type=contract,
            aggregate_by_country=True,
            skip_errors=True,
        )

        if df.empty:
            self.stdout.write(self.style.WARNING("No data returned."))
            return

        self.stdout.write(f"Fetched {len(df)} rows.")

        # --- store ---
        if opts.get("dry_run"):
            self.stdout.write(self.style.WARNING("Dry-run: not writing to DB."))
            return

        written = save_country_prices_df(df)
        self.stdout.write(self.style.SUCCESS(f"Saved {written} price rows to CountryPricePoint."))
