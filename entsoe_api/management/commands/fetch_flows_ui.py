"""Import published web UI physical flows into the existing flow table."""
import csv
import datetime as dt
import json
from pathlib import Path

import requests
import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError

from entsoe_api.flows_ui import UI_TIMEZONE, fetch_day, to_flow_records, to_rows
from entsoe_api.helper import save_flows_df
from entsoe_api.management.commands.fetch_flows import (
    _iter_eics, _load_country_to_eics_from_settings,
)


class Command(BaseCommand):
    help = "Fetch ENTSO-E web UI physical flows and upsert published exports into PhysicalFlow."

    def add_arguments(self, parser):
        parser.add_argument("--date", help="UI day YYYY-MM-DD, in CET/CEST. Defaults to today.")
        areas = parser.add_mutually_exclusive_group()
        areas.add_argument("--countries", help="Comma-separated country codes, e.g. BG or BG,RO,GR. Defaults to BG.")
        areas.add_argument("--all-eu", action="store_true", help="Request all areas configured in ENTSOE_COUNTRY_TO_EICS.")
        areas.add_argument("--area", action="append", help="Exact UI area; repeat for several, e.g. BZN|10YCA-BULGARIA-R.")
        parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds (default: 30).")
        parser.add_argument("--output", help="Export parsed rows to a .csv or .json file.")
        parser.add_argument("--raw-output", help="Save the full source response as JSON.")
        parser.add_argument("--limit", type=int, default=10, help="Number of published rows to preview (default: 10).")
        parser.add_argument("--dry-run", action="store_true", help="Fetch, validate and preview/export without updating the database.")

    def handle(self, *args, **options):
        try:
            day = dt.date.fromisoformat(options["date"]) if options["date"] else dt.datetime.now(UI_TIMEZONE).date()
        except ValueError as exc:
            raise CommandError("--date must be YYYY-MM-DD.") from exc
        if options["timeout"] <= 0 or options["limit"] < 0:
            raise CommandError("--timeout must be positive and --limit must be non-negative.")
        output = Path(options["output"]) if options["output"] else None
        if output and output.suffix.lower() not in (".csv", ".json"):
            raise CommandError("--output must end in .csv or .json.")

        if options["area"]:
            areas = options["area"]
            if any(not area.startswith(("BZN|", "CTA|", "CTY|")) or not area.split("|", 1)[1] for area in areas):
                raise CommandError("--area must have a UI area type and EIC, e.g. BZN|10YCA-BULGARIA-R.")
        else:
            mapping = _load_country_to_eics_from_settings()
            countries = list(mapping) if options["all_eu"] else [country.strip().upper() for country in (options["countries"] or "BG").split(",")]
            unknown = set(countries) - mapping.keys()
            if unknown:
                raise CommandError("Unknown country code(s): " + ", ".join(sorted(unknown)))
            areas = [f"BZN|{eic}" for country in countries for eic in _iter_eics(mapping[country])]
        areas = list(dict.fromkeys(areas))
        self.stdout.write(f"Fetching UI physical flows for {day} (CET/CEST), {len(areas)} area(s)...")
        try:
            data = fetch_day(day, areas, timeout=options["timeout"])
            if options["raw_output"]:
                Path(options["raw_output"]).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            rows = list(to_rows(data))
            records = to_flow_records(rows)
        except (requests.RequestException, ValueError, KeyError, TypeError, OSError) as exc:
            raise CommandError(f"Could not fetch/parse UI flows: {exc}") from exc

        if output:
            try:
                if output.suffix.lower() == ".json":
                    output.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
                else:
                    fields = ["from_area", "to_area", "utc_start", "resolution"] + [item["code"] for item in data["metaData"]]
                    with output.open("w", encoding="utf-8", newline="") as stream:
                        writer = csv.DictWriter(stream, fieldnames=fields)
                        writer.writeheader()
                        writer.writerows(rows)
                self.stdout.write(f"Exported {len(rows)} rows to {output}")
            except OSError as exc:
                raise CommandError(f"Could not write output: {exc}") from exc
        published = [row for row in rows if row["EXPORT_FLOW"] is not None]
        if options["dry_run"]:
            self.stdout.write(f"Dry run: {len(records)} published records would be saved; database unchanged.")
        else:
            try:
                written = save_flows_df(pd.DataFrame.from_records(records))
            except DatabaseError as exc:
                raise CommandError(f"Could not save UI flows; database transaction rolled back: {exc}") from exc
            self.stdout.write(self.style.SUCCESS(f"Saved {written} published flow records (created or updated)."))
        for row in published[:options["limit"]]:
            self.stdout.write(json.dumps(row, ensure_ascii=False))
        self.stdout.write(f"{len(data['instanceList'])} directional series; {len(rows)} points; {len(published)} published export values.")
        if published:
            self.stdout.write("Latest published interval start (UTC): " + max(row["utc_start"] for row in published))
