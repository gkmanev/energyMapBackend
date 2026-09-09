"""Export Bulgarian day-ahead prices as an hourly Excel time series."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from openpyxl import Workbook
from openpyxl.styles import Font

from entsoe_api.models import CountryPricePoint


UTC = timezone.utc
UTC_PLUS_2 = timezone(timedelta(hours=2))


def _hourly_averages(points):
    """Group price points by their UTC hour and return their Decimal averages."""
    values_by_hour = defaultdict(list)
    for timestamp, price in points:
        hour = timestamp.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        values_by_hour[hour].append(price)

    return [
        (hour, sum(values) / len(values))
        for hour, values in sorted(values_by_hour.items())
    ]


class Command(BaseCommand):
    help = (
        "Export BG day-ahead prices for calendar years 2024 and 2025 as hourly "
        "averages in an .xlsx file. Timestamps are rendered in fixed UTC+2."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            type=Path,
            default=Path(settings.BASE_DIR) / "exports" / "bg_prices_2024_2025_utc_plus_2.xlsx",
            help="Output .xlsx path (default: exports/bg_prices_2024_2025_utc_plus_2.xlsx).",
        )
        parser.add_argument(
            "--contract",
            choices=["A01", "A07"],
            default="A01",
            help="Price contract to export: A01=day-ahead (default), A07=intraday.",
        )

    def handle(self, *args, **options):
        # Filter by the requested years in the output timezone. This produces
        # exactly 2024-01-01 00:00 through 2025-12-31 23:00 in UTC+2.
        start_utc = datetime(2024, 1, 1, tzinfo=UTC_PLUS_2).astimezone(UTC)
        end_utc = datetime(2026, 1, 1, tzinfo=UTC_PLUS_2).astimezone(UTC)
        contract = options["contract"]

        points = (
            CountryPricePoint.objects.filter(
                country_id="BG",
                contract_type=contract,
                datetime_utc__gte=start_utc,
                datetime_utc__lt=end_utc,
                price__isnull=False,
            )
            .order_by("datetime_utc")
            .values_list("datetime_utc", "price")
            .iterator(chunk_size=2_000)
        )
        rows = _hourly_averages(points)
        if not rows:
            raise CommandError(
                f"No BG {contract} price points found from {start_utc:%Y-%m-%d %H:%MZ} "
                f"through {end_utc:%Y-%m-%d %H:%MZ}."
            )

        output_path = options["output"].expanduser().resolve()
        if output_path.suffix.lower() != ".xlsx":
            raise CommandError("Output must use the .xlsx extension.")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = "BG prices"
        worksheet.append(["timestamp_utc_plus_2", "price_eur_mwh"])
        for cell in worksheet[1]:
            cell.font = Font(bold=True)
        worksheet.freeze_panes = "A2"
        worksheet.column_dimensions["A"].width = 28
        worksheet.column_dimensions["B"].width = 18

        for hour_utc, average_price in rows:
            timestamp_utc_plus_2 = hour_utc.astimezone(UTC_PLUS_2)
            worksheet.append(
                [
                    timestamp_utc_plus_2.strftime("%Y-%m-%d %H:%M:%S+02:00"),
                    float(average_price),
                ]
            )

        workbook.save(output_path)
        self.stdout.write(
            self.style.SUCCESS(
                f"Exported {len(rows)} hourly BG {contract} price rows to {output_path}"
            )
        )
