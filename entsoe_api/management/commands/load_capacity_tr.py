# entsoe_api/management/commands/load_capacity_tr.py
"""
Load Turkey's installed capacity by technology into CountryCapacitySnapshot.

Neither EPİAŞ nor ENTSO-E publish total installed capacity by source for Turkey,
so the figures are maintained here by hand. Update CAPACITY_MW (and --year)
when new numbers come out, then re-run; rows are upserted per (country, psr_type, year).

    python manage.py load_capacity_tr --dry-run
    python manage.py load_capacity_tr --year 2026
"""
import pandas as pd
from django.core.management.base import BaseCommand

from entsoe_api.entsoe_data import PSRTYPE_MAPPINGS
from entsoe_api.helper import save_capacity_df

# Installed capacity (MW), total 127,355 MW. PSR codes match fetch_generation_tr.
CAPACITY_MW = {
    "B12": 32_332,  # Hydro — total of reservoir + run-of-river, stored under reservoir
    "B16": 28_222,  # Solar
    "B04": 25_009,  # Natural gas
    "B19": 15_540,  # Wind
    "B02": 11_565,  # Domestic coal (mostly lignite)
    "B05": 10_466,  # Imported coal
    "B01": 2_423,   # Biomass
    "B09": 1_798,   # Geothermal
}


class Command(BaseCommand):
    help = "Load hand-maintained Turkish installed capacity by technology into CountryCapacitySnapshot."

    def add_arguments(self, parser):
        parser.add_argument("--year", type=int, default=2026, help="Snapshot year (default: 2026).")
        parser.add_argument("--dry-run", action="store_true", help="Show rows but do NOT write to DB.")

    def handle(self, *args, **opts):
        df = pd.DataFrame([
            {
                "country": "TR",
                "psr_type": psr,
                "psr_name": PSRTYPE_MAPPINGS.get(psr, psr),
                "installed_capacity_MW": float(mw),
                "year": opts["year"],
            }
            for psr, mw in CAPACITY_MW.items()
        ])
        self.stdout.write(df.to_string(index=False))
        self.stdout.write(f"Total: {df['installed_capacity_MW'].sum():,.0f} MW")

        if opts["dry_run"]:
            self.stdout.write(self.style.WARNING(f"🔍 Dry-run: {len(df)} rows would have been saved"))
            return

        written = save_capacity_df(df)
        self.stdout.write(self.style.SUCCESS(
            f"💾 Saved {written} rows to CountryCapacitySnapshot (TR, {opts['year']})"
        ))
