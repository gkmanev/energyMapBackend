# entsoe_api/management/commands/fetch_generation_tr.py
"""
Fetch Turkish real-time generation by source from the EPİAŞ Transparency
Platform and store it in CountryGenerationByType (country=TR), mapped to
ENTSO-E PSR codes so it lines up with the other countries.

Credentials (settings or .env): EPIAS_USERNAME / EPIAS_PASSWORD

Examples:
    python manage.py fetch_generation_tr --period today
    python manage.py fetch_generation_tr --start 2026-11-01 --end 2026-11-10
    python manage.py fetch_generation_tr --period yesterday --dry-run
"""
from __future__ import annotations

import pandas as pd
from django.core.management.base import BaseCommand
from dotenv import load_dotenv

from entsoe_api import epias
from entsoe_api.entsoe_data import PSRTYPE_MAPPINGS
from entsoe_api.helper import save_generation_df

load_dotenv()

RT_GEN_PATH = "/v1/generation/data/realtime-generation"

# EPİAŞ realtime-generation field -> ENTSO-E PSR code. Fields sharing a code are summed.
# Not mapped on purpose: 'total' (sum) and 'importExport' (net cross-border flow).
EPIAS_TO_PSR = {
    "naturalGas": "B04",
    "lng": "B04",
    "lignite": "B02",
    "importCoal": "B05",
    "blackCoal": "B05",
    "asphaltiteCoal": "B05",
    "fueloil": "B06",
    "gasOil": "B06",
    "naphta": "B06",
    "geothermal": "B09",
    "river": "B11",
    "dammedHydro": "B12",
    "nuclear": "B14",
    "sun": "B16",
    "wind": "B19",
    "biomass": "B01",
    "wasteheat": "B20",
}
IGNORED_FIELDS = {"date", "hour", "total", "importExport"}


def items_to_df(items: list[dict], log=None) -> pd.DataFrame:
    """Convert realtime-generation items into the long frame save_generation_df expects."""
    if not items:
        return pd.DataFrame()
    unknown = sorted({k for it in items for k in it} - set(EPIAS_TO_PSR) - IGNORED_FIELDS)
    if unknown and log:
        log(f"  ⚠️  Unmapped EPİAŞ fields ignored: {', '.join(unknown)}")

    rows = []
    for it in items:
        ts = pd.Timestamp(it["date"]).tz_convert("UTC")
        by_psr: dict[str, float] = {}
        for field, psr in EPIAS_TO_PSR.items():
            val = it.get(field)
            if val is None:
                continue
            by_psr[psr] = by_psr.get(psr, 0.0) + float(val)
        for psr, mw in by_psr.items():
            rows.append({
                "country": "TR",
                "datetime_utc": ts.to_pydatetime(),
                "psr_type": psr,
                "psr_name": PSRTYPE_MAPPINGS.get(psr, psr),
                "generation_MW": round(mw, 3),
                "resolution": "PT60M",
            })
    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["datetime_utc", "psr_type"], keep="last").sort_values(
        ["datetime_utc", "psr_type"]
    ).reset_index(drop=True)


class Command(BaseCommand):
    help = (
        "Fetch Turkish real-time generation by source from EPİAŞ and store into "
        "CountryGenerationByType (country=TR, ENTSO-E PSR codes)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--period", choices=["today", "yesterday"],
                            help="Turkish-local day shortcut. Use this OR --start/--end.")
        parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD (Turkish time, inclusive).")
        parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (Turkish time, inclusive). Defaults to --start.")
        parser.add_argument("--chunk-days", type=int, default=7,
                            help="Max days per API request (default: 7).")
        parser.add_argument("--max-retries", type=int, default=3, help="Retries per request (default: 3).")
        parser.add_argument("--dry-run", action="store_true", help="Fetch and show rows but do NOT write to DB.")

    def handle(self, *args, **opts):
        username, password = epias.get_credentials()
        start, end = epias.compute_dates(opts.get("period"), opts.get("start"), opts.get("end"))
        dry_run = opts["dry_run"]
        warn = lambda m: self.stdout.write(self.style.WARNING(m))  # noqa: E731

        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("EPİAŞ Turkey Real-Time Generation Fetch"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"📅 Dates (TR local, inclusive): {start} → {end}")
        if dry_run:
            warn("🔍 DRY RUN MODE: Data will NOT be saved to database")

        tgt = epias.get_tgt(username, password)

        frames = []
        for cur, chunk_end in epias.iter_chunks(start, end, opts["chunk_days"]):
            self.stdout.write(f"  ⬇️  Fetching {cur} → {chunk_end} ...")
            js = epias.post_with_retry(
                tgt, RT_GEN_PATH, epias.date_range_body(cur, chunk_end),
                max_retries=opts["max_retries"], log=warn,
            )
            items = js.get("items") or []
            df = items_to_df(items, log=warn)
            self.stdout.write(f"     {len(items)} hours → {len(df)} rows")
            if not df.empty:
                frames.append(df)

        if not frames:
            warn("⚠️  No data returned.")
            return

        df = pd.concat(frames, ignore_index=True).drop_duplicates(
            subset=["datetime_utc", "psr_type"], keep="last"
        )
        self.stdout.write(
            f"📊 {len(df)} rows, {df['datetime_utc'].nunique()} hours: "
            f"{df['datetime_utc'].min():%Y-%m-%d %H:%MZ} → {df['datetime_utc'].max():%Y-%m-%d %H:%MZ}"
        )

        if dry_run:
            pivot = df.pivot_table(index="datetime_utc", columns="psr_type", values="generation_MW", aggfunc="sum")
            self.stdout.write(pivot.head(6).to_string())
            self.stdout.write(self.style.SUCCESS(f"🔍 Dry-run: {len(df)} rows would have been saved"))
            return

        written = save_generation_df(df)
        self.stdout.write(self.style.SUCCESS(f"💾 Saved {written} rows to CountryGenerationByType (TR)"))
