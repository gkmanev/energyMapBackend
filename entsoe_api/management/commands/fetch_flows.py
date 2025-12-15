# management/commands/fetch_flows.py
import os
import json
import itertools
import datetime as dt
from typing import Dict, List, Tuple, Union, Iterable
from collections import Counter

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from dotenv import load_dotenv
import pandas as pd  # noqa: F401 (kept for potential debugging/exports)

from entsoe_api.entsoe_data import EntsoePhysicalFlows
from entsoe_api.helper import save_flows_df

load_dotenv()

# ----------------- local helpers -----------------
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

def _floor_to_step(d: dt.datetime, minutes: int = 15) -> dt.datetime:
    d = d.astimezone(dt.timezone.utc).replace(second=0, microsecond=0)
    return d - dt.timedelta(minutes=d.minute % minutes)

def _iter_eics(val: Union[str, Iterable[str]]) -> Iterable[str]:
    """Yield normalized EICs from str or iterable[str]."""
    if isinstance(val, (list, tuple, set)):
        for item in val:
            if item:
                s = str(item).strip()
                if s:
                    yield s
    else:
        if val:
            s = str(val).strip()
            if s:
                yield s

def _load_country_to_eics_from_settings() -> Dict[str, Union[str, List[str]]]:
    """
    Prefer ENTSOE_COUNTRY_TO_EICS; fall back to ENTSOE_PRICE_COUNTRY_TO_EICS.
    Values may be str or list[str].
    """
    primary = getattr(settings, "ENTSOE_COUNTRY_TO_EICS", None)
    legacy = getattr(settings, "ENTSOE_PRICE_COUNTRY_TO_EICS", None)
    chosen = primary if isinstance(primary, dict) and primary else legacy
    if not isinstance(chosen, dict) or not chosen:
        raise CommandError(
            "Neither settings.ENTSOE_COUNTRY_TO_EICS nor "
            "settings.ENTSOE_PRICE_COUNTRY_TO_EICS is defined with a non-empty dict."
        )
    return {str(iso).upper().strip(): eics for iso, eics in chosen.items() if str(iso).strip()}

def _dedupe_pairs(pairs: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """Remove exact duplicate EIC pairs while preserving order."""
    seen = set()
    out = []
    for p in pairs:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out
# -------------------------------------------------

# Default neighbors; override in settings.ENTSOE_NEIGHBORS_BY_COUNTRY
DEFAULT_NEIGHBORS_BY_COUNTRY = {
    "AD": {"FR", "ES"},
    "AL": {"ME", "XK", "MK", "GR"},
    "AM": {"GE", "AZ", "TR", "IR"},
    "AT": {"DE", "CZ", "SK", "HU", "SI", "IT", "CH", "LI"},
    "AZ": {"RU", "GE", "AM", "IR", "TR"},
    "BA": {"HR", "RS", "ME"},
    "BE": {"FR", "DE", "NL", "LU", "UK"},
    # Bulgaria: Romania, Greece, Turkey, Serbia, North Macedonia
    "BG": {"RO", "GR", "TR", "RS", "MK"},
    "BY": {"PL", "LT", "LV", "RU", "UA"},
    "CH": {"FR", "DE", "AT", "IT", "LI"},
    "CY": set(),
    "CZ": {"DE", "PL", "SK", "AT"},
    "DE": {"DK", "PL", "CZ", "AT", "CH", "FR", "LU", "BE", "NL"},
    "DK": {"DE", "SE", "NO"},
    "EE": {"LV", "RU", "FI"},
    "ES": {"PT", "FR", "AD"},
    "FI": {"SE", "NO", "RU", "EE"},
    "FR": {"BE", "LU", "DE", "CH", "IT", "ES", "AD", "UK"},
    "GE": {"RU", "AM", "AZ", "TR"},
    "GR": {"AL", "MK", "BG", "TR"},
    "HR": {"SI", "HU", "RS", "BA", "ME"},
    "HU": {"AT", "SK", "UA", "RO", "RS", "HR", "SI"},
    "IE": {"UK"},
    "IS": set(),
    "IT": {"FR", "CH", "AT", "SI"},
    "LI": {"AT", "CH"},
    "LT": {"LV", "PL", "BY", "RU"},
    "LU": {"BE", "FR", "DE"},
    "LV": {"EE", "LT", "RU", "BY"},
    "MC": {"FR"},
    "MD": {"RO", "UA"},
    "ME": {"HR", "BA", "RS", "XK", "AL"},
    "MK": {"AL", "GR", "RS", "BG", "XK"},
    "MT": {"IT"},
    "NL": {"BE", "DE", "UK"},
    "NO": {"SE", "FI", "RU", "DK", "UK"},
    "PL": {"DE", "CZ", "SK", "UA", "BY", "LT", "SE"},
    "PT": {"ES"},
    "RO": {"UA", "MD", "BG", "RS", "HU"},
    "RS": {"HU", "RO", "BG", "MK", "XK", "BA", "HR", "ME"},
    "SE": {"NO", "FI", "DK", "PL", "LT"},
    "SI": {"IT", "AT", "HU", "HR"},
    "SK": {"CZ", "PL", "UA", "HU", "AT"},
    "TR": {"BG", "GR", "GE", "AM"},
    "UA": {"PL", "SK", "HU", "RO", "MD", "BY", "RU"},
    "UK": {"IE", "FR", "NL", "BE", "NO"},
    "XK": {"AL", "ME", "MK", "RS"},
}

class Command(BaseCommand):
    help = (
        "Fetch ENTSO-E Cross-Border Physical Flows (A11) for NEIGHBORS ONLY (bidirectional).\n"
        "Examples:\n"
        "  python manage.py fetch_flows --countries BG --dry-run\n"
        "  python manage.py fetch_flows --countries '[\"BG\",\"RO\"]' --hours 48\n"
        "  python manage.py fetch_flows --countries BG --start 2025-10-20T00:00:00Z --end 2025-10-21T00:00:00Z\n"
        "Notes:\n"
        "  - Expands multi-zone countries (multiple EICs).\n"
        "  - Only connects each selected country to its configured border neighbors, in BOTH directions.\n"
        "  - Neighbor map can be overridden via settings.ENTSOE_NEIGHBORS_BY_COUNTRY.\n"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--countries",
            type=str,
            required=False,
            help=(
                "Single ISO (e.g. BG) or JSON array of ISOs, e.g. '[\"BG\",\"RO\",\"GR\"]'. "
                "Pairs ONLY with direct neighbors (bidirectional)."
            ),
        )
        parser.add_argument(
            "--hours",
            type=int,
            default=24,
            help="Lookback window in hours (default: 24). Ignored if --start/--end provided.",
        )
        parser.add_argument("--start", type=str, help="UTC start ISO, e.g. 2025-10-20T00:00:00Z")
        parser.add_argument("--end", type=str, help="UTC end ISO (exclusive), e.g. 2025-10-21T00:00:00Z")
        parser.add_argument(
            "--output",
            type=str,
            help="Optional path to dump JSON payload (debug/export).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Show the expanded neighbor-only ISO/EIC pairs; do not call ENTSO-E or write to DB.",
        )
        parser.add_argument(
            "--all-eu",
            action="store_true",
            default=False,
            help="Fetch flows for all EU countries defined in settings.",
        )

    def handle(self, *args, **options):
        # Load maps
        country_to_eics = _load_country_to_eics_from_settings()
        neighbors_map = getattr(settings, "ENTSOE_NEIGHBORS_BY_COUNTRY", DEFAULT_NEIGHBORS_BY_COUNTRY)

        if not options["all_eu"] and not options.get("countries"):
            raise CommandError("Either --countries or --all-eu must be provided.")

        if options["all_eu"]:
            # Use all countries from your settings
            iso_list = [iso.upper().strip() for iso in country_to_eics.keys()]
            self.stdout.write(f"Fetching for ALL countries: {', '.join(sorted(iso_list))}")
        else:
            if not options["countries"]:
                raise CommandError("Either --countries or --all-eu must be provided.")

            # Parse countries
            raw = options["countries"].strip()
            try:
                countries = json.loads(raw)
            except json.JSONDecodeError:
                countries = [raw]  # allow --countries BG

            if isinstance(countries, str):
                countries = [countries]
            if not isinstance(countries, list) or not all(isinstance(x, str) for x in countries):
                raise CommandError("--countries must be a JSON array or single ISO code string.")

            iso_list = [x.upper().strip() for x in countries]

        # Validate and build neighbor ISO pairs (bidirectional)
        eic_pairs: List[Tuple[str, str]] = []
        missing_isos = [iso for iso in iso_list if iso not in country_to_eics]
        if missing_isos:
            raise CommandError("Unknown ISO code(s): " + ", ".join(sorted(set(missing_isos))))

        skipped_neighbors_no_eic = set()
        missing_neighbor_map = []

        pair_iso_edges = []
        for src in iso_list:
            neigh = set(neighbors_map.get(src, set()))
            if not neigh:
                missing_neighbor_map.append(src)
                continue
            # (src->n) and (n->src), but only if neighbor has an EIC mapping
            valid_neigh = [n for n in sorted(neigh) if n in country_to_eics and n != src]
            skipped_neighbors_no_eic.update({n for n in neigh if n not in country_to_eics})

            pair_iso_edges.extend((src, n) for n in valid_neigh)
            pair_iso_edges.extend((n, src) for n in valid_neigh)

        if missing_neighbor_map:
            self.stdout.write(self.style.WARNING(
                "No neighbors configured for: " + ", ".join(sorted(set(missing_neighbor_map)))
            ))

        # Expand ISO pairs to EIC pairs
        for out_iso, in_iso in pair_iso_edges:
            out_eics = list(_iter_eics(country_to_eics[out_iso]))
            in_eics = list(_iter_eics(country_to_eics[in_iso]))
            for o in out_eics:
                for i in in_eics:
                    eic_pairs.append((o, i))

        eic_pairs = _dedupe_pairs(eic_pairs)
        if not eic_pairs:
            raise CommandError("No EIC pairs found to query (neighbors-only produced nothing).")

        # ---------- reporting / dry-run ----------
        # Build reverse EIC->ISO for summaries
        rev = {}
        for iso, eics in country_to_eics.items():
            for e in _iter_eics(eics):
                rev[e] = iso

        def _iso_of(eic: str) -> str:
            return rev.get(eic, "?")

        # Neighbor counts per selected ISO
        neighbor_counts = {}
        for src in iso_list:
            neigh = set(neighbors_map.get(src, set()))
            neighbor_counts[src] = len([n for n in neigh if n in country_to_eics and n != src])

        iso_edge_set = {(_iso_of(o), _iso_of(i)) for (o, i) in eic_pairs}
        involved = sorted(set([a for a, _ in iso_edge_set] + [b for _, b in iso_edge_set if b != "?"]))

        self.stdout.write(
            "Neighbors-only mode — built "
            f"{len(eic_pairs)} directed EIC pairs across borders. "
            f"Involved countries: {', '.join(involved)}"
        )
        self.stdout.write(
            "Neighbor counts: " + ", ".join(f"{k}:{v}" for k, v in sorted(neighbor_counts.items()))
        )
        if skipped_neighbors_no_eic:
            self.stdout.write(self.style.WARNING(
                "Neighbors without EIC mapping (skipped): " + ", ".join(sorted(skipped_neighbors_no_eic))
            ))

        if options["dry_run"]:
            show = min(10, len(eic_pairs))
            sample = ", ".join([f"{o}->{i}" for (o, i) in eic_pairs[:show]])
            self.stdout.write(f"Sample EIC pairs ({show}): {sample}")
            return

        # ---------- time window ----------
        start_opt = options.get("start")
        end_opt = options.get("end")
        if start_opt or end_opt:
            if not (start_opt and end_opt):
                raise CommandError("Provide BOTH --start and --end, or neither.")
            try:
                start = _floor_to_step(_parse_iso_utc(start_opt), 15)
                end = _floor_to_step(_parse_iso_utc(end_opt), 15)
            except Exception:
                raise CommandError("Invalid --start/--end format. Use ISO e.g. 2025-10-20T00:00:00Z")
            if start >= end:
                raise CommandError("--start must be earlier than --end.")
        else:
            hours = options["hours"]
            if hours <= 0:
                raise CommandError("--hours must be > 0.")
            now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
            end = now_utc.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
            start = end - dt.timedelta(hours=hours)

        # ---------- fetch ----------
        api_key = os.getenv("ENTSOE_TOKEN")
        if not api_key:
            raise CommandError("Missing ENTSOE_TOKEN in environment or .env file.")

        self.stdout.write(f"Fetching A11 flows for {len(eic_pairs)} neighbor-directed EIC pairs...")
        df = EntsoePhysicalFlows.query_pairs(
            api_key=api_key,
            pairs=eic_pairs,
            start=start,
            end=end,
        )

        if df.empty:
            self.stdout.write(self.style.WARNING("No flow data returned."))
            return

        written = save_flows_df(df)
        self.stdout.write(self.style.SUCCESS(f"Saved {written} cross-border flow rows."))

        # ---------- optional JSON output ----------
        out_path = options.get("output")
        if out_path:
            payload = df.to_dict(orient="records")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            self.stdout.write(self.style.SUCCESS(f"Wrote JSON to {out_path}"))
