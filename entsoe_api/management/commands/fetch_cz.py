import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from django.core.management.base import BaseCommand, CommandError
from dotenv import load_dotenv
load_dotenv()

ENTSOE_API = "https://web-api.tp.entsoe.eu/api"
CZ_AREA = "10YCZ-CEPS-----N"

PSR_MAP = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B09": "Solar",
    "B10": "Wind Onshore",
    "B11": "Waste",
    "B12": "Nuclear",
    "B13": "Hydro Pumped Storage",
    "B14": "Hydro Run-of-river",
    "B15": "Hydro Reservoir",
    "B17": "Other renewable",
    "B18": "Other",
}

def parse_duration_minutes(dur: str) -> int:
    """Convert ENTSO-E PTxxM or PTxxH duration to minutes."""
    if not dur or not dur.startswith("P"):
        return 60
    dur = dur[1:]
    if dur.startswith("T"):
        dur = dur[1:]
    h = m = 0
    val = ""
    for ch in dur:
        if ch.isdigit():
            val += ch
        elif ch == "H":
            h = int(val or 0)
            val = ""
        elif ch == "M":
            m = int(val or 0)
            val = ""
    return h * 60 + m


class Command(BaseCommand):
    help = "Print the latest actual generation per production type for Czech Republic (ČEPS) from ENTSO-E."

    def handle(self, *args, **options):
        token = os.getenv("ENTSOE_TOKEN")
        if not token:
            raise CommandError("Missing ENTSOE_TOKEN environment variable. Add it to .env or export it.")

        now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        end = now_utc + timedelta(hours=1)      # exclusive
        start = end - timedelta(hours=6)        # wider window to ensure coverage
        fmt = "%Y%m%d%H%M"

        params = {
            "securityToken": token,
            "documentType": "A75",
            "processType": "A16",               # Realised
            "in_Domain": CZ_AREA,
            "periodStart": start.strftime(fmt),
            "periodEnd": end.strftime(fmt),
        }

        print(f"Fetching real-time generation for CZ ({CZ_AREA}) {start} → {end} UTC ...")

        try:
            r = requests.get(ENTSOE_API, params=params, timeout=90)
            r.raise_for_status()
        except requests.HTTPError as e:
            print("Status:", r.status_code)
            print("URL:", r.url)
            print("Body (first 400):", r.text[:400])
            raise CommandError(f"HTTPError: {e}")
        except requests.RequestException as e:
            raise CommandError(f"Network error: {e}")

        text = r.text.strip()
        if not text:
            print("Empty response body from ENTSO-E.")
            sys.exit(0)

        # Parse XML
        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            raise CommandError(f"Failed to parse XML: {e}")

        # Error payload detection
        if root.tag.endswith("Acknowledgement_MarketDocument") or root.tag.endswith("Error"):
            msg = root.find(".//{*}text")
            msg_text = msg.text if msg is not None else "(no message)"
            raise CommandError(f"ENTSO-E error: {msg_text}")

        ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generation:power:1:0"}
        ts_list = root.findall(".//ns:TimeSeries", ns) or root.findall(".//{*}TimeSeries")

        if not ts_list:
            print("No TimeSeries found in response.")
            print("Response head:", text[:400])
            sys.exit(0)

        points = []  # list of (timestamp_utc, name, MW)

        for ts in ts_list:
            psr = (ts.findtext(".//{*}psrType") or "").strip()
            name = PSR_MAP.get(psr, psr or "Unknown")

            period = ts.find(".//{*}Period")
            if not period:
                continue

            start_text = period.findtext(".//{*}timeInterval/{*}start")
            res_text = period.findtext(".//{*}resolution") or "PT60M"
            if not start_text:
                continue

            try:
                block_start = datetime.fromisoformat(start_text.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                continue

            step_min = parse_duration_minutes(res_text)
            for p in period.findall(".//{*}Point"):
                pos_text = p.findtext(".//{*}position")
                qty_text = p.findtext(".//{*}quantity")
                if not pos_text or qty_text is None:
                    continue
                try:
                    pos = int(pos_text)
                    qty = float(qty_text)
                except ValueError:
                    continue
                ts_utc = block_start + timedelta(minutes=(pos - 1) * step_min)
                points.append((ts_utc, name, qty))

        if not points:
            print("No generation data points found.")
            sys.exit(0)

        latest_time = max(t for t, _, _ in points)
        latest_rows = [(n, q) for t, n, q in points if t == latest_time]
        latest_rows.sort(key=lambda x: x[0])

        total = sum(q for _, q in latest_rows)

        print("")
        print(f"CZ Generation Mix (ČEPS) at {latest_time:%Y-%m-%d %H:%M UTC}")
        print("-" * 60)
        for name, qty in latest_rows:
            pct = (qty / total * 100) if total else 0
            print(f"{name:<35} {qty:>8.0f} MW   ({pct:>5.1f}%)")
        print("-" * 60)
        print(f"TOTAL{'':<30} {total:>8.0f} MW")
