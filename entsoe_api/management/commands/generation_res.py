import os
import json
import datetime as dt
from typing import Iterable, List, Optional, Set
from xml.etree import ElementTree as ET

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from dotenv import load_dotenv

from entsoe_api.entsoe_data import BASE_URL, PSRTYPE_MAPPINGS

load_dotenv()

DEFAULT_PSR_TYPES = {"B16", "B18", "B19"}  # Solar, Wind Offshore, Wind Onshore


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


def _floor_to_hour(d: dt.datetime) -> dt.datetime:
    d = d.astimezone(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    return d


def _to_utc_compact(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    d = d.astimezone(dt.timezone.utc)
    return d.strftime("%Y%m%d%H%M")


def _iso_duration_to_minutes(duration: str) -> int:
    if not duration or not duration.startswith("P"):
        return 0
    days = hours = minutes = 0
    date_part, time_part = duration, ""
    if "T" in duration:
        date_part, time_part = duration.split("T", 1)
    if "D" in date_part:
        d = date_part.replace("P", "").split("D")[0]
        days = int(d or 0)
    if "H" in time_part:
        h = time_part.split("H")[0]
        hours = int(h or 0)
        time_part = time_part.split("H", 1)[1] if "H" in time_part else ""
    if "M" in time_part:
        m = time_part.split("M")[0]
        minutes = int(m or 0)
    return days * 1440 + hours * 60 + minutes


def _iter_eics(val) -> Iterable[str]:
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


def _parse_a69(xml_text: str, zone_eic: str, allowed_psr: Optional[Set[str]]) -> List[dict]:
    root = ET.fromstring(xml_text)
    ns_uri = root.tag.split("}")[0].strip("{") if "}" in root.tag else ""
    ns = {"ns": ns_uri} if ns_uri else {}

    reasons = root.findall(".//ns:Reason", ns) if ns else root.findall(".//Reason")
    for reason in reasons:
        if ns:
            code = (reason.findtext("ns:code", default="", namespaces=ns) or "").strip()
            text = (reason.findtext("ns:text", default="", namespaces=ns) or "").strip()
        else:
            code = (reason.findtext("code", default="") or "").strip()
            text = (reason.findtext("text", default="") or "").strip()
        if code or text:
            raise RuntimeError(f"ENTSOE API error: {code} {text}".strip())

    series = root.findall(".//ns:TimeSeries", ns) if ns else root.findall(".//TimeSeries")
    if not series:
        return []

    records: List[dict] = []
    for ts in series:
        if ns:
            psr_type = ts.findtext("ns:MktPSRType/ns:psrType", default="", namespaces=ns)
            unit = ts.findtext("ns:quantity_Measure_Unit.name", default="", namespaces=ns)
            zone = ts.findtext("ns:inBiddingZone_Domain.mRID", default="", namespaces=ns)
            ts_resolution = ts.findtext("ns:resolution", default="", namespaces=ns)
            periods = ts.findall("ns:Period", ns)
        else:
            psr_type = ts.findtext("MktPSRType/psrType", default="")
            unit = ts.findtext("quantity_Measure_Unit.name", default="")
            zone = ts.findtext("inBiddingZone_Domain.mRID", default="")
            ts_resolution = ts.findtext("resolution", default="")
            periods = ts.findall("Period")

        psr_type = (psr_type or "").strip()
        if allowed_psr and psr_type not in allowed_psr:
            continue

        psr_name = PSRTYPE_MAPPINGS.get(psr_type, psr_type or None)
        zone = (zone or zone_eic or "").strip()

        for period in periods:
            if ns:
                start_str = period.findtext("ns:timeInterval/ns:start", default="", namespaces=ns)
                resolution = period.findtext("ns:resolution", default=ts_resolution, namespaces=ns)
                points = period.findall("ns:Point", ns)
            else:
                start_str = period.findtext("timeInterval/start", default="")
                resolution = period.findtext("resolution", default=ts_resolution)
                points = period.findall("Point")

            if not start_str:
                if ns:
                    start_str = root.findtext("ns:time_Period.timeInterval/ns:start", default="", namespaces=ns)
                else:
                    start_str = root.findtext("time_Period.timeInterval/start", default="")
            if not start_str:
                continue

            try:
                start_dt = dt.datetime.fromisoformat(start_str.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            except ValueError:
                continue

            resolution = (resolution or ts_resolution or "PT60M").strip()
            step_min = _iso_duration_to_minutes(resolution) or 60

            for pt in points:
                if ns:
                    pos_text = pt.findtext("ns:position", default="1", namespaces=ns)
                    qty_text = pt.findtext("ns:quantity", default=None, namespaces=ns)
                else:
                    pos_text = pt.findtext("position", default="1")
                    qty_text = pt.findtext("quantity", default=None)

                if qty_text is None:
                    continue
                try:
                    pos = int(pos_text or "1")
                    qty = float(qty_text)
                except (ValueError, TypeError):
                    continue

                ts_dt = start_dt + dt.timedelta(minutes=step_min * (pos - 1))
                records.append({
                    "datetime_utc": ts_dt,
                    "zone": zone,
                    "psr_type": psr_type or None,
                    "psr_name": psr_name,
                    "generation_MW": qty,
                    "unit": unit or None,
                    "resolution": resolution,
                })

    records.sort(key=lambda r: (r["zone"] or "", r["psr_type"] or "", r["datetime_utc"]))
    return records


class Command(BaseCommand):
    help = (
        "Fetch ENTSO-E A69 generation for solar and wind (B16/B18/B19) "
        "and print the parsed results."
    )

    def add_arguments(self, parser):
        parser.add_argument("--domain", type=str, help="EIC domain (in_Domain).")
        parser.add_argument("--country", type=str, help="ISO country code, e.g. BG.")
        parser.add_argument(
            "--start",
            type=str,
            help="UTC start ISO, e.g. 2026-02-06T00:00:00Z.",
        )
        parser.add_argument(
            "--end",
            type=str,
            help="UTC end ISO (exclusive), e.g. 2026-02-06T22:00:00Z.",
        )
        parser.add_argument(
            "--hours",
            type=int,
            default=24,
            help="Lookback window in hours (default: 24). Ignored if --start/--end provided.",
        )
        parser.add_argument(
            "--psr-types",
            type=str,
            default="B16,B18,B19",
            help="Comma-separated PSR types to include (default: B16,B18,B19).",
        )

    def handle(self, *args, **options):
        api_key = (
            getattr(settings, "ENTSOE_API_KEY", "")
            or os.getenv("ENTSOE_API_KEY")
            or os.getenv("ENTSOE_TOKEN")
        )
        if not api_key:
            raise CommandError("Missing ENTSOE_API_KEY or ENTSOE_TOKEN in env/settings.")

        domain_opt = (options.get("domain") or "").strip()
        country_opt = (options.get("country") or "").strip().upper()

        mapping = getattr(settings, "ENTSOE_COUNTRY_TO_EICS", None)
        if not isinstance(mapping, dict):
            mapping = {}

        if domain_opt:
            domains = [domain_opt]
        elif country_opt:
            if country_opt not in mapping:
                known = ", ".join(sorted(mapping.keys()))
                raise CommandError(f"Unknown country '{country_opt}'. Known: {known}")
            domains = list(_iter_eics(mapping[country_opt]))
        else:
            if "BG" in mapping:
                domains = list(_iter_eics(mapping["BG"]))
            else:
                raise CommandError("Provide --domain or --country (default BG not found in settings).")

        if not domains:
            raise CommandError("No EIC domains found to query.")

        start_opt = options.get("start")
        end_opt = options.get("end")
        if start_opt or end_opt:
            if not (start_opt and end_opt):
                raise CommandError("Provide BOTH --start and --end, or neither.")
            try:
                start = _floor_to_hour(_parse_iso_utc(start_opt))
                end = _floor_to_hour(_parse_iso_utc(end_opt))
            except Exception:
                raise CommandError("Invalid --start/--end format. Use ISO e.g. 2026-02-06T00:00:00Z")
            if start >= end:
                raise CommandError("--start must be earlier than --end.")
        else:
            hours = options.get("hours")
            if not hours or hours <= 0:
                raise CommandError("--hours must be > 0.")
            now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
            end = _floor_to_hour(now_utc) + dt.timedelta(hours=1)
            start = end - dt.timedelta(hours=hours)

        psr_types = {s.strip() for s in (options.get("psr_types") or "").split(",") if s.strip()}
        if not psr_types:
            psr_types = DEFAULT_PSR_TYPES

        all_records: List[dict] = []
        for domain in domains:
            params = {
                "documentType": "A69",
                "processType": "A01",
                "in_Domain": domain,
                "periodStart": _to_utc_compact(start),
                "periodEnd": _to_utc_compact(end),
                "securityToken": api_key,
            }

            self.stdout.write(
                f"Fetching A69 for {domain} in [{start:%Y-%m-%d %H:%MZ}, {end:%Y-%m-%d %H:%MZ})..."
            )
            try:
                resp = requests.get(BASE_URL, params=params, timeout=60)
                resp.raise_for_status()
            except requests.HTTPError as e:
                raise CommandError(f"HTTP error from ENTSOE: {e} (status {resp.status_code})")
            except requests.RequestException as e:
                raise CommandError(f"Network error from ENTSOE: {e}")

            records = _parse_a69(resp.text, domain, psr_types)
            all_records.extend(records)

        if not all_records:
            self.stdout.write(self.style.WARNING("No data returned."))
            return

        for rec in all_records:
            ts = rec.get("datetime_utc")
            if isinstance(ts, dt.datetime):
                rec["datetime_utc"] = ts.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = json.dumps(all_records, ensure_ascii=False, indent=2)
        self.stdout.write(payload)
