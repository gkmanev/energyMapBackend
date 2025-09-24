# entsoe_installed_capacity.py

import datetime as dt
from typing import Optional, List, Dict, Union, Iterable, Tuple
import xml.etree.ElementTree as ET

import pandas as pd
import requests


BASE_URL = "https://web-api.tp.entsoe.eu/api"

# Extend this mapping anytime ENTSO-E adds new PSR codes.
PSRTYPE_MAPPINGS: Dict[str, str] = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and pondage",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
    # Leave unknown codes unmapped; they will show up as the code itself (e.g., "B25").
}

CONTRACT_TYPES = {
    "A01": "Day-ahead",
    "A07": "Intraday",
    # extend if ENTSO-E adds more
}


class EntsoeInstalledCapacity:
    """
    Fetch the most recently published 'Installed Capacity per Production Type' (A68/A33) from ENTSO-E.

    Key points:
      - Uses in_Domain and processType=A33 (Year-ahead).
      - Returns a pandas DataFrame with columns:
            ['psr_type','psr_name','installed_capacity_MW','valid_from_utc','year','zone']
      - 'valid_from_utc' is the timestamp from the A68 document (often Dec 31 23:00Z).
      - 'year' is the calendar year that snapshot applies to.
    """

    def __init__(self, api_key: str, session: Optional[requests.Session] = None):
        self.api_key = api_key
        self.session = session or requests.Session()

    # ---------- internal helpers ----------

    @staticmethod
    def _to_utc_compact(d: dt.datetime) -> str:
        """Format datetime as yyyyMMddHHmm UTC for ENTSO-E."""
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        d = d.astimezone(dt.timezone.utc)
        return d.strftime("%Y%m%d%H%M")

    def _request_with_retries(self, params: dict, max_retries: int = 5, timeout: int = 45) -> str:
        """GET with basic retry/backoff on 429/5xx; raise on other errors."""
        backoff = 1.0
        for _ in range(max_retries):
            r = self.session.get(BASE_URL, params=params, timeout=timeout)
            if r.status_code == 200 and r.text.strip():
                return r.text
            if r.status_code in (429, 500, 502, 503, 504):
                # Respect Retry-After if present; otherwise exponential backoff.
                retry_after = r.headers.get("Retry-After")
                try:
                    sleep_s = float(retry_after) if retry_after else backoff
                except ValueError:
                    sleep_s = backoff
                import time as _t
                _t.sleep(sleep_s)
                backoff = min(backoff * 2, 30)
                continue
            # Non-retryable HTTP -> try to surface XML body error, then raise.
            try:
                ET.fromstring(r.text)
            except Exception:
                pass
            r.raise_for_status()
        raise TimeoutError("ENTSO-E request failed after retries")

    @staticmethod
    def _parse_a68(xml_text: str, zone: str) -> List[Dict]:
        """Parse A68 XML -> list of dicts (one per PSR/time point)."""
        ns = {"gl": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
        root = ET.fromstring(xml_text)

        # ENTSO-E sometimes encodes errors inside 200 OK XML bodies via <Reason>
        for reason in root.findall(".//gl:Reason", ns):
            code = (reason.findtext("gl:code", default="", namespaces=ns) or "").strip()
            text = (reason.findtext("gl:text", default="", namespaces=ns) or "").strip()
            raise RuntimeError(f"ENTSO-E API error: {code} {text}".strip())

        rows: List[Dict] = []
        for ts in root.findall("gl:TimeSeries", ns):
            psr_code = ts.findtext("gl:MktPSRType/gl:psrType", default="", namespaces=ns)
            psr_name = PSRTYPE_MAPPINGS.get(psr_code, psr_code or None)

            for period in ts.findall("gl:Period", ns):
                start_str = (
                    period.findtext("gl:timeInterval/gl:start", default="", namespaces=ns)
                    or root.findtext("gl:time_Period.timeInterval/gl:start", default="", namespaces=ns)
                )
                if not start_str:
                    continue
                start_dt = dt.datetime.strptime(start_str.replace("Z", "+0000"), "%Y-%m-%dT%H:%M%z")

                for pt in period.findall("gl:Point", ns):
                    qty = pt.findtext("gl:quantity", default=None, namespaces=ns)
                    try:
                        val = float(qty) if qty is not None else None
                    except ValueError:
                        val = None

                    rows.append({
                        "datetime_utc": start_dt.astimezone(dt.timezone.utc).replace(tzinfo=None),
                        "zone": zone,
                        "psr_type": psr_code or None,
                        "psr_name": psr_name,
                        "installed_capacity_MW": val,
                    })

        rows.sort(key=lambda r: (r["datetime_utc"], r.get("psr_type") or ""))
        return rows

    def _query_a68_window(
        self,
        zone_eic: str,
        start_utc: dt.datetime,
        end_utc: dt.datetime,
        psr_type: Optional[str] = None,
    ) -> List[Dict]:
        params = {
            "documentType": "A68",
            "processType": "A33",      # Year-ahead
            "in_Domain": zone_eic,     # generation-side domain
            "periodStart": self._to_utc_compact(start_utc),
            "periodEnd": self._to_utc_compact(end_utc),
            "securityToken": self.api_key,
        }
        if psr_type:
            params["psrType"] = psr_type
        xml_text = self._request_with_retries(params)
        return self._parse_a68(xml_text, zone_eic)

    @staticmethod
    def _window_for_year(year: int):
        """Return [Dec 31 (prev) 23:00Z, Dec 31 (year) 23:00Z] — ≤ 1 year per ENTSO-E rule."""
        return (
            dt.datetime(year - 1, 12, 31, 23, 0, tzinfo=dt.timezone.utc),
            dt.datetime(year,     12, 31, 23, 0, tzinfo=dt.timezone.utc),
        )

    # ---------- public API ----------

    def get_latest(
        self,
        zone_eic: str,
        psr_type: Optional[str] = None,
        now_utc: Optional[dt.datetime] = None
    ) -> pd.DataFrame:
        """
        Return the most recently published A68 snapshot for a single zone.

        Columns: ['psr_type','psr_name','installed_capacity_MW','valid_from_utc','year','zone']
        """
        # Anchor time in UTC
        if now_utc is None:
            now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        elif now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=dt.timezone.utc)
        else:
            now_utc = now_utc.astimezone(dt.timezone.utc)

        # Probe current year; if empty, fall back one year
        cur_y = now_utc.year
        start_cur, end_cur = self._window_for_year(cur_y)
        records = self._query_a68_window(zone_eic, start_cur, end_cur, psr_type)

        if records:
            target_year, window_end = cur_y, end_cur
        else:
            prev_y = cur_y - 1
            start_prev, end_prev = self._window_for_year(prev_y)
            records = self._query_a68_window(zone_eic, start_prev, end_prev, psr_type)
            target_year, window_end = prev_y, end_prev

        df = pd.DataFrame.from_records(records)
        if df.empty:
            # Return an empty frame with the expected schema
            return pd.DataFrame(columns=[
                "psr_type","psr_name","installed_capacity_MW","valid_from_utc","year","zone"
            ])

        # Keep the latest point per PSR up to window_end
        df = df[df["datetime_utc"] <= window_end.replace(tzinfo=None)]
        df = (
            df.sort_values(["psr_type", "datetime_utc"])
              .groupby("psr_type", as_index=False).tail(1)
              .rename(columns={"datetime_utc": "valid_from_utc"})
        )
        df["year"] = target_year
        df["zone"] = zone_eic

        return df[["psr_type","psr_name","installed_capacity_MW","valid_from_utc","year","zone"]]\
                 .sort_values("psr_type").reset_index(drop=True)

    @classmethod
    def query_all_countries(
        cls,
        api_key: str,
        country_to_eics: Dict[str, Union[str, List[str]]],
        psr_type: Optional[str] = None,
        aggregate_by_country: bool = True,
        now_utc: Optional[dt.datetime] = None,
        skip_errors: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch the latest installed capacity for MANY countries.

        Args:
            api_key: ENTSO-E token
            country_to_eics: mapping of ISO country code -> EIC zone OR list of zones
                             e.g. {'CZ': '10YCZ-CEPS-----N',
                                    'DK': ['10YDK-1--------W','10YDK-2--------M']}
            psr_type: optional PSR filter (e.g., 'B16')
            aggregate_by_country: sum capacities across zones into country totals
            now_utc: optional anchor time
            skip_errors: if True, zones that raise are skipped (and logged via print)

        Returns:
            DataFrame
              aggregate=True  -> ['country','psr_type','psr_name','installed_capacity_MW','year']
              aggregate=False -> ['country','zone','psr_type','psr_name','installed_capacity_MW','year']
        """
        client = cls(api_key)
        frames: List[pd.DataFrame] = []

        for country, zones in country_to_eics.items():
            zone_list = zones if isinstance(zones, list) else [zones]
            for z in zone_list:
                try:
                    df = client.get_latest(z, psr_type=psr_type, now_utc=now_utc)
                    df = df.copy()
                    df["country"] = country
                    frames.append(df)
                except Exception as e:
                    if skip_errors:
                        print(f"[entsoe] Skipping {country}/{z} due to error: {e}")
                        continue
                    raise

        if not frames:
            # Empty result with standard columns
            base_cols = ["country","psr_type","psr_name","installed_capacity_MW","year"]
            if not aggregate_by_country:
                base_cols.insert(1, "zone")
            return pd.DataFrame(columns=base_cols)

        full = pd.concat(frames, ignore_index=True, sort=False)

        if aggregate_by_country:
            ok = full.dropna(subset=["installed_capacity_MW", "year"])
            out = (ok.groupby(["country","psr_type","psr_name","year"], as_index=False)["installed_capacity_MW"]
                     .sum(numeric_only=True)
                     .sort_values(["country","psr_type"])
                     .reset_index(drop=True))
            return out
        else:
            cols = ["country","zone","psr_type","psr_name","installed_capacity_MW","year"]
            keep = [c for c in cols if c in full.columns]
            return (full[keep]
                    .sort_values(["country","zone","psr_type"])
                    .reset_index(drop=True))

    # ---------- small utility for DRF ----------

    @staticmethod
    def to_records(df: pd.DataFrame, datetime_cols: Optional[List[str]] = None) -> List[Dict]:
        """
        Convert a DataFrame to list-of-dicts, ISO-formatting datetime columns for JSON responses.
        Example use in DRF: return Response(EntsoeInstalledCapacity.to_records(df))
        """
        if df.empty:
            return []
        if datetime_cols is None:
            datetime_cols = [c for c in df.columns if "time" in c or "date" in c]
        df2 = df.copy()
        for c in datetime_cols:
            if c in df2.columns:
                df2[c] = pd.to_datetime(df2[c], utc=True, errors="ignore").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return df2.to_dict(orient="records")



#GENERATION###########################################################

class EntsoeGenerationByType:
    """
    Fetch ENTSO-E 'Actual Generation per Production Type' (A75, processType=A16 – Realised).

    Public methods:
      - get_range(zone_eic, start, end, psr_type=None)
      - get_last_hours(zone_eic, hours=24, psr_type=None, now_utc=None)
      - query_all_countries(api_key, country_to_eics, start, end, psr_type=None,
                            aggregate_by_country=False, now_utc=None)

    Returns tidy pandas DataFrames with columns:
      ['datetime_utc','zone','psr_type','psr_name','generation_MW','resolution']
      (plus 'country' when using query_all_countries and 'aggregate_by_country=False')
    """

    def __init__(self, api_key: str, session: Optional[requests.Session] = None):
        self.api_key = api_key
        self.session = session or requests.Session()

    # ---------- internal helpers ----------

    @staticmethod
    def _to_utc_compact(d: dt.datetime) -> str:
        """Format datetime as yyyyMMddHHmm UTC for ENTSO-E."""
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        d = d.astimezone(dt.timezone.utc)
        return d.strftime("%Y%m%d%H%M")

    @staticmethod
    def _ensure_utc(d: dt.datetime) -> dt.datetime:
        if d.tzinfo is None:
            return d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)

    @staticmethod
    def _chunk_datetimes(start_utc: dt.datetime, end_utc: dt.datetime, max_days: int = 365) -> Iterable[Tuple[dt.datetime, dt.datetime]]:
        """Yield [start,end) chunks no longer than max_days to respect ENTSO-E's 1-year limit."""
        cur = start_utc
        while cur < end_utc:
            nxt = min(cur + dt.timedelta(days=max_days), end_utc)
            yield cur, nxt
            cur = nxt

    def _request_with_retries(self, params: dict, max_retries: int = 5, timeout: int = 45) -> str:
        """GET with backoff on 429/5xx; raise on other errors."""
        backoff = 1.0
        for _ in range(max_retries):
            r = self.session.get(BASE_URL, params=params, timeout=timeout)
            if r.status_code == 200 and r.text.strip():
                return r.text
            if r.status_code in (429, 500, 502, 503, 504):
                retry_after = r.headers.get("Retry-After")
                try:
                    sleep_s = float(retry_after) if retry_after else backoff
                except ValueError:
                    sleep_s = backoff
                import time as _t
                _t.sleep(sleep_s)
                backoff = min(backoff * 2, 30)
                continue
            # Surface body errors if any XML is present, then raise
            try:
                ET.fromstring(r.text)
            except Exception:
                pass
            r.raise_for_status()
        raise TimeoutError("ENTSO-E request failed after retries")

    @staticmethod
    def _iso8601_duration_to_minutes(duration: str) -> int:
        """
        Convert ISO 8601 duration like 'PT15M', 'PT1H', 'P1D' to minutes.
        Supports days/hours/minutes (sufficient for ENTSO-E time series).
        """
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

    @staticmethod
    def _parse_a75(xml_text: str, zone: str) -> List[Dict]:
        """
        Parse Actual Generation (A75) XML into a list of dicts.
        """
        ns = {"gl": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
        root = ET.fromstring(xml_text)

        # ENTSO-E sometimes encodes errors inside 200 responses via <Reason>
        for reason in root.findall(".//gl:Reason", ns):
            code = (reason.findtext("gl:code", default="", namespaces=ns) or "").strip()
            text = (reason.findtext("gl:text", default="", namespaces=ns) or "").strip()
            raise RuntimeError(f"ENTSO-E API error: {code} {text}".strip())

        rows: List[Dict] = []
        for ts in root.findall("gl:TimeSeries", ns):
            psr_code = ts.findtext("gl:MktPSRType/gl:psrType", default="", namespaces=ns)
            psr_name = PSRTYPE_MAPPINGS.get(psr_code, psr_code or None)
            ts_resolution = ts.findtext("gl:resolution", default="", namespaces=ns)

            for period in ts.findall("gl:Period", ns):
                start_str = (
                    period.findtext("gl:timeInterval/gl:start", default="", namespaces=ns)
                    or root.findtext("gl:time_Period.timeInterval/gl:start", default="", namespaces=ns)
                )
                if not start_str:
                    continue
                start_dt = dt.datetime.strptime(start_str.replace("Z", "+0000"), "%Y-%m-%dT%H:%M%z")
                resolution = period.findtext("gl:resolution", default=ts_resolution, namespaces=ns) or "PT60M"
                step_minutes = EntsoeGenerationByType._iso8601_duration_to_minutes(resolution) or 60

                for pt in period.findall("gl:Point", ns):
                    pos = int(pt.findtext("gl:position", default="1", namespaces=ns))
                    qty = pt.findtext("gl:quantity", default=None, namespaces=ns)
                    try:
                        val = float(qty) if qty is not None else None
                    except ValueError:
                        val = None
                    ts_dt = start_dt + dt.timedelta(minutes=step_minutes * (pos - 1))
                    rows.append({
                        "datetime_utc": ts_dt.astimezone(dt.timezone.utc).replace(tzinfo=None),
                        "zone": zone,
                        "psr_type": psr_code or None,
                        "psr_name": psr_name,
                        "generation_MW": val,
                        "resolution": resolution,
                    })

        rows.sort(key=lambda r: (r["datetime_utc"], r.get("psr_type") or ""))
        return rows

    def _fetch_chunk(
        self,
        zone_eic: str,
        start_utc: dt.datetime,
        end_utc: dt.datetime,
        psr_type: Optional[str] = None,
    ) -> List[Dict]:
        params = {
            "documentType": "A75",      # Actual generation per type
            "processType": "A16",       # Realised
            "in_Domain": zone_eic,      # generation-side domain
            "periodStart": self._to_utc_compact(start_utc),
            "periodEnd": self._to_utc_compact(end_utc),
            "securityToken": self.api_key,
        }
        if psr_type:
            params["psrType"] = psr_type
        xml_text = self._request_with_retries(params)
        return self._parse_a75(xml_text, zone_eic)

    # ---------- public API ----------

    def get_range(
        self,
        zone_eic: str,
        start: dt.datetime,
        end: dt.datetime,
        psr_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch Actual Generation per Type for a zone in [start, end) (UTC).
        Returns a tidy DataFrame:
          ['datetime_utc','zone','psr_type','psr_name','generation_MW','resolution']
        """
        if start >= end:
            return pd.DataFrame(columns=["datetime_utc","zone","psr_type","psr_name","generation_MW","resolution"])

        start_utc = self._ensure_utc(start)
        end_utc = self._ensure_utc(end)

        frames: List[pd.DataFrame] = []
        for s, e in self._chunk_datetimes(start_utc, end_utc, max_days=365):
            recs = self._fetch_chunk(zone_eic, s, e, psr_type=psr_type)
            if recs:
                frames.append(pd.DataFrame.from_records(recs))

        if not frames:
            return pd.DataFrame(columns=["datetime_utc","zone","psr_type","psr_name","generation_MW","resolution"])

        df = pd.concat(frames, ignore_index=True, sort=False)
        # Deduplicate (can happen on chunk boundaries)
        df = df.drop_duplicates(subset=["datetime_utc","zone","psr_type"]).sort_values(["datetime_utc","psr_type"]).reset_index(drop=True)
        # Keep only requested window (end exclusive)
        df = df[(df["datetime_utc"] >= start_utc.replace(tzinfo=None)) & (df["datetime_utc"] < end_utc.replace(tzinfo=None))]
        return df

    def get_last_hours(
        self,
        zone_eic: str,
        hours: int = 24,
        psr_type: Optional[str] = None,
        now_utc: Optional[dt.datetime] = None,
        align_to_hour: bool = True,
    ) -> pd.DataFrame:
        """
        Convenience: fetch last N hours ending at 'now' (UTC).
        """
        if hours <= 0:
            raise ValueError("hours must be > 0")

        if now_utc is None:
            now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        else:
            now_utc = self._ensure_utc(now_utc)

        period_end = now_utc.replace(minute=0, second=0, microsecond=0) if align_to_hour else now_utc
        period_start = period_end - dt.timedelta(hours=hours)
        return self.get_range(zone_eic, period_start, period_end, psr_type=psr_type)

    @classmethod
    def query_all_countries(
        cls,
        api_key: str,
        country_to_eics: Dict[str, Union[str, List[str]]],
        start: dt.datetime,
        end: dt.datetime,
        psr_type: Optional[str] = None,
        aggregate_by_country: bool = False,
        now_utc: Optional[dt.datetime] = None,  # kept for symmetry with other class
        skip_errors: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch Actual Generation per Type for MANY countries in [start, end).
        Args:
          - country_to_eics: {'CZ': '10YCZ-CEPS-----N', 'DK': ['10YDK-1--------W','10YDK-2--------M'], ...}
          - aggregate_by_country: if True, sums across zones per country×psr×timestamp.
        Returns:
          DataFrame:
            aggregate=False -> ['country','zone','datetime_utc','psr_type','psr_name','generation_MW','resolution']
            aggregate=True  -> ['country','datetime_utc','psr_type','psr_name','generation_MW']
        """
        client = cls(api_key)
        frames: List[pd.DataFrame] = []

        for country, zones in country_to_eics.items():
            zone_list = zones if isinstance(zones, list) else [zones]
            for z in zone_list:
                try:
                    df = client.get_range(z, start=start, end=end, psr_type=psr_type)
                    if df.empty:
                        continue
                    df = df.copy()
                    df["country"] = country
                    frames.append(df)
                except Exception as e:
                    if skip_errors:
                        print(f"[entsoe] Skipping {country}/{z} due to error: {e}")
                        continue
                    raise

        if not frames:
            cols = ["country","zone","datetime_utc","psr_type","psr_name","generation_MW","resolution"]
            return pd.DataFrame(columns=cols if not aggregate_by_country else cols[:1] + cols[2:])

        full = pd.concat(frames, ignore_index=True, sort=False)

        if aggregate_by_country:
            # Sum across zones per country × timestamp × psr
            out = (full.groupby(["country","datetime_utc","psr_type","psr_name"], as_index=False)["generation_MW"]
                        .sum(numeric_only=True)
                        .sort_values(["country","datetime_utc","psr_type"])
                        .reset_index(drop=True))
            return out
        else:
            cols = ["country","zone","datetime_utc","psr_type","psr_name","generation_MW","resolution"]
            keep = [c for c in cols if c in full.columns]
            return (full[keep]
                    .sort_values(["country","zone","datetime_utc","psr_type"])
                    .reset_index(drop=True))

    # ---------- small utility for DRF ----------

    @staticmethod
    def to_records(df: pd.DataFrame, datetime_cols: Optional[List[str]] = None) -> List[Dict]:
        """
        Convert a DataFrame to list-of-dicts, ISO-formatting datetime columns for JSON responses.
        Example in DRF: return Response(EntsoeGenerationByType.to_records(df))
        """
        if df.empty:
            return []
        if datetime_cols is None:
            datetime_cols = [c for c in df.columns if "time" in c or "date" in c]
        df2 = df.copy()
        for c in datetime_cols:
            if c in df2.columns:
                df2[c] = pd.to_datetime(df2[c], utc=True, errors="ignore").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return df2.to_dict(orient="records")


##### ENERGY PRICES #####

class EntsoePrices:
    """
    ENTSO-E Price Document (A44).

    - contract_MarketAgreement.type: A01 (Day-ahead), A07 (Intraday)
    - periodStart/periodEnd: UTC, end exclusive, should land on MTU boundary
    - 1-year window limit, 100-document page limit (use 'offset' to paginate)
    """

    def __init__(self, api_key: str, session: Optional[requests.Session] = None):
        self.api_key = api_key
        self.session = session or requests.Session()

    # ---------- helpers ----------

    @staticmethod
    def _to_utc_compact(d: dt.datetime) -> str:
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        d = d.astimezone(dt.timezone.utc)
        return d.strftime("%Y%m%d%H%M")

    @staticmethod
    def _ensure_utc(d: dt.datetime) -> dt.datetime:
        if d.tzinfo is None:
            return d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)

    def _request_with_retries(self, params: dict, max_retries: int = 5, timeout: int = 45) -> str:
        """GET with retry/backoff; ALWAYS return response text (XML string)."""
        backoff = 1.0
        for _ in range(max_retries):
            r = self.session.get(BASE_URL, params=params, timeout=timeout)
            # success
            if r.status_code == 200 and r.text and r.text.strip():
                return r.text  # return TEXT, not the Response object
            # retryable
            if r.status_code in (429, 500, 502, 503, 504):
                retry_after = r.headers.get("Retry-After")
                try:
                    sleep_s = float(retry_after) if retry_after else backoff
                except ValueError:
                    sleep_s = backoff
                import time as _t
                _t.sleep(sleep_s)
                backoff = min(backoff * 2, 30)
                continue
            # non-retryable
            r.raise_for_status()
        raise TimeoutError("ENTSO-E request failed after retries")

    @staticmethod
    def _iso8601_duration_to_minutes(duration: str) -> int:
        if not duration or not duration.startswith("P"):
            return 0
        days = hours = minutes = 0
        date_part, time_part = duration, ""
        if "T" in duration:
            date_part, time_part = duration.split("T", 1)
        if "D" in date_part:
            d = date_part.replace("P", "").split("D")[0]
            try:
                days = int(d or 0)
            except Exception:
                days = 0
        if "H" in time_part:
            h = time_part.split("H")[0]
            try:
                hours = int(h or 0)
            except Exception:
                hours = 0
            time_part = time_part.split("H", 1)[1] if "H" in time_part else ""
        if "M" in time_part:
            m = time_part.split("M")[0]
            try:
                minutes = int(m or 0)
            except Exception:
                minutes = 0
        return days * 1440 + hours * 60 + minutes

    # ---------- parsing ----------

    def _parse_a44(self, xml_text: str, zone_eic: str) -> List[Dict]:
        """
        Parse ENTSO-E A44 (Price Document) XML into a list of dict rows.

        Returns rows with keys:
          - datetime_utc (aware UTC datetime)
          - zone (the EIC queried)
          - price (float)
          - currency (e.g., 'EUR')
          - unit (e.g., 'MWH')
          - resolution (ISO 8601 duration, e.g., 'PT15M', 'PT60M')
          - contract_type (e.g., 'A01' day-ahead, 'A07' intraday)
        """
        ns = {"pub": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}

        # Normalize accidental Response/bytes
        if hasattr(xml_text, "text"):
            xml_text = xml_text.text
        if isinstance(xml_text, bytes):
            xml_text = xml_text.decode("utf-8", errors="replace")

        root = ET.fromstring(xml_text)

        # Surface API-side XML errors
        for reason in root.findall(".//pub:Reason", ns):
            code = (reason.findtext("pub:code", default="", namespaces=ns) or "").strip()
            text = (reason.findtext("pub:text", default="", namespaces=ns) or "").strip()
            raise RuntimeError(f"ENTSO-E API error: {code} {text}".strip())

        # Document-level fallbacks
        contract_type = (root.findtext("pub:contract_MarketAgreement.type", default="", namespaces=ns) or "").strip()
        currency = (root.findtext("pub:currency_Unit.name", default="", namespaces=ns) or "").strip()
        unit = (root.findtext("pub:price_Measure_Unit.name", default="", namespaces=ns) or "").strip()

        rows: List[Dict] = []

        for ts in root.findall("pub:TimeSeries", ns):
            ts_contract = (ts.findtext("pub:contract_MarketAgreement.type", default=contract_type, namespaces=ns) or "").strip()
            ts_currency = (ts.findtext("pub:currency_Unit.name", default=currency, namespaces=ns) or "").strip()
            ts_unit = (ts.findtext("pub:price_Measure_Unit.name", default=unit, namespaces=ns) or "").strip()

            for period in ts.findall("pub:Period", ns):
                start_str = period.findtext("pub:timeInterval/pub:start", default="", namespaces=ns)
                if not start_str:
                    start_str = root.findtext("pub:period.timeInterval/pub:start", default="", namespaces=ns)
                if not start_str:
                    continue

                try:
                    start_dt = dt.datetime.strptime(start_str.replace("Z", "+0000"), "%Y-%m-%dT%H:%M%z")
                except ValueError:
                    start_dt = dt.datetime.fromisoformat(start_str.replace("Z", "+00:00"))

                resolution = period.findtext("pub:resolution", default="", namespaces=ns) or ""
                step_minutes = self._iso8601_duration_to_minutes(resolution) or 60

                for pt in period.findall("pub:Point", ns):
                    pos_text = pt.findtext("pub:position", default="1", namespaces=ns)
                    try:
                        pos = int(pos_text)
                    except Exception:
                        pos = 1

                    price_text = pt.findtext("pub:price.amount", default=None, namespaces=ns)
                    try:
                        price_val = float(price_text) if price_text is not None else None
                    except Exception:
                        price_val = None

                    ts_dt = start_dt + dt.timedelta(minutes=step_minutes * (pos - 1))
                    ts_dt_utc = ts_dt.astimezone(dt.timezone.utc)

                    rows.append({
                        "datetime_utc": ts_dt_utc,
                        "zone": zone_eic,
                        "price": price_val,
                        "currency": ts_currency or None,
                        "unit": ts_unit or None,
                        "resolution": resolution or "PT60M",
                        "contract_type": ts_contract or None,
                    })

        rows.sort(key=lambda r: (r["datetime_utc"], r.get("zone") or ""))
        return rows

    # ---------- public API ----------

    def get_prices_range(
        self,
        zone_eic: str,
        start: dt.datetime,
        end: dt.datetime,
        contract_type: str = "A01",
        paginate: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch A44 prices for a single bidding zone (EIC) in [start, end).
        Returns columns: ['datetime_utc','zone','price','currency','unit','resolution','contract_type'].
        """
        start_utc = self._ensure_utc(start)
        end_utc = self._ensure_utc(end)

        base_params = {
            "documentType": "A44",
            "in_Domain": zone_eic,
            "out_Domain": zone_eic,
            "periodStart": self._to_utc_compact(start_utc),
            "periodEnd": self._to_utc_compact(end_utc),
            "securityToken": self.api_key,
            "contract_MarketAgreement.type": contract_type,
        }

        frames: List[pd.DataFrame] = []
        offset = 0
        while True:
            params = dict(base_params)
            if paginate:
                params["offset"] = offset
            xml_text = self._request_with_retries(params)
            records = self._parse_a44(xml_text, zone_eic)
            if not records:
                break
            frames.append(pd.DataFrame.from_records(records))
            if not paginate or len(records) < 100:
                break
            offset += 100

        if not frames:
            return pd.DataFrame(columns=["datetime_utc", "zone", "price", "currency", "unit", "resolution", "contract_type"])

        df = pd.concat(frames, ignore_index=True, sort=False)
        # Ensure aware UTC dtype & clip to window
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
        df = df[(df["datetime_utc"] >= pd.Timestamp(start_utc)) &
                (df["datetime_utc"] < pd.Timestamp(end_utc))]

        # Defensive de-dup per zone×contract×exact ts (keep last)
        df = (df.sort_values(["datetime_utc", "zone"])
                .drop_duplicates(subset=["datetime_utc", "zone", "contract_type"], keep="last")
                .reset_index(drop=True))
        return df

    @classmethod
    def query_all_countries(
        cls,
        api_key: str,
        country_to_eics: Dict[str, Union[str, List[str]]],
        start: dt.datetime,
        end: dt.datetime,
        contract_type: str = "A01",
        aggregate_by_country: bool = True,
        skip_errors: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch A44 prices for MANY countries in [start, end).
        If aggregate_by_country=True: averages across zones per timestamp (simple mean).
        """
        client = cls(api_key)
        frames: List[pd.DataFrame] = []

        for country, zones in country_to_eics.items():
            zone_list = zones if isinstance(zones, list) else [zones]
            for z in zone_list:
                try:
                    df = client.get_prices_range(z, start, end, contract_type=contract_type)
                    if df.empty:
                        continue
                    df = df.copy()
                    df["country"] = country
                    frames.append(df)
                except Exception as e:
                    if skip_errors:
                        print(f"[entsoe] A44 skip {country}/{z}: {e}")
                        continue
                    raise

        if not frames:
            if aggregate_by_country:
                return pd.DataFrame(columns=["country", "datetime_utc", "price", "currency", "unit", "contract_type"])
            else:
                return pd.DataFrame(columns=["country", "zone", "datetime_utc", "price", "currency", "unit", "resolution", "contract_type"])

        full = pd.concat(frames, ignore_index=True, sort=False)

        if aggregate_by_country:
            # average across zones; currency/unit assumed homogeneous (EUR/MWH)
            out = (full.groupby(["country", "datetime_utc", "contract_type"], as_index=False)
                        .agg(price=("price", "mean"),
                             currency=("currency", "first"),
                             unit=("unit", "first")))
            return out.sort_values(["country", "datetime_utc"]).reset_index(drop=True)
        else:
            cols = ["country", "zone", "datetime_utc", "price", "currency", "unit", "resolution", "contract_type"]
            keep = [c for c in cols if c in full.columns]
            return full[keep].sort_values(["country", "zone", "datetime_utc"]).reset_index(drop=True)

    # ---------- small utility for DRF ----------

    @staticmethod
    def to_records(df: pd.DataFrame, datetime_cols: Optional[List[str]] = None) -> List[Dict]:
        if df.empty:
            return []
        if datetime_cols is None:
            datetime_cols = [c for c in df.columns if "time" in c or "date" in c]
        df2 = df.copy()
        for c in datetime_cols:
            if c in df2.columns:
                df2[c] = pd.to_datetime(df2[c], utc=True, errors="ignore").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return df2.to_dict(orient="records")