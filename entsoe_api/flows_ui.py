"""Experimental reader for the Transparency Platform's web UI JSON endpoint."""
import datetime as dt
import math
from zoneinfo import ZoneInfo

import requests

URL = "https://transparency.entsoe.eu/transmission/physicalFlows/load"
UI_TIMEZONE = ZoneInfo("Europe/Brussels")
FLOW_COLUMNS = ("EXPORT_FLOW", "NET_FLOW", "NETTED_EXPORT_FLOW")
STEPS = {"PT15M": 15, "PT30M": 30, "PT60M": 60}


def day_request(day, areas):
    start = dt.datetime.combine(day, dt.time.min, tzinfo=UI_TIMEZONE)
    end = start + dt.timedelta(days=1)

    def utc(value):
        return value.astimezone(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    return {"dateTimeRange": {"from": utc(start), "to": utc(end)},
            "areaList": list(dict.fromkeys(areas)), "timeZone": "CET",
            "sorterList": [], "filterMap": {}}


def fetch_day(day, areas, timeout=30):
    # A fresh request intentionally uses no authentication, token or stored cookies.
    response = requests.post(URL, json=day_request(day, areas),
                             headers={"Accept": "application/json"}, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or not isinstance(data.get("metaData"), list) or not isinstance(data.get("instanceList"), list):
        raise ValueError("Unexpected ENTSO-E web UI response: missing metaData or instanceList.")
    errors = data.get("uuAppErrorMap", {})
    if any(item.get("type") == "error" for item in errors.values() if isinstance(item, dict)):
        raise ValueError("ENTSO-E web UI reported errors: " + ", ".join(errors))
    total = data.get("intervalPageInfo", {}).get("total")
    if total is not None and total > len(data["instanceList"]):
        raise ValueError("ENTSO-E returned a partial page; request fewer areas.")
    return data


def _timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("ENTSO-E returned a timestamp without a timezone.")
    return parsed.astimezone(dt.timezone.utc)


def _number(value):
    if value is None or isinstance(value, dict) or value in ("", "-", "N/A"):
        return None
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Non-finite flow quantity in ENTSO-E UI response.")
    return number


def to_rows(data):
    """Decode zero-based A01 point positions using the response's column order.

    Preserve raw directional exports separately from signed and netted quantities;
    these are different measurements and must not be mixed during aggregation.
    """
    columns = [item["code"] for item in data["metaData"]]
    if len(columns) != len(set(columns)) or "EXPORT_FLOW" not in columns:
        raise ValueError("Unexpected ENTSO-E flow columns.")
    for instance in data["instanceList"]:
        dimensions = instance["businessDimensionMap"]
        curve = instance["curveData"]
        if curve.get("curveType", "A01") != "A01":
            raise ValueError("Unsupported web UI curve type: " + str(curve["curveType"]))
        for period in curve["periodList"]:
            resolution = period["resolution"]
            if resolution not in STEPS:
                raise ValueError("Unsupported flow resolution: " + str(resolution))
            start = _timestamp(period["timeInterval"]["from"])
            end = _timestamp(period["timeInterval"]["to"])
            for index, values in sorted(period["pointMap"].items(), key=lambda item: int(item[0])):
                position = int(index)
                timestamp = start + dt.timedelta(minutes=STEPS[resolution] * position)
                if position < 0 or timestamp >= end:
                    raise ValueError("Flow point position is outside its period.")
                if not isinstance(values, list) or len(values) != len(columns):
                    raise ValueError("Flow point values do not match metaData columns.")
                row = {"from_area": dimensions["OUT_AREA"], "to_area": dimensions["IN_AREA"],
                       "utc_start": timestamp.isoformat().replace("+00:00", "Z"), "resolution": resolution}
                row.update(zip(columns, values))
                for column in FLOW_COLUMNS:
                    if column in row:
                        row[column] = _number(row[column])
                yield row


def to_flow_records(rows):
    """Convert published directional exports to the existing PhysicalFlow schema.

    Missing publications must never overwrite stored readings. Validate the full
    batch before saving, including conflicting rows sharing the database key.
    """
    records = {}
    for row in rows:
        quantity = _number(row["EXPORT_FLOW"])
        if quantity is None:
            continue
        if quantity < 0:
            raise ValueError("EXPORT_FLOW must be non-negative; NET_FLOW cannot be used as a directional export.")

        def eic(area):
            kind, separator, code = area.partition("|")
            if not separator or kind not in {"BZN", "CTA", "CTY"} or not code or len(code) > 32:
                raise ValueError("Invalid UI area identifier: " + str(area))
            return code

        record = {"datetime_utc": _timestamp(row["utc_start"]),
                  "out_domain_eic": eic(row["from_area"]),
                  "in_domain_eic": eic(row["to_area"]),
                  "quantity_mw": quantity, "resolution": row["resolution"]}
        key = (record["datetime_utc"], record["out_domain_eic"], record["in_domain_eic"])
        if key in records and records[key] != record:
            raise ValueError("Conflicting UI flow values for the same timestamp and EIC pair.")
        records[key] = record
    return list(records.values())
