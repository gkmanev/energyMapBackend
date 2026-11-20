"""Inspect upstream curve metadata without logging tokens or request URLs."""
import concurrent.futures
import datetime as dt
import json
import os
import xml.etree.ElementTree as ET

from dotenv import load_dotenv
import requests

load_dotenv()
BG = "10YCA-BULGARIA-R"
neighbors = {"GR": "10YGR-HTSO-----Y", "MK": "10YMK-MEPSO----8",
             "RO": "10YRO-TEL------P", "RS": "10YCS-SERBIATSOV", "TR": "10YTR-TEIAS----W"}


def inspect(pair):
    label, source, destination = pair
    try:
        response = requests.get("https://web-api.tp.entsoe.eu/api", params={
            "securityToken": os.environ["ENTSOE_TOKEN"], "documentType": "A11",
            "out_Domain": source, "in_Domain": destination,
            "periodStart": "202611180000", "periodEnd": "202611201200",
        }, timeout=45)
        if response.status_code != 200:
            return {"pair": label, "http_status": response.status_code}
        root = ET.fromstring(response.text)
        def local(node):
            return node.tag.split("}")[-1]
        series = []
        for ts in root.iter():
            if local(ts) != "TimeSeries":
                continue
            curves = [n.text for n in ts if local(n) == "curveType"]
            for period in ts:
                if local(period) != "Period":
                    continue
                info = {"curve": curves}
                for node in period.iter():
                    if local(node) in {"start", "end", "resolution"}:
                        info[local(node)] = node.text
                points = [n for n in period if local(n) == "Point"]
                info["points"] = len(points)
                info["last_points"] = [{local(n): n.text for n in point} for point in points[-2:]]
                series.append(info)
        return {"pair": label, "periods": series}
    except Exception as exc:
        return {"pair": label, "error_type": type(exc).__name__}


pairs = [(f"BG->{name}", BG, eic) for name, eic in neighbors.items()]
pairs += [(f"{name}->BG", eic, BG) for name, eic in neighbors.items()]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    for result in pool.map(inspect, pairs):
        print(json.dumps(result), flush=True)
