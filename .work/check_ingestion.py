"""Read-only checks of public coverage and upstream availability; never log credentials."""
import concurrent.futures
import datetime as dt
import json
import os
import xml.etree.ElementTree as ET

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "data_api.settings")
import django
django.setup()
import requests
from django.conf import settings
from entsoe_api import epias


def check_api(dataset):
    params = {"countries": "BG,DE,FR,RO,GR,TR", "start": "2026-11-12T00:00:00Z", "end": "2026-11-14T00:00:00Z"}
    if dataset == "flows":
        path = "flows/latest/"
        params = {"country": "BG"}
    else:
        path = f"{dataset}/bulk-range/"
    try:
        response = requests.get("https://api.visualize.energy/api/" + path, params=params, timeout=35)
        payload = response.json()
        print(json.dumps({"api": dataset, "status": response.status_code, "payload": summarize(payload)}, default=str), flush=True)
    except Exception as error:
        print(json.dumps({"api": dataset, "error_type": type(error).__name__}), flush=True)


def summarize(value):
    if isinstance(value, list):
        stamps = [str(row.get("datetime_utc", row.get("datetime", ""))) for row in value if isinstance(row, dict)]
        return {"rows": len(value), "first": min(stamps, default=None), "last": max(stamps, default=None), "sample": value[:1]}
    if isinstance(value, dict):
        return {key: summarize(item) for key, item in value.items()}
    return value


def check_entsoe(kind, country="BG"):
    mapping = settings.ENTSOE_PRICE_COUNTRY_TO_EICS if kind == "prices" else settings.ENTSOE_COUNTRY_TO_EICS
    zone = mapping[country]
    if isinstance(zone, list):
        zone = zone[0]
    params = {"securityToken": os.environ["ENTSOE_TOKEN"], "periodStart": "202611122200", "periodEnd": "202611132200"}
    if kind == "generation":
        params.update(documentType="A75", processType="A16", in_Domain=zone)
    elif kind == "flows":
        params.update(documentType="A11", out_Domain=zone, in_Domain="10YRO-TEL------P")
    else:
        params.update(documentType="A44", in_Domain=zone, out_Domain=zone)
        params["contract_MarketAgreement.type"] = "A01"
    try:
        response = requests.get("https://web-api.tp.entsoe.eu/api", params=params, timeout=40)
        root = ET.fromstring(response.text)
        reasons = [node.text for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "text"]
        points = sum(node.tag.rsplit("}", 1)[-1] == "Point" for node in root.iter())
        starts = [node.text for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "start"]
        ends = [node.text for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "end"]
        print(json.dumps({"upstream": "ENTSOE", "dataset": kind, "country": country, "status": response.status_code, "points": points, "first_period": min(starts, default=None), "last_period_end": max(ends, default=None), "reasons": reasons}), flush=True)
    except Exception as error:
        print(json.dumps({"upstream": "ENTSOE", "dataset": kind, "error_type": type(error).__name__}), flush=True)


def check_turkey():
    try:
        username, password = epias.get_credentials()
        ticket = epias.get_tgt(username, password)
        print(json.dumps({"upstream": "EPIAS", "login": "ok"}), flush=True)
        for path in ["/v1/markets/dam/data/mcp", "/v1/generation/data/realtime-generation"]:
            response = requests.post(epias.BASE_URL + path, json=epias.date_range_body(dt.date(2026, 11, 12), dt.date(2026, 11, 13)), headers={"TGT": ticket, "Accept": "application/json"}, timeout=40)
            data = response.json()
            print(json.dumps({"upstream": "EPIAS", "path": path, "status": response.status_code, "keys": list(data), "items": len(data.get("items") or []), "body_keys": list(data.get("body") or {}) if isinstance(data.get("body"), dict) else None}), flush=True)
    except Exception as error:
        print(json.dumps({"upstream": "EPIAS", "error_type": type(error).__name__}), flush=True)


if __name__ == "__main__":
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        jobs = [pool.submit(check_api, item) for item in ["generation", "prices", "flows"]]
        jobs += [pool.submit(check_entsoe, item, country) for item in ["generation", "prices"] for country in ["BG", "DE", "FR", "RO", "GR"]]
        jobs += [pool.submit(check_entsoe, "flows")]
        jobs += [pool.submit(check_turkey)]
        for job in jobs:
            job.result()
