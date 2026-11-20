"""Verify recovered data through the public production API."""
import concurrent.futures
import json
import requests


def check(dataset, country=None):
    params = {"start": "2026-11-13T00:00:00Z", "end": "2026-11-14T00:00:00Z"}
    if dataset == "flows":
        path = "flows/latest/"
        params = {"country": country}
    else:
        path = dataset + "/bulk-range/"
        params["countries"] = "BG,DE,FR,RO,GR,TR"
    response = requests.get("https://api.visualize.energy/api/" + path, params=params, timeout=45)
    response.raise_for_status()
    payload = response.json()
    if dataset == "flows":
        print(json.dumps({"api": dataset, "country": country, "count": payload.get("count"), "latest": payload.get("start_utc")}), flush=True)
    else:
        for code, data in payload["data"].items():
            items = data["items"]
            print(json.dumps({"api": dataset, "country": code, "rows_nov13": len(items), "latest": max((row["datetime_utc"] for row in items), default=None)}), flush=True)


with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    jobs = [pool.submit(check, kind) for kind in ["generation", "prices"]]
    jobs += [pool.submit(check, "flows", country) for country in ["BG", "DE", "FR", "RO", "GR"]]
    for job in jobs:
        job.result()
