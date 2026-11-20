"""One-off, bounded-concurrency recovery of the reported November 12-13 gaps."""
import concurrent.futures
import datetime as dt
import io
import itertools
import json
import os
import sys
import warnings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "data_api.settings")
sys.path.insert(0, os.getcwd())
import django
django.setup()

import pandas as pd
from django.conf import settings
from django.core.management import call_command
from django.db import connections, close_old_connections
from entsoe_api.entsoe_data import EntsoeGenerationByType, EntsoePrices, EntsoePhysicalFlows
from entsoe_api.helper import save_generation_df, save_country_prices_df, save_flows_df
from entsoe_api.management.commands.fetch_flows import DEFAULT_NEIGHBORS_BY_COUNTRY, _iter_eics

warnings.filterwarnings("ignore", category=RuntimeWarning, module="django.db.models.fields")
START = dt.datetime(2026, 11, 12, tzinfo=dt.timezone.utc)
END = dt.datetime(2026, 11, 14, tzinfo=dt.timezone.utc)
COUNTRIES = ["BG", "DE", "FR", "RO", "GR"]


def recover(dataset, key):
    close_old_connections()
    try:
        if dataset in ("generation", "prices"):
            mapping = settings.ENTSOE_COUNTRY_TO_EICS if dataset == "generation" else settings.ENTSOE_PRICE_COUNTRY_TO_EICS
            client = EntsoeGenerationByType if dataset == "generation" else EntsoePrices
            df = client.query_all_countries(
                api_key=os.environ["ENTSOE_TOKEN"], country_to_eics={key: mapping[key]},
                start=START, end=END, skip_errors=False, aggregate_by_country=True,
            )
            if not df.empty:
                df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
            saved = (save_generation_df if dataset == "generation" else save_country_prices_df)(df)
        elif dataset == "flows":
            df = EntsoePhysicalFlows(os.environ["ENTSOE_TOKEN"]).get_range(key[0], key[1], START, END)
            saved = save_flows_df(df)
        else:
            output = io.StringIO()
            call_command("fetch_" + key + "_tr", start="2026-11-12", end="2026-11-13", stdout=output, stderr=output)
            print(json.dumps({"dataset": dataset, "key": key, "status": "completed", "summary": output.getvalue().splitlines()[-2:]}), flush=True)
            return True
        print(json.dumps({"dataset": dataset, "key": key, "status": "saved" if saved else "no_data", "rows": saved,
                          "last_utc": str(df["datetime_utc"].max()) if not df.empty else None}), flush=True)
        return True
    except Exception as error:
        response = getattr(error, "response", None)
        code = response.status_code if response is not None else None
        print(json.dumps({"dataset": dataset, "key": key, "status": "failed", "error_type": type(error).__name__, "http_status": code}), flush=True)
        # A border without upstream coverage is expected; other failures require review.
        return dataset == "flows" and code == 400
    finally:
        connections.close_all()


if __name__ == "__main__":
    jobs = [(kind, country) for country in COUNTRIES for kind in ["prices", "generation"]]
    jobs = [("turkey", "prices"), ("turkey", "generation")] + jobs
    mapping = settings.ENTSOE_COUNTRY_TO_EICS
    neighbors = getattr(settings, "ENTSOE_NEIGHBORS_BY_COUNTRY", DEFAULT_NEIGHBORS_BY_COUNTRY)
    pairs = set()
    for country in COUNTRIES:
        for neighbor in neighbors.get(country, set()):
            if neighbor in mapping:
                for a, b in itertools.product(_iter_eics(mapping[country]), _iter_eics(mapping[neighbor])):
                    pairs.update([(a, b), (b, a)])
    jobs += [("flows", pair) for pair in sorted(pairs)]
    if len(sys.argv) > 1 and sys.argv[1] == "generation-de":
        jobs = [("generation", "DE")]
    print(json.dumps({"start": START.isoformat(), "end": END.isoformat(), "jobs": len(jobs), "flow_pairs": len(pairs)}), flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda args: recover(*args), jobs))
    print(json.dumps({"finished": True, "jobs": len(jobs), "failed": results.count(False)}), flush=True)
    raise SystemExit(1 if not all(results) else 0)
