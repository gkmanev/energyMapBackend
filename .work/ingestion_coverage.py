import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.getcwd())
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "data_api.settings")
import django
django.setup()
from django.db.models import Count, Max, Q
from entsoe_api.models import CountryGenerationByType, CountryPricePoint, PhysicalFlow

start = dt.datetime(2026, 11, 12, tzinfo=dt.timezone.utc)
split = start + dt.timedelta(days=1)
for label, model in [("generation", CountryGenerationByType), ("prices", CountryPricePoint)]:
    rows = list(model.objects.filter(country_id__in=["BG", "DE", "FR", "RO", "GR", "TR"], datetime_utc__gte=start).values("country_id").annotate(
        rows=Count("pk"), last=Max("datetime_utc"), rows_nov13=Count("pk", filter=Q(datetime_utc__gte=split)),
    ).order_by("country_id"))
    print(json.dumps({"dataset": label, "coverage": rows}, default=str), flush=True)
for country in ["BG", "DE", "FR", "RO", "GR"]:
    rows = PhysicalFlow.objects.filter(Q(country_from_id=country) | Q(country_to_id=country), datetime_utc__gte=start)
    print(json.dumps({"dataset": "flows", "country": country, **rows.aggregate(rows=Count("pk"), last=Max("datetime_utc"), rows_nov13=Count("pk", filter=Q(datetime_utc__gte=split)))}, default=str), flush=True)
