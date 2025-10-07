from celery import shared_task
from django.core.management import call_command
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo  # Python 3.12+


@shared_task
def fetch_installed_capacity_task():
    # This runs: python manage.py fetch_installed_capacity
    call_command("fetch_installed_capacity")


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_prices_daily_task(self):
    tz = ZoneInfo("Europe/Sofia")
    now_local = datetime.now(tz)

    start_local = datetime.combine(now_local.date() - timedelta(days=1), time(0, 0), tzinfo=tz)
    end_local   = datetime.combine(now_local.date() + timedelta(days=1), time(0, 0), tzinfo=tz)

    start_utc = start_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc   = end_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # If your management command options are named --start/--end, call_command takes them as kwargs:
    call_command("fetch_prices", start=start_utc, end=end_utc)

