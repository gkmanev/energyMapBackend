from celery import shared_task
from django.core.management import call_command
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo  # Python 3.12+
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _format_iso(dt_obj: datetime) -> str:
    return dt_obj.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _local_daily_window(tz: ZoneInfo) -> tuple[str, str]:
    """Return (start, end) ISO strings for the current local day in UTC."""
    today_local = datetime.now(tz).date()
    start_local = datetime.combine(today_local, time(0, 0), tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return _format_iso(start_local), _format_iso(end_local)


def _hourly_window(hours_back: int, hours_forward: int = 1) -> tuple[str, str]:
    """Return (start, end) ISO strings aligned to the current hour in UTC."""
    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end = now_utc + timedelta(hours=hours_forward)
    start = end - timedelta(hours=hours_back)
    return _format_iso(start), _format_iso(end)


@shared_task
def fetch_installed_capacity_task():
    # This runs: python manage.py fetch_installed_capacity
    call_command("fetch_installed_capacity")


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_prices_daily_task(self):
    tz = ZoneInfo("Europe/Sofia")
    start_utc, end_utc = _local_daily_window(tz)
    logger.info("Daily prices window: %s -> %s", start_utc, end_utc)

    # If your management command options are named --start/--end, call_command takes them as kwargs:
    call_command("fetch_prices", start=start_utc, end=end_utc)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_generation_daily_task(self):
    tz = ZoneInfo("Europe/Sofia")
    start_utc, end_utc = _local_daily_window(tz)
    logger.info("Daily generation window: %s -> %s", start_utc, end_utc)

    # If your management command options are named --start/--end, call_command takes them as kwargs:
    call_command("fetch_generation", start=start_utc, end=end_utc)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_generation_forecast_hourly_task(self):
    """Fetch rolling generation forecasts every hour."""
    start_iso, end_iso = _local_daily_window(ZoneInfo("Europe/Sofia"))
    logger.info("Hourly forecast window: %s -> %s", start_iso, end_iso)
    call_command("fetch_generation_forecast", start=start_iso, end=end_iso)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_generation_hourly_task(self):
    """Fetch actual generation using a 24h sliding window each hour."""
    start_iso, end_iso = _local_daily_window(ZoneInfo("Europe/Sofia"))
    logger.info("Hourly generation window: %s -> %s", start_iso, end_iso)
    call_command("fetch_generation", start=start_iso, end=end_iso)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_prices_hourly_task(self):
    """Fetch day-ahead price data for the current local day each hour."""
    start_iso, end_iso = _local_daily_window(ZoneInfo("Europe/Sofia"))
    logger.info("Hourly prices window: %s -> %s", start_iso, end_iso)
    call_command("fetch_prices", start=start_iso, end=end_iso)



@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_flows_hourly_task(self):
    """Fetch flows for all countries."""
    logger.info("Hourly flows window: Last 48")
    call_command("fetch_flows", all_eu=True, hours=48)