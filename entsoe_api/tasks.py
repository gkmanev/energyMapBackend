from celery import shared_task
from django.conf import settings
from django.core.management import call_command
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo  # Python 3.12+
import logging
import os
from uuid import uuid4
from redis import Redis

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
def fetch_generation_res_daily_task(self):
    tz = ZoneInfo("Europe/Sofia")
    start_utc, end_utc = _local_daily_window(tz)
    logger.info("Daily RES generation window: %s -> %s", start_utc, end_utc)
    call_command("generation_res", all=True, start=start_utc, end=end_utc)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_generation_forecast_hourly_task(self):
    """Fetch rolling generation forecasts every hour."""
    start_iso, end_iso = _local_daily_window(ZoneInfo("Europe/Sofia"))
    logger.info("Hourly forecast window: %s -> %s", start_iso, end_iso)
    call_command("fetch_generation_forecast", start=start_iso, end=end_iso)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_generation_hourly_task(self):
    """Fetch a rolling 48h window, including late previous-day generation."""
    start_iso, end_iso = _hourly_window(hours_back=48)
    logger.info("Hourly generation window: %s -> %s", start_iso, end_iso)
    call_command("fetch_generation", start=start_iso, end=end_iso)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_prices_hourly_task(self):
    """Fetch day-ahead price data for the current local day each hour."""
    start_iso, end_iso = _local_daily_window(ZoneInfo("Europe/Sofia"))
    logger.info("Hourly prices window: %s -> %s", start_iso, end_iso)
    call_command("fetch_prices", start=start_iso, end=end_iso)

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_prices_tr_task(self):
    """Fetch Turkish (EPİAŞ) day-ahead prices for today and tomorrow (Turkish time).

    Tomorrow is published ~14:00 TR time; before that the dayahead call just
    reports no data and saves nothing.
    """
    logger.info("Fetching TR prices: today + dayahead")
    call_command("fetch_prices_tr", period="today")
    call_command("fetch_prices_tr", period="dayahead")


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_generation_tr_hourly_task(self):
    """Fetch Turkish (EPİAŞ) real-time generation for yesterday + today (Turkish time).

    Including yesterday fills late-arriving hours around midnight.
    """
    today_tr = datetime.now(ZoneInfo("Europe/Istanbul")).date()
    start, end = (today_tr - timedelta(days=1)).isoformat(), today_tr.isoformat()
    logger.info("TR generation window: %s -> %s", start, end)
    call_command("fetch_generation_tr", start=start, end=end)


def _flow_redis():
    return Redis.from_url(settings.CELERY_BROKER_URL, socket_timeout=10,
                          socket_connect_timeout=10, decode_responses=True)


def _flow_key(source, destination):
    return f"entsoe:flows:pending:{source}:{destination}"


def _release_flow(redis, key, token):
    # Do not remove a newer reservation if this job outlived its own lease.
    redis.eval("if redis.call('get', KEYS[1]) == ARGV[1] then "
               "return redis.call('del', KEYS[1]) else return 0 end", 1, key, token)


@shared_task(queue="flows_dispatch", expires=240)
def fetch_flows_hourly_task():
    """Queue each configured European border once, including multi-zone areas.

    Keep the existing task name so previously queued refreshes use this dispatcher
    after deployment. Reservations cover both queued and running work, preventing
    a slow sweep from creating a fresh backlog on every scheduler tick.
    """
    from entsoe_api.management.commands.fetch_flows import (
        _load_country_to_eics_from_settings, build_flow_pairs,
    )
    redis = _flow_redis()
    queued = 0
    try:
        for source, destination in build_flow_pairs(_load_country_to_eics_from_settings()):
            key, token = _flow_key(source, destination), uuid4().hex
            if not redis.set(key, token, nx=True, ex=14400):
                continue
            try:
                fetch_flow_border_task.apply_async(args=[source, destination, token])
            except Exception:
                _release_flow(redis, key, token)
                raise
            queued += 1
        logger.info("Queued %s independent European flow border refreshes", queued)
    finally:
        redis.close()


@shared_task(queue="flows", soft_time_limit=60, time_limit=90,
             rate_limit="120/m", acks_late=True, reject_on_worker_lost=True)
def fetch_flow_border_task(source, destination, token):
    """Fetch and save one directed border without waiting for other countries."""
    from entsoe_api.entsoe_data import EntsoePhysicalFlows
    from entsoe_api.helper import save_flows_df

    redis = _flow_redis()
    key = _flow_key(source, destination)
    lock = redis.lock(key + ":running", timeout=120, blocking=False)
    try:
        if not lock.acquire(blocking=False):
            return
        try:
            if redis.get(key) != token:
                return
            try:
                # Calculate at execution time so time spent queued cannot make
                # this refresh request an old window. Revisit 48h for revisions.
                end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                client = EntsoePhysicalFlows(os.environ["ENTSOE_TOKEN"],
                                             request_timeout=20, max_retries=2)
                try:
                    frame = client.get_range(source, destination, end - timedelta(hours=48), end)
                finally:
                    client.session.close()
                written = save_flows_df(frame)
                logger.info("Saved %s flow rows for %s -> %s", written, source, destination)
            except Exception as exc:
                # HTTP exceptions contain request URLs with the API token.
                logger.warning("Flow refresh failed for %s -> %s (%s)",
                               source, destination, type(exc).__name__)
                raise RuntimeError(f"Flow refresh failed for {source} -> {destination}: "
                                   f"{type(exc).__name__}") from None
            finally:
                _release_flow(redis, key, token)
        finally:
            lock.release()
    finally:
        redis.close()


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def fetch_generation_eso_bg_quarter_hourly_task(self):
    """Fetch BG generation snapshots from the ESO BG endpoint every 15 minutes."""
    logger.info("Running BG ESO generation fetch task")
    call_command("fetch_generation_eso_bg")
