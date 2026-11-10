"""Shared client helpers for the EPİAŞ Transparency Platform (Turkey).

Docs: https://seffaflik.epias.com.tr/electricity-service/technical/en/index.html
"""
from __future__ import annotations

import datetime as dt
import os
import time
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import requests
from django.conf import settings
from django.core.management.base import CommandError

TGT_URL = "https://giris.epias.com.tr/cas/v1/tickets"
BASE_URL = "https://seffaflik.epias.com.tr/electricity-service"
TR_TZ = ZoneInfo("Europe/Istanbul")


def get_credentials() -> tuple[str, str]:
    username = getattr(settings, "EPIAS_USERNAME", "") or os.getenv("EPIAS_USERNAME")
    password = getattr(settings, "EPIAS_PASSWORD", "") or os.getenv("EPIAS_PASSWORD")
    if not username or not password:
        raise CommandError("Missing EPIAS_USERNAME / EPIAS_PASSWORD (settings or .env).")
    return username, password


def get_tgt(username: str, password: str) -> str:
    """Log in and return a TGT (valid ~2 hours)."""
    try:
        r = requests.post(
            TGT_URL,
            data={"username": username, "password": password},
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "text/plain"},
            timeout=30,
        )
        r.raise_for_status()  # expect 201 Created
    except requests.RequestException as e:
        raise CommandError(f"EPİAŞ login failed: {e}")
    return r.text.strip()


def post(tgt: str, path: str, body: dict) -> dict:
    """POST to an electricity-service endpoint, e.g. path='/v1/markets/dam/data/mcp'."""
    r = requests.post(
        BASE_URL + path,
        json=body,
        headers={"TGT": tgt, "Content-Type": "application/json", "Accept": "application/json"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def post_with_retry(
    tgt: str, path: str, body: dict, max_retries: int = 3, log: Optional[Callable[[str], None]] = None
) -> dict:
    for attempt in range(max_retries):
        try:
            return post(tgt, path, body)
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise CommandError(f"EPİAŞ request {path} failed: {e}")
            wait = 2 ** attempt
            if log:
                log(f"  ⚠️  Attempt {attempt + 1}/{max_retries} failed: {str(e)[:100]} — retrying in {wait}s")
            time.sleep(wait)
    return {}


def date_range_body(start: dt.date, end: dt.date) -> dict:
    """Request body for Turkish calendar dates [start, end] (inclusive)."""
    return {
        "startDate": f"{start.isoformat()}T00:00:00+03:00",
        "endDate": f"{end.isoformat()}T00:00:00+03:00",
    }


def compute_dates(period: str | None, start_s: str | None, end_s: str | None) -> tuple[dt.date, dt.date]:
    """Inclusive [start, end] range of Turkish calendar dates."""
    today_tr = dt.datetime.now(TR_TZ).date()
    if period == "today":
        return today_tr, today_tr
    if period == "yesterday":
        d = today_tr - dt.timedelta(days=1)
        return d, d
    if period == "dayahead":
        d = today_tr + dt.timedelta(days=1)
        return d, d
    if not start_s:
        raise CommandError("Provide --period or --start YYYY-MM-DD [--end YYYY-MM-DD].")
    try:
        start = dt.date.fromisoformat(start_s)
        end = dt.date.fromisoformat(end_s) if end_s else start
    except ValueError as e:
        raise CommandError(f"Invalid date: {e}")
    if start > end:
        raise CommandError("--start must not be after --end.")
    return start, end


def iter_chunks(start: dt.date, end: dt.date, chunk_days: int):
    """Yield (chunk_start, chunk_end) inclusive date pairs."""
    chunk_days = max(1, chunk_days)
    cur = start
    while cur <= end:
        chunk_end = min(end, cur + dt.timedelta(days=chunk_days - 1))
        yield cur, chunk_end
        cur = chunk_end + dt.timedelta(days=1)
