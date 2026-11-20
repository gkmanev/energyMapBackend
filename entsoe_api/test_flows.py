import datetime as dt
from unittest.mock import patch

from django.test import TestCase

from .models import Country, PhysicalFlow


class CountryFlowsRangeTests(TestCase):
    url = "/api/flows/country-range/"

    def setUp(self):
        self.start = dt.datetime(2026, 11, 19, tzinfo=dt.timezone.utc)
        for iso in ("BG", "RO", "GR"):
            Country.objects.create(iso_code=iso, name=iso)

    def flow(self, source, destination, minutes, quantity, resolution="PT15M", **kwargs):
        return PhysicalFlow.objects.create(
            country_from_id=source, country_to_id=destination,
            out_domain_eic=kwargs.pop("out_domain_eic", source or "UNMAPPED"),
            in_domain_eic=kwargs.pop("in_domain_eic", destination or "UNMAPPED"),
            datetime_utc=self.start + dt.timedelta(minutes=minutes),
            quantity_mw=quantity, resolution=resolution, **kwargs,
        )

    def get_series(self, **kwargs):
        params = {"country": "BG", "start": self.start.isoformat(),
                  "end": (self.start + dt.timedelta(hours=1)).isoformat()}
        params.update(kwargs)
        response = self.client.get(self.url, params)
        self.assertEqual(response.status_code, 200, response.content)
        return response.json()

    def test_hourly_time_weighting_and_country_border_totals(self):
        for minute, quantity in zip((0, 15, 30, 45), (100, 200, 300, 400)):
            self.flow("RO", "BG", minute, quantity)
        self.flow("BG", "GR", 0, 80, "PT60M")
        # Include unknown neighbor mappings and multiple zones for a country.
        self.flow(None, "BG", 0, 20, "PT1H")
        self.flow("RO", "BG", 0, 30, "PT1H", out_domain_eic="RO_ZONE_2")
        self.flow("RO", "GR", 0, 999, "PT1H")
        self.flow("BG", "BG", 0, 999, "PT1H")
        payload = self.get_series(country=" bg ")
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["resolution"], "h")
        self.assertEqual(payload["unit"], "MW")
        self.assertEqual(payload["items"][0], {
            "datetime_utc": "2026-11-19T00:00:00Z", "imports_mw": 300,
            "exports_mw": 80, "net_mw": 220, "is_partial": False,
        })

    def test_quarter_hour_buckets_extend_hourly_readings(self):
        self.flow("BG", "RO", 0, 100, "PT1H")
        for minute in (0, 15, 30, 45):
            self.flow("GR", "BG", minute, 40)
        items = self.get_series(resolution="15m")["items"]
        self.assertEqual(len(items), 4)
        self.assertEqual([row["datetime_utc"][14:16] for row in items], ["00", "15", "30", "45"])
        for row in items:
            self.assertEqual(row["net_mw"], -60)
            self.assertFalse(row["is_partial"])

    def test_clips_intervals_and_preserves_precise_exclusive_range(self):
        self.flow("RO", "BG", 0, 100, "PT1H")
        self.flow("BG", "GR", 30, 50)
        self.flow("GR", "BG", 45, 999)
        payload = self.get_series(start="2026-11-19T00:30:00Z",
                                  end="2026-11-19T00:45:00Z", resolution="15m")
        self.assertEqual(payload["start_utc"], "2026-11-19T00:30:00Z")
        self.assertEqual(payload["items"][0]["net_mw"], 50)
        self.assertFalse(payload["items"][0]["is_partial"])

    def test_overlapping_resolutions_do_not_double_count_same_border(self):
        self.flow("RO", "BG", 0, 100, "PT1H")
        self.flow("RO", "BG", 15, 200)
        item = self.get_series()["items"][0]
        self.assertEqual(item["imports_mw"], 125)
        self.assertFalse(item["is_partial"])

    def test_daily_weighting_uses_durations_and_flags_gaps(self):
        self.flow("RO", "BG", 0, 100, "PT1H")
        self.flow("RO", "BG", 60, 200, "PT15M")
        item = self.get_series(end="2026-11-20T00:00:00Z", resolution="d")["items"][0]
        self.assertEqual(item["imports_mw"], 120)
        self.assertTrue(item["is_partial"])

    def test_missing_border_bucket_is_flagged_and_empty_buckets_omitted(self):
        self.flow("RO", "BG", 0, 100, "PT1H")
        self.flow("BG", "GR", 0, 40, "PT1H")
        self.flow("RO", "BG", 60, 100, "PT1H")
        items = self.get_series(end="2026-11-19T03:00:00Z")["items"]
        self.assertEqual(len(items), 2)
        self.assertFalse(items[0]["is_partial"])
        self.assertTrue(items[1]["is_partial"])

    def test_empty_data_returns_empty_series(self):
        payload = self.get_series()
        self.assertEqual(payload["items"], [])
        self.assertEqual(payload["count"], 0)

    @patch("entsoe_api.views._now_utc")
    def test_today_clamps_to_completed_quarter_hour(self, now):
        now.return_value = self.start + dt.timedelta(minutes=37)
        for minute in (0, 15, 30, 45):
            self.flow("RO", "BG", minute, 100)
        response = self.client.get(self.url, {"country": "BG", "period": "today", "resolution": "15m"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["end_utc"], "2026-11-19T00:30:00Z")
        self.assertEqual(response.json()["count"], 2)

    @patch("entsoe_api.views._now_utc")
    def test_today_at_midnight_returns_empty_series(self, now):
        now.return_value = self.start
        response = self.client.get(self.url, {"country": "BG", "period": "today"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 0)

    def test_invalid_requests_return_bad_request(self):
        valid = {"country": "BG", "start": "2026-11-19", "end": "2026-11-20"}
        for change in ({"country": "XX"}, {"country": ""}, {"resolution": "bad"},
                       {"start": "bad"}, {"end": "2026-11-18"}, {"end": "2026-11-19"},
                       {"period": "tomorrow"}, {"period": "today"}, {"end": ""}):
            with self.subTest(change=change):
                response = self.client.get(self.url, {**valid, **change})
                self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(self.client.get(self.url, {"country": "BG"}).status_code, 400)

    def test_api_root_exposes_endpoint(self):
        self.assertTrue(self.client.get("/api/").json()["flows_country_range"].endswith(self.url))
