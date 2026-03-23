import datetime as dt
from unittest.mock import patch

import pandas as pd
from django.core.cache import cache
from django.test import SimpleTestCase, TestCase, override_settings

from entsoe_api.entsoe_data import EntsoeGenerationForecastByType
from entsoe_api.helper import save_country_wind_speed_df
from entsoe_api.management.commands.fetch_generation_eso_bg import (
    _extract_results,
    _normalize_generation_record,
)
from entsoe_api.management.commands.fetch_global_tilted_irradiance import (
    _chunked,
    _compute_date_window,
    _iter_date_chunks,
)
from entsoe_api.models import Country, CountryWindSpeedPoint
from entsoe_api.views import _parse_iso_utc_floor_hour, _partition_country_codes


class GenerationForecastHelpersTest(SimpleTestCase):
    def test_missing_psr_values_are_normalized(self):
        df = pd.DataFrame(
            {
                "datetime_utc": [dt.datetime(2025, 11, 19, 0)],
                "zone": ["10YCA-BULGARIA-R"],
                "psr_type": [None],
                "psr_name": [""],
                "generation_MW": [2263.625],
            }
        )

        normalized = EntsoeGenerationForecastByType._ensure_psr_values(df)

        self.assertEqual(normalized.loc[0, "psr_type"], EntsoeGenerationForecastByType.ALL_PSR_CODE)
        self.assertEqual(normalized.loc[0, "psr_name"], EntsoeGenerationForecastByType.ALL_PSR_NAME)
        self.assertAlmostEqual(normalized.loc[0, "generation_MW"], 2263.625)


class ParseIsoUtcFloorHourTest(SimpleTestCase):
    def test_parses_iso_datetime_with_z(self):
        parsed = _parse_iso_utc_floor_hour("2026-02-11T22:00:00.000Z")
        self.assertEqual(parsed, dt.datetime(2026, 2, 11, 22, 0, tzinfo=dt.timezone.utc))

    def test_parses_date_only_values(self):
        parsed = _parse_iso_utc_floor_hour("2026-02-09")
        self.assertEqual(parsed, dt.datetime(2026, 2, 9, 0, 0, tzinfo=dt.timezone.utc))

    def test_invalid_values_raise_value_error(self):
        with self.assertRaises(ValueError):
            _parse_iso_utc_floor_hour("not-a-date")


class PartitionCountryCodesTest(SimpleTestCase):
    @patch("entsoe_api.views._all_country_codes")
    def test_partitions_valid_and_missing_country_codes(self, mock_all_country_codes):
        mock_all_country_codes.return_value = {"CH", "SE"}

        valid, missing = _partition_country_codes(["GB", "CH", "UA", "SE"])

        self.assertEqual(valid, ["CH", "SE"])
        self.assertEqual(missing, ["GB", "UA"])

    @patch("entsoe_api.views._all_country_codes")
    def test_returns_empty_valid_when_all_countries_are_unknown(self, mock_all_country_codes):
        mock_all_country_codes.return_value = set()

        valid, missing = _partition_country_codes(["GB", "UA"])

        self.assertEqual(valid, [])
        self.assertEqual(missing, ["GB", "UA"])


class FetchGenerationEsoBgHelpersTest(SimpleTestCase):
    def test_extract_results_accepts_paginated_payload(self):
        payload = {"count": 1, "next": None, "previous": None, "results": [{"psr_type": "B16"}]}

        results = _extract_results(payload)

        self.assertEqual(results, [{"psr_type": "B16"}])

    def test_normalize_generation_record_maps_b99_to_bess_charging(self):
        record = {
            "country_iso_code": "BG",
            "datetime_utc": "2026-03-12T09:00:00Z",
            "psr_type": "B99",
            "psr_name": "B99",
            "generation_mw": "62.720",
            "resolution": "snapshot",
        }

        normalized = _normalize_generation_record(record)

        self.assertEqual(normalized["country"], "BG")
        self.assertEqual(normalized["psr_type"], "B99")
        self.assertEqual(normalized["psr_name"], "BESS Charging")
        self.assertEqual(normalized["generation_MW"], 62.72)
        self.assertEqual(normalized["datetime_utc"], dt.datetime(2026, 3, 12, 9, 0, tzinfo=dt.timezone.utc))


class FetchGlobalTiltedIrradianceHelpersTest(SimpleTestCase):
    def test_compute_date_window_uses_explicit_range_when_provided(self):
        start_date, end_date = _compute_date_window("2026-03-10", "2026-03-12", past_days=0, forecast_days=7)

        self.assertEqual(start_date, dt.date(2026, 3, 10))
        self.assertEqual(end_date, dt.date(2026, 3, 12))

    def test_iter_date_chunks_splits_large_ranges(self):
        chunks = list(_iter_date_chunks(dt.date(2026, 3, 19), dt.date(2026, 3, 25), chunk_days=3))

        self.assertEqual(
            chunks,
            [
                (dt.date(2026, 3, 19), dt.date(2026, 3, 21)),
                (dt.date(2026, 3, 22), dt.date(2026, 3, 24)),
                (dt.date(2026, 3, 25), dt.date(2026, 3, 25)),
            ],
        )

    def test_chunked_groups_country_batches(self):
        items = [
            {"code": "AT"},
            {"code": "BG"},
            {"code": "CZ"},
            {"code": "DE"},
            {"code": "ES"},
        ]

        batches = list(_chunked(items, 2))

        self.assertEqual(
            batches,
            [
                [{"code": "AT"}, {"code": "BG"}],
                [{"code": "CZ"}, {"code": "DE"}],
                [{"code": "ES"}],
            ],
        )


class SaveCountryWindSpeedDfTest(TestCase):
    def test_upserts_country_wind_speed_rows(self):
        Country.objects.create(iso_code="BG", name="Bulgaria")

        written = save_country_wind_speed_df(pd.DataFrame(
            {
                "country": ["BG", "BG"],
                "datetime_utc": ["2026-03-19T00:00:00Z", "2026-03-19T01:00:00Z"],
                "wind_speed_120m": [8.7, 9.1],
                "resolution": ["PT1H", "PT1H"],
            }
        ))

        self.assertEqual(written, 2)
        self.assertEqual(CountryWindSpeedPoint.objects.count(), 2)
        self.assertEqual(
            list(CountryWindSpeedPoint.objects.order_by("datetime_utc").values_list("wind_speed_120m", flat=True)),
            [8.7, 9.1],
        )


@override_settings(COUNTRY_COORDS=[{"code": "BG", "lat": 42.7, "lng": 23.3}])
class WindSpeedApiTest(TestCase):
    def setUp(self):
        cache.clear()
        Country.objects.create(iso_code="BG", name="Bulgaria")
        CountryWindSpeedPoint.objects.create(
            country_id="BG",
            datetime_utc=dt.datetime(2026, 3, 19, 10, 0, tzinfo=dt.timezone.utc),
            wind_speed_120m=8.7,
            resolution="PT1H",
        )

    def test_single_country_range_returns_wind_speed_points(self):
        response = self.client.get(
            "/api/generation-wind-speed/range/",
            {
                "country": "BG",
                "start": "2026-03-19T00:00:00Z",
                "end": "2026-03-20T00:00:00Z",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["country"], "BG")
        self.assertEqual(response.json()["items"][0]["wind_speed_120m"], 8.7)

    def test_bulk_range_returns_all_configured_countries(self):
        response = self.client.get(
            "/api/generation-wind-speed/bulk-range/",
            {
                "countries": "ALL",
                "start": "2026-03-19T00:00:00Z",
                "end": "2026-03-20T00:00:00Z",
            },
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["request_info"]["countries_found"], ["BG"])
        self.assertEqual(payload["request_info"]["total_records"], 1)
        self.assertEqual(payload["data"]["BG"]["items"][0]["wind_speed_120m"], 8.7)
