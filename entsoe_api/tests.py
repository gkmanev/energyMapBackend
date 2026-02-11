import datetime as dt
from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase

from entsoe_api.entsoe_data import EntsoeGenerationForecastByType
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
    @patch("entsoe_api.views.Country.objects.filter")
    def test_partitions_valid_and_missing_country_codes(self, mock_filter):
        mock_filter.return_value.values_list.return_value = ["CH", "SE"]

        valid, missing = _partition_country_codes(["GB", "CH", "UA", "SE"])

        self.assertEqual(valid, ["CH", "SE"])
        self.assertEqual(missing, ["GB", "UA"])

    @patch("entsoe_api.views.Country.objects.filter")
    def test_returns_empty_valid_when_all_countries_are_unknown(self, mock_filter):
        mock_filter.return_value.values_list.return_value = []

        valid, missing = _partition_country_codes(["GB", "UA"])

        self.assertEqual(valid, [])
        self.assertEqual(missing, ["GB", "UA"])
