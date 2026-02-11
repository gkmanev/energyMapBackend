import datetime as dt

import pandas as pd
from django.test import SimpleTestCase

from entsoe_api.entsoe_data import EntsoeGenerationForecastByType
from entsoe_api.views import _parse_iso_utc_floor_hour


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
