import datetime as dt

import pandas as pd
from django.test import SimpleTestCase

from entsoe_api.entsoe_data import EntsoeGenerationForecastByType


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
