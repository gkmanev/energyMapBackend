import datetime as dt
import json
from unittest.mock import MagicMock, patch

import pandas as pd
from django.core.cache import cache
from django.test import SimpleTestCase, TestCase, override_settings

from entsoe_api.chart_conversation import load_chart_conversation
from entsoe_api.chart_query import ParsedDataQuery, parse_chart_query
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
from entsoe_api.models import Country, CountryPricePoint, CountryResGenerationByType, CountryWindSpeedPoint
from entsoe_api.views import _parse_iso_utc_floor_hour, _partition_country_codes


def _make_response(input_data: dict) -> MagicMock:
    """Build the mock object returned by client.messages.create (tool_use block)."""
    tool_block = MagicMock()
    tool_block.type = "tool_use"
    tool_block.name = "analyze_energy_query"
    tool_block.input = input_data
    response = MagicMock()
    response.content = [tool_block]
    return response


def _make_client_mock(input_data: dict) -> MagicMock:
    """Build a mock Anthropic client that returns the given tool input on messages.create."""
    client = MagicMock()
    client.messages.create.return_value = _make_response(input_data)
    return client


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


class ChartQueryParserTest(SimpleTestCase):
    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_parses_multi_metric_daily_query(self, mock_anthropic):
        mock_client = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG"],
            "resolution": "d",
            "generation_series": ["wind", "solar"],
            "include_prices": True,
            "timeframe": {"kind": "last_n_weeks", "amount": 2, "start_utc": None, "end_utc": None},
        })
        mock_anthropic.return_value = mock_client

        result = parse_chart_query(
            "Show the wind and solar generation for BG for the last two weeks daily resolution as well as the prices",
            now_utc=dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "ready")
        parsed = result.query
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.country, "BG")
        self.assertEqual(parsed.countries, ["BG"])
        self.assertEqual(parsed.resolution, "d")
        self.assertEqual(parsed.generation_series, ["wind", "solar"])
        self.assertTrue(parsed.include_prices)
        self.assertEqual(parsed.start_utc, dt.datetime(2026, 4, 15, 0, 0, tzinfo=dt.timezone.utc))
        self.assertEqual(parsed.end_utc, dt.datetime(2026, 4, 29, 0, 0, tzinfo=dt.timezone.utc))
        call_kwargs = mock_client.messages.create.call_args.kwargs
        self.assertEqual(call_kwargs.get("model"), "claude-sonnet-4-6")

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_parses_multi_country_price_comparison_query(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG", "RO"],
            "resolution": "d",
            "generation_series": [],
            "include_prices": True,
            "timeframe": {"kind": "last_n_weeks", "amount": 4, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "Compare the prices for BG and RO for the last month. Daily resolution",
            now_utc=dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "ready")
        parsed = result.query
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.country, "BG")
        self.assertEqual(parsed.countries, ["BG", "RO"])
        self.assertEqual(parsed.resolution, "d")
        self.assertEqual(parsed.generation_series, [])
        self.assertTrue(parsed.include_prices)
        self.assertEqual(parsed.start_utc, dt.datetime(2026, 4, 1, 0, 0, tzinfo=dt.timezone.utc))
        self.assertEqual(parsed.end_utc, dt.datetime(2026, 4, 29, 0, 0, tzinfo=dt.timezone.utc))

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_parses_res_query_when_model_returns_res_series(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG", "RO"],
            "resolution": "d",
            "generation_series": ["res"],
            "include_prices": False,
            "timeframe": {"kind": "last_n_weeks", "amount": 4, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "Compare the RES generation for BG and RO last month",
            now_utc=dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "ready")
        parsed = result.query
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.country, "BG")
        self.assertEqual(parsed.countries, ["BG", "RO"])
        self.assertEqual(parsed.resolution, "d")
        self.assertEqual(parsed.generation_series, ["res"])
        self.assertFalse(parsed.include_prices)

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_falls_back_to_res_when_message_mentions_renewables(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG"],
            "resolution": "d",
            "generation_series": [],
            "include_prices": False,
            "timeframe": {"kind": "last_n_weeks", "amount": 4, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "Show renewable generation for BG last month",
            now_utc=dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "ready")
        parsed = result.query
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.generation_series, ["res"])

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_defaults_last_month_to_daily_when_resolution_is_omitted(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG", "RO"],
            "resolution": "native",
            "generation_series": ["res"],
            "include_prices": False,
            "timeframe": {"kind": "last_n_weeks", "amount": 4, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "Compare the RES generation for BG and RO last month",
            now_utc=dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "ready")
        parsed = result.query
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.resolution, "d")
        self.assertEqual(parsed.time_phrase, "last 4 weeks at daily resolution")

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_defaults_short_windows_to_native_when_resolution_is_omitted(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG"],
            "resolution": "native",
            "generation_series": ["wind"],
            "include_prices": False,
            "timeframe": {"kind": "last_n_days", "amount": 2, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "Show the wind generation for BG for the last couple of days",
            now_utc=dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "ready")
        parsed = result.query
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.resolution, "")
        self.assertEqual(parsed.time_phrase, "last 2 days")

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_visual_follow_up_reuses_previous_query_context(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "",
            "countries": [],
            "resolution": "native",
            "generation_series": [],
            "include_prices": False,
            "chart_type": "bar",
            "timeframe": {"kind": "today", "amount": None, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "can you make it as bar chart",
            now_utc=dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc),
            previous_query={
                "country": "BG",
                "countries": ["BG", "RO"],
                "start_utc": "2026-04-01T00:00:00Z",
                "end_utc": "2026-04-29T00:00:00Z",
                "resolution": "d",
                "time_phrase": "last 4 weeks at daily resolution",
                "generation_series": ["res"],
                "include_prices": False,
                "chart_type": "line",
            },
        )

        self.assertEqual(result.status, "ready")
        parsed = result.query
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.countries, ["BG", "RO"])
        self.assertEqual(parsed.generation_series, ["res"])
        self.assertEqual(parsed.chart_type, "bar")
        self.assertEqual(parsed.start_utc, dt.datetime(2026, 4, 1, 0, 0, tzinfo=dt.timezone.utc))
        self.assertEqual(parsed.end_utc, dt.datetime(2026, 4, 29, 0, 0, tzinfo=dt.timezone.utc))
        self.assertEqual(parsed.resolution, "d")

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_visual_follow_up_without_context_returns_actionable_error(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "",
            "countries": [],
            "resolution": "native",
            "generation_series": [],
            "include_prices": False,
            "chart_type": "bar",
            "timeframe": {"kind": "today", "amount": None, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "can you make it as bar chart",
            now_utc=dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "needs_clarification")
        self.assertEqual(result.clarification.missing_fields, ["metric", "country", "timeframe"])
        self.assertIn("Which", result.clarification.question)

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_explicit_message_details_override_stale_model_output(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG"],
            "resolution": "d",
            "generation_series": ["wind", "solar"],
            "include_prices": True,
            "timeframe": {"kind": "last_n_weeks", "amount": 2, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "Compare res generation for BG and RO for April. Daily resolution",
            now_utc=dt.datetime(2026, 4, 30, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "ready")
        parsed = result.query
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.country, "BG")
        self.assertEqual(parsed.countries, ["BG", "RO"])
        self.assertEqual(parsed.resolution, "d")
        self.assertEqual(parsed.generation_series, ["res"])
        self.assertFalse(parsed.include_prices)
        self.assertEqual(parsed.start_utc, dt.datetime(2026, 4, 1, 0, 0, tzinfo=dt.timezone.utc))
        self.assertEqual(parsed.end_utc, dt.datetime(2026, 4, 30, 0, 0, tzinfo=dt.timezone.utc))
        self.assertEqual(parsed.time_phrase, "April at daily resolution")

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_returns_clarification_when_timeframe_is_missing(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "needs_clarification",
            "clarifying_question": "What time range should I use for BG prices?",
            "missing_fields": ["timeframe"],
            "country": "BG",
            "countries": ["BG"],
            "resolution": "native",
            "generation_series": [],
            "include_prices": True,
            "chart_type": "line",
            "timeframe": {"kind": "unknown", "amount": None, "start_utc": None, "end_utc": None},
        })

        result = parse_chart_query(
            "Show me BG prices",
            now_utc=dt.datetime(2026, 4, 30, 13, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "needs_clarification")
        self.assertEqual(result.clarification.missing_fields, ["timeframe"])
        self.assertEqual(result.clarification.question, "What time range should I use for BG prices?")

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_passes_recent_conversation_context_to_claude(self, mock_anthropic):
        mock_client = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG"],
            "resolution": "d",
            "generation_series": [],
            "include_prices": True,
            "chart_type": "line",
            "timeframe": {"kind": "last_n_weeks", "amount": 4, "start_utc": None, "end_utc": None},
        })
        mock_anthropic.return_value = mock_client

        result = parse_chart_query(
            "last month",
            now_utc=dt.datetime(2026, 4, 30, 13, 0, tzinfo=dt.timezone.utc),
            previous_query=None,
            conversation_messages=[
                {"role": "user", "content": "Show me the prices for BG"},
                {"role": "assistant", "content": "What time range should I use for BG prices?"},
            ],
            pending_clarification={
                "question": "What time range should I use for BG prices?",
                "missing_fields": ["timeframe"],
            },
        )

        self.assertEqual(result.status, "ready")
        messages_sent = mock_client.messages.create.call_args.kwargs.get("messages", [])
        self.assertTrue(any(
            item["role"] == "assistant" and item["content"] == "What time range should I use for BG prices?"
            for item in messages_sent
        ))
        self.assertTrue(any(
            item["role"] == "user" and item["content"] == "Show me the prices for BG"
            for item in messages_sent
        ))
        self.assertEqual(messages_sent[-1], {"role": "user", "content": "last month"})


    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    def test_parses_data_intent_as_price_stats_query(self, mock_anthropic):
        mock_anthropic.return_value = _make_client_mock({
            "intent": "data",
            "data_type": "prices",
            "country": "ES",
            "countries": ["ES"],
            "resolution": "native",
            "generation_series": [],
            "include_prices": True,
            "chart_type": "line",
            "timeframe": {
                "kind": "explicit_utc_range",
                "amount": None,
                "start_utc": "2025-04-01T00:00:00Z",
                "end_utc": "2025-05-01T00:00:00Z",
            },
            "text_answer": None,
            "clarifying_question": None,
            "missing_fields": [],
            "country_from": None,
            "country_to": None,
        })

        result = parse_chart_query(
            "What is the average price in ES for April 2025?",
            now_utc=dt.datetime(2025, 5, 7, 12, 0, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(result.status, "data")
        self.assertIsInstance(result.query, ParsedDataQuery)
        parsed = result.query
        self.assertEqual(parsed.country, "ES")
        self.assertEqual(parsed.countries, ["ES"])
        self.assertEqual(parsed.data_type, "prices")
        self.assertTrue(parsed.include_prices)
        self.assertEqual(parsed.start_utc, dt.datetime(2025, 4, 1, 0, 0, tzinfo=dt.timezone.utc))
        self.assertEqual(parsed.end_utc, dt.datetime(2025, 5, 1, 0, 0, tzinfo=dt.timezone.utc))


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


class ChartQueryApiTest(TestCase):
    def setUp(self):
        cache.clear()
        Country.objects.create(iso_code="BG", name="Bulgaria")
        Country.objects.create(iso_code="RO", name="Romania")

        for timestamp, solar, wind_offshore, wind_onshore, price in [
            (dt.datetime(2026, 4, 15, 0, 0, tzinfo=dt.timezone.utc), 10.0, 3.0, 7.0, 60.0),
            (dt.datetime(2026, 4, 15, 1, 0, tzinfo=dt.timezone.utc), 14.0, 4.0, 8.0, 80.0),
            (dt.datetime(2026, 4, 16, 0, 0, tzinfo=dt.timezone.utc), 20.0, 5.0, 5.0, 100.0),
        ]:
            CountryResGenerationByType.objects.create(
                country_id="BG",
                datetime_utc=timestamp,
                psr_type="B16",
                psr_name="Solar",
                generation_mw=solar,
                unit="MW",
                resolution="PT60M",
            )
            CountryResGenerationByType.objects.create(
                country_id="BG",
                datetime_utc=timestamp,
                psr_type="B18",
                psr_name="Wind Offshore",
                generation_mw=wind_offshore,
                unit="MW",
                resolution="PT60M",
            )
            CountryResGenerationByType.objects.create(
                country_id="BG",
                datetime_utc=timestamp,
                psr_type="B19",
                psr_name="Wind Onshore",
                generation_mw=wind_onshore,
                unit="MW",
                resolution="PT60M",
            )
            CountryPricePoint.objects.create(
                country_id="BG",
                datetime_utc=timestamp,
                contract_type="A01",
                price=price,
                currency="EUR",
                unit="MWH",
                resolution="PT60M",
            )

        for timestamp, solar, wind_offshore, wind_onshore, price in [
            (dt.datetime(2026, 4, 15, 0, 0, tzinfo=dt.timezone.utc), 8.0, 2.0, 4.0, 40.0),
            (dt.datetime(2026, 4, 15, 1, 0, tzinfo=dt.timezone.utc), 12.0, 3.0, 5.0, 60.0),
            (dt.datetime(2026, 4, 16, 0, 0, tzinfo=dt.timezone.utc), 16.0, 4.0, 6.0, 110.0),
        ]:
            CountryResGenerationByType.objects.create(
                country_id="RO",
                datetime_utc=timestamp,
                psr_type="B16",
                psr_name="Solar",
                generation_mw=solar,
                unit="MW",
                resolution="PT60M",
            )
            CountryResGenerationByType.objects.create(
                country_id="RO",
                datetime_utc=timestamp,
                psr_type="B18",
                psr_name="Wind Offshore",
                generation_mw=wind_offshore,
                unit="MW",
                resolution="PT60M",
            )
            CountryResGenerationByType.objects.create(
                country_id="RO",
                datetime_utc=timestamp,
                psr_type="B19",
                psr_name="Wind Onshore",
                generation_mw=wind_onshore,
                unit="MW",
                resolution="PT60M",
            )
            CountryPricePoint.objects.create(
                country_id="RO",
                datetime_utc=timestamp,
                contract_type="A01",
                price=price,
                currency="EUR",
                unit="MWH",
                resolution="PT60M",
            )

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    @patch("entsoe_api.views._now_utc")
    def test_chart_query_returns_generation_and_price_panels(self, mock_now_utc, mock_anthropic):
        mock_now_utc.return_value = dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc)
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG"],
            "resolution": "d",
            "generation_series": ["wind", "solar"],
            "include_prices": True,
            "timeframe": {"kind": "last_n_weeks", "amount": 2, "start_utc": None, "end_utc": None},
        })

        response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "Show the wind and solar generation for BG for the last two weeks daily resolution as well as the prices",
            }),
            content_type="application/json",
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["query"]["country"], "BG")
        self.assertEqual(payload["query"]["countries"], ["BG"])
        self.assertEqual(payload["query"]["start_utc"], "2026-04-15T00:00:00Z")
        self.assertEqual(payload["query"]["end_utc"], "2026-04-29T00:00:00Z")
        self.assertEqual(payload["query"]["generation_series"], ["wind", "solar"])

        generation_panel = payload["panels"][0]
        self.assertEqual(generation_panel["id"], "generation")
        self.assertEqual(generation_panel["series"][0]["id"], "wind")
        self.assertEqual(generation_panel["series"][0]["data"][0]["value"], 11.0)
        self.assertEqual(generation_panel["series"][1]["id"], "solar")
        self.assertEqual(generation_panel["series"][1]["data"][0]["value"], 12.0)

        prices_panel = payload["panels"][1]
        self.assertEqual(prices_panel["id"], "prices")
        self.assertEqual(prices_panel["series"][0]["data"][0]["value"], 70.0)

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    @patch("entsoe_api.views._now_utc")
    def test_chart_query_returns_multi_country_price_panel(self, mock_now_utc, mock_anthropic):
        mock_now_utc.return_value = dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc)
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG", "RO"],
            "resolution": "d",
            "generation_series": [],
            "include_prices": True,
            "timeframe": {"kind": "last_n_weeks", "amount": 4, "start_utc": None, "end_utc": None},
        })

        response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "Compare the prices for BG and RO for the last month. Daily resolution",
            }),
            content_type="application/json",
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["query"]["country"], "BG")
        self.assertEqual(payload["query"]["countries"], ["BG", "RO"])

        prices_panel = payload["panels"][0]
        self.assertEqual(prices_panel["id"], "prices")
        self.assertEqual(prices_panel["title"], "BG vs RO day-ahead prices")
        self.assertEqual([series["id"] for series in prices_panel["series"]], ["bg", "ro"])
        self.assertEqual(prices_panel["series"][0]["data"][0]["value"], 70.0)
        self.assertEqual(prices_panel["series"][1]["data"][0]["value"], 50.0)

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    @patch("entsoe_api.views._now_utc")
    def test_chart_query_returns_multi_country_res_panel(self, mock_now_utc, mock_anthropic):
        mock_now_utc.return_value = dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc)
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG", "RO"],
            "resolution": "d",
            "generation_series": ["res"],
            "include_prices": False,
            "timeframe": {"kind": "last_n_weeks", "amount": 4, "start_utc": None, "end_utc": None},
        })

        response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "Compare the RES generation for BG and RO last month",
            }),
            content_type="application/json",
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["query"]["generation_series"], ["res"])

        generation_panel = payload["panels"][0]
        self.assertEqual(generation_panel["id"], "generation")
        self.assertEqual(generation_panel["title"], "BG vs RO renewable generation")
        self.assertEqual([series["id"] for series in generation_panel["series"]], ["bg_res", "ro_res"])
        self.assertEqual(generation_panel["series"][0]["name"], "BG RES (solar + wind)")
        self.assertEqual(generation_panel["series"][0]["data"][0]["value"], 22.0)
        self.assertEqual(generation_panel["series"][1]["data"][0]["value"], 17.0)

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    @patch("entsoe_api.views._now_utc")
    def test_chart_query_follow_up_can_switch_to_bar_chart(self, mock_now_utc, mock_anthropic):
        mock_now_utc.return_value = dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc)
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "",
            "countries": [],
            "resolution": "native",
            "generation_series": [],
            "include_prices": False,
            "chart_type": "bar",
            "timeframe": {"kind": "today", "amount": None, "start_utc": None, "end_utc": None},
        })

        response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "can you make it as bar chart",
                "previous_query": {
                    "country": "BG",
                    "countries": ["BG", "RO"],
                    "start_utc": "2026-04-01T00:00:00Z",
                    "end_utc": "2026-04-29T00:00:00Z",
                    "resolution": "d",
                    "time_phrase": "last 4 weeks at daily resolution",
                    "generation_series": ["res"],
                    "include_prices": False,
                    "chart_type": "line",
                },
            }),
            content_type="application/json",
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["query"]["chart_type"], "bar")
        self.assertEqual(payload["query"]["countries"], ["BG", "RO"])
        self.assertEqual(payload["query"]["generation_series"], ["res"])
        self.assertEqual(payload["panels"][0]["type"], "bar")
        self.assertIn("bar chart", payload["assistant_message"])

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    @patch("entsoe_api.views._now_utc")
    def test_chart_query_uses_explicit_message_details_when_model_output_is_stale(self, mock_now_utc, mock_anthropic):
        mock_now_utc.return_value = dt.datetime(2026, 4, 30, 13, 0, tzinfo=dt.timezone.utc)
        mock_anthropic.return_value = _make_client_mock({
            "intent": "chart",
            "country": "BG",
            "countries": ["BG"],
            "resolution": "d",
            "generation_series": ["wind", "solar"],
            "include_prices": True,
            "timeframe": {"kind": "last_n_weeks", "amount": 2, "start_utc": None, "end_utc": None},
        })

        response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "Compare res generation for BG and RO for April. Daily resolution",
            }),
            content_type="application/json",
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["query"]["countries"], ["BG", "RO"])
        self.assertEqual(payload["query"]["generation_series"], ["res"])
        self.assertFalse(payload["query"]["include_prices"])
        self.assertEqual(payload["query"]["start_utc"], "2026-04-01T00:00:00Z")
        self.assertEqual(payload["query"]["end_utc"], "2026-04-30T00:00:00Z")
        self.assertEqual(payload["panels"][0]["title"], "BG vs RO renewable generation")
        self.assertEqual([series["id"] for series in payload["panels"][0]["series"]], ["bg_res", "ro_res"])

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    @patch("entsoe_api.views._now_utc")
    def test_chart_query_returns_clarifying_question_when_request_is_ambiguous(self, mock_now_utc, mock_anthropic):
        mock_now_utc.return_value = dt.datetime(2026, 4, 30, 13, 0, tzinfo=dt.timezone.utc)
        mock_anthropic.return_value = _make_client_mock({
            "intent": "needs_clarification",
            "clarifying_question": "Which country and what time range should I use for the chart?",
            "missing_fields": ["country", "timeframe"],
            "country": "",
            "countries": [],
            "resolution": "native",
            "generation_series": [],
            "include_prices": True,
            "chart_type": "line",
            "timeframe": {"kind": "unknown", "amount": None, "start_utc": None, "end_utc": None},
        })

        response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "Show me the prices",
            }),
            content_type="application/json",
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "needs_clarification")
        self.assertIsNone(payload["query"])
        self.assertEqual(payload["clarifying_question"], "Which country and what time range should I use for the chart?")
        self.assertEqual(payload["clarification"]["missing_fields"], ["country", "timeframe"])
        self.assertEqual(payload["panels"], [])
        self.assertTrue(payload["conversation_id"])

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    @patch("entsoe_api.views._now_utc")
    def test_chart_query_reuses_redis_conversation_context_for_follow_up(self, mock_now_utc, mock_anthropic):
        mock_now_utc.return_value = dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc)
        mock_client = MagicMock()
        mock_anthropic.return_value = mock_client
        mock_client.messages.create.side_effect = [
            _make_response({
                "intent": "chart",
                "country": "BG",
                "countries": ["BG", "RO"],
                "resolution": "d",
                "generation_series": ["res"],
                "include_prices": False,
                "chart_type": "line",
                "timeframe": {"kind": "last_n_weeks", "amount": 4, "start_utc": None, "end_utc": None},
            }),
            _make_response({
                "intent": "chart",
                "country": "",
                "countries": [],
                "resolution": "native",
                "generation_series": [],
                "include_prices": False,
                "chart_type": "bar",
                "timeframe": {"kind": "today", "amount": None, "start_utc": None, "end_utc": None},
            }),
        ]

        first_response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "Compare the RES generation for BG and RO last month",
            }),
            content_type="application/json",
        )
        first_payload = first_response.json()
        conversation_id = first_payload["conversation_id"]

        second_response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "can you make it as bar chart",
                "conversation_id": conversation_id,
            }),
            content_type="application/json",
        )

        second_payload = second_response.json()
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(second_payload["status"], "ready")
        self.assertEqual(second_payload["conversation_id"], conversation_id)
        self.assertEqual(second_payload["query"]["chart_type"], "bar")
        self.assertEqual(second_payload["query"]["countries"], ["BG", "RO"])
        self.assertEqual(second_payload["query"]["generation_series"], ["res"])

        conversation = load_chart_conversation(conversation_id)
        self.assertIsNotNone(conversation)
        self.assertEqual(len(conversation["messages"]), 4)
        self.assertEqual(conversation["previous_query"]["chart_type"], "bar")

    @override_settings(ANTHROPIC_API_KEY="test-key", CLAUDE_CHAT_MODEL="claude-sonnet-4-6")
    @patch("entsoe_api.chart_query.anthropic.Anthropic")
    @patch("entsoe_api.views._now_utc")
    def test_chart_query_data_intent_fetches_data_and_calls_claude_for_analysis(self, mock_now_utc, mock_anthropic):
        mock_now_utc.return_value = dt.datetime(2026, 4, 29, 13, 0, tzinfo=dt.timezone.utc)

        # First Anthropic() instance → classification call (tool use)
        first_client = _make_client_mock({
            "intent": "data",
            "data_type": "prices",
            "country": "BG",
            "countries": ["BG"],
            "resolution": "native",
            "generation_series": [],
            "include_prices": True,
            "chart_type": "line",
            "timeframe": {"kind": "last_n_weeks", "amount": 2, "start_utc": None, "end_utc": None},
            "text_answer": None,
            "clarifying_question": None,
            "missing_fields": [],
            "country_from": None,
            "country_to": None,
        })

        # Second Anthropic() instance → data analysis call (plain text)
        analysis_text_block = MagicMock()
        analysis_text_block.type = "text"
        analysis_text_block.text = "BG: avg 80.00 EUR/MWh, max 100.00, min 60.00 for last 2 weeks (3 data points)."
        second_client = MagicMock()
        second_client.messages.create.return_value = MagicMock(content=[analysis_text_block])

        mock_anthropic.side_effect = [first_client, second_client]

        response = self.client.post(
            "/api/chart-query/",
            data=json.dumps({
                "message": "What is the average price in BG for the last two weeks?",
            }),
            content_type="application/json",
        )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "data")
        self.assertIsNone(payload["query"])
        self.assertEqual(payload["panels"], [])
        self.assertIn("80.00", payload["text_answer"])
        self.assertIn("EUR/MWh", payload["text_answer"])
        # Verify the second Claude call received the fetched DB data
        analysis_call_kwargs = second_client.messages.create.call_args.kwargs
        user_msg = analysis_call_kwargs["messages"][0]["content"]
        self.assertIn("BG", user_msg)
        self.assertIn("EUR/MWh", user_msg)
