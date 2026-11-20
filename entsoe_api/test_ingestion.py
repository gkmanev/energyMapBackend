import datetime as dt
import json
from unittest.mock import Mock, patch

import requests
from django.test import SimpleTestCase
from django.core.management.base import CommandError

from entsoe_api import tasks
from entsoe_api import epias
from entsoe_api.entsoe_data import EntsoePrices


class IngestionRegressionTests(SimpleTestCase):
    def test_generation_fetch_recovers_previous_day_after_midnight(self):
        with patch("entsoe_api.tasks.datetime") as clock, patch("entsoe_api.tasks.call_command") as command:
            clock.now.return_value = dt.datetime(2026, 11, 12, 22, 5, tzinfo=dt.timezone.utc)
            tasks.fetch_generation_hourly_task.run()

        command.assert_called_once_with(
            "fetch_generation", start="2026-11-10T23:00:00Z", end="2026-11-12T23:00:00Z"
        )

    def test_prices_request_has_timeout(self):
        session = Mock()
        session.get.return_value = Mock(status_code=200, text="<Publication_MarketDocument />")
        result = EntsoePrices("test-token", session=session)._request_with_retries({"documentType": "A44"})
        self.assertEqual(result, "<Publication_MarketDocument />")
        self.assertEqual(session.get.call_args.kwargs["timeout"], 45)

    def test_prices_retries_transient_server_failure(self):
        session = Mock()
        session.get.side_effect = [
            Mock(status_code=503, headers={}, text="unavailable"),
            Mock(status_code=200, text="<Publication_MarketDocument />"),
        ]
        with patch("time.sleep") as sleep:
            result = EntsoePrices("test-token", session=session)._request_with_retries({})
        self.assertEqual(result, "<Publication_MarketDocument />")
        self.assertEqual(session.get.call_count, 2)
        sleep.assert_called_once_with(1.0)

    def test_prices_http_failure_is_not_returned_as_empty_success(self):
        session = Mock()
        session.get.return_value = Mock(status_code=403, text="denied")
        session.get.return_value.raise_for_status.side_effect = requests.HTTPError("denied")
        with self.assertRaises(requests.HTTPError):
            EntsoePrices("test-token", session=session)._request_with_retries({})

    def test_unpublished_turkish_prices_do_not_fail_the_task(self):
        response = requests.Response()
        response.status_code = 400
        response._content = json.dumps({"errors": [{"errorCode": "(BUS)SEF1124"}]}).encode()
        failure = requests.HTTPError(response=response)
        with patch("entsoe_api.epias.post", side_effect=failure) as post:
            result = epias.post_with_retry("test-ticket", "/v1/markets/dam/data/mcp", {})
        self.assertEqual(result, {"items": []})
        post.assert_called_once()

    def test_other_turkish_api_errors_remain_failures(self):
        response = requests.Response()
        response.status_code = 400
        response._content = json.dumps({"errors": [{"errorCode": "OTHER"}]}).encode()
        with patch("entsoe_api.epias.post", side_effect=requests.HTTPError(response=response)):
            with self.assertRaises(CommandError):
                epias.post_with_retry("test-ticket", "/v1/markets/dam/data/mcp", {}, max_retries=1)
