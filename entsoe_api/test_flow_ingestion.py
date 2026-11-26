"""Regression checks for independent, incremental European flow ingestion."""
import datetime as dt
from io import StringIO
import os
from threading import Event
from unittest.mock import Mock, patch

import pandas as pd
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, override_settings

from .entsoe_data import EntsoePhysicalFlows
from .management.commands.fetch_flows import build_flow_pairs
from . import tasks


class FlowIngestionTests(SimpleTestCase):
    def test_great_britain_and_ireland_are_included_using_settings_iso_codes(self):
        self.assertCountEqual(build_flow_pairs({"GB": "gb", "IE": "ie"}),
                              [("gb", "ie"), ("ie", "gb")])

    def run_fetch(self, **options):
        with patch.dict(os.environ, {"ENTSOE_TOKEN": "test-token"}):
            return call_command(
                "fetch_flows", start="2026-11-26T08:00:00Z", end="2026-11-26T09:00:00Z",
                stdout=StringIO(), stderr=StringIO(), **options,
            )

    def test_saves_border_before_requesting_next_completed_result(self):
        frame = pd.DataFrame([{"quantity_mw": 1509.6}])
        with patch("entsoe_api.management.commands.fetch_flows.save_flows_df", return_value=1) as save:
            def results(**kwargs):
                self.assertEqual(len(kwargs["pairs"]), 10)
                yield ("BG", "RO"), frame, None
                save.assert_called_once_with(frame)
                yield ("BG", "GR"), None, TimeoutError("securityToken=must-not-be-logged")

            with patch.object(EntsoePhysicalFlows, "iter_pairs", side_effect=results):
                with self.assertRaisesMessage(CommandError, "1 flow border requests failed"):
                    self.run_fetch(countries="BG", fail_on_error=True)
            save.assert_called_once_with(frame)

    def test_europe_fetch_includes_both_directions_of_bulgarian_borders(self):
        with patch.object(EntsoePhysicalFlows, "iter_pairs", return_value=iter(())) as fetch:
            self.run_fetch(all_eu=True)
        bg_eic = settings.ENTSOE_COUNTRY_TO_EICS["BG"]
        pairs = fetch.call_args.kwargs["pairs"]
        self.assertEqual(len(pairs), len(set(pairs)))
        self.assertEqual(sum(source == bg_eic for source, _ in pairs), 5)
        self.assertEqual(sum(destination == bg_eic for _, destination in pairs), 5)

    def test_fast_border_is_yielded_while_slow_border_is_still_running(self):
        slow_started, release_slow = Event(), Event()
        start = dt.datetime(2026, 11, 26, tzinfo=dt.timezone.utc)

        def get_range(client, src, dst, **kwargs):
            if src == "slow":
                slow_started.set()
                if not release_slow.wait(5):
                    raise TimeoutError("test did not release slow request")
            else:
                if not slow_started.wait(5):
                    raise TimeoutError("slow request did not start")
            return pd.DataFrame([{"source": src}])

        with patch.object(EntsoePhysicalFlows, "get_range", autospec=True, side_effect=get_range):
            results = EntsoePhysicalFlows.iter_pairs(
                "test-token", [("slow", "BG"), ("fast", "BG")], start,
                start + dt.timedelta(hours=1), workers=2,
            )
            try:
                pair, frame, error = next(results)
                self.assertEqual(pair, ("fast", "BG"))
                self.assertIsNone(error)
                self.assertEqual(frame.iloc[0]["source"], "fast")
            finally:
                release_slow.set()
                results.close()

    @override_settings(
        ENTSOE_COUNTRY_TO_EICS={"DE": ["de1", "de2"], "FR": "fr", "BG": "bg", "RO": "ro"},
        ENTSOE_NEIGHBORS_BY_COUNTRY={"DE": {"FR"}, "FR": {"DE"}, "BG": {"RO"}, "RO": {"BG"}},
    )
    def test_dispatch_covers_all_countries_and_zones_without_duplicate_pending_jobs(self):
        pending = {}
        redis = Mock()

        def reserve(key, token, **kwargs):
            if key in pending:
                return False
            pending[key] = token
            return True

        redis.set.side_effect = reserve
        with patch("entsoe_api.tasks._flow_redis", return_value=redis), \
                patch.object(tasks.fetch_flow_border_task, "apply_async") as enqueue:
            tasks.fetch_flows_hourly_task.run()
            tasks.fetch_flows_hourly_task.run()
            pairs = [tuple(call.kwargs["args"][:2]) for call in enqueue.call_args_list]
            self.assertCountEqual(pairs, [("de1", "fr"), ("fr", "de1"), ("de2", "fr"),
                                          ("fr", "de2"), ("bg", "ro"), ("ro", "bg")])

    def test_failed_enqueue_releases_reservation_for_next_dispatch(self):
        redis = Mock()
        redis.set.return_value = True
        with patch("entsoe_api.tasks._flow_redis", return_value=redis), \
                patch.object(tasks.fetch_flow_border_task, "apply_async", side_effect=RuntimeError("broker")), \
                patch("entsoe_api.management.commands.fetch_flows.build_flow_pairs", return_value=[("DE", "FR")]):
            with self.assertRaisesMessage(RuntimeError, "broker"):
                tasks.fetch_flows_hourly_task.run()
        redis.eval.assert_called_once()
        self.assertEqual(redis.eval.call_args.args[2], tasks._flow_key("DE", "FR"))
        redis.close.assert_called_once()

    def test_border_saves_immediately_using_window_at_execution_time(self):
        redis = Mock()
        redis.get.return_value = "owner"
        now = dt.datetime(2026, 11, 26, 12, 23, tzinfo=dt.timezone.utc)
        with patch("entsoe_api.tasks._flow_redis", return_value=redis), \
                patch("entsoe_api.entsoe_data.EntsoePhysicalFlows") as client_class, \
                patch("entsoe_api.helper.save_flows_df", return_value=48) as save, \
                patch("entsoe_api.tasks.datetime") as clock, \
                patch.dict(os.environ, {"ENTSOE_TOKEN": "test-token"}):
            clock.now.return_value = now
            tasks.fetch_flow_border_task.run("DE", "FR", "owner")
            client = client_class.return_value
            client.get_range.assert_called_once_with(
                "DE", "FR", dt.datetime(2026, 11, 24, 13, tzinfo=dt.timezone.utc),
                dt.datetime(2026, 11, 26, 13, tzinfo=dt.timezone.utc),
            )
            save.assert_called_once_with(client.get_range.return_value)
            client.session.close.assert_called_once()
            redis.eval.assert_called_once()
            redis.lock.return_value.release.assert_called_once()

    def test_failed_border_releases_its_reservation_without_leaking_token(self):
        redis = Mock()
        redis.get.return_value = "owner"
        with patch("entsoe_api.tasks._flow_redis", return_value=redis), \
                patch("entsoe_api.entsoe_data.EntsoePhysicalFlows") as client_class, \
                patch("entsoe_api.helper.save_flows_df") as save, \
                patch.dict(os.environ, {"ENTSOE_TOKEN": "secret-token"}):
            client_class.return_value.get_range.side_effect = TimeoutError("securityToken=secret-token")
            with self.assertRaises(RuntimeError) as error:
                tasks.fetch_flow_border_task.run("DE", "FR", "owner")
            self.assertNotIn("secret-token", str(error.exception))
            save.assert_not_called()
            redis.eval.assert_called_once()
            redis.lock.return_value.release.assert_called_once()
            client_class.return_value.session.close.assert_called_once()

    def test_duplicate_delivery_does_not_start_concurrent_border_request(self):
        redis = Mock()
        redis.lock.return_value.acquire.return_value = False
        with patch("entsoe_api.tasks._flow_redis", return_value=redis), \
                patch("entsoe_api.entsoe_data.EntsoePhysicalFlows") as client_class:
            tasks.fetch_flow_border_task.run("DE", "FR", "owner")
            client_class.assert_not_called()
            redis.eval.assert_not_called()
            redis.lock.return_value.release.assert_not_called()

    def test_old_delivery_does_not_delete_newer_reservation(self):
        redis = Mock()
        redis.get.return_value = "new-owner"
        with patch("entsoe_api.tasks._flow_redis", return_value=redis), \
                patch("entsoe_api.entsoe_data.EntsoePhysicalFlows") as client_class:
            tasks.fetch_flow_border_task.run("DE", "FR", "old-owner")
            client_class.assert_not_called()
            redis.eval.assert_not_called()
            redis.lock.return_value.release.assert_called_once()

    def test_europe_schedule_uses_dedicated_dispatch_queue_and_expires_stale_ticks(self):
        schedule = settings.CELERY_BEAT_SCHEDULE["fetch_flows_hourly"]
        self.assertEqual(schedule["options"], {"queue": "flows_dispatch", "expires": 240})
        self.assertEqual(schedule["schedule"].minute, set(range(0, 60, 5)))
        self.assertNotIn("fetch_flows_bg", settings.CELERY_BEAT_SCHEDULE)
