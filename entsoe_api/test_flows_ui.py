import csv
import datetime as dt
from io import StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import SimpleTestCase, TestCase, override_settings

from .flows_ui import day_request, fetch_day, to_flow_records, to_rows
from .models import Country, PhysicalFlow


def sample(points=None, resolution="PT60M"):
    return {
        "metaData": [{"code": name} for name in ["NET_FLOW", "EXPORT_FLOW", "COMMENTS", "NETTED_EXPORT_FLOW"]],
        "instanceList": [{
            "businessDimensionMap": {"OUT_AREA": "BZN|BG", "IN_AREA": "BZN|MK"},
            "curveData": {"curveType": "A01", "periodList": [{
                "timeInterval": {"from": "2026-11-26T23:00:00Z", "to": "2026-11-27T23:00:00Z"},
                "resolution": resolution,
                "pointMap": points if points is not None else {
                    "1": ["-13.83", "3.86", None, "0.00"],
                    "0": ["0.00", "0.00", None, "0.00"],
                    "2": [{"alt": "-"}, {"alt": "-"}, None, {"alt": "-"}],
                },
            }]},
        }],
    }


class FlowsUiTests(SimpleTestCase):
    def test_request_uses_ui_timezone_including_dst_day_lengths(self):
        for day, start, end in [
            ("2026-11-27", "2026-11-26T23:00:00.000Z", "2026-11-27T23:00:00.000Z"),
            ("2026-03-29", "2026-03-28T23:00:00.000Z", "2026-03-29T22:00:00.000Z"),
            ("2026-10-25", "2026-10-24T22:00:00.000Z", "2026-10-25T23:00:00.000Z"),
        ]:
            with self.subTest(day=day):
                body = day_request(dt.date.fromisoformat(day), ["BZN|BG", "BZN|BG"])
                self.assertEqual(body["dateTimeRange"], {"from": start, "to": end})
                self.assertEqual(body["areaList"], ["BZN|BG"])

    def test_parsing_uses_metadata_zero_based_indices_and_preserves_missing_and_zero(self):
        rows = list(to_rows(sample()))
        self.assertEqual([row["utc_start"] for row in rows],
                         ["2026-11-26T23:00:00Z", "2026-11-27T00:00:00Z", "2026-11-27T01:00:00Z"])
        self.assertEqual(rows[0]["EXPORT_FLOW"], 0)
        self.assertEqual(rows[1]["EXPORT_FLOW"], 3.86)
        self.assertEqual(rows[1]["NET_FLOW"], -13.83)
        self.assertEqual(rows[1]["NETTED_EXPORT_FLOW"], 0)
        self.assertIsNone(rows[2]["EXPORT_FLOW"])

    def test_quarter_hour_positions(self):
        rows = list(to_rows(sample(resolution="PT15M")))
        self.assertEqual(rows[1]["utc_start"], "2026-11-26T23:15:00Z")

    def test_unknown_resolution_and_misaligned_columns_fail_explicitly(self):
        with self.assertRaisesMessage(ValueError, "Unsupported flow resolution"):
            list(to_rows(sample(resolution="PT5M")))
        with self.assertRaisesMessage(ValueError, "do not match"):
            list(to_rows(sample(points={"0": [1, 2]})))

    @patch("entsoe_api.flows_ui.requests.post")
    def test_fetch_uses_no_auth_or_cookies_and_surfaces_source_errors(self, post):
        post.return_value.json.return_value = sample()
        self.assertEqual(fetch_day(dt.date(2026, 11, 27), ["BZN|BG"]), sample())
        self.assertEqual(set(post.call_args.kwargs), {"json", "headers", "timeout"})
        self.assertEqual(post.call_args.kwargs["timeout"], 30)
        post.return_value.json.return_value = {**sample(), "uuAppErrorMap": {"invalidArea": {"type": "error"}}}
        with self.assertRaisesMessage(ValueError, "invalidArea"):
            fetch_day(dt.date(2026, 11, 27), ["BZN|bad"])

    @patch("entsoe_api.management.commands.fetch_flows_ui.fetch_day")
    def test_command_exports_csv_and_raw_response_without_accessing_database(self, fetch):
        fetch.return_value = sample()
        with TemporaryDirectory() as temp:
            output, raw = Path(temp) / "flows.csv", Path(temp) / "raw.json"
            stream = StringIO()
            call_command("fetch_flows_ui", date="2026-11-27", countries="BG,RO",
                         output=str(output), raw_output=str(raw), dry_run=True, stdout=stream)
            with output.open(newline="", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[2]["EXPORT_FLOW"], "")
            self.assertEqual(json.loads(raw.read_text(encoding="utf-8")), sample())
            self.assertIn("2 published export values", stream.getvalue())
            self.assertIn("Latest published interval start (UTC): 2026-11-27T00:00:00Z", stream.getvalue())
            self.assertEqual(len(fetch.call_args.args[1]), 2)

    @patch("entsoe_api.management.commands.fetch_flows_ui.fetch_day")
    def test_empty_response_exports_csv_header(self, fetch):
        fetch.return_value = {"metaData": [{"code": "EXPORT_FLOW"}], "instanceList": []}
        with TemporaryDirectory() as temp:
            path = Path(temp) / "empty.csv"
            call_command("fetch_flows_ui", output=str(path), dry_run=True, stdout=StringIO())
            self.assertIn("EXPORT_FLOW", path.read_text())

    @patch("entsoe_api.management.commands.fetch_flows_ui.fetch_day")
    def test_invalid_country_does_not_issue_http_request(self, fetch):
        with self.assertRaisesMessage(CommandError, "Unknown country"):
            call_command("fetch_flows_ui", countries="INVALID")
        fetch.assert_not_called()

    @patch("entsoe_api.flows_ui.requests.post")
    def test_partial_page_is_not_reported_as_complete(self, post):
        post.return_value.json.return_value = {**sample(), "intervalPageInfo": {"total": 10}}
        with self.assertRaisesMessage(ValueError, "partial page"):
            fetch_day(dt.date(2026, 11, 27), ["BZN|BG"])

    def test_conflicting_duplicate_rows_are_rejected_before_saving(self):
        row = list(to_rows(sample()))[0]
        with self.assertRaisesMessage(ValueError, "Conflicting"):
            to_flow_records([row, {**row, "EXPORT_FLOW": 10}])
        self.assertEqual(len(to_flow_records([row, row])), 1)


@override_settings(ENTSOE_COUNTRY_TO_EICS={"BG": "10YCA-BULGARIA-R", "MK": "10YMK-MEPSO----8"})
class FlowsUiImportTests(TestCase):
    def setUp(self):
        Country.objects.create(iso_code="BG", name="Bulgaria")
        Country.objects.create(iso_code="MK", name="North Macedonia")
        self.data = sample()
        self.data["instanceList"][0]["businessDimensionMap"] = {
            "OUT_AREA": "BZN|10YCA-BULGARIA-R", "IN_AREA": "BZN|10YMK-MEPSO----8",
        }

    def fetch(self, **options):
        with patch("entsoe_api.management.commands.fetch_flows_ui.fetch_day", return_value=self.data):
            call_command("fetch_flows_ui", date="2026-11-27", countries="BG", limit=0,
                         stdout=StringIO(), **options)

    def test_default_import_stores_exports_and_country_links_and_is_idempotent(self):
        self.fetch()
        self.assertEqual(PhysicalFlow.objects.count(), 2)
        row = PhysicalFlow.objects.get(datetime_utc="2026-11-27T00:00:00Z")
        self.assertEqual(row.quantity_mw, 3.86)  # Not signed NET_FLOW or NETTED_EXPORT_FLOW.
        self.assertEqual(row.country_from_id, "BG")
        self.assertEqual(row.country_to_id, "MK")
        self.assertEqual(row.resolution, "PT60M")
        self.assertEqual(PhysicalFlow.objects.get(datetime_utc="2026-11-26T23:00:00Z").quantity_mw, 0)

        self.data["instanceList"][0]["curveData"]["periodList"][0]["pointMap"]["1"][1] = "8.5"
        self.fetch()
        row.refresh_from_db()
        self.assertEqual(row.quantity_mw, 8.5)
        self.assertEqual(PhysicalFlow.objects.count(), 2)

    def test_missing_publication_does_not_overwrite_existing_reading(self):
        self.fetch()
        self.data["instanceList"][0]["curveData"]["periodList"][0]["pointMap"]["1"][1] = {"alt": "-"}
        self.fetch()
        self.assertEqual(PhysicalFlow.objects.get(datetime_utc="2026-11-27T00:00:00Z").quantity_mw, 3.86)
        self.assertEqual(PhysicalFlow.objects.count(), 2)

    def test_dry_run_does_not_write_records(self):
        self.fetch(dry_run=True)
        self.assertFalse(PhysicalFlow.objects.exists())

    def test_invalid_batch_is_not_partially_imported(self):
        self.data["instanceList"][0]["curveData"]["periodList"][0]["pointMap"]["1"][1] = "-5"
        with self.assertRaisesMessage(CommandError, "non-negative"):
            self.fetch()
        self.assertFalse(PhysicalFlow.objects.exists())
