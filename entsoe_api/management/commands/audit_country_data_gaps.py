from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.db.models import Count, Max, Min, Q

from entsoe_api.models import (
    Country,
    CountryCapacitySnapshot,
    CountryGenerationByType,
    CountryGenerationForecastByType,
    CountryPricePoint,
    CountryResGenerationByType,
    CountryTiltedIrradiancePoint,
    CountryWindSpeedPoint,
    PhysicalFlow,
)


STATUS_PRIORITY = {
    "table_missing": 0,
    "table_empty": 1,
    "no_rows": 2,
    "partial": 3,
    "ok": 4,
}


@dataclass(frozen=True)
class DatasetSpec:
    key: str
    label: str
    model: type
    applicable_source: str
    stat_kind: str
    unit_field: str | None = None
    time_field: str | None = None
    extra_filters: dict[str, Any] | None = None

    @property
    def table_name(self) -> str:
        return self.model._meta.db_table


DATASET_SPECS = [
    DatasetSpec(
        key="country",
        label="Country dimension",
        model=Country,
        applicable_source="all",
        stat_kind="dimension",
    ),
    DatasetSpec(
        key="capacity",
        label="Installed capacity snapshots",
        model=CountryCapacitySnapshot,
        applicable_source="entsoe",
        stat_kind="country_year",
        unit_field="year",
        time_field="year",
    ),
    DatasetSpec(
        key="generation",
        label="Actual generation",
        model=CountryGenerationByType,
        applicable_source="entsoe",
        stat_kind="country_ts",
        unit_field="datetime_utc",
        time_field="datetime_utc",
    ),
    DatasetSpec(
        key="res_generation",
        label="RES generation",
        model=CountryResGenerationByType,
        applicable_source="entsoe",
        stat_kind="country_ts",
        unit_field="datetime_utc",
        time_field="datetime_utc",
    ),
    DatasetSpec(
        key="generation_forecast",
        label="Generation forecast",
        model=CountryGenerationForecastByType,
        applicable_source="entsoe",
        stat_kind="country_ts",
        unit_field="datetime_utc",
        time_field="datetime_utc",
    ),
    DatasetSpec(
        key="price_day_ahead",
        label="Day-ahead prices",
        model=CountryPricePoint,
        applicable_source="price",
        stat_kind="country_ts",
        unit_field="datetime_utc",
        time_field="datetime_utc",
        extra_filters={"contract_type": "A01"},
    ),
    DatasetSpec(
        key="price_intraday",
        label="Intraday prices",
        model=CountryPricePoint,
        applicable_source="price",
        stat_kind="country_ts",
        unit_field="datetime_utc",
        time_field="datetime_utc",
        extra_filters={"contract_type": "A07"},
    ),
    DatasetSpec(
        key="irradiance",
        label="Tilted irradiance",
        model=CountryTiltedIrradiancePoint,
        applicable_source="coords",
        stat_kind="country_ts",
        unit_field="datetime_utc",
        time_field="datetime_utc",
    ),
    DatasetSpec(
        key="wind_speed",
        label="Wind speed",
        model=CountryWindSpeedPoint,
        applicable_source="coords",
        stat_kind="country_ts",
        unit_field="datetime_utc",
        time_field="datetime_utc",
    ),
    DatasetSpec(
        key="flows",
        label="Physical flows",
        model=PhysicalFlow,
        applicable_source="entsoe",
        stat_kind="flows",
        unit_field="datetime_utc",
        time_field="datetime_utc",
    ),
]


def _normalize_country_codes(items: list[str] | tuple[str, ...] | set[str] | None) -> set[str]:
    codes: set[str] = set()
    if items is None:
        return codes
    for item in items:
        code = str(item or "").strip().upper()
        if code:
            codes.add(code)
    return codes


def _country_codes_from_mapping(setting_name: str) -> set[str]:
    raw = getattr(settings, setting_name, None)
    if not isinstance(raw, dict):
        return set()
    return _normalize_country_codes(list(raw.keys()))


def _country_codes_from_coords() -> set[str]:
    raw = getattr(settings, "COUNTRY_COORDS", None)
    if not isinstance(raw, list):
        return set()
    return _normalize_country_codes(
        [item.get("code") for item in raw if isinstance(item, dict)]
    )


def _serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


class Command(BaseCommand):
    help = (
        "Inspect country-related tables and rank which datasets are most incomplete "
        "for each country. Coverage is normalized per dataset against the best-covered "
        "country in that same dataset."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--countries",
            type=str,
            help="Comma-separated ISO country codes, for example BG,RO,GR. Defaults to all applicable countries.",
        )
        parser.add_argument(
            "--datasets",
            type=str,
            help=(
                "Optional comma-separated dataset keys: "
                + ", ".join(spec.key for spec in DATASET_SPECS)
            ),
        )
        parser.add_argument(
            "--top",
            type=int,
            default=3,
            help="How many worst datasets to show per country in text mode or non-full JSON output (default: 3).",
        )
        parser.add_argument(
            "--full",
            action="store_true",
            help="Include every selected dataset for each country instead of only the worst ones.",
        )
        parser.add_argument(
            "--format",
            choices=["text", "json"],
            default="text",
            help="Output format (default: text).",
        )
        parser.add_argument(
            "--year",
            type=int,
            help="Optional UTC calendar year to audit, for example 2025.",
        )

    def handle(self, *args, **options):
        specs = self._select_specs(options.get("datasets"))
        table_names = set(connection.introspection.table_names())
        selected_year = options.get("year")
        selected_countries = self._select_countries(
            options.get("countries"),
            specs,
            table_names,
        )
        if not selected_countries:
            raise CommandError("No countries matched the selected datasets.")

        report = self._build_report(
            specs=specs,
            countries=selected_countries,
            table_names=table_names,
            top=max(options.get("top", 3), 1),
            include_all=bool(options.get("full")),
            selected_year=selected_year,
        )

        if options.get("format") == "json":
            self.stdout.write(json.dumps(report, indent=2))
            return

        self._write_text_report(report)

    def _select_specs(self, raw_value: str | None) -> list[DatasetSpec]:
        if not raw_value:
            return list(DATASET_SPECS)

        wanted = [part.strip() for part in raw_value.split(",") if part.strip()]
        by_key = {spec.key: spec for spec in DATASET_SPECS}
        unknown = [key for key in wanted if key not in by_key]
        if unknown:
            known = ", ".join(sorted(by_key))
            raise CommandError(f"Unknown dataset key(s): {', '.join(unknown)}. Known: {known}")
        return [by_key[key] for key in wanted]

    def _select_countries(
        self,
        raw_value: str | None,
        specs: list[DatasetSpec],
        table_names: set[str],
    ) -> list[str]:
        applicable = self._all_applicable_countries(specs, table_names)
        if not raw_value:
            return sorted(applicable)

        requested = _normalize_country_codes(raw_value.split(","))
        unknown = sorted(requested - applicable)
        if unknown:
            known = ", ".join(sorted(applicable))
            raise CommandError(
                f"Unknown or not-applicable country code(s): {', '.join(unknown)}. "
                f"Known for the selected datasets: {known}"
            )
        return sorted(requested)

    def _all_applicable_countries(
        self,
        specs: list[DatasetSpec],
        table_names: set[str],
    ) -> set[str]:
        countries: set[str] = set()
        for spec in specs:
            countries.update(self._applicable_countries_for_spec(spec, table_names))
        return countries

    def _applicable_countries_for_spec(
        self,
        spec: DatasetSpec,
        table_names: set[str],
    ) -> set[str]:
        country_table_present = Country._meta.db_table in table_names
        by_source = {
            "all": (
                _country_codes_from_mapping("ENTSOE_COUNTRY_TO_EICS")
                | _country_codes_from_mapping("ENTSOE_PRICE_COUNTRY_TO_EICS")
                | _country_codes_from_coords()
                | (
                    _normalize_country_codes(Country.objects.values_list("iso_code", flat=True))
                    if country_table_present
                    else set()
                )
            ),
            "entsoe": _country_codes_from_mapping("ENTSOE_COUNTRY_TO_EICS"),
            "price": _country_codes_from_mapping("ENTSOE_PRICE_COUNTRY_TO_EICS"),
            "coords": _country_codes_from_coords(),
        }
        countries = set(by_source.get(spec.applicable_source, set()))

        if spec.stat_kind == "flows":
            if spec.table_name in table_names:
                countries.update(
                    _normalize_country_codes(
                        PhysicalFlow.objects.values_list("country_from_id", flat=True)
                    )
                )
                countries.update(
                    _normalize_country_codes(
                        PhysicalFlow.objects.values_list("country_to_id", flat=True)
                    )
                )
            return countries

        if spec.stat_kind == "dimension":
            if country_table_present:
                countries.update(
                    _normalize_country_codes(Country.objects.values_list("iso_code", flat=True))
                )
            return countries

        if spec.table_name in table_names:
            countries.update(
                _normalize_country_codes(spec.model.objects.values_list("country_id", flat=True))
            )
        return countries

    def _build_report(
        self,
        *,
        specs: list[DatasetSpec],
        countries: list[str],
        table_names: set[str],
        top: int,
        include_all: bool,
        selected_year: int | None,
    ) -> dict[str, Any]:
        per_country: dict[str, list[dict[str, Any]]] = {country: [] for country in countries}

        for spec in specs:
            applicable = self._applicable_countries_for_spec(spec, table_names) & set(countries)
            if not applicable:
                continue

            dataset_rows = self._dataset_rows_for_spec(
                spec=spec,
                countries=sorted(applicable),
                table_names=table_names,
                selected_year=selected_year,
            )
            for row in dataset_rows:
                per_country[row["country"]].append(row)

        countries_payload = []
        for country in countries:
            dataset_rows = sorted(per_country[country], key=self._sort_key)
            visible_rows = dataset_rows if include_all else dataset_rows[:top]
            countries_payload.append(
                {
                    "country": country,
                    "datasets": visible_rows,
                    "dataset_count": len(dataset_rows),
                }
            )

        return {
            "countries": countries_payload,
            "selected_datasets": [spec.key for spec in specs],
            "top": top,
            "full": include_all,
            "year": selected_year,
        }

    def _dataset_rows_for_spec(
        self,
        *,
        spec: DatasetSpec,
        countries: list[str],
        table_names: set[str],
        selected_year: int | None,
    ) -> list[dict[str, Any]]:
        table_present = spec.table_name in table_names
        if not table_present:
            return [
                self._result_row(
                    country=country,
                    spec=spec,
                    status="table_missing",
                    units_observed=0,
                    best_units=0,
                    row_count=0,
                    first_seen=None,
                    last_seen=None,
                    table_present=False,
                )
                for country in countries
            ]

        collectors = {
            "dimension": self._collect_dimension_stats,
            "country_year": self._collect_country_year_stats,
            "country_ts": self._collect_country_ts_stats,
            "flows": self._collect_flow_stats,
        }
        stats = collectors[spec.stat_kind](spec, countries, selected_year)
        best_units = max((item["units_observed"] for item in stats.values()), default=0)
        table_empty = best_units == 0

        rows = []
        for country in countries:
            item = stats.get(
                country,
                {
                    "units_observed": 0,
                    "row_count": 0,
                    "first_seen": None,
                    "last_seen": None,
                },
            )
            if table_empty:
                status = "table_empty"
            elif item["units_observed"] == 0:
                status = "no_rows"
            elif item["units_observed"] < best_units:
                status = "partial"
            else:
                status = "ok"

            rows.append(
                self._result_row(
                    country=country,
                    spec=spec,
                    status=status,
                    units_observed=item["units_observed"],
                    best_units=best_units,
                    row_count=item["row_count"],
                    first_seen=item["first_seen"],
                    last_seen=item["last_seen"],
                    table_present=True,
                )
            )
        return rows

    def _collect_dimension_stats(
        self,
        spec: DatasetSpec,
        countries: list[str],
        selected_year: int | None,
    ) -> dict[str, dict[str, Any]]:
        existing = set(Country.objects.filter(iso_code__in=countries).values_list("iso_code", flat=True))
        stats: dict[str, dict[str, Any]] = {}
        for country in countries:
            stats[country] = {
                "units_observed": 1 if country in existing else 0,
                "row_count": 1 if country in existing else 0,
                "first_seen": None,
                "last_seen": None,
            }
        return stats

    def _collect_country_year_stats(
        self,
        spec: DatasetSpec,
        countries: list[str],
        selected_year: int | None,
    ) -> dict[str, dict[str, Any]]:
        queryset = spec.model.objects.filter(country_id__in=countries)
        if selected_year is not None:
            queryset = queryset.filter(year=selected_year)
        if spec.extra_filters:
            queryset = queryset.filter(**spec.extra_filters)

        rows = queryset.values("country_id").annotate(
            units_observed=Count(spec.unit_field, distinct=True),
            row_count=Count("pk"),
            first_seen=Min(spec.time_field),
            last_seen=Max(spec.time_field),
        )
        return {
            row["country_id"]: {
                "units_observed": row["units_observed"],
                "row_count": row["row_count"],
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen"],
            }
            for row in rows
        }

    def _collect_country_ts_stats(
        self,
        spec: DatasetSpec,
        countries: list[str],
        selected_year: int | None,
    ) -> dict[str, dict[str, Any]]:
        queryset = spec.model.objects.filter(country_id__in=countries)
        if selected_year is not None:
            year_start, year_end = self._year_bounds(selected_year)
            queryset = queryset.filter(
                **{
                    f"{spec.time_field}__gte": year_start,
                    f"{spec.time_field}__lt": year_end,
                }
            )
        if spec.extra_filters:
            queryset = queryset.filter(**spec.extra_filters)

        rows = queryset.values("country_id").annotate(
            units_observed=Count(spec.unit_field, distinct=True),
            row_count=Count("pk"),
            first_seen=Min(spec.time_field),
            last_seen=Max(spec.time_field),
        )
        return {
            row["country_id"]: {
                "units_observed": row["units_observed"],
                "row_count": row["row_count"],
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen"],
            }
            for row in rows
        }

    def _collect_flow_stats(
        self,
        spec: DatasetSpec,
        countries: list[str],
        selected_year: int | None,
    ) -> dict[str, dict[str, Any]]:
        stats: dict[str, dict[str, Any]] = {}
        year_start = year_end = None
        if selected_year is not None:
            year_start, year_end = self._year_bounds(selected_year)
        for country in countries:
            queryset = PhysicalFlow.objects.filter(
                Q(country_from_id=country) | Q(country_to_id=country)
            )
            if selected_year is not None:
                queryset = queryset.filter(
                    datetime_utc__gte=year_start,
                    datetime_utc__lt=year_end,
                )
            row_count = queryset.count()
            if row_count == 0:
                stats[country] = {
                    "units_observed": 0,
                    "row_count": 0,
                    "first_seen": None,
                    "last_seen": None,
                }
                continue

            bounds = queryset.aggregate(
                first_seen=Min("datetime_utc"),
                last_seen=Max("datetime_utc"),
            )
            stats[country] = {
                "units_observed": queryset.values("datetime_utc").distinct().count(),
                "row_count": row_count,
                "first_seen": bounds["first_seen"],
                "last_seen": bounds["last_seen"],
            }
        return stats

    def _result_row(
        self,
        *,
        country: str,
        spec: DatasetSpec,
        status: str,
        units_observed: int,
        best_units: int,
        row_count: int,
        first_seen: Any,
        last_seen: Any,
        table_present: bool,
    ) -> dict[str, Any]:
        coverage_ratio = float(units_observed / best_units) if best_units else 0.0
        return {
            "country": country,
            "dataset": spec.key,
            "label": spec.label,
            "table": spec.table_name,
            "status": status,
            "coverage_ratio": round(coverage_ratio, 6),
            "coverage_pct": round(coverage_ratio * 100, 2),
            "units_observed": units_observed,
            "best_units": best_units,
            "missing_units_to_best": max(best_units - units_observed, 0),
            "row_count": row_count,
            "first_seen": _serialize_value(first_seen),
            "last_seen": _serialize_value(last_seen),
            "table_present": table_present,
        }

    def _year_bounds(self, year: int) -> tuple[dt.datetime, dt.datetime]:
        return (
            dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc),
            dt.datetime(year + 1, 1, 1, tzinfo=dt.timezone.utc),
        )

    def _sort_key(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            STATUS_PRIORITY.get(row["status"], 99),
            row["coverage_ratio"],
            -row["missing_units_to_best"],
            row["dataset"],
        )

    def _write_text_report(self, report: dict[str, Any]) -> None:
        self.stdout.write(self.style.SUCCESS("Country data gap audit"))
        self.stdout.write(
            "Datasets: " + ", ".join(report["selected_datasets"])
        )
        if report["year"] is not None:
            self.stdout.write(f"Year: {report['year']}")
        self.stdout.write("")

        for country_payload in report["countries"]:
            country = country_payload["country"]
            rows = country_payload["datasets"]
            total = country_payload["dataset_count"]
            self.stdout.write(self.style.SUCCESS(f"{country} ({len(rows)}/{total} shown)"))
            for row in rows:
                self.stdout.write(
                    "  "
                    f"{row['dataset']:<20} "
                    f"{row['status']:<13} "
                    f"coverage={row['coverage_pct']:>6.2f}% "
                    f"units={row['units_observed']}/{row['best_units']} "
                    f"rows={row['row_count']}"
                )
                if row["first_seen"] or row["last_seen"]:
                    self.stdout.write(
                        "  "
                        f"{'':<20} "
                        f"range={row['first_seen']} -> {row['last_seen']}"
                    )
                if row["status"] == "table_missing":
                    self.stdout.write(
                        "  "
                        f"{'':<20} "
                        f"table={row['table']} is missing from the current database"
                    )
            self.stdout.write("")
