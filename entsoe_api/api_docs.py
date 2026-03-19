from rest_framework import serializers

from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiExample, OpenApiParameter, OpenApiResponse

from .serializers import (
    CountryGenerationForecastByTypeSerializer,
    CountryResGenerationByTypeSerializer,
    CountryTiltedIrradiancePointSerializer,
    PhysicalFlowSerializer,
)


class ErrorResponseSerializer(serializers.Serializer):
    detail = serializers.CharField(help_text="Validation or request error message.")


BAD_REQUEST_RESPONSE = OpenApiResponse(
    response=ErrorResponseSerializer,
    description="The request parameters were missing, invalid, or internally inconsistent.",
)


INVALID_COUNTRY_EXAMPLE = OpenApiExample(
    "Invalid country",
    value={"detail": "Unknown country 'XX'. Make sure it's loaded in the DB."},
    response_only=True,
    status_codes=["400"],
)


INVALID_RANGE_EXAMPLE = OpenApiExample(
    "Invalid range",
    value={
        "detail": (
            "Provide start & end (ISO UTC, e.g. 2025-09-18T00:00:00Z) "
            "or use period=today|yesterday|dayahead."
        )
    },
    response_only=True,
    status_codes=["400"],
)


def country_parameter(*, required: bool = True, description: str | None = None) -> OpenApiParameter:
    return OpenApiParameter(
        name="country",
        type=OpenApiTypes.STR,
        location=OpenApiParameter.QUERY,
        required=required,
        description=description or "Two-letter ISO country code, for example `BG` or `DE`.",
    )


def countries_parameter(*, required: bool = True, max_items: int | None = None) -> OpenApiParameter:
    max_note = f" Maximum {max_items} countries per request." if max_items else ""
    return OpenApiParameter(
        name="countries",
        type=OpenApiTypes.STR,
        location=OpenApiParameter.QUERY,
        required=required,
        description=f"Comma-separated ISO country codes such as `BG,RO,GR`.{max_note}",
    )


def psr_parameter(*, required: bool = False, allow_multiple: bool = False) -> OpenApiParameter:
    suffix = " Comma-separated values are supported." if allow_multiple else ""
    return OpenApiParameter(
        name="psr",
        type=OpenApiTypes.STR,
        location=OpenApiParameter.QUERY,
        required=required,
        description=f"ENTSO-E production type code such as `B16` (solar) or `B18` (wind).{suffix}",
    )


def period_parameter(*, allow_yesterday: bool = True, allow_dayahead: bool = False) -> OpenApiParameter:
    values = ["today"]
    if allow_yesterday:
        values.append("yesterday")
    if allow_dayahead:
        values.append("dayahead")
    values_label = "|".join(values)
    return OpenApiParameter(
        name="period",
        type=OpenApiTypes.STR,
        location=OpenApiParameter.QUERY,
        required=False,
        enum=values,
        description=f"Shortcut for a predefined UTC window. Supported values: `{values_label}`.",
    )


START_PARAMETER = OpenApiParameter(
    name="start",
    type=OpenApiTypes.DATETIME,
    location=OpenApiParameter.QUERY,
    required=False,
    description="Inclusive range start in ISO-8601 UTC, for example `2026-03-01T00:00:00Z`.",
)


END_PARAMETER = OpenApiParameter(
    name="end",
    type=OpenApiTypes.DATETIME,
    location=OpenApiParameter.QUERY,
    required=False,
    description="Exclusive range end in ISO-8601 UTC unless noted otherwise, for example `2026-03-02T00:00:00Z`.",
)


LOCAL_PARAMETER = OpenApiParameter(
    name="local",
    type=OpenApiTypes.BOOL,
    location=OpenApiParameter.QUERY,
    required=False,
    description="When `true`, compute `yesterday` using the Django project timezone instead of UTC.",
)


RESOLUTION_PARAMETER = OpenApiParameter(
    name="resolution",
    type=OpenApiTypes.STR,
    location=OpenApiParameter.QUERY,
    required=False,
    enum=["d", "m", "y"],
    description="Aggregate results by day (`d`), month (`m`), or year (`y`). Omit for native source resolution.",
)


TILT_PARAMETER = OpenApiParameter(
    name="tilt",
    type=OpenApiTypes.NUMBER,
    location=OpenApiParameter.QUERY,
    required=False,
    description="Panel tilt in degrees. Defaults to `30`.",
)


AZIMUTH_PARAMETER = OpenApiParameter(
    name="azimuth",
    type=OpenApiTypes.NUMBER,
    location=OpenApiParameter.QUERY,
    required=False,
    description="Panel azimuth in degrees. Defaults to `0` (south-facing).",
)


MONTHLY_FLAG_PARAMETER = OpenApiParameter(
    name="m",
    type=OpenApiTypes.BOOL,
    location=OpenApiParameter.QUERY,
    required=False,
    description="Legacy shortcut for monthly aggregation. Equivalent to `resolution=m`.",
)


CONTRACT_PARAMETER = OpenApiParameter(
    name="contract",
    type=OpenApiTypes.STR,
    location=OpenApiParameter.QUERY,
    required=False,
    enum=["A01", "A07"],
    description="ENTSO-E contract type. `A01` is day-ahead and `A07` is intraday.",
)


FLOW_FROM_PARAMETER = OpenApiParameter(
    name="from",
    type=OpenApiTypes.STR,
    location=OpenApiParameter.QUERY,
    required=False,
    description="Filter by source country ISO code.",
)


FLOW_TO_PARAMETER = OpenApiParameter(
    name="to",
    type=OpenApiTypes.STR,
    location=OpenApiParameter.QUERY,
    required=False,
    description="Filter by destination country ISO code.",
)


NEIGHBORS_PARAMETER = OpenApiParameter(
    name="neighbors",
    type=OpenApiTypes.BOOL,
    location=OpenApiParameter.QUERY,
    required=False,
    description="When `true`, include inbound, outbound, and net totals grouped by neighboring country.",
)


class ApiRootResponseSerializer(serializers.Serializer):
    capacity_latest = serializers.URLField()
    capacity_bulk_latest = serializers.URLField()
    generation_yesterday = serializers.URLField()
    prices_range = serializers.URLField()
    price_bulk = serializers.URLField()
    generation_range = serializers.URLField()
    generation_res_range = serializers.URLField()
    generation_bulk_range = serializers.URLField()
    generation_forecast_range = serializers.URLField()
    generation_irradiance_range = serializers.URLField()
    generation_irradiance_bulk_range = serializers.URLField()
    flows_range = serializers.URLField()
    flows_latest = serializers.URLField()
    schema = serializers.URLField()
    swagger_ui = serializers.URLField()
    redoc = serializers.URLField()


class CapacityItemSerializer(serializers.Serializer):
    psr_type = serializers.CharField()
    psr_name = serializers.CharField()
    installed_capacity_mw = serializers.DecimalField(max_digits=14, decimal_places=3, allow_null=True)


class CapacityLatestResponseSerializer(serializers.Serializer):
    country = serializers.CharField()
    year = serializers.IntegerField(allow_null=True)
    items = CapacityItemSerializer(many=True)


class CapacityBulkRequestInfoSerializer(serializers.Serializer):
    countries_requested = serializers.ListField(child=serializers.CharField())
    countries_found = serializers.ListField(child=serializers.CharField())
    countries_ignored = serializers.ListField(child=serializers.CharField())
    psr = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    total_countries = serializers.IntegerField()
    total_records = serializers.IntegerField()
    server_elapsed_ms = serializers.FloatField()


class CapacityBulkResponseSerializer(serializers.Serializer):
    request_info = CapacityBulkRequestInfoSerializer()
    data = serializers.JSONField(
        help_text="Object keyed by country ISO code. Each value matches the single-country capacity response."
    )


class PriceItemSerializer(serializers.Serializer):
    datetime_utc = serializers.DateTimeField()
    price = serializers.DecimalField(max_digits=14, decimal_places=6, allow_null=True)
    currency = serializers.CharField()
    unit = serializers.CharField()
    resolution = serializers.CharField()


class PriceRangeResponseSerializer(serializers.Serializer):
    country = serializers.CharField()
    contract_type = serializers.CharField()
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    items = PriceItemSerializer(many=True)


class PriceBulkRequestInfoSerializer(serializers.Serializer):
    countries_requested = serializers.ListField(child=serializers.CharField())
    countries_found = serializers.ListField(child=serializers.CharField())
    countries_ignored = serializers.ListField(child=serializers.CharField())
    contract_type = serializers.CharField()
    resolution = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    total_countries = serializers.IntegerField()
    total_records = serializers.IntegerField()
    server_elapsed_ms = serializers.FloatField()


class PriceBulkResponseSerializer(serializers.Serializer):
    request_info = PriceBulkRequestInfoSerializer()
    data = serializers.JSONField(
        help_text="Object keyed by country ISO code. Each value matches the single-country price response."
    )


class GenerationItemSerializer(serializers.Serializer):
    datetime_utc = serializers.DateTimeField()
    psr_type = serializers.CharField()
    psr_name = serializers.CharField()
    generation_mw = serializers.DecimalField(max_digits=14, decimal_places=3, allow_null=True)
    resolution = serializers.CharField(required=False, allow_blank=True)


class GenerationRangeResponseSerializer(serializers.Serializer):
    country = serializers.CharField()
    date_label = serializers.CharField()
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    items = GenerationItemSerializer(many=True)


class GenerationBulkRequestInfoSerializer(serializers.Serializer):
    countries_requested = serializers.ListField(child=serializers.CharField())
    countries_found = serializers.ListField(child=serializers.CharField())
    countries_ignored = serializers.ListField(child=serializers.CharField())
    psr = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    resolution = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    date_label = serializers.CharField()
    total_countries = serializers.IntegerField()
    total_records = serializers.IntegerField()


class GenerationBulkResponseSerializer(serializers.Serializer):
    request_info = GenerationBulkRequestInfoSerializer()
    data = serializers.JSONField(
        help_text="Object keyed by country ISO code. Each value matches the single-country generation range shape."
    )


class GenerationForecastResponseSerializer(serializers.Serializer):
    country = serializers.CharField()
    date_label = serializers.CharField()
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    items = CountryGenerationForecastByTypeSerializer(many=True)


class TiltedIrradianceResponseSerializer(serializers.Serializer):
    country = serializers.CharField()
    date_label = serializers.CharField()
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    tilt_degrees = serializers.FloatField()
    azimuth_degrees = serializers.FloatField()
    items = CountryTiltedIrradiancePointSerializer(many=True)


class TiltedIrradianceBulkRequestInfoSerializer(serializers.Serializer):
    countries_requested = serializers.ListField(child=serializers.CharField())
    countries_found = serializers.ListField(child=serializers.CharField())
    countries_ignored = serializers.ListField(child=serializers.CharField())
    tilt_degrees = serializers.FloatField()
    azimuth_degrees = serializers.FloatField()
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    date_label = serializers.CharField()
    total_countries = serializers.IntegerField()
    total_records = serializers.IntegerField()


class TiltedIrradianceBulkResponseSerializer(serializers.Serializer):
    request_info = TiltedIrradianceBulkRequestInfoSerializer()
    data = serializers.JSONField(
        help_text="Object keyed by country ISO code. Each value matches the single-country irradiance response shape."
    )


class ResGenerationResponseSerializer(serializers.Serializer):
    country = serializers.CharField()
    date_label = serializers.CharField()
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    items = CountryResGenerationByTypeSerializer(many=True)


class PhysicalFlowsRangeResponseSerializer(serializers.Serializer):
    start_utc = serializers.DateTimeField()
    end_utc = serializers.DateTimeField()
    count = serializers.IntegerField()
    items = PhysicalFlowSerializer(many=True)


class FlowTotalsSerializer(serializers.Serializer):
    in_mw = serializers.FloatField()
    out_mw = serializers.FloatField()
    net_mw = serializers.FloatField()


class NeighborFlowSerializer(serializers.Serializer):
    neighbor = serializers.CharField()
    in_mw = serializers.FloatField()
    out_mw = serializers.FloatField()
    net_mw = serializers.FloatField()


class PhysicalFlowsLatestResponseSerializer(serializers.Serializer):
    country = serializers.CharField()
    start_utc = serializers.DateTimeField(required=False, allow_null=True)
    end_utc = serializers.DateTimeField(required=False, allow_null=True)
    count = serializers.IntegerField(required=False)
    items = PhysicalFlowSerializer(many=True)
    totals = FlowTotalsSerializer()
    neighbors = NeighborFlowSerializer(many=True, required=False)
