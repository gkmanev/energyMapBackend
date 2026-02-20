# entsoe_api/urls.py
from django.urls import path
from .views import (
    CountryCapacityBulkLatestView,
    CountryCapacityLatestView,
    CountryGenerationBulkRangeView,
    CountryGenerationForecastRangeView,
    CountryGenerationRangeView,
    CountryGenerationResRangeView,
    CountryGenerationYesterdayView,
    CountryPricesBulkRangeView,
    CountryPricesRangeView,
    PhysicalFlowsLatestView,
    PhysicalFlowsRangeView,
    api_root,
)

urlpatterns = [
    path("api/", api_root, name="api-root"),
    path("api/capacity/latest/", CountryCapacityLatestView.as_view(), name="capacity-latest"),
    path("api/capacity/bulk-latest/", CountryCapacityBulkLatestView.as_view(), name="capacity-bulk-latest"),
    path("api/generation/yesterday/", CountryGenerationYesterdayView.as_view(), name="generation-yesterday"),
    # PRICES (UTC-only, supports period=today|dayahead or start/end)
    path("api/prices/range/", CountryPricesRangeView.as_view(), name="prices-range"),
    path('api/prices/bulk-range/', CountryPricesBulkRangeView.as_view(), name='country-prices-bulk-range'),
    path('api/generation/range/', CountryGenerationRangeView.as_view(), name='generation-range'),
    path('api/generation-res/range/', CountryGenerationResRangeView.as_view(), name='generation-res-range'),
    path('api/generation/bulk-range/', CountryGenerationBulkRangeView.as_view(), name='generation-bulk-range'),
    path('api/generation-forecast/range/', CountryGenerationForecastRangeView.as_view(), name='generation-forecast-range'),
    path("api/flows/range/",  PhysicalFlowsRangeView.as_view(),  name="flows-range"),
    path("api/flows/latest/", PhysicalFlowsLatestView.as_view(), name="flows-latest"),
]


