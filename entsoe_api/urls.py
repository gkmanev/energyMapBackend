# entsoe_api/urls.py
from django.urls import path
from .views import CountryCapacityLatestView, CountryGenerationYesterdayView, api_root, CountryPricesRangeView, CountryPricesBulkRangeView

urlpatterns = [
    path("api/", api_root, name="api-root"),
    path("api/capacity/latest/", CountryCapacityLatestView.as_view(), name="capacity-latest"),
    path("api/generation/yesterday/", CountryGenerationYesterdayView.as_view(), name="generation-yesterday"),
    # PRICES (UTC-only, supports period=today|dayahead or start/end)
    path("api/prices/range/", CountryPricesRangeView.as_view(), name="prices-range"),
    path('api/prices/bulk-range/', CountryPricesBulkRangeView.as_view(), name='country-prices-bulk-range'),
]
