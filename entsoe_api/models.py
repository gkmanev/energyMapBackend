# yourapp/models.py
from django.db import models

class Country(models.Model):
    iso_code = models.CharField(max_length=2, primary_key=True)  # e.g., "DE"
    name = models.CharField(max_length=64, blank=True, default="")

    class Meta:
        verbose_name_plural = "Countries"

    def __str__(self):
        return self.iso_code


class CountryCapacitySnapshot(models.Model):
    """
    A68/A33 annual snapshot aggregated at country level.
    One row per country × psr_type × year.
    """
    country = models.ForeignKey(Country, on_delete=models.PROTECT, related_name="capacity_country")
    psr_type = models.CharField(max_length=4)
    psr_name = models.CharField(max_length=64, blank=True, default="")
    installed_capacity_mw = models.DecimalField(max_digits=14, decimal_places=3, null=True, blank=True)

    # Snapshot metadata
    valid_from_utc = models.DateTimeField()
    year = models.PositiveSmallIntegerField()

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = (("country", "psr_type", "year"),)
        indexes = [
            models.Index(fields=["country", "year", "psr_type"]),
        ]

    def __str__(self):
        return f"{self.country_id} {self.psr_type} {self.year}"


class CountryGenerationByType(models.Model):
    """
    A75/A16 time series aggregated at country level.
    One row per country × psr_type × timestamp.
    """
    country = models.ForeignKey(Country, on_delete=models.PROTECT, related_name="generation_country")

    datetime_utc = models.DateTimeField(db_index=True)
    psr_type = models.CharField(max_length=4)
    psr_name = models.CharField(max_length=64, blank=True, default="")
    generation_mw = models.DecimalField(max_digits=14, decimal_places=3, null=True, blank=True)

    # Optional: keep resolution from the source (mixed zones -> not guaranteed uniform)
    resolution = models.CharField(max_length=16, blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = (("country", "psr_type", "datetime_utc"),)
        indexes = [
            models.Index(fields=["country", "psr_type", "datetime_utc"]),
        ]

    def __str__(self):
        return f"{self.country_id} {self.psr_type} {self.datetime_utc:%Y-%m-%d %H:%MZ}"




class ContractType(models.TextChoices):
    A01 = "A01", "Day-ahead"
    A07 = "A07", "Intraday"

class CountryPricePoint(models.Model):
    """
    A44: energy price per country and timestamp (aggregated across zones).
    Unique per country×contract_type×datetime_utc.
    """
    country = models.ForeignKey("entsoe_api.Country", on_delete=models.PROTECT, related_name="prices_country")
    datetime_utc = models.DateTimeField(db_index=True)

    contract_type = models.CharField(max_length=3, choices=ContractType.choices)  # A01/A07
    price = models.DecimalField(max_digits=14, decimal_places=6, null=True, blank=True)  # €/MWh typically
    currency = models.CharField(max_length=8, default="EUR", blank=True)   # from XML (usually EUR)
    unit = models.CharField(max_length=8, default="MWH", blank=True)       # from XML (usually MWH)
    # resolution isn't guaranteed uniform when aggregating; store if useful
    resolution = models.CharField(max_length=16, blank=True, default="")   # e.g., PT60M

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = (("country", "contract_type", "datetime_utc"),)
        indexes = [
            # Existing index
            models.Index(fields=["country", "contract_type", "datetime_utc"]),
            # Add these new indexes for bulk operations
            models.Index(fields=["contract_type", "datetime_utc"]),  # For filtering across countries
            models.Index(fields=["datetime_utc", "country"]),        # For time-based queries
        ]

    def __str__(self):
        return f"{self.country_id} {self.contract_type} {self.datetime_utc:%Y-%m-%d %H:%MZ}"
    

class PhysicalFlow(models.Model):
    """
    A11 Cross-Border Physical Flow (per direction).
    quantity_mw: MW at the timestamp.

    - Stores BOTH country-level FKs (for querying) and the original EIC pair
      (for provenance + uniqueness across multi-zone countries).
    """
    datetime_utc = models.DateTimeField(db_index=True)

    # NEW: country-level fields for querying & joins
    country_from = models.ForeignKey(
        "entsoe_api.Country",
        on_delete=models.PROTECT,
        related_name="flows_out",
        null=True,
        blank=True,
        db_index=True,
    )
    country_to = models.ForeignKey(
        "entsoe_api.Country",
        on_delete=models.PROTECT,
        related_name="flows_in",
        null=True,
        blank=True,
        db_index=True,
    )

    # Keep original EICs from the source response (A11)
    out_domain_eic = models.CharField(max_length=32, db_index=True, blank=True, null=True)
    in_domain_eic  = models.CharField(max_length=32, db_index=True, blank=True, null=True)

    resolution = models.CharField(max_length=16, blank=True, null=True)  # e.g. PT15M, PT60M
    quantity_mw = models.FloatField()

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        # Preserve uniqueness at the *EIC pair* level (original semantics)
        unique_together = ("datetime_utc", "out_domain_eic", "in_domain_eic")
        indexes = [
            # Fast country range queries (what your views need)
            models.Index(fields=["country_from", "country_to", "datetime_utc"]),
            models.Index(fields=["country_from", "datetime_utc"]),
            models.Index(fields=["country_to", "datetime_utc"]),
            # Keep existing EIC index patterns
            models.Index(fields=["out_domain_eic", "in_domain_eic", "datetime_utc"]),
        ]

    def __str__(self):
        cf = self.country_from_id or self.out_domain_eic or "?"
        ct = self.country_to_id   or self.in_domain_eic  or "?"
        return f"{self.datetime_utc} {cf}->{ct}: {self.quantity_mw} MW"