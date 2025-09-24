# yourapp/serializers.py
from rest_framework import serializers
from .models import Country, CountryCapacitySnapshot, CountryGenerationByType, CountryPricePoint

class CountrySerializer(serializers.ModelSerializer):
    class Meta:
        model = Country
        fields = ["iso_code", "name"]

class CountryCapacitySnapshotSerializer(serializers.ModelSerializer):
    country = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)
    class Meta:
        model = CountryCapacitySnapshot
        fields = ["country","psr_type","psr_name","installed_capacity_mw","year","valid_from_utc"]

class CountryGenerationByTypeSerializer(serializers.ModelSerializer):
    country = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)
    class Meta:
        model = CountryGenerationByType
        fields = ["country","datetime_utc","psr_type","psr_name","generation_mw","resolution"]


class CountryPricePointSerializer(serializers.ModelSerializer):
    country = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)

    class Meta:
        model = CountryPricePoint
        fields = [
            "country",
            "datetime_utc",
            "contract_type",
            "price",
            "currency",
            "unit",
            "resolution",
        ]