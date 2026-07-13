# yourapp/serializers.py
from django.contrib.auth import authenticate, get_user_model, password_validation
from rest_framework import serializers
from .models import (
    Country,
    CountryCapacitySnapshot,
    CountryGenerationByType,
    CountryResGenerationByType,
    CountryGenerationForecastByType,
    CountryTiltedIrradiancePoint,
    CountryWindSpeedPoint,
    CountryPricePoint,
    PhysicalFlow,
)

User = get_user_model()


class AuthUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ["id", "email", "first_name", "last_name"]


class RegisterSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField(write_only=True, style={"input_type": "password"})
    first_name = serializers.CharField(required=False, allow_blank=True, max_length=150)
    last_name = serializers.CharField(required=False, allow_blank=True, max_length=150)

    def validate_email(self, value: str) -> str:
        email = value.strip().lower()
        if User.objects.filter(email__iexact=email).exists():
            raise serializers.ValidationError("A user with this email already exists.")
        return email

    def validate(self, attrs: dict) -> dict:
        user = User(
            username=attrs["email"],
            email=attrs["email"],
            first_name=attrs.get("first_name", "").strip(),
            last_name=attrs.get("last_name", "").strip(),
        )
        password_validation.validate_password(attrs["password"], user)
        return attrs

    def create(self, validated_data: dict) -> User:
        return User.objects.create_user(
            username=validated_data["email"],
            email=validated_data["email"],
            password=validated_data["password"],
            first_name=validated_data.get("first_name", "").strip(),
            last_name=validated_data.get("last_name", "").strip(),
        )


class LoginSerializer(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField(write_only=True, style={"input_type": "password"})

    def validate(self, attrs: dict) -> dict:
        email = attrs["email"].strip().lower()
        try:
            user = User.objects.get(email__iexact=email)
        except User.DoesNotExist as exc:
            raise serializers.ValidationError({"detail": "Invalid email or password."}) from exc

        authenticated_user = authenticate(
            request=self.context.get("request"),
            username=user.username,
            password=attrs["password"],
        )
        if authenticated_user is None:
            raise serializers.ValidationError({"detail": "Invalid email or password."})
        if not authenticated_user.is_active:
            raise serializers.ValidationError({"detail": "This account is disabled."})

        attrs["user"] = authenticated_user
        attrs["email"] = email
        return attrs


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


class CountryGenerationForecastByTypeSerializer(serializers.ModelSerializer):
    country = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)

    class Meta:
        model = CountryGenerationForecastByType
        fields = [
            "country",
            "datetime_utc",
            "psr_type",
            "psr_name",
            "forecast_mw",
            "resolution",
        ]


class CountryTiltedIrradiancePointSerializer(serializers.ModelSerializer):
    country = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)

    class Meta:
        model = CountryTiltedIrradiancePoint
        fields = [
            "country",
            "datetime_utc",
            "tilt_degrees",
            "azimuth_degrees",
            "irradiance_wm2",
            "resolution",
        ]


class CountryWindSpeedPointSerializer(serializers.ModelSerializer):
    country = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)

    class Meta:
        model = CountryWindSpeedPoint
        fields = [
            "country",
            "datetime_utc",
            "wind_speed_120m",
            "resolution",
        ]


class CountryResGenerationByTypeSerializer(serializers.ModelSerializer):
    country = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)

    class Meta:
        model = CountryResGenerationByType
        fields = [
            "country",
            "datetime_utc",
            "psr_type",
            "psr_name",
            "generation_mw",
            "unit",
            "resolution",
        ]


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

class PhysicalFlowSerializer(serializers.ModelSerializer):
    country_from = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)
    country_to = serializers.SlugRelatedField(slug_field="iso_code", read_only=True)

    class Meta:
        model = PhysicalFlow
        fields = [
            "datetime_utc",
            "country_from",
            "country_to",
            "out_domain_eic",
            "in_domain_eic",
            "resolution",
            "quantity_mw",
            "created_at",
        ]
