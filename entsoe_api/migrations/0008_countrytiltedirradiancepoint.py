from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("entsoe_api", "0007_countryresgenerationbytype"),
    ]

    operations = [
        migrations.CreateModel(
            name="CountryTiltedIrradiancePoint",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("datetime_utc", models.DateTimeField(db_index=True)),
                ("tilt_degrees", models.DecimalField(decimal_places=2, max_digits=5)),
                ("azimuth_degrees", models.DecimalField(decimal_places=2, default=0, max_digits=6)),
                ("irradiance_wm2", models.FloatField(blank=True, null=True)),
                ("resolution", models.CharField(blank=True, default="", max_length=16)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "country",
                    models.ForeignKey(
                        on_delete=models.deletion.PROTECT,
                        related_name="tilted_irradiance_country",
                        to="entsoe_api.country",
                    ),
                ),
            ],
            options={
                "unique_together": {("country", "datetime_utc", "tilt_degrees", "azimuth_degrees")},
            },
        ),
        migrations.AddIndex(
            model_name="countrytiltedirradiancepoint",
            index=models.Index(fields=["country", "datetime_utc"], name="entsoe_api__country_291160_idx"),
        ),
        migrations.AddIndex(
            model_name="countrytiltedirradiancepoint",
            index=models.Index(fields=["datetime_utc", "country"], name="entsoe_api__datetim_5be1c9_idx"),
        ),
    ]
