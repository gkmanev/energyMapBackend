from django.core.management.base import BaseCommand
from django.conf import settings
from entsoe_api.models import Country

class Command(BaseCommand):
    help = "Sync Country rows from settings mappings"

    def handle(self, *args, **kwargs):
        keys = set(getattr(settings, "ENTSOE_PRICE_COUNTRY_TO_EICS", {}).keys())
        keys |= set(getattr(settings, "ENTSOE_COUNTRY_TO_EICS", {}).keys())
        created = []
        for code in sorted(keys):
            obj, was_created = Country.objects.get_or_create(
                iso_code=code,
                defaults={"name": code}
            )
            if was_created:
                created.append(code)
        self.stdout.write(self.style.SUCCESS(f"Created: {created}"))