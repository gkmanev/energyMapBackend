# entsoe_api/management/commands/repair_flow_countries.py
"""
Fill missing PhysicalFlow.country_from / country_to from the stored EIC codes.

Rows end up with NULL country FKs when flows were saved before the Country row
existed (e.g. TR, which is only created by the EPİAŞ commands).

    python manage.py repair_flow_countries --dry-run
    python manage.py repair_flow_countries
"""
from django.core.management.base import BaseCommand
from django.db import transaction

from entsoe_api.helper import _eic_to_country_iso_map
from entsoe_api.models import Country, PhysicalFlow


class Command(BaseCommand):
    help = "Fill NULL country_from/country_to on PhysicalFlow rows using the EIC->country mapping."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Only report what would be updated.")

    def handle(self, *args, **opts):
        eic_to_iso = _eic_to_country_iso_map()
        existing = set(Country.objects.values_list("pk", flat=True))
        dry_run = opts["dry_run"]

        total = 0
        missing_country = set()
        with transaction.atomic():
            for field, eic_field in (("country_from", "out_domain_eic"), ("country_to", "in_domain_eic")):
                eics = (
                    PhysicalFlow.objects.filter(**{f"{field}__isnull": True})
                    .exclude(**{f"{eic_field}__isnull": True})
                    .values_list(eic_field, flat=True)
                    .distinct()
                )
                for eic in eics:
                    iso = eic_to_iso.get(eic)
                    if not iso:
                        continue
                    if iso not in existing:
                        missing_country.add(iso)
                        continue
                    qs = PhysicalFlow.objects.filter(**{f"{field}__isnull": True, eic_field: eic})
                    n = qs.count() if dry_run else qs.update(**{f"{field}_id": iso})
                    total += n
                    self.stdout.write(f"  {field} ← {iso} ({eic}): {n} rows")
            if dry_run:
                transaction.set_rollback(True)

        if missing_country:
            self.stdout.write(self.style.WARNING(
                f"Skipped EICs whose Country row doesn't exist yet: {', '.join(sorted(missing_country))}"
            ))
        verb = "would be updated" if dry_run else "updated"
        self.stdout.write(self.style.SUCCESS(f"{total} field values {verb}."))
