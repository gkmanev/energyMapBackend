from celery import shared_task
from django.core.management import call_command

@shared_task
def fetch_installed_capacity_task():
    # This runs: python manage.py fetch_installed_capacity
    call_command("fetch_installed_capacity")
