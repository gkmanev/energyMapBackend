"""Read-only production flow diagnostics through the existing SSH helper."""
import os
from pathlib import Path
import shlex
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')
from flow_remote import folder, run

probe = '''
import datetime as dt
import hashlib
import inspect
import json
from pathlib import Path
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_api.settings')
import django
django.setup()
from django.db.models import Count, Max, Q
from entsoe_api.entsoe_data import EntsoePhysicalFlows
from entsoe_api.models import PhysicalFlow
from django.conf import settings
print('UTC_NOW', dt.datetime.now(dt.timezone.utc).isoformat())
parser = inspect.getsource(EntsoePhysicalFlows._parse_a11)
print('PARSER_HAS_A03_EXPANSION', 'while ts < block_end' in parser and 'A03' in parser)
for name in ['entsoe_api/entsoe_data.py', 'entsoe_api/management/commands/fetch_flows.py', 'entsoe_api/tasks.py']:
    print('SOURCE_SHA256', name, hashlib.sha256(Path(name).read_bytes().replace(b'\\r\\n', b'\\n')).hexdigest())
print('FLOW_SCHEDULE', str(settings.CELERY_BEAT_SCHEDULE.get('fetch_flows_hourly')))
rows = PhysicalFlow.objects.filter(Q(country_from_id='BG') | Q(country_to_id='BG'))
coverage = rows.values('country_from_id', 'country_to_id', 'out_domain_eic', 'in_domain_eic').annotate(latest=Max('datetime_utc'), count=Count('pk'))
print('BG_BORDER_COVERAGE', json.dumps(list(coverage), default=str))
hour = rows.filter(datetime_utc__gte=dt.datetime(2026,11,26,8,tzinfo=dt.timezone.utc), datetime_utc__lt=dt.datetime(2026,11,26,9,tzinfo=dt.timezone.utc))
print('SCREENSHOT_HOUR', json.dumps(list(hour.values('datetime_utc', 'country_from_id', 'country_to_id', 'quantity_mw', 'resolution')), default=str))
'''

sanitize = '''
import re, sys
pattern = re.compile(r'flow|A11|ERROR|CRITICAL|raised|retry|ready|Connected to', re.I)
lines = [re.sub(r'(securityToken=)[^&\\s\\\'"]+', r'\\1[REDACTED]', line) for line in sys.stdin if pattern.search(line)]
sys.stdout.writelines(lines[-100:])
'''

commands = [
    'cd ' + shlex.quote(folder()),
    'date -Is',
    'git log -1 --format=' + shlex.quote('%h %ci %s'),
    'docker compose ps',
    'docker compose exec -T celery_worker python -c ' + shlex.quote(probe),
    'docker compose exec -T redis redis-cli LLEN celery',
    'docker compose logs --since 4h --tail 20000 celery_worker celery_beat 2>&1 | python3 -c ' + shlex.quote(sanitize),
]
run(' && '.join(commands))
