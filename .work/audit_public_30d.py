"""Read-only public API coverage audit; raw rows stay in memory."""
import concurrent.futures
import datetime as dt
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

sys.path.insert(0, os.getcwd())
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_api.settings')
import django
django.setup()
from django.conf import settings
import requests

TZ = ZoneInfo('Europe/Sofia')
END = dt.datetime(2026, 11, 20, tzinfo=TZ).astimezone(dt.timezone.utc)
START = (dt.datetime(2026, 11, 20, tzinfo=TZ) - dt.timedelta(days=30)).astimezone(dt.timezone.utc)
EXPECTED = {START + dt.timedelta(hours=i) for i in range(int((END-START).total_seconds()/3600))}
OUT = Path('.work/public-coverage-30d-20261120.json')

def check(job):
    dataset, codes = job
    try:
        response = requests.get('https://api.visualize.energy/api/' + dataset + '/bulk-range/', params={
            'countries': ','.join(codes), 'start': START.isoformat(), 'end': END.isoformat(),
        }, timeout=(15, 90))
        response.raise_for_status()
        payload = response.json()
        result = []
        for code in codes:
            data = payload.get('data', {}).get(code, {})
            items = data.get('items', []) if isinstance(data, dict) else []
            stamps = [dt.datetime.fromisoformat(row['datetime_utc'].replace('Z', '+00:00')) for row in items]
            hours = {stamp.replace(minute=0, second=0, microsecond=0) for stamp in stamps if START <= stamp < END}
            missing = sorted(EXPECTED - hours)
            row = {'dataset': dataset, 'country': code, 'rows': len(items), 'observed_hours': len(hours),
                   'expected_hours': len(EXPECTED), 'missing_hours': len(missing),
                   'missing_by_day': {}, 'first': min(stamps).isoformat() if stamps else None,
                   'last': max(stamps).isoformat() if stamps else None}
            for stamp in missing:
                day = stamp.astimezone(TZ).date().isoformat()
                row['missing_by_day'][day] = row['missing_by_day'].get(day, 0) + 1
            result.append(row)
            print(json.dumps({k:v for k,v in row.items() if k != 'missing_by_day'}), flush=True)
        return result
    except Exception as error:
        row = {'dataset': dataset, 'countries': codes, 'error': type(error).__name__,
               'status': getattr(getattr(error, 'response', None), 'status_code', None)}
        print(json.dumps(row), flush=True)
        return [row]

if __name__ == '__main__':
    jobs = []
    for dataset, mapping in [('prices', settings.ENTSOE_PRICE_COUNTRY_TO_EICS), ('generation', settings.ENTSOE_COUNTRY_TO_EICS)]:
        codes = sorted(mapping)
        jobs.extend((dataset, codes[i:i+5]) for i in range(0, len(codes), 5))
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        for rows in pool.map(check, jobs):
            results.extend(rows)
            OUT.write_text(json.dumps({'start': START.isoformat(), 'end_exclusive': END.isoformat(),
                                      'limitation': 'Country-level hourly presence only; does not verify every production type or native-resolution interval.',
                                      'results': results}, indent=2), encoding='utf-8')
    print('Report:', OUT)
