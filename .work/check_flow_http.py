"""Compare cached public flow responses with a cache-bypassing request."""
import concurrent.futures
import json
import time
import requests


def check(bust):
    params = {'country': 'BG', 'neighbors': 1}
    if bust:
        params['_check'] = str(time.time_ns())
    response = requests.get('https://api.visualize.energy/api/flows/latest/', params=params, timeout=40)
    payload = response.json()
    return {'cache_bypass': bust, 'status': response.status_code,
            'headers': {k: v for k, v in response.headers.items() if k.lower() in
                        {'age', 'cache-control', 'cf-cache-status', 'server', 'date', 'via', 'x-cache'}},
            'start': payload.get('start_utc'), 'count': payload.get('count'),
            'totals': payload.get('totals'), 'neighbors': payload.get('neighbors')}


with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
    for result in pool.map(check, [False, True]):
        print(json.dumps(result), flush=True)
