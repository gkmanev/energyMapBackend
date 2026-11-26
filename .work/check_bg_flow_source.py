"""Read-only comparison of raw ENTSO-E A11 data and the public flows API."""
import concurrent.futures
import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from entsoe_api.entsoe_data import EntsoePhysicalFlows

load_dotenv(Path(__file__).resolve().parents[1] / '.env')
EICS = {'BG': '10YCA-BULGARIA-R', 'RO': '10YRO-TEL------P',
        'GR': '10YGR-HTSO-----Y', 'RS': '10YCS-SERBIATSOV',
        'MK': '10YMK-MEPSO----8', 'TR': '10YTR-TEIAS----W'}


def check(pair):
    src, dst = pair
    try:
        response = requests.get('https://web-api.tp.entsoe.eu/api', params={
            'securityToken': os.environ['ENTSOE_TOKEN'], 'documentType': 'A11',
            'out_Domain': EICS[src], 'in_Domain': EICS[dst],
            'periodStart': '202611260800', 'periodEnd': '202611260900',
        }, timeout=40)
        root = ET.fromstring(response.text)
        reasons = [el.text for el in root.iter() if el.tag.split('}')[-1] in ('text', 'code')]
        rows = EntsoePhysicalFlows._parse_a11(response.text, EICS[src], EICS[dst]) if response.ok else []
        return {'pair': f'{src}->{dst}', 'status': response.status_code, 'reasons': reasons,
                'rows': [{**r, 'datetime_utc': str(r['datetime_utc'])} for r in rows],
                'series_metadata': [{el.tag.split('}')[-1]: el.text for el in ts
                                     if el.tag.split('}')[-1] != 'Period'}
                                    for ts in root.iter() if ts.tag.split('}')[-1] == 'TimeSeries']}
    except Exception as exc:
        return {'pair': f'{src}->{dst}', 'error_type': type(exc).__name__}


if __name__ == '__main__':
    pairs = [(a, b) for n in EICS if n != 'BG' for a, b in [('BG', n), (n, 'BG')]]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for result in pool.map(check, pairs):
            print(json.dumps(result), flush=True)
