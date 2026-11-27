import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo
import requests

start=dt.datetime(2026,11,27,tzinfo=ZoneInfo('Europe/Brussels'))
body={'dateTimeRange':{'from':start.astimezone(dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'),'to':(start+dt.timedelta(days=1)).astimezone(dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')},'areaList':['BZN|10YCA-BULGARIA-R'],'timeZone':'CET','sorterList':[],'filterMap':{}}
r=requests.post('https://transparency.entsoe.eu/transmission/physicalFlows/load',json=body,headers={'Accept':'application/json'},timeout=30)
print('HTTP',r.status_code,'TYPE',r.headers.get('Content-Type'),flush=True)
r.raise_for_status()
data=r.json()
Path('.work/flows_ui_sample.json').write_text(json.dumps(data),encoding='utf-8')
print('KEYS',list(data),flush=True)
print('METADATA',json.dumps(data.get('metaData')),flush=True)
print('INSTANCES',len(data.get('instanceList',[])),flush=True)
for inst in data.get('instanceList',[])[:2]:
    print('INSTANCE',json.dumps(inst)[:5000],flush=True)
