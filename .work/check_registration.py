"""Inspect registration without creating accounts or sending emails."""
import json
import shlex
import sys

import requests

if '--remote' in sys.argv:
    from flow_remote import folder, run
    probe = '''
import os, json
os.environ.setdefault('DJANGO_SETTINGS_MODULE','data_api.settings')
import django
django.setup()
from django.conf import settings
from django.urls import resolve
from django.contrib.auth import get_user_model
print('EMAIL_CONFIGURED', json.dumps({key: bool(getattr(settings,key,'')) for key in ['RESEND_API_KEY','RESEND_FROM_EMAIL']}))
print('PUBLIC_URLS', json.dumps({key:getattr(settings,key,None) for key in ['API_PUBLIC_URL','FRONTEND_PUBLIC_URL']}))
print('REGISTER_VIEW', resolve('/api/auth/register/').func.view_class.__name__)
print('ACTIVATE_VIEW', resolve('/api/auth/activate/invalid/invalid/').func.view_class.__name__)
print('USER_COUNTS', json.dumps({'active':get_user_model().objects.filter(is_active=True).count(),'inactive':get_user_model().objects.filter(is_active=False).count()}))
'''
    run('cd ' + shlex.quote(folder()) + ' && docker compose exec -T web python -c ' + shlex.quote(probe))
else:
    base = 'https://api.visualize.energy'
    for method, path, kwargs in [
        ('OPTIONS', '/api/auth/register/', {}),
        ('POST', '/api/auth/register/', {'json': {}}),
        ('GET', '/api/auth/activate/invalid/invalid/', {}),
    ]:
        response = requests.request(method, base + path, timeout=30, allow_redirects=False, **kwargs)
        print(json.dumps({'method':method,'path':path,'status':response.status_code,
                          'allow':response.headers.get('Allow'),
                          'body':response.json() if method != 'OPTIONS' else None}), flush=True)
