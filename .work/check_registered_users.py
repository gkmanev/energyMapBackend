"""Read-only lookup of the two explicitly requested production accounts."""
import shlex

from flow_remote import folder, run

probe = '''
import os,json
os.environ.setdefault('DJANGO_SETTINGS_MODULE','data_api.settings')
import django
django.setup()
from django.contrib.auth import get_user_model
User=get_user_model()
for email in ['georgi.manev@entra.energy','gk.manev@gmail.com']:
    users=list(User.objects.filter(email__iexact=email).values('email','is_active','date_joined','last_login'))
    print(json.dumps({'requested_email':email,'registered':bool(users),'accounts':users},default=str),flush=True)
'''
run('cd ' + shlex.quote(folder()) + ' && docker compose exec -T web python -c ' + shlex.quote(probe))
