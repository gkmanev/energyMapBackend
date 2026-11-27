"""Delete only the production account explicitly named by the user."""
import shlex

from flow_remote import folder, run

probe = '''
import os,json
os.environ.setdefault('DJANGO_SETTINGS_MODULE','data_api.settings')
import django
django.setup()
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models.deletion import Collector
User=get_user_model()
email='georgi.manev@entra.energy'
with transaction.atomic():
    accounts=list(User.objects.select_for_update().filter(email__iexact=email))
    if len(accounts)>1:
        raise RuntimeError('Multiple matching accounts; refusing ambiguous deletion')
    other_before=list(User.objects.filter(email__iexact='gk.manev@gmail.com').values())
    if accounts:
        user=accounts[0]
        collector=Collector(using='default')
        collector.collect([user])
        # Refuse a cascade into another account or application data. Django's
        # built-in admin log and auth join rows may belong to the deleted user.
        allowed={'auth.User','admin.LogEntry','auth.User_groups','auth.User_user_permissions'}
        labels={model._meta.label for model,rows in collector.data.items() if rows}
        labels.update(q.model._meta.label for q in collector.fast_deletes)
        if labels-allowed:
            raise RuntimeError('Deletion would affect additional models: '+', '.join(sorted(labels-allowed)))
        deleted,details=user.delete()
        print(json.dumps({'deleted_email':email,'deleted_rows':deleted,'models':details}),flush=True)
    else:
        print(json.dumps({'email':email,'already_absent':True}),flush=True)
    if User.objects.filter(email__iexact=email).exists():
        raise RuntimeError('Account still exists; rolling back')
    other_after=list(User.objects.filter(email__iexact='gk.manev@gmail.com').values())
    if other_before!=other_after:
        raise RuntimeError('Other account changed; rolling back')
print(json.dumps({'removed':True,'other_account_unchanged':True}),flush=True)
'''
run('cd ' + shlex.quote(folder()) + ' && docker compose exec -T web python -c ' + shlex.quote(probe))
