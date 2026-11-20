import os
from dotenv import load_dotenv
load_dotenv()
print('SSH credential available:', bool(os.getenv('INGEST_SSH_PASSWORD')))
print('DB host:', os.getenv('DB_HOST', 'localhost'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_api.settings')
import sys
sys.path.insert(0, os.getcwd())
import django
django.setup()
from django.db import connection
with connection.cursor() as cursor:
    cursor.execute('SELECT current_database(), now()')
    print(cursor.fetchone())
