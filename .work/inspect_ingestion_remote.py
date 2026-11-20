"""Inspect the production ingestion stack through its SSH gateway."""
import os
import pathlib
import hashlib
import re
import subprocess
import sys
import paramiko

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
password = os.environ.pop("INGEST_SSH_PASSWORD")
gateway = paramiko.SSHClient()
gateway.set_missing_host_key_policy(paramiko.AutoAddPolicy())
backend = paramiko.SSHClient()
backend.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    gateway.connect("85.14.6.37", port=2222, username="entra", password=password, timeout=20, look_for_keys=False, allow_agent=False)
    channel = gateway.get_transport().open_channel("direct-tcpip", ("192.168.1.11", 22), ("127.0.0.1", 0))
    backend.connect("192.168.1.11", username="entsoebackendapi", password=password, sock=channel, timeout=20, look_for_keys=False, allow_agent=False)
    if len(sys.argv) > 1 and sys.argv[1] == "preflight":
        with backend.open_sftp() as sftp:
            root = sftp.normalize(".") + "/enMapBackend/"
            for name in ["requirements.txt", "entsoe_api/tasks.py", "entsoe_api/entsoe_data.py", "docker-compose.yml"]:
                data = sftp.open(root + name).read()
                target = pathlib.Path(".work/production-before") / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                print(name, "sha256", hashlib.sha256(data).hexdigest(), flush=True)
        sys.exit(0)
    if len(sys.argv) > 1 and sys.argv[1] == "stage-followup":
        with backend.open_sftp() as sftp:
            root = sftp.normalize(".") + "/enMapBackend/"
            repair = "/tmp/entsoe-ingestion-repair-20261113"
            current = sftp.open(root + "entsoe_api/epias.py").read()
            expected = subprocess.check_output(["git", "-c", "safe.directory=C:/Users/Georgi/entsoe", "show", "HEAD:entsoe_api/epias.py"])
            if current.replace(b"\r\n", b"\n") != expected.replace(b"\r\n", b"\n"):
                raise RuntimeError("Production EPİAŞ helper differs from expected baseline")
            with sftp.open(repair + "/entsoe_api__epias.py.before", "wb") as backup:
                backup.write(current)
            for name in ["entsoe_api/epias.py", "entsoe_api/test_ingestion.py"]:
                sftp.put(name, root + name)
            script = """#!/bin/sh
set -eu
cd "$HOME/enMapBackend"
repair=/tmp/entsoe-ingestion-repair-20261113
if docker compose --progress plain build celery_worker celery_beat web > "$repair/followup-build.log" 2>&1; then
  touch "$repair/followup-build.ok"
else
  touch "$repair/followup-build.failed"
  exit 1
fi
"""
            with sftp.open(repair + "/followup-build.sh", "w") as output:
                output.write(script)
        command = "nohup sh /tmp/entsoe-ingestion-repair-20261113/followup-build.sh >/tmp/entsoe-ingestion-repair-20261113/followup-launcher.log 2>&1 </dev/null & echo followup_build_pid=$!"
    elif len(sys.argv) > 1 and sys.argv[1] == "correct-de":
        with backend.open_sftp() as sftp:
            sftp.put(".work/backfill_ingestion.py", "/tmp/entsoe-ingestion-repair-20261113/backfill_ingestion.py")
        command = """set -e
cd ~/enMapBackend
docker cp /tmp/entsoe-ingestion-repair-20261113/backfill_ingestion.py enmapbackend-web-1:/tmp/backfill_ingestion.py
nohup sh -c 'cd "$HOME/enMapBackend" && docker compose exec -T web python -u /tmp/backfill_ingestion.py generation-de > /tmp/entsoe-ingestion-repair-20261113/generation-de.log 2>&1' >/tmp/entsoe-ingestion-repair-20261113/generation-de-launcher.log 2>&1 </dev/null & echo correction_pid=$!
"""
    elif len(sys.argv) > 1 and sys.argv[1] == "start-backfill":
        with backend.open_sftp() as sftp:
            repair = "/tmp/entsoe-ingestion-repair-20261113"
            for name in ["backfill_ingestion.py", "ingestion_coverage.py"]:
                sftp.put(".work/" + name, repair + "/" + name)
            script = """#!/bin/sh
set -eu
cd "$HOME/enMapBackend"
repair=/tmp/entsoe-ingestion-repair-20261113
docker cp "$repair/backfill_ingestion.py" enmapbackend-web-1:/tmp/backfill_ingestion.py
docker cp "$repair/ingestion_coverage.py" enmapbackend-web-1:/tmp/ingestion_coverage.py
if docker compose exec -T web python -u /tmp/backfill_ingestion.py > "$repair/backfill.log" 2>&1; then
  touch "$repair/backfill.ok"
else
  touch "$repair/backfill.failed"
  exit 1
fi
"""
            with sftp.open(repair + "/backfill.sh", "w") as output:
                output.write(script)
        command = "nohup sh /tmp/entsoe-ingestion-repair-20261113/backfill.sh >/tmp/entsoe-ingestion-repair-20261113/backfill-launcher.log 2>&1 </dev/null & echo backfill_pid=$!"
    elif len(sys.argv) > 1 and sys.argv[1] == "stage":
        with backend.open_sftp() as sftp:
            root = sftp.normalize(".") + "/enMapBackend/"
            repair = "/tmp/entsoe-ingestion-repair-20261113"
            try:
                sftp.mkdir(repair)
            except OSError:
                raise RuntimeError("Repair directory already exists; inspect before reusing")
            names = ["requirements.txt", "entsoe_api/tasks.py", "entsoe_api/entsoe_data.py"]
            for name in names:
                expected = (pathlib.Path(".work/production-before") / name).read_bytes()
                current = sftp.open(root + name).read()
                if current != expected:
                    raise RuntimeError("Production file changed since inspection: " + name)
                with sftp.open(repair + "/" + name.replace("/", "__") + ".before", "wb") as backup:
                    backup.write(current)
            for name in names + ["entsoe_api/test_ingestion.py"]:
                sftp.put(name, root + name)
            script = """#!/bin/sh
set -eu
cd "$HOME/enMapBackend"
repair=/tmp/entsoe-ingestion-repair-20261113
if docker compose --progress plain build celery_worker celery_beat web > "$repair/build.log" 2>&1; then
  touch "$repair/build.ok"
else
  touch "$repair/build.failed"
  exit 1
fi
"""
            with sftp.open(repair + "/build.sh", "w") as output:
                output.write(script)
        command = "nohup sh /tmp/entsoe-ingestion-repair-20261113/build.sh >/tmp/entsoe-ingestion-repair-20261113/launcher.log 2>&1 </dev/null & echo build_pid=$!"
    else:
        command = sys.argv[1] if len(sys.argv) > 1 else "cd ~/enMapBackend && date -Is && docker compose ps && docker compose logs --since 24h --tail 250 celery_beat celery_worker"
    if command == "build-status":
        command = "ls /tmp/entsoe-ingestion-repair-20261113/build.*; tail -18 /tmp/entsoe-ingestion-repair-20261113/build.log"
    if command == "followup-status":
        command = "ls /tmp/entsoe-ingestion-repair-20261113/followup-build.*; tail -12 /tmp/entsoe-ingestion-repair-20261113/followup-build.log"
    if command == "validate":
        command = """set -e
cd ~/enMapBackend
test -f /tmp/entsoe-ingestion-repair-20261113/build.ok
docker compose run --rm --no-deps celery_worker python manage.py test entsoe_api.test_ingestion --settings=data_api.test_settings --verbosity 2
docker compose run --rm --no-deps celery_worker python -c 'import importlib.metadata as m; from data_api.celery import app; print({n:m.version(n) for n in ["celery","kombu","redis"]}); c=app.connection_for_write(); c.ensure_connection(max_retries=1); channel=c.channel(); print("BROKER_CHANNEL_OK"); channel.close(); c.close()'
docker compose run --rm --no-deps celery_worker pip check
"""
    if command == "activate":
        command = """set -e
cd ~/enMapBackend
test -f /tmp/entsoe-ingestion-repair-20261113/build.ok
docker compose up -d --no-deps celery_worker celery_beat web
docker compose ps
"""
    if command == "health":
        command = """cd ~/enMapBackend
docker compose ps
docker inspect --format '{{.Name}} restarts={{.RestartCount}} started={{.State.StartedAt}}' enmapbackend-celery_worker-1 enmapbackend-celery_beat-1
docker compose logs --since 8m --tail 200 celery_worker celery_beat 2>&1 | grep -E 'ERROR|CRITICAL|ready|Connected to|received|succeeded|Sending due task|retry|failed' | tail -35
docker compose exec -T celery_worker celery -A data_api inspect ping active --timeout 8
docker compose exec -T redis redis-cli LLEN celery
"""
    if command == "enqueue-catchup":
        command = """set -e
cd ~/enMapBackend
docker compose exec -T web python manage.py shell -c 'from entsoe_api.tasks import fetch_generation_hourly_task, fetch_prices_hourly_task, fetch_generation_eso_bg_quarter_hourly_task; tasks=[fetch_generation_hourly_task,fetch_prices_hourly_task,fetch_generation_eso_bg_quarter_hourly_task]; [print(t.name,t.delay().id) for t in tasks]'
"""
    if command == "task-status":
        command = """cd ~/enMapBackend
docker compose exec -T web python manage.py shell -c 'from data_api.celery import app; ids={"generation":"ff77f0f5-7810-488c-8f64-9f4419c4e735","prices":"30072a69-2046-4e9f-94cd-830da2e20c1f","eso":"3a0fed00-c47a-48ca-9422-d7285a0555ec"}; [print(name,app.AsyncResult(task_id).state,type(app.AsyncResult(task_id).result).__name__) for name,task_id in ids.items()]'
docker compose logs --since 15m --tail 10000 celery_worker celery_beat 2>&1 | grep -E 'ERROR|CRITICAL|Task .*received|Task .*succeeded|Task .*retry|Task .*raised|Sending due task' | tail -25
docker compose exec -T celery_worker celery -A data_api inspect active --timeout 5
"""
    if command == "worker-progress":
        command = """cd ~/enMapBackend
date -Is
docker compose logs --since 10m --tail 65 celery_worker celery_beat
docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} memory={{.MemUsage}}' enmapbackend-celery_worker-1 enmapbackend-web-1
"""
    if command == "eso-source":
        command = """cd ~/enMapBackend
docker compose exec -T web python -c 'import requests; u="http://85.14.6.37:17999/api/generation/"; r=requests.get(u,timeout=30); r.raise_for_status(); j=r.json(); items=j.get("results",[]); print("count",j.get("count"),"page_rows",len(items),"next",j.get("next")); print("first",items[0] if items else None); print("last",items[-1] if items else None)'
"""
    if command == "cancel-manual-eso":
        command = """cd ~/enMapBackend
docker compose exec -T web python manage.py shell -c 'from data_api.celery import app; app.control.revoke("3a0fed00-c47a-48ca-9422-d7285a0555ec",terminate=True,signal="SIGTERM"); print("Cancelled redundant manual historical ESO replay; scheduled job remains active")'
"""
    if command == "verify-turkey-task":
        command = """set -e
cd ~/enMapBackend
docker compose exec -T web python manage.py shell -c 'from entsoe_api.tasks import fetch_prices_tr_task; result=fetch_prices_tr_task.delay(); print("TR_PRICE_TASK_SENT",result.id,flush=True); result.get(timeout=60); print("TR_PRICE_TASK_SUCCESS",result.id,flush=True)'
"""
    if command == "final-rollout":
        command = """set -e
cd ~/enMapBackend
test -f /tmp/entsoe-ingestion-repair-20261113/followup-build.ok
docker compose run --rm --no-deps celery_worker python manage.py test entsoe_api.test_ingestion --settings=data_api.test_settings --verbosity 2
echo 'Allowing current worker tasks to finish before replacement'
docker compose stop -t 120 celery_worker
docker compose up -d --no-deps celery_worker celery_beat web
docker compose exec -T web python manage.py shell -c 'from entsoe_api.tasks import fetch_prices_tr_task; result=fetch_prices_tr_task.delay(); print("TR_PRICE_TASK_SENT",result.id,flush=True); result.get(timeout=60); print("TR_PRICE_TASK_SUCCESS",result.id,flush=True)'
docker compose ps
docker inspect --format '{{.Name}} restarts={{.RestartCount}}' enmapbackend-celery_worker-1 enmapbackend-celery_beat-1
"""
    if command == "epias-daycheck":
        command = """cd ~/enMapBackend
docker compose exec -T web python manage.py shell -c 'import datetime as dt,requests; from entsoe_api import epias; ticket=epias.get_tgt(*epias.get_credentials()); dates=[dt.date(2026,11,13),dt.date(2026,11,14)]; [(lambda r: print(day,r.status_code,r.text[:800] if r.status_code!=200 else "items="+str(len(r.json().get("items",[])))))(requests.post(epias.BASE_URL+"/v1/markets/dam/data/mcp",json=epias.date_range_body(day,day),headers={"TGT":ticket,"Accept":"application/json"},timeout=30)) for day in dates]'
"""
    if command == "backfill-status":
        command = """cd ~/enMapBackend
ls /tmp/entsoe-ingestion-repair-20261113/backfill.*
tail -12 /tmp/entsoe-ingestion-repair-20261113/backfill.log
tail -3 /tmp/entsoe-ingestion-repair-20261113/generation-de.log
docker compose exec -T web python /tmp/ingestion_coverage.py
"""
    if command == "details":
        command = """cd ~/enMapBackend
docker inspect --format '{{.Name}} restarts={{.RestartCount}} started={{.State.StartedAt}} oom={{.State.OOMKilled}}' enmapbackend-celery_worker-1 enmapbackend-celery_beat-1
docker compose logs --since 24h --tail 4000 celery_beat celery_worker 2>&1 | grep -E 'VersionMismatch|Unrecoverable|Error|Sending due task|ready|Connected to' | tail -35
docker compose exec -T web python -c 'import importlib.metadata as m; print({n:m.version(n) for n in ["celery", "kombu", "redis"]})'
docker compose exec -T web pip check
docker compose exec -T redis redis-cli LLEN celery
docker compose exec -T redis redis-cli INFO memory | grep -E 'used_memory_human|maxmemory_human|maxmemory_policy'
grep -nE 'celery|kombu|redis' requirements.txt
git status --short
git log -1 --format='%h %ci %s'
"""
    _, stdout, stderr = backend.exec_command(command, timeout=180)
    for line in stdout:
        print(re.sub(r"(securityToken=)[^&\s'\"]+", r"\1[REDACTED]", line), end="", flush=True)
    print(stderr.read().decode(errors="replace"), end="", flush=True)
    print("REMOTE_EXIT", stdout.channel.recv_exit_status(), flush=True)
finally:
    backend.close()
    gateway.close()
