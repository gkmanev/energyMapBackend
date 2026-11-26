"""Production SSH connection using separately configured gateway and VM accounts."""
import os
import re
import shlex
from pathlib import Path

import paramiko
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / '.env')


def endpoint(name):
    parts = shlex.split(os.environ[name])
    target = parts[-1]
    user, host = target.rsplit('@', 1)
    port = int(parts[parts.index('-p') + 1]) if '-p' in parts else 22
    return host, port, user


def folder():
    parts = shlex.split(os.environ['BACKEND_FOLDER'])
    return parts[1] if parts[0] == 'cd' else os.environ['BACKEND_FOLDER']


def connect():
    gateway = paramiko.SSHClient()
    backend = paramiko.SSHClient()
    for client in (gateway, backend):
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        host, port, user = endpoint('BACKENDHOST_SERVER_SSH')
        gateway.connect(host, port=port, username=user,
                        password=os.environ['BACKENDHOST_SERVER_SSH_PASS'],
                        timeout=20, look_for_keys=False, allow_agent=False)
        host, port, user = endpoint('BACKEND_VM')
        channel = gateway.get_transport().open_channel('direct-tcpip', (host, port), ('127.0.0.1', 0))
        backend.connect(host, port=port, username=user, password=os.environ['BACKEND_VM_PASS'],
                        sock=channel, timeout=20, look_for_keys=False, allow_agent=False)
        return gateway, backend
    except Exception:
        backend.close()
        gateway.close()
        raise


def run(command):
    gateway, backend = connect()
    try:
        _, stdout, stderr = backend.exec_command(command, timeout=180)
        for line in stdout:
            print(re.sub(r'(securityToken=)[^&\s\'"]+', r'\1[REDACTED]', line), end='', flush=True)
        error = stderr.read().decode(errors='replace')
        print(re.sub(r'(securityToken=)[^&\s\'"]+', r'\1[REDACTED]', error), end='', flush=True)
        status = stdout.channel.recv_exit_status()
        print('REMOTE_EXIT', status, flush=True)
        if status:
            raise RuntimeError(f'Remote command failed with exit status {status}')
    finally:
        backend.close()
        gateway.close()


if __name__ == '__main__':
    import sys
    run('cd ' + shlex.quote(folder()) + ' && ' + Path(sys.argv[1]).read_text(encoding='utf-8'))
