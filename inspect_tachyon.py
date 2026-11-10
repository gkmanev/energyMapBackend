"""Read-only Tachyon preflight; credentials are read locally, never printed."""
from pathlib import Path
import json
import re
import paramiko


def read_password():
    content = Path("tachyon-pass.txt").read_text(encoding="utf-8-sig").strip()
    try:
        password = json.loads(content)["ssh-tachyon-pass"]
    except (json.JSONDecodeError, KeyError, TypeError):
        match = re.search(r"^\s*ssh-tachyon-pass\s*[:=]\s*(.*?)\s*$", content, re.MULTILINE)
        if not match:
            raise ValueError("Password file must contain the key ssh-tachyon-pass") from None
        password = match.group(1)
        if len(password) >= 2 and password[0] == password[-1] and password[0] in "\"'":
            password = password[1:-1]
    return password


def connect():
    password = read_password()
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    # OpenSSH saved this host's key during the initial connection.
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect("192.168.1.76", username="particle", password=password,
                   look_for_keys=False, allow_agent=False, timeout=15,
                   auth_timeout=15, banner_timeout=15)
    return client


def main():
    client = connect()
    try:
        command = """
uname -m
cat /etc/os-release
id
command -v python3
python3 --version
python3 -c 'import tkinter; print("Tkinter available")'
for tool in chromium chromium-browser chromium.chromedriver chromedriver; do
    command -v "$tool" || true
done
ls -l /snap/bin/chromium* 2>/dev/null || true
snap list chromium 2>/dev/null || true
loginctl list-sessions --no-legend
ps -eo user,pid,comm | awk '$3 ~ /gnome-shell|Xorg|Xwayland|chromium|chromedriver/ {print}'
df -h "$HOME"
test ! -d "$HOME/kiosk" || ls -la "$HOME/kiosk"
"""
        _, stdout, stderr = client.exec_command(command, timeout=45)
        print(stdout.read().decode(errors="replace"), end="")
        print(stderr.read().decode(errors="replace"), end="")
        print("Remote exit status:", stdout.channel.recv_exit_status())
    finally:
        client.close()


if __name__ == "__main__":
    main()
