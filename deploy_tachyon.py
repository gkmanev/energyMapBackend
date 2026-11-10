"""Install and inspect the explicitly requested Tachyon kiosk over SSH."""
import sys
from inspect_tachyon import connect, read_password

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

def run(client, command, sudo=False):
    stdin, stdout, stderr = client.exec_command(command, timeout=900)
    if sudo:
        stdin.write(read_password() + "\n")
        stdin.flush()
    stdin.channel.shutdown_write()
    for line in stdout:
        print(line, end="", flush=True)
    error = stderr.read().decode(errors="replace")
    if error:
        print(error, end="", flush=True)
    status = stdout.channel.recv_exit_status()
    if status:
        raise RuntimeError(f"Remote command failed with exit status {status}")


def main():
    client = connect()
    try:
        if sys.argv[1] == "packages":
            run(client, "sudo -S -p '' sh -c 'apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv python3-tk xwayland snapd'", sudo=True)
            run(client, "sudo -S -p '' snap install chromium", sudo=True)
            run(client, "/snap/bin/chromium --version; /snap/bin/chromium.chromedriver --version")
        elif sys.argv[1] == "inspect":
            run(client, "loginctl show-session 2 -p Type -p Display -p Desktop; systemctl is-active snapd; ls -l /run/user/1000/.mutter-Xwaylandauth*; command -v gnome-session-inhibit; test ! -d ~/kiosk || ls -la ~/kiosk")
        elif sys.argv[1] == "upload":
            run(client, "mkdir -p ~/kiosk; test ! -f ~/kiosk/kiosk.py")
            with client.open_sftp() as sftp:
                for name in ("kiosk.py", "requirements-kiosk.txt", "KIOSK-TACHYON.md"):
                    sftp.put(name, "/home/particle/kiosk/" + name)
            run(client, "cd ~/kiosk && python3 -m venv .venv && .venv/bin/python -m pip install -r requirements-kiosk.txt && .venv/bin/python -m py_compile kiosk.py")
        elif sys.argv[1] == "desktop":
            run(client, "XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus systemctl --user show-environment | sed -n '/^DISPLAY=/p; /^XAUTHORITY=/p; /^WAYLAND_DISPLAY=/p'; XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus systemctl --user is-active graphical-session.target; pgrep -a snap; snap changes")
        elif sys.argv[1] == "python":
            run(client, "cd ~/kiosk && .venv/bin/python -m pip install --progress-bar off -r requirements-kiosk.txt && .venv/bin/python -m py_compile kiosk.py")
        elif sys.argv[1] == "smoke":
            with client.open_sftp() as sftp:
                sftp.put("smoke_tachyon.py", "/home/particle/kiosk/smoke_tachyon.py")
                sftp.put("kiosk.py", "/home/particle/kiosk/kiosk.py")
            run(client, "XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus systemd-run --user --unit=energy-kiosk-smoke --wait --pipe --collect --setenv=KIOSK_DRIVER_LOG=/home/particle/kiosk/driver.log --working-directory=/home/particle/kiosk /home/particle/kiosk/.venv/bin/python /home/particle/kiosk/smoke_tachyon.py; result=$?; if [ $result -ne 0 ]; then tail -100 /home/particle/kiosk/driver.log; fi; exit $result")
        elif sys.argv[1] == "diagnose":
            run(client, "snap connections chromium; XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus systemd-run --user --unit=energy-kiosk-browser-check --wait --pipe --collect /usr/bin/timeout 12 /snap/bin/chromium --ozone-platform=x11 --user-data-dir=/home/particle/snap/chromium/common/kiosk/left --no-first-run about:blank")
        elif sys.argv[1] == "screenshots":
            from pathlib import Path
            Path("tachyon-checks").mkdir(exist_ok=True)
            with client.open_sftp() as sftp:
                for side in ("left", "right"):
                    sftp.get(f"/home/particle/kiosk/smoke-{side}.png", f"tachyon-checks/{side}.png")
        elif sys.argv[1] == "docs":
            with client.open_sftp() as sftp:
                sftp.put("KIOSK-TACHYON.md", "/home/particle/kiosk/KIOSK-TACHYON.md")
        elif sys.argv[1] == "service":
            run(client, "mkdir -p ~/.config/systemd/user; test ! -f ~/.config/systemd/user/energy-kiosk.service")
            with client.open_sftp() as sftp:
                sftp.put("energy-kiosk.service", "/home/particle/.config/systemd/user/energy-kiosk.service")
                sftp.put("kiosk.py", "/home/particle/kiosk/kiosk.py")
            run(client, "export XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus; systemctl --user daemon-reload && systemctl --user enable --now energy-kiosk.service && systemctl --user status energy-kiosk.service --no-pager")
        elif sys.argv[1] == "status":
            run(client, "export XDG_RUNTIME_DIR=/run/user/1000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus; systemctl --user show energy-kiosk.service -p ActiveState -p SubState -p NRestarts; journalctl --user -u energy-kiosk.service -n 15 --no-pager; grep -E '^[[:space:]]*AutomaticLogin(Enable)?=' /etc/gdm3/custom.conf || true")
            with client.open_sftp() as sftp:
                sftp.put("check_running_tachyon.py", "/home/particle/kiosk/check_running_tachyon.py")
            run(client, "/home/particle/kiosk/.venv/bin/python /home/particle/kiosk/check_running_tachyon.py")
        else:
            raise ValueError("Unknown deployment step")
    finally:
        client.close()


if __name__ == "__main__":
    main()
