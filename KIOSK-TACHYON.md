# Tachyon kiosk on Ubuntu 24.04 Desktop

Run this directly on the Tachyon desktop as your normal login user. The repository's
Dockerfile is for the backend and installs an amd64 browser; it is not this kiosk's
deployment route. Only `kiosk.py` and `requirements-kiosk.txt` are needed.

## Current installation on particle@192.168.1.76

The kiosk is installed in `/home/particle/kiosk` with its own `.venv`.
Chromium and ChromeDriver come from the same ARM64 Snap. The Snap's driver must
launch `/snap/chromium/current/usr/lib/chromium-browser/chrome`, rather than the
`/snap/bin/chromium` launcher; the script selects that executable automatically.

The enabled user service is `~/.config/systemd/user/energy-kiosk.service`.
It starts with the GNOME graphical session, restarts after Python failures, and
inhibits idle and suspend while running. A 32-pixel top panel allowance is set in
the service. Do not add the desktop autostart entry below on this device, since
the service already starts the kiosk.

From a terminal in the Tachyon desktop:

```bash
systemctl --user status energy-kiosk
systemctl --user restart energy-kiosk
systemctl --user stop energy-kiosk
journalctl --user -u energy-kiosk -f
```

Live checks confirmed the 3840x2160 display, two 1920-pixel-wide windows, loaded
energy data, and available WebGL. Blynk currently requires an interactive login
in the right window. Reboot recovery and the five-minute refresh cycle have not
yet been tested. Automatic desktop login has not been configured by this deployment.

## Install

Copy those two files into `~/kiosk` on the Tachyon, then open a terminal on its
graphical desktop and run:

```bash
sudo apt update
sudo apt install python3-venv python3-tk xwayland snapd
sudo snap install chromium
cd ~/kiosk
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-kiosk.txt
/snap/bin/chromium --version
/snap/bin/chromium.chromedriver --version
.venv/bin/python kiosk.py
```

If Chromium is already installed, skip `snap install`. Use Chromium and ChromeDriver
from the same Snap so their versions track each other. Both must be ARM64 builds
on the Tachyon. The script discovers the Snap commands before other system commands.

Log into Blynk in the right-hand kiosk window. Profiles are persistent and separate:
`~/snap/chromium/common/kiosk/left` and `~/snap/chromium/common/kiosk/right`.
An existing login in your ordinary Chromium profile will not be reused. The optional
"Go to Web Console" button may be absent once already logged into the dashboard.

Stop with Ctrl+C in the terminal. Run only one instance at a time to avoid profile locks.

## Desktop and display

The script inherits `DISPLAY` and `XAUTHORITY` from the desktop; do not launch it
with sudo or from a bare SSH shell. Chromium uses X11, including through XWayland
in a Wayland session, to request side-by-side positions. If positioning fails under
Wayland, log out, select an Ubuntu on Xorg session if offered, and retry.

Use one monitor at 100% scaling for the first test. Tk's screen size can span
multiple monitors; multi-monitor placement and fractional scaling need manual
verification. In Ubuntu Settings, turn off screen blanking, automatic suspend,
and automatic screen lock for the dedicated kiosk account. Enable automatic login
if it must recover after power loss without someone logging in.

For a visible top panel, run for example:

```bash
KIOSK_PANEL_HEIGHT=32 .venv/bin/python kiosk.py
```

Other optional environment settings are `CHROMIUM_BINARY`, `CHROMEDRIVER_BINARY`,
and `KIOSK_PROFILE_ROOT`. Keep profiles inside Chromium's Snap-accessible directory
when using the Snap. A custom browser installation needs a matching driver.

## Start after desktop login

After the manual test succeeds, create a desktop autostart entry. This inherits
the correct desktop environment and appends logs to `~/kiosk/kiosk.log`:

```bash
mkdir -p ~/.config/autostart
cat > ~/.config/autostart/energy-kiosk.desktop <<EOF
[Desktop Entry]
Type=Application
Name=Energy kiosk
Exec=/bin/sh -c 'exec "$HOME/kiosk/.venv/bin/python" "$HOME/kiosk/kiosk.py" >> "$HOME/kiosk/kiosk.log" 2>&1'
Terminal=false
X-GNOME-Autostart-enabled=true
EOF
```

This starts after login. The script retries failed browser sessions, but this
autostart entry does not supervise or restart a terminated Python process.

## Acceptance checks on the device

1. Both windows occupy their intended halves and both dashboards render.
2. Blynk stays logged in after stopping and restarting the script.
3. Wait at least five minutes and confirm both dashboards refresh correctly.
4. Close each browser window and confirm it is recreated.
5. Disconnect and reconnect networking, then confirm both dashboards recover.
6. Reboot and confirm automatic login, kiosk startup, and a usable display.

Logs include browser information and WebGL availability for the Blynk window.
GPU behavior depends on the Tachyon image and must be verified on the device.
The current health check detects unresponsive or closed browser sessions; it does
not prove that dashboard data is fresh or that a Blynk login is still valid.

References: [Particle Ubuntu releases](https://developer.particle.io/tachyon/software/ubuntu),
[Ubuntu Chromium Snap packaging](https://ubuntu.com/blog/chromium-in-ubuntu-deb-to-snap-transition),
[Chromium Snap](https://snapcraft.io/chromium).
