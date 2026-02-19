#!/usr/bin/env sh
set -e

# Start a virtual display when running headed Chrome in Docker.
if [ "${CHROME_HEADLESS:-1}" = "0" ]; then
  if [ -z "${DISPLAY:-}" ]; then
    export DISPLAY=:99
  fi
  Xvfb "$DISPLAY" -screen 0 1920x1080x24 -nolisten tcp >/tmp/xvfb.log 2>&1 &
fi

exec "$@"
