"""On-device browser smoke test, using the desktop's environment via systemd."""
import json
from pathlib import Path
import kiosk

drivers = []
try:
    width, height = kiosk.get_screen_size()
    print("Screen:", width, height, flush=True)
    for name, url, profile, x, w, spoof in (
        ("left", kiosk.LEFT_URL, kiosk.LEFT_PROFILE, 0, width // 2, False),
        ("right", kiosk.RIGHT_URL, kiosk.RIGHT_PROFILE, width // 2, width - width // 2, True),
    ):
        driver = kiosk.create_browser(url, profile, x, 32, w, height - 32, spoof)
        drivers.append(driver)
        print(json.dumps({"window": name, "title": driver.title,
                          "url": driver.current_url, "rect": driver.get_window_rect()}), flush=True)
        driver.save_screenshot(str(Path.home() / "kiosk" / f"smoke-{name}.png"))
finally:
    for driver in drivers:
        kiosk.close_browser(driver)
