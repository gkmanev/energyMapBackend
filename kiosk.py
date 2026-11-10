import os
import logging
import signal
import sys
import time
import shutil
import threading
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from pathlib import Path
import tkinter as tk

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


# Inherit DISPLAY and XAUTHORITY from the logged-in desktop session.


LEFT_URL = "https://visualize.energy"
RIGHT_URL = (
    "https://blynk.cloud/dashboard/12845/global/devices/1472/"
    "organization/12845/devices/157718/dashboard"
)


def click_blynk_web_console(driver):
    try:
        wait = WebDriverWait(driver, 20)

        button = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//button[contains(translate(normalize-space(.), "
                    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                    "'abcdefghijklmnopqrstuvwxyz'), "
                    "'go to web console')]"
                    " | "
                    "//a[contains(translate(normalize-space(.), "
                    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                    "'abcdefghijklmnopqrstuvwxyz'), "
                    "'go to web console')]",
                )
            )
        )

        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});",
            button,
        )

        try:
            button.click()
        except WebDriverException:
            driver.execute_script("arguments[0].click();", button)

        logging.info("Clicked Blynk 'Go to Web Console' button")
        return True

    except TimeoutException:
        logging.warning(
            "Blynk 'Go to Web Console' button was not found"
        )
        return False


def get_screen_size():
    root = tk.Tk()
    root.withdraw()

    width = root.winfo_screenwidth()
    height = root.winfo_screenheight()

    root.destroy()

    return width, height


# Set this to the height of your desktop panel if one is visible.
PANEL_HEIGHT = int(os.environ.get("KIOSK_PANEL_HEIGHT", "0"))

REFRESH_INTERVAL = 5 * 60
HEALTH_CHECK_INTERVAL = 10
RESTART_DELAY = 5

def find_binary(variable, candidates):
    override = os.environ.get(variable)
    for candidate in ([override] if override else candidates):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise RuntimeError(f"Set {variable} to an installed executable: {candidates}")

HOME = Path.home()

PROFILE_ROOT = Path(os.environ.get(
    "KIOSK_PROFILE_ROOT", str(HOME / "snap/chromium/common/kiosk")
)).expanduser()
LEFT_PROFILE = PROFILE_ROOT / "left"
RIGHT_PROFILE = PROFILE_ROOT / "right"

running = True
stop_event = threading.Event()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def stop_program(signum=None, frame=None):
    global running

    logging.info("Stopping kiosk...")
    running = False
    stop_event.set()


def create_browser(
    url: str,
    profile_path: Path,
    x: int,
    y: int,
    width: int,
    height: int,
    spoof_desktop: bool = False,
):
    profile_path.mkdir(parents=True, exist_ok=True)

    options = Options()
    # The Snap's ChromeDriver must launch its bundled executable directly;
    # /snap/bin/chromium cannot be executed inside its confined environment.
    options.binary_location = find_binary("CHROMIUM_BINARY", [
        "/snap/chromium/current/usr/lib/chromium-browser/chrome",
        "/snap/bin/chromium", "chromium", "chromium-browser",
    ])
    # Native Wayland clients cannot reliably request absolute window positions.
    options.add_argument("--ozone-platform=x11")
    options.page_load_strategy = "eager"

    options.add_argument(f"--user-data-dir={profile_path}")
    options.add_argument(f"--window-position={x},{y}")
    options.add_argument(f"--window-size={width},{height}")

    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-session-crashed-bubble")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-translate")
    options.add_argument("--noerrdialogs")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--password-store=basic")
    # Keep hardware acceleration available for the dashboards.
    options.add_argument("--enable-gpu")
    options.add_argument("--enable-webgl")
    options.add_argument("--ignore-gpu-blocklist")

    # Removes tabs and address bar.
    options.add_argument(f"--app={url}")

    service = Service(find_binary("CHROMEDRIVER_BINARY", [
        "/snap/bin/chromium.chromedriver", "chromedriver",
        "chromium.chromedriver",
    ]), log_output=os.environ.get("KIOSK_DRIVER_LOG"),
        service_args=["--verbose"] if os.environ.get("KIOSK_DRIVER_LOG") else None)

    logging.info(
        "Opening %s at x=%s y=%s width=%s height=%s",
        url,
        x,
        y,
        width,
        height,
    )

    driver = webdriver.Chrome(
        service=service,
        options=options,
    )

    try:
        driver.set_window_rect(
            x=x,
            y=y,
            width=width,
            height=height,
        )

        driver.set_page_load_timeout(45)
        driver.set_script_timeout(15)
        if spoof_desktop:
            # Use the actual browser version while presenting a desktop to Blynk.
            version = driver.capabilities["browserVersion"]
            major = version.split(".")[0]
            driver.execute_cdp_cmd("Network.setUserAgentOverride", {
                "userAgent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    f"(KHTML, like Gecko) Chrome/{version} Safari/537.36"
                ),
                "platform": "Linux x86_64",
                "userAgentMetadata": {
                    "brands": [{"brand": "Chromium", "version": major}],
                    "fullVersionList": [{"brand": "Chromium", "version": version}],
                    "platform": "Linux", "platformVersion": "",
                    "architecture": "x86", "model": "", "mobile": False,
                    "bitness": "64", "wow64": False,
                },
            })

        driver.get(url)

        if "blynk.cloud" in url:
            stop_event.wait(2)
            click_blynk_web_console(driver)

        if spoof_desktop:
            log_browser_information(driver)

        return driver
    except Exception:
        close_browser(driver)
        raise


def log_browser_information(driver):
    try:
        info = driver.execute_script(
            """
            const canvas = document.createElement('canvas');

            const webgl =
                canvas.getContext('webgl2') ||
                canvas.getContext('webgl') ||
                canvas.getContext('experimental-webgl');

            return {
                userAgent: navigator.userAgent,
                platform: navigator.platform,
                mobile:
                    navigator.userAgentData
                    ? navigator.userAgentData.mobile
                    : null,
                width: window.innerWidth,
                height: window.innerHeight,
                screenWidth: screen.width,
                screenHeight: screen.height,
                webgl: Boolean(webgl)
            };
            """
        )

        logging.info("Blynk browser information: %s", info)

    except WebDriverException as error:
        logging.warning(
            "Could not read Blynk browser information: %s",
            error,
        )


def browser_is_alive(driver) -> bool:
    if driver is None:
        return False

    try:
        driver.execute_script("return document.readyState")
        return True

    except WebDriverException:
        return False


def close_browser(driver):
    if driver is None:
        return

    try:
        driver.quit()

    except Exception:
        pass


def refresh_browser(driver, name: str):
    try:
        driver.refresh()

        if name == "Right":
            stop_event.wait(2)
            click_blynk_web_console(driver)

        logging.info("%s window refreshed", name)
        return driver

    except WebDriverException as error:
        logging.error("%s refresh failed: %s", name, error)
        close_browser(driver)
        return None


def main():
    signal.signal(signal.SIGTERM, stop_program)
    signal.signal(signal.SIGINT, stop_program)
    if not os.environ.get("DISPLAY"):
        raise RuntimeError("Start kiosk from the graphical desktop with DISPLAY set (X11/XWayland).")
    while running:
        try:
            SCREEN_WIDTH, SCREEN_HEIGHT = get_screen_size()
            break
        except tk.TclError as error:
            logging.warning("Waiting for desktop display: %s", error)
            stop_event.wait(RESTART_DELAY)
    if not running:
        return
    usable_height = SCREEN_HEIGHT - PANEL_HEIGHT
    if usable_height <= 0 or SCREEN_WIDTH < 2 or PANEL_HEIGHT < 0:
        raise ValueError("Invalid screen size or KIOSK_PANEL_HEIGHT")
    half_width = SCREEN_WIDTH // 2

    left_driver = None
    right_driver = None

    last_refresh = time.monotonic()

    logging.info(
        "Detected screen size: %sx%s",
        SCREEN_WIDTH,
        SCREEN_HEIGHT,
    )

    try:
        while running:
            if not browser_is_alive(left_driver):
                close_browser(left_driver)
                left_driver = None

                try:
                    left_driver = create_browser(
                        url=LEFT_URL,
                        profile_path=LEFT_PROFILE,
                        x=0,
                        y=PANEL_HEIGHT,
                        width=half_width,
                        height=usable_height,
                        spoof_desktop=False,
                    )

                    logging.info("Left window started")

                except Exception as error:
                    logging.error(
                        "Could not start left window: %s",
                        error,
                    )

            if not browser_is_alive(right_driver):
                close_browser(right_driver)
                right_driver = None

                try:
                    right_driver = create_browser(
                        url=RIGHT_URL,
                        profile_path=RIGHT_PROFILE,
                        x=half_width,
                        y=PANEL_HEIGHT,
                        width=SCREEN_WIDTH - half_width,
                        height=usable_height,
                        spoof_desktop=True,
                    )

                    logging.info("Right window started")

                except Exception as error:
                    logging.error(
                        "Could not start right window: %s",
                        error,
                    )

            current_time = time.monotonic()

            if current_time - last_refresh >= REFRESH_INTERVAL:
                if left_driver is not None:
                    left_driver = refresh_browser(
                        left_driver,
                        "Left",
                    )

                if right_driver is not None:
                    right_driver = refresh_browser(
                        right_driver,
                        "Right",
                    )

                last_refresh = current_time

            if left_driver is None or right_driver is None:
                stop_event.wait(RESTART_DELAY)
            else:
                stop_event.wait(HEALTH_CHECK_INTERVAL)
    finally:
        close_browser(left_driver)
        close_browser(right_driver)

    logging.info("Kiosk stopped")


if __name__ == "__main__":
    try:
        main()

    except Exception:
        logging.exception("Fatal kiosk error")
        sys.exit(1)
