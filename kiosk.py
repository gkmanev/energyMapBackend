import os
import logging
import signal
import sys
import time
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


os.environ.setdefault("DISPLAY", ":0")
os.environ.setdefault("XAUTHORITY", "/home/entra/.Xauthority")


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


SCREEN_WIDTH, SCREEN_HEIGHT = get_screen_size()

# Set this to the height of your desktop panel if one is visible.
PANEL_HEIGHT = 0

REFRESH_INTERVAL = 5 * 60
HEALTH_CHECK_INTERVAL = 10
RESTART_DELAY = 5

CHROMIUM_BINARY = "/usr/bin/chromium"
CHROMEDRIVER_BINARY = "/usr/bin/chromedriver"

HOME = Path.home()

LEFT_PROFILE = HOME / ".config" / "kiosk-chromium-left"
RIGHT_PROFILE = HOME / ".config" / "kiosk-chromium-right"

DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)

running = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def stop_program(signum=None, frame=None):
    global running

    logging.info("Stopping kiosk...")
    running = False


signal.signal(signal.SIGTERM, stop_program)
signal.signal(signal.SIGINT, stop_program)


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
    options.binary_location = CHROMIUM_BINARY

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
    # Helps WebGL-based dashboards on Raspberry Pi.
    options.add_argument("--enable-gpu")
    options.add_argument("--enable-webgl")
    options.add_argument("--ignore-gpu-blocklist")

    if spoof_desktop:
        options.add_argument(f"--user-agent={DESKTOP_USER_AGENT}")

    # Removes tabs and address bar.
    options.add_argument(f"--app={url}")

    service = Service(CHROMEDRIVER_BINARY)

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

    driver.set_window_rect(
        x=x,
        y=y,
        width=width,
        height=height,
    )

    if spoof_desktop:
        # Override values frequently used for browser compatibility checks.
        driver.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {
                "userAgent": DESKTOP_USER_AGENT,
                "platform": "Linux x86_64",
                "userAgentMetadata": {
                    "brands": [
                        {
                            "brand": "Chromium",
                            "version": "150",
                        },
                        {
                            "brand": "Google Chrome",
                            "version": "150",
                        },
                        {
                            "brand": "Not_A Brand",
                            "version": "99",
                        },
                    ],
                    "fullVersionList": [
                        {
                            "brand": "Chromium",
                            "version": "150.0.7871.124",
                        },
                        {
                            "brand": "Google Chrome",
                            "version": "150.0.7871.124",
                        },
                        {
                            "brand": "Not_A Brand",
                            "version": "99.0.0.0",
                        },
                    ],
                    "fullVersion": "150.0.7871.124",
                    "platform": "Linux",
                    "platformVersion": "6.0.0",
                    "architecture": "x86",
                    "model": "",
                    "mobile": False,
                    "bitness": "64",
                    "wow64": False,
                },
            },
        )

        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'platform', {
                        get: () => 'Linux x86_64'
                    });

                    Object.defineProperty(navigator, 'vendor', {
                        get: () => 'Google Inc.'
                    });

                    Object.defineProperty(navigator, 'maxTouchPoints', {
                        get: () => 0
                    });
                """
            },
        )

    driver.get(url)
   
    if "blynk.cloud" in url:
    	time.sleep(2)
    	click_blynk_web_console(driver)

    if spoof_desktop:
        log_browser_information(driver)

    return driver


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
            time.sleep(2)
            click_blynk_web_console(driver)

        logging.info("%s window refreshed", name)
        return driver

    except WebDriverException as error:
        logging.error("%s refresh failed: %s", name, error)
        close_browser(driver)
        return None


def main():
    usable_height = SCREEN_HEIGHT - PANEL_HEIGHT
    half_width = SCREEN_WIDTH // 2

    left_driver = None
    right_driver = None

    last_refresh = time.monotonic()

    logging.info(
        "Detected screen size: %sx%s",
        SCREEN_WIDTH,
        SCREEN_HEIGHT,
    )

    while running:
        if not browser_is_alive(left_driver):
            close_browser(left_driver)
            left_driver = None

            try:
                left_driver = create_browser(
                    url=LEFT_URL,
                    profile_path=LEFT_PROFILE,
                    x=0,
                    y=0,
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
                    y=0,
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
            time.sleep(RESTART_DELAY)
        else:
            time.sleep(HEALTH_CHECK_INTERVAL)

    close_browser(left_driver)
    close_browser(right_driver)

    logging.info("Kiosk stopped")


if __name__ == "__main__":
    try:
        main()

    except Exception:
        logging.exception("Fatal kiosk error")
        sys.exit(1)
