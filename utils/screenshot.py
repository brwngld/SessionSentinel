import os
import datetime
import re
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoAlertPresentException, TimeoutException
from config import screenshot_dir




def capture_debug_state(driver, log_func, step_name="login", retry_count=None):
    """
    Takes a timestamped screenshot and dismisses any lingering alerts.
    Filenames include step_name, retry_count, and sanitized page title.
    """
    # Sanitize page title or URL to safe filename
    page_info = driver.title if driver.title else driver.current_url
    safe_page_info = re.sub(r'[\\/*?:"<>|]', "_", page_info)[:50]  # max 50 chars

    # Include retry count if provided
    retry_part = f"_retry{retry_count}" if retry_count is not None else ""

    # Timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Construct filename
    screenshot_file = os.path.join(
        screenshot_dir, f"{step_name}{retry_part}_{safe_page_info}_{timestamp}.png"
    )

    # Save screenshot
    try:
        driver.save_screenshot(screenshot_file)
        log_func(f"Screenshot saved: {screenshot_file}")
    except Exception as e:
        log_func(f"Failed to save screenshot: {e}")

    # Handle any lingering alerts
    try:
        WebDriverWait(driver, 2).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        log_func(f"Dismissing unexpected alert: {alert.text}")
        alert.accept()
    except (NoAlertPresentException, TimeoutException):
        log_func("No alert present — safe to continue")