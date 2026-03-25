
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils import log, capture_debug_state
from helpers.dialogs import click_dialog_button


# Loging out with a max retries
def logout(driver, max_retries=3):
    """
    Performs a safe logout with retries in case the logout button
    or confirmation modal isn't immediately clickable.
    """
    wait = WebDriverWait(driver, 10)
    
    for attempt in range(1, max_retries + 1):
        try:
            # Step 1: Click logout button
            wait.until(EC.element_to_be_clickable((By.ID, "fwLogout"))).click()
            
            # Step 2: Click "Yes" in confirmation modal
            click_dialog_button(driver, button_text="Yes")
            
            log(f"Logout completed successfully on attempt {attempt}")
            return  # exit function if successful
        
        except Exception as e:
            log(f"Logout attempt {attempt} failed: {e}")
            capture_debug_state(driver, log, step_name="logout", retry_count=attempt)
            time.sleep(1)  # small delay before retry

    log(f"Logout failed after {max_retries} attempts")