
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoAlertPresentException, TimeoutException
from utils import log


#Handle alert for invoalid login
def handle_login_alert(driver, timeout=5):
    try:
        WebDriverWait(driver, timeout).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        text = alert.text.strip()
        log(f"Login failed: {text}")
        alert.accept()
        return True  # login failed
    except (TimeoutException, NoAlertPresentException):
        return False  # no alert, probably successful


# Dynamic dialog click (Yes/No)
def click_dialog_button(driver, button_text="Yes", timeout=15):
    wait = WebDriverWait(driver, timeout)
    
    # XPath that finds a button inside a dialog with exact text
    xpath = f"//div[contains(@class,'ui-dialog-buttonset')]//button[text()='{button_text}']"
    
    button = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    button.click()
    log(f"Clicked '{button_text}' button in dialog.")