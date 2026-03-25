import os
import time
import datetime
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoAlertPresentException, TimeoutException
from utils import log, capture_debug_state

# Login function
def login(driver, username, password):
    wait = WebDriverWait(driver, 15)

    try:
        log("Clicking login button")
        wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "btn_login"))).click()

        log("Entering username/password")
        wait.until(EC.visibility_of_element_located((By.ID, "userid"))).send_keys(username)
        driver.find_element(By.ID, "userpw").send_keys(password)

        log("Clicking submit/login button")
        wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "submit"))).click()

    except Exception as e:
        log(f"Login step failed: {e}")
        capture_debug_state(driver, log, step_name="login_failure")
        raise  # re-raise so the main script knows login didn’t complete