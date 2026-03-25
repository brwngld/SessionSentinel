import time
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils import log

def go_to_cargo(driver):
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Cargo"))
    ).click()
    log("Navigated to Cargo menu")

def go_to_clearance(driver):
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Clearance"))
    ).click()
    log("Navigated to Clearance menu")

#Submenu for clearance
def go_to_register_declaration(driver):
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.ID, "Register Declaration"))
    ).click()
    log("Navigated to Register Declaration menu")

def go_to_search_boe(driver):
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "CLM01S01V02"))
    ).click()
    log("Navigated to Search BOE menu")

def go_to_declaration_report(driver):
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "Declaration Report"))
    ).click()
    log("Navigated to Declaration Report menu")

def go_to_non_exited_boe(driver):
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "List of non-exited BOE"))
    ).click()
    log("Navigated to Non-Exited BOE menu")

def go_to_single_window(driver):
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "Single Window"))
    ).click()
    log("Navigated to Single Window menu")

def navigate_to_section(driver, section_name):
    if section_name.lower() == "cargo":
        go_to_cargo(driver)
    elif section_name.lower() == "clearance":
        go_to_clearance(driver)
    elif section_name.lower() == "single window":
        go_to_single_window(driver)
    else:
        log(f"Unknown section: {section_name}")


def search_date_range(driver, start_date, end_date, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            # Enter start date
            start = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "startSubmissionDtts"))
            )
            start.clear()
            start.send_keys(start_date)

            # Enter end date
            end = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "endSubmissionDtts"))
            )
            end.clear()
            end.send_keys(end_date)

            # Click search button
            search_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "searchBtn"))
            )
            search_btn.click()

            # ✅ Check for popup (only if visible)
            try:
                popup = WebDriverWait(driver, 3).until(
                    EC.visibility_of_element_located((By.ID, "messagePopup"))
                )
                log("Validation popup detected, closing and retrying...")
                close_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class,'dialog_close')]"))
                )
                close_btn.click()
                retries += 1
                continue  # retry loop
            except TimeoutException:
                # No visible popup, search succeeded
                log(f"Entered search date range: {start_date} to {end_date} and triggered search")
                return True

        except StaleElementReferenceException:
            log("Stale element — retrying search_date_range")
            retries += 1
            continue
        except (TimeoutException, NoSuchElementException) as e:
            log(f"Search failed: {e}")
            return False

    log("Max retries reached, search_date_range failed")
    return False