import time
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    NoAlertPresentException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils import log


def _is_non_exited_boe_page_ready(driver, timeout=8):
    """Return True when Non-Exited BOE page appears loaded."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: (
                "selectNonExitedBoeList" in (d.current_url or "")
                or len(d.find_elements(By.ID, "searchStartDeclarationDt")) > 0
            )
        )
        return True
    except TimeoutException:
        return False


def _accept_alert_if_present(driver):
    """Dismiss unexpected blocking alerts and return True when one was handled."""
    try:
        alert = driver.switch_to.alert
        text = (alert.text or "").strip()
        alert.accept()
        log(f"Dismissed portal alert during BOE navigation: {text}")
        return True
    except NoAlertPresentException:
        return False


def _recover_from_http_404_if_present(driver):
    """Recover from ICUMS HTTP 404 error page by pressing its back button."""
    try:
        has_404_heading = len(driver.find_elements(By.XPATH, "//*[contains(normalize-space(),'HTTP 404 Error') or contains(normalize-space(),'Page not found') ]")) > 0
        has_back_button = len(driver.find_elements(By.XPATH, "//button[contains(normalize-space(),'Back')]")) > 0
        if not (has_404_heading and has_back_button):
            return False

        back_btn = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(normalize-space(),'Back')]"))
        )
        back_btn.click()
        log("Recovered from HTTP 404 page using C Back button")
        return True
    except TimeoutException:
        return False

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


def collapse_register_declaration_if_open(driver):
    """Collapse Register Declaration submenu when it is expanded.

    On this portal, Register Declaration can stay expanded and block access to
    Declaration Report items. This is a best-effort action.
    """
    try:
        anchor = WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//li[a[normalize-space()='Declaration']]//a[normalize-space()='Register Declaration']",
                )
            )
        )
    except TimeoutException:
        log("Register Declaration menu not found for collapse")
        return

    try:
        parent_li = anchor.find_element(By.XPATH, "./parent::li")
        class_name = (parent_li.get_attribute("class") or "").lower()
        submenu = parent_li.find_element(By.XPATH, "./ul")
        submenu_style = (submenu.get_attribute("style") or "").lower()
        is_open = ("on" in class_name or "active" in class_name) or ("display: block" in submenu_style)

        if not is_open:
            log("Register Declaration already collapsed")
            return

        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", anchor)
        try:
            WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//li[a[normalize-space()='Declaration']]//a[normalize-space()='Register Declaration']",
                    )
                )
            ).click()
        except TimeoutException:
            driver.execute_script("arguments[0].click();", anchor)

        WebDriverWait(driver, 5).until(
            lambda d: "display: none"
            in (
                parent_li.find_element(By.XPATH, "./ul").get_attribute("style") or ""
            ).lower()
            or "on" not in ((parent_li.get_attribute("class") or "").lower())
        )
        log("Collapsed Register Declaration menu")
    except (NoSuchElementException, StaleElementReferenceException, TimeoutException):
        log("Could not confirm Register Declaration collapse; continuing")

def go_to_search_boe(driver):
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "CLM01S01V02"))
    ).click()
    log("Navigated to Search BOE menu")

def go_to_declaration_report(driver):
    # Expand Declaration Report group under Clearance -> Declaration (left menu only)
    # without relying on click navigation that can intermittently route to error pages.
    try:
        report_anchor = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//li[a[normalize-space()='Declaration']]//li[a[normalize-space()='Declaration Report']]/a[1]",
                )
            )
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", report_anchor)
        driver.execute_script(
            """
            const a = arguments[0];
            const li = a.closest('li');
            if (!li) return;
            li.classList.add('on');
            li.classList.add('active');
            const ul = li.querySelector(':scope > ul');
            if (ul) ul.style.display = 'block';
            """,
            report_anchor,
        )
        log("Expanded Declaration Report menu")
        return
    except TimeoutException as exc:
        raise TimeoutException("Declaration Report menu not found") from exc

def go_to_non_exited_boe(driver):
    # Stable menu ID from portal HTML.
    menu_item = (By.ID, "CLM01S02V07")
    if _recover_from_http_404_if_present(driver):
        go_to_clearance(driver)

    collapse_register_declaration_if_open(driver)
    go_to_declaration_report(driver)

    menu_count = len(driver.find_elements(*menu_item))
    log(f"Non-Exited BOE menu element count after Declaration Report expand: {menu_count}")
    if menu_count == 0:
        _accept_alert_if_present(driver)
        _recover_from_http_404_if_present(driver)
        raise TimeoutException("Non-Exited BOE menu item not present in expanded menu")

    anchor = WebDriverWait(driver, 8).until(
        EC.presence_of_element_located(menu_item)
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", anchor)

    try:
        WebDriverWait(driver, 4).until(
            EC.element_to_be_clickable(menu_item)
        ).click()
        if _is_non_exited_boe_page_ready(driver, timeout=8):
            log("Navigated to Non-Exited BOE menu")
            return
    except TimeoutException:
        pass

    driver.execute_script("arguments[0].click();", anchor)
    if _is_non_exited_boe_page_ready(driver, timeout=8):
        log("Navigated to Non-Exited BOE menu via JS fallback")
        return

    _accept_alert_if_present(driver)
    if _recover_from_http_404_if_present(driver):
        raise TimeoutException("Non-Exited BOE click redirected to 404 page")
    raise TimeoutException("Non-Exited BOE click did not load target page")

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


def search_non_exited_boe_blocking(
    driver,
    start_date,
    end_date,
    elapsed_only=True,
    customs_office_code="",
    im_exporter_code="",
    created_by="M",
    max_retries=3,
):
    """Run List of non-exited BOE search for blocking items.

    BOE blocking mode intentionally relies on Elapsed 60 Days instead of date
    range inputs to avoid over-constraining results.
    """
    retries = 0
    while retries < max_retries:
        try:
            if customs_office_code:
                office = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "customsOfficeCd"))
                )
                office.clear()
                office.send_keys(customs_office_code)

            if im_exporter_code:
                imex = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "searchImExPorterCd"))
                )
                imex.clear()
                imex.send_keys(im_exporter_code)

            if elapsed_only:
                elapsed_checkbox = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "searchElapsed"))
                )
                if not elapsed_checkbox.is_selected():
                    elapsed_checkbox.click()

            created_by_id = "searchCreatedBy_02" if str(created_by).upper() == "M" else "searchCreatedBy_01"
            try:
                created_radio = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, created_by_id))
                )
                if not created_radio.is_selected():
                    created_radio.click()
            except TimeoutException:
                pass

            if not _click_non_exited_search_button(driver):
                log("Could not locate BOE blocking Search button")
                retries += 1
                continue

            WebDriverWait(driver, 12).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.g-table tbody tr")) > 0
                or len(d.find_elements(By.ID, "messagePopup")) > 0
            )
            log(f"Ran non-exited BOE search with elapsed_only={bool(elapsed_only)}")
            return True
        except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as exc:
            log(f"BOE blocking search retry {retries + 1}/{max_retries}: {exc}")
            retries += 1

    log("Max retries reached, search_non_exited_boe_blocking failed")
    return False


def search_non_exited_boe_status_by_date(driver, start_date, end_date, max_retries=3):
    """Run List of non-exited BOE search using date range only (no elapsed filter)."""
    retries = 0
    while retries < max_retries:
        try:
            start = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "searchStartDeclarationDt"))
            )
            start.clear()
            start.send_keys(start_date)

            end = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "searchEndDeclarationDt"))
            )
            end.clear()
            end.send_keys(end_date)

            try:
                elapsed_checkbox = driver.find_element(By.ID, "searchElapsed")
                if elapsed_checkbox.is_selected():
                    elapsed_checkbox.click()
            except NoSuchElementException:
                pass

            if not _click_non_exited_search_button(driver):
                log("Could not locate BOE status Search button")
                retries += 1
                continue

            WebDriverWait(driver, 12).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.g-table tbody tr")) > 0
                or len(d.find_elements(By.ID, "messagePopup")) > 0
            )
            log(f"Ran non-exited BOE status search: {start_date} to {end_date}")
            return True
        except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as exc:
            log(f"BOE status search retry {retries + 1}/{max_retries}: {exc}")
            retries += 1

    log("Max retries reached, search_non_exited_boe_status_by_date failed")
    return False


def _click_non_exited_search_button(driver):
    """Click Search on Non-Exited BOE page, preferring form-local controls."""
    clicked = False

    try:
        scope_seed = driver.find_element(By.ID, "searchStartDeclarationDt")
        search_scope = scope_seed.find_element(By.XPATH, "ancestor::form[1]")
    except NoSuchElementException:
        search_scope = None

    if search_scope is not None:
        candidate_xpaths = [
            ".//*[@id='searchBtn']",
            ".//button[contains(@class,'g-button') and contains(@class,'search')]",
            ".//button[normalize-space()='Search']",
            ".//input[(translate(@type,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')='button' or translate(@type,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')='submit') and normalize-space(@value)='Search']",
            ".//a[normalize-space()='Search']",
        ]
        for candidate in candidate_xpaths:
            try:
                btn = search_scope.find_element(By.XPATH, candidate)
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                driver.execute_script("arguments[0].click();", btn)
                clicked = True
                break
            except NoSuchElementException:
                continue

    if clicked:
        return True

    search_locators = [
        (By.ID, "searchBtn"),
        (By.CSS_SELECTOR, "button.g-button.search"),
        (By.XPATH, "//button[normalize-space()='Search']"),
        (By.XPATH, "//input[(translate(@type,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')='button' or translate(@type,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')='submit') and normalize-space(@value)='Search']"),
        (By.XPATH, "//a[normalize-space()='Search']"),
    ]
    for by, selector in search_locators:
        try:
            btn = WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((by, selector))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            driver.execute_script("arguments[0].click();", btn)
            return True
        except TimeoutException:
            continue

    return False