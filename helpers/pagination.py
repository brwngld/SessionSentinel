from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException
)

from utils import log

def get_expected_columns(driver):
    """Determine number of columns from the table header."""
    headers = driver.find_elements(By.CSS_SELECTOR, "table.g-table thead th")
    return len(headers)

def scrape_current_page(driver, expected_columns, page_size=None):
    """Scrape a single page, ignoring headers, placeholders, empty rows."""
    rows = driver.find_elements(By.CSS_SELECTOR, "table.g-table tbody tr")
    page_data = []

    for row in rows:
        # skip invisible rows
        if not row.is_displayed():
            continue

        # skip rows that look like headers or placeholders
        row_class = row.get_attribute("class") or ""
        if "header" in row_class.lower() or "placeholder" in row_class.lower():
            continue

        cells = [cell.text.strip().replace("\xa0", "") for cell in row.find_elements(By.TAG_NAME, "td")]

        # skip empty rows
        if not any(cells):
            continue

        # skip rows with wrong number of columns
        if len(cells) != expected_columns:
            continue

        page_data.append(cells)

    # enforce page_size if needed
    if page_size and len(page_data) > page_size:
        page_data = page_data[:page_size]

    return page_data

def get_next_page_button(driver, current_page):
    """Detect next page button or numbered link."""
    # try numbered page link first
    next_page_selector = f"a.g-page__link[href*=\"miv_goPage('{current_page+1}')\"]"
    next_page_links = driver.find_elements(By.CSS_SELECTOR, next_page_selector)
    if next_page_links:
        return next_page_links[0]

    # fallback: generic "Next" button
    next_buttons = driver.find_elements(By.CSS_SELECTOR, "a.g-page__link.next")
    if next_buttons:
        return next_buttons[0]

    return None

def scrape_all_pages(driver, page_size=None, should_stop=None):
    """Scrape all pages of the table safely."""
    all_data = []
    current_page = 1
    expected_columns = get_expected_columns(driver)
    stopped = False

    while True:
        try:
            if should_stop and should_stop():
                log("Stop requested — ending pagination and keeping rows collected so far")
                stopped = True
                break

            # scrape current page
            page_data = scrape_current_page(driver, expected_columns, page_size)
            log(f"Scraped {len(page_data)} rows on page {current_page}")
            all_data.extend(page_data)

            # get next page button
            next_button = get_next_page_button(driver, current_page)
            if not next_button or "disabled" in next_button.get_attribute("class"):
                log("No more pages — stopping pagination")
                break

            if should_stop and should_stop():
                log("Stop requested after current page — skipping remaining pages")
                stopped = True
                break

            # wait for old first row to disappear to avoid stale data
            rows = driver.find_elements(By.CSS_SELECTOR, "table.g-table tbody tr")
            old_first_row = rows[0] if rows else None
            next_button.click()
            if old_first_row:
                WebDriverWait(driver, 10).until(EC.staleness_of(old_first_row))
            current_page += 1

        except Exception as e:
            log(f"Pagination stopped due to error: {e}")
            break

    log(f"Total scraped rows: {len(all_data)}")
    return all_data, stopped

def set_page_size(driver, size=100, max_retries=3):
    """Set table page size safely with retries."""
    retries = 0
    while retries < max_retries:
        try:
            table_candidates = driver.find_elements(By.CSS_SELECTOR, "table.g-table")
            old_table = table_candidates[0] if table_candidates else None

            dropdown = None
            dropdown_locators = [
                (By.CSS_SELECTOR, "select.text_input.paging"),
                (By.CSS_SELECTOR, "select.paging"),
                (By.CSS_SELECTOR, "select[name*='page']"),
                (By.CSS_SELECTOR, "select[id*='page']"),
                (By.CSS_SELECTOR, ".g-page select"),
                (By.CSS_SELECTOR, ".g-paging select"),
            ]

            for by, selector in dropdown_locators:
                elements = driver.find_elements(by, selector)
                if elements:
                    dropdown = elements[0]
                    break

            if dropdown is None:
                log("Page size control not found on this screen")
                return False

            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", dropdown)
            select = Select(dropdown)

            # Build flexible option map from visible text and value.
            option_values = {}
            for opt in select.options:
                text_key = (opt.text or "").strip()
                value_key = (opt.get_attribute("value") or "").strip()
                if text_key:
                    option_values[text_key] = value_key or text_key
                if value_key:
                    option_values[value_key] = value_key

            requested = str(size).strip()
            if requested not in option_values:
                log(f"Page size {requested} not available on this screen")
                return False

            selected_value = option_values[requested]

            # Try normal select API first.
            try:
                if requested in [opt.text.strip() for opt in select.options]:
                    select.select_by_visible_text(requested)
                else:
                    select.select_by_value(selected_value)
            except Exception:
                # Fallback to JS change event for flaky custom controls.
                driver.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('change', { bubbles: true }));",
                    dropdown,
                    selected_value,
                )

            log(f"Set page size to {requested}")

            if old_table is not None:
                try:
                    WebDriverWait(driver, 8).until(EC.staleness_of(old_table))
                except TimeoutException:
                    pass
            return True

        except StaleElementReferenceException:
            log("Stale element when setting page size — retrying...")
            retries += 1
            continue
        except Exception as e:
            log(f"Failed to set page size ({type(e).__name__})")
            retries += 1
            continue

    log("Max retries reached, set_page_size failed")
    return False


def scrape_boe_by_date(driver, should_stop=None):
    """Main entry to scrape all table pages safely."""
    results, stopped = scrape_all_pages(driver, should_stop=should_stop)
    return results, stopped