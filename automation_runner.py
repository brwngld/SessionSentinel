from selenium import webdriver
from selenium.webdriver.edge.service import Service

from config import (
    DEFAULT_END_DATE,
    DEFAULT_PAGE_SIZE,
    DEFAULT_START_DATE,
    EDGE_DRIVER_PATH,
    MAX_RETRIES,
    OUTPUT_DIR,
    UNIPASS_PASSWORD,
    UNIPASS_URL,
    UNIPASS_USER,
)
from helpers.data_processing import export_all_formats, scrape_headers
from helpers.dialogs import handle_login_alert
from helpers.login import login
from helpers.logout import logout
from helpers.navigation import (
    go_to_clearance,
    go_to_declaration_report,
    go_to_non_exited_boe,
    go_to_search_boe,
    search_date_range,
    search_non_exited_boe_blocking,
    search_non_exited_boe_status_by_date,
)
from helpers.pagination import scrape_boe_by_date, set_page_size
from utils import capture_debug_state, log


def run_session(
    user_name=None,
    user_password=None,
    start_date=None,
    end_date=None,
    page_size=None,
    label="BOE Account",
    output_dir=None,
    edge_driver_path=None,
    headless=False,
    status_callback=None,
    cancel_requested=None,
    retrieval_type="financial",
    elapsed_only=False,
    customs_office_code="",
    im_exporter_code="",
    created_by="M",
):
    """Run the existing Selenium export workflow and return a result payload."""

    user_name = user_name or UNIPASS_USER
    user_password = user_password or UNIPASS_PASSWORD
    start_date = start_date or DEFAULT_START_DATE
    end_date = end_date or DEFAULT_END_DATE
    page_size = int(page_size or DEFAULT_PAGE_SIZE)
    output_dir = output_dir or OUTPUT_DIR
    edge_driver_path = edge_driver_path or EDGE_DRIVER_PATH

    if not user_name or not user_password:
        return {
            "ok": False,
            "message": "Missing portal credentials",
            "row_count": 0,
            "files": {},
        }

    def emit(message):
        log(message)
        if status_callback:
            status_callback(message)

    def should_stop():
        return bool(cancel_requested and cancel_requested())

    service = Service(edge_driver_path)
    options = webdriver.EdgeOptions()
    options.add_argument("--window-size=1920,1080")
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
    else:
        options.add_argument("--start-maximized")

    driver = webdriver.Edge(service=service, options=options)
    driver.get(UNIPASS_URL)
    if not headless:
        try:
            driver.maximize_window()
        except Exception:
            pass

    retries = 0
    login_successful = False

    try:
        if should_stop():
            return {
                "ok": False,
                "stopped": True,
                "message": "Run stopped by user before login",
                "row_count": 0,
                "files": {},
            }

        while retries < MAX_RETRIES:
            if should_stop():
                return {
                    "ok": False,
                    "stopped": True,
                    "message": "Run stopped by user during login",
                    "row_count": 0,
                    "files": {},
                }
            emit("Step 2/6: Logging in")
            login(driver, user_name, user_password)
            login_failed = handle_login_alert(driver)

            if login_failed:
                retries += 1
                emit(f"Login failed. Retry {retries}/{MAX_RETRIES}")
                continue

            if "login.do" in driver.current_url:
                retries += 1
                emit(f"Still on login page. Retry {retries}/{MAX_RETRIES}")
                continue

            login_successful = True
            emit("Step 2/6: Login successful")
            break

        if not login_successful:
            return {
                "ok": False,
                "message": "Max login retries reached",
                "row_count": 0,
                "files": {},
            }

        normalized_retrieval = str(retrieval_type or "financial").lower()

        if normalized_retrieval in {"boe_blocking_current", "boe_status_dates"}:
            emit("Step 3/6: Navigating to non-exited BOE")
            go_to_clearance(driver)
            go_to_non_exited_boe(driver)
        else:
            emit("Step 3/6: Navigating to BOE search")
            go_to_clearance(driver)
            go_to_search_boe(driver)

        if should_stop():
            return {
                "ok": False,
                "stopped": True,
                "message": "Run stopped by user before data search",
                "row_count": 0,
                "files": {},
            }

        if normalized_retrieval == "boe_blocking_current":
            emit("Step 4/6: Running BOE blocking search (Elapsed 60 Days)")
        elif normalized_retrieval == "boe_status_dates":
            emit("Step 4/6: Running BOE status search (Date range)")
        else:
            emit("Step 4/6: Searching by date range")
        if normalized_retrieval == "boe_blocking_current":
            search_ok = search_non_exited_boe_blocking(
                driver,
                start_date,
                end_date,
                elapsed_only=bool(elapsed_only),
                customs_office_code=customs_office_code,
                im_exporter_code=im_exporter_code,
                created_by=created_by,
            )
        elif normalized_retrieval == "boe_status_dates":
            search_ok = search_non_exited_boe_status_by_date(
                driver,
                start_date,
                end_date,
            )
        else:
            search_ok = search_date_range(driver, start_date, end_date)
        if not search_ok:
            if normalized_retrieval == "boe_blocking_current":
                emit("Step 4/6: BOE blocking search failed")
            elif normalized_retrieval == "boe_status_dates":
                emit("Step 4/6: BOE status search failed")
            else:
                emit("Step 4/6: Search failed for selected date range")
            return {
                "ok": False,
                "message": (
                    "BOE blocking search failed"
                    if normalized_retrieval == "boe_blocking_current"
                    else (
                        "BOE status search failed"
                        if normalized_retrieval == "boe_status_dates"
                        else "Search failed for selected date range"
                    )
                ),
                "row_count": 0,
                "files": {},
            }

        emit("Step 5/6: Scraping paginated rows")
        if not set_page_size(driver, page_size):
            emit("Step 5/6: Could not set requested page size; continuing with current page size")
        headers = scrape_headers(driver)
        data, stopped = scrape_boe_by_date(driver, should_stop=should_stop)

        emit(f"Step 5/6: Scraped {len(data)} rows")

        if not data and stopped:
            return {
                "ok": False,
                "stopped": True,
                "message": "Run stopped by user before any rows were exported",
                "row_count": 0,
                "files": {},
            }

        emit("Step 6/6: Exporting files")
        exported_files = export_all_formats(
            data=data,
            headers=headers,
            report_rows=data,
            start_date=start_date,
            label=label,
            output_dir=output_dir,
        )

        emit("Step 6/6: Logging out")
        logout(driver)

        if stopped:
            return {
                "ok": True,
                "stopped": True,
                "message": f"Run stopped by user. Exported {len(data)} rows",
                "row_count": len(data),
                "files": exported_files,
            }

        return {
            "ok": True,
            "message": "Step 6/6: Export completed",
            "row_count": len(data),
            "files": exported_files,
        }
    except Exception as exc:
        capture_debug_state(driver, log, step_name="run_session_failure")
        error_message = str(exc).strip() or "Automation failed. Check latest screenshot in screenshots folder."
        emit(error_message)
        return {
            "ok": False,
            "message": error_message,
            "row_count": 0,
            "files": {},
        }
    finally:
        try:
            driver.quit()
        except Exception:
            pass
