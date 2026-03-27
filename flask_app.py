import os
import re
import secrets
import threading
import uuid
from difflib import SequenceMatcher
from datetime import datetime, timedelta, timezone
from io import BytesIO
from io import StringIO

import pandas as pd
from flask import Flask, abort, current_app, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

from automation_runner import run_session
from config import (
    APP_ADMIN_PASSWORD_HASH,
    APP_ADMIN_USER,
    ALLOW_DEV_ADMIN_SETUP,
    CREDENTIAL_ENCRYPTION_KEY,
    DATABASE_PATH,
    DB_BACKEND,
    DEFAULT_END_DATE,
    DEFAULT_PAGE_SIZE,
    DEFAULT_START_DATE,
    EDGE_DRIVER_PATH,
    FILE_RETENTION_HOURS,
    RUN_RETENTION_DAYS,
    FLASK_SECRET_KEY,
    OUTPUT_DIR,
    REMEMBER_ME_DAYS,
    LOGIN_LOCK_MINUTES,
    LOGIN_MAX_ATTEMPTS,
    MANUAL_UPLOAD_RETENTION_DAYS,
    SESSION_COOKIE_SECURE,
    SESSION_TIMEOUT_MINUTES,
    TURSO_DATABASE_URL,
    PASSWORD_MAX_AGE_DAYS_ADMIN,
    PASSWORD_MAX_AGE_DAYS_USER,
    PASSWORD_EXPIRY_WARNING_DAYS,
)
from credential_store import (
    CredentialDecryptionError,
    clear_failed_login,
    delete_app_user,
    get_account_alias_rules_for_user,
    delete_expired_generated_files,
    delete_expired_retrieval_runs,
    delete_generated_files_for_job,
    delete_retrieval_run,
    delete_portal_credentials,
    ensure_app_user,
    get_generated_file,
    get_app_user,
    get_app_user_by_email,
    get_recent_auth_events,
    get_recent_auth_events_for_user,
    get_retrieval_run_for_user,
    list_recent_retrieval_runs_for_user,
    get_portal_credentials,
    list_manual_upload_runs_for_user,
    init_db,
    list_app_users,
    log_auth_event,
    register_failed_login,
    save_generated_file,
    save_portal_credentials,
    set_user_active,
    set_user_email,
    set_user_password,
    set_user_role,
    set_manual_upload_pinned,
    set_user_company_profile,
    upsert_account_pricing_profile,
    get_account_pricing_profile,
    list_account_pricing_profiles_for_file,
    list_account_pricing_rate_history,
    record_account_pricing_rate_history,
    delete_account_pricing_profile,
    upsert_retrieval_run,
)
from services.account_matching import (
    build_account_report as service_build_account_report,
    build_decision_ref_key as service_build_decision_ref_key,
    resolve_account_columns as service_resolve_account_columns,
)
from services.account_export import (
    build_all_accounts_pdf_bytes,
    build_all_accounts_view_html,
    build_account_pdf_bytes,
    build_account_report_dataframe,
    build_account_view_html,
)
from routes.reports_account_routes import build_reports_account_payload
from routes.reports_account_routes import (
    build_reports_account_assign_payload,
    build_reports_account_custom_add_payload,
    build_reports_account_custom_remove_payload,
    build_reports_account_delete_payload,
    build_reports_account_decision_payload,
    build_reports_account_rename_payload,
)

app = Flask(__name__)
app.config["SECRET_KEY"] = FLASK_SECRET_KEY
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SECURE"] = SESSION_COOKIE_SECURE
app.config["REMEMBER_COOKIE_HTTPONLY"] = True
app.config["REMEMBER_COOKIE_DURATION"] = timedelta(days=REMEMBER_ME_DAYS)
app.permanent_session_lifetime = timedelta(minutes=SESSION_TIMEOUT_MINUTES)


def _startup_db_message():
    if DB_BACKEND == "sqlite":
        db_path = DATABASE_PATH if os.path.isabs(DATABASE_PATH) else os.path.abspath(DATABASE_PATH)
        return f"[startup] DB backend: sqlite | path={db_path}"

    turso_target = TURSO_DATABASE_URL or "<missing>"
    return f"[startup] DB backend: turso | url={turso_target}"

login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)
app.logger.info(_startup_db_message())
init_db()
if APP_ADMIN_PASSWORD_HASH and not APP_ADMIN_PASSWORD_HASH.startswith("replace-"):
    ensure_app_user(APP_ADMIN_USER, APP_ADMIN_PASSWORD_HASH, role="admin", is_active=True)

# Keep the configured primary admin account privileged, even for existing DB rows.
existing_admin = get_app_user(APP_ADMIN_USER)
if existing_admin:
    set_user_role(APP_ADMIN_USER, "admin")

_jobs = {}
_jobs_lock = threading.Lock()
_job_cancel_events = {}


class AppUser(UserMixin):
    def __init__(self, user_id, role="user", is_active=True):
        self.id = user_id
        self.role = role
        self._is_active = bool(is_active)

    @property
    def is_admin(self):
        return self.role == "admin"

    @property
    def is_active(self):
        return self._is_active


@login_manager.user_loader
def load_user(user_id):
    user = get_app_user(user_id)
    if user and bool(user.get("is_active", 0)):
        return AppUser(user_id, role=user.get("role", "user"), is_active=user.get("is_active", 1))
    return None


def _new_csrf_token():
    token = secrets.token_urlsafe(24)
    session["csrf_token"] = token
    return token


def _validate_csrf_or_abort():
    form_token = request.form.get("csrf_token", "")
    session_token = session.get("csrf_token", "")
    if not form_token or not session_token or form_token != session_token:
        abort(400, description="Invalid CSRF token")


def _client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr


def _admin_required_or_403():
    if not getattr(current_user, "is_admin", False):
        abort(403)


def _post_redirect_endpoint(default_endpoint="index"):
    target = request.form.get("return_to", default_endpoint).strip().lower()
    if target == "profile":
        return "profile"
    if target == "password_required":
        return "password_required"
    return default_endpoint


def _to_date_picker_value(value):
    """Normalize supported date strings to YYYY-MM-DD for HTML date inputs."""
    if not value:
        return ""
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return value


def _to_portal_date(value):
    """Accept common formats and normalize to DD/MM/YYYY for portal automation."""
    if not value:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(value, fmt).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return value


def _is_file_expired(file_path):
    if not os.path.exists(file_path):
        return True
    age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(file_path))
    return age > timedelta(hours=FILE_RETENTION_HOURS)


def _cleanup_old_generated_files(owner_id):
    # Storage is DB-backed; cleanup is global and no longer tied to in-memory paths.
    delete_expired_generated_files(FILE_RETENTION_HOURS, MANUAL_UPLOAD_RETENTION_DAYS)
    delete_expired_retrieval_runs(RUN_RETENTION_DAYS)


def _session_log_path():
    return os.path.join(app.root_path, "session.log")


def _to_utc_datetime(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _password_max_age_days_for_role(role):
    if str(role or "").lower() == "admin":
        return max(1, int(PASSWORD_MAX_AGE_DAYS_ADMIN))
    return max(1, int(PASSWORD_MAX_AGE_DAYS_USER))


def _looks_placeholder(value):
    text = str(value or "").strip().lower()
    if not text:
        return True
    markers = ("replace-", "change-this", "change-me", "placeholder")
    return text.startswith(markers) or text in {"default", "test", "dummy"}


def _security_config_warnings():
    warnings = []

    if _looks_placeholder(CREDENTIAL_ENCRYPTION_KEY):
        warnings.append(
            "Security warning: credential encryption key appears placeholder; saved portal credentials are not production-safe."
        )

    if _looks_placeholder(FLASK_SECRET_KEY) or len(str(FLASK_SECRET_KEY or "").strip()) < 32:
        warnings.append("Security warning: Flask secret key appears weak or placeholder.")

    if _looks_placeholder(APP_ADMIN_PASSWORD_HASH):
        warnings.append("Security warning: admin password hash is still placeholder.")

    return warnings


def _flash_security_warnings(scope):
    warnings = _security_config_warnings()
    if not warnings:
        return

    today = datetime.now(timezone.utc).date().isoformat()
    seen = session.get("security_warning_seen")
    if not isinstance(seen, dict):
        seen = {}

    for message in warnings:
        key = f"{scope}:{message}"
        if seen.get(key) == today:
            continue
        flash(message, "error")
        seen[key] = today

    session["security_warning_seen"] = seen


def _get_portal_credentials_or_recover(user_id, flash_on_error=False):
    try:
        return get_portal_credentials(user_id)
    except CredentialDecryptionError:
        delete_portal_credentials(user_id)
        log_auth_event(user_id, "portal_credentials_decrypt", "failed", _client_ip(), "invalid token; stale credentials removed")
        if flash_on_error:
            flash(
                "Saved portal credentials could not be decrypted with the current key and were cleared. Please save credentials again.",
                "error",
            )
        return None


def _password_policy_state(user_record):
    must_change = bool(user_record.get("must_change_password", 0))
    changed_at = _to_utc_datetime(user_record.get("password_changed_at"))
    max_age_days = _password_max_age_days_for_role(user_record.get("role"))

    if not changed_at:
        return {
            "must_change": must_change,
            "expired": False,
            "days_remaining": None,
            "max_age_days": max_age_days,
        }

    now_utc = datetime.now(timezone.utc)
    expiry_at = changed_at + timedelta(days=max_age_days)
    remaining_seconds = (expiry_at - now_utc).total_seconds()
    expired = remaining_seconds <= 0
    days_remaining = 0 if expired else int(remaining_seconds // 86400) + 1

    return {
        "must_change": must_change,
        "expired": expired,
        "days_remaining": days_remaining,
        "max_age_days": max_age_days,
    }


def _normalize_email(value):
    email = (value or "").strip().lower()
    if not email:
        return ""
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        return None
    return email


def _company_profile_from_user_record(user_record):
    user_record = user_record or {}
    return {
        "name": str(user_record.get("company_name") or "").strip(),
        "address": str(user_record.get("company_address") or "").strip(),
        "phone": str(user_record.get("company_phone") or "").strip(),
        "logo_path": str(user_record.get("company_logo_path") or "").strip(),
    }


def _normalize_pricing_profile(profile):
    profile = profile or {}
    mode = str(profile.get("pricing_mode") or "none").strip().lower()
    if mode not in {"none", "automatic", "manual"}:
        mode = "none"
    try:
        fixed_price = float(profile.get("fixed_price") or 0)
    except (TypeError, ValueError):
        fixed_price = 0.0
    line_prices = profile.get("line_prices") if isinstance(profile.get("line_prices"), dict) else {}
    currency_code = str(profile.get("currency_code") or "GHS").strip().upper()
    if currency_code not in {"GHS", "USD"}:
        currency_code = "GHS"
    try:
        manual_rate = float(profile.get("manual_rate")) if profile.get("manual_rate") is not None else None
    except (TypeError, ValueError):
        manual_rate = None
    if manual_rate is not None and manual_rate <= 0:
        manual_rate = None
    conversion_note = str(profile.get("conversion_note") or "").strip()
    return mode, max(0.0, fixed_price), line_prices, currency_code, manual_rate, conversion_note


def _build_conversion_note(currency_code, manual_rate):
    code = str(currency_code or "GHS").strip().upper()
    if code == "USD" and manual_rate is not None and float(manual_rate) > 0:
        return f"Converted with manual rate: 1 USD = {float(manual_rate):,.4f} GHS"
    return ""


def _resolve_line_amount(mode, fixed_price, line_prices, line_key):
    if mode == "automatic":
        return float(fixed_price)
    if mode == "manual":
        raw = line_prices.get(str(line_key), 0)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _calculate_account_amount_total(rows, pricing_profile):
    mode, fixed_price, line_prices, _currency_code, _manual_rate, _conversion_note = _normalize_pricing_profile(pricing_profile)
    if mode == "none":
        return 0.0
    total = 0.0
    for row in rows or []:
        line_key = str(row.get("source_idx") if row.get("source_idx") is not None else "")
        total += _resolve_line_amount(mode, fixed_price, line_prices, line_key)
    return float(total)


def _build_priced_account_export_df(report_df):
    export_df = report_df.copy()
    if "_week_label" in export_df.columns:
        export_df["Week"] = export_df["_week_label"]
    ordered = [c for c in ["Week", "User Ref", "BOE Number", "Submission Date", "Amount"] if c in export_df.columns]
    if not ordered:
        ordered = [c for c in export_df.columns if not str(c).startswith("_")]
    return export_df[ordered]


def _build_report_file_index(user_id):
    runs = list_recent_retrieval_runs_for_user(user_id, limit=500)
    indexed = []
    for run in runs:
        run_payload = run.get("payload") or {}
        files = ((run.get("result") or {}).get("files") or {})
        for key, file_data in files.items():
            if key not in {"csv", "xlsx"}:
                continue
            indexed.append(
                {
                    "job_id": run.get("id"),
                    "file_key": key,
                    "file_name": (file_data or {}).get("name") if isinstance(file_data, dict) else str(file_data),
                    "created_at": (file_data or {}).get("created_at") if isinstance(file_data, dict) else run.get("created_at"),
                    "status": run.get("status"),
                    "source": (run_payload.get("source") or "retrieval"),
                    "pinned": bool(run_payload.get("pinned", False)),
                    "retrieval_type": str(run_payload.get("retrieval_type") or "financial"),
                }
            )
    return indexed


@app.route("/reports/upload", methods=["POST"])
@login_required
def reports_upload():
    _validate_csrf_or_abort()

    uploaded = request.files.get("report_file")
    if not uploaded or not (uploaded.filename or "").strip():
        flash("Please choose a CSV or XLSX file to upload.", "error")
        return redirect(url_for("reports_dashboard"))

    original_name = secure_filename(uploaded.filename or "")
    ext = os.path.splitext(original_name)[1].lower()
    file_key_map = {".csv": "csv", ".xlsx": "xlsx"}
    file_key = file_key_map.get(ext)
    if not file_key:
        flash("Unsupported file type. Please upload .csv or .xlsx.", "error")
        return redirect(url_for("reports_dashboard"))

    blob = uploaded.read()
    if not blob:
        flash("Uploaded file is empty.", "error")
        return redirect(url_for("reports_dashboard"))

    try:
        if file_key == "csv":
            try:
                df = pd.read_csv(StringIO(blob.decode("utf-8")))
            except UnicodeDecodeError:
                df = pd.read_csv(StringIO(blob.decode("latin-1")))
        else:
            df = pd.read_excel(BytesIO(blob))
    except Exception as exc:
        flash(f"Could not read uploaded file: {exc}", "error")
        return redirect(url_for("reports_dashboard"))

    now_ts = datetime.now(timezone.utc).isoformat()
    job_id = f"manual_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    upsert_retrieval_run(
        job_id=job_id,
        user_id=current_user.id,
        status="completed",
        last_message="Manual report upload",
        row_count=int(len(df.index)),
        created_at=now_ts,
        ended_at=now_ts,
        payload={"source": "manual_upload", "original_name": original_name, "pinned": False},
    )
    save_generated_file(
        job_id=job_id,
        user_id=current_user.id,
        file_key=file_key,
        file_name=original_name,
        mime_type=uploaded.mimetype or ("text/csv" if file_key == "csv" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        file_blob=blob,
        created_at=now_ts,
    )

    flash(f"Uploaded '{original_name}' successfully.", "success")
    return redirect(url_for("reports_dashboard", job_id=job_id, file_key=file_key, page=1))


def _load_dataframe_from_generated_file(file_record):
    file_key = str(file_record.get("file_key") or "").lower()
    blob = file_record.get("file_blob") or b""

    if file_key == "csv":
        try:
            return pd.read_csv(StringIO(blob.decode("utf-8")))
        except UnicodeDecodeError:
            return pd.read_csv(StringIO(blob.decode("latin-1")))
    if file_key == "xlsx":
        return pd.read_excel(BytesIO(blob))
    raise ValueError("Unsupported file type for dashboard")


def _build_financial_dashboard(df, page=1, page_size=20, sort_by_due_date_desc=False):
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]

    def pick_col(candidates):
        lookup = {str(c).strip().lower(): c for c in df.columns}
        for candidate in candidates:
            col = lookup.get(str(candidate).strip().lower())
            if col is not None:
                return col
        return None

    job_col = pick_col(["Job No.", "Job No", "Job Number"])
    boe_col = pick_col(["BoE No.", "BoE No", "BOE No.", "BOE No"])
    bl_col = pick_col(["BL/AWB No.", "BL/AWB No", "BL No", "AWB No"])
    code_col = pick_col(["IMP. CODE/EXP. CODE", "IMP CODE/EXP CODE", "Importer Code", "Exporter Code"])
    regime_col = pick_col(["Regime"])
    status_col = pick_col(["Status"])
    processing_status_col = pick_col(["Processing Status", "ProcessingStatus"])
    date_col = pick_col(["Submission Date", "Submitted Date", "Date"])
    due_date_col = pick_col(["Due Date", "BOE Due Date", "Boe Due Date"])
    created_by_col = pick_col(["Created By", "User", "Creator"])

    def non_blank_count(col):
        if not col:
            return 0
        series = df[col].astype(str).str.strip()
        return int((series != "").sum())

    unique_jobs = int(df[job_col].astype(str).str.strip().replace("", pd.NA).dropna().nunique()) if job_col else 0
    unique_boe = int(df[boe_col].astype(str).str.strip().replace("", pd.NA).dropna().nunique()) if boe_col else 0
    unique_bl = int(df[bl_col].astype(str).str.strip().replace("", pd.NA).dropna().nunique()) if bl_col else 0
    unique_codes = int(df[code_col].astype(str).str.strip().replace("", pd.NA).dropna().nunique()) if code_col else 0

    duplicate_boe_count = 0
    if boe_col:
        boe_series = df[boe_col].astype(str).str.strip().replace("", pd.NA).dropna()
        if not boe_series.empty:
            duplicate_boe_count = int(boe_series.duplicated().sum())

    missing_core_count = 0
    if job_col or boe_col:
        core_df = pd.DataFrame(index=df.index)
        core_df["job"] = df[job_col].astype(str).str.strip() if job_col else ""
        core_df["boe"] = df[boe_col].astype(str).str.strip() if boe_col else ""
        missing_core_count = int(((core_df["job"] == "") | (core_df["boe"] == "")).sum())

    status_breakdown = []
    if status_col:
        status_counts = (
            df[status_col]
            .astype(str)
            .str.strip()
            .replace("", "(Blank)")
            .value_counts()
            .head(8)
        )
        status_breakdown = [{"label": str(k), "count": int(v)} for k, v in status_counts.items()]

    regime_breakdown = []
    if regime_col:
        regime_counts = (
            df[regime_col]
            .astype(str)
            .str.strip()
            .replace("", "(Blank)")
            .value_counts()
            .head(8)
        )
        regime_breakdown = [{"label": str(k), "count": int(v)} for k, v in regime_counts.items()]

    created_by_breakdown = []
    if created_by_col:
        creator_counts = (
            df[created_by_col]
            .astype(str)
            .str.strip()
            .replace("", "(Blank)")
            .value_counts()
            .head(10)
        )
        created_by_breakdown = [{"label": str(k), "count": int(v)} for k, v in creator_counts.items()]

    boe_status_breakdown = []
    if processing_status_col:
        boe_status_counts = (
            df[processing_status_col]
            .astype(str)
            .str.strip()
            .replace("", "(Blank)")
            .value_counts()
            .head(10)
        )
        boe_status_breakdown = [{"label": str(k), "count": int(v)} for k, v in boe_status_counts.items()]

    timeline = []
    timeline_label = date_col or ""
    if date_col:
        parsed = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
        grouped = (
            pd.DataFrame({"date": parsed.dt.date})
            .dropna(subset=["date"])
            .groupby("date", as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .sort_values("date")
            .tail(31)
        )
        timeline = [
            {"date": str(row["date"]), "count": int(row["count"])}
            for _, row in grouped.iterrows()
        ]

    boe_due_timeline = []
    boe_due_timeline_label = due_date_col or ""
    if due_date_col:
        due_parsed = pd.to_datetime(df[due_date_col], errors="coerce", dayfirst=True)
        due_grouped = (
            pd.DataFrame({"date": due_parsed.dt.date})
            .dropna(subset=["date"])
            .groupby("date", as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .sort_values("date")
            .tail(31)
        )
        boe_due_timeline = [
            {"date": str(row["date"]), "count": int(row["count"])}
            for _, row in due_grouped.iterrows()
        ]

    preview_source_df = df
    if sort_by_due_date_desc and due_date_col:
        due_sort = pd.to_datetime(df[due_date_col], errors="coerce", dayfirst=True)
        preview_source_df = (
            df.assign(_due_sort=due_sort)
            .sort_values("_due_sort", ascending=True, na_position="last", kind="mergesort")
            .drop(columns=["_due_sort"])
        )

    allowed_page_sizes = {10, 20, 50, 100, 200, 500}
    page_size = int(page_size) if int(page_size) in allowed_page_sizes else 20
    total_rows = int(len(preview_source_df))
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    page = max(1, min(int(page), total_pages))
    start_idx = (page - 1) * page_size
    end_idx = min(start_idx + page_size, total_rows)

    preview_df = preview_source_df.iloc[start_idx:end_idx].copy()
    
    # Convert columns to string, formatting numeric values as integers (no .0) and NaN to empty
    for col in preview_df.columns:
        if preview_df[col].dtype in ['float64', 'float32', 'int64', 'int32']:
            # Convert numeric values to int strings, NaN to empty string
            preview_df[col] = preview_df[col].apply(
                lambda x: str(int(x)) if pd.notna(x) else ""
            )
        else:
            # For non-numeric columns, handle NaN before converting to string
            preview_df[col] = preview_df[col].where(pd.notna(preview_df[col]), "").astype(str)
    
    preview_rows = preview_df.to_dict(orient="records")
    preview_columns = [str(c) for c in preview_source_df.columns]  # Use full columns for consistency

    return {
        "total_rows": total_rows,
        "total_columns": int(len(df.columns)),
        "unique_jobs": unique_jobs,
        "unique_boe": unique_boe,
        "unique_bl": unique_bl,
        "unique_codes": unique_codes,
        "missing_core_count": missing_core_count,
        "duplicate_boe_count": duplicate_boe_count,
        "status_breakdown": status_breakdown,
        "regime_breakdown": regime_breakdown,
        "created_by_breakdown": created_by_breakdown,
        "boe_status_breakdown": boe_status_breakdown,
        "timeline": timeline,
        "timeline_label": timeline_label,
        "boe_due_timeline": boe_due_timeline,
        "boe_due_timeline_label": boe_due_timeline_label,
        "known_columns_present": {
            "job_no": bool(job_col),
            "boe_no": bool(boe_col),
            "bl_awb_no": bool(bl_col),
            "imp_exp_code": bool(code_col),
            "regime": bool(regime_col),
            "status": bool(status_col),
            "processing_status": bool(processing_status_col),
            "submission_date": bool(date_col),
            "due_date": bool(due_date_col),
            "created_by": bool(created_by_col),
        },
        "non_blank_counts": {
            "job_no": non_blank_count(job_col),
            "boe_no": non_blank_count(boe_col),
            "bl_awb_no": non_blank_count(bl_col),
            "imp_exp_code": non_blank_count(code_col),
        },
        "preview_columns": preview_columns,
        "preview_rows": preview_rows,
        "preview_page": page,
        "preview_page_size": page_size,
        "preview_total_pages": total_pages,
        "preview_start_row": (start_idx + 1) if total_rows > 0 else 0,
        "preview_end_row": end_idx,
    }


def _extract_account_base_name(user_ref):
    """Compatibility wrapper that returns the extracted canonical account token."""
    base_name, _ = _extract_account_features(user_ref)
    return base_name


def _extract_account_features(user_ref):
    """Extract canonical token and classify if it is person-like.

    Examples:
    - IMP/MAD/23 -> (MAD, False)
    - MAD/IMP -> (MAD, False)
    - MAD KOFI -> (MAD, False)
    - BERNARD JOYCE -> (JOYCE, True)
    - BERNARD PRINCE -> (PRINCE, True)
    """
    if not user_ref:
        return "", False

    text = str(user_ref).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL", "NAT"}:
        return "", False

    tokens = re.findall(r"[A-Z0-9]+", text)
    if not tokens:
        return "", False

    # Ignore generic routing words so IMP/MAD/23 resolves to MAD.
    ignore_tokens = {
        "IMP", "EXP", "IMPORT", "EXPORT", "USER", "REF", "NO", "NUM", "NUMBER", "LTD", "LIMITED",
    }

    meaningful_tokens = []
    for token in tokens:
        if token.isdigit():
            continue
        if token in ignore_tokens:
            continue
        meaningful_tokens.append(token)

    if not meaningful_tokens:
        return "", False

    # Person-like refs: two long alphabetic words, keep surname distinct.
    alpha_tokens = [t for t in meaningful_tokens if t.isalpha()]
    is_person_like = (
        len(alpha_tokens) >= 2
        and len(alpha_tokens[0]) >= 5
        and len(alpha_tokens[1]) >= 5
        and not any(ch.isdigit() for ch in text)
        and "/" not in text
        and "-" not in text
    )
    if is_person_like:
        return alpha_tokens[-1], True

    return meaningful_tokens[0], False


def _consonant_signature(value):
    return "".join(ch for ch in value if ch.isalpha() and ch not in {"A", "E", "I", "O", "U"})


def _is_code_alias(left, right):
    matched, _, _ = _code_alias_details(left, right)
    return matched


def _code_alias_details(left, right):
    if left == right:
        return True, "exact token", 1.0
    if len(left) < 3 or len(right) < 3:
        return False, "", 0.0
    if left.startswith(right) or right.startswith(left):
        return True, "prefix match", 0.93

    sig_left = _consonant_signature(left)
    sig_right = _consonant_signature(right)
    if len(sig_left) >= 2 and len(sig_right) >= 2:
        if sig_left.startswith(sig_right) or sig_right.startswith(sig_left):
            return True, "consonant signature", 0.84

    score = SequenceMatcher(None, left, right).ratio()
    if score >= 0.78:
        return True, f"fuzzy similarity ({score:.2f})", score
    return False, "", score


def _code_alias_reason(left, right):
    _, reason, _ = _code_alias_details(left, right)
    return reason

    


def _normalize_report_value(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "null", "nat"}:
        return ""
    if re.fullmatch(r"-?\d+\.0", text):
        return text[:-2]
    return text


def _normalize_user_ref_key(value):
    text = _normalize_report_value(value).upper()
    if not text:
        return ""
    tokens = re.findall(r"[A-Z0-9]+", text)
    return " ".join(tokens)


def _build_decision_ref_key(user_ref_raw="", imp_code="", created_by="", boe=""):
    return service_build_decision_ref_key(user_ref_raw, imp_code, created_by, boe)


def _resolve_account_columns(df):
    return service_resolve_account_columns(df)


def _build_account_report(df, user_id=None):
    return service_build_account_report(df, user_id=user_id)


@app.before_request
def refresh_session():
    session.permanent = True
    if not current_user.is_authenticated:
        return None

    user_record = get_app_user(current_user.id)
    if not user_record:
        return None

    policy = _password_policy_state(user_record)
    password_must_change = bool(policy["must_change"] or policy["expired"])
    session["password_must_change"] = password_must_change

    endpoint = request.endpoint or ""
    allowed_endpoints = {
        "logout",
        "change_password",
        "close_account",
        "password_required",
        "static",
    }

    if password_must_change and endpoint not in allowed_endpoints:
        return redirect(url_for("password_required"))

    days_remaining = policy.get("days_remaining")
    if days_remaining and 0 < days_remaining <= int(PASSWORD_EXPIRY_WARNING_DAYS):
        today_key = datetime.now(timezone.utc).date().isoformat()
        if session.get("password_warning_sent_for_day") != today_key:
            flash(
                f"Password expires in about {days_remaining} day(s). Please change it soon.",
                "error",
            )
            session["password_warning_sent_for_day"] = today_key

    return None


def _update_job(job_id, **kwargs):
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id].update(kwargs)


def _find_duplicate_active_job(owner_id, payload_signature):
    active_statuses = {"queued", "running", "stopping"}
    with _jobs_lock:
        for existing_job in _jobs.values():
            if existing_job.get("owner") != owner_id:
                continue
            if str(existing_job.get("status", "")).lower() not in active_statuses:
                continue

            existing_payload = existing_job.get("payload") or {}
            existing_signature = {
                "retrieval_type": str(existing_payload.get("retrieval_type", "financial")),
                "start_date": existing_payload.get("start_date", ""),
                "end_date": existing_payload.get("end_date", ""),
                "page_size": str(existing_payload.get("page_size", "")),
                "headless": bool(existing_payload.get("headless", False)),
            }

            if existing_signature == payload_signature:
                return existing_job
    return None


def _count_active_admins():
    users = list_app_users()
    return sum(1 for user in users if str(user.get("role", "")).lower() == "admin" and bool(user.get("is_active", 0)))


def _enqueue_job(payload):
    job_id = str(uuid.uuid4())[:8]
    cancel_event = threading.Event()
    payload["cancel_event"] = cancel_event

    with _jobs_lock:
        _jobs[job_id] = {
            "id": job_id,
            "owner": payload.get("owner"),
            "status": "queued",
            "created_at": datetime.now().isoformat(),
            "payload": {
                "retrieval_type": payload.get("retrieval_type", "financial"),
                "start_date": payload.get("start_date"),
                "end_date": payload.get("end_date"),
                "page_size": payload.get("page_size"),
                "output_dir": payload.get("output_dir"),
                "headless": payload.get("headless", False),
                "label": payload.get("label"),
                "elapsed_only": payload.get("elapsed_only", False),
                "customs_office_code": payload.get("customs_office_code", ""),
                "im_exporter_code": payload.get("im_exporter_code", ""),
                "created_by": payload.get("created_by", "M"),
            },
            "last_message": "Step 0/6: Queued",
            "result": None,
        }
        _job_cancel_events[job_id] = cancel_event

    upsert_retrieval_run(
        job_id=job_id,
        user_id=payload.get("owner"),
        status="queued",
        last_message="Step 0/6: Queued",
        row_count=0,
        created_at=datetime.now(timezone.utc).isoformat(),
        payload={
            "retrieval_type": payload.get("retrieval_type", "financial"),
            "start_date": payload.get("start_date"),
            "end_date": payload.get("end_date"),
            "page_size": payload.get("page_size"),
            "output_dir": payload.get("output_dir"),
            "headless": payload.get("headless", False),
            "label": payload.get("label"),
            "elapsed_only": payload.get("elapsed_only", False),
            "customs_office_code": payload.get("customs_office_code", ""),
            "im_exporter_code": payload.get("im_exporter_code", ""),
            "created_by": payload.get("created_by", "M"),
        },
    )

    worker = threading.Thread(target=_run_background_job, args=(job_id, payload), daemon=True)
    worker.start()
    return job_id


def _run_background_job(job_id, payload):
    created_at = datetime.now(timezone.utc).isoformat()
    _update_job(job_id, status="running", started_at=datetime.now().isoformat())
    upsert_retrieval_run(
        job_id=job_id,
        user_id=payload.get("owner"),
        status="running",
        last_message="Step 1/6: Starting browser",
        row_count=0,
        created_at=created_at,
        payload={
            "retrieval_type": payload.get("retrieval_type", "financial"),
            "start_date": payload.get("start_date"),
            "end_date": payload.get("end_date"),
            "page_size": payload.get("page_size"),
            "output_dir": payload.get("output_dir"),
            "headless": payload.get("headless", False),
            "label": payload.get("label"),
            "elapsed_only": payload.get("elapsed_only", False),
            "customs_office_code": payload.get("customs_office_code", ""),
            "im_exporter_code": payload.get("im_exporter_code", ""),
            "created_by": payload.get("created_by", "M"),
        },
    )

    def emit_status(message):
        _update_job(job_id, last_message=message)

    result = run_session(
        user_name=payload.get("username"),
        user_password=payload.get("password"),
        start_date=payload.get("start_date"),
        end_date=payload.get("end_date"),
        page_size=payload.get("page_size"),
        label=payload.get("label") or "BOE Account",
        output_dir=payload.get("output_dir") or OUTPUT_DIR,
        edge_driver_path=payload.get("edge_driver_path") or EDGE_DRIVER_PATH,
        headless=payload.get("headless", False),
        retrieval_type=payload.get("retrieval_type", "financial"),
        elapsed_only=payload.get("elapsed_only", False),
        customs_office_code=payload.get("customs_office_code", ""),
        im_exporter_code=payload.get("im_exporter_code", ""),
        created_by=payload.get("created_by", "M"),
        status_callback=emit_status,
        cancel_requested=lambda: bool(payload.get("cancel_event") and payload["cancel_event"].is_set()),
    )

    if result.get("stopped"):
        final_status = "stopped"
    else:
        final_status = "completed" if result.get("ok") else "failed"
    ended_at = datetime.now(timezone.utc).isoformat()
    _update_job(
        job_id,
        status=final_status,
        ended_at=datetime.now().isoformat(),
        result=result,
        last_message=result.get("message", ""),
    )

    if result.get("ok") and payload.get("owner"):
        files = result.get("files") or {}
        stored_files = {}
        mime_by_key = {
            "csv": "text/csv",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        }
        for key, file_path in files.items():
            if not file_path or not os.path.exists(file_path):
                continue
            try:
                with open(file_path, "rb") as f:
                    blob = f.read()
                file_name = os.path.basename(file_path)
                mime_type = mime_by_key.get(str(key).lower(), "application/octet-stream")
                save_generated_file(job_id, payload["owner"], key, file_name, mime_type, blob, created_at=ended_at)
                stored_files[key] = {"name": file_name, "created_at": ended_at}
            finally:
                try:
                    os.remove(file_path)
                except OSError:
                    pass

        result["files"] = stored_files

    upsert_retrieval_run(
        job_id=job_id,
        user_id=payload.get("owner"),
        status=final_status,
        last_message=result.get("message", ""),
        row_count=result.get("row_count") or 0,
        created_at=created_at,
        ended_at=ended_at,
        payload={
            "retrieval_type": payload.get("retrieval_type", "financial"),
            "start_date": payload.get("start_date"),
            "end_date": payload.get("end_date"),
            "page_size": payload.get("page_size"),
            "output_dir": payload.get("output_dir"),
            "headless": payload.get("headless", False),
            "label": payload.get("label"),
            "elapsed_only": payload.get("elapsed_only", False),
            "customs_office_code": payload.get("customs_office_code", ""),
            "im_exporter_code": payload.get("im_exporter_code", ""),
            "created_by": payload.get("created_by", "M"),
        },
    )

    with _jobs_lock:
        _job_cancel_events.pop(job_id, None)


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    _flash_security_warnings("login")

    if request.method == "POST":
        _validate_csrf_or_abort()
        app_user = request.form.get("app_username", "").strip()
        app_password = request.form.get("app_password", "")
        remember_me = request.form.get("remember_me") == "on"

        app_user_record = get_app_user(app_user)
        if not app_user_record:
            log_auth_event(app_user or None, "login", "failed", _client_ip(), "unknown user")
            flash("Invalid username or password", "error")
            return redirect(url_for("login"))

        if not bool(app_user_record.get("is_active", 0)):
            log_auth_event(app_user, "login", "blocked", _client_ip(), "inactive account")
            flash("Account is inactive. Contact an admin.", "error")
            return redirect(url_for("login"))

        locked_until = app_user_record.get("locked_until")
        if locked_until:
            lock_time = datetime.fromisoformat(locked_until)
            now = datetime.now(timezone.utc)
            if lock_time > now:
                remaining_minutes = int((lock_time - now).total_seconds() // 60) + 1
                log_auth_event(
                    app_user,
                    "login",
                    "blocked",
                    _client_ip(),
                    f"locked, remaining_minutes={remaining_minutes}",
                )
                flash(
                    f"Account temporarily locked. Try again in about {remaining_minutes} minute(s).",
                    "error",
                )
                return redirect(url_for("login"))

        try:
            valid_password = check_password_hash(app_user_record["password_hash"], app_password)
        except ValueError:
            valid_password = False

        if not valid_password:
            updated = register_failed_login(app_user, LOGIN_MAX_ATTEMPTS, LOGIN_LOCK_MINUTES)
            details = "invalid password"
            if updated and updated.get("locked_until"):
                details = f"invalid password; locked until {updated['locked_until']}"
            log_auth_event(app_user, "login", "failed", _client_ip(), details)
            flash("Invalid username or password", "error")
            return redirect(url_for("login"))

        clear_failed_login(app_user)
        login_user(
            AppUser(app_user, role=app_user_record.get("role", "user"), is_active=app_user_record.get("is_active", 1)),
            remember=remember_me,
        )
        log_auth_event(app_user, "login", "success", _client_ip(), "authenticated")

        policy = _password_policy_state(app_user_record)
        if policy["must_change"]:
            flash("Password reset detected. You must change your password before continuing.", "error")
            return redirect(url_for("password_required"))
        if policy["expired"]:
            flash("Password expired. Change your password to continue.", "error")
            return redirect(url_for("password_required"))

        flash("Login successful", "success")
        return redirect(url_for("index"))

    return render_template(
        "login.html",
        csrf_token=_new_csrf_token(),
        allow_dev_setup=ALLOW_DEV_ADMIN_SETUP,
    )


@app.route("/setup-admin", methods=["GET", "POST"])
def setup_admin():
    if not ALLOW_DEV_ADMIN_SETUP:
        abort(404)

    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        _validate_csrf_or_abort()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if len(password) < 8:
            flash("Password must be at least 8 characters", "error")
            return redirect(url_for("setup_admin"))

        if password != confirm_password:
            flash("Passwords do not match", "error")
            return redirect(url_for("setup_admin"))

        ensure_app_user(APP_ADMIN_USER, generate_password_hash(password), role="admin", is_active=True)
        clear_failed_login(APP_ADMIN_USER)
        log_auth_event(APP_ADMIN_USER, "admin_setup", "success", _client_ip(), "admin password reset")
        flash(f"Admin account is ready. Login with username '{APP_ADMIN_USER}'.", "success")
        return redirect(url_for("login"))

    return render_template(
        "setup_admin.html",
        csrf_token=_new_csrf_token(),
        admin_user=APP_ADMIN_USER,
    )


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    _validate_csrf_or_abort()
    log_auth_event(current_user.id, "logout", "success", _client_ip(), "session ended")
    logout_user()
    session.clear()
    return redirect(url_for("login"))


@app.route("/", methods=["GET"])
@login_required
def index():
    _flash_security_warnings("dashboard")
    _cleanup_old_generated_files(current_user.id)

    persisted_runs = list_recent_retrieval_runs_for_user(current_user.id, limit=200)
    with _jobs_lock:
        live_runs = [job for job in _jobs.values() if job.get("owner") == current_user.id]

    jobs_by_id = {job["id"]: job for job in persisted_runs}
    for job in live_runs:
        jobs_by_id[job["id"]] = job
    jobs_snapshot = sorted(jobs_by_id.values(), key=lambda item: item.get("created_at", ""), reverse=True)

    saved_credentials = _get_portal_credentials_or_recover(current_user.id, flash_on_error=True)
    return render_template(
        "index.html",
        defaults={
            "start_date": _to_date_picker_value(DEFAULT_START_DATE),
            "end_date": _to_date_picker_value(DEFAULT_END_DATE),
            "page_size": DEFAULT_PAGE_SIZE,
            "output_dir": OUTPUT_DIR,
            "edge_driver_path": EDGE_DRIVER_PATH,
            "label": "Customs Account",
        },
        jobs=jobs_snapshot,
        has_portal_credentials=bool(saved_credentials),
        portal_username=(saved_credentials or {}).get("portal_username", ""),
        file_retention_hours=FILE_RETENTION_HOURS,
        is_admin=getattr(current_user, "is_admin", False),
        current_role=getattr(current_user, "role", "user"),
        csrf_token=_new_csrf_token(),
    )


@app.route("/profile", methods=["GET"])
@login_required
def profile():
    user_record = get_app_user(current_user.id) or {}
    policy = _password_policy_state(user_record)
    return render_template(
        "profile.html",
        is_admin=getattr(current_user, "is_admin", False),
        current_role=getattr(current_user, "role", "user"),
        password_policy=policy,
        company_profile=_company_profile_from_user_record(user_record),
        csrf_token=_new_csrf_token(),
    )


@app.route("/profile/company", methods=["POST"])
@login_required
def profile_company_update():
    _validate_csrf_or_abort()

    company_name = request.form.get("company_name", "")
    company_address = request.form.get("company_address", "")
    company_phone = request.form.get("company_phone", "")

    user_record = get_app_user(current_user.id) or {}
    current_logo_path = str(user_record.get("company_logo_path") or "").strip()
    clear_logo = request.form.get("clear_company_logo", "").strip().lower() in {"1", "true", "yes", "on"}
    uploaded_logo = request.files.get("company_logo")

    new_logo_path = current_logo_path
    if clear_logo:
        if current_logo_path:
            old_logo_abs = os.path.join(app.root_path, current_logo_path)
            if os.path.exists(old_logo_abs):
                try:
                    os.remove(old_logo_abs)
                except OSError:
                    pass
        new_logo_path = ""

    if uploaded_logo and (uploaded_logo.filename or "").strip():
        original_name = secure_filename(uploaded_logo.filename or "")
        ext = os.path.splitext(original_name)[1].lower()
        if ext not in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
            flash("Logo must be PNG, JPG, JPEG, GIF, or WEBP.", "error")
            return redirect(url_for("profile"))

        logo_dir = os.path.join(app.root_path, "static", "uploads", "company_logos")
        os.makedirs(logo_dir, exist_ok=True)
        logo_file = f"{current_user.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
        logo_abs = os.path.join(logo_dir, logo_file)
        uploaded_logo.save(logo_abs)
        new_logo_path = os.path.join("static", "uploads", "company_logos", logo_file).replace("\\", "/")

        if current_logo_path and current_logo_path != new_logo_path:
            old_logo_abs = os.path.join(app.root_path, current_logo_path)
            if os.path.exists(old_logo_abs):
                try:
                    os.remove(old_logo_abs)
                except OSError:
                    pass

    set_user_company_profile(
        current_user.id,
        company_name=company_name,
        company_address=company_address,
        company_phone=company_phone,
        company_logo_path=new_logo_path,
    )
    flash("Company profile updated.", "success")
    return redirect(url_for("profile"))


@app.route("/password-required", methods=["GET"])
@login_required
def password_required():
    user_record = get_app_user(current_user.id) or {}
    policy = _password_policy_state(user_record)
    if not (policy["must_change"] or policy["expired"]):
        return redirect(url_for("index"))

    return render_template(
        "password_required.html",
        is_admin=getattr(current_user, "is_admin", False),
        current_role=getattr(current_user, "role", "user"),
        password_policy=policy,
        csrf_token=_new_csrf_token(),
    )


@app.route("/reports", methods=["GET"])
@login_required
def reports_dashboard():
    _cleanup_old_generated_files(current_user.id)

    report_files = _build_report_file_index(current_user.id)
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()
    try:
        selected_page = int(request.args.get("page", "1"))
    except ValueError:
        selected_page = 1
    try:
        selected_page_size = int(request.args.get("page_size", "20"))
    except ValueError:
        selected_page_size = 20
    if selected_page_size not in {10, 20, 50, 100, 200, 500}:
        selected_page_size = 20
    selected_page = max(1, selected_page)

    if not selected_job_id or not selected_file_key:
        if report_files:
            selected_job_id = report_files[0]["job_id"]
            selected_file_key = report_files[0]["file_key"]

    selected_summary = None
    selected_file_name = ""
    selected_retrieval_type = "financial"
    sort_by_due_date_desc = False
    recent_uploads = list_manual_upload_runs_for_user(current_user.id, limit=30)
    if selected_job_id and selected_file_key in {"csv", "xlsx"}:
        selected_meta = next(
            (
                item
                for item in report_files
                if str(item.get("job_id")) == selected_job_id and str(item.get("file_key")) == selected_file_key
            ),
            None,
        )
        if selected_meta:
            selected_retrieval_type = str(selected_meta.get("retrieval_type") or "financial")
            sort_by_due_date_desc = selected_retrieval_type == "boe_status_dates"

        record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
        if record:
            try:
                df = _load_dataframe_from_generated_file(record)
                selected_summary = _build_financial_dashboard(
                    df,
                    page=selected_page,
                    page_size=selected_page_size,
                    sort_by_due_date_desc=sort_by_due_date_desc,
                )
                selected_file_name = record.get("file_name") or ""
            except Exception as exc:
                flash(f"Failed to build report dashboard: {exc}", "error")

    return render_template(
        "reports.html",
        report_files=report_files,
        selected_job_id=selected_job_id,
        selected_file_key=selected_file_key,
        selected_page=selected_page,
        selected_page_size=selected_page_size,
        selected_file_name=selected_file_name,
        selected_retrieval_type=selected_retrieval_type,
        summary=selected_summary,
        recent_uploads=recent_uploads,
        manual_upload_retention_days=MANUAL_UPLOAD_RETENTION_DAYS,
        is_admin=getattr(current_user, "is_admin", False),
        current_role=getattr(current_user, "role", "user"),
        csrf_token=_new_csrf_token(),
    )


@app.route("/reports/upload/pin", methods=["POST"])
@login_required
def reports_upload_pin_toggle():
    _validate_csrf_or_abort()

    job_id = _normalize_report_value(request.form.get("job_id"))
    file_key = _normalize_report_value(request.form.get("file_key")).lower()
    if file_key not in {"csv", "xlsx"}:
        file_key = "csv"
    pin_value = _normalize_report_value(request.form.get("pin")).lower()
    should_pin = pin_value in {"1", "true", "yes", "on"}

    if not job_id:
        flash("Missing upload identifier.", "error")
        return redirect(url_for("reports_dashboard"))

    updated = set_manual_upload_pinned(current_user.id, job_id, should_pin)
    if not updated:
        flash("Upload not found or not eligible for pinning.", "error")
        return redirect(url_for("reports_dashboard"))

    flash("Upload pinned." if should_pin else "Upload unpinned.", "success")
    return redirect(url_for("reports_dashboard", job_id=job_id, file_key=file_key, page=1))


@app.route("/reports/upload/delete", methods=["POST"])
@login_required
def reports_upload_delete():
    _validate_csrf_or_abort()

    job_id = _normalize_report_value(request.form.get("job_id"))
    if not job_id:
        flash("Missing upload identifier.", "error")
        return redirect(url_for("reports_dashboard"))

    runs = list_manual_upload_runs_for_user(current_user.id, limit=500)
    exists = any(str(item.get("job_id")) == job_id for item in runs)
    if not exists:
        flash("Upload not found.", "error")
        return redirect(url_for("reports_dashboard"))

    deleted_files = delete_generated_files_for_job(current_user.id, job_id)
    delete_retrieval_run(current_user.id, job_id)
    flash(f"Upload deleted ({deleted_files} file entries removed).", "success")
    return redirect(url_for("reports_dashboard"))


@app.route("/reports/preview", methods=["GET"])
@login_required
def reports_preview_ajax():
    """AJAX endpoint to fetch just the preview table without full page refresh."""
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()
    try:
        selected_page = int(request.args.get("page", "1"))
    except ValueError:
        selected_page = 1
    try:
        selected_page_size = int(request.args.get("page_size", "20"))
    except ValueError:
        selected_page_size = 20
    if selected_page_size not in {10, 20, 50, 100, 200, 500}:
        selected_page_size = 20
    selected_page = max(1, selected_page)

    if not selected_job_id or not selected_file_key or selected_file_key not in {"csv", "xlsx"}:
        return jsonify({"error": "Invalid parameters"}), 400

    record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
    if not record:
        return jsonify({"error": "File not found"}), 404

    run = get_retrieval_run_for_user(current_user.id, selected_job_id) or {}
    retrieval_type = str(((run.get("payload") or {}).get("retrieval_type") or "financial"))
    sort_by_due_date_desc = retrieval_type == "boe_status_dates"

    try:
        df = _load_dataframe_from_generated_file(record)
        summary = _build_financial_dashboard(
            df,
            page=selected_page,
            page_size=selected_page_size,
            sort_by_due_date_desc=sort_by_due_date_desc,
        )
        return jsonify({
            "preview_columns": summary["preview_columns"],
            "preview_rows": summary["preview_rows"],
            "preview_page": summary["preview_page"],
            "preview_page_size": summary["preview_page_size"],
            "preview_total_pages": summary["preview_total_pages"],
            "preview_start_row": summary["preview_start_row"],
            "preview_end_row": summary["preview_end_row"],
            "total_rows": summary["total_rows"],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/reports/account", methods=["GET"])
@login_required
def reports_account_ajax():
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()
    payload, status = build_reports_account_payload(
        current_user.id,
        selected_job_id,
        selected_file_key,
        get_generated_file=get_generated_file,
        load_dataframe=_load_dataframe_from_generated_file,
    )
    if status == 200 and isinstance(payload, dict):
        pricing_by_account = list_account_pricing_profiles_for_file(
            current_user.id,
            selected_job_id,
            selected_file_key,
        )
        total_amount = 0.0
        for account in payload.get("accounts", []):
            account_name = str(account.get("name") or "")
            profile = pricing_by_account.get(account_name, {})
            mode, fixed_price, _line_prices, currency_code, manual_rate, conversion_note = _normalize_pricing_profile(profile)
            amount_total = _calculate_account_amount_total(account.get("rows", []), profile)
            account["pricing_mode"] = mode
            account["fixed_price"] = float(fixed_price)
            account["currency_code"] = currency_code
            account["manual_rate"] = manual_rate
            account["conversion_note"] = conversion_note
            account["amount_total"] = float(amount_total)
            total_amount += float(amount_total)
        payload["amount_report_total"] = float(total_amount)
        payload["amount_report_accounts_with_amount"] = int(
            sum(1 for a in payload.get("accounts", []) if float(a.get("amount_total") or 0) > 0)
        )

    if status == 500 and payload.get("traceback"):
        current_app.logger.error("Account report ajax failed\n%s", payload.get("traceback"))
        payload = {"error": payload.get("error", "Internal error")}
    return jsonify(payload), status


@app.route("/reports/account/custom/add", methods=["POST"])
@login_required
def reports_account_custom_add():
    payload = request.get_json(silent=True) or {}
    account_name = _normalize_report_value(payload.get("account_name"))
    response, status = build_reports_account_custom_add_payload(current_user.id, account_name)
    return jsonify(response), status


@app.route("/reports/account/custom/remove", methods=["POST"])
@login_required
def reports_account_custom_remove():
    payload = request.get_json(silent=True) or {}
    account_name = _normalize_report_value(payload.get("account_name"))
    response, status = build_reports_account_custom_remove_payload(current_user.id, account_name)
    return jsonify(response), status


@app.route("/reports/account/pricing", methods=["GET"])
@login_required
def reports_account_pricing_get():
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()
    account_name = _normalize_report_value(request.args.get("account_name"))

    if not selected_job_id or selected_file_key not in {"csv", "xlsx"} or not account_name:
        return jsonify({"error": "Invalid parameters"}), 400

    profile = get_account_pricing_profile(current_user.id, selected_job_id, selected_file_key, account_name)
    if not profile:
        return jsonify({"pricing_mode": "none", "fixed_price": 0, "line_prices": {}, "currency_code": "GHS", "manual_rate": None, "conversion_note": "", "rate_history": []})
    profile["rate_history"] = list_account_pricing_rate_history(
        current_user.id,
        selected_job_id,
        selected_file_key,
        account_name,
    )
    return jsonify(profile)


@app.route("/reports/account/pricing/save", methods=["POST"])
@login_required
def reports_account_pricing_save():
    payload = request.get_json(silent=True) or {}
    selected_job_id = _normalize_report_value(payload.get("job_id"))
    selected_file_key = _normalize_report_value(payload.get("file_key")).lower()
    account_name = _normalize_report_value(payload.get("account_name"))
    pricing_mode = _normalize_report_value(payload.get("pricing_mode")).lower() or "none"
    fixed_price = payload.get("fixed_price", 0)
    line_prices = payload.get("line_prices")
    currency_code = _normalize_report_value(payload.get("currency_code")).upper() or "GHS"
    manual_rate = payload.get("manual_rate")
    conversion_note = _normalize_report_value(payload.get("conversion_note"))

    if not selected_job_id or selected_file_key not in {"csv", "xlsx"} or not account_name:
        return jsonify({"error": "Invalid parameters"}), 400

    if currency_code not in {"GHS", "USD"}:
        currency_code = "GHS"
    try:
        parsed_manual_rate = float(manual_rate) if manual_rate is not None and str(manual_rate).strip() != "" else None
    except (TypeError, ValueError):
        return jsonify({"error": "Manual rate must be a valid number"}), 400
    if parsed_manual_rate is not None and parsed_manual_rate <= 0:
        return jsonify({"error": "Manual rate must be greater than 0"}), 400

    if pricing_mode in {"automatic", "manual"} and currency_code == "USD" and parsed_manual_rate is None:
        return jsonify({"error": "Manual rate is required for USD pricing"}), 400

    if currency_code == "USD":
        conversion_note = conversion_note or _build_conversion_note(currency_code, parsed_manual_rate)
    else:
        conversion_note = ""

    if pricing_mode == "none":
        delete_account_pricing_profile(current_user.id, selected_job_id, selected_file_key, account_name)
        return jsonify({"success": True, "deleted": True})

    upsert_account_pricing_profile(
        current_user.id,
        selected_job_id,
        selected_file_key,
        account_name,
        pricing_mode=pricing_mode,
        fixed_price=fixed_price,
        line_prices=line_prices,
        currency_code=currency_code,
        manual_rate=parsed_manual_rate,
        conversion_note=conversion_note,
    )

    report_total = 0.0
    try:
        record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
        if record:
            df = _load_dataframe_from_generated_file(record)
            account_report = _build_account_report(df, user_id=current_user.id)
            target = next((acc for acc in account_report.get("accounts", []) if str(acc.get("name") or "") == account_name), None)
            if target:
                report_total = _calculate_account_amount_total(
                    target.get("rows", []),
                    {
                        "pricing_mode": pricing_mode,
                        "fixed_price": fixed_price,
                        "line_prices": line_prices,
                        "currency_code": currency_code,
                        "manual_rate": parsed_manual_rate,
                        "conversion_note": conversion_note,
                    },
                )
    except Exception:
        current_app.logger.exception("Could not compute pricing history total", extra={"account_name": account_name})

    record_account_pricing_rate_history(
        current_user.id,
        selected_job_id,
        selected_file_key,
        account_name,
        pricing_mode,
        currency_code,
        parsed_manual_rate,
        conversion_note,
        report_total,
    )
    return jsonify(
        {
            "success": True,
            "rate_history": list_account_pricing_rate_history(
                current_user.id,
                selected_job_id,
                selected_file_key,
                account_name,
            ),
        }
    )


@app.route("/reports/account/download/<account_name>", methods=["GET"])
@login_required
def reports_account_download(account_name):
    """Download account-specific PDF grouped by submission week."""
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()

    if not selected_job_id or not selected_file_key or selected_file_key not in {"csv", "xlsx"}:
        abort(400)

    record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
    if not record:
        abort(404)

    try:
        df = _load_dataframe_from_generated_file(record)
        account_report = _build_account_report(df, user_id=current_user.id)
        company_profile = _company_profile_from_user_record(get_app_user(current_user.id))
        
        # Find matching account and filter rows
        account_data = None
        for acc in account_report.get("accounts", []):
            if acc["name"] == account_name:
                account_data = acc
                break
        
        if not account_data:
            abort(404)
        pricing_profile = get_account_pricing_profile(
            current_user.id,
            selected_job_id,
            selected_file_key,
            account_name,
        )
        report_df = build_account_report_dataframe(
            df,
            account_data,
            resolve_account_columns=_resolve_account_columns,
            normalize_value=_normalize_report_value,
            pricing_profile=pricing_profile,
        )
        output = build_account_pdf_bytes(
            account_name,
            report_df,
            generated_at_text=datetime.now().strftime('%d %b %Y %H:%M'),
            normalize_value=_normalize_report_value,
            company_profile=company_profile,
            app_root_path=app.root_path,
            pricing_profile=pricing_profile,
        )

        return send_file(
            output,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"Account_{account_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        )
    except Exception as e:
        current_app.logger.exception(
            "Account download failed",
            extra={
                "account_name": account_name,
                "job_id": selected_job_id,
                "file_key": selected_file_key,
            },
        )
        abort(500)


@app.route("/reports/account/download-priced/<account_name>/<fmt>", methods=["GET"])
@login_required
def reports_account_download_priced(account_name, fmt):
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()
    fmt = (fmt or "").strip().lower()

    if not selected_job_id or selected_file_key not in {"csv", "xlsx"} or fmt not in {"csv", "xlsx"}:
        abort(400)

    record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
    if not record:
        abort(404)

    try:
        df = _load_dataframe_from_generated_file(record)
        account_report = _build_account_report(df, user_id=current_user.id)
        account_data = None
        for acc in account_report.get("accounts", []):
            if acc.get("name") == account_name:
                account_data = acc
                break
        if not account_data:
            abort(404)

        pricing_profile = get_account_pricing_profile(
            current_user.id,
            selected_job_id,
            selected_file_key,
            account_name,
        )
        report_df = build_account_report_dataframe(
            df,
            account_data,
            resolve_account_columns=_resolve_account_columns,
            normalize_value=_normalize_report_value,
            pricing_profile=pricing_profile,
        )
        export_df = _build_priced_account_export_df(report_df)
        now_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if fmt == "csv":
            content = export_df.to_csv(index=False).encode("utf-8")
            return send_file(
                BytesIO(content),
                mimetype="text/csv",
                as_attachment=True,
                download_name=f"Account_{account_name}_{now_stamp}.csv",
            )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_df.to_excel(writer, index=False, sheet_name="Priced Account")
        output.seek(0)
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"Account_{account_name}_{now_stamp}.xlsx",
        )
    except Exception:
        abort(500)


@app.route("/reports/account/view/<account_name>", methods=["GET"])
@login_required
def reports_account_view(account_name):
    """Display account data as HTML."""
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()

    if not selected_job_id or not selected_file_key or selected_file_key not in {"csv", "xlsx"}:
        abort(400)

    record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
    if not record:
        abort(404)

    try:
        df = _load_dataframe_from_generated_file(record)
        account_report = _build_account_report(df, user_id=current_user.id)
        company_profile = _company_profile_from_user_record(get_app_user(current_user.id))
        
        # Find matching account
        account_data = None
        for acc in account_report.get("accounts", []):
            if acc["name"] == account_name:
                account_data = acc
                break
        
        if not account_data:
            abort(404)

        # Get the rows for this account
        rows = account_data.get("rows", [])
        pricing_profile = get_account_pricing_profile(
            current_user.id,
            selected_job_id,
            selected_file_key,
            account_name,
        )
        html = build_account_view_html(
            account_name,
            rows,
            generated_at_text=datetime.now().strftime('%d %b %Y %H:%M'),
            company_profile=company_profile,
            pricing_profile=pricing_profile,
        )

        return html
    except Exception as e:
        abort(500)


@app.route("/reports/account/view-all", methods=["GET"])
@login_required
def reports_account_view_all():
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()

    if not selected_job_id or not selected_file_key or selected_file_key not in {"csv", "xlsx"}:
        abort(400)

    record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
    if not record:
        abort(404)

    try:
        df = _load_dataframe_from_generated_file(record)
        account_report = _build_account_report(df, user_id=current_user.id)
        company_profile = _company_profile_from_user_record(get_app_user(current_user.id))
        pricing_by_account = list_account_pricing_profiles_for_file(
            current_user.id,
            selected_job_id,
            selected_file_key,
        )
        html = build_all_accounts_view_html(
            account_report,
            generated_at_text=datetime.now().strftime('%d %b %Y %H:%M'),
            company_profile=company_profile,
            pricing_by_account=pricing_by_account,
        )
        return html
    except Exception:
        abort(500)


@app.route("/reports/account/download-all", methods=["GET"])
@login_required
def reports_account_download_all():
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()

    if not selected_job_id or not selected_file_key or selected_file_key not in {"csv", "xlsx"}:
        abort(400)

    record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
    if not record:
        abort(404)

    try:
        df = _load_dataframe_from_generated_file(record)
        account_report = _build_account_report(df, user_id=current_user.id)
        company_profile = _company_profile_from_user_record(get_app_user(current_user.id))
        pricing_by_account = list_account_pricing_profiles_for_file(
            current_user.id,
            selected_job_id,
            selected_file_key,
        )
        output = build_all_accounts_pdf_bytes(
            account_report,
            generated_at_text=datetime.now().strftime('%d %b %Y %H:%M'),
            normalize_value=_normalize_report_value,
            company_profile=company_profile,
            app_root_path=app.root_path,
            pricing_by_account=pricing_by_account,
        )

        return send_file(
            output,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"Account_Groups_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
        )
    except Exception:
        abort(500)


@app.route("/reports/account/download-priced-all/<fmt>", methods=["GET"])
@login_required
def reports_account_download_priced_all(fmt):
    selected_job_id = request.args.get("job_id", "").strip()
    selected_file_key = request.args.get("file_key", "").strip().lower()
    fmt = (fmt or "").strip().lower()

    if not selected_job_id or selected_file_key not in {"csv", "xlsx"} or fmt not in {"csv", "xlsx"}:
        abort(400)

    record = get_generated_file(current_user.id, selected_job_id, selected_file_key)
    if not record:
        abort(404)

    try:
        df = _load_dataframe_from_generated_file(record)
        account_report = _build_account_report(df, user_id=current_user.id)
        pricing_by_account = list_account_pricing_profiles_for_file(
            current_user.id,
            selected_job_id,
            selected_file_key,
        )
        combined_rows = []

        for account_data in account_report.get("accounts", []):
            account_name = str(account_data.get("name") or "")
            report_df = build_account_report_dataframe(
                df,
                account_data,
                resolve_account_columns=_resolve_account_columns,
                normalize_value=_normalize_report_value,
                pricing_profile=pricing_by_account.get(account_name),
            )
            export_df = _build_priced_account_export_df(report_df)
            export_df.insert(0, "Account", account_name)
            combined_rows.append(export_df)

        out_df = pd.concat(combined_rows, ignore_index=True) if combined_rows else pd.DataFrame(columns=["Account", "User Ref", "BOE Number", "Submission Date", "Amount"])
        now_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if fmt == "csv":
            content = out_df.to_csv(index=False).encode("utf-8")
            return send_file(
                BytesIO(content),
                mimetype="text/csv",
                as_attachment=True,
                download_name=f"Account_Priced_All_{now_stamp}.csv",
            )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            out_df.to_excel(writer, index=False, sheet_name="Priced Accounts")
        output.seek(0)
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"Account_Priced_All_{now_stamp}.xlsx",
        )
    except Exception:
        abort(500)


@app.route("/reports/account/assign", methods=["POST"])
@login_required
def reports_account_assign():
    """Assign unassigned entries to an account and generate full report."""
    selected_job_id = request.json.get("job_id", "").strip()
    selected_file_key = request.json.get("file_key", "").strip().lower()
    assign_to = request.json.get("assign_to", "").strip()
    response, status = build_reports_account_assign_payload(
        current_user.id,
        selected_job_id,
        selected_file_key,
        assign_to,
        get_generated_file=get_generated_file,
        load_dataframe=_load_dataframe_from_generated_file,
    )
    return jsonify(response), status


@app.route("/reports/account/decision", methods=["POST"])
@login_required
def reports_account_decision():
    payload = request.get_json(silent=True) or {}
    raw_ref = _normalize_report_value(payload.get("raw_user_ref"))
    imp_code = _normalize_report_value(payload.get("imp_code"))
    created_by = _normalize_report_value(payload.get("created_by"))
    boe = _normalize_report_value(payload.get("boe"))
    provided_key = _normalize_report_value(payload.get("decision_ref_key"))
    canonical = _normalize_report_value(payload.get("canonical_account"))
    action = _normalize_report_value(payload.get("action")).lower() or "accept"
    response, status = build_reports_account_decision_payload(
        current_user.id,
        raw_ref,
        imp_code,
        created_by,
        boe,
        provided_key,
        canonical,
        action,
    )
    return jsonify(response), status


@app.route("/reports/account/rename", methods=["POST"])
@login_required
def reports_account_rename():
    payload = request.get_json(silent=True) or {}
    selected_job_id = _normalize_report_value(payload.get("job_id"))
    selected_file_key = _normalize_report_value(payload.get("file_key")).lower()
    old_account = _normalize_report_value(payload.get("old_account"))
    new_account = _normalize_report_value(payload.get("new_account"))
    response, status = build_reports_account_rename_payload(
        current_user.id,
        selected_job_id,
        selected_file_key,
        old_account,
        new_account,
        get_generated_file=get_generated_file,
        load_dataframe=_load_dataframe_from_generated_file,
    )
    return jsonify(response), status


@app.route("/reports/account/delete", methods=["POST"])
@login_required
def reports_account_delete():
    payload = request.get_json(silent=True) or {}
    selected_job_id = _normalize_report_value(payload.get("job_id"))
    selected_file_key = _normalize_report_value(payload.get("file_key")).lower()
    account_name = _normalize_report_value(payload.get("account_name"))
    response, status = build_reports_account_delete_payload(
        current_user.id,
        selected_job_id,
        selected_file_key,
        account_name,
        get_generated_file=get_generated_file,
        load_dataframe=_load_dataframe_from_generated_file,
    )
    return jsonify(response), status


@app.route("/credentials/save", methods=["POST"])
@login_required
def save_credentials():
    _validate_csrf_or_abort()
    portal_username = request.form.get("portal_username", "").strip()
    portal_password = request.form.get("portal_password", "")

    if not portal_username or not portal_password:
        log_auth_event(current_user.id, "portal_credentials_save", "failed", _client_ip(), "missing field")
        flash("Portal username and password are required", "error")
        return redirect(url_for("index"))

    save_portal_credentials(current_user.id, portal_username, portal_password)
    log_auth_event(current_user.id, "portal_credentials_save", "success", _client_ip(), None)
    flash("Portal credentials saved securely", "success")
    return redirect(url_for("index"))


@app.route("/credentials/delete", methods=["POST"])
@login_required
def delete_credentials():
    _validate_csrf_or_abort()
    delete_portal_credentials(current_user.id)
    log_auth_event(current_user.id, "portal_credentials_delete", "success", _client_ip(), None)
    flash("Saved portal credentials removed", "success")
    return redirect(url_for("index"))


@app.route("/account/change-password", methods=["POST"])
@login_required
def change_password():
    _validate_csrf_or_abort()
    redirect_endpoint = _post_redirect_endpoint("index")

    current_password = request.form.get("current_password", "")
    new_password = request.form.get("new_password", "")
    confirm_password = request.form.get("confirm_password", "")

    if not current_password or not new_password or not confirm_password:
        log_auth_event(current_user.id, "password_change", "failed", _client_ip(), "missing fields")
        flash("All password fields are required", "error")
        return redirect(url_for(redirect_endpoint))

    user_record = get_app_user(current_user.id)
    if not user_record:
        log_auth_event(current_user.id, "password_change", "failed", _client_ip(), "user record missing")
        flash("User account not found", "error")
        return redirect(url_for(redirect_endpoint))

    if not check_password_hash(user_record["password_hash"], current_password):
        log_auth_event(current_user.id, "password_change", "failed", _client_ip(), "invalid current password")
        flash("Current password is incorrect", "error")
        return redirect(url_for(redirect_endpoint))

    if len(new_password) < 8:
        log_auth_event(current_user.id, "password_change", "failed", _client_ip(), "password too short")
        flash("New password must be at least 8 characters", "error")
        return redirect(url_for(redirect_endpoint))

    if new_password != confirm_password:
        log_auth_event(current_user.id, "password_change", "failed", _client_ip(), "confirmation mismatch")
        flash("New password and confirmation do not match", "error")
        return redirect(url_for(redirect_endpoint))

    if check_password_hash(user_record["password_hash"], new_password):
        log_auth_event(current_user.id, "password_change", "failed", _client_ip(), "same as existing password")
        flash("New password must be different from current password", "error")
        return redirect(url_for(redirect_endpoint))

    set_user_password(current_user.id, generate_password_hash(new_password), must_change_password=False)
    log_auth_event(current_user.id, "password_change", "success", _client_ip(), "password updated")

    # Force a fresh login after password change to invalidate prior session state.
    logout_user()
    session.clear()
    flash("Password changed successfully. Please log in again.", "success")
    return redirect(url_for("login"))


@app.route("/account/close", methods=["POST"])
@login_required
def close_account():
    _validate_csrf_or_abort()

    current_password = request.form.get("current_password", "")
    confirmation = request.form.get("confirmation", "").strip().upper()

    if confirmation != "CLOSE":
        flash("Type CLOSE to confirm account closure", "error")
        return redirect(url_for("profile"))

    user_record = get_app_user(current_user.id)
    if not user_record:
        flash("User account not found", "error")
        return redirect(url_for("profile"))

    if not check_password_hash(user_record["password_hash"], current_password):
        flash("Current password is incorrect", "error")
        return redirect(url_for("profile"))

    if str(user_record.get("role", "")).lower() == "admin" and _count_active_admins() <= 1:
        flash("Cannot close the last active admin account", "error")
        return redirect(url_for("profile"))

    deleted = delete_app_user(current_user.id)
    if deleted:
        log_auth_event(current_user.id, "account_close", "success", _client_ip(), "self-service close")
        logout_user()
        session.clear()
        flash("Your account has been closed", "success")
        return redirect(url_for("login"))

    flash("Unable to close account", "error")
    return redirect(url_for("profile"))


@app.route("/run", methods=["POST"])
@login_required
def run_job():
    _validate_csrf_or_abort()
    saved_credentials = _get_portal_credentials_or_recover(current_user.id, flash_on_error=True)
    if not saved_credentials:
        log_auth_event(current_user.id, "job_start", "failed", _client_ip(), "missing portal credentials")
        flash("Save portal credentials first", "error")
        return redirect(url_for("index"))

    admin_debug_browser = request.form.get("show_browser") == "on"
    is_admin = getattr(current_user, "is_admin", False)
    headless = not admin_debug_browser if is_admin else True

    payload = {
        "retrieval_type": "financial",
        "owner": current_user.id,
        "username": saved_credentials["portal_username"],
        "password": saved_credentials["portal_password"],
        "start_date": _to_portal_date(request.form.get("start_date", "").strip()),
        "end_date": _to_portal_date(request.form.get("end_date", "").strip()),
        "page_size": request.form.get("page_size", "").strip(),
        "output_dir": request.form.get("output_dir", "").strip(),
        "edge_driver_path": request.form.get("edge_driver_path", "").strip(),
        "label": request.form.get("label", "").strip() or "Customs Account",
        "headless": headless,
    }

    payload_signature = {
        "retrieval_type": payload.get("retrieval_type", "financial"),
        "start_date": payload["start_date"],
        "end_date": payload["end_date"],
        "page_size": str(payload["page_size"]),
        "headless": bool(payload["headless"]),
    }

    duplicate_job = _find_duplicate_active_job(current_user.id, payload_signature)
    if duplicate_job:
        existing_id = duplicate_job.get("id")
        flash(f"A matching run ({existing_id}) is already active", "error")
        log_auth_event(current_user.id, "job_start", "blocked", _client_ip(), f"duplicate_of={existing_id}")
        return redirect(url_for("index"))
    job_id = _enqueue_job(payload)

    log_auth_event(current_user.id, "job_start", "success", _client_ip(), f"job_id={job_id}")
    flash(f"Job {job_id} started", "success")
    return redirect(url_for("index"))


@app.route("/run/boe-blocking", methods=["POST"])
@login_required
def run_boe_blocking_job():
    _validate_csrf_or_abort()
    saved_credentials = _get_portal_credentials_or_recover(current_user.id, flash_on_error=True)
    if not saved_credentials:
        log_auth_event(current_user.id, "job_start", "failed", _client_ip(), "missing portal credentials")
        flash("Save portal credentials first", "error")
        return redirect(url_for("index"))

    admin_debug_browser = request.form.get("show_browser") == "on"
    is_admin = getattr(current_user, "is_admin", False)
    headless = not admin_debug_browser if is_admin else True

    payload = {
        "retrieval_type": "boe_blocking_current",
        "elapsed_only": request.form.get("elapsed_60_days") == "on",
        "customs_office_code": request.form.get("customs_office_code", "").strip(),
        "im_exporter_code": request.form.get("im_exporter_code", "").strip(),
        "created_by": request.form.get("created_by", "M").strip() or "M",
        "owner": current_user.id,
        "username": saved_credentials["portal_username"],
        "password": saved_credentials["portal_password"],
        "start_date": "",
        "end_date": "",
        "page_size": request.form.get("boe_page_size", "").strip() or "200",
        "output_dir": request.form.get("output_dir", "").strip(),
        "edge_driver_path": request.form.get("edge_driver_path", "").strip(),
        "label": request.form.get("label", "").strip() or "Current BOE Blocking",
        "headless": headless,
    }

    payload_signature = {
        "retrieval_type": payload.get("retrieval_type", "financial"),
        "start_date": payload["start_date"],
        "end_date": payload["end_date"],
        "page_size": str(payload["page_size"]),
        "headless": bool(payload["headless"]),
    }

    duplicate_job = _find_duplicate_active_job(current_user.id, payload_signature)
    if duplicate_job:
        existing_id = duplicate_job.get("id")
        flash(f"A matching run ({existing_id}) is already active", "error")
        log_auth_event(current_user.id, "job_start", "blocked", _client_ip(), f"duplicate_of={existing_id}")
        return redirect(url_for("index"))

    job_id = _enqueue_job(payload)
    log_auth_event(current_user.id, "job_start", "success", _client_ip(), f"job_id={job_id}")
    flash(f"BOE Blocking job {job_id} started", "success")
    return redirect(url_for("index"))


@app.route("/run/boe-status", methods=["POST"])
@login_required
def run_boe_status_job():
    _validate_csrf_or_abort()
    saved_credentials = _get_portal_credentials_or_recover(current_user.id, flash_on_error=True)
    if not saved_credentials:
        log_auth_event(current_user.id, "job_start", "failed", _client_ip(), "missing portal credentials")
        flash("Save portal credentials first", "error")
        return redirect(url_for("index"))

    admin_debug_browser = request.form.get("show_browser") == "on"
    is_admin = getattr(current_user, "is_admin", False)
    headless = not admin_debug_browser if is_admin else True

    payload = {
        "retrieval_type": "boe_status_dates",
        "elapsed_only": False,
        "customs_office_code": "",
        "im_exporter_code": "",
        "created_by": request.form.get("created_by", "M").strip() or "M",
        "owner": current_user.id,
        "username": saved_credentials["portal_username"],
        "password": saved_credentials["portal_password"],
        "start_date": _to_portal_date(request.form.get("boe_status_start_date", "").strip()),
        "end_date": _to_portal_date(request.form.get("boe_status_end_date", "").strip()),
        "page_size": request.form.get("boe_status_page_size", "").strip() or "200",
        "output_dir": request.form.get("output_dir", "").strip(),
        "edge_driver_path": request.form.get("edge_driver_path", "").strip(),
        "label": request.form.get("label", "").strip() or "BOE Status Retrieval",
        "headless": headless,
    }

    payload_signature = {
        "retrieval_type": payload.get("retrieval_type", "financial"),
        "start_date": payload["start_date"],
        "end_date": payload["end_date"],
        "page_size": str(payload["page_size"]),
        "headless": bool(payload["headless"]),
    }

    duplicate_job = _find_duplicate_active_job(current_user.id, payload_signature)
    if duplicate_job:
        existing_id = duplicate_job.get("id")
        flash(f"A matching run ({existing_id}) is already active", "error")
        log_auth_event(current_user.id, "job_start", "blocked", _client_ip(), f"duplicate_of={existing_id}")
        return redirect(url_for("index"))

    job_id = _enqueue_job(payload)
    log_auth_event(current_user.id, "job_start", "success", _client_ip(), f"job_id={job_id}")
    flash(f"BOE Status job {job_id} started", "success")
    return redirect(url_for("index"))


@app.route("/run/<job_id>/retry", methods=["POST"])
@login_required
def retry_job(job_id):
    _validate_csrf_or_abort()

    original = get_retrieval_run_for_user(current_user.id, job_id)
    if not original:
        flash("Original run not found", "error")
        return redirect(url_for("index"))

    original_status = str(original.get("status", "")).lower()
    if original_status not in {"failed", "stopped"}:
        flash("Only failed or stopped runs can be retried", "error")
        return redirect(url_for("index"))

    saved_credentials = _get_portal_credentials_or_recover(current_user.id, flash_on_error=True)
    if not saved_credentials:
        flash("Save portal credentials first", "error")
        return redirect(url_for("index"))

    original_payload = original.get("payload") or {}
    is_admin = getattr(current_user, "is_admin", False)
    headless = bool(original_payload.get("headless", True)) if is_admin else True

    payload = {
        "retrieval_type": str(original_payload.get("retrieval_type", "financial") or "financial"),
        "elapsed_only": bool(original_payload.get("elapsed_only", False)),
        "customs_office_code": str(original_payload.get("customs_office_code", "")),
        "im_exporter_code": str(original_payload.get("im_exporter_code", "")),
        "created_by": str(original_payload.get("created_by", "M") or "M"),
        "owner": current_user.id,
        "username": saved_credentials["portal_username"],
        "password": saved_credentials["portal_password"],
        "start_date": _to_portal_date(str(original_payload.get("start_date", ""))),
        "end_date": _to_portal_date(str(original_payload.get("end_date", ""))),
        "page_size": str(original_payload.get("page_size", DEFAULT_PAGE_SIZE)),
        "output_dir": str(original_payload.get("output_dir", OUTPUT_DIR)),
        "edge_driver_path": EDGE_DRIVER_PATH,
        "label": str(original_payload.get("label", "Customs Account")),
        "headless": headless,
    }

    payload_signature = {
        "retrieval_type": payload.get("retrieval_type", "financial"),
        "start_date": payload["start_date"],
        "end_date": payload["end_date"],
        "page_size": str(payload["page_size"]),
        "headless": bool(payload["headless"]),
    }
    duplicate_job = _find_duplicate_active_job(current_user.id, payload_signature)
    if duplicate_job:
        existing_id = duplicate_job.get("id")
        flash(f"A matching run ({existing_id}) is already active", "error")
        return redirect(url_for("index"))

    new_job_id = _enqueue_job(payload)
    log_auth_event(current_user.id, "job_retry", "success", _client_ip(), f"from={job_id}, to={new_job_id}")
    flash(f"Retry started as run {new_job_id}", "success")
    return redirect(url_for("index"))


@app.route("/run/<job_id>/stop", methods=["POST"])
@login_required
def stop_job(job_id):
    _validate_csrf_or_abort()

    persisted = get_retrieval_run_for_user(current_user.id, job_id)

    with _jobs_lock:
        job = _jobs.get(job_id)
        cancel_event = _job_cancel_events.get(job_id)

        if not job or job.get("owner") != current_user.id:
            if not persisted:
                flash("Run not found", "error")
                return redirect(url_for("index"))

            persisted_status = str(persisted.get("status", "")).lower()
            if persisted_status in {"queued", "running", "stopping"}:
                upsert_retrieval_run(
                    job_id=job_id,
                    user_id=current_user.id,
                    status="stopped",
                    last_message="Stop requested but run was no longer active; marked stopped",
                    row_count=((persisted.get("result") or {}).get("row_count") or 0),
                    created_at=(persisted.get("created_at") or datetime.now(timezone.utc).isoformat()),
                    ended_at=datetime.now(timezone.utc).isoformat(),
                    payload=persisted.get("payload") or {},
                )
                log_auth_event(current_user.id, "job_stop", "success", _client_ip(), f"job_id={job_id}, source=persisted")
                flash(f"Run {job_id} marked as stopped", "success")
                return redirect(url_for("index"))

            flash("Run is not active", "error")
            return redirect(url_for("index"))

        if job.get("status") not in {"queued", "running"}:
            flash("Run is not active", "error")
            return redirect(url_for("index"))

        if cancel_event:
            cancel_event.set()
        job["status"] = "stopping"
        job["last_message"] = "Stop requested by user"

    upsert_retrieval_run(
        job_id=job_id,
        user_id=current_user.id,
        status="stopping",
        last_message="Stop requested by user",
        row_count=0,
        created_at=(job.get("created_at") or datetime.now(timezone.utc).isoformat()),
        payload=job.get("payload") or {},
    )
    log_auth_event(current_user.id, "job_stop", "success", _client_ip(), f"job_id={job_id}")
    flash(f"Stop requested for run {job_id}", "success")
    return redirect(url_for("index"))


@app.route("/download/<job_id>/<file_key>", methods=["GET"])
@login_required
def download_generated_file(job_id, file_key):
    _cleanup_old_generated_files(current_user.id)

    file_record = get_generated_file(current_user.id, job_id, file_key)
    if not file_record:
        abort(404)

    log_auth_event(current_user.id, "file_download", "success", _client_ip(), f"job_id={job_id}, file={file_key}")
    return send_file(
        BytesIO(file_record["file_blob"]),
        mimetype=file_record.get("mime_type") or "application/octet-stream",
        as_attachment=True,
        download_name=file_record.get("file_name") or f"{job_id}_{file_key}",
    )


@app.route("/view/<job_id>/<file_key>", methods=["GET"])
@login_required
def view_generated_file(job_id, file_key):
    _cleanup_old_generated_files(current_user.id)

    file_record = get_generated_file(current_user.id, job_id, file_key)
    if not file_record:
        abort(404)

    log_auth_event(current_user.id, "file_view", "success", _client_ip(), f"job_id={job_id}, file={file_key}")
    response = send_file(
        BytesIO(file_record["file_blob"]),
        mimetype=file_record.get("mime_type") or "application/octet-stream",
        as_attachment=False,
        download_name=file_record.get("file_name") or f"{job_id}_{file_key}",
    )
    response.headers["Content-Disposition"] = f'inline; filename="{file_record.get("file_name") or f"{job_id}_{file_key}"}"'
    return response


@app.route("/files/<job_id>/delete", methods=["POST"])
@login_required
def delete_job_files(job_id):
    _validate_csrf_or_abort()
    
    total_deleted = 0
    
    # Check for live job files first
    with _jobs_lock:
        job = _jobs.get(job_id)
    
    if job and job.get("owner") == current_user.id:
        # Live job - delete files from disk
        result = job.get("result") or {}
        files = result.get("files") or {}
        for file_key, file_path in files.items():
            if file_path and isinstance(file_path, str) and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    total_deleted += 1
                except OSError:
                    pass
        # Clear files from live job
        if files:
            job["result"]["files"] = {}
    
    # Also delete any persisted files from database
    db_deleted = delete_generated_files_for_job(current_user.id, job_id)
    total_deleted += db_deleted
    
    if total_deleted > 0:
        log_auth_event(current_user.id, "file_delete_all", "success", _client_ip(), f"job_id={job_id}, count={total_deleted}")
        flash(f"Deleted {total_deleted} file(s) for run {job_id}", "success")
    else:
        log_auth_event(current_user.id, "file_delete_all", "failed", _client_ip(), f"job_id={job_id}, no files")
        flash("No files found for this run", "error")
    return redirect(url_for("index"))


@app.route("/run/<job_id>/delete", methods=["POST"])
@login_required
def delete_run(job_id):
    """Hard delete an entire run (files + record). Permanent removal."""
    _validate_csrf_or_abort()
    
    # Check if run exists and belongs to user
    run = get_retrieval_run_for_user(current_user.id, job_id)
    if not run:
        abort(404)
    
    # Delete from live jobs if present
    with _jobs_lock:
        if job_id in _jobs:
            job = _jobs[job_id]
            if job.get("owner") == current_user.id:
                # Delete files from disk
                result = job.get("result") or {}
                files = result.get("files") or {}
                for file_key, file_path in files.items():
                    if file_path and isinstance(file_path, str) and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except OSError:
                            pass
                del _jobs[job_id]
    
    # Delete from database (files + run record)
    delete_generated_files_for_job(current_user.id, job_id)
    delete_retrieval_run(current_user.id, job_id)
    
    log_auth_event(current_user.id, "run_delete", "success", _client_ip(), f"job_id={job_id}")
    flash(f"Run {job_id} permanently deleted", "success")
    return redirect(url_for("index"))


@app.route("/admin/audit", methods=["GET"])
@login_required
def admin_audit():
    _admin_required_or_403()
    events = get_recent_auth_events(limit=300)
    return render_template("admin_audit.html", events=events, csrf_token=_new_csrf_token())


@app.route("/admin/session-log", methods=["GET"])
@login_required
def admin_session_log():
    _admin_required_or_403()
    log_path = _session_log_path()
    log_content = ""
    total_line_count = 0
    shown_line_count = 0
    file_size = 0
    query = request.args.get("q", "").strip()

    lines_raw = request.args.get("lines", "1000").strip()
    try:
        lines_limit = int(lines_raw)
    except ValueError:
        lines_limit = 1000
    lines_limit = max(1, min(lines_limit, 20000))

    if os.path.exists(log_path):
        file_size = os.path.getsize(log_path)
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()

        total_line_count = len(lines)

        if query:
            lowered_query = query.lower()
            lines = [line for line in lines if lowered_query in line.lower()]

        if lines_limit and len(lines) > lines_limit:
            lines = lines[-lines_limit:]

        shown_line_count = len(lines)
        log_content = "\n".join(lines)

    return render_template(
        "admin_session_log.html",
        log_exists=os.path.exists(log_path),
        log_content=log_content,
        query=query,
        lines_limit=lines_limit,
        total_line_count=total_line_count,
        shown_line_count=shown_line_count,
        file_size=file_size,
        csrf_token=_new_csrf_token(),
    )


@app.route("/admin/session-log/download", methods=["GET"])
@login_required
def admin_session_log_download():
    _admin_required_or_403()
    log_path = _session_log_path()
    if not os.path.exists(log_path):
        abort(404)
    return send_file(log_path, mimetype="text/plain", as_attachment=True, download_name="session.log")


@app.route("/admin/users", methods=["GET"])
@login_required
def admin_users():
    _admin_required_or_403()
    q = request.args.get("q", "").strip()
    role_filter = request.args.get("role", "all").strip().lower()
    status_filter = request.args.get("status", "all").strip().lower()
    sort_by = request.args.get("sort", "user_id").strip().lower()
    sort_dir = request.args.get("dir", "asc").strip().lower()

    users = list_app_users()
    expiring_soon_count = 0

    now_utc = datetime.now(timezone.utc)
    for user in users:
        user["lock_remaining_minutes"] = 0
        locked_until_raw = user.get("locked_until")
        if locked_until_raw:
            lock_time = _to_utc_datetime(locked_until_raw)
            if lock_time:
                remaining_seconds = (lock_time - now_utc).total_seconds()
                if remaining_seconds > 0:
                    user["lock_remaining_minutes"] = int(remaining_seconds // 60) + 1

        policy = _password_policy_state(user)
        user["password_expired"] = bool(policy.get("expired"))
        user["password_days_remaining"] = policy.get("days_remaining")
        user["password_expiring_soon"] = bool(
            user["password_days_remaining"]
            and 0 < int(user["password_days_remaining"]) <= int(PASSWORD_EXPIRY_WARNING_DAYS)
        )
        if user["password_expiring_soon"]:
            expiring_soon_count += 1

    if q:
        q_lower = q.lower()
        users = [
            user for user in users
            if q_lower in str(user.get("user_id", "")).lower() or q_lower in str(user.get("email", "")).lower()
        ]

    if role_filter in {"admin", "user"}:
        users = [user for user in users if str(user.get("role", "")).lower() == role_filter]

    if status_filter in {"active", "inactive", "locked", "expiring_soon"}:
        if status_filter == "active":
            users = [user for user in users if bool(user.get("is_active", 0))]
        elif status_filter == "inactive":
            users = [user for user in users if not bool(user.get("is_active", 0))]
        elif status_filter == "expiring_soon":
            users = [user for user in users if bool(user.get("password_expiring_soon", False))]
        else:
            users = [user for user in users if bool(user.get("locked_until"))]

    allowed_sort_fields = {"user_id", "email", "role", "is_active", "failed_attempts", "locked_until", "updated_at", "password_days_remaining"}
    if sort_by not in allowed_sort_fields:
        sort_by = "user_id"
    reverse = sort_dir == "desc"

    def _sort_key(user):
        value = user.get(sort_by)
        if sort_by in {"is_active", "failed_attempts"}:
            return int(value or 0)
        if sort_by == "password_days_remaining":
            return int(value) if value is not None else 999999
        return str(value or "").lower()

    users = sorted(users, key=_sort_key, reverse=reverse)

    return render_template(
        "admin_users.html",
        users=users,
        q=q,
        role_filter=role_filter,
        status_filter=status_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        expiring_soon_count=expiring_soon_count,
        warning_days=int(PASSWORD_EXPIRY_WARNING_DAYS),
        csrf_token=_new_csrf_token(),
    )


@app.route("/admin/users", methods=["POST"])
@login_required
def admin_users_action():
    _admin_required_or_403()
    _validate_csrf_or_abort()

    action = request.form.get("action", "").strip()
    target_user = request.form.get("target_user", "").strip()

    if action == "create":
        new_user = request.form.get("new_user", "").strip()
        new_password = request.form.get("new_password", "")
        new_role = request.form.get("new_role", "user").strip()
        new_email_raw = request.form.get("new_email", "")
        company_name = request.form.get("company_name", "").strip()
        company_address = request.form.get("company_address", "").strip()
        company_phone = request.form.get("company_phone", "").strip()
        new_email = _normalize_email(new_email_raw)

        if not new_user or not new_password:
            flash("Username and password are required", "error")
            return redirect(url_for("admin_users"))

        if get_app_user(new_user):
            flash("User already exists", "error")
            return redirect(url_for("admin_users"))

        if new_email is None:
            flash("Invalid email format", "error")
            return redirect(url_for("admin_users"))
        if new_email:
            existing_by_email = get_app_user_by_email(new_email)
            if existing_by_email:
                flash("Email already in use", "error")
                return redirect(url_for("admin_users"))

        ensure_app_user(new_user, generate_password_hash(new_password), role=new_role, is_active=True, email=new_email or None)
        set_user_company_profile(
            new_user,
            company_name=company_name,
            company_address=company_address,
            company_phone=company_phone,
            company_logo_path=None,
        )
        log_auth_event(current_user.id, "admin_user_create", "success", _client_ip(), f"target={new_user}")
        flash(f"User '{new_user}' created", "success")
        return redirect(url_for("admin_users"))

    if not target_user:
        flash("Target user is required", "error")
        return redirect(url_for("admin_users"))

    if action == "reset_password":
        new_password = request.form.get("new_password", "")
        if not new_password:
            flash("New password is required", "error")
            return redirect(url_for("admin_users"))
        set_user_password(target_user, generate_password_hash(new_password), must_change_password=True)
        log_auth_event(current_user.id, "admin_user_reset_password", "success", _client_ip(), f"target={target_user}")
        flash(f"Password reset for '{target_user}'. User must change password on next login.", "success")
        return redirect(url_for("admin_users"))

    if action == "toggle_active":
        if target_user == current_user.id:
            flash("You cannot deactivate your own account", "error")
            return redirect(url_for("admin_users"))
        target = get_app_user(target_user)
        if not target:
            flash("Target user not found", "error")
            return redirect(url_for("admin_users"))
        will_deactivate = bool(target.get("is_active", 0))
        is_admin_target = str(target.get("role", "")).lower() == "admin"
        if will_deactivate and is_admin_target and _count_active_admins() <= 1:
            flash("Cannot deactivate the last active admin", "error")
            return redirect(url_for("admin_users"))
        set_user_active(target_user, not bool(target.get("is_active", 0)))
        log_auth_event(current_user.id, "admin_user_toggle_active", "success", _client_ip(), f"target={target_user}")
        flash(f"Updated active status for '{target_user}'", "success")
        return redirect(url_for("admin_users"))

    if action == "set_role":
        new_role = request.form.get("new_role", "user").strip()
        target = get_app_user(target_user)
        if not target:
            flash("Target user not found", "error")
            return redirect(url_for("admin_users"))
        if target_user == current_user.id and new_role != "admin":
            flash("You cannot remove your own admin role", "error")
            return redirect(url_for("admin_users"))
        is_admin_target = str(target.get("role", "")).lower() == "admin"
        is_target_active = bool(target.get("is_active", 0))
        if is_admin_target and is_target_active and new_role != "admin" and _count_active_admins() <= 1:
            flash("Cannot remove role from the last active admin", "error")
            return redirect(url_for("admin_users"))
        set_user_role(target_user, new_role)
        log_auth_event(current_user.id, "admin_user_set_role", "success", _client_ip(), f"target={target_user}, role={new_role}")
        flash(f"Updated role for '{target_user}'", "success")
        return redirect(url_for("admin_users"))

    if action == "unlock":
        target = get_app_user(target_user)
        if not target:
            flash("Target user not found", "error")
            return redirect(url_for("admin_users"))
        clear_failed_login(target_user)
        log_auth_event(current_user.id, "admin_user_unlock", "success", _client_ip(), f"target={target_user}")
        flash(f"Unlocked account for '{target_user}'", "success")
        return redirect(url_for("admin_users"))

    if action == "notify_reminder":
        target = get_app_user(target_user)
        if not target:
            flash("Target user not found", "error")
            return redirect(url_for("admin_users"))
        target_email = (target.get("email") or "").strip()
        if not target_email:
            log_auth_event(
                current_user.id,
                "admin_password_reminder",
                "failed",
                _client_ip(),
                f"target={target_user}, reason=no_email",
            )
            flash(f"No email on file for '{target_user}'", "error")
            return redirect(url_for("admin_users"))
        policy = _password_policy_state(target)
        days_remaining = policy.get("days_remaining")
        log_auth_event(
            current_user.id,
            "admin_password_reminder",
            "success",
            _client_ip(),
            f"target={target_user}, email={target_email}, days_remaining={days_remaining}",
        )
        flash(f"Password reminder recorded for '{target_user}' ({target_email})", "success")
        return redirect(url_for("admin_users"))

    if action == "delete_user":
        target = get_app_user(target_user)
        if not target:
            flash("Target user not found", "error")
            return redirect(url_for("admin_users"))
        delete_confirmation = request.form.get("delete_confirmation", "").strip().upper()
        if delete_confirmation != "DELETE":
            flash("Type DELETE to confirm user deletion", "error")
            return redirect(url_for("admin_users"))
        if target_user == current_user.id:
            flash("Use Close Account from your profile to delete your own account", "error")
            return redirect(url_for("admin_users"))

        is_admin_target = str(target.get("role", "")).lower() == "admin"
        is_target_active = bool(target.get("is_active", 0))
        if is_admin_target and is_target_active and _count_active_admins() <= 1:
            flash("Cannot delete the last active admin", "error")
            return redirect(url_for("admin_users"))

        deleted = delete_app_user(target_user)
        if deleted:
            log_auth_event(current_user.id, "admin_user_delete", "success", _client_ip(), f"target={target_user}")
            flash(f"Deleted user '{target_user}'", "success")
            return redirect(url_for("admin_users"))

        flash("Delete failed", "error")
        return redirect(url_for("admin_users"))

    if action == "set_email":
        target = get_app_user(target_user)
        if not target:
            flash("Target user not found", "error")
            return redirect(url_for("admin_users"))

        new_email_raw = request.form.get("new_email", "")
        new_email = _normalize_email(new_email_raw)
        if new_email is None:
            flash("Invalid email format", "error")
            return redirect(url_for("admin_users"))

        if new_email:
            existing_by_email = get_app_user_by_email(new_email)
            if existing_by_email and existing_by_email.get("user_id") != target_user:
                flash("Email already in use", "error")
                return redirect(url_for("admin_users"))

        set_user_email(target_user, new_email)
        log_auth_event(current_user.id, "admin_user_set_email", "success", _client_ip(), f"target={target_user}, email={new_email or ''}")
        flash(f"Updated email for '{target_user}'", "success")
        return redirect(url_for("admin_users"))

    flash("Unknown action", "error")
    return redirect(url_for("admin_users"))


@app.route("/status/<job_id>", methods=["GET"])
@login_required
def status(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job or job.get("owner") != current_user.id:
        persisted = get_retrieval_run_for_user(current_user.id, job_id)
        if not persisted:
            return jsonify({"error": "Job not found"}), 404
        return jsonify(persisted)

    return jsonify(job)


@app.route("/jobs/status/batch", methods=["GET"])
@login_required
def jobs_status_batch():
    """Return all jobs for current user in a single request (optimization)."""
    with _jobs_lock:
        live_jobs = [job for job in _jobs.values() if job.get("owner") == current_user.id]
    
    persisted_jobs = list_recent_retrieval_runs_for_user(current_user.id, limit=500)
    
    jobs_by_id = {job["id"]: job for job in persisted_jobs}
    for job in live_jobs:
        jobs_by_id[job["id"]] = job
    
    all_jobs = sorted(jobs_by_id.values(), key=lambda j: j.get("created_at", ""), reverse=True)
    return jsonify({"jobs": all_jobs})


@app.route("/logs", methods=["GET"])
@login_required
def logs():
    outcome_filter = request.args.get("outcome", "all").strip().lower()
    event_type_filter = request.args.get("event_type", "all").strip().lower()
    try:
        limit = int(request.args.get("limit", "200"))
    except ValueError:
        limit = 200
    limit = max(10, min(limit, 500))

    all_events = get_recent_auth_events_for_user(current_user.id, limit=limit)
    available_event_types = sorted(
        {str(event.get("event_type", "")).strip() for event in all_events if event.get("event_type")}
    )

    events = all_events
    if outcome_filter != "all":
        events = [event for event in events if str(event.get("outcome", "")).lower() == outcome_filter]
    if event_type_filter != "all":
        events = [event for event in events if str(event.get("event_type", "")).lower() == event_type_filter]

    lines = [
        f"[{event['event_time']}] {event['event_type']} | {event['outcome']} | {event.get('details') or ''}"
        for event in events
    ]
    return jsonify({
        "lines": lines,
        "events": events,
        "available_event_types": available_event_types,
    })


if __name__ == "__main__":
    app.run(debug=True)
