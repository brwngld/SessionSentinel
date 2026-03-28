import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from cryptography.fernet import Fernet
from cryptography.fernet import InvalidToken

from config import (
    ALLOW_DEV_ADMIN_SETUP,
    CREDENTIAL_ENCRYPTION_KEY,
    DATABASE_PATH,
    DB_BACKEND,
    TURSO_AUTH_TOKEN,
    TURSO_DATABASE_URL,
)

try:
    import libsql
except Exception:  # pragma: no cover - import depends on selected backend
    libsql = None

DB_PATH = DATABASE_PATH if os.path.isabs(DATABASE_PATH) else os.path.join(os.path.dirname(__file__), DATABASE_PATH)

# For Vercel/serverless: use /tmp if the default path is not writable
# This is because Vercel's /var/task is read-only
if not os.access(os.path.dirname(DB_PATH) or '.', os.W_OK):
    DB_PATH = os.path.join('/tmp', 'app.db')

DB_DIR = os.path.dirname(DB_PATH)
if DB_DIR:
    os.makedirs(DB_DIR, exist_ok=True)


class CredentialDecryptionError(Exception):
    """Raised when saved portal credentials cannot be decrypted with active key."""


def _build_fernet():
    key = CREDENTIAL_ENCRYPTION_KEY.strip()
    key_lower = key.lower()
    placeholder_key = (not key) or key_lower.startswith("replace-") or "change-this" in key_lower

    if not ALLOW_DEV_ADMIN_SETUP and placeholder_key:
        raise RuntimeError(
            "Refusing startup: CREDENTIAL_ENCRYPTION_KEY is missing or placeholder while running in non-dev mode."
        )

    if not key:
        key = Fernet.generate_key().decode("utf-8")

    try:
        return Fernet(key.encode("utf-8"))
    except Exception:
        raise RuntimeError(
            "Refusing startup: CREDENTIAL_ENCRYPTION_KEY must be a valid Fernet key."
        )


_FERNET = _build_fernet()

_db_initialized = False
_db_init_lock = __import__('threading').Lock()


class _LibsqlConnectionWrapper:
    """Wrapper around libsql connection to support dict-like row access via row_factory simulation."""
    def __init__(self, conn):
        self._conn = conn
    
    def execute(self, sql, params=()):
        """Execute and return cursor that wraps result in dict-like rows."""
        result = self._conn.execute(sql, params)
        # Wrap result to provide dict-like row access
        return _LibsqlCursor(result)
    
    def executemany(self, sql, params_list):
        """Execute multiple statements."""
        for params in params_list:
            self._conn.execute(sql, params)
    
    def commit(self):
        """Commit changes."""
        if hasattr(self._conn, 'commit'):
            self._conn.commit()
    
    def rollback(self):
        """Rollback changes."""
        if hasattr(self._conn, 'rollback'):
            try:
                self._conn.rollback()
            except Exception:
                pass
    
    def close(self):
        """Close connection."""
        try:
            if hasattr(self._conn, 'close'):
                self._conn.close()
        except Exception:
            pass


class _LibsqlCursor:
    """Cursor wrapper that provides dict-like access to libsql results."""
    def __init__(self, result):
        self._result = result
        self._rows = []
        self._index = 0
        self.rowcount = int(getattr(result, "rows_affected", 0) or 0)
        self.lastrowid = getattr(result, "last_insert_rowid", None)
        
        # Cache all rows upfront - handle both libsql tuples and columns
        if hasattr(result, 'rows') and hasattr(result, 'columns'):
            # libsql: rows are tuples, columns are list of names
            cols = result.columns if result.columns else []
            for row_tuple in result.rows:
                if cols:
                    row_dict = {col: val for col, val in zip(cols, row_tuple)}
                    self._rows.append(row_dict)
                else:
                    # No column names, return tuple as-is
                    self._rows.append(row_tuple)
        elif hasattr(result, 'rows'):
            # libsql without columns - just return tuples
            self._rows = list(result.rows)
    
    def fetchone(self):
        """Fetch one row."""
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row
    
    def fetchall(self):
        """Fetch all remaining rows."""
        rows = self._rows[self._index:]
        self._index = len(self._rows)
        return rows


def _connect():
    """Create a new database connection."""
    if DB_BACKEND == "sqlite":
        # Use libsql if available (with sync), otherwise fall back to sqlite3
        if libsql is not None and TURSO_DATABASE_URL and TURSO_AUTH_TOKEN:
            try:
                # Use libsql with local SQLite synced to Turso
                conn = libsql.connect(
                    DB_PATH,
                    sync_url=TURSO_DATABASE_URL,
                    auth_token=TURSO_AUTH_TOKEN,
                    sync_interval=60  # Sync every 60 seconds
                )
                return _LibsqlConnectionWrapper(conn)
            except Exception as e:
                import logging
                logging.warning(f"Failed to connect to Turso, falling back to local SQLite: {e}")
                # Fall through to regular sqlite3 connection
        
        # Regular SQLite connection
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    if DB_BACKEND == "turso":
        if libsql is None:
            raise RuntimeError("DB_BACKEND=turso requires the 'libsql' package. Install with: pip install libsql")
        
        if not TURSO_DATABASE_URL:
            raise RuntimeError("DB_BACKEND=turso requires TURSO_DATABASE_URL environment variable")
        
        if not TURSO_AUTH_TOKEN:
            raise RuntimeError("DB_BACKEND=turso requires TURSO_AUTH_TOKEN environment variable")
        
        try:
            # Try to connect with sync enabled (local file synced to Turso)
            conn = libsql.connect(
                DB_PATH,
                sync_url=TURSO_DATABASE_URL,
                auth_token=TURSO_AUTH_TOKEN,
                sync_interval=60
            )
            return _LibsqlConnectionWrapper(conn)
        except Exception as e:
            import logging
            logging.warning(f"Failed to connect to Turso with sync: {e}")
            
            # If sync fails (e.g., on Vercel due to read-only fs), use remote-only mode
            try:
                logging.info("Attempting Turso remote-only mode (no local sync)...")
                conn = libsql.connect(
                    sync_url=TURSO_DATABASE_URL,
                    auth_token=TURSO_AUTH_TOKEN
                )
                return _LibsqlConnectionWrapper(conn)
            except Exception as e2:
                logging.error(f"Failed to connect to Turso remote-only: {e2}")
                # Last resort: fallback local SQLite
                try:
                    logging.warning("Falling back to local SQLite only (no Turso sync)")
                    fallback_path = "/tmp/app.db"
                    conn = sqlite3.connect(fallback_path)
                    conn.row_factory = sqlite3.Row
                    return conn
                except Exception as e3:
                    logging.error(f"Failed to connect to fallback database: {e3}")
                    # In-memory SQLite as last resort
                    conn = sqlite3.connect(":memory:")
                    conn.row_factory = sqlite3.Row
                    return conn
    
    raise RuntimeError(f"Invalid DB_BACKEND: {DB_BACKEND}")



def init_db():
    conn = _connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_users (
                user_id TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                is_active INTEGER NOT NULL DEFAULT 1,
                failed_attempts INTEGER NOT NULL DEFAULT 0,
                locked_until TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        existing_cols = {
            row["name"] if isinstance(row, dict) else row[1]
            for row in conn.execute("PRAGMA table_info(app_users)").fetchall()
        }
        if "role" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'")
        if "is_active" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if "must_change_password" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0")
        if "password_changed_at" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN password_changed_at TEXT")
            conn.execute(
                "UPDATE app_users SET password_changed_at = ? WHERE password_changed_at IS NULL",
                (datetime.now(timezone.utc).isoformat(),),
            )
        if "email" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN email TEXT")
        if "company_name" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN company_name TEXT")
        if "company_address" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN company_address TEXT")
        if "company_phone" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN company_phone TEXT")
        if "company_logo_path" not in existing_cols:
            conn.execute("ALTER TABLE app_users ADD COLUMN company_logo_path TEXT")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS portal_credentials (
                user_id TEXT PRIMARY KEY,
                portal_username TEXT NOT NULL,
                portal_password_encrypted TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time TEXT NOT NULL,
                user_id TEXT,
                event_type TEXT NOT NULL,
                outcome TEXT NOT NULL,
                source_ip TEXT,
                details TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS retrieval_runs (
                job_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                status TEXT NOT NULL,
                last_message TEXT,
                row_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                ended_at TEXT,
                payload_json TEXT
            )
            """
        )
        run_cols = {
            row["name"] if isinstance(row, dict) else row[1]
            for row in conn.execute("PRAGMA table_info(retrieval_runs)").fetchall()
        }
        if "payload_json" not in run_cols:
            conn.execute("ALTER TABLE retrieval_runs ADD COLUMN payload_json TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS generated_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                file_key TEXT NOT NULL,
                file_name TEXT NOT NULL,
                mime_type TEXT,
                file_blob BLOB NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(job_id, user_id, file_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_alias_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                ref_key TEXT NOT NULL,
                canonical_account TEXT NOT NULL,
                decision_type TEXT NOT NULL DEFAULT 'accept',
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, ref_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_custom_names (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                account_name TEXT NOT NULL COLLATE NOCASE,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, account_name)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_pricing_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                file_key TEXT NOT NULL,
                account_name TEXT NOT NULL COLLATE NOCASE,
                pricing_mode TEXT NOT NULL DEFAULT 'none',
                fixed_price REAL NOT NULL DEFAULT 0,
                line_prices_json TEXT NOT NULL DEFAULT '{}',
                currency_code TEXT NOT NULL DEFAULT 'GHS',
                manual_rate REAL,
                conversion_note TEXT,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, job_id, file_key, account_name)
            )
            """
        )
        pricing_cols = {
            row["name"] if isinstance(row, dict) else row[1]
            for row in conn.execute("PRAGMA table_info(account_pricing_profiles)").fetchall()
        }
        if "currency_code" not in pricing_cols:
            conn.execute("ALTER TABLE account_pricing_profiles ADD COLUMN currency_code TEXT NOT NULL DEFAULT 'GHS'")
        if "manual_rate" not in pricing_cols:
            conn.execute("ALTER TABLE account_pricing_profiles ADD COLUMN manual_rate REAL")
        if "conversion_note" not in pricing_cols:
            conn.execute("ALTER TABLE account_pricing_profiles ADD COLUMN conversion_note TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_pricing_rate_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                file_key TEXT NOT NULL,
                account_name TEXT NOT NULL COLLATE NOCASE,
                pricing_mode TEXT NOT NULL,
                currency_code TEXT NOT NULL,
                manual_rate REAL,
                conversion_note TEXT,
                report_total REAL NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def ensure_db_initialized():
    """Lazily initialize the database. Safe to call multiple times."""
    global _db_initialized
    
    if _db_initialized:
        return
    
    with _db_init_lock:
        if _db_initialized:
            return
        
        try:
            init_db()
            _db_initialized = True
        except Exception as e:
            import logging
            logging.warning(f"Failed to initialize database on first request: {e}")
            # Don't raise - let the app continue without DB
            # Routes that need DB will fail gracefully


def upsert_account_alias_rule(user_id, ref_key, canonical_account, decision_type="accept"):
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO account_alias_rules (user_id, ref_key, canonical_account, decision_type, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, ref_key)
            DO UPDATE SET
                canonical_account=excluded.canonical_account,
                decision_type=excluded.decision_type,
                updated_at=excluded.updated_at
            """,
            (user_id, ref_key, canonical_account, decision_type, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_account_alias_rules_for_user(user_id):
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT ref_key, canonical_account, decision_type
            FROM account_alias_rules
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]


def delete_account_alias_rule(user_id, ref_key):
    conn = _connect()
    try:
        cursor = conn.execute(
            "DELETE FROM account_alias_rules WHERE user_id = ? AND ref_key = ?",
            (user_id, ref_key),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def upsert_custom_account_name(user_id, account_name):
    account_name = str(account_name or "").strip()
    if not account_name:
        return
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO account_custom_names (user_id, account_name, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, account_name)
            DO UPDATE SET
                updated_at=excluded.updated_at
            """,
            (user_id, account_name, now),
        )
        conn.commit()
    finally:
        conn.close()


def list_custom_account_names_for_user(user_id):
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT account_name
            FROM account_custom_names
            WHERE user_id = ?
            ORDER BY account_name COLLATE NOCASE ASC
            """,
            (user_id,),
        ).fetchall()
    finally:
        conn.close()

    return [str(row["account_name"]).strip() for row in rows if str(row["account_name"]).strip()]


def delete_custom_account_name(user_id, account_name):
    account_name = str(account_name or "").strip()
    if not account_name:
        return 0
    conn = _connect()
    try:
        cursor = conn.execute(
            "DELETE FROM account_custom_names WHERE user_id = ? AND account_name = ?",
            (user_id, account_name),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def upsert_account_pricing_profile(
    user_id,
    job_id,
    file_key,
    account_name,
    pricing_mode="none",
    fixed_price=0.0,
    line_prices=None,
    currency_code="GHS",
    manual_rate=None,
    conversion_note="",
):
    mode = str(pricing_mode or "none").strip().lower()
    if mode not in {"none", "automatic", "manual"}:
        mode = "none"
    try:
        fixed = float(fixed_price or 0)
    except (TypeError, ValueError):
        fixed = 0.0

    cleaned_line_prices = {}
    if isinstance(line_prices, dict):
        for key, value in line_prices.items():
            line_key = str(key or "").strip()
            if not line_key:
                continue
            try:
                amount = float(value)
            except (TypeError, ValueError):
                amount = 0.0
            cleaned_line_prices[line_key] = max(0.0, amount)

    currency = str(currency_code or "GHS").strip().upper()
    if currency not in {"GHS", "USD"}:
        currency = "GHS"
    try:
        parsed_manual_rate = float(manual_rate) if manual_rate is not None else None
    except (TypeError, ValueError):
        parsed_manual_rate = None
    if parsed_manual_rate is not None and parsed_manual_rate <= 0:
        parsed_manual_rate = None
    note = str(conversion_note or "").strip()

    now = datetime.now(timezone.utc).isoformat()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO account_pricing_profiles (
                user_id, job_id, file_key, account_name, pricing_mode, fixed_price, line_prices_json, currency_code, manual_rate, conversion_note, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, job_id, file_key, account_name)
            DO UPDATE SET
                pricing_mode=excluded.pricing_mode,
                fixed_price=excluded.fixed_price,
                line_prices_json=excluded.line_prices_json,
                currency_code=excluded.currency_code,
                manual_rate=excluded.manual_rate,
                conversion_note=excluded.conversion_note,
                updated_at=excluded.updated_at
            """,
            (
                user_id,
                job_id,
                file_key,
                account_name,
                mode,
                max(0.0, fixed),
                json.dumps(cleaned_line_prices),
                currency,
                parsed_manual_rate,
                note,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_account_pricing_profile(user_id, job_id, file_key, account_name):
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT pricing_mode, fixed_price, line_prices_json, currency_code, manual_rate, conversion_note, updated_at
            FROM account_pricing_profiles
            WHERE user_id = ? AND job_id = ? AND file_key = ? AND account_name = ?
            """,
            (user_id, job_id, file_key, account_name),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return None

    try:
        line_prices = json.loads(row["line_prices_json"] or "{}")
    except json.JSONDecodeError:
        line_prices = {}

    return {
        "pricing_mode": str(row["pricing_mode"] or "none").strip().lower(),
        "fixed_price": float(row["fixed_price"] or 0),
        "line_prices": line_prices if isinstance(line_prices, dict) else {},
        "currency_code": str(row["currency_code"] or "GHS").strip().upper() or "GHS",
        "manual_rate": float(row["manual_rate"] or 0) if row["manual_rate"] is not None else None,
        "conversion_note": str(row["conversion_note"] or "").strip(),
        "updated_at": row["updated_at"],
    }


def list_account_pricing_profiles_for_file(user_id, job_id, file_key):
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT account_name, pricing_mode, fixed_price, line_prices_json, currency_code, manual_rate, conversion_note, updated_at
            FROM account_pricing_profiles
            WHERE user_id = ? AND job_id = ? AND file_key = ?
            """,
            (user_id, job_id, file_key),
        ).fetchall()
    finally:
        conn.close()

    out = {}
    for row in rows:
        try:
            line_prices = json.loads(row["line_prices_json"] or "{}")
        except json.JSONDecodeError:
            line_prices = {}
        out[str(row["account_name"])] = {
            "pricing_mode": str(row["pricing_mode"] or "none").strip().lower(),
            "fixed_price": float(row["fixed_price"] or 0),
            "line_prices": line_prices if isinstance(line_prices, dict) else {},
            "currency_code": str(row["currency_code"] or "GHS").strip().upper() or "GHS",
            "manual_rate": float(row["manual_rate"] or 0) if row["manual_rate"] is not None else None,
            "conversion_note": str(row["conversion_note"] or "").strip(),
            "updated_at": row["updated_at"],
        }
    return out


def record_account_pricing_rate_history(
    user_id,
    job_id,
    file_key,
    account_name,
    pricing_mode,
    currency_code,
    manual_rate,
    conversion_note,
    report_total,
):
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO account_pricing_rate_history (
                user_id, job_id, file_key, account_name, pricing_mode, currency_code, manual_rate, conversion_note, report_total, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                job_id,
                file_key,
                account_name,
                str(pricing_mode or "none").strip().lower(),
                str(currency_code or "GHS").strip().upper(),
                float(manual_rate) if manual_rate is not None else None,
                str(conversion_note or "").strip(),
                float(report_total or 0),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_account_pricing_rate_history(user_id, job_id, file_key, account_name, limit=12):
    try:
        limited = max(1, min(int(limit or 12), 50))
    except (TypeError, ValueError):
        limited = 12

    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT pricing_mode, currency_code, manual_rate, conversion_note, report_total, created_at
            FROM account_pricing_rate_history
            WHERE user_id = ? AND job_id = ? AND file_key = ? AND account_name = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (user_id, job_id, file_key, account_name, limited),
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "pricing_mode": str(row["pricing_mode"] or "none").strip().lower(),
            "currency_code": str(row["currency_code"] or "GHS").strip().upper() or "GHS",
            "manual_rate": float(row["manual_rate"] or 0) if row["manual_rate"] is not None else None,
            "conversion_note": str(row["conversion_note"] or "").strip(),
            "report_total": float(row["report_total"] or 0),
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def delete_account_pricing_profile(user_id, job_id, file_key, account_name):
    conn = _connect()
    try:
        cursor = conn.execute(
            """
            DELETE FROM account_pricing_profiles
            WHERE user_id = ? AND job_id = ? AND file_key = ? AND account_name = ?
            """,
            (user_id, job_id, file_key, account_name),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def upsert_retrieval_run(
    job_id,
    user_id,
    status,
    last_message="",
    row_count=0,
    created_at=None,
    ended_at=None,
    payload=None,
):
    now = datetime.now(timezone.utc).isoformat()
    created = created_at or now
    payload_json = json.dumps(payload or {})

    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO retrieval_runs (job_id, user_id, status, last_message, row_count, created_at, ended_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id)
            DO UPDATE SET
                user_id=excluded.user_id,
                status=excluded.status,
                last_message=excluded.last_message,
                row_count=excluded.row_count,
                created_at=excluded.created_at,
                ended_at=excluded.ended_at,
                payload_json=excluded.payload_json
            """,
            (job_id, user_id, status, last_message, int(row_count or 0), created, ended_at, payload_json),
        )
        conn.commit()
    finally:
        conn.close()


def save_generated_file(job_id, user_id, file_key, file_name, mime_type, file_blob, created_at=None):
    ts = created_at or datetime.now(timezone.utc).isoformat()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO generated_files (job_id, user_id, file_key, file_name, mime_type, file_blob, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id, user_id, file_key)
            DO UPDATE SET
                file_name=excluded.file_name,
                mime_type=excluded.mime_type,
                file_blob=excluded.file_blob,
                created_at=excluded.created_at
            """,
            (job_id, user_id, file_key, file_name, mime_type, file_blob, ts),
        )
        conn.commit()
    finally:
        conn.close()


def list_recent_retrieval_runs_for_user(user_id, limit=200):
    conn = _connect()
    try:
        run_rows = conn.execute(
            """
            SELECT job_id, user_id, status, last_message, row_count, created_at, ended_at, payload_json
            FROM retrieval_runs
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
        file_rows = conn.execute(
            """
            SELECT job_id, file_key, file_name, created_at
            FROM generated_files
            WHERE user_id = ?
            ORDER BY id DESC
            """,
            (user_id,),
        ).fetchall()
    finally:
        conn.close()

    files_by_job = {}
    for row in file_rows:
        files_by_job.setdefault(row["job_id"], {})[row["file_key"]] = {
            "name": row["file_name"],
            "created_at": row["created_at"],
        }

    runs = []
    for row in run_rows:
        payload_raw = row["payload_json"] or "{}"
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {}
        runs.append(
            {
                "id": row["job_id"],
                "owner": row["user_id"],
                "status": row["status"],
                "last_message": row["last_message"] or "",
                "created_at": row["created_at"] or "",
                "ended_at": row["ended_at"],
                "payload": payload,
                "result": {
                    "row_count": int(row["row_count"] or 0),
                    "files": files_by_job.get(row["job_id"], {}),
                },
            }
        )
    return runs


def get_retrieval_run_for_user(user_id, job_id):
    runs = list_recent_retrieval_runs_for_user(user_id, limit=500)
    for run in runs:
        if run.get("id") == job_id:
            return run
    return None


def get_generated_file(user_id, job_id, file_key):
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT id, job_id, user_id, file_key, file_name, mime_type, file_blob, created_at
            FROM generated_files
            WHERE user_id = ? AND job_id = ? AND file_key = ?
            """,
            (user_id, job_id, file_key),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return None
    return {
        "id": row["id"],
        "job_id": row["job_id"],
        "user_id": row["user_id"],
        "file_key": row["file_key"],
        "file_name": row["file_name"],
        "mime_type": row["mime_type"],
        "file_blob": row["file_blob"],
        "created_at": row["created_at"],
    }


def delete_expired_generated_files(retention_hours, manual_upload_retention_days=180):
    retrieval_cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(retention_hours or 24)))
    upload_cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(manual_upload_retention_days or 180)))

    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT g.id, g.created_at, r.payload_json
            FROM generated_files g
            LEFT JOIN retrieval_runs r ON r.job_id = g.job_id
            """
        ).fetchall()

        to_delete_ids = []
        for row in rows:
            try:
                created_at = datetime.fromisoformat(str(row["created_at"]))
            except (TypeError, ValueError):
                to_delete_ids.append(int(row["id"]))
                continue

            payload_raw = row["payload_json"] or "{}"
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError:
                payload = {}

            source = str(payload.get("source") or "retrieval").strip().lower()
            is_pinned = bool(payload.get("pinned", False))

            if source == "manual_upload":
                if is_pinned:
                    continue
                if created_at < upload_cutoff:
                    to_delete_ids.append(int(row["id"]))
            elif created_at < retrieval_cutoff:
                to_delete_ids.append(int(row["id"]))

        for file_id in to_delete_ids:
            conn.execute("DELETE FROM generated_files WHERE id = ?", (file_id,))

        conn.commit()
    finally:
        conn.close()


def list_manual_upload_runs_for_user(user_id, limit=50):
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT r.job_id, r.created_at, r.payload_json,
                   g.file_name, g.file_key
            FROM retrieval_runs r
            LEFT JOIN generated_files g
              ON g.job_id = r.job_id
             AND g.user_id = r.user_id
            WHERE r.user_id = ?
            ORDER BY r.created_at DESC
            """,
            (user_id,),
        ).fetchall()
    finally:
        conn.close()

    grouped = {}
    for row in rows:
        payload_raw = row["payload_json"] or "{}"
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {}

        if str(payload.get("source") or "").strip().lower() != "manual_upload":
            continue

        job_id = row["job_id"]
        item = grouped.get(job_id)
        if not item:
            item = {
                "job_id": job_id,
                "created_at": row["created_at"],
                "original_name": payload.get("original_name") or "",
                "pinned": bool(payload.get("pinned", False)),
                "file_keys": set(),
                "file_names": set(),
            }
            grouped[job_id] = item

        if row["file_key"]:
            item["file_keys"].add(str(row["file_key"]))
        if row["file_name"]:
            item["file_names"].add(str(row["file_name"]))

        if not item["original_name"]:
            item["original_name"] = row["file_name"] or ""

    out = []
    for item in grouped.values():
        out.append(
            {
                "job_id": item["job_id"],
                "created_at": item["created_at"],
                "original_name": item["original_name"],
                "pinned": bool(item["pinned"]),
                "file_keys": sorted(item["file_keys"]),
                "file_names": sorted(item["file_names"]),
            }
        )

    out.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return out[: max(1, int(limit or 50))]


def set_manual_upload_pinned(user_id, job_id, pinned):
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT payload_json FROM retrieval_runs WHERE user_id = ? AND job_id = ?",
            (user_id, job_id),
        ).fetchone()
        if not row:
            return False

        payload_raw = row["payload_json"] or "{}"
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {}

        if str(payload.get("source") or "").strip().lower() != "manual_upload":
            return False

        payload["pinned"] = bool(pinned)
        conn.execute(
            "UPDATE retrieval_runs SET payload_json = ? WHERE user_id = ? AND job_id = ?",
            (json.dumps(payload), user_id, job_id),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def delete_expired_retrieval_runs(retention_days):
    """Hard delete retrieval runs older than retention_days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, int(retention_days or 60)))).isoformat()

    conn = _connect()
    try:
        # First, get all job IDs that are older than cutoff
        old_runs = conn.execute(
            "SELECT job_id FROM retrieval_runs WHERE created_at < ?",
            (cutoff,),
        ).fetchall()
        
        # Delete files for these old runs
        for run in old_runs:
            job_id = run["job_id"]
            conn.execute("DELETE FROM generated_files WHERE job_id = ?", (job_id,))
        
        # Then delete the runs themselves
        conn.execute("DELETE FROM retrieval_runs WHERE created_at < ?", (cutoff,))
        conn.commit()
    finally:
        conn.close()


def delete_generated_files_for_job(user_id, job_id):
    conn = _connect()
    try:
        cursor = conn.execute(
            "DELETE FROM generated_files WHERE user_id = ? AND job_id = ?",
            (user_id, job_id),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def delete_retrieval_run(user_id, job_id):
    """Hard delete a retrieval run record."""
    conn = _connect()
    try:
        conn.execute(
            "DELETE FROM retrieval_runs WHERE user_id = ? AND job_id = ?",
            (user_id, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def save_portal_credentials(user_id, portal_username, portal_password):
    encrypted = _FERNET.encrypt(portal_password.encode("utf-8")).decode("utf-8")
    now = datetime.now(timezone.utc).isoformat()

    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO portal_credentials (user_id, portal_username, portal_password_encrypted, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                portal_username=excluded.portal_username,
                portal_password_encrypted=excluded.portal_password_encrypted,
                updated_at=excluded.updated_at
            """,
            (user_id, portal_username, encrypted, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_portal_credentials(user_id):
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT portal_username, portal_password_encrypted, updated_at FROM portal_credentials WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return None

    try:
        password = _FERNET.decrypt(row["portal_password_encrypted"].encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise CredentialDecryptionError("Saved portal credentials could not be decrypted") from exc
    return {
        "portal_username": row["portal_username"],
        "portal_password": password,
        "updated_at": row["updated_at"],
    }


def delete_portal_credentials(user_id):
    conn = _connect()
    try:
        conn.execute("DELETE FROM portal_credentials WHERE user_id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()


def ensure_app_user(
    user_id,
    password_hash,
    role="user",
    is_active=True,
    must_change_password=False,
    password_changed_at=None,
    email=None,
):
    if not user_id or not password_hash:
        return

    now = datetime.now(timezone.utc).isoformat()
    changed_at = password_changed_at or now
    normalized_role = role if role in {"admin", "user"} else "user"
    active_value = 1 if is_active else 0
    must_change_value = 1 if must_change_password else 0
    normalized_email = (email or "").strip().lower() or None

    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO app_users (user_id, password_hash, role, is_active, failed_attempts, locked_until, must_change_password, password_changed_at, email, updated_at)
            VALUES (?, ?, ?, ?, 0, NULL, ?, ?, ?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                password_hash=excluded.password_hash,
                role=excluded.role,
                is_active=excluded.is_active,
                must_change_password=excluded.must_change_password,
                password_changed_at=excluded.password_changed_at,
                email=excluded.email,
                updated_at=excluded.updated_at
            """,
            (user_id, password_hash, normalized_role, active_value, must_change_value, changed_at, normalized_email, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_app_user_by_email(email):
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        return None

    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT user_id, password_hash, role, is_active, failed_attempts, locked_until, must_change_password, password_changed_at, email, company_name, company_address, company_phone, company_logo_path, updated_at
            FROM app_users
            WHERE lower(coalesce(email, '')) = ?
            """,
            (normalized_email,),
        ).fetchone()
    finally:
        conn.close()

    return dict(row) if row else None


def get_app_user(user_id):
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT user_id, password_hash, role, is_active, failed_attempts, locked_until, must_change_password, password_changed_at, email, company_name, company_address, company_phone, company_logo_path, updated_at
            FROM app_users
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return None

    # If row is already a dict (from _LibsqlCursor), return it
    # If it's a tuple (from sqlite3), convert it to dict
    if isinstance(row, dict):
        return row
    return row


def register_failed_login(user_id, max_attempts, lock_minutes):
    user = get_app_user(user_id)
    if not user:
        return None

    failed_attempts = int(user.get("failed_attempts") or 0) + 1
    locked_until = None
    if failed_attempts >= max_attempts:
        locked_until = (datetime.now(timezone.utc) + timedelta(minutes=lock_minutes)).isoformat()
        failed_attempts = 0

    conn = _connect()
    try:
        conn.execute(
            """
            UPDATE app_users
            SET failed_attempts = ?, locked_until = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (failed_attempts, locked_until, datetime.now(timezone.utc).isoformat(), user_id),
        )
        conn.commit()
    finally:
        conn.close()

    return get_app_user(user_id)


def clear_failed_login(user_id):
    conn = _connect()
    try:
        conn.execute(
            """
            UPDATE app_users
            SET failed_attempts = 0, locked_until = NULL, updated_at = ?
            WHERE user_id = ?
            """,
            (datetime.now(timezone.utc).isoformat(), user_id),
        )
        conn.commit()
    finally:
        conn.close()


def list_app_users():
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT user_id, role, is_active, failed_attempts, locked_until, must_change_password, password_changed_at, email, company_name, company_address, company_phone, company_logo_path, updated_at
            FROM app_users
            ORDER BY user_id ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]


def set_user_password(user_id, password_hash, must_change_password=False):
    changed_at = datetime.now(timezone.utc).isoformat()
    conn = _connect()
    try:
        conn.execute(
            """
            UPDATE app_users
            SET password_hash = ?, failed_attempts = 0, locked_until = NULL, must_change_password = ?, password_changed_at = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (password_hash, 1 if must_change_password else 0, changed_at, changed_at, user_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_user_active(user_id, is_active):
    conn = _connect()
    try:
        conn.execute(
            """
            UPDATE app_users
            SET is_active = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (1 if is_active else 0, datetime.now(timezone.utc).isoformat(), user_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_user_role(user_id, role):
    normalized_role = role if role in {"admin", "user"} else "user"
    conn = _connect()
    try:
        conn.execute(
            """
            UPDATE app_users
            SET role = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (normalized_role, datetime.now(timezone.utc).isoformat(), user_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_user_email(user_id, email):
    normalized_email = (email or "").strip().lower() or None
    conn = _connect()
    try:
        conn.execute(
            """
            UPDATE app_users
            SET email = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (normalized_email, datetime.now(timezone.utc).isoformat(), user_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_user_company_profile(user_id, company_name=None, company_address=None, company_phone=None, company_logo_path=None):
    conn = _connect()
    try:
        conn.execute(
            """
            UPDATE app_users
            SET company_name = ?, company_address = ?, company_phone = ?, company_logo_path = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (
                (company_name or "").strip() or None,
                (company_address or "").strip() or None,
                (company_phone or "").strip() or None,
                (company_logo_path or "").strip() or None,
                datetime.now(timezone.utc).isoformat(),
                user_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def delete_app_user(user_id):
    conn = _connect()
    try:
        conn.execute("DELETE FROM generated_files WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM retrieval_runs WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM portal_credentials WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM account_alias_rules WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM account_custom_names WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM account_pricing_profiles WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM account_pricing_rate_history WHERE user_id = ?", (user_id,))
        cursor = conn.execute("DELETE FROM app_users WHERE user_id = ?", (user_id,))
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def log_auth_event(user_id, event_type, outcome, source_ip=None, details=None):
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO auth_audit_log (event_time, user_id, event_type, outcome, source_ip, details)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                user_id,
                event_type,
                outcome,
                source_ip,
                details,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_recent_auth_events(limit=200):
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT id, event_time, user_id, event_type, outcome, source_ip, details
            FROM auth_audit_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]


def get_recent_auth_events_for_user(user_id, limit=200):
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT id, event_time, user_id, event_type, outcome, source_ip, details
            FROM auth_audit_log
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
    finally:
        conn.close()

    return [dict(row) for row in rows]
