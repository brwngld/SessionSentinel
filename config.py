import datetime
import os

from dotenv import load_dotenv


load_dotenv()


def _env_bool(name, default=False):
	value = os.getenv(name)
	if value is None:
		return default
	return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name, default):
	value = os.getenv(name)
	if value is None:
		return default
	try:
		return int(value)
	except ValueError:
		return default


def _looks_placeholder(value):
	if value is None:
		return True
	cleaned = value.strip().lower()
	if not cleaned:
		return True
	placeholder_markers = ("replace", "change-this", "change-me", "your-")
	return any(marker in cleaned for marker in placeholder_markers)


UNIPASS_URL = os.getenv("UNIPASS_URL", "https://external.unipassghana.com/login/login.do")
UNIPASS_USER = os.getenv("UNIPASS_USER", "")
UNIPASS_PASSWORD = os.getenv("UNIPASS_PASSWORD", "")

MAX_RETRIES = _env_int("MAX_RETRIES", 2)
DEFAULT_PAGE_SIZE = _env_int("DEFAULT_PAGE_SIZE", 30)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "reports")
EDGE_DRIVER_PATH = os.getenv("EDGE_DRIVER_PATH", "C:/WebDriver/msedgedriver.exe")

today = datetime.datetime.now().strftime("%d/%m/%Y")
DEFAULT_START_DATE = os.getenv("DEFAULT_START_DATE", today)
DEFAULT_END_DATE = os.getenv("DEFAULT_END_DATE", today)

APP_ADMIN_USER = os.getenv("APP_ADMIN_USER", "admin")
APP_ADMIN_PASSWORD_HASH = os.getenv("APP_ADMIN_PASSWORD_HASH", "")

FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "change-me-in-env")
SESSION_TIMEOUT_MINUTES = _env_int("SESSION_TIMEOUT_MINUTES", 60)
REMEMBER_ME_DAYS = _env_int("REMEMBER_ME_DAYS", 7)
SESSION_COOKIE_SECURE = _env_bool("SESSION_COOKIE_SECURE", False)
LOGIN_MAX_ATTEMPTS = _env_int("LOGIN_MAX_ATTEMPTS", 5)
LOGIN_LOCK_MINUTES = _env_int("LOGIN_LOCK_MINUTES", 15)
ALLOW_DEV_ADMIN_SETUP = _env_bool("ALLOW_DEV_ADMIN_SETUP", True)
FILE_RETENTION_HOURS = _env_int("FILE_RETENTION_HOURS", 12)
RUN_RETENTION_DAYS = _env_int("RUN_RETENTION_DAYS", 60)
MANUAL_UPLOAD_RETENTION_DAYS = _env_int("MANUAL_UPLOAD_RETENTION_DAYS", 180)
PASSWORD_MAX_AGE_DAYS_ADMIN = _env_int("PASSWORD_MAX_AGE_DAYS_ADMIN", 45)
PASSWORD_MAX_AGE_DAYS_USER = _env_int("PASSWORD_MAX_AGE_DAYS_USER", 90)
PASSWORD_EXPIRY_WARNING_DAYS = _env_int("PASSWORD_EXPIRY_WARNING_DAYS", 7)

CREDENTIAL_ENCRYPTION_KEY = os.getenv("CREDENTIAL_ENCRYPTION_KEY", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "app.db")
DB_BACKEND = os.getenv("DB_BACKEND", "sqlite").strip().lower()
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", os.getenv("TURSO_CONNECTION_URL", "")).strip()
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "").strip()

if DB_BACKEND not in {"sqlite", "turso"}:
	raise RuntimeError("Invalid DB_BACKEND. Use 'sqlite' or 'turso'.")

if DB_BACKEND == "turso":
	if not TURSO_DATABASE_URL:
		raise RuntimeError("DB_BACKEND=turso requires TURSO_DATABASE_URL (or TURSO_CONNECTION_URL).")
	if not TURSO_AUTH_TOKEN:
		raise RuntimeError("DB_BACKEND=turso requires TURSO_AUTH_TOKEN.")
	if _looks_placeholder(FLASK_SECRET_KEY):
		raise RuntimeError("DB_BACKEND=turso requires a non-placeholder FLASK_SECRET_KEY.")
	if _looks_placeholder(APP_ADMIN_PASSWORD_HASH):
		raise RuntimeError("DB_BACKEND=turso requires a valid APP_ADMIN_PASSWORD_HASH.")
	if not SESSION_COOKIE_SECURE:
		raise RuntimeError("DB_BACKEND=turso requires SESSION_COOKIE_SECURE=true.")

screenshot_dir = os.getenv("SCREENSHOT_DIR", "screenshots")
os.makedirs(screenshot_dir, exist_ok=True)