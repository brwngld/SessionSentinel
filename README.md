# SessionSentinel

SessionSentinel is a Flask-based customs workflow assistant for retrieval runs, reporting, account grouping, and export generation.

## What This App Does

- Runs retrieval jobs and stores generated files (CSV/XLSX).
- Shows a dashboard with status, saved searches, and run history.
- Builds financial and account-focused reports.
- Groups account names with rule-based matching plus user feedback learning.
- Supports manual correction workflows:
  - Accept / Separate / Move
  - Unassign
  - Reset decision
  - Rename account
- Exports account reports to PDF with weekly grouping by submission date.

## Tech Stack

- Python
- Flask
- SQLite
- Pandas
- FPDF
- Vanilla JS + HTML/CSS

## Local Setup

### 1) Create/activate environment

```powershell
python -m venv myenv
.\myenv\Scripts\Activate.ps1
```

### 2) Install dependencies

```powershell
pip install -r requirements.txt
```

### 3) Configure environment variables

Copy and edit:

```powershell
Copy-Item .env.example .env
```

Then update important values in `.env`:

- `FLASK_SECRET_KEY`
- `CREDENTIAL_ENCRYPTION_KEY`
- `APP_ADMIN_PASSWORD_HASH`
- Any portal credentials/URLs needed in your environment

Set database backend explicitly:

- Local (default): `DB_BACKEND=sqlite` and `DATABASE_PATH=app.db`
- Turso (production): `DB_BACKEND=turso` with `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`

On Vercel, the deployment filesystem is read-only except `/tmp`. If you store runtime screenshots, set `SCREENSHOT_DIR=/tmp/screenshots`.

The app now fails fast at startup if `DB_BACKEND` is invalid or if Turso credentials are missing while `DB_BACKEND=turso`.
In Turso mode it also fails fast when `FLASK_SECRET_KEY` or `APP_ADMIN_PASSWORD_HASH` are placeholders, or if `SESSION_COOKIE_SECURE` is not `true`.

### 4) Run the app

```powershell
python .\flask_app.py
```

Open the URL shown in terminal (typically `http://127.0.0.1:5000`).

## Running Tests

```powershell
.\myenv\Scripts\python.exe -m pytest -q
```

## GitHub Actions ("Actions tab")

A CI workflow is included at:

- `.github/workflows/ci.yml`

It automatically runs tests on:

- Push to `main`
- Pull requests to `main`

In GitHub, open your repository and click the **Actions** tab in the top menu to see run history and logs.

## Screenshots

SessionSentinel stores runtime screenshots in:

- `screenshots/`

Current screenshots in this project are mostly troubleshooting captures (for example login/run failures), and this folder is excluded from git by default to avoid leaking sensitive information.

If you want UI screenshots to appear on GitHub:

1. Take clean screenshots without credentials or personal data.
2. Place them in a tracked folder like `docs/images/`.
3. Add Markdown image links in this README.

Example Markdown:

```markdown
![Dashboard](docs/images/dashboard.png)
![Account Report](docs/images/account-report.png)
```

## Notes

- `.env`, local DB/logs, and generated artifacts are excluded via `.gitignore`.
- If account grouping looks wrong, use the in-app review tools to correct and teach the matcher.
