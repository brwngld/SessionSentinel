import argparse
import os
import sqlite3

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_db_path():
    database_path = os.getenv("DATABASE_PATH", "app.db")
    if os.path.isabs(database_path):
        return database_path
    return os.path.join(PROJECT_ROOT, database_path)


def _row_count(conn, query):
    row = conn.execute(query).fetchone()
    return int(row[0] if row else 0)


def run_backfill(dry_run=False):
    db_path = _resolve_db_path()
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        before_profiles_missing = _row_count(
            conn,
            """
            SELECT COUNT(*)
            FROM account_pricing_profiles
            WHERE currency_code IS NULL
               OR TRIM(currency_code) = ''
               OR UPPER(TRIM(currency_code)) NOT IN ('GHS', 'USD')
            """,
        )
        before_history_missing = _row_count(
            conn,
            """
            SELECT COUNT(*)
            FROM account_pricing_rate_history
            WHERE currency_code IS NULL
               OR TRIM(currency_code) = ''
               OR UPPER(TRIM(currency_code)) NOT IN ('GHS', 'USD')
            """,
        )

        if dry_run:
            print(f"[DRY RUN] account_pricing_profiles rows needing currency backfill: {before_profiles_missing}")
            print(f"[DRY RUN] account_pricing_rate_history rows needing currency backfill: {before_history_missing}")
            return

        conn.execute(
            """
            UPDATE account_pricing_profiles
            SET currency_code = 'GHS'
            WHERE currency_code IS NULL
               OR TRIM(currency_code) = ''
               OR UPPER(TRIM(currency_code)) NOT IN ('GHS', 'USD')
            """
        )
        conn.execute(
            """
            UPDATE account_pricing_profiles
            SET conversion_note = ''
            WHERE conversion_note IS NULL
            """
        )

        conn.execute(
            """
            UPDATE account_pricing_rate_history
            SET currency_code = 'GHS'
            WHERE currency_code IS NULL
               OR TRIM(currency_code) = ''
               OR UPPER(TRIM(currency_code)) NOT IN ('GHS', 'USD')
            """
        )
        conn.execute(
            """
            UPDATE account_pricing_rate_history
            SET conversion_note = ''
            WHERE conversion_note IS NULL
            """
        )

        conn.commit()

        after_profiles_missing = _row_count(
            conn,
            """
            SELECT COUNT(*)
            FROM account_pricing_profiles
            WHERE currency_code IS NULL
               OR TRIM(currency_code) = ''
               OR UPPER(TRIM(currency_code)) NOT IN ('GHS', 'USD')
            """,
        )
        after_history_missing = _row_count(
            conn,
            """
            SELECT COUNT(*)
            FROM account_pricing_rate_history
            WHERE currency_code IS NULL
               OR TRIM(currency_code) = ''
               OR UPPER(TRIM(currency_code)) NOT IN ('GHS', 'USD')
            """,
        )

        print(f"Updated account_pricing_profiles rows: {before_profiles_missing - after_profiles_missing}")
        print(f"Updated account_pricing_rate_history rows: {before_history_missing - after_history_missing}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill pricing currency defaults to GHS for legacy rows.")
    parser.add_argument("--dry-run", action="store_true", help="Report how many rows would be updated without applying changes.")
    args = parser.parse_args()
    run_backfill(dry_run=args.dry_run)
