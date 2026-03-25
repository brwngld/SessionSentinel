import argparse
import os
import shutil
from datetime import datetime

from credential_store import DB_PATH, init_db


def backup_db(output_path=None):
    init_db()
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    target = output_path
    if not target:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = os.path.join(os.path.dirname(DB_PATH) or ".", f"app_backup_{stamp}.db")

    target_dir = os.path.dirname(os.path.abspath(target))
    os.makedirs(target_dir, exist_ok=True)
    shutil.copy2(DB_PATH, target)
    print(f"Backup created: {target}")


def restore_db(backup_path):
    if not os.path.exists(backup_path):
        raise FileNotFoundError(f"Backup file not found: {backup_path}")

    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    shutil.copy2(backup_path, DB_PATH)
    print(f"Database restored to: {DB_PATH}")


def show_db_path():
    print(DB_PATH)


def main():
    parser = argparse.ArgumentParser(description="SessionSentinel SQLite backup/restore tools")
    sub = parser.add_subparsers(dest="command", required=True)

    backup = sub.add_parser("backup", help="Create a backup of the SQLite database")
    backup.add_argument("--output", help="Target backup file path")

    restore = sub.add_parser("restore", help="Restore SQLite database from backup")
    restore.add_argument("backup_file", help="Path to backup .db file")

    sub.add_parser("path", help="Print active SQLite database path")

    args = parser.parse_args()

    if args.command == "backup":
        backup_db(args.output)
    elif args.command == "restore":
        restore_db(args.backup_file)
    elif args.command == "path":
        show_db_path()


if __name__ == "__main__":
    main()
