#!/usr/bin/env python3
"""
Test script to diagnose Turso connection issues.
Run this locally or on Vercel to verify the connection works.
"""

import os
import sys
from datetime import datetime, timezone

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "").strip()
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "").strip()
DB_BACKEND = os.getenv("DB_BACKEND", "sqlite").strip().lower()

print("=" * 60)
print("TURSO CONNECTION DIAGNOSTIC TEST")
print("=" * 60)
print()

# Check configuration
print("[1] Checking Configuration")
print(f"    DB_BACKEND: {DB_BACKEND}")
print(f"    TURSO_DATABASE_URL: {TURSO_DATABASE_URL if TURSO_DATABASE_URL else '(NOT SET)'}")
print(f"    TURSO_AUTH_TOKEN: {'(SET)' if TURSO_AUTH_TOKEN else '(NOT SET)'}")
print()

if not TURSO_DATABASE_URL:
    print("[ERROR] TURSO_DATABASE_URL not configured. Set it in .env or Vercel env vars.")
    sys.exit(1)

if not TURSO_AUTH_TOKEN:
    print("[ERROR] TURSO_AUTH_TOKEN not configured. Set it in .env or Vercel env vars.")
    sys.exit(1)

print("[OK] Configuration looks good")
print()

# Try to import libsql_client
print("[2] Checking libsql_client import")
try:
    from libsql_client import create_client_sync
    print("[OK] libsql_client imported successfully")
except ImportError as e:
    print(f"[ERROR] Failed to import libsql_client: {e}")
    print("       Install with: pip install libsql-client")
    sys.exit(1)

print()

# Try to create a client
print("[3] Creating Turso client connection")
try:
    client = create_client_sync(url=TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
    print("[OK] Client created successfully")
except Exception as e:
    print(f"[ERROR] Failed to create client: {e}")
    print(f"        Type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Try to create a transaction
print("[4] Creating transaction")
try:
    transaction = client.transaction()
    print("[OK] Transaction created successfully")
except Exception as e:
    print(f"[ERROR] Failed to create transaction: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Try to execute a simple query
print("[5] Testing basic query execution")
try:
    result = transaction.execute("SELECT 1 as test_value")
    rows = list(result.rows)
    print(f"[OK] Query executed. Result: {rows}")
except Exception as e:
    print(f"[ERROR] Failed to execute query: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Try to create a test table
print("[6] Creating test table")
try:
    transaction.execute("""
        CREATE TABLE IF NOT EXISTS turso_connection_test (
            id INTEGER PRIMARY KEY,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    print("[OK] Test table created")
except Exception as e:
    print(f"[ERROR] Failed to create table: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Try to insert data
print("[7] Inserting test data")
try:
    now = datetime.now(timezone.utc).isoformat()
    transaction.execute(
        "INSERT INTO turso_connection_test (message, created_at) VALUES (?, ?)",
        ("Test message from diagnostic script", now)
    )
    print("[OK] Data inserted successfully")
except Exception as e:
    print(f"[ERROR] Failed to insert data: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Try to commit
print("[8] Committing transaction")
try:
    transaction.commit()
    print("[OK] Transaction committed")
except Exception as e:
    print(f"[ERROR] Failed to commit: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Try to read the data back
print("[9] Reading test data back")
try:
    transaction = client.transaction()
    result = transaction.execute("SELECT * FROM turso_connection_test ORDER BY id DESC LIMIT 1")
    rows = list(result.rows)
    if rows:
        print(f"[OK] Data retrieved: {rows[0]}")
    else:
        print("[WARN] No data found")
except Exception as e:
    print(f"[ERROR] Failed to read data: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# Clean up
print("[10] Cleaning up test table")
try:
    transaction.execute("DROP TABLE IF EXISTS turso_connection_test")
    transaction.commit()
    print("[OK] Test table dropped")
except Exception as e:
    print(f"[WARN] Failed to clean up: {e}")

print()
print("=" * 60)
print("ALL TESTS PASSED - TURSO IS WORKING!")
print("=" * 60)
print()
print("If you see this message, your Turso database is properly configured")
print("and working correctly. The issue must be elsewhere in the code.")
