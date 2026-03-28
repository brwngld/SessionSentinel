#!/usr/bin/env python3
"""
Standalone Turso Connection Test
Place your URL and token directly in this file and run it.
"""

# ============================================================================
# EDIT THESE WITH YOUR TURSO CREDENTIALS
# ============================================================================
TURSO_DATABASE_URL = "libsql://sessionsentiel-brwngld.aws-us-east-1.turso.io"
TURSO_AUTH_TOKEN ="eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NzQ2NTY3ODcsImlkIjoiMDE5ZDMxOTctZWQwMS03N2RkLWE3NDUtZDQwNjhiZjAwNTRlIiwicmlkIjoiMzY0ODEzODgtN2RkOS00YzM4LTk5ODUtMTNkMmEwYzZiMjFjIn0._ZoH-DPZuXKIuFPFkSy25pCV7eoJ2ieywhs88qwB42sYKQIaCHfJh_D2SLp7hRbJ5k7YppEfF5aE6G1XrWvLAg"
# ============================================================================

import sys

print("=" * 70)
print("TURSO CONNECTION TEST")
print("=" * 70)
print()

# Validate inputs
if not TURSO_DATABASE_URL or "YOUR_" in TURSO_DATABASE_URL:
    print("[ERROR] TURSO_DATABASE_URL not set. Edit the file and add your URL.")
    sys.exit(1)

if not TURSO_AUTH_TOKEN or "YOUR_" in TURSO_AUTH_TOKEN:
    print("[ERROR] TURSO_AUTH_TOKEN not set. Edit the file and add your token.")
    sys.exit(1)

print(f"Database URL: {TURSO_DATABASE_URL}")
print(f"Auth Token: {TURSO_AUTH_TOKEN[:20]}..." if len(TURSO_AUTH_TOKEN) > 20 else f"Auth Token: {TURSO_AUTH_TOKEN}")
print()

# Step 1: Import
print("[Step 1] Importing libsql_client...")
try:
    from libsql_client import create_client_sync
    print("         SUCCESS - Library imported")
except ImportError as e:
    print(f"         FAILED - {e}")
    print("         Install with: pip install libsql-client")
    sys.exit(1)

print()

# Step 2: Create client
print("[Step 2] Creating client connection...")
try:
    client = create_client_sync(url=TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
    print("         SUCCESS - Client created")
except Exception as e:
    print(f"         FAILED - {type(e).__name__}: {e}")
    sys.exit(1)

print()

# Step 3: Create transaction
print("[Step 3] Creating transaction...")
try:
    transaction = client.transaction()
    print("         SUCCESS - Transaction created")
except Exception as e:
    print(f"         FAILED - {type(e).__name__}: {e}")
    sys.exit(1)

print()

# Step 4: Test query
print("[Step 4] Testing simple query (SELECT 1)...")
try:
    result = transaction.execute("SELECT 1 as test")
    rows = list(result.rows)
    print(f"         SUCCESS - Query returned: {rows}")
except Exception as e:
    print(f"         FAILED - {type(e).__name__}: {e}")
    sys.exit(1)

print()

# Step 5: Create test table
print("[Step 5] Creating test table...")
try:
    transaction.execute("""
        CREATE TABLE IF NOT EXISTS connection_test (
            id INTEGER PRIMARY KEY,
            test_message TEXT,
            created_at TEXT
        )
    """)
    print("         SUCCESS - Table created")
except Exception as e:
    print(f"         FAILED - {type(e).__name__}: {e}")
    sys.exit(1)

print()

# Step 6: Insert data
print("[Step 6] Inserting test data...")
try:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    transaction.execute(
        "INSERT INTO connection_test (test_message, created_at) VALUES (?, ?)",
        ("Test from standalone script", now)
    )
    print("         SUCCESS - Data inserted")
except Exception as e:
    print(f"         FAILED - {type(e).__name__}: {e}")
    sys.exit(1)

print()

# Step 7: Commit
print("[Step 7] Committing transaction...")
try:
    transaction.commit()
    print("         SUCCESS - Transaction committed")
except Exception as e:
    print(f"         FAILED - {type(e).__name__}: {e}")
    sys.exit(1)

print()

# Step 8: Read data back
print("[Step 8] Reading data back...")
try:
    transaction = client.transaction()
    result = transaction.execute("SELECT * FROM connection_test ORDER BY id DESC LIMIT 1")
    rows = list(result.rows)
    if rows:
        print(f"         SUCCESS - Data retrieved: {dict(rows[0])}")
    else:
        print("         WARNING - No data found")
except Exception as e:
    print(f"         FAILED - {type(e).__name__}: {e}")
    sys.exit(1)

print()

# Step 9: Cleanup
print("[Step 9] Cleaning up test table...")
try:
    transaction = client.transaction()
    transaction.execute("DROP TABLE IF EXISTS connection_test")
    transaction.commit()
    print("         SUCCESS - Table dropped")
except Exception as e:
    print(f"         WARNING - {type(e).__name__}: {e}")

print()
print("=" * 70)
print("ALL TESTS PASSED!")
print("=" * 70)
print()
print("Your Turso database is working correctly.")
print("The issue must be in the SessionSentinel configuration.")
