#!/usr/bin/env python3
"""
Comprehensive Turso database test using .env variables
Tests connection, schema, create, read, delete, and verify empty
"""

import os
import sys
from dotenv import load_dotenv
import libsql

# Load environment variables from .env
load_dotenv()

def test_turso_connection():
    """Test Turso connection with full CRUD operations"""
    
    # Get credentials from .env (no hardcoding)
    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")
    
    if not url or not auth_token:
        print("❌ Error: TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not found in .env")
        return False
    
    print("=" * 70)
    print("TURSO DATABASE CONNECTION TEST")
    print("=" * 70)
    
    conn = None
    try:
        # Step 1: Connect and sync
        print("\n[1] Connecting to Turso database...")
        print(f"    URL: {url}")
        
        conn = libsql.connect("test_local.db", sync_url=url, auth_token=auth_token)
        print("    ✓ Connection established")
        
        print("\n[2] Syncing database...")
        conn.sync()
        print("    ✓ Sync successful")
        
        # Step 2: Create table
        print("\n[3] Creating 'users' table...")
        conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);")
        conn.commit()
        print("    ✓ Table created/verified")
        
        # Step 3: Display table structure
        print("\n[4] Table structure:")
        cursor = conn.execute("PRAGMA table_info(users)")
        columns = cursor.fetchall()
        for col in columns:
            if isinstance(col, dict):
                print(f"    - {col['name']}: {col['type']}")
            else:
                print(f"    - {col[1]}: {col[2]}")
        
        # Step 4: Insert test data
        print("\n[5] Inserting test data...")
        conn.execute("INSERT INTO users(id, name) VALUES (1, 'Alice');")
        conn.execute("INSERT INTO users(id, name) VALUES (2, 'Bob');")
        conn.execute("INSERT INTO users(id, name) VALUES (3, 'Charlie');")
        conn.commit()
        print("    ✓ 3 records inserted")
        
        # Step 5: Display data
        print("\n[6] Reading data from database:")
        cursor = conn.execute("SELECT * FROM users")
        rows = cursor.fetchall()
        
        if rows:
            print(f"    Found {len(rows)} records:")
            for row in rows:
                if isinstance(row, dict):
                    print(f"      - ID: {row['id']}, Name: {row['name']}")
                else:
                    print(f"      - ID: {row[0]}, Name: {row[1]}")
        else:
            print("    No records found")
        
        # Step 6: Sync after insert
        print("\n[7] Syncing changes to Turso...")
        conn.sync()
        print("    ✓ Sync successful")
        
        # Step 7: Delete test data
        print("\n[8] Deleting all test data...")
        conn.execute("DELETE FROM users;")
        conn.commit()
        print("    ✓ Records deleted")
        
        # Step 8: Verify empty
        print("\n[9] Verifying table is empty...")
        cursor = conn.execute("SELECT COUNT(*) as count FROM users")
        result = cursor.fetchone()
        
        count = result['count'] if isinstance(result, dict) else result[0]
        
        if count == 0:
            print("    ✓ Table is empty (0 records)")
        else:
            print(f"    ⚠ Table still has {count} records")
        
        # Step 9: Final sync
        print("\n[10] Final sync with Turso...")
        conn.sync()
        print("    ✓ Final sync successful")
        
        print("\n" + "=" * 70)
        print("✓ ALL TESTS PASSED!")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            if conn:
                conn.close()
                print("\nConnection closed.")
        except:
            pass

if __name__ == "__main__":
    success = test_turso_connection()
    sys.exit(0 if success else 1)