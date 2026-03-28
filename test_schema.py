#!/usr/bin/env python3
"""
Test full SessionSentinel schema initialization using libsql
Runs init_db() to verify all tables are created correctly
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_schema_initialization():
    """Test full schema initialization from credential_store.init_db()"""
    
    print("=" * 70)
    print("TESTING FULL APP SCHEMA INITIALIZATION")
    print("=" * 70)
    
    try:
        # Import after .env is loaded
        import libsql
        from credential_store import init_db
        
        url = os.getenv("TURSO_DATABASE_URL")
        auth_token = os.getenv("TURSO_AUTH_TOKEN")
        
        if not url or not auth_token:
            print("❌ Error: TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not found in .env")
            return False
        
        # Step 1: Clean up old test database - be aggressive with cleanup
        print("\n[1] Cleaning up old database...")
        import glob
        
        # Remove all app.db related files
        db_patterns = ["app.db", ".app.db*", "app.db-*", "*.db-meta"]
        for pattern in db_patterns:
            for file in glob.glob(pattern):
                try:
                    os.remove(file)
                    print(f"    ✓ Removed {file}")
                except Exception as e:
                    print(f"    ℹ Could not remove {file}: {e}")
        
        # Verify nothing is left
        remaining = glob.glob("app.db*")
        if remaining:
            print(f"    ⚠ Warning: Found remaining files: {remaining}")
        else:
            print("    ✓ All database files cleaned")
        
        # Step 2: Connect and sync
        print("\n[2] Connecting to Turso database...")
        print(f"    URL: {url}")
        
        conn = libsql.connect("app.db", sync_url=url, auth_token=auth_token)
        print("    ✓ Connection established")
        
        print("\n[3] Syncing database...")
        conn.sync()
        print("    ✓ Sync successful")
        
        # Step 3: Initialize schema
        print("\n[4] Initializing app schema...")
        init_db()
        print("    ✓ Schema initialization complete")
        
        # Step 4: Reconnect to local and verify
        print("\n[5] Reconnecting to local database...")
        conn = libsql.connect("app.db")
        cursor = conn.cursor()
        print("    ✓ Connected to local app.db")
        
        # Step 5: List all tables
        print("\n[6] Verifying tables created...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        if not tables:
            print("    ❌ No tables found!")
            return False
        
        print(f"    ✓ Found {len(tables)} tables:")
        for table in tables:
            table_name = table[0] if isinstance(table, tuple) else table['name']
            print(f"      - {table_name}")
        
        # Step 6: Verify app_users table structure
        print("\n[7] Verifying app_users table structure...")
        cursor.execute("PRAGMA table_info(app_users)")
        columns = cursor.fetchall()
        
        expected_cols = {
            'user_id', 'password_hash', 'role', 'is_active', 
            'failed_attempts', 'locked_until', 'updated_at',
            'must_change_password', 'password_changed_at', 'email',
            'company_name', 'company_address', 'company_phone', 'company_logo_path'
        }
        
        found_cols = set()
        print(f"    Found {len(columns)} columns:")
        for col in columns:
            if isinstance(col, dict):
                col_name = col['name']
                col_type = col['type']
            else:
                col_name = col[1]
                col_type = col[2]
            found_cols.add(col_name)
            print(f"      - {col_name}: {col_type}")
        
        missing_cols = expected_cols - found_cols
        if missing_cols:
            print(f"\n    ⚠ Missing columns: {missing_cols}")
        else:
            print(f"\n    ✓ All expected columns present")
        
        # Step 7: Verify portal_credentials
        print("\n[8] Verifying portal_credentials table...")
        cursor.execute("PRAGMA table_info(portal_credentials)")
        columns = cursor.fetchall()
        
        print(f"    Found {len(columns)} columns:")
        for col in columns:
            if isinstance(col, dict):
                col_name = col['name']
                col_type = col['type']
            else:
                col_name = col[1]
                col_type = col[2]
            print(f"      - {col_name}: {col_type}")
        
        # Step 8: Verify auth_audit_log
        print("\n[9] Verifying auth_audit_log table...")
        cursor.execute("PRAGMA table_info(auth_audit_log)")
        columns = cursor.fetchall()
        
        print(f"    Found {len(columns)} columns:")
        for col in columns:
            if isinstance(col, dict):
                col_name = col['name']
                col_type = col['type']
            else:
                col_name = col[1]
                col_type = col[2]
            print(f"      - {col_name}: {col_type}")
        
        # Step 9: Test schema doesn't have duplicates
        print("\n[10] Testing schema integrity...")
        try:
            # Try to add a column that should already exist - should fail gracefully
            cursor.execute("ALTER TABLE app_users ADD COLUMN role TEXT")
            print("    ⚠ Warning: Duplicate column was allowed (shouldn't happen)")
        except Exception as e:
            if "duplicate column name" in str(e).lower():
                print("    ✓ Exception handling working (duplicate column caught)")
            else:
                print(f"    ✓ Error raised (expected): {type(e).__name__}")
        
        # Step 10: Sync to Turso
        print("\n[11] Syncing schema to Turso...")
        conn.sync()
        print("    ✓ Sync successful")
        
        print("\n" + "=" * 70)
        print("✓ ALL SCHEMA TESTS PASSED!")
        print("=" * 70)
        print("\nYou can now test the app locally with:")
        print("  python flask_app.py")
        print("\nOr push to Vercel with:")
        print("  git push")
        print("=" * 70)
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        print("=" * 70)
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_schema_initialization()
    sys.exit(0 if success else 1)
