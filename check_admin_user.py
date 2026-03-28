#!/usr/bin/env python3
"""
Check if admin user exists in the database
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import APP_ADMIN_USER
from credential_store import get_app_user, _connect

print("=" * 70)
print("CHECKING ADMIN USER")
print("=" * 70)

print(f"\nExpected admin username: {APP_ADMIN_USER}")

try:
    user_record = get_app_user(APP_ADMIN_USER)
    
    if user_record:
        print(f"\n✓ Admin user EXISTS in database!")
        print(f"   User ID: {user_record.get('user_id')}")
        print(f"   Role: {user_record.get('role')}")
        print(f"   Is Active: {user_record.get('is_active')}")
        print(f"   Password Hash: {user_record.get('password_hash')[:50]}...")
    else:
        print(f"\n✗ Admin user NOT FOUND in database")
        print(f"   Username '{APP_ADMIN_USER}' not in app_users table")
        
        # List all users in database
        print("\n   Checking what users exist in app_users table:")
        conn = _connect()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, role, is_active FROM app_users")
        users = cursor.fetchall()
        
        if users:
            print(f"   Found {len(users)} user(s):")
            for user in users:
                user_id = user['user_id'] if isinstance(user, dict) else user[0]
                role = user['role'] if isinstance(user, dict) else user[1]
                is_active = user['is_active'] if isinstance(user, dict) else user[2]
                print(f"     - {user_id} (role={role}, active={is_active})")
        else:
            print("   No users found in database")
        
        conn.close()

except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 70)
