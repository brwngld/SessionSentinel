#!/usr/bin/env python3
"""
Generate a new password hash for the admin user
"""

from werkzeug.security import generate_password_hash

# Change this to your desired password
PASSWORD = "your-password"

# Generate the hash
hash_result = generate_password_hash(PASSWORD)

print("=" * 70)
print("PASSWORD HASH GENERATOR")
print("=" * 70)
print(f"\nPassword: {PASSWORD}")
print(f"\nHash: {hash_result}")
print("\n" + "=" * 70)
print("Update your .env file with:")
print(f"APP_ADMIN_PASSWORD_HASH={hash_result}")
print("=" * 70)
