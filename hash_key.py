from werkzeug.security import generate_password_hash, check_password_hash

# Generate a hash
hashed_pw = generate_password_hash("your-password")
print("Hashed password:", hashed_pw)

# Verify later
is_valid = check_password_hash(hashed_pw, "your-password")
print("Password valid?", is_valid)