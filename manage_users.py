import argparse

from werkzeug.security import generate_password_hash

from credential_store import (
    clear_failed_login,
    ensure_app_user,
    get_app_user,
    init_db,
    list_app_users,
    set_user_active,
    set_user_password,
    set_user_role,
)


def cmd_create(args):
    if get_app_user(args.username):
        print(f"User '{args.username}' already exists")
        return 1

    ensure_app_user(
        args.username,
        generate_password_hash(args.password),
        role=args.role,
        is_active=True,
        email=(args.email or "").strip().lower() or None,
    )
    print(f"Created user '{args.username}' with role '{args.role}'")
    return 0


def cmd_set_password(args):
    if not get_app_user(args.username):
        print(f"User '{args.username}' not found")
        return 1

    set_user_password(args.username, generate_password_hash(args.password))
    clear_failed_login(args.username)
    print(f"Password updated for '{args.username}'")
    return 0


def cmd_activate(args):
    if not get_app_user(args.username):
        print(f"User '{args.username}' not found")
        return 1

    set_user_active(args.username, True)
    print(f"User '{args.username}' activated")
    return 0


def cmd_deactivate(args):
    if not get_app_user(args.username):
        print(f"User '{args.username}' not found")
        return 1

    set_user_active(args.username, False)
    print(f"User '{args.username}' deactivated")
    return 0


def cmd_set_role(args):
    if not get_app_user(args.username):
        print(f"User '{args.username}' not found")
        return 1

    set_user_role(args.username, args.role)
    print(f"User '{args.username}' role set to '{args.role}'")
    return 0


def cmd_list(_args):
    users = list_app_users()
    if not users:
        print("No users found")
        return 0

    for user in users:
        status = "active" if user.get("is_active") else "inactive"
        must_change = "yes" if user.get("must_change_password") else "no"
        changed_at = user.get("password_changed_at") or "-"
        email = user.get("email") or "-"
        print(
            f"{user['user_id']} | role={user['role']} | {status} | email={email} | failed={user['failed_attempts']} | "
            f"must_change={must_change} | changed_at={changed_at}"
        )
    return 0


def build_parser():
    parser = argparse.ArgumentParser(description="Manage SessionSentinel app users")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="Create a new app user")
    create.add_argument("username")
    create.add_argument("password")
    create.add_argument("--role", choices=["admin", "user"], default="user")
    create.add_argument("--email", default="", help="Optional email address")
    create.set_defaults(func=cmd_create)

    set_password = sub.add_parser("set-password", help="Reset user password")
    set_password.add_argument("username")
    set_password.add_argument("password")
    set_password.set_defaults(func=cmd_set_password)

    activate = sub.add_parser("activate", help="Activate a user")
    activate.add_argument("username")
    activate.set_defaults(func=cmd_activate)

    deactivate = sub.add_parser("deactivate", help="Deactivate a user")
    deactivate.add_argument("username")
    deactivate.set_defaults(func=cmd_deactivate)

    set_role = sub.add_parser("set-role", help="Change user role")
    set_role.add_argument("username")
    set_role.add_argument("role", choices=["admin", "user"])
    set_role.set_defaults(func=cmd_set_role)

    list_users = sub.add_parser("list", help="List users")
    list_users.set_defaults(func=cmd_list)

    return parser


def main():
    init_db()
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
