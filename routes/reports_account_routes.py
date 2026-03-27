import traceback

from credential_store import (
    delete_account_alias_rule,
    delete_custom_account_name,
    list_custom_account_names_for_user,
    upsert_account_alias_rule,
    upsert_custom_account_name,
)
from services.account_matching import build_account_report, build_decision_ref_key


def build_reports_account_payload(user_id, selected_job_id, selected_file_key, get_generated_file, load_dataframe):
    """Return (payload, status_code) for account report ajax endpoint."""
    if not selected_job_id or not selected_file_key or selected_file_key not in {"csv", "xlsx"}:
        return {"error": "Invalid parameters"}, 400

    record = get_generated_file(user_id, selected_job_id, selected_file_key)
    if not record:
        return {"error": "File not found"}, 404

    try:
        df = load_dataframe(record)
        account_report = build_account_report(df, user_id=user_id)
        account_report["custom_account_names"] = list_custom_account_names_for_user(user_id)
        return account_report, 200
    except Exception as exc:
        return {
            "error": f"Error: {exc}",
            "traceback": traceback.format_exc(),
        }, 500


def build_reports_account_custom_add_payload(user_id, account_name):
    if not account_name:
        return {"error": "Missing account name"}, 400

    upsert_custom_account_name(user_id, account_name)
    names = list_custom_account_names_for_user(user_id)
    return {"success": True, "custom_account_names": names}, 200


def build_reports_account_custom_remove_payload(user_id, account_name):
    if not account_name:
        return {"error": "Missing account name"}, 400

    removed = delete_custom_account_name(user_id, account_name)
    if not removed:
        return {"error": "Custom account not found"}, 404

    names = list_custom_account_names_for_user(user_id)
    return {"success": True, "custom_account_names": names}, 200


def build_reports_account_assign_payload(user_id, selected_job_id, selected_file_key, assign_to, get_generated_file, load_dataframe):
    if not selected_job_id or not selected_file_key or not assign_to:
        return {"error": "Missing parameters"}, 400

    record = get_generated_file(user_id, selected_job_id, selected_file_key)
    if not record:
        return {"error": "File not found"}, 404

    try:
        df = load_dataframe(record)
        account_report = build_account_report(df, user_id=user_id)
        all_accounts = account_report.get("accounts", [])
        unassigned_rows = account_report.get("unassigned_rows", [])

        for acc in all_accounts:
            if acc.get("name") == assign_to:
                acc["rows"].extend(unassigned_rows)
                acc["count"] += len(unassigned_rows)
                break

        return {
            "success": True,
            "accounts": all_accounts,
            "message": f"Assigned {len(unassigned_rows)} entries to {assign_to}",
        }, 200
    except Exception as exc:
        return {"error": str(exc)}, 500


def build_reports_account_decision_payload(user_id, raw_ref, imp_code, created_by, boe, provided_key, canonical, action):
    if action not in {"accept", "separate", "unassign", "reset"}:
        return {"error": "Invalid action"}, 400
    if action not in {"unassign", "reset"} and not canonical:
        return {"error": "Missing canonical account"}, 400

    ref_key = provided_key or build_decision_ref_key(raw_ref, imp_code, created_by, boe)
    if not ref_key:
        return {"error": "Missing decision key context"}, 400

    if action == "reset":
        removed = delete_account_alias_rule(user_id, ref_key)
        return {"success": True, "message": "Decision reset", "removed": removed}, 200

    upsert_account_alias_rule(
        user_id,
        ref_key,
        "__UNASSIGNED__" if action == "unassign" else canonical.upper(),
        decision_type=action,
    )
    return {"success": True, "message": "Decision saved"}, 200


def build_reports_account_rename_payload(
    user_id,
    selected_job_id,
    selected_file_key,
    old_account,
    new_account,
    get_generated_file,
    load_dataframe,
):
    if not selected_job_id or selected_file_key not in {"csv", "xlsx"}:
        return {"error": "Invalid job or file type"}, 400
    if not old_account or not new_account:
        return {"error": "Missing old/new account name"}, 400

    record = get_generated_file(user_id, selected_job_id, selected_file_key)
    if not record:
        return {"error": "File not found"}, 404

    try:
        df = load_dataframe(record)
        account_report = build_account_report(df, user_id=user_id)
        target = None
        for account in account_report.get("accounts", []):
            if str(account.get("name", "")) == old_account:
                target = account
                break

        if not target:
            return {"error": "Account not found"}, 404

        changed = 0
        for row in target.get("rows", []):
            ref_key = build_decision_ref_key(
                row.get("user_ref_raw", ""),
                row.get("imp_code", ""),
                row.get("created_by", ""),
                row.get("boe", ""),
            )
            if not ref_key:
                continue
            upsert_account_alias_rule(
                user_id,
                ref_key,
                new_account.upper(),
                decision_type="separate",
            )
            changed += 1

        return {"success": True, "changed": changed, "message": "Account renamed"}, 200
    except Exception as exc:
        return {"error": str(exc)}, 500


def build_reports_account_delete_payload(
    user_id,
    selected_job_id,
    selected_file_key,
    account_name,
    get_generated_file,
    load_dataframe,
):
    if not selected_job_id or selected_file_key not in {"csv", "xlsx"}:
        return {"error": "Invalid job or file type"}, 400
    if not account_name:
        return {"error": "Missing account name"}, 400

    record = get_generated_file(user_id, selected_job_id, selected_file_key)
    if not record:
        return {"error": "File not found"}, 404

    try:
        df = load_dataframe(record)
        account_report = build_account_report(df, user_id=user_id)
        target = None
        for account in account_report.get("accounts", []):
            if str(account.get("name", "")) == account_name:
                target = account
                break

        if not target:
            return {"error": "Account not found"}, 404

        changed = 0
        for row in target.get("rows", []):
            ref_key = build_decision_ref_key(
                row.get("user_ref_raw", ""),
                row.get("imp_code", ""),
                row.get("created_by", ""),
                row.get("boe", ""),
            )
            if not ref_key:
                continue
            upsert_account_alias_rule(
                user_id,
                ref_key,
                "__UNASSIGNED__",
                decision_type="unassign",
            )
            changed += 1

        return {
            "success": True,
            "changed": changed,
            "message": "Account released to unassigned",
        }, 200
    except Exception as exc:
        return {"error": str(exc)}, 500
