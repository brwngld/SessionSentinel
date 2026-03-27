import re
from difflib import SequenceMatcher

import pandas as pd

from credential_store import get_account_alias_rules_for_user


def normalize_report_value(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "null", "nat"}:
        return ""
    if re.fullmatch(r"-?\d+\.0", text):
        return text[:-2]
    return text


def normalize_user_ref_key(value):
    text = normalize_report_value(value).upper()
    if not text:
        return ""
    tokens = re.findall(r"[A-Z0-9]+", text)
    return " ".join(tokens)


def build_decision_ref_key(user_ref_raw="", imp_code="", created_by="", boe=""):
    user_ref_key = normalize_user_ref_key(user_ref_raw)
    if user_ref_key:
        return f"USR::{user_ref_key}"

    imp_key = normalize_user_ref_key(imp_code)
    creator_key = normalize_user_ref_key(created_by)
    if imp_key or creator_key:
        return f"CTX::{imp_key}::{creator_key}"

    boe_key = normalize_user_ref_key(boe)
    if boe_key:
        return f"BOE::{boe_key}"
    return ""


def extract_account_features(user_ref):
    if not user_ref:
        return "", False

    text = str(user_ref).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL", "NAT"}:
        return "", False

    tokens = re.findall(r"[A-Z0-9]+", text)
    if not tokens:
        return "", False

    ignore_tokens = {
        "IMP", "EXP", "IMPORT", "EXPORT", "USER", "REF", "NO", "NUM", "NUMBER", "LTD", "LIMITED",
    }

    meaningful_tokens = []
    for token in tokens:
        if token.isdigit():
            continue
        if token in ignore_tokens:
            continue
        meaningful_tokens.append(token)

    if not meaningful_tokens:
        return "", False

    alpha_tokens = [t for t in meaningful_tokens if t.isalpha()]
    is_person_like = (
        len(alpha_tokens) >= 2
        and len(alpha_tokens[0]) >= 5
        and len(alpha_tokens[1]) >= 5
        and not any(ch.isdigit() for ch in text)
        and "/" not in text
        and "-" not in text
    )
    if is_person_like:
        return alpha_tokens[-1], True

    return meaningful_tokens[0], False


def _consonant_signature(value):
    return "".join(ch for ch in value if ch.isalpha() and ch not in {"A", "E", "I", "O", "U"})


def code_alias_details(left, right):
    if left == right:
        return True, "exact token", 1.0
    if len(left) < 3 or len(right) < 3:
        return False, "", 0.0
    if left.startswith(right) or right.startswith(left):
        return True, "prefix match", 0.93

    sig_left = _consonant_signature(left)
    sig_right = _consonant_signature(right)
    if len(sig_left) >= 2 and len(sig_right) >= 2:
        if sig_left.startswith(sig_right) or sig_right.startswith(sig_left):
            return True, "consonant signature", 0.84

    score = SequenceMatcher(None, left, right).ratio()
    if score >= 0.78:
        return True, f"fuzzy similarity ({score:.2f})", score
    return False, "", score


def is_code_alias(left, right):
    matched, _, _ = code_alias_details(left, right)
    return matched


def resolve_account_columns(df):
    def pick_col(candidates):
        lookup = {str(c).strip().lower(): c for c in df.columns}
        for candidate in candidates:
            col = lookup.get(str(candidate).strip().lower())
            if col is not None:
                return col
        return None

    boe_col = pick_col(["BoE No.", "BoE No", "BOE No.", "BOE No", "boe no.", "boe no"])
    user_ref_col = pick_col(["User Ref. No", "User Ref No", "User Reference", "user ref. no", "user ref no", "userref.no", "userrefno"])
    date_col = pick_col(["Submission Date", "Submitted Date", "Date"])
    imp_code_col = pick_col(["IMP. CODE/EXP. CODE", "IMP CODE/EXP CODE", "IMP CODE", "EXP CODE", "Importer Code", "Exporter Code"])
    created_by_col = pick_col(["Created By", "User", "Creator"])

    if not boe_col and len(df.columns) > 2:
        boe_col = df.columns[2]
    if not user_ref_col and len(df.columns) > 8:
        user_ref_col = df.columns[8]
    if not date_col and len(df.columns) > 9:
        date_col = df.columns[9]
    if not imp_code_col and len(df.columns) > 6:
        imp_code_col = df.columns[6]
    if not created_by_col and len(df.columns) > 11:
        created_by_col = df.columns[11]

    return boe_col, user_ref_col, date_col, imp_code_col, created_by_col


def build_account_report(df, user_id=None):
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]

    boe_col, user_ref_col, date_col, imp_code_col, created_by_col = resolve_account_columns(df)

    if not boe_col or not user_ref_col:
        return {
            "accounts": [],
            "unassigned_count": 0,
            "has_unassigned": False,
            "total_entries": 0,
            "error": f"Required columns not found. Available columns: {', '.join(df.columns[:10])}...",
        }

    work_df = pd.DataFrame(
        {
            "source_idx": df.index,
            "boe": df[boe_col].apply(normalize_report_value),
            "user_ref_raw": df[user_ref_col].apply(normalize_report_value),
            "date": df[date_col].apply(normalize_report_value) if date_col else "",
            "imp_code": df[imp_code_col].apply(normalize_report_value) if imp_code_col else "",
            "created_by": df[created_by_col].apply(normalize_report_value) if created_by_col else "",
        }
    )
    work_df = work_df[work_df["boe"] != ""].copy()
    work_df["user_ref_key"] = work_df["user_ref_raw"].apply(normalize_user_ref_key)
    work_df["decision_ref_key"] = work_df.apply(
        lambda row: build_decision_ref_key(
            row["user_ref_raw"],
            row["imp_code"],
            row["created_by"],
            row["boe"],
        ),
        axis=1,
    )

    features = work_df["user_ref_raw"].apply(extract_account_features)
    work_df["account_base"] = features.apply(lambda x: x[0])
    work_df["is_person_like"] = features.apply(lambda x: bool(x[1]))
    work_df["created_by_base"] = work_df["created_by"].apply(lambda v: extract_account_features(v)[0])

    def _candidate_tokens(value):
        text = normalize_report_value(value).upper()
        if not text:
            return []
        ignore_tokens = {
            "IMP", "EXP", "IMPORT", "EXPORT", "USER", "REF", "NO", "NUM", "NUMBER", "LTD", "LIMITED",
        }
        out = []
        for token in re.findall(r"[A-Z0-9]+", text):
            if token.isdigit() or token in ignore_tokens:
                continue
            out.append(token)
        return out

    work_df["candidate_tokens"] = work_df["user_ref_raw"].apply(_candidate_tokens)

    non_person_labels = sorted(
        {
            label
            for label in work_df.loc[~work_df["is_person_like"], "account_base"].tolist()
            if label
        }
    )
    clusters = []
    for label in non_person_labels:
        placed = False
        for cluster in clusters:
            if any(is_code_alias(label, existing) for existing in cluster):
                cluster.add(label)
                placed = True
                break
        if not placed:
            clusters.append(set([label]))

    alias_map = {}
    alias_reason_map = {}
    alias_confidence_map = {}
    for cluster in clusters:
        canonical = sorted(cluster, key=lambda x: (len(x), x))[0]
        for item in cluster:
            alias_map[item] = canonical
            _, reason, confidence = code_alias_details(item, canonical)
            alias_reason_map[item] = reason or "similar token"
            alias_confidence_map[item] = float(confidence or 0.8)

    def resolve_account(row):
        base = row["account_base"]
        if not base:
            return "", "unassigned", 0.0
        if row["is_person_like"]:
            return base, "person surname rule", 1.0
        canonical = alias_map.get(base, base)
        return canonical, alias_reason_map.get(base, "exact token"), alias_confidence_map.get(base, 1.0)

    resolved = work_df.apply(resolve_account, axis=1)
    work_df["account"] = resolved.apply(lambda x: x[0])
    work_df["account_reason"] = resolved.apply(lambda x: x[1])
    work_df["account_confidence"] = resolved.apply(lambda x: float(x[2]))

    def _apply_created_by_hint(row):
        if row["is_person_like"]:
            return row["account"], row["account_reason"], row["account_confidence"]
        cb = row["created_by_base"]
        if not cb:
            return row["account"], row["account_reason"], row["account_confidence"]
        candidates = row["candidate_tokens"] or []
        if len(candidates) >= 2 and cb in candidates:
            return cb, "created by token", 0.9
        return row["account"], row["account_reason"], row["account_confidence"]

    cb_adjusted = work_df.apply(_apply_created_by_hint, axis=1)
    work_df["account"] = cb_adjusted.apply(lambda x: x[0])
    work_df["account_reason"] = cb_adjusted.apply(lambda x: x[1])
    work_df["account_confidence"] = cb_adjusted.apply(lambda x: float(x[2]))

    imp_majority = {}
    reliable = work_df[
        (work_df["imp_code"] != "") & (work_df["account"] != "") & (work_df["account_confidence"] >= 0.9)
    ]
    if not reliable.empty:
        for imp, group in reliable.groupby("imp_code"):
            counts = group["account"].value_counts()
            top_account = counts.index[0]
            ratio = float(counts.iloc[0]) / float(counts.sum())
            if counts.iloc[0] >= 2 and ratio >= 0.6:
                imp_majority[imp] = top_account

    def _apply_imp_hint(row):
        imp = row["imp_code"]
        if not imp or imp not in imp_majority:
            return row["account"], row["account_reason"], row["account_confidence"]
        dominant = imp_majority[imp]
        candidates = row["candidate_tokens"] or []
        if row["account"] == "":
            return dominant, "imp code majority", 0.82
        if len(candidates) >= 2 and dominant in candidates:
            return dominant, "imp code consistency", 0.91
        return row["account"], row["account_reason"], row["account_confidence"]

    imp_adjusted = work_df.apply(_apply_imp_hint, axis=1)
    work_df["account"] = imp_adjusted.apply(lambda x: x[0])
    work_df["account_reason"] = imp_adjusted.apply(lambda x: x[1])
    work_df["account_confidence"] = imp_adjusted.apply(lambda x: float(x[2]))

    if user_id:
        persisted_rules = {
            row.get("ref_key", ""): row
            for row in get_account_alias_rules_for_user(user_id)
            if row.get("ref_key")
        }

        def apply_persisted(row):
            rule = persisted_rules.get(row["decision_ref_key"])
            if not rule:
                return row["account"], row["account_reason"], row["account_confidence"]
            canonical = normalize_report_value(rule.get("canonical_account"))
            decision = normalize_report_value(rule.get("decision_type")) or "accept"
            if decision == "unassign" or canonical == "__UNASSIGNED__":
                return "", "user decision (unassign)", 1.0
            if not canonical:
                return row["account"], row["account_reason"], row["account_confidence"]
            return canonical, f"user decision ({decision})", 1.0

        persisted = work_df.apply(apply_persisted, axis=1)
        work_df["account"] = persisted.apply(lambda x: x[0])
        work_df["account_reason"] = persisted.apply(lambda x: x[1])
        work_df["account_confidence"] = persisted.apply(lambda x: float(x[2]))

    assigned_df = work_df[work_df["account"] != ""].copy()
    unassigned_df = work_df[work_df["account"] == ""].copy()

    accounts = []
    for account_name in sorted(assigned_df["account"].unique()):
        account_data = assigned_df[assigned_df["account"] == account_name]
        rows_list = []
        for _, row in account_data[
            ["source_idx", "boe", "user_ref_raw", "date", "imp_code", "created_by", "account_reason", "account_confidence"]
        ].iterrows():
            rows_list.append(
                {
                    "source_idx": int(row["source_idx"]),
                    "boe": str(row["boe"]) if str(row["boe"]).strip() else None,
                    "user_ref_raw": str(row["user_ref_raw"]) if str(row["user_ref_raw"]).strip() else None,
                    "date": str(row["date"]) if str(row["date"]).strip() else None,
                    "imp_code": str(row["imp_code"]) if str(row["imp_code"]).strip() else None,
                    "created_by": str(row["created_by"]) if str(row["created_by"]).strip() else None,
                    "match_reason": str(row["account_reason"]),
                    "match_confidence": float(row["account_confidence"]),
                }
            )

        accounts.append(
            {
                "name": str(account_name),
                "count": int(len(account_data)),
                "rows": rows_list,
                "match_reasons": sorted(set(account_data["account_reason"].tolist())),
                "low_confidence_count": int((account_data["account_confidence"] < 0.86).sum()),
            }
        )

    unassigned_rows_list = []
    for _, row in unassigned_df[["source_idx", "boe", "user_ref_raw", "date", "imp_code", "created_by"]].iterrows():
        unassigned_rows_list.append(
            {
                "source_idx": int(row["source_idx"]),
                "boe": str(row["boe"]) if str(row["boe"]).strip() else None,
                "user_ref_raw": str(row["user_ref_raw"]) if str(row["user_ref_raw"]).strip() else None,
                "date": str(row["date"]) if str(row["date"]).strip() else None,
                "imp_code": str(row["imp_code"]) if str(row["imp_code"]).strip() else None,
                "created_by": str(row["created_by"]) if str(row["created_by"]).strip() else None,
            }
        )

    uncertain_df = assigned_df[(~assigned_df["is_person_like"]) & (assigned_df["account_confidence"] < 0.86)].copy()
    uncertain_matches = []
    for _, row in uncertain_df[
        ["user_ref_raw", "user_ref_key", "decision_ref_key", "account", "account_reason", "account_confidence", "boe", "date", "imp_code", "created_by"]
    ].iterrows():
        uncertain_matches.append(
            {
                "raw_user_ref": str(row["user_ref_raw"]),
                "ref_key": str(row["user_ref_key"]),
                "decision_ref_key": str(row["decision_ref_key"]),
                "suggested_account": str(row["account"]),
                "reason": str(row["account_reason"]),
                "confidence": float(row["account_confidence"]),
                "boe": str(row["boe"]),
                "date": str(row["date"]),
                "imp_code": str(row["imp_code"]),
                "created_by": str(row["created_by"]),
            }
        )

    assigned_rows_list = []
    for _, row in assigned_df[
        ["source_idx", "boe", "user_ref_raw", "date", "imp_code", "created_by", "decision_ref_key", "account", "account_reason", "account_confidence"]
    ].head(300).iterrows():
        assigned_rows_list.append(
            {
                "source_idx": int(row["source_idx"]),
                "boe": str(row["boe"]),
                "user_ref_raw": str(row["user_ref_raw"]),
                "date": str(row["date"]),
                "imp_code": str(row["imp_code"]),
                "created_by": str(row["created_by"]),
                "decision_ref_key": str(row["decision_ref_key"]),
                "current_account": str(row["account"]),
                "reason": str(row["account_reason"]),
                "confidence": float(row["account_confidence"]),
            }
        )

    return {
        "accounts": accounts,
        "unassigned_count": int(len(unassigned_df)),
        "unassigned_rows": unassigned_rows_list,
        "has_unassigned": bool(len(unassigned_df) > 0),
        "total_entries": int(len(work_df)),
        "uncertain_count": int(len(uncertain_matches)),
        "uncertain_matches": uncertain_matches,
        "assigned_rows": assigned_rows_list,
    }