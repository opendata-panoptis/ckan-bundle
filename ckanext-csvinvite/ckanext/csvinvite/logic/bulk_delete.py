import csv
import io

import ckan.model as model
import ckan.plugins.toolkit as toolkit
from sqlalchemy import func, case


EMAIL_HEADERS = ("email", "mail")


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _norm_lower(s: str | None) -> str:
    return _norm(s).lower()


# ---------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------

def _parse_bulk_delete_csv(text: str):
    """
    Accepts headers:
      - username (optional)
      - email (optional)
    Requires at least one of them per row.

    Returns:
      rows: [{line, username, email}]
      errors: [str]
    """
    errors: list[str] = []
    reader = csv.DictReader(io.StringIO(text))

    if not reader.fieldnames:
        return [], ["Empty CSV or missing headers."]

    header_map = {h: (h or "").strip().lower() for h in reader.fieldnames}
    fieldnames_norm = [header_map[h] for h in reader.fieldnames]

    has_username = "username" in fieldnames_norm
    has_email = any(h in fieldnames_norm for h in EMAIL_HEADERS)

    if not has_username and not has_email:
        errors.append("CSV must include 'username' and/or 'email' columns.")

    rows = []
    seen_keys = set()

    for line_no, row in enumerate(reader, start=2):
        norm = {header_map[k]: (v or "").strip() for k, v in row.items()}

        username = _norm_lower(norm.get("username"))
        email = ""
        for eh in EMAIL_HEADERS:
            if norm.get(eh):
                email = _norm_lower(norm.get(eh))
                break

        if not username and not email:
            errors.append(f"Line {line_no}: either username or email is required.")
            continue

        key = ("u", username) if username else ("e", email)
        if key in seen_keys:
            errors.append(f"Line {line_no}: duplicate entry for {key[0]}='{key[1]}'.")
            continue
        seen_keys.add(key)

        rows.append({"line": line_no, "username": username, "email": email})

    return rows, errors


# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------

def _db_find_user(username: str | None = None, email: str | None = None):
    q = model.Session.query(model.User)

    if username:
        u = q.filter(func.lower(model.User.name) == username.lower()).first()
        if u:
            return u

    if email:
        # Prefer active, then pending, then deleted, then others
        state_rank = case(
            (model.User.state == "active", 0),
            (model.User.state == "pending", 1),
            (model.User.state == "deleted", 2),
            else_=9,
        )

        u = (
            q.filter(func.lower(model.User.email) == email.lower())
            .order_by(state_rank, model.User.name.asc())
            .first()
        )
        if u:
            return u

    return None


def _sysadmin_usernames_set() -> set[str]:
    q = model.Session.query(model.User).filter(model.User.sysadmin == True)
    return set((u.name or "").lower() for u in q.all() if u and u.name)


# ---------------------------------------------------------------------
# PLAN
# ---------------------------------------------------------------------

def plan_bulk_user_delete(context, csv_text: str):
    """
    Returns:
      plan: dict (for UI)
      results_rows: list of dicts (for CSV export)
    """
    rows, errors = _parse_bulk_delete_csv(csv_text)

    plan = {
        "csv_count": len(rows),
        "errors": [{"error": e} for e in errors],
        "to_delete": [],
        "protected_skips": [],
        "not_found": [],
        "already_deleted": [],
    }

    results_rows = []

    if errors:
        return plan, results_rows

    current_username = (context.get("user") or "").lower()
    sysadmins = _sysadmin_usernames_set()
    seen_user_ids: set[str] = set()

    for r in rows:
        username = r["username"]
        email = r["email"]

        u = _db_find_user(username=username or None, email=email or None)

        # -------------------------------------------------- not found
        if not u:
            plan["not_found"].append({
                "line": r["line"],
                "username": username,
                "email": email,
            })
            results_rows.append({
                "action": "not_found",
                "line": r["line"],
                "input_username": username,
                "input_email": email,
                "matched_user_id": "",
                "matched_username": "",
                "matched_email": "",
                "state": "",
                "reason": "no match",
            })
            continue

        uid = u.id
        uname = (u.name or "").lower()
        state = getattr(u, "state", "") or ""

        # -------------------------------------------------- protected: sysadmin
        if uname in sysadmins or bool(getattr(u, "sysadmin", False)):
            plan["protected_skips"].append({
                "line": r["line"],
                "username": u.name,
                "email": getattr(u, "email", "") or "",
                "reason": "sysadmin",
            })
            results_rows.append({
                "action": "protected_skip",
                "line": r["line"],
                "input_username": username,
                "input_email": email,
                "matched_user_id": uid,
                "matched_username": u.name or "",
                "matched_email": getattr(u, "email", "") or "",
                "state": state,
                "reason": "sysadmin",
            })
            continue

        # -------------------------------------------------- protected: current user
        if current_username and uname == current_username:
            plan["protected_skips"].append({
                "line": r["line"],
                "username": u.name,
                "email": getattr(u, "email", "") or "",
                "reason": "current_user",
            })
            results_rows.append({
                "action": "protected_skip",
                "line": r["line"],
                "input_username": username,
                "input_email": email,
                "matched_user_id": uid,
                "matched_username": u.name or "",
                "matched_email": getattr(u, "email", "") or "",
                "state": state,
                "reason": "current_user",
            })
            continue

        # -------------------------------------------------- duplicate resolved user
        if uid in seen_user_ids:
            results_rows.append({
                "action": "skip_duplicate",
                "line": r["line"],
                "input_username": username,
                "input_email": email,
                "matched_user_id": uid,
                "matched_username": u.name or "",
                "matched_email": getattr(u, "email", "") or "",
                "state": state,
                "reason": "duplicate_resolved_user",
            })
            continue
        seen_user_ids.add(uid)

        # -------------------------------------------------- already deleted
        if state == "deleted":
            plan["already_deleted"].append({
                "line": r["line"],
                "username": u.name,
                "email": getattr(u, "email", "") or "",
            })
            results_rows.append({
                "action": "already_deleted",
                "line": r["line"],
                "input_username": username,
                "input_email": email,
                "matched_user_id": uid,
                "matched_username": u.name or "",
                "matched_email": getattr(u, "email", "") or "",
                "state": state,
                "reason": "state=deleted",
            })
            continue

        # -------------------------------------------------- would delete
        plan["to_delete"].append({
            "line": r["line"],
            "id": uid,
            "username": u.name,
            "email": getattr(u, "email", "") or "",
        })
        results_rows.append({
            "action": "would_delete",
            "line": r["line"],
            "input_username": username,
            "input_email": email,
            "matched_user_id": uid,
            "matched_username": u.name or "",
            "matched_email": getattr(u, "email", "") or "",
            "state": state,
            "reason": "",
        })

    return plan, results_rows


# ---------------------------------------------------------------------
# APPLY
# ---------------------------------------------------------------------

def apply_bulk_user_delete(context, plan: dict, results_rows: list[dict]):
    target_ids = {x["id"] for x in plan.get("to_delete", [])}

    deleted = 0
    errors = 0

    for rr in results_rows:
        if rr.get("action") != "would_delete":
            continue

        uid = rr.get("matched_user_id")
        if uid not in target_ids:
            continue

        try:
            toolkit.get_action("user_delete")(toolkit.fresh_context(context), {"id": uid})
            rr["action"] = "deleted"
            deleted += 1

        except Exception as e:
            rr["action"] = "error"
            rr["reason"] = str(e)
            errors += 1

    plan2 = dict(plan)
    plan2["applied"] = {"deleted": deleted, "errors": errors}
    return plan2, results_rows


# ---------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------

def results_to_csv(results_rows: list[dict]) -> str:
    output = io.StringIO()
    output.write("\N{BOM}")

    fieldnames = [
        "action",
        "line",
        "input_username",
        "input_email",
        "matched_user_id",
        "matched_username",
        "matched_email",
        "state",
        "reason",
    ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for r in results_rows or []:
        writer.writerow({k: r.get(k, "") for k in fieldnames})

    return output.getvalue()