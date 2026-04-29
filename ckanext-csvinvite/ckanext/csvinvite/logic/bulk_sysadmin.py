import csv
import io
import uuid

import ckan.model as model
import ckan.plugins.toolkit as toolkit
from sqlalchemy import func, case


EMAIL_HEADERS = ("email", "mail")


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _norm_lower(s: str | None) -> str:
    return _norm(s).lower()


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

def _protected_sysadmin_emails() -> set[str]:
    raw = toolkit.config.get("ckanext.csvinvite.protected_sysadmin_emails", "")
    if not raw:
        return set()
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


# ---------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------

def _parse_bulk_sysadmin_csv(text: str):
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


def _all_current_sysadmins() -> list:
    return (
        model.Session.query(model.User)
        .filter(model.User.sysadmin == True)
        .filter(model.User.state != "deleted")
        .all()
    )


# ---------------------------------------------------------------------
# PLAN
# ---------------------------------------------------------------------

def plan_bulk_sysadmin_promote(context, csv_text: str, remove_missing: bool = False):
    rows, errors = _parse_bulk_sysadmin_csv(csv_text)

    plan = {
        "csv_count": len(rows),
        "errors": [{"error": e} for e in errors],
        "to_promote": [],
        "already_sysadmin": [],
        "to_create": [],
        "not_found": [],
        "to_demote": [],
        "protected_skips": [],
        "remove_missing": remove_missing,
    }

    results_rows = []

    if errors:
        return plan, results_rows

    current_username = (context.get("user") or "").lower()
    protected_emails = _protected_sysadmin_emails()
    seen_user_ids: set[str] = set()
    csv_user_ids: set[str] = set()

    for r in rows:
        username = r["username"]
        email = r["email"]

        u = _db_find_user(username=username or None, email=email or None)

        # -------------------------------------------------- not found & no email => not_found
        if not u and not email:
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
                "reason": "no match and no email to create user",
            })
            continue

        # -------------------------------------------------- not found but has email => to_create
        if not u and email:
            plan["to_create"].append({
                "line": r["line"],
                "username": username,
                "email": email,
            })
            results_rows.append({
                "action": "to_create",
                "line": r["line"],
                "input_username": username,
                "input_email": email,
                "matched_user_id": "",
                "matched_username": "",
                "matched_email": "",
                "state": "",
                "reason": "user not found; will create as pending sysadmin",
            })
            continue

        uid = u.id
        uname = (u.name or "").lower()
        state = getattr(u, "state", "") or ""

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
        csv_user_ids.add(uid)

        # -------------------------------------------------- deleted => truly not found
        if state == "deleted":
            plan["not_found"].append({
                "line": r["line"],
                "username": u.name or "",
                "email": getattr(u, "email", "") or "",
                "state": state,
            })
            results_rows.append({
                "action": "not_found",
                "line": r["line"],
                "input_username": username,
                "input_email": email,
                "matched_user_id": uid,
                "matched_username": u.name or "",
                "matched_email": getattr(u, "email", "") or "",
                "state": state,
                "reason": "user found but is deleted",
            })
            continue

        # -------------------------------------------------- already sysadmin (active OR pending)
        if bool(getattr(u, "sysadmin", False)):
            plan["already_sysadmin"].append({
                "line": r["line"],
                "username": u.name or "",
                "email": getattr(u, "email", "") or "",
                "state": state,
            })
            results_rows.append({
                "action": "already_sysadmin",
                "line": r["line"],
                "input_username": username,
                "input_email": email,
                "matched_user_id": uid,
                "matched_username": u.name or "",
                "matched_email": getattr(u, "email", "") or "",
                "state": state,
                "reason": "already sysadmin",
            })
            continue

        # -------------------------------------------------- to promote (active OR pending)
        plan["to_promote"].append({
            "line": r["line"],
            "id": uid,
            "username": u.name or "",
            "email": getattr(u, "email", "") or "",
            "state": state,
        })
        results_rows.append({
            "action": "to_promote",
            "line": r["line"],
            "input_username": username,
            "input_email": email,
            "matched_user_id": uid,
            "matched_username": u.name or "",
            "matched_email": getattr(u, "email", "") or "",
            "state": state,
            "reason": "" if state == "active" else f"user is {state}; will still be promoted",
        })

    # -------------------------------------------------- remove_missing: demote sysadmins not in CSV
    if remove_missing:
        current_sysadmins = _all_current_sysadmins()
        for sa in current_sysadmins:
            sa_id = sa.id
            sa_name = (sa.name or "").lower()
            sa_email = (getattr(sa, "email", "") or "").lower()

            # already in CSV => keep
            if sa_id in csv_user_ids:
                continue

            # protected by config
            if sa_email and sa_email in protected_emails:
                plan["protected_skips"].append({
                    "username": sa.name or "",
                    "email": getattr(sa, "email", "") or "",
                    "reason": "protected_by_config",
                })
                results_rows.append({
                    "action": "protected_skip",
                    "line": "",
                    "input_username": "",
                    "input_email": "",
                    "matched_user_id": sa_id,
                    "matched_username": sa.name or "",
                    "matched_email": getattr(sa, "email", "") or "",
                    "state": getattr(sa, "state", "") or "",
                    "reason": "protected_by_config",
                })
                continue

            # protected: current user
            if sa_name == current_username:
                plan["protected_skips"].append({
                    "username": sa.name or "",
                    "email": getattr(sa, "email", "") or "",
                    "reason": "current_user",
                })
                results_rows.append({
                    "action": "protected_skip",
                    "line": "",
                    "input_username": "",
                    "input_email": "",
                    "matched_user_id": sa_id,
                    "matched_username": sa.name or "",
                    "matched_email": getattr(sa, "email", "") or "",
                    "state": getattr(sa, "state", "") or "",
                    "reason": "current_user",
                })
                continue

            # to demote
            plan["to_demote"].append({
                "id": sa_id,
                "username": sa.name or "",
                "email": getattr(sa, "email", "") or "",
                "state": getattr(sa, "state", "") or "",
            })
            results_rows.append({
                "action": "to_demote",
                "line": "",
                "input_username": "",
                "input_email": "",
                "matched_user_id": sa_id,
                "matched_username": sa.name or "",
                "matched_email": getattr(sa, "email", "") or "",
                "state": getattr(sa, "state", "") or "",
                "reason": "not in CSV; will be demoted from sysadmin",
            })

    return plan, results_rows


# ---------------------------------------------------------------------
# APPLY
# ---------------------------------------------------------------------

def apply_bulk_sysadmin_promote(context, plan: dict, results_rows: list[dict]):
    promoted = 0
    created = 0
    demoted = 0
    apply_errors = 0

    # --- Promote existing users
    target_promote_ids = {x["id"] for x in plan.get("to_promote", [])}
    for rr in results_rows:
        if rr.get("action") != "to_promote":
            continue
        uid = rr.get("matched_user_id")
        if uid not in target_promote_ids:
            continue
        try:
            u = model.Session.query(model.User).get(uid)
            if u:
                u.sysadmin = True
                model.Session.commit()
                rr["action"] = "promoted"
                promoted += 1
            else:
                rr["action"] = "error"
                rr["reason"] = "user not found during apply"
                apply_errors += 1
        except Exception as e:
            model.Session.rollback()
            rr["action"] = "error"
            rr["reason"] = str(e)
            apply_errors += 1

    # --- Create new pending sysadmin users
    for rr in results_rows:
        if rr.get("action") != "to_create":
            continue
        email = rr.get("input_email", "")
        if not email:
            rr["action"] = "error"
            rr["reason"] = "no email to create user"
            apply_errors += 1
            continue
        try:
            new_user = model.User(
                id=str(uuid.uuid4()),
                name=email,
                email=email,
                sysadmin=True,
                state="pending",
            )
            model.Session.add(new_user)
            model.Session.commit()
            rr["action"] = "created"
            rr["matched_user_id"] = new_user.id
            rr["matched_username"] = new_user.name
            rr["matched_email"] = new_user.email
            rr["state"] = "pending"
            created += 1
        except Exception as e:
            model.Session.rollback()
            rr["action"] = "error"
            rr["reason"] = str(e)
            apply_errors += 1

    # --- Demote sysadmins not in CSV
    target_demote_ids = {x["id"] for x in plan.get("to_demote", [])}
    for rr in results_rows:
        if rr.get("action") != "to_demote":
            continue
        uid = rr.get("matched_user_id")
        if uid not in target_demote_ids:
            continue
        try:
            u = model.Session.query(model.User).get(uid)
            if u:
                u.sysadmin = False
                model.Session.commit()
                rr["action"] = "demoted"
                demoted += 1
            else:
                rr["action"] = "error"
                rr["reason"] = "user not found during apply"
                apply_errors += 1
        except Exception as e:
            model.Session.rollback()
            rr["action"] = "error"
            rr["reason"] = str(e)
            apply_errors += 1

    plan2 = dict(plan)
    plan2["applied"] = {
        "promoted": promoted,
        "created": created,
        "demoted": demoted,
        "errors": apply_errors,
    }
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
