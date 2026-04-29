import csv
import io
import json
import re
import magic
import secrets

from ckanext.csvinvite.logic.bulk_delete import (
    plan_bulk_user_delete,
    apply_bulk_user_delete,
    results_to_csv,
)
from ckanext.csvinvite.logic.bulk_sysadmin import (
    plan_bulk_sysadmin_promote,
    apply_bulk_sysadmin_promote,
    results_to_csv as sysadmin_results_to_csv,
)

from ckan.lib.redis import connect_to_redis, is_redis_available
import ckan.plugins.toolkit as toolkit
import ckan.model as model
from flask import Blueprint, request, render_template, Response, session
from sqlalchemy import func

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# ----------------------------------------------------------------------------------------------------------------------------------

CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX = "csvinvite:bulk_org_sync:"
CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX = "csvinvite:bulk_org_invite:"
CSVINVITE_BULK_USER_DELETE_CACHE_PREFIX = "csvinvite:bulk_user_delete:"
CSVINVITE_BULK_SYSADMIN_CACHE_PREFIX = "csvinvite:bulk_sysadmin:"
CSVINVITE_SYNC_MEMBERS_CACHE_PREFIX = "csvinvite:sync_members:"
CSVINVITE_IMPORT_MEMBERS_CACHE_PREFIX = "csvinvite:import_members:"
CSVINVITE_BULK_ORG_SYNC_TTL_SECONDS = 1800  # 30 minutes


def _csvinvite_cache_set(key: str, payload: dict, ttl_seconds: int = CSVINVITE_BULK_ORG_SYNC_TTL_SECONDS) -> None:
    if not is_redis_available():
        raise RuntimeError("Redis is not available")
    r = connect_to_redis()
    r.setex(key, ttl_seconds, json.dumps(payload, ensure_ascii=False, default=str))


def _csvinvite_cache_get(key: str) -> dict | None:
    if not is_redis_available():
        return None
    r = connect_to_redis()
    raw = r.get(key)
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    return json.loads(raw)


def _csvinvite_cache_delete(key: str) -> None:
    if not is_redis_available():
        return
    r = connect_to_redis()
    r.delete(key)


def _csvinvite_new_token() -> str:
    return secrets.token_urlsafe(16)


# ----------------------------------------------------------------------------------------------------------------------------------

def _is_sysadmin(context):
    uobj = context.get("auth_user_obj")
    return bool(getattr(uobj, "sysadmin", False))


def _require_sysadmin(context):
    if not _is_sysadmin(context):
        return toolkit.abort(403, toolkit._("Sysadmin only"))

# ----------------------------------------------------------------------------------------------------------------------------------

def _feature_enabled(helper_name: str) -> bool:
    """
    Evaluate a template helper like `h.csvinvite_enable_sync_process()`
    server-side from views. Fail-closed if helper is missing/broken.
    """
    try:
        helper = getattr(toolkit.h, helper_name, None)
        if not callable(helper):
            return False
        return bool(helper())
    except Exception:
        return False


def _require_invite_process_enabled():
    if not _feature_enabled("csvinvite_enable_invite_process"):
        return toolkit.abort(404)


def _require_sync_process_enabled():
    if not _feature_enabled("csvinvite_enable_sync_process"):
        return toolkit.abort(404)


def _require_bulk_user_delete_enabled():
    if not _feature_enabled("csvinvite_enable_bulk_user_delete"):
        return toolkit.abort(404)


def _require_bulk_sysadmin_promote_enabled():
    if not _feature_enabled("csvinvite_enable_bulk_sysadmin_promote"):
        return toolkit.abort(404)


def _require_bulk_org_invite_enabled():
    if not _feature_enabled("csvinvite_enable_bulk_org_invite"):
        return toolkit.abort(404)


def _require_bulk_org_sync_enabled():
    if not _feature_enabled("csvinvite_enable_bulk_org_sync"):
        return toolkit.abort(404)


def _require_user_management_tab_enabled():
    """
    Gate for the /ckan-admin/users/management landing page.
    Uses the same helper as the template navigation.
    """
    if not _feature_enabled("csvinvite_show_user_management_tab"):
        return toolkit.abort(404)

# ----------------------------------------------------------------------------------------------------------------------------------

def _fresh_action_context(context: dict) -> dict:
    """
    Always call CKAN actions with a fresh context to avoid poisoning __auth_audit
    after an exception (eg ObjectNotFound).
    """
    return toolkit.fresh_context(context)

# ----------------------------------------------------------------------------------------------------------------------------------

def _is_org_admin(context, org_id):
    # strict: must be able to manage members (or at least update org)
    for perm in ("organization_member_create", "organization_update"):
        try:
            toolkit.check_access(perm, context, {"id": org_id})
            return True
        except toolkit.NotAuthorized:
            continue
    return False


def _normalize_role(role):
    if not role:
        return "member"
    role = role.strip().lower()
    return role if role in ("member", "editor", "admin") else "member"


def _call_user_invite(context, org_id, email, role):
    """
    Uses CKAN action 'user_invite'.
    """
    try:
        user_invite = toolkit.get_action("user_invite")
    except Exception:
        raise RuntimeError(
            "Action 'user_invite' is not available. "
            "Enable/keep the invite flow extension/action you are using."
        )

    try:
        return user_invite(context, {"email": email, "group_id": org_id, "role": role, "send_email": False, })
    except toolkit.ValidationError as e:
        raise e

# ----------------------------------------------------------------------------------------------------------------------------------

def _parse_bulk_org_invite_csv(text):
    """
    Headers:
      - org (required)  -> organization name/slug
      - email (required)
      - role (optional; defaults to member)
    Returns: (rows, errors)
      rows: [{line, org, email, role}]
    """
    errors = []
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return [], ["Empty CSV or missing headers."]

    header_map = {h: (h or "").strip().lower() for h in reader.fieldnames}
    fieldnames_norm = [header_map[h] for h in reader.fieldnames]

    if "org" not in fieldnames_norm:
        errors.append("CSV must include an 'org' column (organization slug).")
    if "email" not in fieldnames_norm:
        errors.append("CSV must include an 'email' column.")

    rows = []
    seen = set()

    for line_no, row in enumerate(reader, start=2):
        norm = {header_map[k]: (v or "").strip() for k, v in row.items()}

        org = _norm(norm.get("org", ""))
        email = _norm_email(norm.get("email", ""))
        role = _normalize_role(norm.get("role", ""))

        if not org:
            errors.append(f"Line {line_no}: org is required.")
            continue

        if not email or not EMAIL_RE.match(email):
            errors.append(f"Line {line_no}: invalid email '{email}'.")
            continue

        key = (org.lower(), email)
        if key in seen:
            errors.append(f"Line {line_no}: duplicate entry for org='{org}' email='{email}'.")
            continue
        seen.add(key)

        rows.append({"line": line_no, "org": org, "email": email, "role": role})

    return rows, errors


def _read_uploaded_csv_text(f):
    raw = f.read()
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1253", errors="replace")


def _validate_upload_is_csv(f, template_name: str, extra_vars: dict):
    if not f or not f.filename:
        return render_template(template_name, **extra_vars, error="Please choose a CSV file.")

    if not f.filename.lower().endswith(".csv"):
        return render_template(template_name, **extra_vars, error="Only CSV files are allowed.")

    header_bytes = f.read(2048)
    f.seek(0)

    mime = magic.from_buffer(header_bytes, mime=True)
    if not mime.startswith("text/"):
        return render_template(
            template_name,
            **extra_vars,
            error="Invalid file content. Please upload a valid CSV text file.",
        )

    return None

# ----------------------------------------------------------------------------------------------------------------------------------

def _norm(s):
    return (s or "").strip()


def _norm_email(s):
    return _norm(s).lower()


def _is_truthy(v):
    return str(v or "").lower() in ("1", "true", "yes", "on")


def _parse_sync_csv(text):
    """
    Headers:
      - username (optional)
      - email (optional, but required if username missing)
      - role (required)
    Returns: (rows, errors)
      rows: [{line, username, email, role}]
    """
    errors = []
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return [], ["Empty CSV or missing headers."]

    header_map = {h: (h or "").strip().lower() for h in reader.fieldnames}
    fieldnames_norm = [header_map[h] for h in reader.fieldnames]

    if "role" not in fieldnames_norm:
        errors.append("CSV must include a 'role' column.")
    if "username" not in fieldnames_norm and "email" not in fieldnames_norm:
        errors.append("CSV must include 'username' and/or 'email' columns.")

    rows = []
    seen_keys = set()

    for line_no, row in enumerate(reader, start=2):
        norm = {header_map[k]: (v or "").strip() for k, v in row.items()}

        username = _norm(norm.get("username", ""))
        email = _norm_email(norm.get("email", ""))
        role = _normalize_role(norm.get("role", ""))

        if not username and not email:
            errors.append(f"Line {line_no}: either username or email is required.")
            continue

        if email and not EMAIL_RE.match(email):
            errors.append(f"Line {line_no}: invalid email '{email}'.")
            continue

        # de-dup key (prefer username)
        key = ("u", username.lower()) if username else ("e", email)
        if key in seen_keys:
            errors.append(f"Line {line_no}: duplicate entry for {key[0]}='{key[1]}'.")
            continue
        seen_keys.add(key)

        rows.append({"line": line_no, "username": username, "email": email, "role": role})

    return rows, errors


CAPACITY_LABEL_MAP = {
    "διαχειριστής": "admin",
    "εκδότης": "editor",
    "συντάκτης": "editor",
    "μέλος": "member",
    "admin": "admin",
    "editor": "editor",
    "member": "member",
}

def _capacity_from_member_list_item(item):
    # item: (user_id, 'user', 'Μέλος')
    if not isinstance(item, (list, tuple)) or len(item) < 3:
        return None
    label = (item[2] or "").strip().lower()
    return CAPACITY_LABEL_MAP.get(label, label or None)


def _build_org_member_index(context, org_id):
    """
    Build existing-members index querying model.Member directly so that
    both active AND pending memberships are included (the standard
    member_list action only returns state='active').
    """
    # Resolve org_id (may be slug/name) to the actual UUID.
    group = model.Group.get(org_id)
    if not group:
        return [], {}, {}
    resolved_org_id = group.id

    member_rows = (
        model.Session.query(model.Member)
        .filter(model.Member.group_id == resolved_org_id)
        .filter(model.Member.table_name == "user")
        .filter(model.Member.state.in_(("active", "pending")))
        .all()
    )

    existing = []
    by_username = {}
    by_email = {}

    for mrow in member_rows:
        user_obj = model.User.get(mrow.table_id)
        if not user_obj:
            continue

        capacity = (mrow.capacity or "member").lower()

        md = {
            "username": user_obj.name,
            "email": _norm_email(getattr(user_obj, "email", "") or ""),
            "role": capacity,
            "sysadmin": bool(getattr(user_obj, "sysadmin", False)),
            "state": getattr(user_obj, "state", "") or "active",
        }

        existing.append(md)

        if md["username"]:
            by_username[md["username"].lower()] = md
        if md["email"]:
            by_email[md["email"]] = md

    return existing, by_username, by_email


def _db_find_user(username: str | None = None, email: str | None = None):
    """
    Find a CKAN user directly from DB by username/email (case-insensitive).
    Returns model.User or None.
    """
    q = (
        model.Session.query(model.User)
        .filter(model.User.state.in_(("active", "pending")))
    )

    if username:
        u = q.filter(func.lower(model.User.name) == username.strip().lower()).first()
        if u:
            return u

    if email:
        e = email.strip().lower()
        u = q.filter(func.lower(model.User.email) == e).first()
        if u:
            return u

    return None


def _plan_full_sync(context, org_id, existing, by_username, by_email, csv_rows):
    desired_keys = set()
    desired_role_by_key = {}
    csv_keys = []

    for r in csv_rows:
        ku = ("u", r["username"].lower()) if r["username"] else None
        ke = ("e", r["email"]) if r["email"] else None

        # κρατάμε και τα δύο ώστε να μπορεί να γίνει match από οπουδήποτε
        if ku:
            desired_keys.add(ku)
            desired_role_by_key[ku] = r["role"]
        if ke:
            desired_keys.add(ke)
            desired_role_by_key[ke] = r["role"]

        # “primary key” για να αποφασίσουμε add/missing later
        primary = ku or ke
        csv_keys.append((r, primary))

    existing_keys = set()
    removals = []
    role_updates = []
    protected_skips = []
    unchanged = []

    for m in existing:
        ku = ("u", m["username"].lower()) if m.get("username") else None
        ke = ("e", m["email"]) if m.get("email") else None

        # βάλε ΚΑΙ τα δύο ώστε το CSV email-only να σε βρίσκει ως “already member”
        if ku: existing_keys.add(ku)
        if ke: existing_keys.add(ke)

        if m.get("sysadmin"):
            protected_skips.append({"member": m, "reason": "sysadmin"})
            continue

        current_user = context.get("user")

        if m.get("username") == current_user:
            protected_skips.append({"member": m, "reason": "current_user"})
            continue

        # member is present in CSV if EITHER key is in desired
        present = (ku in desired_keys) or (ke in desired_keys)

        if not present:
            removals.append(m)
            continue

        # role preference: username match first, then email
        desired_role = None
        if ku and ku in desired_role_by_key:
            desired_role = desired_role_by_key[ku]
        elif ke and ke in desired_role_by_key:
            desired_role = desired_role_by_key[ke]

        if desired_role and (m.get("role") or "").lower() != desired_role:
            role_updates.append({"member": m, "new_role": desired_role})
        else:
            # present in CSV and not protected, and no role change -> unchanged
            unchanged.append(m)

    creations = []
    missing_users = []
    pending_invites = []

    for r, primary in csv_keys:
        # αν ο primary key δεν βρέθηκε, αλλά ο άλλος (πχ existing has username) μπορεί να βρέθηκε
        # άρα ελέγχουμε και τα δύο πιθανά keys του row
        ku = ("u", r["username"].lower()) if r["username"] else None
        ke = ("e", r["email"]) if r["email"] else None

        if (ku and ku in existing_keys) or (ke and ke in existing_keys):
            continue  # ήδη μέλος

        u_by_username = _db_find_user(username=r.get("username"), email=None) if r.get("username") else None
        u_by_email = _db_find_user(username=None, email=r.get("email")) if r.get("email") else None

        # αν και τα δύο υπάρχουν αλλά δείχνουν σε άλλους χρήστες -> conflict
        if u_by_username and u_by_email and u_by_username.id != u_by_email.id:
            missing_users.append({
                "line": r["line"],
                "username": r["username"],
                "email": r["email"],
                "role": r["role"],
                "reason": "username_email_conflict",
            })
            continue

        u = u_by_username or u_by_email

        if u is None:
            # ΔΕΝ υπάρχει user -> pending invite
            pending_invites.append({
                "line": r["line"],
                "username": r.get("username"),
                "email": r.get("email"),
                "role": r["role"],
                "reason": "user_not_found",
            })
            continue

        if bool(getattr(u, "sysadmin", False)):
            protected_skips.append({"member": {"username": u.name, "email": getattr(u, "email", ""), "role": r["role"]},
                                    "reason": "sysadmin"})
            continue

        creations.append({
            "line": r["line"],
            "username": u.name,
            "email": _norm_email(getattr(u, "email", "") or ""),
            "role": r["role"],
            "state": getattr(u, "state", "") or "active",
        })

    warnings = []
    if existing and len(removals) / float(len(existing)) >= 0.5:
        warnings.append("More than 50% of existing members are scheduled for removal. Please double-check the CSV.")

    return {
        "existing_count": len(existing),
        "csv_count": len(csv_rows),
        "removals": removals,
        "role_updates": role_updates,
        "creations": creations,
        "pending_invites": pending_invites,
        "missing_users": missing_users,
        "protected_skips": protected_skips,
        "unchanged": unchanged,
        "warnings": warnings,
    }


def _apply_full_sync(context, org_id, plan, remove_missing=True):
    removed = updated = created = 0

    if remove_missing:
        for m in plan["removals"]:
            if m.get("state") == "pending":
                # organization_member_delete only works on active members;
                # pending memberships must be removed via direct DB query.
                u = model.User.get(m["username"])
                if u:
                    model.Session.query(model.Member).filter(
                        model.Member.group_id == org_id,
                        model.Member.table_name == "user",
                        model.Member.table_id == u.id,
                        model.Member.state == "pending",
                    ).delete()
                    model.Session.commit()
                    removed += 1
            else:
                toolkit.get_action("organization_member_delete")(
                    _fresh_action_context(context),
                    {"id": org_id, "username": m["username"]}
                )
                removed += 1

    for u in plan["role_updates"]:
        toolkit.get_action("organization_member_create")(_fresh_action_context(context), {
            "id": org_id,
            "username": u["member"]["username"],
            "role": u["new_role"],
        })
        updated += 1

    for c in plan["creations"]:
        toolkit.get_action("organization_member_create")(_fresh_action_context(context), {
            "id": org_id,
            "username": c["username"],
            "role": c["role"],
        })
        created += 1

    return {"removed": removed, "updated": updated, "created": created}

# ----------------------------------------------------------------------------------------------------------------------------------

def _csv_template_response(filename: str, headers: list[str], sample_row: dict[str, str] | None = None) -> Response:
    """
    Return a CSV template (headers + optional single sample row) as an attachment.
    Adds UTF-8 BOM for Excel compatibility.
    """
    output = io.StringIO()
    output.write("\N{BOM}")

    writer = csv.DictWriter(output, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    if sample_row is not None:
        writer.writerow(sample_row)

    resp = Response(output.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


def import_members_template_csv(org_id):
    _require_invite_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to import members"))

    headers = ["email", "role"]
    sample = {"email": "user@example.org", "role": "member"}
    return _csv_template_response(f"import_members_template_{org_id}.csv", headers, sample)


def sync_members_template_csv(org_id):
    _require_sync_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to sync members"))

    headers = ["role", "username", "email"]
    sample = {"role": "member", "username": "someuser", "email": "user@example.org"}
    return _csv_template_response(f"sync_members_template_{org_id}.csv", headers, sample)


def admin_bulk_org_invite_template_csv():
    _require_bulk_org_invite_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    headers = ["org", "email", "role"]
    sample = {"org": "my-organization-slug", "email": "user@example.org", "role": "member"}
    return _csv_template_response("bulk_org_invite_template.csv", headers, sample)


def admin_bulk_org_sync_template_csv():
    _require_bulk_org_sync_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    headers = ["org", "username", "email", "role"]
    sample = {"org": "my-organization-slug", "username": "someuser", "email": "user@example.org", "role": "member"}
    return _csv_template_response("bulk_org_sync_template.csv", headers, sample)


def admin_bulk_user_delete_template_csv():
    _require_bulk_user_delete_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    headers = ["username", "email"]
    sample = {"username": "someuser", "email": "user@example.org"}
    return _csv_template_response("bulk_user_delete_template.csv", headers, sample)


# ----------------------------------------------------------------------------------------------------------------------------------

def import_members_reset(org_id):
    _require_invite_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to import members"))

    token_key = f"csvinvite_import_members_token:{org_id}"
    token = session.pop(token_key, None)
    if token:
        _csvinvite_cache_delete(f"{CSVINVITE_IMPORT_MEMBERS_CACHE_PREFIX}{token}")

    return toolkit.redirect_to("csvinvite.import_members_get", org_id=org_id)


def import_members_get(org_id):
    _require_invite_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to import members"))

    org = toolkit.get_action("organization_show")(context, {"id": org_id})
    return render_template("csvinvite/import_members.html", org=org)


def import_members_post(org_id):
    _require_invite_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to import members"))

    if not is_redis_available():
        return toolkit.abort(503, toolkit._("Service unavailable"))

    org = toolkit.get_action("organization_show")(context, {"id": org_id})

    f = request.files.get("file")
    if not f or not f.filename:
        return render_template("csvinvite/import_members.html", org=org, error="Please choose a CSV file.")

    # --------------------------------------------------------
    # Έλεγχος extension
    if not f.filename.lower().endswith('.csv'):
        return render_template("csvinvite/import_members.html", org=org, error="Only CSV files are allowed.")

    # Έλεγχος περιεχομένου (MIME type)
    header_bytes = f.read(2048)
    f.seek(0)  # Επαναφορά του κέρσορα στην αρχή για το μετέπειτα διάβασμα

    mime = magic.from_buffer(header_bytes, mime=True)
    if not mime.startswith("text/"):
        return render_template(
            "csvinvite/import_members.html",
            org=org,
            error="Invalid file content. Please upload a valid CSV text file.",
        )

    raw = f.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1253", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return render_template("csvinvite/import_members.html", org=org, error="Empty CSV or missing headers.")

    fieldnames_norm = [h.strip().lower() for h in reader.fieldnames]
    if "email" not in fieldnames_norm:
        return render_template("csvinvite/import_members.html", org=org, error="CSV must include an 'email' column.")

    header_map = {h: h.strip().lower() for h in reader.fieldnames}

    dry_run = _is_truthy(request.form.get("dry_run"))

    results = {
        "total": 0,
        "invited": 0,
        "would_invite": 0,
        "skipped_invalid": 0,
        "errors": 0,
        "rows": [],
        "dry_run": dry_run,
    }

    for line_no, row in enumerate(reader, start=2):
        results["total"] += 1
        norm = {header_map[k]: (v or "").strip() for k, v in row.items()}

        email = norm.get("email", "")
        role = _normalize_role(norm.get("role", ""))

        if not email or not EMAIL_RE.match(email):
            results["skipped_invalid"] += 1
            results["rows"].append(
                {"line": line_no, "email": email, "role": role, "status": "skip", "message": "Invalid email"}
            )
            continue

        if dry_run:
            results["would_invite"] += 1
            results["rows"].append(
                {"line": line_no, "email": email, "role": role, "status": "dry_run",
                 "message": "Would be invited (dry-run)"}
            )
            continue

        try:
            _call_user_invite(_fresh_action_context(context), org_id, email, role)
            results["invited"] += 1
            results["rows"].append(
                {"line": line_no, "email": email, "role": role, "status": "ok", "message": "Invited / pending created"}
            )
        except toolkit.ValidationError as e:
            results["errors"] += 1
            results["rows"].append(
                {"line": line_no, "email": email, "role": role, "status": "error", "message": str(e.error_dict or e)}
            )
        except Exception as e:
            results["errors"] += 1
            results["rows"].append(
                {"line": line_no, "email": email, "role": role, "status": "error", "message": str(e)}
            )

    # PRG + Redis-only: stash server-side και cookie κρατάει μόνο token
    token = _csvinvite_new_token()
    session[f"csvinvite_import_members_token:{org_id}"] = token
    _csvinvite_cache_set(
        f"{CSVINVITE_IMPORT_MEMBERS_CACHE_PREFIX}{token}",
        {"results": results},
        ttl_seconds=CSVINVITE_BULK_ORG_SYNC_TTL_SECONDS,  # 30 minutes
    )

    return toolkit.redirect_to("csvinvite.import_members_result_get", org_id=org_id)


def import_members_result_get(org_id):
    _require_invite_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized"))

    if not is_redis_available():
        return toolkit.abort(503, toolkit._("Service unavailable"))

    org = toolkit.get_action("organization_show")(context, {"id": org_id})

    token = session.get(f"csvinvite_import_members_token:{org_id}")
    payload = _csvinvite_cache_get(f"{CSVINVITE_IMPORT_MEMBERS_CACHE_PREFIX}{token}") if token else None
    results = (payload or {}).get("results")

    if not results:
        return toolkit.redirect_to("csvinvite.import_members_get", org_id=org_id)

    return render_template("csvinvite/import_members_result.html", org=org, results=results)


def export_results_csv(org_id):
    """
    Exports the results as a CSV file with authorization check.
    """
    _require_invite_process_enabled()

    context = {
        "user": toolkit.c.user,
        "auth_user_obj": toolkit.c.userobj,
        "model": model
    }

    # Authorization check: Similar to member_dump
    try:
        toolkit.check_access("organization_member_create", context, {"id": org_id})
    except toolkit.NotAuthorized:
        return toolkit.abort(403, toolkit._("Not authorized to export member results"))

    data_raw = request.form.get("results_data")
    if not data_raw:
        return toolkit.abort(400, toolkit._("No data to export"))

    try:
        results_rows = json.loads(data_raw)
    except (ValueError, TypeError):
        return toolkit.abort(400, toolkit._("Invalid data format"))

    output = io.StringIO()
    # Adding BOM for Excel compatibility as seen in member_dump
    output.write('\N{BOM}')

    writer = csv.DictWriter(output, fieldnames=["line", "email", "role", "status", "message"])
    writer.writeheader()
    writer.writerows(results_rows)

    response = Response(output.getvalue(), mimetype="text/csv")
    response.headers["Content-Disposition"] = f'attachment; filename="import_results_{org_id}.csv"'
    return response

# ----------------------------------------------------------------------------------------------------------------------------------

def sync_members_reset(org_id):
    _require_sync_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to sync members"))

    token_key = f"csvinvite_sync_members_token:{org_id}"
    token = session.pop(token_key, None)
    if token:
        _csvinvite_cache_delete(f"{CSVINVITE_SYNC_MEMBERS_CACHE_PREFIX}{token}")

    return toolkit.redirect_to("csvinvite.sync_members_get", org_id=org_id)


def sync_members_get(org_id):
    _require_sync_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to sync members"))

    org = toolkit.get_action("organization_show")(context, {"id": org_id})
    return render_template("csvinvite/sync_members.html", org=org)


def sync_members_post(org_id):
    _require_sync_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to sync members"))

    if not is_redis_available():
        return toolkit.abort(503, toolkit._("Service unavailable"))

    org = toolkit.get_action("organization_show")(context, {"id": org_id})

    f = request.files.get("file")
    if not f or not f.filename:
        return render_template("csvinvite/sync_members.html", org=org, error="Please choose a CSV file.")

    # --------------------------------------------------------
    # Έλεγχος extension
    if not f.filename.lower().endswith('.csv'):
        return render_template("csvinvite/sync_members.html", org=org, error="Only CSV files are allowed.")

    # Έλεγχος περιεχομένου (MIME type)
    header_bytes = f.read(2048)
    f.seek(0)  # Επαναφορά του κέρσορα στην αρχή για το μετέπειτα διάβασμα

    mime = magic.from_buffer(header_bytes, mime=True)
    # Τα CSV συνήθως αναγνωρίζονται ως text/plain ή text/csv
    if not mime.startswith('text/'):
        return render_template("csvinvite/sync_members.html", org=org,
                               error="Invalid file content. Please upload a valid CSV text file.")
    # --------------------------------------------------------

    raw = f.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1253", errors="replace")

    csv_rows, parse_errors = _parse_sync_csv(text)
    if parse_errors:
        return render_template("csvinvite/sync_members.html", org=org, error="; ".join(parse_errors))

    dry_run = _is_truthy(request.form.get("dry_run", ""))
    remove_missing = _is_truthy(request.form.get("remove_missing", ""))

    existing, by_username, by_email = _build_org_member_index(context, org_id)
    plan = _plan_full_sync(context, org_id, existing, by_username, by_email, csv_rows)

    applied = None
    apply_error = None

    if not dry_run:
        try:
            applied = _apply_full_sync(context, org_id, plan, remove_missing=remove_missing)
        except toolkit.NotAuthorized:
            apply_error = "Not authorized to apply changes."
        except toolkit.ValidationError as e:
            apply_error = str(e.error_dict or e)
        except Exception as e:
            apply_error = str(e)

    # Redis-only: stash server-side, cookie κρατάει μόνο token
    token = _csvinvite_new_token()
    session[f"csvinvite_sync_members_token:{org_id}"] = token
    _csvinvite_cache_set(
        f"{CSVINVITE_SYNC_MEMBERS_CACHE_PREFIX}{token}",
        {
            "plan": plan,
            "dry_run": bool(dry_run),
            "remove_missing": bool(remove_missing),
            "applied": applied,
            "apply_error": apply_error,
        },
        ttl_seconds=CSVINVITE_BULK_ORG_SYNC_TTL_SECONDS,  # 30 minutes
    )

    return toolkit.redirect_to("csvinvite.sync_members_result_get", org_id=org_id)


def sync_members_result_get(org_id):
    _require_sync_process_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj}
    if not _is_org_admin(context, org_id):
        return toolkit.abort(403, toolkit._("Unauthorized to sync members"))

    if not is_redis_available():
        return toolkit.abort(503, toolkit._("Service unavailable"))

    org = toolkit.get_action("organization_show")(context, {"id": org_id})

    token = session.get(f"csvinvite_sync_members_token:{org_id}")
    payload = _csvinvite_cache_get(f"{CSVINVITE_SYNC_MEMBERS_CACHE_PREFIX}{token}") if token else None

    if not payload or not payload.get("plan"):
        return toolkit.redirect_to("csvinvite.sync_members_get", org_id=org_id)

    return render_template(
        "csvinvite/sync_members_result.html",
        org=org,
        plan=payload.get("plan"),
        dry_run=bool(payload.get("dry_run")),
        remove_missing=bool(payload.get("remove_missing")),
        applied=payload.get("applied"),
        apply_error=payload.get("apply_error"),
    )


def export_sync_plan_csv(org_id):
    _require_sync_process_enabled()

    context = {
        "user": toolkit.c.user,
        "auth_user_obj": toolkit.c.userobj,
        "model": model
    }

    try:
        toolkit.check_access(
            "organization_member_create",
            context,
            {"id": org_id}
        )
    except toolkit.NotAuthorized:
        return toolkit.abort(403, toolkit._("Not authorized"))

    raw = request.form.get("plan_data")
    if not raw:
        return toolkit.abort(400, toolkit._("No data to export"))

    try:
        plan = json.loads(raw)
    except (ValueError, TypeError):
        return toolkit.abort(400, toolkit._("Invalid JSON"))

    rows = []

    # removals
    for m in plan.get("removals", []) or []:
        rows.append({
            "action": "remove",
            "line": "",
            "username": m.get("username", ""),
            "email": m.get("email", ""),
            "current_role": m.get("role", ""),
            "new_role": "",
            "state": m.get("state", ""),
            "reason": "",
        })

    # role updates
    for u in plan.get("role_updates", []) or []:
        member = (u or {}).get("member", {}) or {}
        rows.append({
            "action": "role_update",
            "line": "",
            "username": member.get("username", ""),
            "email": member.get("email", ""),
            "current_role": member.get("role", ""),
            "new_role": u.get("new_role", ""),
            "state": member.get("state", ""),
            "reason": "",
        })

    # unchanged (present in CSV, not protected, no role change)
    for m in plan.get("unchanged", []) or []:
        rows.append({
            "action": "unchanged",
            "line": "",
            "username": m.get("username", "") or "",
            "email": m.get("email", "") or "",
            "current_role": m.get("role", "") or "",
            "new_role": "",
            "state": m.get("state", ""),
            "reason": "",
        })

    # creations
    for c in plan.get("creations", []) or []:
        rows.append({
            "action": "add",
            "line": c.get("line", ""),
            "username": c.get("username", ""),
            "email": c.get("email", ""),
            "current_role": "",
            "new_role": c.get("role", ""),
            "state": c.get("state", ""),
            "reason": "",
        })

    # pending invites
    for p in plan.get("pending_invites", []) or []:
        rows.append({
            "action": "pending_invite",
            "line": p.get("line", ""),
            "username": p.get("username", "") or "",
            "email": p.get("email", "") or "",
            "current_role": "",
            "new_role": p.get("role", ""),
            "state": "",
            "reason": p.get("reason", "") or "",
        })

    # protected skips
    for ps in plan.get("protected_skips", []) or []:
        member = (ps or {}).get("member", {}) or {}
        rows.append({
            "action": "protected_skip",
            "line": "",
            "username": member.get("username", "") or "",
            "email": member.get("email", "") or "",
            "current_role": member.get("role", "") or "",
            "new_role": "",
            "state": member.get("state", ""),
            "reason": ps.get("reason", "") or "",
        })

    output = io.StringIO()
    output.write('\N{BOM}')

    fieldnames = [
        "action",
        "line",
        "username",
        "email",
        "current_role",
        "new_role",
        "state",
        "reason",
    ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    response = Response(output.getvalue(), mimetype="text/csv")
    response.headers["Content-Disposition"] = (
        f'attachment; filename="sync_plan_{org_id}.csv"'
    )
    return response


# ----------------------------------------------------------------------------------------------------------------------------------

def _parse_bulk_org_sync_csv(text):
    """
    Bulk multi-org sync CSV.

    Headers:
      - org (required) -> organization id OR name/slug (passed to organization_show as {"id": org})
      - username (optional)
      - email (optional, but required if username missing)
      - role (required)

    Returns: (rows, errors)
      rows: [{line, org, username, email, role}]
    """
    errors = []
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return [], ["Empty CSV or missing headers."]

    header_map = {h: (h or "").strip().lower() for h in reader.fieldnames}
    fieldnames_norm = [header_map[h] for h in reader.fieldnames]

    if "org" not in fieldnames_norm:
        errors.append("CSV must include an 'org' column (organization id or name/slug).")
    if "role" not in fieldnames_norm:
        errors.append("CSV must include a 'role' column.")
    if "username" not in fieldnames_norm and "email" not in fieldnames_norm:
        errors.append("CSV must include 'username' and/or 'email' columns.")

    rows = []
    seen = set()

    for line_no, row in enumerate(reader, start=2):
        norm = {header_map[k]: (v or "").strip() for k, v in row.items()}

        org = _norm(norm.get("org", ""))
        username = _norm(norm.get("username", ""))
        email = _norm_email(norm.get("email", ""))
        role = _normalize_role(norm.get("role", ""))

        if not org:
            errors.append(f"Line {line_no}: org is required.")
            continue

        if not username and not email:
            errors.append(f"Line {line_no}: either username or email is required.")
            continue

        if email and not EMAIL_RE.match(email):
            errors.append(f"Line {line_no}: invalid email '{email}'.")
            continue

        # de-dup inside same org (prefer username key)
        key = (org.lower(), "u", username.lower()) if username else (org.lower(), "e", email)
        if key in seen:
            errors.append(f"Line {line_no}: duplicate entry for org='{org}' {key[1]}='{key[2]}'.")
            continue
        seen.add(key)

        rows.append({"line": line_no, "org": org, "username": username, "email": email, "role": role})

    return rows, errors


def _bulk_org_sync_plan_to_export_rows(report: dict) -> list[dict]:
    """
    Flatten per-org plans into exportable rows.
    """
    out = []
    for org_block in (report or {}).get("orgs", []) or []:
        org_ref = org_block.get("org_ref", "")
        org_id = org_block.get("org_id", "")
        org_name = org_block.get("org_name", "")

        plan = org_block.get("plan") or {}
        if not plan:
            # e.g. org resolution failure block
            continue

        # removals
        for m in plan.get("removals", []) or []:
            out.append({
                "org": org_ref,
                "org_id": org_id,
                "org_name": org_name,
                "action": "remove",
                "line": "",
                "username": m.get("username", ""),
                "email": m.get("email", ""),
                "current_role": m.get("role", ""),
                "new_role": "",
                "state": m.get("state", ""),
                "reason": "",
            })

        # role updates
        for u in plan.get("role_updates", []) or []:
            member = (u or {}).get("member", {}) or {}
            out.append({
                "org": org_ref,
                "org_id": org_id,
                "org_name": org_name,
                "action": "role_update",
                "line": "",
                "username": member.get("username", ""),
                "email": member.get("email", ""),
                "current_role": member.get("role", ""),
                "new_role": u.get("new_role", ""),
                "state": member.get("state", ""),
                "reason": "",
            })

        # unchanged
        for m in plan.get("unchanged", []) or []:
            out.append({
                "org": org_ref,
                "org_id": org_id,
                "org_name": org_name,
                "action": "unchanged",
                "line": "",
                "username": m.get("username", "") or "",
                "email": m.get("email", "") or "",
                "current_role": m.get("role", "") or "",
                "new_role": "",
                "state": m.get("state", ""),
                "reason": "",
            })

        # creations
        for c in plan.get("creations", []) or []:
            out.append({
                "org": org_ref,
                "org_id": org_id,
                "org_name": org_name,
                "action": "add",
                "line": c.get("line", ""),
                "username": c.get("username", ""),
                "email": c.get("email", ""),
                "current_role": "",
                "new_role": c.get("role", ""),
                "state": c.get("state", ""),
                "reason": "",
            })

        # pending invites
        for p in plan.get("pending_invites", []) or []:
            out.append({
                "org": org_ref,
                "org_id": org_id,
                "org_name": org_name,
                "action": "pending_invite",
                "line": p.get("line", ""),
                "username": p.get("username", "") or "",
                "email": p.get("email", "") or "",
                "current_role": "",
                "new_role": p.get("role", ""),
                "state": "",
                "reason": p.get("reason", "") or "",
            })

        # protected skips
        for ps in plan.get("protected_skips", []) or []:
            member = (ps or {}).get("member", {}) or {}
            out.append({
                "org": org_ref,
                "org_id": org_id,
                "org_name": org_name,
                "action": "protected_skip",
                "line": "",
                "username": member.get("username", "") or "",
                "email": member.get("email", "") or "",
                "current_role": member.get("role", "") or "",
                "new_role": "",
                "state": member.get("state", ""),
                "reason": ps.get("reason", "") or "",
            })

    return out


# ----------------------------------------------------------------------------------------------------------------------------------

def admin_bulk_user_delete_reset():
    _require_bulk_user_delete_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.pop("csvinvite_bulk_user_delete_token", None)
    if token:
        _csvinvite_cache_delete(f"{CSVINVITE_BULK_USER_DELETE_CACHE_PREFIX}{token}")

    return toolkit.redirect_to("csvinvite.admin_bulk_user_delete_get")


def admin_bulk_user_delete_get():
    _require_bulk_user_delete_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.get("csvinvite_bulk_user_delete_token")
    payload = _csvinvite_cache_get(f"{CSVINVITE_BULK_USER_DELETE_CACHE_PREFIX}{token}") if token else None

    return render_template(
        "csvinvite/admin_bulk_user_delete.html",
        plan=(payload or {}).get("plan"),
    )


def admin_bulk_user_delete_post():
    _require_bulk_user_delete_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    if not is_redis_available():
        return render_template(
            "csvinvite/admin_bulk_user_delete.html",
            plan={"errors": [{"error": "Redis is not available; cannot store bulk delete results safely."}]},
        )

    f = request.files.get("file")

    # PRG-friendly validation: γράφουμε plan(error) στο Redis + redirect σε GET
    if not f or not getattr(f, "filename", ""):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_user_delete_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_USER_DELETE_CACHE_PREFIX}{token}",
            {"plan": {"errors": [{"error": "Please choose a CSV file."}]}, "results_rows": []},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_user_delete_get")

    if not f.filename.lower().endswith(".csv"):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_user_delete_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_USER_DELETE_CACHE_PREFIX}{token}",
            {"plan": {"errors": [{"error": "Only CSV files are allowed."}]}, "results_rows": []},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_user_delete_get")

    header_bytes = f.read(2048)
    f.seek(0)
    mime = magic.from_buffer(header_bytes, mime=True)
    if not mime.startswith("text/"):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_user_delete_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_USER_DELETE_CACHE_PREFIX}{token}",
            {"plan": {"errors": [{"error": "Invalid file content. Please upload a valid CSV text file."}]}, "results_rows": []},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_user_delete_get")

    raw = f.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1253", errors="replace")

    plan, results_rows = plan_bulk_user_delete(context, text)

    # apply αν πατήθηκε
    if _is_truthy(request.form.get("apply")):
        plan, results_rows = apply_bulk_user_delete(context, plan, results_rows)

    # PRG + Redis: stash server-side και κρατάμε μόνο token στο cookie
    token = _csvinvite_new_token()
    session["csvinvite_bulk_user_delete_token"] = token
    _csvinvite_cache_set(
        f"{CSVINVITE_BULK_USER_DELETE_CACHE_PREFIX}{token}",
        {"plan": plan, "results_rows": results_rows},
        ttl_seconds=CSVINVITE_BULK_ORG_SYNC_TTL_SECONDS,  # 30 minutes
    )

    return toolkit.redirect_to("csvinvite.admin_bulk_user_delete_get")


def admin_bulk_user_delete_export():
    _require_bulk_user_delete_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.get("csvinvite_bulk_user_delete_token")
    payload = _csvinvite_cache_get(f"{CSVINVITE_BULK_USER_DELETE_CACHE_PREFIX}{token}") if token else None
    rows = (payload or {}).get("results_rows") or []

    csv_text = results_to_csv(rows)

    response = Response(csv_text, mimetype="text/csv")
    response.headers["Content-Disposition"] = 'attachment; filename="bulk_user_delete_results.csv"'
    return response

# ----------------------------------------------------------------------------------------------------------------------------------
# Bulk sysadmin promote
# ----------------------------------------------------------------------------------------------------------------------------------


def admin_bulk_sysadmin_promote_template_csv():
    _require_bulk_sysadmin_promote_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    headers = ["username", "email"]
    sample = {"username": "someuser", "email": "user@example.org"}
    return _csv_template_response("bulk_sysadmin_promote_template.csv", headers, sample)


def admin_bulk_sysadmin_promote_reset():
    _require_bulk_sysadmin_promote_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.pop("csvinvite_bulk_sysadmin_token", None)
    if token:
        _csvinvite_cache_delete(f"{CSVINVITE_BULK_SYSADMIN_CACHE_PREFIX}{token}")

    return toolkit.redirect_to("csvinvite.admin_bulk_sysadmin_promote_get")


def admin_bulk_sysadmin_promote_get():
    _require_bulk_sysadmin_promote_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.get("csvinvite_bulk_sysadmin_token")
    payload = _csvinvite_cache_get(f"{CSVINVITE_BULK_SYSADMIN_CACHE_PREFIX}{token}") if token else None

    return render_template(
        "csvinvite/admin_bulk_sysadmin_promote.html",
        plan=(payload or {}).get("plan"),
    )


def admin_bulk_sysadmin_promote_post():
    _require_bulk_sysadmin_promote_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    if not is_redis_available():
        return render_template(
            "csvinvite/admin_bulk_sysadmin_promote.html",
            plan={"errors": [{"error": "Redis is not available; cannot store results safely."}]},
        )

    f = request.files.get("file")

    if not f or not getattr(f, "filename", ""):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_sysadmin_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_SYSADMIN_CACHE_PREFIX}{token}",
            {"plan": {"errors": [{"error": "Please choose a CSV file."}]}, "results_rows": []},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_sysadmin_promote_get")

    if not f.filename.lower().endswith(".csv"):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_sysadmin_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_SYSADMIN_CACHE_PREFIX}{token}",
            {"plan": {"errors": [{"error": "Only CSV files are allowed."}]}, "results_rows": []},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_sysadmin_promote_get")

    header_bytes = f.read(2048)
    f.seek(0)
    mime = magic.from_buffer(header_bytes, mime=True)
    if not mime.startswith("text/"):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_sysadmin_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_SYSADMIN_CACHE_PREFIX}{token}",
            {"plan": {"errors": [{"error": "Invalid file content. Please upload a valid CSV text file."}]}, "results_rows": []},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_sysadmin_promote_get")

    raw = f.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1253", errors="replace")

    remove_missing = _is_truthy(request.form.get("remove_missing"))

    plan, results_rows = plan_bulk_sysadmin_promote(context, text, remove_missing=remove_missing)

    if _is_truthy(request.form.get("apply")):
        plan, results_rows = apply_bulk_sysadmin_promote(context, plan, results_rows)

    token = _csvinvite_new_token()
    session["csvinvite_bulk_sysadmin_token"] = token
    _csvinvite_cache_set(
        f"{CSVINVITE_BULK_SYSADMIN_CACHE_PREFIX}{token}",
        {"plan": plan, "results_rows": results_rows},
        ttl_seconds=CSVINVITE_BULK_ORG_SYNC_TTL_SECONDS,
    )

    return toolkit.redirect_to("csvinvite.admin_bulk_sysadmin_promote_get")


def admin_bulk_sysadmin_promote_export():
    _require_bulk_sysadmin_promote_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.get("csvinvite_bulk_sysadmin_token")
    payload = _csvinvite_cache_get(f"{CSVINVITE_BULK_SYSADMIN_CACHE_PREFIX}{token}") if token else None
    rows = (payload or {}).get("results_rows") or []

    csv_text = sysadmin_results_to_csv(rows)

    response = Response(csv_text, mimetype="text/csv")
    response.headers["Content-Disposition"] = 'attachment; filename="bulk_sysadmin_promote_results.csv"'
    return response


# ----------------------------------------------------------------------------------------------------------------------------------


def admin_bulk_org_invite_reset():
    _require_bulk_org_invite_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.pop("csvinvite_bulk_org_invite_token", None)
    if token:
        _csvinvite_cache_delete(f"{CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX}{token}")

    return toolkit.redirect_to("csvinvite.admin_bulk_org_invite_get")


def admin_bulk_org_invite_get():
    _require_bulk_org_invite_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.get("csvinvite_bulk_org_invite_token")
    payload = _csvinvite_cache_get(f"{CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX}{token}") if token else None

    return render_template(
        "csvinvite/admin_bulk_org_invite.html",
        results=(payload or {}).get("results"),
        error=(payload or {}).get("error"),
    )

def admin_bulk_org_invite_post():
    _require_bulk_org_invite_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    if not is_redis_available():
        return render_template(
            "csvinvite/admin_bulk_org_invite.html",
            results=None,
            error="Redis is not available; cannot store bulk invite results safely.",
        )

    f = request.files.get("file")

    # PRG-friendly validation: γράφουμε error στο Redis + redirect σε GET
    if not f or not getattr(f, "filename", ""):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_org_invite_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX}{token}",
            {"results": None, "error": "Please choose a CSV file."},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_org_invite_get")

    if not f.filename.lower().endswith(".csv"):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_org_invite_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX}{token}",
            {"results": None, "error": "Only CSV files are allowed."},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_org_invite_get")

    header_bytes = f.read(2048)
    f.seek(0)
    mime = magic.from_buffer(header_bytes, mime=True)
    if not mime.startswith("text/"):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_org_invite_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX}{token}",
            {"results": None, "error": "Invalid file content. Please upload a valid CSV text file."},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_org_invite_get")

    text = _read_uploaded_csv_text(f)
    csv_rows, parse_errors = _parse_bulk_org_invite_csv(text)
    if parse_errors:
        token = _csvinvite_new_token()
        session["csvinvite_bulk_org_invite_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX}{token}",
            {"results": None, "error": "; ".join(parse_errors)},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_org_invite_get")

    dry_run = _is_truthy(request.form.get("dry_run"))

    results = {
        "total": 0,
        "would_invite": 0,
        "invited": 0,
        "skipped_invalid": 0,
        "errors": 0,
        "rows": [],
        "dry_run": dry_run,
    }

    org_show = toolkit.get_action("organization_show")

    for r in csv_rows:
        results["total"] += 1
        org_slug = r["org"]
        email = r["email"]
        role = r["role"]
        line_no = r["line"]

        # Resolve org by slug
        try:
            org = org_show(_fresh_action_context(context), {"id": org_slug})
            org_id = org.get("id")
            if not org_id:
                raise toolkit.ObjectNotFound(f"Organization '{org_slug}' not found.")
        except Exception as e:
            results["errors"] += 1
            results["rows"].append({
                "line": line_no,
                "org": org_slug,
                "email": email,
                "role": role,
                "status": "error",
                "message": f"Organization not found or not accessible: {e}",
            })
            continue

        if dry_run:
            results["would_invite"] += 1
            results["rows"].append({
                "line": line_no,
                "org": org_slug,
                "email": email,
                "role": role,
                "status": "dry_run",
                "message": "Would be invited (dry-run)",
            })
            continue

        try:
            _call_user_invite(_fresh_action_context(context), org_id, email, role)
            results["invited"] += 1
            results["rows"].append({
                "line": line_no,
                "org": org_slug,
                "email": email,
                "role": role,
                "status": "ok",
                "message": "Invited / pending created",
            })
        except toolkit.ValidationError as e:
            results["errors"] += 1
            results["rows"].append({
                "line": line_no,
                "org": org_slug,
                "email": email,
                "role": role,
                "status": "error",
                "message": str(e.error_dict or e),
            })
        except Exception as e:
            results["errors"] += 1
            results["rows"].append({
                "line": line_no,
                "org": org_slug,
                "email": email,
                "role": role,
                "status": "error",
                "message": str(e),
            })

    # PRG + Redis: stash results server-side και κρατάμε μόνο token στο cookie
    token = _csvinvite_new_token()
    session["csvinvite_bulk_org_invite_token"] = token
    _csvinvite_cache_set(
        f"{CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX}{token}",
        {"results": results, "error": None},
        ttl_seconds=CSVINVITE_BULK_ORG_SYNC_TTL_SECONDS,  # 30 minutes
    )

    return toolkit.redirect_to("csvinvite.admin_bulk_org_invite_get")


def admin_bulk_org_invite_export():
    _require_bulk_org_invite_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.get("csvinvite_bulk_org_invite_token")
    payload = _csvinvite_cache_get(f"{CSVINVITE_BULK_ORG_INVITE_CACHE_PREFIX}{token}") if token else None
    results = (payload or {}).get("results") or {}
    rows = (results.get("rows") if isinstance(results, dict) else None) or []

    output = io.StringIO()
    output.write('\N{BOM}')

    writer = csv.DictWriter(output, fieldnames=["line", "org", "email", "role", "status", "message"])
    writer.writeheader()
    writer.writerows(rows)

    response = Response(output.getvalue(), mimetype="text/csv")
    response.headers["Content-Disposition"] = 'attachment; filename="bulk_org_invite_results.csv"'
    return response

# ----------------------------------------------------------------------------------------------------------------------------------

def admin_bulk_org_sync_reset():
    _require_bulk_org_sync_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.pop("csvinvite_bulk_org_sync_token", None)
    if token:
        _csvinvite_cache_delete(f"{CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX}{token}")

    return toolkit.redirect_to("csvinvite.admin_bulk_org_sync_get")


def admin_bulk_org_sync_get():
    _require_bulk_org_sync_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.get("csvinvite_bulk_org_sync_token")
    payload = _csvinvite_cache_get(f"{CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX}{token}") if token else None

    return render_template(
        "csvinvite/admin_bulk_org_sync.html",
        report=(payload or {}).get("report"),
        error=(payload or {}).get("error"),
    )

def admin_bulk_org_sync_post():
    _require_bulk_org_sync_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    # αν Redis δεν είναι διαθέσιμο, fail
    if not is_redis_available():
        return render_template(
            "csvinvite/admin_bulk_org_sync.html",
            report=None,
            error="Redis is not available; cannot store bulk sync results safely.",
        )

    f = request.files.get("file")

    # PRG-friendly validation: γράφουμε error στο Redis + redirect σε GET
    if not f or not getattr(f, "filename", ""):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_org_sync_token"] = token
        _csvinvite_cache_set(f"{CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX}{token}", {"report": None, "error": "Please choose a CSV file."})
        return toolkit.redirect_to("csvinvite.admin_bulk_org_sync_get")

    if not f.filename.lower().endswith(".csv"):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_org_sync_token"] = token
        _csvinvite_cache_set(f"{CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX}{token}", {"report": None, "error": "Only CSV files are allowed."})
        return toolkit.redirect_to("csvinvite.admin_bulk_org_sync_get")

    header_bytes = f.read(2048)
    f.seek(0)
    mime = magic.from_buffer(header_bytes, mime=True)
    if not mime.startswith("text/"):
        token = _csvinvite_new_token()
        session["csvinvite_bulk_org_sync_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX}{token}",
            {"report": None, "error": "Invalid file content. Please upload a valid CSV text file."},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_org_sync_get")

    text = _read_uploaded_csv_text(f)
    csv_rows, parse_errors = _parse_bulk_org_sync_csv(text)
    if parse_errors:
        token = _csvinvite_new_token()
        session["csvinvite_bulk_org_sync_token"] = token
        _csvinvite_cache_set(
            f"{CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX}{token}",
            {"report": None, "error": "; ".join(parse_errors)},
        )
        return toolkit.redirect_to("csvinvite.admin_bulk_org_sync_get")

    dry_run = _is_truthy(request.form.get("dry_run", ""))

    # default OFF; allow sysadmin to turn it ON manually
    remove_missing = _is_truthy(request.form.get("remove_missing", ""))

    # group rows by org reference
    rows_by_org = {}
    for r in csv_rows:
        rows_by_org.setdefault(r["org"], []).append({
            "line": r["line"],
            "username": r["username"],
            "email": r["email"],
            "role": r["role"],
        })

    org_show = toolkit.get_action("organization_show")

    report = {
        "dry_run": dry_run,
        "remove_missing": remove_missing,
        "total_orgs": len(rows_by_org),
        "total_rows": len(csv_rows),
        "orgs": [],
        "errors": 0,
        "warnings": 0,
        "applied": (not dry_run),
    }

    for org_ref, org_rows in rows_by_org.items():
        block = {
            "org_ref": org_ref,
            "org_id": None,
            "org_name": None,
            "status": "ok",
            "message": "",
            "plan": None,
            "applied": None,
            "apply_error": None,
        }

        # resolve org by id OR slug/name (same behavior as bulk-invite)
        try:
            org = org_show(_fresh_action_context(context), {"id": org_ref})
            block["org_id"] = org.get("id")
            block["org_name"] = org.get("name") or org.get("title") or ""
            if not block["org_id"]:
                raise toolkit.ObjectNotFound(f"Organization '{org_ref}' not found.")
        except Exception as e:
            report["errors"] += 1
            block["status"] = "error"
            block["message"] = f"Organization not found or not accessible: {e}"
            report["orgs"].append(block)
            continue

        org_id = block["org_id"]

        # build plan using existing single-org logic
        try:
            existing, by_username, by_email = _build_org_member_index(_fresh_action_context(context), org_id)
            plan = _plan_full_sync(_fresh_action_context(context), org_id, existing, by_username, by_email, org_rows)
            block["plan"] = plan

            if plan.get("warnings"):
                report["warnings"] += len(plan["warnings"] or [])
        except Exception as e:
            report["errors"] += 1
            block["status"] = "error"
            block["message"] = f"Failed to build sync plan: {e}"
            report["orgs"].append(block)
            continue

        # apply if requested
        if not dry_run:
            try:
                block["applied"] = _apply_full_sync(_fresh_action_context(context), org_id, block["plan"], remove_missing=remove_missing)
            except toolkit.NotAuthorized:
                report["errors"] += 1
                block["apply_error"] = "Not authorized to apply changes."
            except toolkit.ValidationError as e:
                report["errors"] += 1
                block["apply_error"] = str(e.error_dict or e)
            except Exception as e:
                report["errors"] += 1
                block["apply_error"] = str(e)

        report["orgs"].append(block)

    export_rows = _bulk_org_sync_plan_to_export_rows(report)

    token = _csvinvite_new_token()
    session["csvinvite_bulk_org_sync_token"] = token
    _csvinvite_cache_set(
        f"{CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX}{token}",
        {"report": report, "error": None, "export_rows": export_rows},
        ttl_seconds=CSVINVITE_BULK_ORG_SYNC_TTL_SECONDS,
    )

    return toolkit.redirect_to("csvinvite.admin_bulk_org_sync_get")


def admin_bulk_org_sync_export():
    _require_bulk_org_sync_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    token = session.get("csvinvite_bulk_org_sync_token")
    payload = _csvinvite_cache_get(f"{CSVINVITE_BULK_ORG_SYNC_CACHE_PREFIX}{token}") if token else None
    rows = (payload or {}).get("export_rows") or []

    output = io.StringIO()
    output.write('\N{BOM}')

    fieldnames = [
        "org",
        "org_id",
        "org_name",
        "action",
        "line",
        "username",
        "email",
        "current_role",
        "new_role",
        "state",
        "reason",
    ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    response = Response(output.getvalue(), mimetype="text/csv")
    response.headers["Content-Disposition"] = 'attachment; filename="bulk_org_sync_plan.csv"'
    return response

# ----------------------------------------------------------------------------------------------------------------------------------

def admin_user_management_get():
    """
    Landing page (grid) for sysadmin user-management tools.
    """
    _require_user_management_tab_enabled()

    context = {"user": toolkit.c.user, "auth_user_obj": toolkit.c.userobj, "model": model}
    _require_sysadmin(context)

    return render_template(
        "csvinvite/admin_user_management.html",
        page_title=toolkit._("User management"),
    )

# ----------------------------------------------------------------------------------------------------------------------------------

def get_blueprints():
    bp = Blueprint("csvinvite", __name__)
    bp.add_url_rule("/organization/<org_id>/members/import", "import_members_get", import_members_get, methods=["GET"])
    bp.add_url_rule("/organization/<org_id>/members/import", "import_members_post", import_members_post, methods=["POST"])
    bp.add_url_rule("/organization/<org_id>/members/import/export", "export_results_csv", export_results_csv, methods=["POST"])

    bp.add_url_rule(
        "/organization/<org_id>/members/import/result",
        "import_members_result_get",
        import_members_result_get,
        methods=["GET"],
    )

    bp.add_url_rule(
        "/organization/<org_id>/members/import/reset",
        "import_members_reset",
        import_members_reset,
        methods=["POST"],
    )

    bp.add_url_rule(
        "/organization/<org_id>/members/import/template",
        "import_members_template_csv",
        import_members_template_csv,
        methods=["GET"],
    )

    bp.add_url_rule("/organization/<org_id>/members/sync", "sync_members_get", sync_members_get, methods=["GET"])
    bp.add_url_rule("/organization/<org_id>/members/sync", "sync_members_post", sync_members_post, methods=["POST"])

    bp.add_url_rule(
        "/organization/<org_id>/members/sync/result",
        "sync_members_result_get",
        sync_members_result_get,
        methods=["GET"],
    )

    bp.add_url_rule(
        "/organization/<org_id>/members/sync/reset",
        "sync_members_reset",
        sync_members_reset,
        methods=["POST"],
    )

    bp.add_url_rule("/organization/<org_id>/members/sync/export", "export_sync_plan_csv", export_sync_plan_csv, methods=["POST"])

    bp.add_url_rule(
        "/organization/<org_id>/members/sync/template",
        "sync_members_template_csv",
        sync_members_template_csv,
        methods=["GET"],
    )

    bp.add_url_rule(
        "/ckan-admin/users/management",
        "admin_user_management_get",
        admin_user_management_get,
        methods=["GET"],
    )
    bp.add_url_rule("/ckan-admin/users/bulk-delete", "admin_bulk_user_delete_get", admin_bulk_user_delete_get,
                    methods=["GET"])
    bp.add_url_rule("/ckan-admin/users/bulk-delete", "admin_bulk_user_delete_post", admin_bulk_user_delete_post,
                    methods=["POST"])
    bp.add_url_rule(
        "/ckan-admin/users/bulk-delete/reset",
        "admin_bulk_user_delete_reset",
        admin_bulk_user_delete_reset,
        methods=["POST"],
    )
    bp.add_url_rule("/ckan-admin/users/bulk-delete/export", "admin_bulk_user_delete_export",
                    admin_bulk_user_delete_export, methods=["GET"])
    bp.add_url_rule(
        "/ckan-admin/users/bulk-delete/template",
        "admin_bulk_user_delete_template_csv",
        admin_bulk_user_delete_template_csv,
        methods=["GET"],
    )

    bp.add_url_rule(
        "/ckan-admin/users/bulk-invite",
        "admin_bulk_org_invite_get",
        admin_bulk_org_invite_get,
        methods=["GET"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/bulk-invite",
        "admin_bulk_org_invite_post",
        admin_bulk_org_invite_post,
        methods=["POST"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/bulk-invite/export",
        "admin_bulk_org_invite_export",
        admin_bulk_org_invite_export,
        methods=["GET"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/bulk-invite/reset",
        "admin_bulk_org_invite_reset",
        admin_bulk_org_invite_reset,
        methods=["POST"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/bulk-invite/template",
        "admin_bulk_org_invite_template_csv",
        admin_bulk_org_invite_template_csv,
        methods=["GET"],
    )

    bp.add_url_rule(
        "/ckan-admin/users/bulk-sync",
        "admin_bulk_org_sync_get",
        admin_bulk_org_sync_get,
        methods=["GET"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/bulk-sync",
        "admin_bulk_org_sync_post",
        admin_bulk_org_sync_post,
        methods=["POST"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/bulk-sync/reset",
        "admin_bulk_org_sync_reset",
        admin_bulk_org_sync_reset,
        methods=["POST"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/bulk-sync/export",
        "admin_bulk_org_sync_export",
        admin_bulk_org_sync_export,
        methods=["GET"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/bulk-sync/template",
        "admin_bulk_org_sync_template_csv",
        admin_bulk_org_sync_template_csv,
        methods=["GET"],
    )

    bp.add_url_rule(
        "/ckan-admin/users/sysadmin-promote",
        "admin_bulk_sysadmin_promote_get",
        admin_bulk_sysadmin_promote_get,
        methods=["GET"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/sysadmin-promote",
        "admin_bulk_sysadmin_promote_post",
        admin_bulk_sysadmin_promote_post,
        methods=["POST"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/sysadmin-promote/reset",
        "admin_bulk_sysadmin_promote_reset",
        admin_bulk_sysadmin_promote_reset,
        methods=["POST"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/sysadmin-promote/export",
        "admin_bulk_sysadmin_promote_export",
        admin_bulk_sysadmin_promote_export,
        methods=["GET"],
    )
    bp.add_url_rule(
        "/ckan-admin/users/sysadmin-promote/template",
        "admin_bulk_sysadmin_promote_template_csv",
        admin_bulk_sysadmin_promote_template_csv,
        methods=["GET"],
    )

    return [bp]