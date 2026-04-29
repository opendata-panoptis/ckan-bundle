[![Tests](https://github.com/apostolosvadrachanis/ckanext-csvinvite/workflows/Tests/badge.svg?branch=main)](https://github.com/apostolosvadrachanis/ckanext-csvinvite/actions)

# ckanext-csvinvite

`ckanext-csvinvite` is a CKAN extension that adds **CSV-driven user and membership management** workflows, designed to make **bulk operations safe, auditable, and repeatable**.

It focuses on two common operational needs:

1. **Inviting organization members from CSV** (creating pending memberships from email addresses).
2. **Synchronizing organization members from CSV** (add/remove/update roles based on a "source of truth" list).

In addition, it can optionally provide **bulk user deletion** and **bulk sysadmin promotion** (sysadmin-only), with a strong emphasis on guardrails and reporting.

---

## Key features

### Organization-level tools (for organization admins)

- **Import members from CSV (invite / pending)**
  - Upload a CSV with `email` and optional `role`
  - Creates **pending invitations**
  - **Email notifications are configurable**: depending on your CKAN/extension configuration, invitations may be sent automatically or created silently (no email), while still recording the pending membership.
  - Supports **dry-run** preview (validate your CSV before sending any invitations)
  - Produces a detailed per-row report and allows exporting results to CSV
  - Includes a **downloadable CSV template** for correct formatting

- **Sync members from CSV (add / remove / role update)**
  - Upload a CSV with `role` and (`username` and/or `email`)
  - Supports **dry-run** preview (plan first, apply later)
  - Optionally **remove members missing from the CSV** (full synchronization)
  - Protects **sysadmins** and the **current user** from accidental removal
  - Produces a structured sync plan with warnings (eg large removal ratios) and export to CSV
  - Includes a **downloadable CSV template**

### Sysadmin tools (optional feature flags)

- **Bulk user deletion from CSV (destructive)**
  - Upload a CSV containing usernames/emails to delete
  - Sysadmin-only access, designed for controlled cleanup operations
  - Generates exportable results for traceability

- **Bulk organization invite from CSV (multi-org)**
  - Invite users into multiple organizations in one operation
  - Supports dry-run and produces an exportable report

- **Bulk organization sync from CSV (multi-org)**
  - Build/apply sync plans per organization from a single CSV
  - Supports dry-run, optional removal of missing members, warnings, and exportable plans

- **Bulk sysadmin promotion from CSV**
  - Upload a CSV with `username` and/or `email` to promote users to sysadmin in bulk
  - Supports **dry-run** preview before applying changes
  - Handles three cases automatically:
    - **Existing active or pending users** -> promoted to sysadmin (`sysadmin=true`)
    - **Already sysadmin** -> reported and skipped (no duplicate action)
    - **Unknown email (user not found)** -> creates a new `pending` user with `sysadmin=true`
    - **Username-only with no match** -> reported as "Not found" (no email = cannot create)
  - Optional **"Remove missing"** mode: demotes existing sysadmins not listed in the CSV
  - **Protected accounts** (configured via `ckan.ini`) and the current user are **never demoted**
  - Produces a detailed per-category report and allows exporting results to CSV
  - Sysadmin-only access, disabled by default

---

## CSV format reference

Each operation expects a specific set of CSV columns. All CSV files must use comma (`,`) as delimiter.

| Operation | Required columns | Optional columns | Notes |
| --- | --- | --- | --- |
| Org Import (invite) | `email` | `role` (default: `member`) | |
| Org Sync | `role` + (`username` and/or `email`) | | At least one of `username`/`email` required |
| Bulk Org Invite | `org`, `email` | `role` (default: `member`) | `org` = organization slug |
| Bulk Org Sync | `org`, `role` + (`username` and/or `email`) | | At least one of `username`/`email` required |
| Bulk User Delete | `username` and/or `email` | | At least one required per row |
| Bulk Sysadmin Promote | `username` and/or `email` | | At least one required per row |

### Column aliases

- The `email` column also accepts the header name `mail`.

### Accepted roles

Roles are case-insensitive. The following values are accepted:

| Role | Aliases (Greek) |
| --- | --- |
| `admin` | `διαχειριστής` |
| `editor` | `εκδότης`, `συντάκτης` |
| `member` | `μέλος` |

If `role` is omitted or empty, it defaults to `member`.

### Encoding

- **Input**: CSV files are parsed as **UTF-8**. If decoding fails, **CP1253** (Windows Greek) is attempted as a fallback.
- **Output**: Exported CSV files include a **UTF-8 BOM** for Excel compatibility.

---

## Typical workflows

- **Onboard a team into an organization**
  1. Download the CSV template
  2. Fill `email, role`
  3. Upload -> review per-row results -> export report if needed

- **Keep an organization aligned with an HR/IdP roster**
  1. Export your roster to CSV (username/email + role)
  2. Upload as **dry-run** to preview additions/removals/role changes
  3. Apply when the plan looks correct
  4. Export the sync plan for auditing

- **Promote a set of users to sysadmin (or sync the full sysadmin list)**
  1. Prepare a CSV with `username` and/or `email`
  2. Upload as **dry-run** to preview: who will be promoted, who is already sysadmin, who will be created as pending, who cannot be matched
  3. Optionally enable **"Remove missing"** to demote sysadmins not in the CSV
  4. Apply when the plan looks correct
  5. Export the results CSV for auditing

---

## Authorization & access control

| Scope | Required role | Additional condition |
| --- | --- | --- |
| Organization import / sync | Organization admin | Feature flag enabled |
| User Management landing page | Sysadmin | At least one admin tool enabled |
| Bulk user delete | Sysadmin | `enable_bulk_user_delete = true` |
| Bulk org invite | Sysadmin | `enable_bulk_org_invite = true` |
| Bulk org sync | Sysadmin | `enable_bulk_org_sync = true` |
| Bulk sysadmin promote | Sysadmin | `enable_bulk_sysadmin_promote = true` |

### Protection rules

The plugin enforces safety guardrails that **cannot be overridden** from the UI:

| Context | Protected from | Rule |
| --- | --- | --- |
| Org Sync | Removal | Sysadmins and the current user are never removed |
| Bulk User Delete | Deletion | Sysadmins and the current user are never deleted |
| Bulk Sysadmin Promote | Demotion | Protected emails (from config) and the current user are never demoted |
| Org Sync | Mass removal | A warning is displayed when >50% of current members are scheduled for removal |

---

## Safety & design notes

- **Access control is enforced**:
  - Organization tools require organization admin privileges
  - Bulk destructive actions are **sysadmin-only**
- **Dry-run support** for all write operations (import, sync, bulk invite, bulk sync, sysadmin promote) to reduce risk
- **PRG + Redis-backed results storage**: results and plans are stored server-side in Redis (identified by a random token saved in the user's session cookie), keeping large payloads out of the browser and allowing clean refresh/back behavior. Data expires automatically after **30 minutes**.
- **Excel-friendly exports**: generated CSV exports include UTF-8 BOM for better compatibility

---

## Requirements

- **CKAN 2.11** (see Compatibility below)
- **Redis** (used by CKAN; the plugin stores temporary results/plans in Redis)

## Compatibility

This extension has been **developed and tested exclusively on CKAN 2.11**.

Compatibility with other CKAN versions has **not been tested** and is not guaranteed.
If you plan to use it with a different CKAN release, please test it thoroughly in a
staging environment first.

| CKAN version    | Compatible? |
| --------------- | ----------- |
| 2.11            | yes         |
| 2.10 and earlier| not tested  |


## Installation

To install ckanext-csvinvite:

1. Activate your CKAN virtual environment, for example:

     . /usr/lib/ckan/default/bin/activate

2. Clone the source and install it on the virtualenv

    git clone https://github.com/apostolosvadrachanis/ckanext-csvinvite.git
    cd ckanext-csvinvite
    pip install -e .
	pip install -r requirements.txt

3. Add `csvinvite` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

     sudo service apache2 reload


## Config settings

`ckanext-csvinvite` provides several **feature flags** that control its processes.

If a configuration option is **not defined** in `ckan.ini`, it defaults to the value shown below.
This allows the plugin to work **out of the box** without requiring additional configuration.

### Enable CSV invite process (pending memberships)

Controls whether the **CSV-based invite process** is enabled (creation of pending organization memberships based on email addresses).

```ini
# Enable CSV-based invite process (default: true)
ckanext.csvinvite.enable_invite_process = true
```

When set to `false`:

* The *"Import from CSV"* button is hidden from the UI

---

### Enable CSV sync process (member synchronization)

Controls whether the **CSV-based organization member synchronization** process is enabled
(additions, removals, and role updates).

```ini
# Enable CSV-based member synchronization (default: true)
ckanext.csvinvite.enable_sync_process = true
```

When set to `false`:

* The *"Sync from CSV"* button is hidden from the UI
* The synchronization functionality can be fully disabled

---

### Enable bulk user deletion via CSV

Controls whether administrators can **delete users in bulk using a CSV file**.

This is a **destructive operation** and is therefore **disabled by default**.

```ini
# Enable bulk user deletion via CSV (default: false)
ckanext.csvinvite.enable_bulk_user_delete = true
```

---

### Enable sysadmin bulk org invite via CSV

Controls whether **sysadmins** can invite users to **multiple organizations** from a single CSV upload.

This is a **non-destructive** workflow (creates invites / pending memberships) and is **enabled by default**.

```ini
# Enable sysadmin bulk org invitations via CSV (default: true)
ckanext.csvinvite.enable_bulk_org_invite = true
```

When set to `false`:

* The *Bulk invite to organizations* tool is disabled/hidden in the sysadmin UI.

---

### Enable sysadmin bulk org sync via CSV

Controls whether **sysadmins** can synchronize members across **multiple organizations** from a single CSV upload.

This workflow can be **potentially destructive** (it may remove members if "remove missing" is enabled) and is **enabled by default**. Without `remove_missing`, it operates in additive-only mode.

```ini
# Enable sysadmin bulk org synchronization via CSV (default: true)
ckanext.csvinvite.enable_bulk_org_sync = true
```

When set to `false`:

* The *Bulk sync members* tool is disabled/hidden in the sysadmin UI.

---

### Enable bulk sysadmin promotion via CSV

Controls whether **sysadmins** can promote users to sysadmin in bulk from a CSV upload.

This is a **privileged operation** (grants sysadmin rights) and is therefore **disabled by default**.

```ini
# Enable bulk sysadmin promotion via CSV (default: false)
ckanext.csvinvite.enable_bulk_sysadmin_promote = true
```

When set to `false`:

* The *Bulk sysadmin promotion* card is hidden from the sysadmin User Management UI.

---

### Protected sysadmin emails

A comma-separated list of email addresses whose sysadmin rights are **permanently protected**.
Users matching these emails will **never be demoted**, even when the *"Remove missing"* option is enabled.

```ini
# Comma-separated list of emails to protect from sysadmin demotion (default: empty)
ckanext.csvinvite.protected_sysadmin_emails = admin@example.org,superuser@example.org
```

If not set, only the **currently logged-in user** is automatically protected from demotion.

---

## URL endpoints

The plugin registers the following URL routes:

### Organization-level endpoints

| Method | URL | Description |
| --- | --- | --- |
| GET | `/organization/<id>/members/import` | Import members form |
| POST | `/organization/<id>/members/import` | Upload CSV and process invitations |
| GET | `/organization/<id>/members/import/result` | View import results |
| GET | `/organization/<id>/members/import/template` | Download CSV template |
| POST | `/organization/<id>/members/import/reset` | Clear cached results |
| POST | `/organization/<id>/members/import/export` | Export results as CSV |
| GET | `/organization/<id>/members/sync` | Sync members form |
| POST | `/organization/<id>/members/sync` | Upload CSV and process sync |
| GET | `/organization/<id>/members/sync/result` | View sync plan/results |
| GET | `/organization/<id>/members/sync/template` | Download CSV template |
| POST | `/organization/<id>/members/sync/reset` | Clear cached plan |
| POST | `/organization/<id>/members/sync/export` | Export sync plan as CSV |

### Sysadmin endpoints

| Method | URL | Description |
| --- | --- | --- |
| GET | `/ckan-admin/users/management` | User management landing page |
| GET/POST | `/ckan-admin/users/bulk-delete` | Bulk user deletion |
| GET | `/ckan-admin/users/bulk-delete/template` | Download CSV template |
| GET | `/ckan-admin/users/bulk-delete/export` | Export results as CSV |
| POST | `/ckan-admin/users/bulk-delete/reset` | Clear cached results |
| GET/POST | `/ckan-admin/users/bulk-invite` | Bulk org invitation |
| GET | `/ckan-admin/users/bulk-invite/template` | Download CSV template |
| GET | `/ckan-admin/users/bulk-invite/export` | Export results as CSV |
| POST | `/ckan-admin/users/bulk-invite/reset` | Clear cached results |
| GET/POST | `/ckan-admin/users/bulk-sync` | Bulk org sync |
| GET | `/ckan-admin/users/bulk-sync/template` | Download CSV template |
| GET | `/ckan-admin/users/bulk-sync/export` | Export results as CSV |
| POST | `/ckan-admin/users/bulk-sync/reset` | Clear cached results |
| GET/POST | `/ckan-admin/users/sysadmin-promote` | Bulk sysadmin promotion |
| GET | `/ckan-admin/users/sysadmin-promote/template` | Download CSV template |
| GET | `/ckan-admin/users/sysadmin-promote/export` | Export results as CSV |
| POST | `/ckan-admin/users/sysadmin-promote/reset` | Clear cached results |

---

## Error handling

The plugin returns the following HTTP status codes for error conditions:

| Code | Condition |
| --- | --- |
| 400 | Invalid request (missing file, bad format) |
| 403 | Unauthorized (insufficient permissions) |
| 404 | Feature disabled via config or resource not found |
| 503 | Redis unavailable |

Row-level errors (invalid email, user not found, etc.) are reported per-row in the results page and included in CSV exports.

---

## Internationalization

The plugin supports the following languages:

- **English** (en)
- **Greek** (el)

Translations cover all admin-level templates, flash messages, and UI labels.

To compile translations after changes:

```bash
python setup.py compile_catalog -d ckanext/csvinvite/i18n
```

---

## Template helpers

The following Jinja2 helper functions are available for use in templates (via `h.*`):

| Helper | Returns | Description |
| --- | --- | --- |
| `h.csvinvite_enable_invite_process()` | `bool` | Whether org-level CSV invite is enabled |
| `h.csvinvite_enable_sync_process()` | `bool` | Whether org-level CSV sync is enabled |
| `h.csvinvite_enable_bulk_user_delete()` | `bool` | Whether bulk user deletion is enabled |
| `h.csvinvite_enable_bulk_org_invite()` | `bool` | Whether bulk org invite is enabled |
| `h.csvinvite_enable_bulk_org_sync()` | `bool` | Whether bulk org sync is enabled |
| `h.csvinvite_enable_bulk_sysadmin_promote()` | `bool` | Whether bulk sysadmin promote is enabled |
| `h.csvinvite_show_user_management_tab()` | `bool` | Whether to show the admin User Management tab (requires sysadmin + at least one admin tool enabled) |

These are useful for theme developers who want to conditionally display UI elements based on enabled features.

---

## Developer installation

To install ckanext-csvinvite for development, activate your CKAN virtualenv and
do:

    git clone https://github.com/apostolosvadrachanis/ckanext-csvinvite.git
    cd ckanext-csvinvite
    pip install -e .
    pip install -r dev-requirements.txt


## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini


## Releasing a new version of ckanext-csvinvite

If ckanext-csvinvite should be available on PyPI you can follow these steps to publish a new version:

1. Update the version number in the `pyproject.toml` file. See [PEP 440](http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers) for how to choose version numbers.

2. Make sure you have the latest version of necessary packages:

    pip install --upgrade setuptools wheel twine

3. Create a source and binary distributions of the new version:

       python -m build && twine check dist/*

   Fix any errors you get.

4. Upload the source distribution to PyPI:

       twine upload dist/*

5. Commit any outstanding changes:

       git commit -a
       git push

6. Tag the new release of the project on GitHub with the version number from
   the `pyproject.toml` file. For example if the version number is
   0.0.1 then do:

       git tag 0.0.1
       git push --tags

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
