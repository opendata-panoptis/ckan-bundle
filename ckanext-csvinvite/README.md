[![Tests](https://github.com/apostolosvadrachanis/ckanext-csvinvite/workflows/Tests/badge.svg?branch=main)](https://github.com/apostolosvadrachanis/ckanext-csvinvite/actions)

# ckanext-csvinvite

`ckanext-csvinvite` is a CKAN extension that adds **CSV-driven user and membership management** workflows, designed to make **bulk operations safe, auditable, and repeatable**.

It focuses on two common operational needs:

1. **Inviting organization members from CSV** (creating pending memberships from email addresses).
2. **Synchronizing organization members from CSV** (add/remove/update roles based on a “source of truth” list).

In addition, it can optionally provide **bulk user deletion** (sysadmin-only), with a strong emphasis on guardrails and reporting.

---

## Key features

### Organization-level tools (for organization admins)

- **Import members from CSV (invite / pending)**
  - Upload a CSV with `email` and optional `role`
  - Creates **pending invitations**
  - **Email notifications are configurable**: depending on your CKAN/extension configuration, invitations may be sent automatically or created silently (no email), while still recording the pending membership.
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

---

## Typical workflows

- **Onboard a team into an organization**
  1. Download the CSV template
  2. Fill `email, role`
  3. Upload → review per-row results → export report if needed

- **Keep an organization aligned with an HR/IdP roster**
  1. Export your roster to CSV (username/email + role)
  2. Upload as **dry-run** to preview additions/removals/role changes
  3. Apply when the plan looks correct
  4. Export the sync plan for auditing

---

## Safety & design notes

- **Access control is enforced**:
  - Organization tools require organization admin privileges
  - Bulk destructive actions are **sysadmin-only**
- **Dry-run support** for synchronization to reduce risk
- **PRG + Redis-backed results storage**: results/plans are stored server-side (temporary), keeping large payloads out of the browser and allowing clean refresh/back behavior
- **Excel-friendly exports**: generated CSV exports include UTF-8 BOM for better compatibility

---


## Requirements

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

**TODO:** Add any additional install steps to the list below.
   For example installing any non-Python dependencies or adding any required
   config settings.

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

`ckanext-csvinvite` provides two **feature flags** that control its main processes.
Both features are **enabled by default** when the plugin is activated.

### Enable CSV invite process (pending memberships)

Controls whether the **CSV-based invite process** is enabled (creation of pending organization memberships based on email addresses).

```ini
# Enable CSV-based invite process (default: true)
ckanext.csvinvite.enable_invite_process = true
```

When set to `false`:

* The *“Import from CSV”* button is hidden from the UI

---

### Enable CSV sync process (member synchronization)

Controls whether the **CSV-based organization member synchronization** process is enabled
(additions, removals, and role updates).

```ini
# Enable CSV-based member synchronization (default: true)
ckanext.csvinvite.enable_sync_process = true
```

When set to `false`:

* The *“Sync from CSV”* button is hidden from the UI
* The synchronization functionality can be fully disabled

---

### ℹ️ Note

If a configuration option is **not defined** in `ckan.ini`, it defaults to `true`.
This allows the plugin to work **out of the box** without requiring additional configuration.

---

### Enable bulk user deletion via CSV

Controls whether administrators can **delete users in bulk using a CSV file**.

⚠️ This is a **destructive operation** and is therefore **disabled by default**.

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

⚠️ This workflow can be **potentially destructive** (it may remove members, if “remove missing” is enabled) and is **enabled by default**.

```ini
# Enable sysadmin bulk org synchronization via CSV (default: true)
ckanext.csvinvite.enable_bulk_org_sync = true
```

When set to `false`:

* The *Bulk sync members* tool is disabled/hidden in the sysadmin UI.

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
   the `setup.py` file. For example if the version number in `setup.py` is
   0.0.1 then do:

       git tag 0.0.1
       git push --tags

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
