# ckanext/csvinvite/helpers.py

import ckan.plugins.toolkit as toolkit


def _cfg_bool(key: str, default: bool = True) -> bool:
    val = toolkit.config.get(key)
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def enable_invite_process() -> bool:
    """
    Enables CSV-based invite (pending memberships).
    """
    return _cfg_bool("ckanext.csvinvite.enable_invite_process", True)


def enable_sync_process() -> bool:
    """
    Enables CSV-based member synchronization.
    """
    return _cfg_bool("ckanext.csvinvite.enable_sync_process", True)


def enable_bulk_user_delete() -> bool:
    """
    Enables bulk user deletion via CSV.
    Disabled by default (destructive operation).
    """
    return _cfg_bool("ckanext.csvinvite.enable_bulk_user_delete", False)


def enable_bulk_org_invite() -> bool:
    """
    Enables sysadmin bulk invitations to multiple organizations via CSV.
    Non-destructive, enabled by default.
    """
    return _cfg_bool("ckanext.csvinvite.enable_bulk_org_invite", True)


def enable_bulk_org_sync() -> bool:
    """
    Enables sysadmin bulk member synchronization for multiple organizations via CSV.
    Potentially destructive (can remove members), enabled by default.
    """
    return _cfg_bool("ckanext.csvinvite.enable_bulk_org_sync", True)


def show_user_management_tab() -> bool:
    """
    Controls visibility of the "User management" admin tab.

    Rules:
    - Visible only to sysadmins
    - Visible only if at least one user-management tool is enabled (feature flags)
      so we don't show an empty landing page.
    """
    uobj = getattr(toolkit.c, "userobj", None)
    if not bool(getattr(uobj, "sysadmin", False)):
        return False

    # For now, the only tool under /ckan-admin/users/management is bulk delete.
    # Later, we will extend this OR-chain with more feature flags.
    return bool(enable_bulk_user_delete() or enable_bulk_org_invite())


def get_helpers():
    return {
        "csvinvite_enable_invite_process": enable_invite_process,
        "csvinvite_enable_sync_process": enable_sync_process,
        "csvinvite_enable_bulk_user_delete": enable_bulk_user_delete,
        "csvinvite_enable_bulk_org_invite": enable_bulk_org_invite,
        "csvinvite_enable_bulk_org_sync": enable_bulk_org_sync,
        "csvinvite_show_user_management_tab": show_user_management_tab,
    }
