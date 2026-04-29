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


def enable_bulk_sysadmin_promote() -> bool:
    """
    Enables bulk sysadmin promotion via CSV.
    Disabled by default.
    """
    return _cfg_bool("ckanext.csvinvite.enable_bulk_sysadmin_promote", False)


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
    return bool(enable_bulk_user_delete() or enable_bulk_org_invite() or enable_bulk_sysadmin_promote())


_USER_MGMT_ENDPOINTS = frozenset([
    'admin_user_management_get',
    'admin_bulk_org_invite_get', 'admin_bulk_org_invite_post',
    'admin_bulk_org_invite_reset', 'admin_bulk_org_invite_export',
    'admin_bulk_org_invite_template_csv',
    'admin_bulk_org_sync_get', 'admin_bulk_org_sync_post',
    'admin_bulk_org_sync_reset', 'admin_bulk_org_sync_export',
    'admin_bulk_org_sync_template_csv',
    'admin_bulk_user_delete_get', 'admin_bulk_user_delete_post',
    'admin_bulk_user_delete_reset', 'admin_bulk_user_delete_export',
    'admin_bulk_user_delete_template_csv',
    'admin_bulk_sysadmin_promote_get', 'admin_bulk_sysadmin_promote_post',
    'admin_bulk_sysadmin_promote_reset', 'admin_bulk_sysadmin_promote_export',
    'admin_bulk_sysadmin_promote_template_csv',
])


def show_active_directory_label() -> bool:
    """
    When True, Keycloak labels become 'Keycloak/Active Directory'.
    Default: False (plain 'Keycloak' label).
    ckan.ini: ckanext.csvinvite.show_active_directory_label = true
    """
    return _cfg_bool("ckanext.csvinvite.show_active_directory_label", False)


def build_user_management_nav():
    """Build nav icon for User Management that stays active on all sub-routes."""
    blueprint, endpoint = toolkit.get_endpoint()

    is_active = (blueprint == 'csvinvite' and endpoint in _USER_MGMT_ENDPOINTS)
    active_class = ' class="active"' if is_active else ''

    url = toolkit.url_for('csvinvite.admin_user_management_get')
    title = toolkit._('User management')

    return toolkit.literal(
        f'<li{active_class}><a href="{url}">'
        f'<i class="fa fa-users"></i> {title}</a></li>'
    )


def get_helpers():
    return {
        "csvinvite_enable_invite_process": enable_invite_process,
        "csvinvite_enable_sync_process": enable_sync_process,
        "csvinvite_enable_bulk_user_delete": enable_bulk_user_delete,
        "csvinvite_enable_bulk_org_invite": enable_bulk_org_invite,
        "csvinvite_enable_bulk_org_sync": enable_bulk_org_sync,
        "csvinvite_enable_bulk_sysadmin_promote": enable_bulk_sysadmin_promote,
        "csvinvite_show_user_management_tab": show_user_management_tab,
        "csvinvite_build_user_management_nav": build_user_management_nav,
        "csvinvite_show_active_directory_label": show_active_directory_label,
    }
