from __future__ import annotations

from typing import Any, Dict

import ckan.authz as authz
import ckan.model as model

import ckan.plugins.toolkit as toolkit
from ckanext.keycloak.helpers import enable_internal_login


def _dataset_activity_stream_visibility_is_restricted() -> bool:
    return toolkit.asbool(
        toolkit.config.get(
            "ckanext.data_gov_gr.activity_stream.dataset.restrict_visibility",
            True,
        )
    )


def _user_can_view_dataset_activity_stream(
    context: Dict[str, Any], package_id_or_name: str | None
) -> bool:
    user_name = context.get("user")

    if authz.is_sysadmin(user_name):
        return True

    if not package_id_or_name:
        return False

    if not user_name:
        return False

    package = model.Package.get(package_id_or_name)
    if not package or not package.owner_org:
        return False

    user_role = authz.users_role_for_group_or_org(package.owner_org, user_name)
    return bool(user_role and user_role.lower() == "admin")


def _user_can_view_organization_activity_stream(
    context: Dict[str, Any], organization_id_or_name: str | None
) -> bool:
    user_name = context.get("user")

    if authz.is_sysadmin(user_name):
        return True

    if not organization_id_or_name:
        return False

    if not user_name:
        return False

    organization = model.Group.get(organization_id_or_name)
    if not organization or not organization.is_organization:
        return False

    user_role = authz.users_role_for_group_or_org(organization.id, user_name)
    return bool(user_role and user_role.lower() == "admin")


def organization_list_with_user_extras_auth(context, data_dict):
    """
    Authorization function for organization_list_with_user_extras.
    Only sysadmins are allowed to access this action.
    """
    # Check if user is a sysadmin
    return {'success': toolkit.check_access('sysadmin', context, data_dict)}

def user_organization_capacity_auth(context, data_dict):
    """
    Authorization function for user_organization_capacity.
    Only sysadmins are allowed to access this action.
    """
    # Check if user is a sysadmin
    return {'success': toolkit.check_access('sysadmin', context, data_dict)}

def check_user_org_permission(context: Dict[str, Any], data_dict: Dict[str, Any]) -> Dict[str, bool]:
    """
    Auth function για το check_user_org_permission endpoint.
    Επιτρέπει ανώνυμη πρόσβαση καθώς ο έλεγχος εξουσιοδότησης γίνεται εσωτερικά.
    """
    return {'success': True}

def user_reset_override(context, data_dict):
    """Override της user_reset function που ελέγχει αν είναι ενεργοποιημένο το internal login"""
    if not enable_internal_login():
        return {'success': False, 'msg': 'Password reset is disabled when internal login is not enabled'}
    else:
        return {'success': True}


@toolkit.chained_auth_function
@toolkit.auth_allow_anonymous_access
def package_activity_list(next_auth, context, data_dict):
    auth_result = next_auth(context, data_dict)

    if not auth_result.get("success"):
        return auth_result

    if not _dataset_activity_stream_visibility_is_restricted():
        return auth_result

    if _user_can_view_dataset_activity_stream(context, data_dict.get("id")):
        return auth_result

    return {
        "success": False,
        "msg": toolkit._(
            "The dataset activity stream is restricted to authorized users."
        ),
    }


@toolkit.chained_auth_function
@toolkit.auth_allow_anonymous_access
def organization_activity_list(next_auth, context, data_dict):
    auth_result = next_auth(context, data_dict)

    if not auth_result.get("success"):
        return auth_result

    if _user_can_view_organization_activity_stream(context, data_dict.get("id")):
        return auth_result

    return {
        "success": False,
        "msg": toolkit._(
            "The organization activity stream is restricted to organization "
            "admins and sysadmins."
        ),
    }
