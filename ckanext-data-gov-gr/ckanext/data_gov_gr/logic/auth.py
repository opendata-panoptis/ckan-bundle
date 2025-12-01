from typing import Dict, Any

import ckan.plugins.toolkit as toolkit

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
