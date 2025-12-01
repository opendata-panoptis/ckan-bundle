import ckan.plugins.toolkit as toolkit
from typing import Any, Dict
import logging

from ckan import authz
from ckan.lib.api_token import get_user_from_token
from ckan.lib.mailer import mail_recipient
from ckan.common import _, request
import ckan.logic.schema
from ckan.logic import _validate
import ckan.lib.helpers as h

log = logging.getLogger(__name__)


def organization_list_with_user_extras(context: Dict[str, Any], data_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a list of organizations with full user information including extras.

    Only sysadmins can access this action.

    Args:
        context: CKAN context
        data_dict: Can contain 'all_fields', 'include_extras', 'include_users'

    Returns:
        List of organizations with enriched user information
    """
    # Check if user has permission
    toolkit.check_access('organization_list_with_user_extras', context, data_dict)

    # Get the original organization list
    org_list = toolkit.get_action('organization_list')(
        context,
        {
            'all_fields': True,
            'include_extras': True,
            'include_users': True
        }
    )

    # Enrich user information for each organization
    for org in org_list:
        if 'users' in org:
            enriched_users = []
            for user in org['users']:
                try:
                    full_user = toolkit.get_action('user_show')(
                        context,
                        {
                            'id': user['id'],
                            'include_plugin_extras': True
                        }
                    )
                    enriched_users.append(full_user)
                except toolkit.ObjectNotFound:
                    continue
            org['users'] = enriched_users

    return org_list


def user_organization_capacity(context: Dict[str, Any], data_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Check if a user with given sub ID is a member of specified organization and return their capacity.

    Args:
        context: CKAN context
        data_dict: Must contain:
            - org_id: Organization ID or name
            - sub: User's sub ID from Keycloak

    Returns:
        dict with:
            - is_member: boolean indicating if user is member
            - capacity: user's role in organization (if member)
            - user_found (bool): True if user was found

    Raises:
        NotAuthorized: If the user is not a sysadmin
        ValidationError: If required parameters are missing
        ObjectNotFound: If the organization doesn't exist
    """
    # Check if user has permission (only sysadmins)
    if not toolkit.check_access('sysadmin', context, data_dict):
        raise toolkit.NotAuthorized('Only system administrators can perform this action')

    org_id = toolkit.get_or_bust(data_dict, 'org_id')
    sub = toolkit.get_or_bust(data_dict, 'sub')

    # Get organization with users
    try:
        org = toolkit.get_action('organization_show')(
            {'ignore_auth': True},
            {
                'id': org_id,
                'include_users': True
            }
        )
    except toolkit.ObjectNotFound:
        raise toolkit.ValidationError('Organization not found')

    result = {
        'is_member': False,
        'capacity': None,
        'user_found': False
    }

    if 'users' in org:
        for user in org['users']:
            try:
                user_info = toolkit.get_action('user_show')(
                    {'ignore_auth': True},
                    {
                        'id': user['id'],
                        'include_plugin_extras': True
                    }
                )

                # Check if this user has matching sub in plugin_extras
                plugin_extras = user_info.get('plugin_extras', {})
                if plugin_extras.get('sub') == sub:
                    result['is_member'] = True
                    result['capacity'] = user.get('capacity')
                    result['user_found'] = True
                    break

            except toolkit.ObjectNotFound:
                continue
            except Exception as e:
                log.error(f"Unexpected error while checking user {user.get('id')}: {str(e)}")
                continue

    return result


def organization_member_create_custom(context, data_dict):
    '''
    Custom organization_member_create που παρακάμπτει το member creation
    αν το username είναι ο default user
    '''

    # Ελέγχουμε αν είναι ο default site user
    username = data_dict.get('username', '')

    if username == 'default':
        # Έλεγχος αν έρχεται από notification context
        if context.get('skip_member_creation'):
            log.info(f"Skipping member creation for default user in notification context")
            # Επιστρέφουμε fake membership response
            return {
                'group_id': data_dict.get('id'),
                'table_name': 'user',
                'table_id': 'default',  # Hardcoded default
                'capacity': data_dict.get('role', 'member'),
                'state': 'notification_sent'  # Custom state
            }

    # Κανονικό member creation για άλλες περιπτώσεις
    from ckan.logic.action.create import organization_member_create as original_org_member_create
    return original_org_member_create(context, data_dict)

def user_invite_notify(context, data_dict):
    '''
    Αντικατάσταση του user_invite - στέλνει μόνο email ειδοποίηση.
    '''

    # Χρησιμοποιούμε την αρχική authorization του user_invite
    toolkit.check_access('user_invite', context, data_dict)

    # Χρησιμοποιούμε το αρχικό schema για συμβατότητα
    schema = context.get('schema', ckan.logic.schema.default_user_invite_schema())

    data, errors = _validate(data_dict, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)

    # Παίρνουμε πληροφορίες για την ομάδα/οργανισμό
    model = context['model']
    group = model.Group.get(data['group_id'])
    if not group:
        raise toolkit.ObjectNotFound(_('Group not found'))

    # Παίρνουμε πληροφορίες για τον οργανισμό
    org_or_group = 'organization' if group.is_organization else 'group'
    group_dict = toolkit.get_action(f'{org_or_group}_show')(
        context, {'id': data['group_id']}
    )

    # Στέλνουμε το email ειδοποίησης
    try:
        _send_registration_notification_email(
            recipient_email=data['email'],
            group_dict=group_dict,
            role=data['role']
        )

        # Προσθήκη flash message
        h.flash_success(_('Email ειδοποίησης στάλθηκε επιτυχώς στο {0}').format(data['email']))

        # Σηματοδοτούμε ότι δεν θέλουμε member creation
        context['skip_member_creation'] = True

        # Παίρνουμε τον default site user
        site_user = toolkit.get_action('get_site_user')({'ignore_auth': True}, {})

        # Επιστρέφουμε τον site user με custom state
        site_user['state'] = 'notification_sent'
        site_user['target_email'] = data['email']  # Κρατάμε το πραγματικό email

        return site_user

    except Exception as error:
        # Όμοια με το αρχικό user_invite, πετάμε ValidationError
        message = _('Error sending registration notification email: {0}').format(error)
        raise toolkit.ValidationError(message)

def _send_registration_notification_email(recipient_email, group_dict, role):
    '''Στέλνει το email ειδοποίησης'''

    # Πληροφορίες οργανισμού
    org_name = group_dict.get('display_name', group_dict.get('title', group_dict.get('name', '')))
    org_type = 'οργανισμό' if group_dict.get('is_organization') else 'ομάδα'
    contact_email = _get_organization_contact_email(group_dict)
    org_url = group_dict.get('url', '')

    # Role στα ελληνικά
    role_translations = {
        'member': 'μέλος',
        'editor': 'συντάκτης',
        'admin': 'διαχειριστής'
    }
    role_gr = role_translations.get(role, role)

    # Template variables
    extra_vars = {
        'org_name': org_name,
        'org_type': org_type,
        'contact_email': contact_email,
        'org_url': org_url,
        'role_gr': role_gr
    }

    subject = f"Πρόσκληση εγγραφής στην πλατφόρμα DATA.GOV.GR - {org_name}"

    # Render το template
    body = toolkit.render('emails/user_invite_notification.txt', extra_vars)

    mail_recipient(
        recipient_name="",
        recipient_email=recipient_email,
        subject=subject,
        body=body
    )

def _get_organization_contact_email(group_dict):
    '''Παίρνει το contact email του οργανισμού αν υπάρχει'''
    return group_dict.get('email')

def check_user_org_permission(context: Dict[str, Any], data_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ελέγχει αν ένας χρήστης (μέσω API token) έχει δικαιώματα εκδότη σε έναν οργανισμό.

    :param organization_id: Το ID του οργανισμού
    :type organization_id: string

    :returns: Αποτέλεσμα ελέγχου εξουσιοδότησης
    :rtype: dictionary
    """

    # Validation
    organization_id = data_dict.get('organization_id')
    if not organization_id:
        raise toolkit.ValidationError({'organization_id': ['Organization ID is required']})

    # Παίρνουμε το token από το Authorization header
    token = request.headers.get('Authorization', '')
    if not token:
        return {
            'success': False,
            'message': 'No authorization token provided',
            'user_id': None
        }

    # Επικύρωση token και λήψη χρήστη
    user = get_user_from_token(token)
    if not user:
        return {
            'success': False,
            'message': 'Invalid or expired token',
            'user_id': None
        }

    # Έλεγχος ύπαρξης οργανισμού
    try:
        toolkit.get_action('organization_show')(
            {'ignore_auth': True},
            {'id': organization_id}
        )
    except toolkit.ObjectNotFound:
        return {
            'success': False,
            'message': 'Organization not found',
            'user_id': user.id
        }

    # Έλεγχος δικαιωμάτων εκδότη στον οργανισμό
    has_permission = authz._has_user_permission_for_groups(
        user.id,
        'create_dataset',  # Permission που χρειάζεται ένας εκδότης
        [organization_id],
        capacity='editor'  # Ελέγχουμε συγκεκριμένα για editor role
    ) or authz._has_user_permission_for_groups(
        user.id,
        'create_dataset',
        [organization_id],
        capacity='admin'  # Ή admin role
    )

    return {
        'success': has_permission,
        'message': 'User authorized' if has_permission else 'User not authorized for this organization',
        'user_id': user.id,
        'username': user.name,
        'organization_id': organization_id
    }
