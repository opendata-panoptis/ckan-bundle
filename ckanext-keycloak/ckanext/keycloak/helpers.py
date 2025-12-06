import logging
import string
import re
import random
import secrets


import ckan.model as model
import ckan.plugins.toolkit as tk
from os import environ


log = logging.getLogger(__name__)


def generate_password():
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(8))


def ensure_unique_username_from_email(email):
    localpart = email.split('@')[0]
    cleaned_localpart = re.sub(r'[^\w]', '-', localpart).lower()

    if not model.User.get(cleaned_localpart):
        return cleaned_localpart

    max_name_creation_attempts = 10

    for _ in range(max_name_creation_attempts):
        random_number = random.SystemRandom().random() * 10000
        name = '%s-%d' % (cleaned_localpart, random_number)
        if not model.User.get(name):
            return name

    return cleaned_localpart


def process_user(userinfo):
    """
    Κύρια μέθοδος επεξεργασίας χρήστη από Keycloak SSO.

    Αυτή η μέθοδος είναι υπεύθυνη για τη διαχείριση των χρηστών που συνδέονται μέσω Keycloak.
    Ελέγχει αν ο χρήστης υπάρχει ήδη στο σύστημα με βάση το Keycloak subject identifier (sub)
    και είτε ενημερώνει τον υπάρχοντα χρήστη είτε δημιουργεί νέο.

    Args:
        userinfo (dict): Λεξικό με τα στοιχεία του χρήστη από το Keycloak που περιέχει:
            - name (str): Το preferred_username από το Keycloak
            - email (str): Το email του χρήστη
            - fullname (str): Το πλήρες όνομα του χρήστη
            - plugin_extras (dict): Επιπλέον μεταδεδομένα που περιέχουν:
                - sub (str): Το μοναδικό Keycloak subject identifier
            - password (str): Αυτόματα δημιουργημένος κωδικός

    Returns:
        User: Το αντικείμενο του χρήστη (νέο ή ενημερωμένο) από το CKAN model

    """

    # Ελέγχουμε μόνο αν υπάρχει χρήστης με το ίδιο sub
    sub = userinfo.get('plugin_extras', {}).get('sub')
    if sub:
        existing_user = _get_user_by_sub(sub)
        if existing_user:
            # Ενημερώνουμε τον υπάρχοντα χρήστη με τα νέα στοιχεία
            return _update_user(existing_user, userinfo)

    # Αν δεν βρεθεί χρήστης με το ίδιο sub, δημιουργούμε νέο
    return _create_user(userinfo)


def _get_user_by_email(email):
    user = model.User.by_email(email)
    if user and isinstance(user, list):
        user = user[0]

    activate_user_if_deleted(user)
    
    return user


def _get_user_by_sub(sub):
    """
    Εύρεση χρήστη με βάση το sub στο plugin_extras
    """
    try:
        users = model.Session.query(model.User).filter(
            model.User.plugin_extras.contains({'sub': sub})
        ).all()

        if users:
            user = users[0]
            activate_user_if_deleted(user)
            return user
    except Exception as e:
        log.error(f"Error searching for user by sub: {e}")

    return None


def activate_user_if_deleted(user):
    u'''Reactivates deleted user.'''
    if not user:
        return
    if user.is_deleted():
        user.activate()
        user.commit()
        log.info(u'User {} reactivated'.format(user.name))

def _has_user_data_changed(user, userinfo):
    """
    Ελέγχει αν έχουν αλλάξει τα βασικά δεδομένα του χρήστη
    """
    # Ελέγχουμε το name (από preferred_username)
    if userinfo.get('name') and userinfo['name'] != user.name:
        return True

    # Ελέγχουμε το email
    if userinfo.get('email') and userinfo['email'] != user.email:
        return True

    # Ελέγχουμε το fullname
    if userinfo.get('fullname') and userinfo['fullname'] != user.fullname:
        return True

    return False

def _update_user(user, userinfo):
    """
    Ενημέρωση υπάρχοντος χρήστη με νέα στοιχεία από το Keycloak
    """

    # Ελέγχουμε αν έχουν αλλάξει τα βασικά δεδομένα
    if not _has_user_data_changed(user, userinfo):
        log.info(f'No changes detected for user {user.name}, skipping update')
        activate_user_if_deleted(user)
        return user

    try:

        # Ειδικός χειρισμός για το name - άμεση ενημέρωση στη βάση
        new_name = userinfo.get('name')
        if new_name and new_name != user.name:
            # Ελέγχουμε αν το νέο name είναι διαθέσιμο
            if not model.User.get(new_name):
                old_name = user.name
                user.name = new_name
                model.Session.add(user)
                model.Session.commit()
                log.info(f'Username updated from {old_name} to {new_name}')
            else:
                # Αν το username υπάρχει, δημιουργούμε ένα μοναδικό από το email
                if userinfo.get('email'):
                    unique_name = ensure_unique_username_from_email(userinfo['email'])
                    old_name = user.name
                    user.name = unique_name
                    model.Session.add(user)
                    model.Session.commit()
                    log.info(f'Username {new_name} already exists. Updated from {old_name} to {unique_name}')
                else:
                    log.warning(
                        f'Cannot update username to {new_name} - already exists and no email available for fallback')

        context = {
            'ignore_auth': True,
        }

        # Προετοιμάζουμε τα δεδομένα για ενημέρωση
        patch_data = {'id': user.id}

        # Στέλνουμε πάντα το email αν υπάρχει στο userinfo
        if userinfo.get('email'):
            patch_data['email'] = userinfo['email']
            if userinfo['email'] != user.email:
                log.debug(f'Email will be patched from {user.email} to {userinfo["email"]}')

        if userinfo.get('fullname') and userinfo['fullname'] != user.fullname:
            patch_data['fullname'] = userinfo['fullname']

        # Εκτελούμε την ενημέρωση μόνο αν έχουμε κάτι άλλο εκτός από το id
        if len(patch_data) > 1:
            log.debug(f'Calling user_patch with data: {patch_data}')
            tk.get_action('user_patch')(context, patch_data)
            changed_fields = [k for k in patch_data.keys() if k != 'id']
            log.info(f'User {user.name} updated with Keycloak data: {changed_fields}')

        # Επιστρέφουμε τον ενημερωμένο χρήστη
        updated_user = model.User.get(user.id)
        activate_user_if_deleted(updated_user)
        return updated_user

    except Exception as e:
        log.error(f"Error updating user {user.name}: {e}")
        # Αν αποτύχει η ενημέρωση, επιστρέφουμε τον αρχικό χρήστη
        return user

def _create_user(userinfo):
    context = {
        u'ignore_auth': True,
    }
    created_user_dict = tk.get_action(
        u'user_create'
    )(context, userinfo)

    sub = userinfo.get('plugin_extras', {}).get('sub')
    return _get_user_by_sub(sub)


def button_style():

    return tk.config.get('ckanext.keycloak.button_style',
                         environ.get('CKANEXT__KEYCLOAK__BUTTON_STYLE'))


def enable_internal_login():

    return tk.asbool(tk.config.get(
        'ckanext.keycloak.enable_ckan_internal_login',
        environ.get('CKANEXT__KEYCLOAK__CKAN_INTERNAL_LOGIN')))

def force_keycloak_reauth():
    """Επιστρέφει True αν πρέπει να γίνεται πάντα επαναπιστοποίηση"""
    return tk.asbool(tk.config.get(
        'ckanext.keycloak.force_reauth',
        environ.get('CKANEXT__KEYCLOAK__FORCE_REAUTH', False)))
