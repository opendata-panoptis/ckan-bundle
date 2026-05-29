import ckan.plugins.toolkit as toolkit
import ckan.model as model

from ckanext.showcase.model import ShowcaseAdmin

import logging
from ..utils import is_user_creator_of_showcase
from ..utils import read_showcase
from flask import g

log = logging.getLogger(__name__)


def get_auth_functions():
    return {
        'ckanext_showcase_create': create,
        'ckanext_showcase_update': update,
        'ckanext_showcase_delete': delete,
        'ckanext_showcase_show': show,
        'ckanext_showcase_list': showcase_list,
        'ckanext_showcase_package_association_create': package_association_create,
        'ckanext_showcase_package_association_delete': package_association_delete,
        'ckanext_showcase_package_list': showcase_package_list,
        'ckanext_package_showcase_list': package_showcase_list,
        'ckanext_showcase_admin_add': add_showcase_admin,
        'ckanext_showcase_admin_remove': remove_showcase_admin,
        'ckanext_showcase_admin_list': showcase_admin_list,
        'ckanext_showcase_upload': showcase_upload,
    }


def _is_showcase_admin(context):
    '''
    Determines whether user in context can act as a showcase admin.
    '''
    user = context.get('user', '')
    userobj = model.User.get(user)
    if userobj == None:
        return False
    if getattr(userobj, 'sysadmin', False):
        return True
    return ShowcaseAdmin.is_user_showcase_admin(userobj)


def _get_logged_user_id(context):
    if '_showcase_logged_user_id' in context:
        return context['_showcase_logged_user_id']

    user_id = getattr(getattr(g, 'userobj', None), 'id', None)
    if user_id:
        context['_showcase_logged_user_id'] = user_id
        return user_id

    user = context.get('user', '')
    userobj = model.User.get(user)
    user_id = getattr(userobj, 'id', None)
    context['_showcase_logged_user_id'] = user_id
    return user_id


def create(context, data_dict):
    """
    Custom auth function που θα επιτρέψει δημιουργία showcases αν υπάρχει συνδεδεμένος χρήστης
    """
    user = context.get('user')
    # Επιτρέπουμε όλους τους εγγεγραμμένους χρήστες
    if user:
        return {'success': True}

    # Αν είναι ο ιδιοκτήτης του συνόλου δεδομένων τοτε επέστρεψε true
    logged_user_id = _get_logged_user_id(context)
    if not logged_user_id:
        return {'success': False, 'msg': 'You must be logged in to create showcases'}
    is_logged_user_creator_of_showcase = is_user_creator_of_showcase(data_dict, logged_user_id)
    if is_logged_user_creator_of_showcase:
        return {'success': True}


    # Εμφανίζουμε σφάλμα όταν είναι ανώνυμος χρήστης
    return {'success': False, 'msg': 'You must be logged in to create showcases'}


def delete(context, data_dict):
    '''Delete a Showcase.

       Only sysadmin or users listed as Showcase Admins can delete a Showcase.
    '''
    return {'success': _is_showcase_admin(context)}


def update(context, data_dict):
    '''Update a Showcase.

       Μόνο αν ο συνδεδεμένος χρήστης είναι
       1. Διαχειριστές συστήματος
       2. Διαχειριστές εφαρμογών μπορούν να κάνουν update ένα showcase
       3. Δημιουργός της εφαρμογής
    '''
    # Ανάκτηση id showcase και αντικειμένου
    showcase_id = data_dict['id']
    showcase_data_dict = read_showcase(showcase_id, context)

    # αν είναι approved το showcase μην επιτρέπεις επεξεργασία
    approved = False
    if 'approval_status' in showcase_data_dict and showcase_data_dict['approval_status'] == 'approved':
        approved = True

    # Αν είναι ο ιδιοκτήτης του συνόλου δεδομένων και το showcase είναι σε κατάσταση διαφορετική από approved τότε επέστρεψε true
    logged_user_id = _get_logged_user_id(context)
    if not logged_user_id:
        return {'success': False}
    is_logged_user_creator_of_showcase = is_user_creator_of_showcase(showcase_data_dict, logged_user_id)
    if is_logged_user_creator_of_showcase and not approved:
        return {'success': True}

    # Αν δεν είναι ο ιδιοκτήτης και το showcase δεν είναι σε κατάσταση διαφορετική από approved,
    #   επέστρεψε true μόνο αν είναι admin
    return {'success': _is_showcase_admin(context)}


def _can_manage_package_association(context, showcase_id):
    if _is_showcase_admin(context):
        return True

    if not showcase_id:
        return False

    showcase_data_dict = read_showcase(showcase_id, context)
    approved = showcase_data_dict.get('approval_status') == 'approved'

    logged_user_id = _get_logged_user_id(context)
    if not logged_user_id:
        return False

    is_logged_user_creator_of_showcase = is_user_creator_of_showcase(
        showcase_data_dict,
        logged_user_id,
    )
    return is_logged_user_creator_of_showcase and not approved


@toolkit.auth_allow_anonymous_access
def show(context, data_dict):
    '''All users can access a showcase show'''
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def showcase_list(context, data_dict):
    '''All users can access a showcase list'''
    return {'success': True}


def package_association_create(context, data_dict):
    '''Create a package showcase association.

       Μόνο αν ο συνδεδεμένος χρήστης είναι
       1. Διαχειριστές συστήματος
       2. Διαχειριστές εφαρμογών μπορούν να κάνουν update ένα showcase
       3. Δημιουργός της εφαρμογής
    '''

    showcase_id = data_dict.get('showcase_id')
    return {'success': _can_manage_package_association(context, showcase_id)}


def package_association_delete(context, data_dict):
    '''Delete a package showcase association.

       Showcase admins μπορούν πάντα να αφαιρούν association.
       Ο creator μπορεί μόνο όσο το showcase δεν είναι approved.
    '''
    showcase_id = data_dict.get('showcase_id')
    return {'success': _can_manage_package_association(context, showcase_id)}


@toolkit.auth_allow_anonymous_access
def showcase_package_list(context, data_dict):
    '''All users can access a showcase's package list'''
    return {'success': True}


@toolkit.auth_allow_anonymous_access
def package_showcase_list(context, data_dict):
    '''All users can access a packages's showcase list'''
    return {'success': True}


def add_showcase_admin(context, data_dict):
    '''Only sysadmins can add users to showcase admin list.'''
    return {'success': False}


def remove_showcase_admin(context, data_dict):
    '''Only sysadmins can remove users from showcase admin list.'''
    return {'success': False}


def showcase_admin_list(context, data_dict):
    '''Only sysadmins can list showcase admin users.'''
    return {'success': False}


def showcase_upload(context, data_dict):
    '''Only sysadmins can upload images.'''
    return {'success': _is_showcase_admin(context)}
