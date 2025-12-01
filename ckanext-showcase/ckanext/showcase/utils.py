# -*- coding: utf-8 -*-

from __future__ import print_function
import json
import logging

from collections import OrderedDict
from urllib.parse import urlencode

import ckan.model as model
import ckan.plugins as p
import ckan.logic as logic
import ckan.lib.navl.dictization_functions as dict_fns
import ckan.lib.helpers as h
import ckan.plugins.toolkit as tk
from ckanext.showcase.model import ShowcasePackageAssociation
from flask import g

_ = tk._
abort = tk.abort

log = logging.getLogger(__name__)
DATASET_TYPE_NAME = 'showcase'


def check_edit_view_auth(id):
    context = {
        'model': model,
        'session': model.Session,
        'user': tk.g.user or tk.g.author,
        'auth_user_obj': tk.g.userobj,
        'save': 'save' in tk.request.args,
        'pending': True
    }

    data_dict = {'id': id}
    try:
        tk.check_access('ckanext_showcase_update', context, data_dict)
    except tk.NotAuthorized:
        return tk.abort(
            401,
            _('User not authorized to edit {showcase_id}').format(
                showcase_id=id))


def check_new_view_auth():
    context = {
        'model': model,
        'session': model.Session,
        'user': tk.g.user or tk.g.author,
        'auth_user_obj': tk.g.userobj,
        'save': 'save' in tk.request.args
    }

    # Check access here, then continue with PackageController.new()
    # PackageController.new will also check access for package_create.
    # This is okay for now, while only sysadmins can create Showcases, but
    # may not work if we allow other users to create Showcases, who don't
    # have access to create dataset package types. Same for edit below.
    try:
        tk.check_access('ckanext_showcase_create', context)
    except tk.NotAuthorized:
        return tk.abort(401, _('Unauthorized to create a package'))


def read_view(id):
    context = {
        'model': model,
        'session': model.Session,
        'user': tk.g.user or tk.g.author,
        'for_view': True,
        'auth_user_obj': tk.g.userobj
    }
    pkg_dict = read_showcase(id, context)

    # get showcase packages
    showcase_pkgs = tk.get_action('ckanext_showcase_package_list')(
        context, {
            'showcase_id': pkg_dict['id']
        })

    package_type = DATASET_TYPE_NAME
    return tk.render('showcase/read.html',
                     extra_vars={'dataset_type': package_type,
                                 'pkg_dict': pkg_dict,
                                 'showcase_pkgs': showcase_pkgs})

def read_showcase(id, context):

    data_dict = {'id': id}


    pkg_dict = None

    # check if showcase exists
    try:
        context['ignore_auth'] = True
        pkg_dict = tk.get_action('ckanext_showcase_show')(context, data_dict)
    except tk.ObjectNotFound:
        return tk.abort(404, _('Showcase not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _('Unauthorized to read showcase'))

    # Ένδειξη αν ο συνδεδεμένος χρήστης είναι ο ιδιοκτήτης του συνόλου δεδομένων
    is_logged_user_creator_of_showcase = None

    is_logged_user_creator_of_showcase = (
        is_user_creator_of_showcase(pkg_dict, getattr(g.userobj, "id", None))
        if g.userobj not in (None, "") else False
    )

    from ckanext.showcase.logic.auth import _is_showcase_admin


    if (not is_sysadmin(tk.g.user) and # Διαχειριστή Συστήματος
            not _is_showcase_admin(context) and # Διαχειριστής Εφαρμογών
            not is_showcase_approved(pkg_dict) and # Η εφαρμογή δεν είναι εγκεκριμένη
            not is_logged_user_creator_of_showcase): # Ο Συνδεδεμένος Χρήστης δεν είναι δημιουργός της εφαρμογής
        return tk.abort(403, _('You do not have access to this showcase'))

    return pkg_dict


def is_user_creator_of_showcase(pkg_dict, logged_user_id):
    """
    Checks if the logged-in user is the creator of the showcase.

    Args:
        pkg_dict (dict): Dictionary containing showcase details.
        logged_user_id (str or int): ID of the currently logged-in user.

    Returns:
        bool: True if the logged-in user is the creator, False otherwise.
    """
    showcase_creator_id = pkg_dict.get('creator_user_id')

    if showcase_creator_id == logged_user_id:
        print("The logged-in user is the creator of this dataset.")
        return True
    else:
        print("The logged-in user is not the creator.")
        return False


def manage_datasets_view(id):

    context = {
        'model': model,
        'session': model.Session,
        'user': tk.g.user or tk.g.author
    }

    data_dict = read_showcase(id, context)

    try:
        tk.check_access('ckanext_showcase_update', context, data_dict)
    except tk.NotAuthorized:
        return tk.abort(
            401,
            _('User not authorized to edit {showcase_id}').format(
                showcase_id=id))

    # check if showcase exists
    try:
        pkg_dict = tk.get_action('package_show')(context, data_dict)
    except tk.ObjectNotFound:
        return tk.abort(404, _('Showcase not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _('Unauthorized to read showcase'))

    form_data = tk.request.form
    manage_route = 'showcase_blueprint.manage_datasets'

    # Αν γίνεται διαγραφή της συσχέτισης showcase-dataset
    if (tk.request.method == 'POST' and 'bulk_action.showcase_remove' in form_data):

        # Αν η εφαρμογή είναι εγκεκριμένη δεν επιτρέπεται η αφαίρεση σύνδεσης
        if is_showcase_approved(pkg_dict):
            return tk.abort(403, _('Removing the relation of an approved showcase is not permitted.'))

        # Find the datasets to perform the action on, they are prefixed by
        # dataset_ in the form data
        dataset_ids = []
        for param in form_data:
            if param.startswith('dataset_'):
                dataset_ids.append(param[8:])
        if dataset_ids:
            for dataset_id in dataset_ids:
                tk.get_action('ckanext_showcase_package_association_delete')(
                    context, {
                        'showcase_id': pkg_dict['id'],
                        'package_id': dataset_id
                    })
            h.flash_success(
                tk.ungettext(
                    "The dataset has been removed from the showcase.",
                    "The datasets have been removed from the showcase.",
                    len(dataset_ids)))
            url = h.url_for(manage_route, id=id)
            return h.redirect_to(url)

    # Are we creating a showcase/dataset association?
    elif (tk.request.method == 'POST'
          and 'bulk_action.showcase_add' in form_data):

        if is_showcase_approved(pkg_dict):
            return tk.abort(403, _('Adding relation of an approved showcase is not permitted.'))

        # Find the datasets to perform the action on, they are prefixed by
        # dataset_ in the form data
        dataset_ids = []
        for param in form_data:
            if param.startswith('dataset_'):
                dataset_ids.append(param[8:])
        if dataset_ids:
            successful_adds = []
            for dataset_id in dataset_ids:
                try:
                    tk.get_action(
                        'ckanext_showcase_package_association_create')(
                            context, {
                                'showcase_id': pkg_dict['id'],
                                'package_id': dataset_id
                            })
                except tk.ValidationError as e:
                    h.flash_notice(e.error_summary)
                else:
                    successful_adds.append(dataset_id)
            if successful_adds:
                h.flash_success(
                    tk.ungettext(
                        "The dataset has been added to the showcase.",
                        "The datasets have been added to the showcase.",
                        len(successful_adds)))
            url = h.url_for(manage_route, id=id)
            return h.redirect_to(url)

    extra_vars = _add_dataset_search(pkg_dict['id'], pkg_dict['name'])

    # get showcase packages
    showcase_pkgs = tk.get_action('ckanext_showcase_package_list')(
        context, {
            'showcase_id': pkg_dict['id']
        })

    extra_vars['pkg_dict'] = pkg_dict
    extra_vars['showcase_pkgs'] = showcase_pkgs

    return tk.render('showcase/manage_datasets.html',
                     extra_vars=extra_vars)


def _add_dataset_search(showcase_id, showcase_name):
    '''
    Search logic for discovering datasets to add to a showcase.
    '''

    from ckan.lib.search import SearchError

    package_type = 'dataset'
    extra_vars = {}

    # unicode format (decoded from utf8)
    extra_vars['q'] = q = tk.request.args.get('q', '')
    extra_vars['query_error'] = False
    page = h.get_page_number(tk.request.args)

    limit = int(tk.config.get('ckan.datasets_per_page', 20))

    # most search operations should reset the page counter:
    params_nopage = [(k, v) for k, v in tk.request.args.items()
                     if k != 'page']

    def remove_field(key, value=None, replace=None):
        return h.remove_url_param(key,
                                  value=value,
                                  replace=replace,
                                  controller='dataset' if
                                  tk.check_ckan_version('2.9') else 'package',
                                  action='search')

    extra_vars['remove_field'] = remove_field

    sort_by = tk.request.args.get('sort', None)
    params_nosort = [(k, v) for k, v in params_nopage if k != 'sort']

    def _sort_by(fields):
        """
        Sort by the given list of fields.

        Each entry in the list is a 2-tuple: (fieldname, sort_order)

        eg - [('metadata_modified', 'desc'), ('name', 'asc')]

        If fields is empty, then the default ordering is used.
        """
        params = params_nosort[:]

        if fields:
            sort_string = ', '.join('%s %s' % f for f in fields)
            params.append(('sort', sort_string))
        return _search_url(params, showcase_name)

    extra_vars['sort_by'] = _sort_by
    if sort_by is None:
        extra_vars['sort_by_fields'] = []
    else:
        extra_vars['sort_by_fields'] = [field.split()[0] for field in sort_by.split(',')]

    def pager_url(q=None, page=None):
        params = list(params_nopage)
        params.append(('page', page))
        return _search_url(params, showcase_name)

    extra_vars['search_url_params'] = urlencode(_encode_params(params_nopage))

    try:
        fields = []
        # fields_grouped will contain a dict of params containing
        # a list of values eg {'tags':['tag1', 'tag2']}
        fields_grouped = {}
        search_extras = {}
        fq = ''
        for (param, value) in tk.request.args.items():
            if param not in ['q', 'page', 'sort'] \
                    and len(value) and not param.startswith('_'):
                if not param.startswith('ext_'):
                    fields.append((param, value))
                    fq += ' %s:"%s"' % (param, value)
                    if param not in fields_grouped:
                        fields_grouped[param] = [value]
                    else:
                        fields_grouped[param].append(value)
                else:
                    search_extras[param] = value

        extra_vars['fields'] = fields
        extra_vars['fields_grouped'] = fields_grouped

        context = {
            'model': model,
            'session': model.Session,
            'user': tk.g.user or tk.g.author,
            'for_view': True,
            'auth_user_obj': tk.g.userobj
        }

        # Unless changed via config options, don't show other dataset
        # types any search page. Potential alternatives are do show them
        # on the default search page (dataset) or on one other search page
        search_all_type = tk.config.get('ckan.search.show_all_types')
        search_all = False

        try:
            # If the "type" is set to True or False, convert to bool
            # and we know that no type was specified, so use traditional
            # behaviour of applying this only to dataset type
            search_all = tk.asbool(search_all_type)
            search_all_type = 'dataset'
        # Otherwise we treat as a string representing a type
        except ValueError:
            search_all = True

        if not search_all or package_type != search_all_type:
            # Only show datasets of this particular type
            fq += ' +dataset_type:{type}'.format(type=package_type)

        # Only search for packages that aren't already associated with the
        # Showcase
        associated_package_ids = ShowcasePackageAssociation.get_package_ids_for_showcase(
            showcase_id)
        # flatten resulting list to space separated string
        if associated_package_ids:
            associated_package_ids_str = \
                ' OR '.join([id[0] for id in associated_package_ids])
            fq += ' !id:({0})'.format(associated_package_ids_str)

        facets = OrderedDict()

        default_facet_titles = {
            'organization': _('Organizations'),
            'groups': _('Groups'),
            'tags': _('Tags'),
            'res_format': _('Formats'),
            'license_id': _('Licenses'),
        }

        # for CKAN-Versions that do not provide the facets-method from
        # helper-context, import facets from ckan.common
        if hasattr(h, 'facets'):
            current_facets = h.facets()
        else:
            from ckan.common import g
            current_facets = g.facets

        for facet in current_facets:
            if facet in default_facet_titles:
                facets[facet] = default_facet_titles[facet]
            else:
                facets[facet] = facet

        # Facet titles
        for plugin in p.PluginImplementations(p.IFacets):
            facets = plugin.dataset_facets(facets, package_type)

        extra_vars['facet_titles'] = facets

        data_dict = {
            'q': q,
            'fq': fq.strip(),
            'facet.field': list(facets.keys()),
            'rows': limit,
            'start': (page - 1) * limit,
            'sort': sort_by,
            'extras': search_extras
        }

        query = tk.get_action('package_search')(context, data_dict)
        extra_vars['sort_by_selected'] = query['sort']

        extra_vars['page'] = h.Page(collection=query['results'],
                        page=page,
                        url=pager_url,
                        item_count=query['count'],
                        items_per_page=limit)
        extra_vars['facets'] = query['facets']
        extra_vars['search_facets'] = query['search_facets']
        extra_vars['page.items'] = query['results']
    except SearchError as se:
        log.error('Dataset search error: %r', se.args)
        extra_vars['query_error'] = True
        extra_vars['facets'] = {}
        extra_vars['search_facets'] = {}
        extra_vars['page'] = h.Page(collection=[])
    extra_vars['search_facets_limits'] = {}
    for facet in extra_vars['search_facets'].keys():
        try:
            limit = int(
                tk.request.args.get(
                    '_%s_limit' % facet,
                    int(tk.config.get('search.facets.default', 10))))
        except tk.ValueError:
            abort(
                400,
                _("Parameter '{parameter_name}' is not an integer").format(
                    parameter_name='_%s_limit' % facet))
        extra_vars['search_facets_limits'][facet] = limit
    return extra_vars


def _search_url(params, name):
    url = h.url_for('showcase_blueprint.manage_datasets', id=name)
    return url_with_params(url, params)


def _encode_params(params):
    return [(k, str(v)) for k, v in params]


def url_with_params(url, params):
    params = _encode_params(params)
    return url + '?' + urlencode(params)


def delete_view(id):
    if 'cancel' in tk.request.args:
        tk.redirect_to('showcase_blueprint.edit', id=id)

    context = {
        'model': model,
        'session': model.Session,
        'user': tk.g.user or tk.g.author,
        'auth_user_obj': tk.g.userobj
    }

    try:
        tk.check_access('ckanext_showcase_delete', context, {'id': id})
    except tk.NotAuthorized:
        return tk.abort(401, _('Unauthorized to delete showcase'))

    index_route = 'showcase_blueprint.index'

    context = {'user': tk.g.user}
    try:
        if tk.request.method == 'POST':
            tk.get_action('ckanext_showcase_delete')(context, {'id': id})
            h.flash_notice(_('Showcase has been deleted.'))
            return tk.redirect_to(index_route)
        pkg_dict = tk.get_action('package_show')(context, {'id': id})
    except tk.NotAuthorized:
        tk.abort(401, _('Unauthorized to delete showcase'))
    except tk.ObjectNotFound:
        tk.abort(404, _('Showcase not found'))

    return tk.render('showcase/confirm_delete.html',
                     extra_vars={'dataset_type': DATASET_TYPE_NAME,
                                 'pkg_dict': pkg_dict})

# Ανάκτητη των showcases με βάση το id του συνόλου δεδομένων
def dataset_showcase_list(id):
    context = {
        'model': model,
        'session': model.Session,
        'user': tk.g.user or tk.g.author,
        'for_view': True,
        'auth_user_obj': tk.g.userobj
    }
    data_dict = {'id': id}

    try:
        tk.check_access('package_show', context, data_dict)
    except tk.ObjectNotFound:
        return tk.abort(404, _('Dataset not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _('Not authorized to see this page'))

    try:
        pkg_dict = tk.get_action('package_show')(context, data_dict)
        showcase_list = tk.get_action('ckanext_package_showcase_list')(
            context, {
                'package_id': pkg_dict['id']
            })
    except tk.ObjectNotFound:
        return tk.abort(404, _('Dataset not found'))
    except tk.NotAuthorized:
        return tk.abort(401, _('Unauthorized to read package'))

    list_route = 'showcase_blueprint.dataset_showcase_list'

    if tk.request.method == 'POST':
        # Are we adding the dataset to a showcase?
        form_data = tk.request.form

        showcase_id = form_data.get('showcase_added')

        showcase = read_showcase(showcase_id, context)

        # Δεν μπορεί να γίνει αφαίρεση διασύνδεσης μη εγκεκριμένης εφαρμογής με σύνολο δεδομένων.
        if not showcase['approval_status'] == 'approved':
            return tk.abort(403, _('Δεν μπορεί να γίνει αφαίρεση διασύνδεσης μη εγκεκριμένης εφαρμογής με σύνολο δεδομένων.'))


        if showcase_id:
            data_dict = {
                "showcase_id": showcase_id,
                "package_id": pkg_dict['id']
            }
            try:
                tk.get_action('ckanext_showcase_package_association_create')(
                    context, data_dict)
            except tk.ObjectNotFound:
                return tk.abort(404, _('Showcase not found'))
            else:
                h.flash_success(
                    _("The dataset has been added to the showcase."))

        # Are we removing a dataset from a showcase?
        showcase_to_remove = form_data.get('remove_showcase_id')
        if showcase_to_remove:
            data_dict = {
                "showcase_id": showcase_to_remove,
                "package_id": pkg_dict['id']
            }
            try:
                tk.get_action('ckanext_showcase_package_association_delete')(
                    context, data_dict)
            except tk.ObjectNotFound:
                return tk.abort(404, _('Showcase not found'))
            else:
                h.flash_success(
                    _("The dataset has been removed from the showcase."))
        return h.redirect_to(
            h.url_for(list_route, id=pkg_dict['name']))

    pkg_showcase_ids = [showcase['id'] for showcase in showcase_list]
    site_showcases = tk.get_action('ckanext_showcase_list')(context, {})


    showcase_dropdown = [
        [showcase['id'], showcase['title']]
        for showcase in site_showcases
            # Φιλτράρουμε τα showcases τα οποία
            # 1. δεν είναι ηδη στο σύνολο δεδομένων
            # 2. Είναι εγκεκριμένα
            if showcase['id'] not in pkg_showcase_ids and any(
                extra['key'] == 'approval_status' and extra['value'] == 'approved'
                for extra in showcase.get('extras', [])
            )

    ]

    extra_vars = {
        'pkg_dict': pkg_dict,
        'showcase_dropdown': showcase_dropdown,
        'showcase_list': showcase_list,
    }

    return tk.render("package/dataset_showcase_list.html",
                     extra_vars=extra_vars)


def manage_showcase_admins():
    context = {
        'model': model,
        'session': model.Session,
        'user': tk.g.user or tk.g.author
    }

    try:
        tk.check_access('sysadmin', context, {})
    except tk.NotAuthorized:
        return tk.abort(401, _('User not authorized to view page'))

    form_data = tk.request.form
    admins_route = 'showcase_blueprint.admins'

    # We're trying to add a user to the showcase admins list.
    if tk.request.method == 'POST' and form_data['username']:
        username = form_data['username']
        try:
            tk.get_action('ckanext_showcase_admin_add')(
                {}, {'username': username}
                )
        except tk.NotAuthorized:
            abort(401, _('Unauthorized to perform that action'))
        except tk.ObjectNotFound:
            h.flash_error(
                _("User '{user_name}' not found.").format(user_name=username))
        except tk.ValidationError as e:
            h.flash_notice(e.error_summary)
        else:
            h.flash_success(_("The user is now a Showcase Admin"))

        return tk.redirect_to(h.url_for(admins_route))

    showcase_admins = tk.get_action('ckanext_showcase_admin_list')({},{})

    return tk.render('admin/manage_showcase_admins.html',
                     extra_vars={'showcase_admins': showcase_admins})


def remove_showcase_admin():
    '''
    Remove a user from the Showcase Admin list.
    '''
    context = {
        'model': model,
        'session': model.Session,
        'user': tk.g.user or tk.g.author
    }

    try:
        tk.check_access('sysadmin', context, {})
    except tk.NotAuthorized:
        return tk.abort(401, _('User not authorized to view page'))

    form_data = tk.request.form
    admins_route = 'showcase_blueprint.admins'

    if 'cancel' in form_data:
        return tk.redirect_to(admins_route)

    user_id = tk.request.args['user']
    if tk.request.method == 'POST' and user_id:
        user_id = tk.request.args['user']
        try:
            tk.get_action('ckanext_showcase_admin_remove')(
                {}, {'username': user_id}
                )
        except tk.NotAuthorized:
            return tk.abort(401, _('Unauthorized to perform that action'))
        except tk.ObjectNotFound:
            h.flash_error(_('The user is not a Showcase Admin'))
        else:
            h.flash_success(_('The user is no longer a Showcase Admin'))

        return tk.redirect_to(h.url_for(admins_route))

    user_dict = tk.get_action('user_show')({}, {'id': user_id})
    return tk.render('admin/confirm_remove_showcase_admin.html',
                     extra_vars={'user_dict': user_dict, 'user_id': user_id})


def markdown_to_html():
    ''' Migrates the notes of all showcases from markdown to html.

    When using CKEditor, notes on showcases are stored in html instead of
    markdown, this command will migrate all nothes using CKAN's
    render_markdown core helper.
    '''
    showcases = tk.get_action('ckanext_showcase_list')({},{})

    site_user = tk.get_action('get_site_user')({
        'model': model,
        'ignore_auth': True},
        {}
    )
    context = {
        'model': model,
        'session': model.Session,
        'ignore_auth': True,
        'user': site_user['name'],
    }

    for showcase in showcases:
        tk.get_action('package_patch')(
            context,
            {
                'id': showcase['id'],
                'notes': h.render_markdown(showcase['notes'])
            }
        )
    log.info('All notes were migrated successfully.')


def upload():
    if not tk.request.method == 'POST':
        tk.abort(409, _('Only Posting is availiable'))

    data_dict = logic.clean_dict(
        dict_fns.unflatten(
            logic.tuplize_dict(
                logic.parse_params(tk.request.files)
            )
        )
    )

    try:

        url = tk.get_action('ckanext_showcase_upload')(
            None,
            data_dict
        )
    except tk.NotAuthorized:
        tk.abort(401, _('Unauthorized to upload file %s') % id)

    return json.dumps(url)

def is_sysadmin(user_name):
    """
    Check if a CKAN user is a sysadmin.

    :param user_name: The name (not the object) of the user to check.
    :return: True if the user is a sysadmin, False otherwise.
    """
    import ckan.model as model
    user = model.User.get(user_name)
    return user and user.sysadmin

def is_showcase_approved(pkg_dict):
    """
    Check if a showcase (package) is approved based on the 'approval_status' extra.

    :param pkg_dict: The package dictionary (as returned by package_show).
    :return: True if the 'approval_status' extra is set to 'approved', False otherwise.
    """
    if pkg_dict.get('type') != 'showcase':
        return False  # or raise an exception if this must be strictly a showcase

    if pkg_dict.get('approval_status') == 'approved':
        return True

    return False
