# -*- coding: utf-8 -*-

from flask import Blueprint


import ckan.plugins.toolkit as tk
import ckan.views.dataset as dataset

import ckanext.showcase.utils as utils
from ckanext.showcase.logic.auth import _is_showcase_admin

showcase = Blueprint('showcase_blueprint', __name__)
from flask import request

from collections import OrderedDict
from functools import partial

from ckan.common import asbool, current_user

import ckan.lib.base as base
from ckan.lib.helpers import helper_functions as h
from ckan.lib.helpers import Page

import ckan.plugins as plugins
from ckan.common import _, config, g, request
from ckan.lib.search import (
    SearchError, SearchQueryError, SearchIndexError, SolrConnectionError
)
from ckan.types import Context, Response
import logging
import ckan.plugins.toolkit as toolkit

log = logging.getLogger(__name__)

NotFound = dataset.logic.NotFound
NotAuthorized = dataset.logic.NotAuthorized
clean_dict = dataset.logic.clean_dict
import ckan.lib.navl.dictization_functions as dict_fns
ValidationError = dataset.ValidationError
check_access = dataset.check_access
get_action = dataset.get_action
tuplize_dict = dataset.tuplize_dict
parse_params = dataset.parse_params
flatten_to_string_key = dataset.flatten_to_string_key
from ckan.views.home import CACHE_PARAMETERS
from ckan.lib.mailer import mail_recipient
from typing import Any, Iterable, Optional, Union
def index():
    return search_showcases(utils.DATASET_TYPE_NAME)

"""
    Αναζήτηση Showcases
"""
def search_showcases(package_type: str) -> str:
    extra_vars: dict[str, Any] = {}

    extra_vars['q'] = q = request.args.get('q', '')

    extra_vars['query_error'] = False
    page = h.get_page_number(request.args)

    limit = config.get(u'ckan.datasets_per_page')

    # most search operations should reset the page counter:
    params_nopage = [(k, v) for k, v in request.args.items(multi=True)
                     if k != u'page']

    extra_vars[u'remove_field'] = partial(dataset.remove_field, package_type)

    sort_by = request.args.get(u'sort', None)
    params_nosort = [(k, v) for k, v in params_nopage if k != u'sort']

    extra_vars[u'sort_by'] = partial(dataset._sort_by, params_nosort, package_type)

    if not sort_by:
        sort_by_fields = []
    else:
        sort_by_fields = [field.split()[0] for field in sort_by.split(u',')]
    extra_vars[u'sort_by_fields'] = sort_by_fields

    pager_url = partial(dataset._pager_url, params_nopage, package_type)

    details = dataset._get_search_details()
    extra_vars[u'fields'] = details[u'fields']
    extra_vars[u'fields_grouped'] = details[u'fields_grouped']
    fq = details[u'fq']
    search_extras = details[u'search_extras']

    context: Context = {
        u'user': current_user.name,
        u'for_view': True,
        u'auth_user_obj': current_user
    }

    # Unless changed via config options, don't show other dataset
    # types any search page. Potential alternatives are do show them
    # on the default search page (dataset) or on one other search page
    search_all_type = config.get(u'ckan.search.show_all_types')
    search_all = False

    try:
        # If the "type" is set to True or False, convert to bool
        # and we know that no type was specified, so use traditional
        # behaviour of applying this only to dataset type
        search_all = asbool(search_all_type)
        search_all_type = u'dataset'
    # Otherwise we treat as a string representing a type
    except ValueError:
        search_all = True

    if not search_all or package_type != search_all_type:
        # Only show datasets of this particular type
        fq += u' +dataset_type:{type}'.format(type=package_type)


    isAdmin =  getattr(current_user, 'sysadmin', False)
    isShowcaseAdmin = _is_showcase_admin(context)

    # Αν δεν είναι sysadmin εμφανισε μόνο τα εγκεκριμένα
    if not isAdmin and not isShowcaseAdmin:
        fq += u' +extras_approval_status:approved'



    facets: dict[str, str] = OrderedDict()

    org_label = h.humanize_entity_type(
        u'organization',
        h.default_group_type(u'organization'),
        u'facet label') or _(u'Organizations')

    group_label = h.humanize_entity_type(
        u'group',
        h.default_group_type(u'group'),
        u'facet label') or _(u'Groups')

    default_facet_titles = {
        u'organization': org_label,
        u'groups': group_label,
        u'tags': _(u'Tags'),
        u'res_format': _(u'Formats'),
        u'license_id': _(u'Licenses'),
    }

    for facet in h.facets():
        if facet in default_facet_titles:
            facets[facet] = default_facet_titles[facet]
        else:
            facets[facet] = facet

    # Facet titles
    for plugin in plugins.PluginImplementations(plugins.IFacets):
        facets = plugin.dataset_facets(facets, package_type)

    extra_vars[u'facet_titles'] = facets
    data_dict: dict[str, Any] = {
        u'q': q,
        u'fq': fq.strip(),
        u'facet.field': list(facets.keys()),
        u'rows': limit,
        u'start': (page - 1) * limit,
        u'sort': sort_by,
        u'extras': search_extras,
        u'include_private': config.get(
            u'ckan.search.default_include_private'),
    }
    try:
        query = dataset.get_action(u'package_search')(context, data_dict)

        extra_vars[u'sort_by_selected'] = query[u'sort']

        extra_vars[u'page'] = Page(
            collection=query[u'results'],
            page=page,
            url=pager_url,
            item_count=query[u'count'],
            items_per_page=limit
        )
        extra_vars[u'search_facets'] = query[u'search_facets']
        extra_vars[u'page'].items = query[u'results']
    except SearchQueryError as se:
        # User's search parameters are invalid, in such a way that is not
        # achievable with the web interface, so return a proper error to
        # discourage spiders which are the main cause of this.
        log.info(u'Dataset search query rejected: %r', se.args)
        base.abort(
            400,
            _(u'Invalid search query: {error_message}')
            .format(error_message=str(se))
        )
    except (SearchError, SolrConnectionError) as se:
        if isinstance(se, SolrConnectionError):
            base.abort(500, se.args[0])

        # May be bad input from the user, but may also be more serious like
        # bad code causing a SOLR syntax error, or a problem connecting to
        # SOLR
        log.error(u'Dataset search error: %r', se.args)
        extra_vars[u'query_error'] = True
        extra_vars[u'search_facets'] = {}
        extra_vars[u'page'] = Page(collection=[])

    # FIXME: try to avoid using global variables
    g.search_facets_limits = {}
    default_limit: int = config.get(u'search.facets.default')
    for facet in extra_vars[u'search_facets'].keys():
        try:
            limit = int(
                request.args.get(
                    u'_%s_limit' % facet,
                    default_limit
                )
            )
        except ValueError:
            base.abort(
                400,
                _(u'Parameter u"{parameter_name}" is not '
                  u'an integer').format(parameter_name=u'_%s_limit' % facet)
            )

        g.search_facets_limits[facet] = limit

    dataset._setup_template_variables(context, {}, package_type=package_type)

    extra_vars[u'dataset_type'] = package_type

    # TODO: remove
    for key, value in extra_vars.items():
        setattr(g, key, value)

    return base.render(
        dataset._get_pkg_template(u'search_template', package_type), extra_vars
    )


class CreateView(dataset.CreateView):

    def _prepare(self) -> Context:
        """Bypass package_create permission check for showcases."""
        context: Context = {
            u'user': current_user.name,
            u'auth_user_obj': current_user,
            u'save': self._is_save()
        }
        return context

    # Override της get μεθόδου από το dataset προκειμένου στην prepare να γίνει αφαίρεση του δικαιώματος όταν πάμε να δημιουργήσουμε showcase
    def get(self, data=None, errors=None, error_summary=None) -> str:
        package_type = "showcase"
        context = self._prepare()
        if data and u'type' in data:
            package_type = data[u'type']

        data = data or clean_dict(
            dict_fns.unflatten(
                tuplize_dict(
                    parse_params(request.args, ignore_keys=CACHE_PARAMETERS)
                )
            )
        )
        resources_json = h.dump_json(data.get(u'resources', []))
        # convert tags if not supplied in data
        if data and not data.get(u'tag_string'):
            data[u'tag_string'] = u', '.join(
                h.dict_list_reduce(data.get(u'tags', {}), u'name')
            )

        errors = errors or {}
        error_summary = error_summary or {}
        # in the phased add dataset we need to know that
        # we have already completed stage 1
        stage = [u'active']
        if data.get(u'state', u'').startswith(u'draft'):
            stage = [u'active', u'complete']

        # if we are creating from a group then this allows the group to be
        # set automatically
        data[
            u'group_id'
        ] = request.args.get(u'group') or request.args.get(u'groups__0__id')

        form_snippet = dataset._get_pkg_template(
            u'package_form', package_type=package_type
        )
        form_vars: dict[str, Any] = {
            u'data': data,
            u'errors': errors,
            u'error_summary': error_summary,
            u'action': u'new',
            u'stage': stage,
            u'dataset_type': package_type,
            u'form_style': u'new'
        }
        errors_json = h.dump_json(errors)

        # TODO: remove
        g.resources_json = resources_json
        g.errors_json = errors_json

        dataset._setup_template_variables(context, {}, package_type=package_type)

        new_template = dataset._get_pkg_template(u'new_template', package_type)
        return base.render(
            new_template,
            extra_vars={
                u'form_vars': form_vars,
                u'form_snippet': form_snippet,
                u'dataset_type': package_type,
                u'resources_json': resources_json,
                u'errors_json': errors_json
            }
        )

    def post(self):

        data_dict = dataset.clean_dict(
            dataset.dict_fns.unflatten(
                dataset.tuplize_dict(dataset.parse_params(tk.request.form))))
        data_dict.update(
            dataset.clean_dict(
                dataset.dict_fns.unflatten(
                    dataset.tuplize_dict(dataset.parse_params(
                        tk.request.files)))))
        context = self._prepare()
        data_dict['type'] = utils.DATASET_TYPE_NAME

        try:
            pkg_dict = tk.get_action('ckanext_showcase_create')(context,
                                                                data_dict)

        except tk.ValidationError as e:
            errors = e.error_dict
            error_summary = e.error_summary
            data_dict['state'] = 'none'
            return self.get(data_dict, errors, error_summary)

        # redirect to manage datasets
        url = h.url_for('showcase_blueprint.manage_datasets',
                        id=pkg_dict['name'])
        return h.redirect_to(url)


def manage_datasets(id):
    return utils.manage_datasets_view(id)


def delete(id):
    return utils.delete_view(id)


def read(id):
    return utils.read_view(id)


class EditView(dataset.EditView):

    def _prepare(self) -> Context:
        context: Context = {
            u'user': current_user.name,
            u'auth_user_obj': current_user,
            u'save': u'save' in request.form
        }
        return context

    # Override της get μεθόδου από το dataset προκειμένου να γίνει αντικατάσταση των δικαιωμάτων
    def get(self, id, data=None, errors=None, error_summary=None):

        context = self._prepare()
        package_type = 'showcase' #dataset._get_package_type(id) or package_type
        try:
            view_context = context.copy()
            view_context['for_view'] = True
            pkg_dict = get_action(u'ckanext_showcase_show')(
                view_context, {u'id': id})
            context[u'for_edit'] = True
            old_data = get_action(u'ckanext_showcase_show')(context, {u'id': id})
            # old data is from the database and data is passed from the
            # user if there is a validation error. Use users data if there.
            if data:
                old_data.update(data)
            data = old_data
        except (NotFound, NotAuthorized):
            return base.abort(404, _(u'Dataset not found'))
        assert data is not None
        # are we doing a multiphase add?
        if data.get(u'state', u'').startswith(u'draft'):
            g.form_action = h.url_for(u'{}.new'.format(package_type))
            g.form_style = u'new'

            return CreateView().get(
                package_type,
                data=data,
                errors=errors,
                error_summary=error_summary
            )

        pkg = context.get(u"package")
        rj = h.dump_json(data.get(u'resources', []))
        user = current_user.name
        showcase_package = utils.read_showcase(pkg_dict.get('id'), context)
        try:
            check_access(
                'ckanext_showcase_update',
                context,
                showcase_package
            )
        except NotAuthorized:
            return base.abort(
                403,
                _(u'User %r not authorized to edit %s') % (user, id)
            )
        # convert tags if not supplied in data
        if data and not data.get(u'tag_string'):
            data[u'tag_string'] = u', '.join(
                h.dict_list_reduce(pkg_dict.get(u'tags', {}), u'name')
            )
        errors = errors or {}
        form_snippet = dataset._get_pkg_template(
            u'package_form', package_type=package_type
        )
        form_vars: dict[str, Any] = {
            u'data': data,
            u'errors': errors,
            u'error_summary': error_summary,
            u'action': u'edit',
            u'dataset_type': package_type,
            u'form_style': u'edit'
        }
        errors_json = h.dump_json(errors)

        # TODO: remove
        g.pkg = pkg
        g.resources_json = rj
        g.errors_json = errors_json

        dataset._setup_template_variables(
            context, {u'id': id}, package_type=package_type
        )

        # we have already completed stage 1
        form_vars[u'stage'] = [u'active']
        if data.get(u'state', u'').startswith(u'draft'):
            form_vars[u'stage'] = [u'active', u'complete']

        edit_template = dataset._get_pkg_template(u'edit_template', package_type)
        return base.render(
            edit_template,
            extra_vars={
                u'form_vars': form_vars,
                u'form_snippet': form_snippet,
                u'dataset_type': package_type,
                u'pkg_dict': pkg_dict,
                u'pkg': pkg,
                u'resources_json': rj,
                u'errors_json': errors_json
            }
        )

    def post(self, id):
        if tk.check_ckan_version(min_version='2.10.0'):
            context = self._prepare()
        else:
            # Remove when dropping support for 2.9
            context = self._prepare(id)

        utils.check_edit_view_auth(id)

        data_dict = dataset.clean_dict(
            dataset.dict_fns.unflatten(
                dataset.tuplize_dict(dataset.parse_params(tk.request.form))))
        data_dict.update(
            dataset.clean_dict(
                dataset.dict_fns.unflatten(
                    dataset.tuplize_dict(dataset.parse_params(
                        tk.request.files)))))

        data_dict['id'] = id
        try:

            # Ανάκτηση του υφιστάμενου showcase
            pkg = utils.read_showcase(id, context)

            # Το approval_status δεν υπάρχει στο data_dict όταν πολίτης τροποποιεί και υποβάλει δημιουργημένο showcase

            # Safely get approval status with default values to prevent KeyError
            current_approval_status = pkg.get('approval_status', '')
            new_approval_status = data_dict.get('approval_status', '')

            # Αν έχει γίνει μεταβολή έγκρισης από όχι εγκεκριμένο σε εγκεκριμένο θα πρέπει να γίνεται από admin
            if current_approval_status != 'approved' and new_approval_status == 'approved':

                # TODO: Validation οτι ο admin κάνει την έγκριση

                try:
                    # Αποστολή στον δημιουργό ότι έγινε έγκριση του showcase

                    # Ανάκτηση ονόματος του δημιουργού του showcase
                    creator_email = get_email_from_id(context, pkg['creator_user_id'])

                    # Δημιουργία του URL του showcase
                    from ckan.common import config
                    site_url = config.get('ckan.site_url', 'http://localhost:5000')
                    showcase_url = f"{site_url}/showcase/{pkg['name']}"

                    send_approved_showcase_email(context, creator_email, data_dict, showcase_url)

                except Exception as e:
                    tk.error_shout(f"Email sending failed: {e}")

            pkg = tk.get_action('ckanext_showcase_update')(context, data_dict)
        except tk.ValidationError as e:
            errors = e.error_dict
            error_summary = e.error_summary
            return self.get(id, data_dict, errors, error_summary)

        tk.g.pkg_dict = pkg

        # redirect to showcase details page
        url = h.url_for('showcase_blueprint.read', id=pkg['name'])
        return h.redirect_to(url)

def send_approved_showcase_email(context, recipient, data_dict, showcase_url):
    mail_recipient(
        recipient_name="",
        recipient_email=recipient,
        subject=f"DATA GOV GR: Εγκεκριμένη Εφαρμογή: '{data_dict['title']}'",
        body=f"Η εφαρμογή '{data_dict['title']}' εγκρίθηκε. Η εφαρμογή είναι διαθέσιμη εδώ. URL: '{showcase_url}'"
    )

def get_email_from_id(context, user_id):
    try:
        user = toolkit.get_action('user_show')(context, {'id': user_id})
        return user.get('email')  # or 'fullname' if you want full name
    except toolkit.ObjectNotFound:
        return None


def dataset_showcase_list(id):
    return utils.dataset_showcase_list(id)


def admins():
    return utils.manage_showcase_admins()


def admin_remove():
    return utils.remove_showcase_admin()


def upload():
    return utils.upload()


showcase.add_url_rule('/showcase', view_func=index, endpoint="index")
showcase.add_url_rule('/showcase/new', view_func=CreateView.as_view('new'), endpoint="new")
showcase.add_url_rule('/showcase/delete/<id>',
                      view_func=delete,
                      methods=['GET', 'POST'],
                      endpoint="delete")
showcase.add_url_rule('/showcase/<id>', view_func=read, endpoint="read")
showcase.add_url_rule('/showcase/edit/<id>',
                      view_func=EditView.as_view('edit'),
                      methods=['GET', 'POST'],
                      endpoint="edit")
showcase.add_url_rule('/showcase/manage_datasets/<id>',
                      view_func=manage_datasets,
                      methods=['GET', 'POST'],
                      endpoint="manage_datasets")
showcase.add_url_rule('/dataset/showcases/<id>',
                      view_func=dataset_showcase_list,
                      methods=['GET', 'POST'],
                      endpoint="dataset_showcase_list")
showcase.add_url_rule('/ckan-admin/showcase_admins',
                      view_func=admins,
                      methods=['GET', 'POST'],
                      endpoint="admins")
showcase.add_url_rule('/ckan-admin/showcase_admin_remove',
                      view_func=admin_remove,
                      methods=['GET', 'POST'],
                      endpoint='admin_remove')
showcase.add_url_rule('/showcase_upload',
                      view_func=upload,
                      methods=['POST'])


def get_blueprints():
    return [showcase]
