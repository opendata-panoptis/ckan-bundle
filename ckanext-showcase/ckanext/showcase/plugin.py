# -*- coding: utf-8 -*-

import os
import re
import sys
import logging
from collections import OrderedDict

import ckan.plugins as plugins
import ckan.plugins.toolkit as tk
import ckan.lib.plugins as lib_plugins
import ckan.lib.helpers as h
from ckan import authz, model

from ckanext.showcase import cli
from ckanext.showcase import utils
from ckanext.showcase import views
from ckanext.showcase.logic import auth, action

import ckanext.showcase.logic.schema as showcase_schema
import ckanext.showcase.logic.helpers as showcase_helpers
from ckanext.showcase.model import ShowcaseAdmin
from ckanext.showcase.logic.auth import _is_showcase_admin

_ = tk._

log = logging.getLogger(__name__)

DATASET_TYPE_NAME = utils.DATASET_TYPE_NAME


class ShowcasePlugin(plugins.SingletonPlugin, lib_plugins.DefaultDatasetForm):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDatasetForm)
    plugins.implements(plugins.IFacets, inherit=True)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IClick)

    # IBlueprint

    def get_blueprint(self):
        return views.get_blueprints()

    # IClick

    def get_commands(self):
        return cli.get_commands()

    # IConfigurer

    def update_config(self, config):
        tk.add_template_directory(config, 'templates')
        tk.add_public_directory(config, 'public')
        tk.add_resource('assets', 'showcase')

    # IDatasetForm

    def package_types(self):
        return [DATASET_TYPE_NAME]

    def is_fallback(self):
        return False

    def search_template(self):
        return 'showcase/search.html'

    def new_template(self):
        return 'showcase/new.html'

    def read_template(self):
        return 'showcase/read.html'

    def edit_template(self):
        return 'showcase/edit.html'

    def package_form(self):
        return 'showcase/new_package_form.html'

    def create_package_schema(self):
        return showcase_schema.showcase_create_schema()

    def update_package_schema(self):
        return showcase_schema.showcase_update_schema()

    def show_package_schema(self):
        return showcase_schema.showcase_show_schema()

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'facet_remove_field': showcase_helpers.facet_remove_field,
            'get_site_statistics': showcase_helpers.get_site_statistics,
            'showcase_get_wysiwyg_editor': showcase_helpers.showcase_get_wysiwyg_editor,
            'is_showcase_admin':  _is_showcase_admin
        }

    # IFacets

    def dataset_facets(self, facets_dict, package_type):
        '''Only show tags for Showcase search list.'''
        if package_type != DATASET_TYPE_NAME:
            return facets_dict
        return OrderedDict([('extras_approval_status', _('Approval Status')), ('tags', _('Tags'))])

    # IAuthFunctions

    def get_auth_functions(self):
        return auth.get_auth_functions()

    # IActions

    def get_actions(self):
        return action.get_actions()

    # IPackageController

    def _add_to_pkg_dict(self, context, pkg_dict):
        '''Add key/values to pkg_dict and return it.'''

        if pkg_dict['type'] != 'showcase':
            return pkg_dict

        # Add a display url for the Showcase image to the pkg dict so template
        # has access to it.
        image_url = pkg_dict.get('image_url')
        pkg_dict['image_display_url'] = image_url
        if image_url and not image_url.startswith('http'):
            pkg_dict['image_url'] = image_url
            pkg_dict['image_display_url'] = \
                h.url_for_static('uploads/{0}/{1}'
                                 .format(DATASET_TYPE_NAME,
                                         pkg_dict.get('image_url')),
                                 qualified=True)

        # Add dataset count
        pkg_dict['num_datasets'] = len(
            tk.get_action('ckanext_showcase_package_list')(
                context, {'showcase_id': pkg_dict['id']}))

        # Rendered notes
        if showcase_helpers.showcase_get_wysiwyg_editor() == 'ckeditor':
            pkg_dict['showcase_notes_formatted'] = pkg_dict['notes']
        else:
            pkg_dict['showcase_notes_formatted'] = \
                h.render_markdown(pkg_dict['notes'])

        return pkg_dict

    # CKAN >= 2.10
    def after_dataset_show(self, context, pkg_dict):
        '''Modify package_show pkg_dict.'''
        pkg_dict = self._add_to_pkg_dict(context, pkg_dict)

    def before_dataset_view(self, pkg_dict):
        '''Modify pkg_dict that is sent to templates.'''
        context = {'user': tk.g.user or tk.g.author}

        return self._add_to_pkg_dict(context, pkg_dict)

    def before_dataset_search(self, search_params):
        '''
        Unless the query is already being filtered by this dataset_type
        (either positively, or negatively), exclude datasets of type
        `showcase`.
        '''
        fq = search_params.get('fq', '')
        filter = 'dataset_type:{0}'.format(DATASET_TYPE_NAME)

        # Αν κάνουμε search για showcases, προσθέτουμε το approval status filter
        if filter in fq and f'-{filter}' not in fq:
            # Είμαστε σε showcase search - προσθέτουμε conditional approval filter
            search_params = self._add_approval_status_filter(search_params)
        elif filter not in fq:
            # Κανονική αναζήτηση - αποκλείουμε τα showcases
            search_params.update({'fq': fq + " -" + filter})

        return search_params

    def _add_approval_status_filter(self, search_params):
        '''
        Προσθέτει φίλτρο approval_status για showcases βάσει του χρήστη.
        Μόνο sysadmin και showcase admin βλέπουν όλα τα showcases.
        '''

        # Παίρνουμε τον τρέχοντα χρήστη
        context = {'user': tk.g.user or tk.g.author}
        user = context.get('user')

        # Ελέγχουμε αν πρέπει να εφαρμόσουμε το φίλτρο
        should_filter = True

        if user:
            # Έλεγχος αν είναι sysadmin
            is_sysadmin = authz.is_sysadmin(user)

            # Έλεγχος αν είναι showcase admin
            is_showcase_admin = False
            try:
                user_obj = model.User.get(user)
                if user_obj:
                    is_showcase_admin = ShowcaseAdmin.is_user_showcase_admin(user_obj)
            except:
                # Showcase extension δεν έχει ShowcaseAdmin, skip check
                pass

            # Δεν φιλτράρουμε για sysadmin ή showcase admin
            if is_sysadmin or is_showcase_admin:
                should_filter = False

        # Εφαρμόζουμε το φίλτρο αν χρειάζεται
        if should_filter:
            current_fq = search_params.get('fq', '')
            approval_filter = 'extras_approval_status:approved'

            # Αφαιρούμε τυχόν υπάρχοντα approval_status φίλτρα από μη διαχειριστές
            if 'extras_approval_status' in current_fq:
                # Αφαίρεση όλων των approval_status patterns
                current_fq = re.sub(r'\s*AND\s+extras_approval_status:[^\s\)]+', '', current_fq)
                current_fq = re.sub(r'extras_approval_status:[^\s\)]+\s*AND\s*', '', current_fq)
                current_fq = re.sub(r'extras_approval_status:[^\s\)]+', '', current_fq)
                current_fq = current_fq.strip()

            # Πάντα προσθέτουμε το approved φίλτρο για μη διαχειριστές
            if current_fq:
                search_params['fq'] = current_fq + ' AND ' + approval_filter
            else:
                search_params['fq'] = approval_filter

        return search_params

    def after_dataset_search(self, search_results, search_params):

        # TODO: Να παίρνει δυναμικά την γλώσσα
        # Μετάφραση των facets από τιμές της βάσης σε Ελληνικά
        for facet_name, facet_items in search_results['search_facets'].items():
            for item in facet_items['items']:
                if item['name'] == 'approved':
                    item['display_name'] = 'Εγκεκριμένο'
                if item['name'] == 'rejected':
                    item['display_name'] = 'Απορρίφθηκε'
                if item['name'] == 'pending':
                    item['display_name'] = 'Σε αναμονή'
        return search_results

    # CKAN < 2.10 (Remove when dropping support for 2.9)
    def after_show(self, context, pkg_dict):
        '''Modify package_show pkg_dict.'''
        pkg_dict = self.after_dataset_show(context, pkg_dict)

    def before_view(self, pkg_dict):
        '''Modify pkg_dict that is sent to templates.'''
        return self.before_dataset_view(pkg_dict)

    def before_search(self, search_params):
        '''
        Unless the query is already being filtered by this dataset_type
        (either positively, or negatively), exclude datasets of type
        `showcase`.
        '''
        return self.before_dataset_search(search_params)

    # ITranslation
    def i18n_directory(self):
        '''Change the directory of the *.mo translation files

        The default implementation assumes the plugin is
        ckanext/myplugin/plugin.py and the translations are stored in
        i18n/
        '''
        # assume plugin is called ckanext.<myplugin>.<...>.PluginClass
        extension_module_name = '.'.join(self.__module__.split('.')[0:2])
        module = sys.modules[extension_module_name]
        return os.path.join(os.path.dirname(module.__file__), 'i18n')

    def i18n_locales(self):
        '''Change the list of locales that this plugin handles

        By default the will assume any directory in subdirectory in the
        directory defined by self.directory() is a locale handled by this
        plugin
        '''
        directory = self.i18n_directory()
        return [d for
                d in os.listdir(directory)
                if os.path.isdir(os.path.join(directory, d))]

    def i18n_domain(self):
        '''Change the gettext domain handled by this plugin

        This implementation assumes the gettext domain is
        ckanext-{extension name}, hence your pot, po and mo files should be
        named ckanext-{extension name}.mo'''
        return 'ckanext-{name}'.format(name=self.name)
