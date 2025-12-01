# -*- coding: utf-8 -*-
import os
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from flask import Blueprint

import ckanext.vocabulary_admin.controllers.admin as admin_controller
from ckanext.vocabulary_admin.model import vocabulary as vocabulary_model
import ckanext.vocabulary_admin.helpers as helpers
import ckanext.vocabulary_admin.actions as actions


class VocabularyAdminPlugin(plugins.SingletonPlugin):
    """
    CKAN extension for vocabulary management.

    This plugin adds a new tab in the admin interface for managing
    controlled vocabularies and their tags.
    """
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.ITranslation)

    # IConfigurer
    def update_config(self, config_):
        """
        Add our templates and static files to CKAN's paths.
        """
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('assets', 'vocabulary_admin')

    # IBlueprint
    def get_blueprint(self):
        """
        Return a Flask Blueprint object to be registered by the app.
        """
        blueprint = Blueprint('vocabularyadmin', __name__)

        # Add URL rules
        blueprint.add_url_rule(
            '/ckan-admin/vocabulary-management',
            'vocabulary_admin',
            admin_controller.index,
            strict_slashes=False
        )

        # Add routes for creating vocabularies and tags
        blueprint.add_url_rule(
            '/ckan-admin/vocabulary-management/new',
            'vocabulary_create',
            admin_controller.create_vocabulary,
            methods=['GET', 'POST'],
            strict_slashes=False
        )

        blueprint.add_url_rule(
            '/ckan-admin/vocabulary-management/tag/new',
            'tag_create',
            admin_controller.create_tag,
            methods=['GET', 'POST'],
            strict_slashes=False
        )

        blueprint.add_url_rule(
            '/ckan-admin/vocabulary-management/tag/edit/<tag_id>',
            'tag_edit',
            admin_controller.edit_tag,
            methods=['GET', 'POST'],
            strict_slashes=False
        )

        blueprint.add_url_rule(
            '/ckan-admin/vocabulary-management/edit/<vocabulary_id>',
            'vocabulary_edit',
            admin_controller.edit_vocabulary,
            methods=['GET', 'POST'],
            strict_slashes=False
        )

        blueprint.add_url_rule(
            '/ckan-admin/vocabulary-management/delete/<vocabulary_id>',
            'vocabulary_delete',
            admin_controller.delete_vocabulary,
            methods=['GET', 'POST'],
            strict_slashes=False
        )

        blueprint.add_url_rule(
            '/ckan-admin/vocabulary-management/tag/delete/<tag_id>',
            'tag_delete',
            admin_controller.delete_tag,
            methods=['GET', 'POST'],
            strict_slashes=False
        )

        return blueprint


    # ITemplateHelpers
    def get_helpers(self):
        """
        Return the helper functions to be available in templates.
        """
        return {
            'vocabularyadmin_get_vocabularies': helpers.get_vocabularies,
            'vocabularyadmin_get_vocabulary': helpers.get_vocabulary,
            'vocabularyadmin_get_tags': helpers.get_tags,
            'vocabularyadmin_get_vocabulary_count': helpers.get_vocabulary_count,
            'vocabularyadmin_get_tag_count': helpers.get_tag_count,
            'vocabularyadmin_get_tag_metadata': helpers.get_tag_metadata,
            'vocabularyadmin_get_vocabulary_description': helpers.get_vocabulary_description,
            'vocabularyadmin_get_tags_for_scheming': helpers.vocabularyadmin_get_tags_for_scheming,
        }

    # IActions
    def get_actions(self):
        """
        Register the action functions.
        """
        return {
            'tag_update': actions.tag_update,
            'vocabularyadmin_vocabulary_delete': actions.vocabularyadmin_vocabulary_delete,
            'vocabularyadmin_tag_delete': actions.vocabularyadmin_tag_delete,
            'vocabularyadmin_vocabulary_show': actions.vocabularyadmin_vocabulary_show,
        }

    # IAuthFunctions
    def get_auth_functions(self):
        """
        Register the authorization functions.
        """
        return {
            'tag_update': actions.tag_update_auth,
            'vocabularyadmin_vocabulary_delete': actions.vocabularyadmin_vocabulary_delete_auth,
            'vocabularyadmin_tag_delete': actions.vocabularyadmin_tag_delete_auth,
        }

    # ITranslation
    def i18n_directory(self):
        """Return the directory containing this extension's i18n files."""
        return os.path.join(os.path.dirname(__file__), 'i18n')
        
    def i18n_domain(self):
        """Return the gettext domain for this extension."""
        return 'ckanext-vocabulary_admin'
        
    def i18n_locales(self):
        """Return a list of locales that this plugin handles."""
        directory = self.i18n_directory()
        return [d for d in os.listdir(directory)
                if os.path.isdir(os.path.join(directory, d))]
