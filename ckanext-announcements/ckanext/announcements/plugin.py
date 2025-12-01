import os
from ckanext.announcements import actions, auth, blueprints, helpers
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit


class announcementsPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.ITranslation)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("assets", "announcements")

    # ITranslation

    def i18n_directory(self):
        '''Change the directory of the *.mo translation files'''
        return os.path.join(
            os.path.dirname(__file__),
            'i18n'
        )

    def i18n_domain(self):
        '''Change the gettext domain name'''
        return 'ckanext-announcements'

    def i18n_locales(self):
        '''Force the presence of locale codes'''
        return ['el']

    def i18n_schema(self):
        '''Force the presence of the schema for extracting messages'''
        return {
            'ckanext-announcements': [
                ('**.py', 'python', None),
                ('**.js', 'javascript', None),
                ('templates/**.html', 'ckan', None),
            ]
        }

    # ITemplateHelpers

    def get_helpers(self):
        return {
            "get_all_announcements": helpers.get_all_announcements,
            "get_public_announcements": helpers.get_public_announcements,
        }

    # IBlueprint

    def get_blueprint(self):
        return [
            blueprints.announcements_blueprint,
        ]

    # IAuthFunctions

    def get_auth_functions(self):
        functions = {
            "announcement_create": auth.announcement_create,
            "announcement_update": auth.announcement_update,
            "announcement_delete": auth.announcement_delete,
            "announcement_show": auth.announcement_show,
        }
        return functions

    # IActions

    def get_actions(self):
        functions = {
            "announcement_create": actions.announcement_create,
            "announcement_update": actions.announcement_update,
            "announcement_delete": actions.announcement_delete,
            "announcement_show": actions.announcement_show,
        }
        return functions
