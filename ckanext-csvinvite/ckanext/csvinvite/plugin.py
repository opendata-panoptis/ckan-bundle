import os

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.csvinvite import views
from ckanext.csvinvite import helpers


class CsvinvitePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.ITranslation)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("assets", "csvinvite")

    # IBlueprint
    def get_blueprint(self):
        return views.get_blueprints()

    # ITemplateHelpers
    def get_helpers(self):
        return helpers.get_helpers()

    # ITranslation
    def i18n_directory(self):
        return os.path.join(os.path.dirname(__file__), "i18n")

    def i18n_domain(self):
        # Must match: ckanext/csvinvite/i18n/el/LC_MESSAGES/ckanext-csvinvite.(po|mo)
        return "ckanext-csvinvite"

    def i18n_locales(self):
        return ["el", "en"]