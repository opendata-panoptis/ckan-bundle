import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import os
import sys
from ckanext.athens_theme import helpers
import logging

log = logging.getLogger(__name__)
plugin_dir = os.path.dirname(sys.modules[__name__].__file__)


class AthensThemePlugin(plugins.SingletonPlugin):
    """Athens Municipality theme plugin."""

    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IFacets)

    # IConfigurer
    def update_config(self, config_):
        log.info("Configuring athens-theme plugin")

        # Add template and static directories
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")

        # Set up default logo
        config_["ckan.site_logo"] = "/images/logo.png"
        log.info(f"Loading static files from {os.path.join(plugin_dir, 'public')}")

    # ITemplateHelpers
    def get_helpers(self):
        return {
            'athens_theme_get_municipality_name': helpers.get_municipality_name,
            'get_featured_categories': helpers.get_featured_categories,
            'get_recent_datasets': helpers.get_recent_datasets,
            'get_site_statistics': helpers.get_site_statistics
        }

    # ITranslation
    def i18n_directory(self):
        return os.path.join(plugin_dir, 'i18n')

    def i18n_domain(self):
        return 'athens_theme'

    def i18n_locales(self):
        return ['el']

    # IFacets
    def dataset_facets(self, facets_dict, package_type):
        if package_type == 'dataset':
            facets_dict['organization'] = plugins.toolkit._('Organizations')
            facets_dict['groups'] = plugins.toolkit._('Categories')
            facets_dict['tags'] = plugins.toolkit._('Tags')
            facets_dict['res_format'] = plugins.toolkit._('Formats')
        return facets_dict

    def group_facets(self, facets_dict, group_type, package_type):
        if group_type == 'group':
            facets_dict['organization'] = plugins.toolkit._('Organizations')
            facets_dict['tags'] = plugins.toolkit._('Tags')
            facets_dict['res_format'] = plugins.toolkit._('Formats')
        return facets_dict

    def organization_facets(self, facets_dict, organization_type, package_type):
        if organization_type == 'organization':
            facets_dict['groups'] = plugins.toolkit._('Categories')
            facets_dict['tags'] = plugins.toolkit._('Tags')
            facets_dict['res_format'] = plugins.toolkit._('Formats')
        return facets_dict