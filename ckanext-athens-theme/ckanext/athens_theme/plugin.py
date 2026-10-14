import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import os
import sys
from ckanext.athens_theme import helpers
import logging

log = logging.getLogger(__name__)
plugin_dir = os.path.dirname(sys.modules[__name__].__file__)


@toolkit.blanket.config_declarations
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
        toolkit.add_resource("assets/citation", "athens_theme_citation")

        log.info(f"Loading static files from {os.path.join(plugin_dir, 'public')}")

    def update_config_schema(self, schema):
        """Κάνει τα κείμενα αναφοράς επεξεργάσιμα από το /ckan-admin/config."""
        ignore_missing = toolkit.get_validator('ignore_missing')
        unicode_safe = toolkit.get_validator('unicode_safe')

        for language in helpers.CITATION_LANGUAGES:
            schema[helpers.CITATION_TEMPLATE_OPTION.format(language)] = [
                ignore_missing, unicode_safe, helpers.citation_template_validator
            ]
        return schema

    # ITemplateHelpers
    def get_helpers(self):
        return {
            'athens_theme_get_municipality_name': helpers.get_municipality_name,
            'athens_theme_dataset_citation': helpers.get_dataset_citation,
            'get_featured_categories': helpers.get_featured_categories,
            'get_recent_datasets': helpers.get_recent_datasets,
            'get_site_statistics': helpers.get_site_statistics,
            'render_athens_tree_html': helpers.render_athens_tree_html,
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