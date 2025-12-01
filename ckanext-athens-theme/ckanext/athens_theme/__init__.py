import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

class AthensThemePlugin(plugins.SingletonPlugin):
    """A custom theme plugin for Athens Open Data Portal that overrides the govgr theme."""
    
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    
    def update_config(self, config):
        """Update the config for this plugin to override the govgr theme."""
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_public_directory(config, 'public')
        toolkit.add_resource('fanstatic', 'fanstatic')
        config['ckan.theme'] = 'athens-theme'  
    
    def get_helpers(self):
        """Return helper functions for templates."""
        return {
            'athens_theme_helper': self.athens_theme_helper,
            'athens_get_logo_url': self._get_logo_url,
            'athens_get_header_class': self._get_header_class,
        }
    
    def athens_theme_helper(self):
        """Helper function for the Athens theme."""
        return "Athens Theme Helper"
    
    def _get_logo_url(self):
        """Get the logo URL for the Athens theme."""
        # Χρησιμοποιούμε την ίδια λογική με το data.gov.gr
        site_logo = toolkit.config.get('ckan.site_logo', '/images/athens-logo.png')
        return site_logo
    
    def _get_header_class(self):
        """Get CSS class for header based on page context."""
        return 'athens-header'