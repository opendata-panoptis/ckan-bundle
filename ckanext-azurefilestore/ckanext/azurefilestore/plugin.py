import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from routes.mapper import SubMapper

from ckanext.azurefilestore.controller import AzureController
from ckanext.azurefilestore.uploader import AzureResourceUploader, AzureUploader, BaseAzureUploader
from ckan.views.resource import Blueprint

class AzurefilestorePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IUploader)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'azurefilestore')

    # IConfigurable

    def configure(self, config):
        # Certain config options must exists for the plugin to work. Raise an
        # exception if they're missing.
        missing_config = '{0} is not configured. Please amend your .ini file.'
        config_options = (
            'ckanext.azurefilestore.connect_str',
            'ckanext.azurefilestore.container_name',
            'ckanext.azurefilestore.storage_account',
            'ckanext.azurefilestore.account_key'
        )
        for option in config_options:
            if not config.get(option, None):
                raise RuntimeError(missing_config.format(option))

        # Check that options actually work, if not exceptions will be raised
        if toolkit.asbool(
                config.get('ckanext.azurefilestore.check_access_on_startup',
                           True)):
            BaseAzureUploader().get_container_client(
                config.get('ckanext.azurefilestore.container_name'))

    # IUploader
    # Οι δύο μέθοδοι αυτές αντικαθιστούν την default λειτουργικότητα του CKAN με το ανέβασμα στο azure
    def get_resource_uploader(self, data_dict):
        '''Return an uploader object used to upload resource files.'''
        return AzureResourceUploader(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        '''Return an uploader object used to upload general files.'''
        return AzureUploader(upload_to, old_filename)

    # IBlueprint

    def get_blueprint(self):
        azure_bp = Blueprint('azurefilestore', __name__)

        controller = AzureController()

        # Replacement for original resource_download routes
        azure_bp.add_url_rule(
            '/dataset/<id>/resource/<resource_id>/download',
            view_func=controller.resource_download,
            endpoint='resource_download'
        )

        azure_bp.add_url_rule(
            '/dataset/<id>/resource/<resource_id>/download/<filename>',
            view_func=controller.resource_download,
            endpoint='resource_download_with_filename'
        )

        # Replacement for filesystem_resource_download
        azure_bp.add_url_rule(
            '/dataset/<id>/resource/<resource_id>/fs_download/<filename>',
            view_func=controller.filesystem_resource_download,
            endpoint='filesystem_resource_download'
        )

        # Replacement for uploaded_file route
        azure_bp.add_url_rule(
            '/uploads/<upload_to>/<filename>',
            view_func=controller.uploaded_file_redirect,
            endpoint='uploaded_file_redirect'
        )
        return azure_bp
