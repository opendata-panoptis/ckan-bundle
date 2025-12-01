import ckan.plugins as p
from ckanext.report.interfaces import IReport
from ckan.plugins import toolkit
import ckanext.report.logic.action.get as action_get
import ckanext.report.logic.action.update as action_update
import ckanext.report.logic.auth.get as auth_get
import ckanext.report.logic.auth.update as auth_update
from ckan.common import config_declaration

# Δήλωση της ρύθμισης ckanext-report.notes.dataset
config_declaration.declare('ckanext-report.notes.dataset', default='')

try:
    toolkit.requires_ckan_version("2.9")
except toolkit.CkanVersionException:
    from ckanext.report.plugin.pylons_plugin import MixinPlugin
else:
    from ckanext.report.plugin.flask_plugin import MixinPlugin


class ReportPlugin(MixinPlugin, p.SingletonPlugin):
    p.implements(p.IConfigurer)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IActions, inherit=True)
    p.implements(p.IAuthFunctions, inherit=True)
    p.implements(p.ITranslation)

    # IConfigurer

    def update_config(self, config):
        p.toolkit.add_template_directory(config, '../templates')
        p.toolkit.add_resource('../assets', 'report')

    # ITemplateHelpers

    def get_helpers(self):
        from ckanext.report import helpers as h
        return {
            'report__relative_url_for': h.relative_url_for,
            'report__chunks': h.chunks,
            'report__organization_list': h.organization_list,
            'report__render_datetime': h.render_datetime,
            'report__explicit_default_options': h.explicit_default_options,
            }

    # IActions
    def get_actions(self):
        return {'report_list': action_get.report_list,
                'report_show': action_get.report_show,
                'report_data_get': action_get.report_data_get,
                'report_key_get': action_get.report_key_get,
                'report_refresh': action_update.report_refresh}

    # IAuthFunctions
    def get_auth_functions(self):
        return {'report_list': auth_get.report_list,
                'report_show': auth_get.report_show,
                'report_data_get': auth_get.report_data_get,
                'report_key_get': auth_get.report_key_get,
                'report_refresh': auth_update.report_refresh}

    # ITranslation
    def i18n_directory(self):
        import os
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'i18n')

    def i18n_domain(self):
        return 'ckanext-report'

    def i18n_locales(self):
        return ['el', 'en']


class TaglessReportPlugin(p.SingletonPlugin):
    '''
    This is a working example only. To be kept simple and demonstrate features,
    rather than be particularly meaningful.
    '''
    p.implements(IReport)

    # IReport

    def register_reports(self):
        import ckanext.report.reports as reports
        import ckanext.report.stale_datasets_report as stale_report
        return [reports.tagless_report_info, stale_report.stale_datasets_report_info]


