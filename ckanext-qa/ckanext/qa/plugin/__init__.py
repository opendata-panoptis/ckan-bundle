import logging
from pathlib import Path

import ckan.model as model
import ckan.plugins as p
from ckan.plugins import toolkit

from ckanext.archiver.interfaces import IPipe
from ckanext.qa.logic import action, auth
from ckanext.qa.model import QA, aggregate_qa_for_a_dataset
from ckanext.qa.helpers import qa_openness_stars_resource_html, qa_openness_stars_dataset_html, qa_mqa_rating_label
from ckanext.qa.lib import create_qa_update_package_task
from ckanext.report.interfaces import IReport
import ckanext.data_gov_gr.helpers as data_gov_gr_helpers


log = logging.getLogger(__name__)


if toolkit.check_ckan_version(min_version='2.9.0'):
    from ckanext.qa.plugin.flask_plugin import MixinPlugin
else:
    from ckanext.qa.plugin.pylons_plugin import MixinPlugin


class QAPlugin(MixinPlugin, p.SingletonPlugin, toolkit.DefaultDatasetForm):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(IPipe, inherit=True)
    p.implements(IReport)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.IFacets)
    p.implements(p.IPluginObserver)
    p.implements(p.ITranslation)

    # IConfigurer

    def update_config(self, config):
        toolkit.add_template_directory(config, '../templates')

        # check for qsv config
        qsv_bin = config.get('ckanext.qa.qsv_bin')
        if qsv_bin:
            qsv_path = Path(qsv_bin)
            if not qsv_path.is_file():
                log.error('ckanext.qa.qsv_bin file not found: %s', qsv_path)
        else:
            log.error('ckanext.qa.qsv_bin not set')

        # Warm the tldextract cache to avoid blocking on first use
        try:
            import tldextract
            log.debug('Warming tldextract cache...')
            # This will download the PSL file if not already cached
            tldextract.extract('example.com', include_psl_private_domains=True)
            log.debug('tldextract cache warmed successfully')
        except ImportError:
            log.debug('tldextract not available, skipping cache warming')

    # IPipe

    def receive_data(self, operation, queue, **params):
        '''Receive notification from ckan-archiver that a dataset has been
        archived.'''
        if not operation == 'package-archived':
            return
        dataset_id = params['package_id']

        dataset = model.Package.get(dataset_id)
        assert dataset

        create_qa_update_package_task(dataset, queue=queue)

    # IReport

    def register_reports(self):
        """Register details of an extension's reports"""
        from ckanext.qa import reports
        return [reports.openness_report_info, reports.metadata_quality_report_info]

    # IActions

    def get_actions(self):
        return {
            'qa_resource_show': action.qa_resource_show,
            'qa_package_openness_show': action.qa_package_openness_show,
            }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'qa_resource_show': auth.qa_resource_show,
            'qa_package_openness_show': auth.qa_package_openness_show,
            }

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'qa_openness_stars_resource_html':
            qa_openness_stars_resource_html,
            'qa_openness_stars_dataset_html':
            qa_openness_stars_dataset_html,
            'qa_mqa_rating_label':
            qa_mqa_rating_label,
            }

    # IPackageController

    def after_show(self, context, pkg_dict):
        """ Old CKAN function name """
        return self.after_dataset_show(context, pkg_dict)

    def after_dataset_create(self, context, pkg_dict):
        """
        Calculate and store MQA scores after a dataset is created.
        """
        try:
            from ckanext.qa.tasks import calculate_and_store_mqa_scores
            log.info('Calculating MQA scores for newly created dataset: %s', pkg_dict['id'])
            calculate_and_store_mqa_scores(pkg_dict['id'])
        except Exception as e:
            log.error('Error calculating MQA scores for dataset %s: %s', pkg_dict['id'], str(e))

    def after_dataset_update(self, context, pkg_dict):
        """
        Calculate and store MQA scores after a dataset is updated.
        """
        try:
            from ckanext.qa.tasks import calculate_and_store_mqa_scores
            log.info('Calculating MQA scores for updated dataset: %s', pkg_dict['id'])
            calculate_and_store_mqa_scores(pkg_dict['id'])
        except Exception as e:
            log.error('Error calculating MQA scores for dataset %s: %s', pkg_dict['id'], str(e))

    def after_dataset_show(self, context, pkg_dict):
        # Insert the qa info into the package_dict so that it is
        # available on the API.
        # When you edit the dataset, these values will not show in the form,
        # it they will be saved in the resources (not the dataset). I can't see
        # and easy way to stop this, but I think it is harmless. It will get
        # overwritten here when output again.

        # Skip QA info for decisions and data-services
        if pkg_dict.get('type') in ['decision', 'data-service']:
            return

        qa_objs = QA.get_for_package(pkg_dict['id'])
        if not qa_objs:
            return
        # dataset
        dataset_qa = aggregate_qa_for_a_dataset(qa_objs)
        pkg_dict['qa'] = dataset_qa
        # resources
        qa_by_res_id = dict((a.resource_id, a) for a in qa_objs)
        for res in pkg_dict['resources']:
            qa = qa_by_res_id.get(res['id'])
            if qa:
                qa_dict = qa.as_dict()
                del qa_dict['id']
                del qa_dict['package_id']
                del qa_dict['resource_id']
                res['qa'] = qa_dict

    """
    Μέθοδος που εκτελείται πριν την index.py:index_package
    """
    # IPackageController
    def before_dataset_index(self, pkg_dict):
        '''
        Extract QA scores from `qa` and add them to the package dictionary for indexing
        '''
        # Skip openness score for decisions and data-services
        if pkg_dict.get('type') in ['decision', 'data-service']:
            pkg_dict.pop('qa', None)
            return pkg_dict

        qa = pkg_dict.get('qa')
        if qa != None:
            # Add openness score
            openness_score = qa['openness_score']
            pkg_dict['qa_openness_score'] = openness_score

            # Add MQA score and categorize it
            mqa_score = qa.get('mqa_score')
            if mqa_score is not None:
                pkg_dict['qa_mqa_score'] = mqa_score

                # Categorize MQA score according to specified ranges
                if mqa_score >= 86.4 and mqa_score <= 100:
                    pkg_dict['qa_mqa_rating'] = 'excellent'
                elif mqa_score >= 54.3 and mqa_score < 86.4:
                    pkg_dict['qa_mqa_rating'] = 'good'
                elif mqa_score >= 29.6 and mqa_score < 54.3:
                    pkg_dict['qa_mqa_rating'] = 'sufficient'
                elif mqa_score >= 0 and mqa_score < 29.6:
                    pkg_dict['qa_mqa_rating'] = 'bad'

        pkg_dict.pop('qa', None)
        return pkg_dict

    # IFacets
    def dataset_facets(self, facets_dict, package_type):
        """
        Προσθέτει τα facets μόνο για datasets,
        και τα εξαιρεί για data-service και showcases.
        """
        if package_type == 'dataset':
            # Προσθήκη facet για Openness Score
            facets_dict['qa_openness_score'] = toolkit._('Openness Score')
            # Προσθήκη facet για MQA Rating
            if not data_gov_gr_helpers.should_hide_mqa_tab():
                facets_dict['qa_mqa_rating'] = toolkit._('MQA Rating')
        return facets_dict

    def organization_facets(self, facets, organization_type, package_type):
        return facets

    # IPluginObserver

    def before_load(self, plugin):
        # Called before a plugin is loaded
        pass

    def after_load(self, service):
        # Called after a plugin is loaded
        pass

    def before_unload(self, plugin):
        # Called before a plugin is unloaded
        # If this plugin is being unloaded, shut down the RDF_THREAD_POOL
        if plugin == 'qa':
            log.info('Shutting down RDF_THREAD_POOL...')
            try:
                from ckanext.qa.tasks import RDF_THREAD_POOL
                RDF_THREAD_POOL.shutdown(wait=True)
                log.info('RDF_THREAD_POOL shut down successfully')
            except Exception as e:
                log.error('Error shutting down RDF_THREAD_POOL: %s', str(e))

    def after_unload(self, service):
        # Called after a plugin is unloaded
        pass

    # ITranslation
    def i18n_directory(self):
        import os
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'i18n')

    def i18n_domain(self):
        return 'ckanext-qa'

    def i18n_locales(self):
        return ['el', 'en']
