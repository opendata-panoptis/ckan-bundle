import re

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.lib.jobs import DEFAULT_QUEUE_NAME
from ckan.lib.plugins import DefaultTranslation

from ckan import model

from .tasks import update_zip
from . import helpers
from . import action


log = __import__('logging').getLogger(__name__)


class DownloadallPlugin(plugins.SingletonPlugin, DefaultTranslation):
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IActions)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'downloadall')

    # IDomainObjectModification

    def notify(self, entity, operation):
        u'''
        Send a notification on entity modification.

        :param entity: instance of module.Package.
        :param operation: 'new', 'changed' or 'deleted'.
        '''

        if operation == 'deleted':
            return

        # Check if entity is None or doesn't have a name attribute
        if entity is None:
            log.debug(u'{} None'.format(operation))
            log.debug(u'{} None'.format(operation))
            return

        try:
            log.debug(u'{} {} \'{}\''
                      .format(operation, type(entity).__name__, entity.name))
        except AttributeError:
            log.debug(u'{} {} (no name)'.format(operation, type(entity).__name__))
            return
        # We should regenerate zip if these happen:
        # 1 change of title, description etc (goes into package.json)
        # 2 add/change/delete resource metadata
        # 3 change resource data by upload (results in URL change)
        # 4 change resource data by remote data
        # BUT not:
        # 5 if this was just an update of the Download All zip itself
        #   (or you get an infinite loop)
        #
        # 4 - we're ignoring this for now (ideally new data means a new URL)
        # 1&2&3 - will change package.json and notify(res) and possibly
        #         notify(package) too
        # 5 - will cause these notifies but package.json only in limit places
        #
        # SO if package.json (not including Package Zip bits) remains the same
        # then we don't need to regenerate zip.
        # Try to get the current user from the context, but handle the case
        # when running outside of a Flask request context
        try:
            user = toolkit.c.user
        except (RuntimeError, TypeError):
            log.debug('Running outside of Flask application context, user will be None')
            user = None
        if isinstance(entity, model.Package) and operation == 'changed' and user is None:
            # Αυτό πιθανότατα σημαίνει ότι η ειδοποίηση προήλθε από τον ίδιο τον worker
            # που μόλις ενημέρωσε το resource του zip.
            # Δεν χρειάζεται να ξαναβάλουμε την εργασία στην ουρά.
            log.debug(f"Package '{getattr(entity, 'name', 'UnknownPackage')}' change notification received, "
                      f"but user context is None (likely worker-triggered for 'downloadall'). Skipping re-queue.")
            return
        if isinstance(entity, model.Package):
            if helpers.is_data_service(entity) or getattr(entity, 'type', '') == 'showcase':
                log.debug('Skipping downloadall queue for dataset type "{}": {}'.format(
                    getattr(entity, 'type', 'unknown'), getattr(entity, 'name', 'UnknownPackage')))
                purge_downloadall_zip(entity.id, user)
                return
            enqueue_update_zip(entity.name, entity.id, operation, user)
        elif isinstance(entity, model.Resource):
            if entity.extras.get('downloadall_metadata_modified'):
                # this is the zip of all the resources - no need to react to
                # it being changed
                log.debug('Ignoring change to zip resource')
                return
            dataset = entity.related_packages()[0]
            if helpers.is_data_service(dataset) or getattr(dataset, 'type', '') == 'showcase':
                log.debug('Skipping downloadall queue for dataset type "{}": {}'.format(
                    getattr(dataset, 'type', 'unknown'), getattr(dataset, 'name', 'UnknownPackage')))
                purge_downloadall_zip(dataset.id, user)
                return
            enqueue_update_zip(dataset.name, dataset.id, operation, user)
        else:
            return

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'downloadall__pop_zip_resource': helpers.pop_zip_resource,
            'downloadall__is_data_service': helpers.is_data_service,
        }

    # IPackageController

    def before_index(self, pkg_dict):
        try:
            if u'All resource data' in pkg_dict['res_name']:
                # we've got a 'Download all zip', so remove it's ZIP from the
                # SOLR facet of resource formats, as it's not really a data
                # resource
                pkg_dict['res_format'].remove('ZIP')
        except KeyError:
            # this happens when you save a new package without a resource yet
            pass
        return pkg_dict

    # IActions

    def get_actions(self):
        actions = {}
        if plugins.get_plugin('datastore'):
            # datastore is enabled, so we need to chain the datastore_create
            # action, to update the zip when it is called
            actions['datastore_create'] = action.datastore_create
        return actions


def enqueue_update_zip(dataset_name, dataset_id, operation,user=None ):
    # skip task if the dataset is already queued
    queue = 'bulk'
    jobs = toolkit.get_action('job_list')(
        {'ignore_auth': True}, {'queues': [queue]})
    if jobs:
        for job in jobs:
            if not job['title']:
                continue
            match = re.match(
                r'DownloadAll \w+ "[^"]*" ([\w-]+)', job[u'title'])
            if match:
                queued_dataset_id = match.groups()[0]
                if dataset_id == queued_dataset_id:
                    log.info('Already queued dataset: {} {}'
                             .format(dataset_name, dataset_id))
                    return

    # add this dataset to the queue
    log.debug(u'Queuing job update_zip: {} {}'
              .format(operation, dataset_name))

    # Pass the user parameter to the update_zip function
    toolkit.enqueue_job(
        update_zip, [dataset_id, user, True],
        title=u'DownloadAll {} "{}" {}'.format(operation, dataset_name,
                                               dataset_id),
        queue=queue)


def purge_downloadall_zip(dataset_id, user=None):
    """Delete downloadall ZIP resources for given dataset."""
    context = {'model': model, 'session': model.Session}
    if user:
        context['user'] = user
    else:
        context['ignore_auth'] = True

    try:
        dataset = toolkit.get_action('package_show')(context, {'id': dataset_id})
    except Exception as e:
        log.error('Failed to fetch dataset %s for ZIP purge: %s', dataset_id, e)
        return

    zip_resource_ids = [
        res['id'] for res in dataset.get('resources', [])
        if res.get('downloadall_metadata_modified')
    ]

    if not zip_resource_ids:
        return

    log.info('Purging %d downloadall ZIP resource(s) from dataset %s', len(zip_resource_ids), dataset_id)

    for res_id in zip_resource_ids:
        try:
            toolkit.get_action('resource_delete')(context, {'id': res_id})
            log.info('Deleted downloadall ZIP resource %s from dataset %s', res_id, dataset_id)
        except Exception as e:
            log.error('Failed to delete downloadall ZIP resource %s: %s', res_id, e)
