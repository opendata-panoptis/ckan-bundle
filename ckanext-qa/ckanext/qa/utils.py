import sys
from sqlalchemy import or_
import six
import logging
import datetime
log = logging.getLogger(__name__)


def init_db():
    import ckan.model as model
    from ckanext.qa.model import init_tables
    init_tables(model.meta.engine)


def update(ids, queue):
    from ckan import model
    from ckanext.qa import lib
    packages = []
    resources = []
    if len(ids) > 0:
        for id in ids:
            # try id as a group id/name
            group = model.Group.get(id)
            if group and group.is_organization:
                # group.packages() is unreliable for an organization -
                # member objects are not definitive whereas owner_org, so
                # get packages using owner_org
                query = model.Session.query(model.Package)\
                    .filter(
                        or_(model.Package.state == 'active',
                            model.Package.state == 'pending'))\
                    .filter_by(owner_org=group.id)
                packages.extend(query.all())
                if not queue:
                    queue = 'bulk'
                continue
            elif group:
                packages.extend(group.packages())
                if not queue:
                    queue = 'bulk'
                continue
            # try id as a package id/name
            pkg = model.Package.get(id)
            if pkg:
                packages.append(pkg)
                if not queue:
                    queue = 'priority'
                continue
            # try id as a resource id
            res = model.Resource.get(id)
            if res:
                resources.append(res)
                if not queue:
                    queue = 'priority'
                continue
            else:
                log.error('Could not recognize as a group, package '
                          'or resource: %r', id)
                sys.exit(1)
    else:
        # all packages
        pkgs = model.Session.query(model.Package)\
                    .filter_by(state='active')\
                    .order_by('name').all()
        packages.extend(pkgs)
        if not queue:
            queue = 'bulk'

    if packages:
        log.info('Datasets to QA: %d', len(packages))
    if resources:
        log.info('Resources to QA: %d', len(resources))
    if not (packages or resources):
        log.error('No datasets or resources to process')
        sys.exit(1)

    log.info('Queue: %s', queue)
    for package in packages:
        lib.create_qa_update_package_task(package, queue)
        log.info('Queuing dataset %s (%s resources)',
                 package.name, len(package.resources))

    for resource in resources:
        package = resource.resource_group.package
        log.info('Queuing resource %s/%s', package.name, resource.id)
        lib.create_qa_update_task(resource, queue)

    log.info('Completed queueing')


def sniff(filepaths):
    from ckanext.qa.sniff_format import sniff_file_format

    for filepath in filepaths:
        format_ = sniff_file_format(
            filepath)
        if format_:
            print('Detected as: %s - %s' % (format_['display_name'],
                                            filepath))
        else:
            print('ERROR: Could not recognise format of: %s' % filepath)


def view(package_ref=None):
    from ckan import model

    q = model.Session.query(model.TaskStatus).filter_by(task_type='qa')
    print('QA records - %i TaskStatus rows' % q.count())
    print('      across %i Resources' % q.distinct('entity_id').count())

    if package_ref:
        pkg = model.Package.get(package_ref)
        print('Package %s %s' % (pkg.name, pkg.id))
        for res in pkg.resources:
            print('Resource %s' % res.id)
            for row in q.filter_by(entity_id=res.id):
                print('* %s = %r error=%r' % (row.key, row.value,
                                              row.error))


def clean():
    from ckan import model

    print('Before:')
    view()

    q = model.Session.query(model.TaskStatus).filter_by(task_type='qa')
    q.delete()
    model.Session.commit()

    print('After:')
    view()


def migrate1():
    from ckan import model
    from ckan.lib.helpers import json

    q_status = model.Session.query(model.TaskStatus) \
        .filter_by(task_type='qa') \
        .filter_by(key='status')
    print('* %s with "status" will be deleted e.g. %s' % (q_status.count(),
                                                          q_status.first()))
    q_failures = model.Session.query(model.TaskStatus) \
        .filter_by(task_type='qa') \
        .filter_by(key='openness_score_failure_count')
    print('* %s with openness_score_failure_count to be deleted e.g.\n%s'
          % (q_failures.count(), q_failures.first()))
    q_score = model.Session.query(model.TaskStatus) \
        .filter_by(task_type='qa') \
        .filter_by(key='openness_score')
    print('* %s with openness_score to migrate e.g.\n%s' %
          (q_score.count(), q_score.first()))
    q_reason = model.Session.query(model.TaskStatus) \
        .filter_by(task_type='qa') \
        .filter_by(key='openness_score_reason')
    print('* %s with openness_score_reason to migrate e.g.\n%s' %
          (q_reason.count(), q_reason.first()))

    six.moves.input('Press Enter to continue')

    q_status.delete()
    model.Session.commit()
    print('..."status" deleted')

    q_failures.delete()
    model.Session.commit()
    print('..."openness_score_failure_count" deleted')

    for task_status in q_score:
        reason_task_status = q_reason \
            .filter_by(entity_id=task_status.entity_id) \
            .first()
        if reason_task_status:
            reason = reason_task_status.value
            reason_task_status.delete()
        else:
            reason = None

        task_status.key = 'status'
        task_status.error = json.dumps({
            'reason': reason,
            'format': None,
            'is_broken': None,
            })
        model.Session.commit()
    print('..."openness_score" and "openness_score_reason" migrated')

    count = q_reason.count()
    q_reason.delete()
    model.Session.commit()
    print('... %i remaining "openness_score_reason" deleted' % count)

    model.Session.flush()
    model.Session.remove()
    print('Migration succeeded')


def populate_mqa(ids):
    """
    Calculate and store MQA scores for all datasets or a specific dataset.

    MQA scores are stored only for the first resource of each dataset since
    these scores are the same for all resources in a dataset.

    Args:
        ids: List of dataset IDs or names. If empty, all datasets will be processed.
    """
    from ckan import model
    from ckan.plugins import toolkit
    from ckanext.qa.model import QA

    # Try to import the MQACalculator
    try:
        from ckanext.data_gov_gr.logic.mqa_calculator import MQACalculator
        calculator = MQACalculator()
    except ImportError:
        log.error('MQACalculator not available. Please install ckanext-data-gov-gr extension.')
        sys.exit(1)

    # Get the datasets to process
    packages = []
    if len(ids) > 0:
        for id in ids:
            # try id as a package id/name
            pkg = model.Package.get(id)
            if pkg:
                packages.append(pkg)
                continue
            else:
                log.error('Could not recognize as a package: %r', id)
                sys.exit(1)
    else:
        # all packages
        pkgs = model.Session.query(model.Package)\
                    .filter_by(state='active')\
                    .order_by('name').all()
        packages.extend(pkgs)

    if not packages:
        log.error('No datasets to process')
        sys.exit(1)

    log.info('Datasets to process: %d', len(packages))

    # Process each dataset
    for package in packages:
        log.info('Processing dataset %s (%s resources)', package.name, len(package.resources))

        # Skip datasets with no resources
        if not package.resources:
            log.info('Skipping dataset %s (no resources)', package.name)
            continue

        # Get the dataset as a dictionary
        context = {'model': model, 'session': model.Session, 'ignore_auth': True}
        try:
            dataset_dict = toolkit.get_action('package_show')(context, {'id': package.id})
        except toolkit.ObjectNotFound:
            log.error('Dataset %s not found', package.name)
            continue
        except Exception as e:
            log.error('Error getting dataset %s: %s', package.name, str(e))
            continue

        # Calculate MQA scores
        try:
            mqa_scores = calculator.calculate_all_scores(dataset_dict)
            log.info('MQA scores calculated for dataset %s: %s', package.name, mqa_scores)
        except Exception as e:
            log.error('Error calculating MQA scores for dataset %s: %s', package.name, str(e))
            continue

        # Update QA record only for the first resource
        # MQA scores that we keep here in dataset level, are the same for all resources in the context that we keep here
        if package.resources:
            now = datetime.datetime.now()
            first_resource = package.resources[0]

            # Get or create QA record for the first resource
            qa = QA.get_for_resource(first_resource.id)
            if not qa:
                qa = QA.create(first_resource.id)
                model.Session.add(qa)

            # Update MQA scores (rounded to 1 decimal place)
            qa.mqa_score = round(mqa_scores['percentage'], 1)
            qa.mqa_findability_score = round(mqa_scores['findability'], 1)
            qa.mqa_accessibility_score = round(mqa_scores['accessibility'], 1)
            qa.mqa_interoperability_score = round(mqa_scores['interoperability'], 1)
            qa.mqa_reusability_score = round(mqa_scores['reusability'], 1)
            qa.mqa_contextuality_score = round(mqa_scores['contextuality'], 1)
            qa.updated = now

            # Commit changes
            model.Session.commit()
            log.info('MQA scores updated for dataset %s (resource %s)', package.name, first_resource.id)

            # Clear MQA scores from other resources to avoid redundancy
            if len(package.resources) > 1:
                for resource in package.resources[1:]:
                    qa = QA.get_for_resource(resource.id)
                    if qa:
                        qa.mqa_score = None
                        qa.mqa_findability_score = None
                        qa.mqa_accessibility_score = None
                        qa.mqa_interoperability_score = None
                        qa.mqa_reusability_score = None
                        qa.mqa_contextuality_score = None
                        qa.updated = now
                        model.Session.commit()
                        log.info('MQA scores cleared for resource %s (using dataset-level scores)', resource.id)

    log.info('Completed MQA score population')
