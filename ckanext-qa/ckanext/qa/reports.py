import copy
import logging
from collections import Counter

import ckan.model as model
import ckan.plugins as p

from ckanext.report import lib

try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict


log = logging.getLogger(__name__)


def openness_report(organization):
    if organization is None:
        return openness_index()
    else:
        return openness_for_organization(organization=organization)


def openness_index():
    '''Returns the counts of 5 stars of openness for all organizations.'''

    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    total_score_counts = Counter()
    counts = {}
    # Get all the scores and build up the results by org
    orgs = add_progress_bar(model.Session.query(model.Group)
                            .filter(model.Group.type == 'organization')
                            .filter(model.Group.state == 'active').all())
    for org in orgs:
        scores = []
        # NB org.packages() misses out many - see:
        # http://redmine.dguteam.org.uk/issues/1844
        pkgs = model.Session.query(model.Package) \
                    .filter_by(owner_org=org.id) \
                    .filter_by(state='active') \
                    .all()
        
        # Filter to exclude only the types that can interfere with reports
        pkgs = [pkg for pkg in pkgs if not (hasattr(pkg, 'type') and pkg.type in ['showcase', 'data-service', 'decision', 'harvest'])]
        
        for pkg in pkgs:
            try:
                qa = p.toolkit.get_action('qa_package_openness_show')(context, {'id': pkg.id})
            except p.toolkit.ObjectNotFound:
                log.warning('No QA info for package %s', pkg.name)
                return
            scores.append(qa['openness_score'])
        score_counts = Counter(scores)
        total_score_counts += score_counts
        counts[org.name] = {
            'organization_title': org.title,
            'score_counts': score_counts,
        }

    results = counts

    table = []
    for org_name, org_counts in results.items():
        if not org_counts['score_counts']:  # Let's skip if there are no counts at all.
            continue
        total_stars = sum([k*v for k, v in org_counts['score_counts'].items() if k])
        num_pkgs_scored = sum([v for k, v in org_counts['score_counts'].items()
                              if k is not None])
        average_stars = round(float(total_stars) / num_pkgs_scored, 1) \
            if num_pkgs_scored else 0.0
        row = OrderedDict((
            ('organization_title', results[org_name]['organization_title']),
            ('organization_name', org_name),
            ('total_stars', total_stars),
            ('average_stars', average_stars),
            ))
        row.update(jsonify_counter(org_counts['score_counts']))
        table.append(row)

    table.sort(key=lambda x: (-x['total_stars'],
                              -x['average_stars']))

    # Get total number of packages & resources
    num_packages = model.Session.query(model.Package)\
                        .filter_by(state='active')\
                        .count()
    return {'table': table,
            'total_score_counts': jsonify_counter(total_score_counts),
            'num_packages_scored': sum(total_score_counts.values()),
            'num_packages': num_packages,
            }


def openness_for_organization(organization=None):
    org = model.Group.get(organization)
    if not org:
        raise p.toolkit.ObjectNotFound

    orgs = [org]

    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    score_counts = Counter()
    rows = []
    num_packages = 0
    for org in orgs:
        # NB org.packages() misses out many - see:
        # http://redmine.dguteam.org.uk/issues/1844
        pkgs = model.Session.query(model.Package) \
                    .filter_by(owner_org=org.id) \
                    .filter_by(state='active') \
                    .all()
        
        # Filter to exclude only the types that can interfere with reports
        pkgs = [pkg for pkg in pkgs if not (hasattr(pkg, 'type') and pkg.type in ['showcase', 'data-service', 'decision', 'harvest'])]
        
        num_packages += len(pkgs)
        for pkg in pkgs:
            try:
                qa = p.toolkit.get_action('qa_package_openness_show')(context, {'id': pkg.id})
            except p.toolkit.ObjectNotFound:
                log.warning('No QA info for package %s', pkg.name)
                return
            rows.append(OrderedDict((
                ('dataset_name', pkg.name),
                ('dataset_title', pkg.title),
                ('dataset_notes', lib.dataset_notes(pkg)),
                ('organization_name', org.name),
                ('organization_title', org.title),
                ('openness_score', qa['openness_score']),
                ('openness_score_reason', qa['openness_score_reason']),
                )))
            score_counts[qa['openness_score']] += 1

    total_stars = sum([k*v for k, v in score_counts.items() if k])
    num_pkgs_with_stars = sum([v for k, v in score_counts.items()
                               if k is not None])
    average_stars = round(float(total_stars) / num_pkgs_with_stars, 1) \
        if num_pkgs_with_stars else 0.0

    return {'table': rows,
            'score_counts': jsonify_counter(score_counts),
            'total_stars': total_stars,
            'average_stars': average_stars,
            'num_packages_scored': len(rows),
            'num_packages': num_packages,
            }


def openness_report_combinations():
    for organization in lib.all_organizations(include_none=True):
        yield {'organization': organization}


openness_report_info = {
    'name': 'openness',
    'title': p.toolkit._('Openness (Five Stars)'),
    'description': p.toolkit._('Datasets graded on Tim Berners Lees\' Five Stars of Openness - openly licensed,'
                   ' openly accessible, structured, open format, URIs for entities, linked.'),
    'option_defaults': OrderedDict((('organization', None),
                                    )),
    'option_combinations': openness_report_combinations,
    'generate': openness_report,
    'template': 'report/openness.html',
    }


def jsonify_counter(counter):
    # When counters are stored as JSON, integers become strings. Do the conversion
    # here to ensure that when you run the report the first time, you get the same
    # response as subsequent times that go through the cache/JSON.
    return dict((str(k) if k is not None else k, v) for k, v in counter.items())


def add_progress_bar(iterable, caption=None):
    try:
        # Add a progress bar, if it is installed
        import progressbar
        bar = progressbar.ProgressBar(widgets=[
            (caption + ' ') if caption else '',
            progressbar.Percentage(), ' ',
            progressbar.Bar(), ' ', progressbar.ETA()])
        return bar(iterable)
    except ImportError:
        return iterable


def metadata_quality_report(organization):
    # We no longer recalculate MQA scores when viewing the report
    # The report everytime is generated, will use the existing MQA scores from the QA table

    if organization is None:
        return metadata_quality_index()
    else:
        return metadata_quality_for_organization(organization=organization)


def metadata_quality_index():
    '''Returns the metadata quality metrics for all organizations.'''

    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    counts = {}


    # Get all organizations
    orgs = add_progress_bar(model.Session.query(model.Group)
                          .filter(model.Group.type == 'organization')
                          .filter(model.Group.state == 'active').all())

    for org in orgs:
        # Get all active packages for this organization
        pkgs = model.Session.query(model.Package) \
                  .filter_by(owner_org=org.id) \
                  .filter_by(state='active') \
                  .all()
        
        # Filter to exclude only the types that can interfere with reports
        pkgs = [pkg for pkg in pkgs if not (hasattr(pkg, 'type') and pkg.type in ['showcase', 'data-service', 'decision', 'harvest'])]

        # Initialize counters for this organization
        org_counts = {
            'total_packages': len(pkgs),
            'total_mqa_score': 0,
            'packages_with_mqa_score': 0,
            'total_findability': 0,
            'total_accessibility': 0,
            'total_interoperability': 0,
            'total_reusability': 0,
            'total_contextuality': 0,
            'resources_with_mqa': 0
        }

        # Count metadata quality metrics for each package
        for pkg in pkgs:
            # Try to get MQA score from QA table
            from ckanext.qa.model import QA, aggregate_qa_for_a_dataset
            qa_objs = QA.get_for_package(pkg.id)

            # If we have QA objects with MQA scores, use them
            if qa_objs:
                qa_dict = aggregate_qa_for_a_dataset(qa_objs)
                mqa_score = qa_dict.get('mqa_score')

                if mqa_score is not None:
                    # Use the stored MQA score
                    org_counts['total_mqa_score'] += mqa_score
                    org_counts['packages_with_mqa_score'] += 1
                    log.info(f"Using stored MQA score for package {pkg.name}: {mqa_score}")

        # Get all resources for this organization and calculate dimension scores
        resources = model.Session.query(model.Resource)\
            .join(model.Package, model.Resource.package_id == model.Package.id)\
            .filter(model.Package.owner_org == org.id)\
            .filter(model.Package.state == 'active')\
            .all()

        # Calculate MQA dimension scores for this organization
        for resource in resources:
            qa = QA.get_for_resource(resource.id)
            if qa and qa.mqa_score is not None:
                org_counts['resources_with_mqa'] += 1
                if qa.mqa_findability_score is not None:
                    org_counts['total_findability'] += qa.mqa_findability_score
                if qa.mqa_accessibility_score is not None:
                    org_counts['total_accessibility'] += qa.mqa_accessibility_score
                if qa.mqa_interoperability_score is not None:
                    org_counts['total_interoperability'] += qa.mqa_interoperability_score
                if qa.mqa_reusability_score is not None:
                    org_counts['total_reusability'] += qa.mqa_reusability_score
                if qa.mqa_contextuality_score is not None:
                    org_counts['total_contextuality'] += qa.mqa_contextuality_score

        # Store organization counts
        counts[org.name] = {
            'organization_title': org.title,
            'quality_counts': org_counts
        }

    results = counts

    # Build the table for the report
    table = []
    for org_name, org_data in results.items():
        quality_counts = org_data['quality_counts']
        total_packages = quality_counts['total_packages']

        if total_packages == 0:
            continue

        # Calculate overall quality score - only use MQA scores
        if quality_counts.get('packages_with_mqa_score', 0) > 0:
            overall_score = round(quality_counts['total_mqa_score'] / quality_counts['packages_with_mqa_score'], 1)
            log.info(f"Using average MQA score for organization {org_name}: {overall_score} (from {quality_counts['packages_with_mqa_score']} packages)")

            # Calculate average MQA dimension scores from the totals we collected
            mqa_findability_score = None
            mqa_accessibility_score = None
            mqa_interoperability_score = None
            mqa_reusability_score = None
            mqa_contextuality_score = None

            if quality_counts.get('resources_with_mqa', 0) > 0:
                if quality_counts.get('total_findability', 0) > 0:
                    mqa_findability_score = round(quality_counts['total_findability'] / quality_counts['resources_with_mqa'], 1)
                if quality_counts.get('total_accessibility', 0) > 0:
                    mqa_accessibility_score = round(quality_counts['total_accessibility'] / quality_counts['resources_with_mqa'], 1)
                if quality_counts.get('total_interoperability', 0) > 0:
                    mqa_interoperability_score = round(quality_counts['total_interoperability'] / quality_counts['resources_with_mqa'], 1)
                if quality_counts.get('total_reusability', 0) > 0:
                    mqa_reusability_score = round(quality_counts['total_reusability'] / quality_counts['resources_with_mqa'], 1)
                if quality_counts.get('total_contextuality', 0) > 0:
                    mqa_contextuality_score = round(quality_counts['total_contextuality'] / quality_counts['resources_with_mqa'], 1)
        else:
            # If no MQA scores are available, set overall score to None
            overall_score = None
            mqa_findability_score = None
            mqa_accessibility_score = None
            mqa_interoperability_score = None
            mqa_reusability_score = None
            mqa_contextuality_score = None
            log.info(f"No MQA scores available for organization {org_name}, setting overall score to None")


        row = OrderedDict((
            ('organization_title', org_data['organization_title']),
            ('organization_name', org_name),
            ('total_packages', total_packages),
            ('overall_score', overall_score),
            ('mqa_findability_score', mqa_findability_score),
            ('mqa_accessibility_score', mqa_accessibility_score),
            ('mqa_interoperability_score', mqa_interoperability_score),
            ('mqa_reusability_score', mqa_reusability_score),
            ('mqa_contextuality_score', mqa_contextuality_score)
        ))

        table.append(row)

    # Sort by overall score (descending)
    # Handle None values by placing them at the end
    table.sort(key=lambda x: float('-inf') if x['overall_score'] is None else -x['overall_score'])

    # Get total number of packages
    num_packages = model.Session.query(model.Package)\
                      .filter_by(state='active')\
                      .count()

    return {
        'table': table,
        'total_packages': num_packages,

    }

def metadata_quality_for_organization(organization=None):
    '''Returns the metadata quality metrics for a specific organization.'''

    org = model.Group.get(organization)
    if not org:
        raise p.toolkit.ObjectNotFound

    orgs = [org]

    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    rows = []
    num_packages = 0
    total_mqa_score = 0
    packages_with_mqa_score = 0

    for org in orgs:
        # Get all active packages for this organization
        pkgs = model.Session.query(model.Package) \
                  .filter_by(owner_org=org.id) \
                  .filter_by(state='active') \
                  .all()
        
        # Filter to exclude only the types that can interfere with reports
        pkgs = [pkg for pkg in pkgs if not (hasattr(pkg, 'type') and pkg.type in ['showcase', 'data-service', 'decision', 'harvest'])]

        num_packages += len(pkgs)

        for pkg in pkgs:
            # Try to get MQA score from QA table
            from ckanext.qa.model import QA, aggregate_qa_for_a_dataset
            qa_objs = QA.get_for_package(pkg.id)

            # Default MQA scores to None
            mqa_quality_score = None
            mqa_findability_score = None
            mqa_accessibility_score = None
            mqa_interoperability_score = None
            mqa_reusability_score = None
            mqa_contextuality_score = None

            # If we have QA objects with MQA scores, use them
            if qa_objs:
                qa_dict = aggregate_qa_for_a_dataset(qa_objs)
                mqa_score = qa_dict.get('mqa_score')

                if mqa_score is not None:
                    # Use the stored MQA score (rounded to 1 decimal place)
                    mqa_quality_score = round(mqa_score, 1) if mqa_score is not None else None
                    packages_with_mqa_score += 1
                    total_mqa_score += mqa_score
                    log.info(f"Using stored MQA score for package {pkg.name}: {mqa_score}")

                    # Get individual MQA dimension scores and round to 1 decimal place
                    mqa_findability_score = round(qa_dict.get('mqa_findability_score', 0), 1) if qa_dict.get('mqa_findability_score') is not None else None
                    mqa_accessibility_score = round(qa_dict.get('mqa_accessibility_score', 0), 1) if qa_dict.get('mqa_accessibility_score') is not None else None
                    mqa_interoperability_score = round(qa_dict.get('mqa_interoperability_score', 0), 1) if qa_dict.get('mqa_interoperability_score') is not None else None
                    mqa_reusability_score = round(qa_dict.get('mqa_reusability_score', 0), 1) if qa_dict.get('mqa_reusability_score') is not None else None
                    mqa_contextuality_score = round(qa_dict.get('mqa_contextuality_score', 0), 1) if qa_dict.get('mqa_contextuality_score') is not None else None


            # Add row for this package
            rows.append(OrderedDict((
                    ('dataset_name', pkg.name),
                    ('dataset_title', lib.resolve_dataset_title(pkg)),
                    ('dataset_notes', lib.dataset_notes(pkg)),
                    ('organization_name', org.name),
                    ('organization_title', org.title),
                    ('mqa_quality_score', mqa_quality_score),
                    ('mqa_findability_score', mqa_findability_score),
                    ('mqa_accessibility_score', mqa_accessibility_score),
                    ('mqa_interoperability_score', mqa_interoperability_score),
                    ('mqa_reusability_score', mqa_reusability_score),
                    ('mqa_contextuality_score', mqa_contextuality_score)
                )))


    # Calculate overall quality score - only use MQA scores
    if packages_with_mqa_score > 0:
        overall_score = round(total_mqa_score / packages_with_mqa_score, 1)
        log.info(f"Using average MQA score for organization {org.name}: {overall_score} (from {packages_with_mqa_score} packages)")
    else:
        # If no MQA scores are available, set overall score to None
        overall_score = None
        log.info(f"No MQA scores available for organization {org.name}, setting overall score to None")

    return {
        'table': rows,
        'num_packages': num_packages,
        'overall_score': overall_score
    }


def metadata_quality_report_combinations():
    for organization in lib.all_organizations(include_none=True):
        yield {'organization': organization}


metadata_quality_report_info = {
    'name': 'metadata-quality',
    'title': p.toolkit._('Metadata Quality'),
    'description': p.toolkit._('Datasets graded on metadata quality, based on the MQA method'),
    'option_defaults': OrderedDict((('organization', None),
                                  )),
    'option_combinations': metadata_quality_report_combinations,
    'generate': metadata_quality_report,
    'template': 'report/metadata_quality.html',
}