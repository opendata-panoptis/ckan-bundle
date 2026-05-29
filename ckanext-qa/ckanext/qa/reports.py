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

EXCLUDED_PACKAGE_TYPES = frozenset(['showcase', 'data-service', 'decision', 'harvest'])


def _filter_report_packages(packages):
    return [
        pkg for pkg in packages
        if not (hasattr(pkg, 'type') and pkg.type in EXCLUDED_PACKAGE_TYPES)
    ]


def _get_active_report_packages_for_org(org_id):
    # NB org.packages() misses out many - see:
    # http://redmine.dguteam.org.uk/issues/1844
    pkgs = model.Session.query(model.Package) \
        .filter_by(owner_org=org_id) \
        .filter_by(state='active') \
        .all()
    return _filter_report_packages(pkgs)


def _get_active_qa_for_packages(package_ids):
    from ckanext.qa.model import QA

    if not package_ids:
        return {}

    qa_rows = model.Session.query(QA) \
        .join(model.Resource, QA.resource_id == model.Resource.id) \
        .filter(QA.package_id.in_(package_ids)) \
        .filter(model.Resource.state == 'active') \
        .all()

    qa_by_package = {}
    for qa_row in qa_rows:
        qa_by_package.setdefault(qa_row.package_id, []).append(qa_row)
    return qa_by_package


def _iter_qa_rows(qa_by_package):
    for package_qa_rows in qa_by_package.values():
        for qa_row in package_qa_rows:
            yield qa_row


def _calculate_mqa_dimension_totals(qa_rows):
    totals = {
        'total_findability': 0,
        'total_accessibility': 0,
        'total_interoperability': 0,
        'total_reusability': 0,
        'total_contextuality': 0,
        'findability_values_count': 0,
        'accessibility_values_count': 0,
        'interoperability_values_count': 0,
        'reusability_values_count': 0,
        'contextuality_values_count': 0,
        'resources_with_mqa': 0,
    }

    for qa_row in qa_rows:
        if qa_row.mqa_score is None:
            continue

        totals['resources_with_mqa'] += 1
        if qa_row.mqa_findability_score is not None:
            totals['total_findability'] += qa_row.mqa_findability_score
            totals['findability_values_count'] += 1
        if qa_row.mqa_accessibility_score is not None:
            totals['total_accessibility'] += qa_row.mqa_accessibility_score
            totals['accessibility_values_count'] += 1
        if qa_row.mqa_interoperability_score is not None:
            totals['total_interoperability'] += qa_row.mqa_interoperability_score
            totals['interoperability_values_count'] += 1
        if qa_row.mqa_reusability_score is not None:
            totals['total_reusability'] += qa_row.mqa_reusability_score
            totals['reusability_values_count'] += 1
        if qa_row.mqa_contextuality_score is not None:
            totals['total_contextuality'] += qa_row.mqa_contextuality_score
            totals['contextuality_values_count'] += 1

    return totals


def _calculate_mqa_dimension_scores(dimension_totals):
    scores = {
        'mqa_findability_score': None,
        'mqa_accessibility_score': None,
        'mqa_interoperability_score': None,
        'mqa_reusability_score': None,
        'mqa_contextuality_score': None,
    }
    resources_with_mqa = dimension_totals.get('resources_with_mqa', 0)

    if resources_with_mqa > 0:
        if dimension_totals.get('findability_values_count', 0) > 0:
            scores['mqa_findability_score'] = round(dimension_totals['total_findability'] / resources_with_mqa, 1)
        if dimension_totals.get('accessibility_values_count', 0) > 0:
            scores['mqa_accessibility_score'] = round(dimension_totals['total_accessibility'] / resources_with_mqa, 1)
        if dimension_totals.get('interoperability_values_count', 0) > 0:
            scores['mqa_interoperability_score'] = round(dimension_totals['total_interoperability'] / resources_with_mqa, 1)
        if dimension_totals.get('reusability_values_count', 0) > 0:
            scores['mqa_reusability_score'] = round(dimension_totals['total_reusability'] / resources_with_mqa, 1)
        if dimension_totals.get('contextuality_values_count', 0) > 0:
            scores['mqa_contextuality_score'] = round(dimension_totals['total_contextuality'] / resources_with_mqa, 1)

    return scores


def _accumulate_mqa_dimension_totals_from_scores(dimension_totals, mqa_scores):
    if not mqa_scores:
        return

    dimension_totals['resources_with_mqa'] += 1

    score_mapping = (
        ('mqa_findability_score', 'total_findability', 'findability_values_count'),
        ('mqa_accessibility_score', 'total_accessibility', 'accessibility_values_count'),
        ('mqa_interoperability_score', 'total_interoperability', 'interoperability_values_count'),
        ('mqa_reusability_score', 'total_reusability', 'reusability_values_count'),
        ('mqa_contextuality_score', 'total_contextuality', 'contextuality_values_count'),
    )

    for score_key, total_key, count_key in score_mapping:
        score = mqa_scores.get(score_key)
        if score is not None:
            dimension_totals[total_key] += score
            dimension_totals[count_key] += 1


def _normalize_mqa_score(value):
    if value is None:
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return None


def _extract_mqa_scores_from_pkg_dict(pkg_dict):
    qa_dict = pkg_dict.get('qa')
    if not isinstance(qa_dict, dict):
        return None

    mqa_score = _normalize_mqa_score(qa_dict.get('mqa_score'))
    if mqa_score is None:
        return None

    return {
        'mqa_score': mqa_score,
        'mqa_findability_score': _normalize_mqa_score(qa_dict.get('mqa_findability_score')),
        'mqa_accessibility_score': _normalize_mqa_score(qa_dict.get('mqa_accessibility_score')),
        'mqa_interoperability_score': _normalize_mqa_score(qa_dict.get('mqa_interoperability_score')),
        'mqa_reusability_score': _normalize_mqa_score(qa_dict.get('mqa_reusability_score')),
        'mqa_contextuality_score': _normalize_mqa_score(qa_dict.get('mqa_contextuality_score')),
    }


def _build_mqa_fallback_for_dataset_without_resources(pkg):
    active_resources = [
        resource for resource in getattr(pkg, 'resources', [])
        if getattr(resource, 'state', None) == 'active'
    ]
    if active_resources:
        return None

    try:
        from ckanext.data_gov_gr.logic.mqa_calculator import MQACalculator
    except ImportError:
        return None

    context = {'model': model, 'session': model.Session, 'ignore_auth': True}

    try:
        pkg_dict = p.toolkit.get_action('package_show')(context, {'id': pkg.id})
    except Exception as e:
        log.warning(
            'Error loading dataset %s for fallback MQA score calculation: %s',
            pkg.id,
            str(e)
        )
        return None

    if pkg_dict.get('resources'):
        return None

    existing_mqa_scores = _extract_mqa_scores_from_pkg_dict(pkg_dict)
    if existing_mqa_scores is not None:
        return existing_mqa_scores

    try:
        check_urls = p.toolkit.asbool(
            p.toolkit.config.get('ckanext.data_gov_gr.mqa.check_urls', True)
        )
        mqa_scores = MQACalculator(check_urls=check_urls).calculate_all_scores(pkg_dict)
    except Exception as e:
        log.warning(
            'Error calculating fallback MQA score for dataset %s: %s',
            pkg.id,
            str(e)
        )
        return None

    return {
        'mqa_score': round(mqa_scores.get('percentage', 0), 1),
        'mqa_findability_score': round(mqa_scores.get('findability', 0), 1),
        'mqa_accessibility_score': round(mqa_scores.get('accessibility', 0), 1),
        'mqa_interoperability_score': round(mqa_scores.get('interoperability', 0), 1),
        'mqa_reusability_score': round(mqa_scores.get('reusability', 0), 1),
        'mqa_contextuality_score': round(mqa_scores.get('contextuality', 0), 1),
    }


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
    total_packages = 0
    # Get all the scores and build up the results by org
    orgs = add_progress_bar(model.Session.query(model.Group)
                            .filter(model.Group.type == 'organization')
                            .filter(model.Group.state == 'active').all())
    for org in orgs:
        scores = []
        pkgs = _get_active_report_packages_for_org(org.id)
        total_packages += len(pkgs)

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

    return {'table': table,
            'total_score_counts': jsonify_counter(total_score_counts),
            'num_packages_scored': sum(total_score_counts.values()),
            'num_packages': total_packages,
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
        pkgs = _get_active_report_packages_for_org(org.id)

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


def openness_post_access_filter(data, context):
    table = data.get('table', [])
    if not table:
        data['score_counts'] = {}
        data['num_packages_scored'] = 0
        data['num_packages'] = 0
        data['average_stars'] = 0.0
        return data

    # Index view rows are organization aggregates and do not reference
    # individual datasets, so they cannot be safely recomputed here.
    if 'dataset_name' not in table[0]:
        return data

    score_counts = {}
    total_stars = 0.0
    num_packages_scored = 0

    for row in table:
        score = row.get('openness_score')
        try:
            score = float(score) if score is not None else None
        except (TypeError, ValueError):
            score = None

        if score is None:
            key = 'null'
        else:
            key = str(int(score))
            total_stars += score
            num_packages_scored += 1

        score_counts[key] = score_counts.get(key, 0) + 1

    data['score_counts'] = score_counts
    data['num_packages_scored'] = num_packages_scored
    data['num_packages'] = len(table)
    data['average_stars'] = round(float(total_stars) / num_packages_scored, 1) \
        if num_packages_scored else 0.0
    return data


openness_report_info = {
    'name': 'openness',
    'title': p.toolkit._('Openness (Five Stars)'),
    'description': p.toolkit._('Datasets graded on Tim Berners Lees\' Five Stars of Openness - openly licensed,'
                   ' openly accessible, structured, open format, URIs for entities, linked.'),
    'option_defaults': OrderedDict((('organization', None),
                                    )),
    'option_combinations': openness_report_combinations,
    'generate': openness_report,
    'post_access_filter': openness_post_access_filter,
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

    from ckanext.qa.model import aggregate_qa_for_a_dataset

    counts = {}
    total_packages_count = 0


    # Get all organizations
    orgs = add_progress_bar(model.Session.query(model.Group)
                          .filter(model.Group.type == 'organization')
                          .filter(model.Group.state == 'active').all())

    for org in orgs:
        pkgs = _get_active_report_packages_for_org(org.id)
        pkg_ids = [pkg.id for pkg in pkgs]
        qa_by_package = _get_active_qa_for_packages(pkg_ids)
        total_packages_count += len(pkgs)

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
        org_counts.update(
            _calculate_mqa_dimension_totals(_iter_qa_rows(qa_by_package))
        )

        # Count metadata quality metrics for each package
        for pkg in pkgs:
            qa_objs = qa_by_package.get(pkg.id, [])
            mqa_score = None

            # If we have QA objects with MQA scores, use them
            if qa_objs:
                qa_dict = aggregate_qa_for_a_dataset(qa_objs)
                mqa_score = qa_dict.get('mqa_score')
                if mqa_score is not None:
                    log.info(f"Using stored MQA score for package {pkg.name}: {mqa_score}")

            if mqa_score is None:
                fallback_mqa_scores = _build_mqa_fallback_for_dataset_without_resources(pkg)
                if fallback_mqa_scores:
                    mqa_score = fallback_mqa_scores.get('mqa_score')
                    if mqa_score is not None:
                        _accumulate_mqa_dimension_totals_from_scores(org_counts, fallback_mqa_scores)
                        log.info(f"Using fallback MQA score for dataset without resources {pkg.name}: {mqa_score}")

            if mqa_score is not None:
                org_counts['total_mqa_score'] += mqa_score
                org_counts['packages_with_mqa_score'] += 1

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
        org_total_packages = quality_counts['total_packages']

        if org_total_packages == 0:
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

            dimension_scores = _calculate_mqa_dimension_scores(quality_counts)
            mqa_findability_score = dimension_scores['mqa_findability_score']
            mqa_accessibility_score = dimension_scores['mqa_accessibility_score']
            mqa_interoperability_score = dimension_scores['mqa_interoperability_score']
            mqa_reusability_score = dimension_scores['mqa_reusability_score']
            mqa_contextuality_score = dimension_scores['mqa_contextuality_score']
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
            ('total_packages', org_total_packages),
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

    return {
        'table': table,
        'total_packages': total_packages_count,

    }

def metadata_quality_for_organization(organization=None):
    '''Returns the metadata quality metrics for a specific organization.'''

    from ckanext.qa.model import aggregate_qa_for_a_dataset

    org = model.Group.get(organization)
    if not org:
        raise p.toolkit.ObjectNotFound

    orgs = [org]

    rows = []
    num_packages = 0
    total_mqa_score = 0
    packages_with_mqa_score = 0

    for org in orgs:
        pkgs = _get_active_report_packages_for_org(org.id)
        pkg_ids = [pkg.id for pkg in pkgs]
        qa_by_package = _get_active_qa_for_packages(pkg_ids)

        num_packages += len(pkgs)

        for pkg in pkgs:
            # Try to get MQA score from QA table
            qa_objs = qa_by_package.get(pkg.id, [])
            dimension_totals = _calculate_mqa_dimension_totals(qa_objs)
            dimension_scores = _calculate_mqa_dimension_scores(dimension_totals)

            # Default MQA scores to None
            mqa_quality_score = None
            mqa_findability_score = dimension_scores['mqa_findability_score']
            mqa_accessibility_score = dimension_scores['mqa_accessibility_score']
            mqa_interoperability_score = dimension_scores['mqa_interoperability_score']
            mqa_reusability_score = dimension_scores['mqa_reusability_score']
            mqa_contextuality_score = dimension_scores['mqa_contextuality_score']

            # If we have QA objects with MQA scores, use them
            if qa_objs:
                qa_dict = aggregate_qa_for_a_dataset(qa_objs)
                mqa_score = qa_dict.get('mqa_score')

                if mqa_score is not None:
                    # Use the stored MQA score (rounded to 1 decimal place)
                    mqa_quality_score = round(mqa_score, 1)
                    packages_with_mqa_score += 1
                    total_mqa_score += mqa_score
                    log.info(f"Using stored MQA score for package {pkg.name}: {mqa_score}")

            if mqa_quality_score is None:
                fallback_mqa_scores = _build_mqa_fallback_for_dataset_without_resources(pkg)
                if fallback_mqa_scores:
                    mqa_quality_score = fallback_mqa_scores.get('mqa_score')
                    if mqa_quality_score is not None:
                        mqa_findability_score = fallback_mqa_scores.get('mqa_findability_score')
                        mqa_accessibility_score = fallback_mqa_scores.get('mqa_accessibility_score')
                        mqa_interoperability_score = fallback_mqa_scores.get('mqa_interoperability_score')
                        mqa_reusability_score = fallback_mqa_scores.get('mqa_reusability_score')
                        mqa_contextuality_score = fallback_mqa_scores.get('mqa_contextuality_score')
                        packages_with_mqa_score += 1
                        total_mqa_score += mqa_quality_score
                        log.info(f"Using fallback MQA score for dataset without resources {pkg.name}: {mqa_quality_score}")


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


def metadata_quality_post_access_filter(data, context):
    table = data.get('table', [])
    if not table:
        data['num_packages'] = 0
        data['overall_score'] = None
        return data

    # Index view rows are organization aggregates and do not reference
    # individual datasets, so they cannot be safely recomputed here.
    if 'dataset_name' not in table[0]:
        return data

    scores = []
    for row in table:
        score = row.get('mqa_quality_score')
        try:
            score = float(score) if score is not None else None
        except (TypeError, ValueError):
            score = None
        if score is not None:
            scores.append(score)

    data['num_packages'] = len(table)
    data['overall_score'] = round(sum(scores) / len(scores), 1) if scores else None
    return data


metadata_quality_report_info = {
    'name': 'metadata-quality',
    'title': p.toolkit._('Metadata Quality'),
    'description': p.toolkit._('Datasets graded on metadata quality, based on the MQA method'),
    'option_defaults': OrderedDict((('organization', None),
                                  )),
    'option_combinations': metadata_quality_report_combinations,
    'generate': metadata_quality_report,
    'post_access_filter': metadata_quality_post_access_filter,
    'template': 'report/metadata_quality.html',
}
