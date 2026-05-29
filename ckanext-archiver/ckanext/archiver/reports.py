try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from ckan.common import _
import ckan.model as model
import ckan.plugins as p

from ckanext.report import lib
from ckanext.archiver.resource_evaluation import (
    BROKEN,
    DOWNLOADALL_REASON,
    NON_EVALUABLE,
    get_non_evaluable_resource_reason,
    get_resource_evaluation_state,
)


EXCLUDED_PACKAGE_TYPES = ['showcase', 'data-service', 'decision', 'harvest']


def broken_links(organization):
    if organization is None:
        return broken_links_index()
    else:
        return broken_links_for_organization(organization=organization)


def _filtered_packages_query(org_id=None):
    query = model.Session.query(model.Package) \
        .filter(model.Package.state == 'active') \
        .filter(~model.Package.type.in_(EXCLUDED_PACKAGE_TYPES))
    if org_id:
        query = query.filter(model.Package.owner_org == org_id)
    return query


def _resource_rows_query(org_id=None):
    from ckanext.archiver.model import Archival

    query = (model.Session.query(model.Resource, model.Package, model.Group,
                                 Archival)
        .join(model.Package, model.Resource.package_id == model.Package.id)
        .join(model.Group, model.Package.owner_org == model.Group.id)
        .outerjoin(Archival, Archival.resource_id == model.Resource.id)
        .filter(model.Group.type == 'organization')
        .filter(model.Group.state == 'active')
        .filter(model.Package.state == 'active')
        .filter(model.Resource.state == 'active')
        .filter(~model.Package.type.in_(EXCLUDED_PACKAGE_TYPES)))
    if org_id:
        query = query.filter(model.Package.owner_org == org_id)
    return query


def _get_archived_resource(resource, archival):
    if not archival:
        return resource
    if p.toolkit.check_ckan_version(max_version='2.8.99'):
        return model.Session.query(model.ResourceRevision) \
            .filter_by(id=resource.id) \
            .filter_by(revision_timestamp=archival.resource_timestamp) \
            .first() or resource
    return resource


def _get_via(pkg):
    via = ''
    er = pkg.extras.get('external_reference', '')
    if er == 'ONSHUB':
        via = 'Stats Hub'
    elif er.startswith('DATA4NR'):
        via = 'Data4nr'
    return via


def _get_non_evaluable_resource_url(resource, pkg):
    reason = get_non_evaluable_resource_reason(resource)
    if reason == DOWNLOADALL_REASON:
        return p.toolkit.url_for('dataset.read', id=pkg.name)
    return resource.url


def _build_row(resource, pkg, org, archival):
    evaluation_state = get_resource_evaluation_state(resource, archival)
    if not evaluation_state:
        return None

    archived_resource = _get_archived_resource(resource, archival)

    if evaluation_state == NON_EVALUABLE:
        resource_url = _get_non_evaluable_resource_url(resource, pkg)
        status = 'Chose not to download'
        reason = get_non_evaluable_resource_reason(resource)
        failure_count = 0
        first_failure = None
        last_success = None
        url_redirected_to = None
    else:
        resource_url = archived_resource.url
        status = archival.status if archival else 'not recorded'
        reason = archival.reason if archival else 'not recorded'
        failure_count = archival.failure_count if archival else None
        first_failure = archival.first_failure.isoformat() \
            if archival and archival.first_failure else None
        last_success = archival.last_success.isoformat() \
            if archival and archival.last_success else None
        url_redirected_to = archival.url_redirected_to if archival else None

    return OrderedDict((
        ('dataset_title', lib.resolve_dataset_title(pkg)),
        ('dataset_name', pkg.name),
        ('dataset_notes', lib.dataset_notes(pkg)),
        ('organization_title', org.title),
        ('organization_name', org.name),
        ('resource_position', resource.position),
        ('resource_id', resource.id),
        ('resource_url', resource_url),
        ('url_up_to_date', resource.url == archived_resource.url),
        ('via', _get_via(pkg)),
        ('evaluation_state', evaluation_state),
        ('first_failure', first_failure),
        ('last_updated', archival.updated.isoformat()
         if archival and archival.updated else None),
        ('last_success', last_success),
        ('url_redirected_to', url_redirected_to),
        ('reason', reason),
        ('status', status),
        ('failure_count', failure_count),
    ))


def broken_links_index():
    '''Returns the count of broken links for all organizations.'''
    counts = {}
    orgs = model.Session.query(model.Group) \
        .filter(model.Group.type == 'organization') \
        .filter(model.Group.state == 'active').all()

    for org in add_progress_bar(orgs):
        resource_rows = _resource_rows_query(org.id).all()
        broken_dataset_ids = set()
        broken_resources = 0
        non_evaluable_resources = 0

        for resource, pkg, _group, archival in resource_rows:
            evaluation_state = get_resource_evaluation_state(resource, archival)
            if evaluation_state == BROKEN:
                broken_resources += 1
                broken_dataset_ids.add(pkg.id)
            elif evaluation_state == NON_EVALUABLE:
                non_evaluable_resources += 1

        counts[org.name] = {
            'organization_title': org.title,
            'broken_packages': len(broken_dataset_ids),
            'broken_resources': broken_resources,
            'non_evaluable_resources': non_evaluable_resources,
            'packages': _filtered_packages_query(org.id).count(),
            'resources': len(resource_rows),
        }

    results = counts

    data = []
    num_broken_packages = 0
    num_broken_resources = 0
    num_non_evaluable_resources = 0
    num_packages = 0
    num_resources = 0
    for org_name, org_counts in results.items():
        data.append(OrderedDict((
            ('organization_title', results[org_name]['organization_title']),
            ('organization_name', org_name),
            ('package_count', org_counts['packages']),
            ('resource_count', org_counts['resources']),
            ('broken_package_count', org_counts['broken_packages']),
            ('broken_package_percent',
             lib.percent(org_counts['broken_packages'], org_counts['packages'])),
            ('broken_resource_count', org_counts['broken_resources']),
            ('broken_resource_percent',
             lib.percent(org_counts['broken_resources'], org_counts['resources'])),
            ('non_evaluable_resource_count',
             org_counts['non_evaluable_resources']),
        )))
        num_broken_packages += org_counts['broken_packages']
        num_broken_resources += org_counts['broken_resources']
        num_non_evaluable_resources += org_counts['non_evaluable_resources']
        num_packages += org_counts['packages']
        num_resources += org_counts['resources']

    data.sort(key=lambda x: (-x['broken_package_count'],
                             -x['broken_resource_count']))

    return {'table': data,
            'num_broken_packages': num_broken_packages,
            'num_broken_resources': num_broken_resources,
            'num_non_evaluable_resources': num_non_evaluable_resources,
            'num_packages': num_packages,
            'num_resources': num_resources,
            'broken_package_percent': lib.percent(num_broken_packages, num_packages),
            'broken_resource_percent': lib.percent(num_broken_resources, num_resources),
            }


def broken_links_for_organization(organization):
    '''
    Returns a dictionary detailing broken resource links for the organization
    or if organization it returns the index page for all organizations.

    params:
      organization - name of an organization

    Returns:
    {'organization_name': 'cabinet-office',
     'organization_title:': 'Cabinet Office',
     'table': [
       {'package_name', 'package_title', 'resource_url', 'status', 'reason', 'last_success',
       'first_failure', 'failure_count', 'last_updated'}
      ...]

    '''
    org = model.Group.get(organization)
    if not org:
        raise p.toolkit.ObjectNotFound()

    name = org.name
    title = org.title

    results = []
    broken_package_names = set()
    num_broken_resources = 0
    num_non_evaluable_resources = 0

    for resource, pkg, group, archival in _resource_rows_query(org.id).all():
        row_data = _build_row(resource, pkg, group, archival)
        if not row_data:
            continue

        results.append(row_data)
        if row_data['evaluation_state'] == BROKEN:
            num_broken_resources += 1
            broken_package_names.add(pkg.name)
        elif row_data['evaluation_state'] == NON_EVALUABLE:
            num_non_evaluable_resources += 1

    num_broken_packages = len(broken_package_names)

    # Get total number of packages & resources (excluding showcases)
    num_packages = _filtered_packages_query(org.id).count()
    num_resources = _resource_rows_query(org.id).count()

    return {'organization_name': name,
            'organization_title': title,
            'num_broken_packages': num_broken_packages,
            'num_broken_resources': num_broken_resources,
            'num_non_evaluable_resources': num_non_evaluable_resources,
            'num_packages': num_packages,
            'num_resources': num_resources,
            'broken_package_percent': lib.percent(num_broken_packages, num_packages),
            'broken_resource_percent': lib.percent(num_broken_resources, num_resources),
            'table': results}


def broken_links_option_combinations():
    for organization in lib.all_organizations(include_none=True):
        yield {'organization': organization}


def broken_links_post_access_filter(data, context):
    table = data.get('table', [])
    if not table:
        data['num_broken_packages'] = 0
        data['num_broken_resources'] = 0
        data['num_non_evaluable_resources'] = 0
        # Avoid leaking cached unfiltered totals when all rows were hidden.
        data['num_packages'] = None
        data['num_resources'] = None
        data['broken_package_percent'] = None
        data['broken_resource_percent'] = None
        return data

    # Organization index rows are pre-aggregated and do not reference
    # individual datasets, so we cannot safely recompute them here.
    if 'dataset_name' not in table[0]:
        return data

    broken_dataset_names = set()
    num_broken_resources = 0
    num_non_evaluable_resources = 0

    for row in table:
        if row.get('evaluation_state') == BROKEN:
            dataset_name = row.get('dataset_name')
            if dataset_name:
                broken_dataset_names.add(dataset_name)
            num_broken_resources += 1
        elif row.get('evaluation_state') == NON_EVALUABLE:
            num_non_evaluable_resources += 1

    data['num_broken_packages'] = len(broken_dataset_names)
    data['num_broken_resources'] = num_broken_resources
    data['num_non_evaluable_resources'] = num_non_evaluable_resources
    # Denominators are unknown after access filtering without extra privileged
    # queries; leave them unset in the summary.
    data['num_packages'] = None
    data['num_resources'] = None
    data['broken_package_percent'] = None
    data['broken_resource_percent'] = None
    return data


broken_links_report_info = {
    'name': 'broken-links',
    'title': _('Resource Links Report'),
    'description': _('Dataset resource URLs that are found to result in errors when resolved.'),
    'option_defaults': OrderedDict((('organization', None),
                                    )),
    'option_combinations': broken_links_option_combinations,
    'generate': broken_links,
    'post_access_filter': broken_links_post_access_filter,
    'template': 'report/broken_links.html',
    }


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
