import copy
try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from ckan.common import _
import ckan.model as model
import ckan.plugins as p

from ckanext.report import lib


def broken_links(organization):
    if organization is None:
        return broken_links_index()
    else:
        return broken_links_for_organization(organization=organization)


def broken_links_index():
    '''Returns the count of broken links for all organizations.'''

    from ckanext.archiver.model import Archival

    counts = {}
    # Get all the broken datasets and build up the results by org
    orgs = model.Session.query(model.Group)\
        .filter(model.Group.type == 'organization')\
        .filter(model.Group.state == 'active').all()
    for org in add_progress_bar(orgs):
        archivals = (model.Session.query(Archival)
            .filter(Archival.is_broken == True) # noqa
            .join(model.Package, Archival.package_id == model.Package.id)
            .filter(model.Package.owner_org == org.id)
            .filter(model.Package.state == 'active')
            .join(model.Resource, Archival.resource_id == model.Resource.id)
            .filter(model.Resource.state == 'active'))
        
        # Filter to exclude only the types that can interfere with reports
        archivals = archivals.filter(~model.Package.type.in_(['showcase', 'data-service', 'decision', 'harvest']))
        broken_resources = archivals.count()
        broken_datasets = archivals.distinct(model.Package.id).count()
        # Count datasets excluding showcases
        datasets_query = model.Session.query(model.Package)\
            .filter_by(owner_org=org.id)\
            .filter_by(state='active')
        
        # Filter to exclude only the types that can interfere with reports
        datasets_query = datasets_query.filter(~model.Package.type.in_(['showcase', 'data-service', 'decision', 'harvest']))
            
        num_datasets = datasets_query.count()
        num_resources = model.Session.query(model.Package)\
            .filter_by(owner_org=org.id)\
            .filter_by(state='active')
        if p.toolkit.check_ckan_version(max_version='2.2.99'):
            num_resources = num_resources.join(model.ResourceGroup)
        num_resources = num_resources \
            .join(model.Resource)\
            .filter_by(state='active')\
            .count()
        counts[org.name] = {
            'organization_title': org.title,
            'broken_packages': broken_datasets,
            'broken_resources': broken_resources,
            'packages': num_datasets,
            'resources': num_resources
        }

    results = counts

    data = []
    num_broken_packages = 0
    num_broken_resources = 0
    num_packages = 0
    num_resources = 0
    for org_name, org_counts in results.items():
        data.append(OrderedDict((
            ('organization_title', results[org_name]['organization_title']),
            ('organization_name', org_name),
            ('package_count', org_counts['packages']),
            ('resource_count', org_counts['resources']),
            ('broken_package_count', org_counts['broken_packages']),
            ('broken_package_percent', lib.percent(org_counts['broken_packages'], org_counts['packages'])),
            ('broken_resource_count', org_counts['broken_resources']),
            ('broken_resource_percent', lib.percent(org_counts['broken_resources'], org_counts['resources'])),
            )))
        # Totals - always use the counts, rather than counts_with_sub_orgs, to
        # avoid counting a package in both its org and parent org
        org_counts_ = counts[org_name]
        num_broken_packages += org_counts_['broken_packages']
        num_broken_resources += org_counts_['broken_resources']
        num_packages += org_counts_['packages']
        num_resources += org_counts_['resources']

    data.sort(key=lambda x: (-x['broken_package_count'],
                             -x['broken_resource_count']))

    return {'table': data,
            'num_broken_packages': num_broken_packages,
            'num_broken_resources': num_broken_resources,
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
    from ckanext.archiver.model import Archival

    org = model.Group.get(organization)
    if not org:
        raise p.toolkit.ObjectNotFound()

    name = org.name
    title = org.title

    archivals = (model.Session.query(Archival, model.Package, model.Group).
        filter(Archival.is_broken == True). # noqa
        join(model.Package, Archival.package_id == model.Package.id).
        filter(model.Package.state == 'active').
        join(model.Resource, Archival.resource_id == model.Resource.id).
        filter(model.Resource.state == 'active'))

    # Only the main organization is considered
    org_ids = [org.id]
    archivals = archivals.filter(model.Package.owner_org == org.id)

    archivals = archivals.join(model.Group, model.Package.owner_org == model.Group.id)
    
    # Filter to exclude only the types that can interfere with reports
    archivals = archivals.filter(~model.Package.type.in_(['showcase', 'data-service', 'decision', 'harvest']))

    results = []

    for archival, pkg, org in archivals.all():
        pkg = model.Package.get(archival.package_id)
        resource = model.Resource.get(archival.resource_id)

        via = ''
        er = pkg.extras.get('external_reference', '')
        if er == 'ONSHUB':
            via = "Stats Hub"
        elif er.startswith("DATA4NR"):
            via = "Data4nr"

        # CKAN 2.9 does not have revisions
        if p.toolkit.check_ckan_version(max_version="2.8.99"):
            archived_resource = model.Session.query(model.ResourceRevision)\
                                    .filter_by(id=resource.id)\
                                    .filter_by(revision_timestamp=archival.resource_timestamp)\
                                    .first() or resource
        else:
            archived_resource = resource

        row_data = OrderedDict((
            ('dataset_title', lib.resolve_dataset_title(pkg)),
            ('dataset_name', pkg.name),
            ('dataset_notes', lib.dataset_notes(pkg)),
            ('organization_title', org.title),
            ('organization_name', org.name),
            ('resource_position', resource.position),
            ('resource_id', resource.id),
            ('resource_url', archived_resource.url),
            ('url_up_to_date', resource.url == archived_resource.url),
            ('via', via),
            ('first_failure', archival.first_failure.isoformat() if archival.first_failure else None),
            ('last_updated', archival.updated.isoformat() if archival.updated else None),
            ('last_success', archival.last_success.isoformat() if archival.last_success else None),
            ('url_redirected_to', archival.url_redirected_to),
            ('reason', archival.reason),
            ('status', archival.status),
            ('failure_count', archival.failure_count),
            ))

        results.append(row_data)

    num_broken_packages = archivals.distinct(model.Package.name).count()
    num_broken_resources = len(results)

    # Get total number of packages & resources (excluding showcases)
    packages_query = model.Session.query(model.Package)\
                        .filter(model.Package.owner_org.in_(org_ids))\
                        .filter_by(state='active')
    
    # Filter to exclude only the types that can interfere with reports
    packages_query = packages_query.filter(~model.Package.type.in_(['showcase', 'data-service', 'decision', 'harvest']))
        
    num_packages = packages_query.count()
    num_resources = model.Session.query(model.Resource)\
                         .filter_by(state='active')
    if p.toolkit.check_ckan_version(max_version='2.2.99'):
        num_resources = num_resources.join(model.ResourceGroup)
    num_resources = num_resources \
        .join(model.Package)\
        .filter(model.Package.owner_org.in_(org_ids))\
        .filter_by(state='active').count()

    return {'organization_name': name,
            'organization_title': title,
            'num_broken_packages': num_broken_packages,
            'num_broken_resources': num_broken_resources,
            'num_packages': num_packages,
            'num_resources': num_resources,
            'broken_package_percent': lib.percent(num_broken_packages, num_packages),
            'broken_resource_percent': lib.percent(num_broken_resources, num_resources),
            'table': results}


def broken_links_option_combinations():
    for organization in lib.all_organizations(include_none=True):
        yield {'organization': organization}


broken_links_report_info = {
    'name': 'broken-links',
    'title': _('Broken links'),
    'description': _('Dataset resource URLs that are found to result in errors when resolved.'),
    'option_defaults': OrderedDict((('organization', None),
                                    )),
    'option_combinations': broken_links_option_combinations,
    'generate': broken_links,
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