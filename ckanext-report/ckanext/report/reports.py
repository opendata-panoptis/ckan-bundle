'''
Working examples - simple tag report.
'''

from ckan import model

try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from ckanext.report import lib


def tagless_report(organization):
    '''
    Produces a report on packages without tags.
    Returns something like this:
        {
         'table': [
            {'name': 'river-levels', 'title': 'River levels', 'notes': 'Harvested',
             'user': 'bob', 'created': '2008-06-13T10:24:59.435631'},
            {'name': 'co2-monthly', 'title' 'CO2 monthly', 'notes': '',
             'user': 'bob', 'created': '2009-12-14T08:42:45.473827'},
            ],
         'num_packages': 56,
         'packages_without_tags_percent': 4,
         'average_tags_per_package': 3.5,
        }
    '''
    # Find the packages without tags (excluding showcases)
    q = model.Session.query(model.Package) \
             .outerjoin(model.PackageTag) \
             .filter(model.PackageTag.id == None)  # noqa: E711

    # Filter to include only active datasets (excluding harvest sources)
    q = q.filter(model.Package.state == 'active')
    q = lib.filter_datasets_only(q)
    
    if organization:
        q = lib.filter_by_organizations(q, organization, False)
    tagless_pkgs = [OrderedDict((
        ('name', pkg.name),
        ('title', lib.resolve_dataset_title(pkg)),
        ('notes', lib.dataset_notes(pkg)),
        ('user', pkg.creator_user_id),
        ('created', pkg.metadata_created.isoformat()),
    )) for pkg in q.slice(0, 100)]  # First 100 only for this demo

    # Average number of tags per package
    q = model.Session.query(model.Package)
    q = q.filter(model.Package.state == 'active')
    q = lib.filter_datasets_only(q)
    if organization:
        q = lib.filter_by_organizations(q, organization, False)
    num_packages = q.count()
    q = q.join(model.PackageTag)
    num_taggings = q.count()
    if num_packages:
        average_tags_per_package = round(float(num_taggings) / num_packages, 1)
    else:
        average_tags_per_package = None
    packages_without_tags_percent = lib.percent(len(tagless_pkgs), num_packages)

    return {
        'table': tagless_pkgs,
        'num_packages': num_packages,
        'packages_without_tags_percent': packages_without_tags_percent,
        'average_tags_per_package': average_tags_per_package,
    }


def tagless_report_option_combinations():
    for organization in lib.all_organizations(include_none=True):
        yield {'organization': organization}


from ckan.plugins import toolkit

tagless_report_info = {
    'name': 'tagless-datasets',
    'title': toolkit._('Tagless Datasets'),
    'description': toolkit._('Datasets which have no tags.'),
    'option_defaults': OrderedDict((('organization', None),
                                    )),
    'option_combinations': tagless_report_option_combinations,
    'generate': tagless_report,
    'template': 'report/tagless-datasets.html',
}
