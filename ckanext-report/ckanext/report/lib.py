'''
These functions are for use by other extensions for their reports.
'''
from datetime import datetime
import json

import six
from six.moves import cStringIO as StringIO, zip
try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

import ckan.plugins as p
from ckan.plugins.toolkit import config


def resolve_dataset_title(pkg):
    '''Return the best available title for a dataset, preferring the core title,
    then multilingual variants and finally the dataset name.'''

    def _normalise(value):
        if not value:
            return None

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                parsed = json.loads(stripped)
            except ValueError:
                return stripped
            else:
                value = parsed

        if isinstance(value, dict):
            for lang in ('el', 'en'):
                lang_value = value.get(lang)
                if isinstance(lang_value, str):
                    lang_value = lang_value.strip()
                    if lang_value:
                        return lang_value
            for lang_value in value.values():
                if isinstance(lang_value, str):
                    lang_value = lang_value.strip()
                    if lang_value:
                        return lang_value
        return None

    title = (pkg.title or '').strip() if pkg.title else ''
    if title:
        return title

    translations = getattr(pkg, 'title_translated', None)
    resolved = _normalise(translations)
    if resolved:
        return resolved

    extras = getattr(pkg, 'extras', {}) or {}
    for lang in ('el', 'en'):
        resolved = _normalise(extras.get(f'title_translated-{lang}'))
        if resolved:
            return resolved

    resolved = _normalise(extras.get('title_translated'))
    if resolved:
        return resolved

    for key, value in extras.items():
        if key.startswith('title_translated-'):
            resolved = _normalise(value)
            if resolved:
                return resolved

    return pkg.name


def all_organizations(include_none=False):
    '''Yields all the organization names, and also None if requested. Useful
    when assembling option_combinations'''
    from ckan import model
    if include_none:
        yield None
    organizations = model.Session.query(model.Group).\
        filter(model.Group.type == 'organization').\
        filter(model.Group.state == 'active').order_by('name')
    for organization in organizations:
        yield organization.name


def go_down_tree(organization):
    '''Provided with an organization object, it walks down the hierarchy and yields
    each organization, including the one you supply.

    Essentially this is a slower version of Group.get_children_group_hierarchy
    because it returns Group objects, rather than dicts.
    '''
    yield organization
    for child in organization.get_children_groups(type='organization'):
        for grandchild in go_down_tree(child):
            yield grandchild


def filter_by_organizations(query, organization, include_sub_organizations):
    '''Given an SQLAlchemy ORM query object, it returns it filtered by the
    given organization and optionally its sub organizations too.
    '''
    from ckan import model
    if not organization:
        return query
    if isinstance(organization, six.string_types):
        organization = model.Group.get(organization)
        assert organization
    if include_sub_organizations:
        orgs = sorted([x for x in go_down_tree(organization)], key=lambda x: x.name)
        org_ids = [org.id for org in orgs]
        return query.filter(model.Package.owner_org.in_(org_ids))
    else:
        return query.filter(model.Package.owner_org == organization.id)


def dataset_notes(pkg):
    '''Returns a string with notes about the given package. It is
    configurable.'''
    expression = config.get('ckanext-report.notes.dataset')
    if not expression:
        return ''
    return eval(expression, None, {'pkg': pkg, 'asbool': p.toolkit.asbool})


def percent(numerator, denominator):
    if denominator == 0:
        return 100 if numerator else 0
    return int((numerator * 100.0) / denominator)


def make_csv_from_dicts(rows):
    import csv

    csvout = StringIO()

    # Προσθήκη BOM για σωστή εμφάνιση των Ελληνικών στο Excel
    csvout.write('\N{BOM}')

    csvwriter = csv.writer(
        csvout,
        dialect='excel',
        quoting=csv.QUOTE_NONNUMERIC
    )
    # extract the headers by looking at all the rows and
    # get a full list of the keys, retaining their ordering
    headers_ordered = []
    headers_set = set()
    for row in rows:
        new_headers = set(row.keys()) - headers_set
        headers_set |= new_headers
        for header in row.keys():
            if header in new_headers:
                headers_ordered.append(header)
    csvwriter.writerow(headers_ordered)
    for row in rows:
        items = []
        for header in headers_ordered:
            item = row.get(header, 'no record')
            if isinstance(item, datetime):
                item = item.strftime('%Y-%m-%d %H:%M')
            elif isinstance(item, (int, float, list, tuple)):
                item = six.text_type(item)
            elif item is None:
                item = ''
            else:
                item = str(item)
            items.append(item)
        try:
            csvwriter.writerow(items)
        except Exception as e:
            raise Exception("%s: %s, %s" % (e, row, items))
    csvout.seek(0)
    return csvout.read()


def ensure_data_is_dicts(data):
    '''Ensure that the data is a list of dicts, rather than a list of tuples
    with column names, as sometimes is the case. Changes it in place'''
    if data['table'] and isinstance(data['table'][0], (list, tuple)):
        new_data = []
        columns = data['columns']
        for row in data['table']:
            new_data.append(OrderedDict(zip(columns, row)))
        data['table'] = new_data
        del data['columns']


def anonymise_user_names(data, organization=None):
    '''Ensure any columns with names in are anonymised, unless the current user
    has privileges.

    NB this is only enabled for data.gov.uk - it is custom functionality.
    '''
    try:
        import ckanext.dgu.lib.helpers as dguhelpers
    except ImportError:
        # If this is not DGU then cannot do the anonymization
        return
    column_names = data['table'][0].keys() if data['table'] else []
    for col in column_names:
        if col.lower() in ('user', 'username', 'user name', 'author'):
            for row in data['table']:
                row[col] = dguhelpers.user_link_info(
                    row[col], organization=organization)[0]


def filter_datasets_only(query):
    '''Filter query to exclude showcases and other non-dataset types'''
    from ckan import model

    # Exclude only the types that can interfere with reports
    # (showcases, data-services, decisions have resources that can affect reporting)
    # Also exclude harvest sources which are not real datasets
    query = query.filter(~model.Package.type.in_(['showcase', 'data-service', 'decision', 'harvest']))

    return query


def _looks_like_dataset_row(row):
    # some reports store the dataset identifier under `name`, which
    # may clash with other report tables (eg organization summaries).
    return any(
        key in row
        for key in (
            'title',
            'dataset_title',
            'package_title',
            'notes',
            'dataset_notes',
            'frequency',
            'status',
            'resource_id',
            'resource_url',
            'created',
        )
    )


def _extract_package_id_from_report_row(row):
    if not isinstance(row, dict):
        return None

    # Prefer explicit dataset/package identifiers. Avoid generic `id` because
    # many report tables contain non-dataset IDs and we'd risk filtering out
    # unrelated rows.
    for key in ('dataset_name', 'package_name', 'package_id'):
        value = row.get(key)
        if isinstance(value, six.string_types) and value:
            return value

    for key in ('package', 'dataset'):
        value = row.get(key)
        if isinstance(value, dict):
            for subkey in ('id', 'name'):
                subval = value.get(subkey)
                if isinstance(subval, six.string_types) and subval:
                    return subval
        elif isinstance(value, six.string_types) and value:
            return value

    value = row.get('name')
    if isinstance(value, six.string_types) and value and _looks_like_dataset_row(row):
        return value

    return None


def _apply_post_access_filter(report, data, context):
    if not report:
        return data
    post_access_filter = getattr(report, 'post_access_filter', None)
    if not callable(post_access_filter):
        return data
    try:
        maybe_data = post_access_filter(data, context)
    except Exception:
        return data
    return maybe_data if maybe_data is not None else data


def filter_report_data_by_package_show_access(data, context, report=None):
    '''
    Filters report rows that reference datasets, keeping only rows
    for datasets the current user is allowed to `package_show`.

    This is applied at request-time (after pulling cached report data) so that
    private dataset details do not leak to unauthorized users via reports.
    '''
    if not isinstance(data, dict) or 'table' not in data:
        return data

    if context is None:
        context = {}

    # Some reports store tuples + `columns`. Normalize first.
    ensure_data_is_dicts(data)

    table = data.get('table')
    if not isinstance(table, list) or not table:
        return data

    try:
        import ckan.logic as logic
        from ckan.plugins import toolkit
    except Exception:
        # If CKAN isn't fully available (eg during imports), avoid breaking.
        return data

    filtered = []
    package_access_cache = {}
    for row in table:
        package_id = _extract_package_id_from_report_row(row)
        if not package_id:
            filtered.append(row)
            continue
        cached_access = package_access_cache.get(package_id)
        if cached_access is True:
            filtered.append(row)
            continue
        if cached_access is False:
            continue
        try:
            logic.check_access('package_show', context, {'id': package_id})
        except toolkit.NotAuthorized:
            package_access_cache[package_id] = False
            continue
        except Exception:
            # Treat unexpected failures (eg deleted datasets) as not visible.
            package_access_cache[package_id] = False
            continue
        package_access_cache[package_id] = True
        filtered.append(row)

    was_filtered = len(filtered) != len(table)
    data['table'] = filtered
    if was_filtered:
        data['access_filtered'] = True
        data = _apply_post_access_filter(report, data, context)
    return data
